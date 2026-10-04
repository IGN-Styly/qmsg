//! SDK for qmsg providers.
//!
//! A provider is a `wasm32-wasip2` component that owns its own network
//! connections, and talks to the orchestrator over a WebSocket that
//! [`Context`] manages. Implement [`Provider`] and register it with
//! [`export_provider!`]:
//!
//! ```ignore
//! struct MyProvider;
//!
//! impl qmsg_sdk::Provider for MyProvider {
//!     fn run(cx: &mut qmsg_sdk::Context) -> qmsg_sdk::Result {
//!         cx.log(qmsg_sdk::LogLevel::Info, "started");
//!         Ok(())
//!     }
//! }
//!
//! qmsg_sdk::export_provider!(MyProvider);
//! ```

use std::collections::VecDeque;
use std::io;
use std::net::TcpStream;
use std::time::Duration;

pub use qmsg_types::{
    self as types, Channel, ChannelKind, ChannelRef, Command, Content, ContentKind,
    DirectoryUpdate, LogLevel, Media, MediaSource, Message, MessageEvent, Organization,
    ProviderConfig, User,
};
use qmsg_types::{HostMessage, ProviderMessage};
use tungstenite::WebSocket;
use tungstenite::protocol::{Message as Frame, WebSocketConfig};

#[doc(hidden)]
pub mod bindings {
    wit_bindgen::generate!({
        path: "../../wit",
        world: "provider",
        pub_export_macro: true,
        export_macro_name: "__export_provider_world",
        default_bindings_module: "qmsg_sdk::bindings",
    });
}

pub type Error = Box<dyn std::error::Error>;
pub type Result<T = ()> = std::result::Result<T, Error>;

pub trait Provider {
    /// Runs until the provider is done. Returning ends the provider.
    fn run(cx: &mut Context) -> Result;
}

/// The provider's connection to the orchestrator.
pub struct Context {
    config: ProviderConfig,
    socket: WebSocket<TcpStream>,
    /// Commands that arrived while waiting for something else.
    pending: VecDeque<Command>,
}

impl Context {
    fn connect(config: ProviderConfig) -> Result<Self> {
        let addr = config
            .host_url
            .strip_prefix("ws://")
            .and_then(|rest| rest.split('/').next())
            .ok_or_else(|| format!("invalid host url `{}`", config.host_url))?;
        let limits = WebSocketConfig::default()
            .max_message_size(Some(types::MAX_FRAME))
            .max_frame_size(Some(types::MAX_FRAME));
        let (socket, _) = tungstenite::client::client_with_config(
            &config.host_url,
            TcpStream::connect(addr)?,
            Some(limits),
        )
        .map_err(|e| e.to_string())?;
        Ok(Self {
            config,
            socket,
            pending: VecDeque::new(),
        })
    }

    pub fn config(&self) -> &ProviderConfig {
        &self.config
    }

    /// Looks up a setting from the provider's config.
    pub fn setting(&self, key: &str) -> Result<&str> {
        self.config
            .settings
            .get(key)
            .map(String::as_str)
            .ok_or_else(|| format!("missing setting `{key}`").into())
    }

    pub fn log(&mut self, level: LogLevel, message: impl Into<String>) {
        // Logging must not fail the provider.
        let _ = self.send(&ProviderMessage::Log {
            level,
            message: message.into(),
        });
    }

    /// Tells the orchestrator something happened to a message: a [`Message`]
    /// that was received, or a [`MessageEvent`] such as an edit.
    pub fn emit(&mut self, event: impl Into<MessageEvent>) -> Result {
        self.send(&ProviderMessage::Message(event.into()))
    }

    /// Answers a [`Command::Send`] with the sent message's id, or why it
    /// wasn't sent.
    pub fn sent(&mut self, request: u64, result: std::result::Result<String, String>) -> Result {
        self.send(&ProviderMessage::Sent { request, result })
    }

    /// Tells the orchestrator about a change to the organizations, users and
    /// channels the provider is part of. Report a channel and its users before
    /// emitting messages from it.
    pub fn directory(&mut self, update: DirectoryUpdate) -> Result {
        self.send(&ProviderMessage::Directory(update))
    }

    /// Waits for the next command, or returns `None` once `timeout` elapses.
    pub fn next_command(&mut self, timeout: Option<Duration>) -> Result<Option<Command>> {
        if let Some(command) = self.pending.pop_front() {
            return Ok(Some(command));
        }
        self.set_read_timeout(timeout)?;
        let result = match self.receive() {
            Ok(HostMessage::Command(command)) => Ok(Some(command)),
            Ok(other) => Err(unexpected(other)),
            Err(e) if is_timeout(&e) => Ok(None),
            Err(e) => Err(e),
        };
        self.set_read_timeout(None)?;
        result
    }

    /// Reads a secret, or `None` if it was never set.
    ///
    /// Secrets are kept by the orchestrator, encrypted with a key from the
    /// OS keychain when one is available. Each provider sees only its own.
    pub fn secret_get(&mut self, key: impl Into<String>) -> Result<Option<Vec<u8>>> {
        let key = key.into();
        self.send(&ProviderMessage::SecretGet { key: key.clone() })?;
        match self.reply()? {
            HostMessage::Secret { key: k, result } if k == key => Ok(result?),
            other => Err(unexpected(other)),
        }
    }

    /// Stores a secret. Fails if it doesn't fit in the provider's secret
    /// storage.
    pub fn secret_set(&mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Result {
        let key = key.into();
        self.send(&ProviderMessage::SecretSet {
            key: key.clone(),
            value: value.into(),
        })?;
        self.secret_stored(&key)
    }

    /// Removes a secret. Removing one that isn't set is not an error.
    pub fn secret_delete(&mut self, key: impl Into<String>) -> Result {
        let key = key.into();
        self.send(&ProviderMessage::SecretDelete { key: key.clone() })?;
        self.secret_stored(&key)
    }

    fn secret_stored(&mut self, key: &str) -> Result {
        match self.reply()? {
            HostMessage::SecretStored { key: k, result } if k == key => Ok(result?),
            other => Err(unexpected(other)),
        }
    }

    /// Waits for the answer to a request, saving commands that arrive first.
    fn reply(&mut self) -> Result<HostMessage> {
        loop {
            match self.receive()? {
                HostMessage::Command(command) => self.pending.push_back(command),
                reply => return Ok(reply),
            }
        }
    }

    /// Fails without sending if the message is over [`types::MAX_FRAME`],
    /// which would otherwise close the connection.
    fn send(&mut self, message: &ProviderMessage) -> Result {
        let bytes = types::encode(message)?;
        if bytes.len() > types::MAX_FRAME {
            return Err(format!(
                "message is {} bytes, over the {} byte limit",
                bytes.len(),
                types::MAX_FRAME
            )
            .into());
        }
        self.socket.send(Frame::Binary(bytes.into()))?;
        Ok(())
    }

    fn receive(&mut self) -> Result<HostMessage> {
        loop {
            match self.socket.read()? {
                Frame::Binary(bytes) => return Ok(types::decode(&bytes)?),
                Frame::Close(_) => return Err("orchestrator closed the connection".into()),
                _ => {}
            }
        }
    }

    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result {
        Ok(self.socket.get_mut().set_read_timeout(timeout)?)
    }
}

fn is_timeout(error: &Error) -> bool {
    matches!(
        error.downcast_ref::<tungstenite::Error>(),
        Some(tungstenite::Error::Io(e))
            if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut)
    )
}

fn unexpected(message: HostMessage) -> Error {
    format!("unexpected message from orchestrator: {message:?}").into()
}

#[doc(hidden)]
pub fn __run<P: Provider>(config: Vec<u8>) -> std::result::Result<(), String> {
    let config = types::decode(&config).map_err(|e| format!("invalid provider config: {e}"))?;
    let mut cx =
        Context::connect(config).map_err(|e| format!("connecting to orchestrator: {e}"))?;
    P::run(&mut cx).map_err(|e| e.to_string())
}

/// Exports a [`Provider`] implementation as the component's entry point.
#[macro_export]
macro_rules! export_provider {
    ($provider:ty) => {
        struct __QmsgProvider;

        impl $crate::bindings::Guest for __QmsgProvider {
            fn abi_version() -> u32 {
                $crate::types::ABI_VERSION
            }

            fn run(config: ::std::vec::Vec<u8>) -> ::std::result::Result<(), ::std::string::String> {
                $crate::__run::<$provider>(config)
            }
        }

        $crate::bindings::__export_provider_world!(__QmsgProvider with_types_in $crate::bindings);
    };
}
