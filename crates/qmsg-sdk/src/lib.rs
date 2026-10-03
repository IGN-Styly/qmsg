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

pub use qmsg_types::{self as types, Command, LogLevel, Message, ProviderConfig};
use qmsg_types::{HostMessage, ProviderMessage};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{WebSocket, protocol::Message as Frame};

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
    socket: WebSocket<MaybeTlsStream<TcpStream>>,
    /// Commands that arrived while waiting for something else.
    pending: VecDeque<Command>,
}

impl Context {
    fn connect(config: ProviderConfig) -> Result<Self> {
        let (socket, _) = tungstenite::connect(&config.host_url)?;
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

    /// Hands a message received from the platform to the orchestrator.
    pub fn emit(&mut self, message: Message) -> Result {
        self.send(&ProviderMessage::Emit(message))
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

    pub fn kv_get(&mut self, key: impl Into<String>) -> Result<Option<Vec<u8>>> {
        let key = key.into();
        self.send(&ProviderMessage::KvGet { key: key.clone() })?;
        loop {
            match self.receive()? {
                HostMessage::Value { key: k, value } if k == key => return Ok(value),
                HostMessage::Command(command) => self.pending.push_back(command),
                other => return Err(unexpected(other)),
            }
        }
    }

    pub fn kv_set(&mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Result {
        self.send(&ProviderMessage::KvSet {
            key: key.into(),
            value: value.into(),
        })
    }

    fn send(&mut self, message: &ProviderMessage) -> Result {
        self.socket
            .send(Frame::Binary(types::encode(message)?.into()))?;
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
        if let MaybeTlsStream::Plain(stream) = self.socket.get_mut() {
            stream.set_read_timeout(timeout)?;
        }
        Ok(())
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
