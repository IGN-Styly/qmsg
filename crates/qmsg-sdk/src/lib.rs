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

use std::collections::{HashMap, VecDeque};
use std::io::{self, Read};
use std::net::TcpStream;
use std::time::{Duration, Instant};

pub use qmsg_types::{
    self as types, Channel, ChannelKind, ChannelRef, Command, CommandError, Content, ContentKind,
    ContentRule, DirectoryUpdate, LogLevel, Media, MediaSource, Message, MessageEvent,
    MessageLimits, Organization, ProviderConfig, Reply, TextUnit, User, Violation,
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
    shared: Shared,
}

/// Blobs answered without involving the provider, kept until released.
#[derive(Default)]
struct Shared {
    blobs: HashMap<String, Vec<u8>>,
    next: u64,
}

impl Shared {
    fn insert(&mut self, bytes: Vec<u8>) -> String {
        self.next += 1;
        let id = format!("shared-{}", self.next);
        self.blobs.insert(id.clone(), bytes);
        id
    }

    fn remove(&mut self, id: &str) -> bool {
        self.blobs.remove(id).is_some()
    }
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
            shared: Shared::default(),
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

    /// Answers a [`Command`] by its `request`.
    ///
    /// Emit any events the caller must keep before answering, including on
    /// partial failure. The host queues preceding events before delivering
    /// this answer, unless the provider is killed (which cancels requests and
    /// can discard events). Thus an answer may wait for event queue space;
    /// callers must drain events on a separate task from requests.
    pub fn reply(
        &mut self,
        request: u64,
        result: std::result::Result<Reply, CommandError>,
    ) -> Result {
        self.send(&ProviderMessage::Reply { request, result })
    }

    /// Makes `bytes` readable by the orchestrator as a [`MediaSource::Blob`],
    /// returning its id. [`Context::next_command`] answers its reads until the
    /// orchestrator releases it or [`Context::unshare`] is called.
    ///
    /// For files the provider has to fetch from the platform, use an id of
    /// its own instead and answer [`Command::ReadBlob`] itself.
    pub fn share(&mut self, bytes: Vec<u8>) -> String {
        self.shared.insert(bytes)
    }

    pub fn unshare(&mut self, id: &str) {
        self.shared.remove(id);
    }

    /// Reads up to `len` bytes, at most [`types::MAX_READ`], of a blob the
    /// orchestrator sent, starting at `offset`. Shorter than `len` only at
    /// the end of the blob.
    pub fn read_blob(&mut self, blob: &str, offset: u64, len: u32) -> Result<Vec<u8>> {
        check_id(blob)?;
        self.send(&ProviderMessage::ReadBlob {
            blob: blob.to_owned(),
            offset,
            len,
        })?;
        match self.answer()? {
            HostMessage::Blob {
                blob: b,
                offset: o,
                result,
            } if b == blob && o == offset => Ok(result?),
            other => Err(unexpected(other)),
        }
    }

    /// Reads a blob the orchestrator sent from start to end, such as to
    /// stream it into an upload.
    pub fn blob_reader(&mut self, blob: impl Into<String>) -> BlobReader<'_> {
        BlobReader {
            cx: self,
            blob: blob.into(),
            offset: 0,
            done: false,
        }
    }

    /// Tells the orchestrator about a change to the organizations, users and
    /// channels the provider is part of. Report a channel and its users before
    /// emitting messages from it.
    pub fn directory(&mut self, update: DirectoryUpdate) -> Result {
        self.send(&ProviderMessage::Directory(update))
    }

    /// Waits for the next command, or returns `None` once `timeout` elapses.
    ///
    /// Reads and releases of [shared](Context::share) blobs are handled here
    /// rather than returned.
    pub fn next_command(&mut self, timeout: Option<Duration>) -> Result<Option<Command>> {
        let deadline = timeout.map(|t| Instant::now() + t);
        loop {
            let Some(command) = self.receive_command(deadline)? else {
                return Ok(None);
            };
            match command {
                Command::ReadBlob {
                    request,
                    blob,
                    offset,
                    len,
                } if self.shared.blobs.contains_key(&blob) => {
                    let bytes = &self.shared.blobs[&blob];
                    let start = usize::try_from(offset).map_or(bytes.len(), |o| o.min(bytes.len()));
                    let len = len.min(types::MAX_READ) as usize;
                    let end = start + len.min(bytes.len() - start);
                    let chunk = bytes[start..end].to_vec();
                    self.reply(request, Ok(Reply::Blob(chunk)))?;
                }
                Command::ReleaseBlob { blob } if self.shared.remove(&blob) => {}
                command => return Ok(Some(command)),
            }
        }
    }

    fn receive_command(&mut self, deadline: Option<Instant>) -> Result<Option<Command>> {
        if let Some(command) = self.pending.pop_front() {
            return Ok(Some(command));
        }
        let timeout = match deadline {
            Some(deadline) => match deadline.checked_duration_since(Instant::now()) {
                // A zero timeout is an error for sockets.
                Some(left) if !left.is_zero() => Some(left),
                _ => return Ok(None),
            },
            None => None,
        };
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
        check_id(&key)?;
        self.send(&ProviderMessage::SecretGet { key: key.clone() })?;
        match self.answer()? {
            HostMessage::Secret { key: k, result } if k == key => Ok(result?),
            other => Err(unexpected(other)),
        }
    }

    /// Stores a secret. Fails if it doesn't fit in the provider's secret
    /// storage.
    pub fn secret_set(&mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Result {
        let key = key.into();
        check_id(&key)?;
        self.send(&ProviderMessage::SecretSet {
            key: key.clone(),
            value: value.into(),
        })?;
        self.secret_stored(&key)
    }

    /// Removes a secret. Removing one that isn't set is not an error.
    pub fn secret_delete(&mut self, key: impl Into<String>) -> Result {
        let key = key.into();
        check_id(&key)?;
        self.send(&ProviderMessage::SecretDelete { key: key.clone() })?;
        self.secret_stored(&key)
    }

    fn secret_stored(&mut self, key: &str) -> Result {
        match self.answer()? {
            HostMessage::SecretStored { key: k, result } if k == key => Ok(result?),
            other => Err(unexpected(other)),
        }
    }

    /// Waits for the answer to a request, saving commands that arrive first.
    fn answer(&mut self) -> Result<HostMessage> {
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

/// Reads a blob the orchestrator sent; see [`Context::blob_reader`].
pub struct BlobReader<'a> {
    cx: &'a mut Context,
    blob: String,
    offset: u64,
    done: bool,
}

impl Read for BlobReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.done || buf.is_empty() {
            return Ok(0);
        }
        let len = u32::try_from(buf.len())
            .unwrap_or(u32::MAX)
            .min(types::MAX_READ);
        let chunk = self
            .cx
            .read_blob(&self.blob, self.offset, len)
            .map_err(|e| io::Error::other(e.to_string()))?;
        if chunk.len() > len as usize {
            return Err(io::Error::other(
                "the orchestrator sent more than was asked",
            ));
        }
        buf[..chunk.len()].copy_from_slice(&chunk);
        self.offset += chunk.len() as u64;
        self.done = chunk.len() < len as usize;
        Ok(chunk.len())
    }
}

/// The orchestrator disconnects providers that send longer ids.
fn check_id(id: &str) -> Result {
    if id.len() > types::MAX_ID {
        return Err(format!(
            "`{}…` is over {} bytes",
            &id[..id.floor_char_boundary(32)],
            types::MAX_ID
        )
        .into());
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::thread;

    use super::*;

    /// Connects a context to a server running `script` on its own thread.
    fn connect(
        script: impl FnOnce(WebSocket<TcpStream>) + Send + 'static,
    ) -> (Context, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            script(tungstenite::accept(stream).unwrap());
        });
        let config = ProviderConfig {
            name: "test".into(),
            host_url: format!("ws://{addr}/token"),
            settings: Default::default(),
        };
        (Context::connect(config).unwrap(), server)
    }

    fn command(socket: &mut WebSocket<TcpStream>, command: Command) {
        let bytes = types::encode(&HostMessage::Command(command)).unwrap();
        socket.send(Frame::Binary(bytes.into())).unwrap();
    }

    fn blob_reply(socket: &mut WebSocket<TcpStream>) -> (u64, Vec<u8>) {
        loop {
            if let Frame::Binary(bytes) = socket.read().unwrap() {
                match types::decode(&bytes).unwrap() {
                    ProviderMessage::Reply {
                        request,
                        result: Ok(Reply::Blob(bytes)),
                    } => return (request, bytes),
                    other => panic!("expected a blob, got {other:?}"),
                }
            }
        }
    }

    #[test]
    fn next_command_times_out() {
        let (mut cx, server) = connect(|mut socket| {
            // Holds the connection open until the provider closes it.
            while socket.read().is_ok() {}
        });
        let started = Instant::now();
        assert!(
            cx.next_command(Some(Duration::from_millis(50)))
                .unwrap()
                .is_none()
        );
        assert!(started.elapsed() >= Duration::from_millis(50));
        drop(cx);
        server.join().unwrap();
    }

    #[test]
    fn shared_blobs_are_answered_until_released() {
        let bytes: Vec<u8> = (0..types::MAX_READ as usize + 10)
            .map(|i| i as u8)
            .collect();
        let expected = bytes.clone();
        let (mut cx, server) = connect(move |mut socket| {
            let read = |request, offset, len| Command::ReadBlob {
                request,
                blob: "shared-1".into(),
                offset,
                len,
            };
            // Asking for more than `MAX_READ` gets `MAX_READ`.
            command(&mut socket, read(1, 0, u32::MAX));
            assert_eq!(
                blob_reply(&mut socket),
                (1, expected[..types::MAX_READ as usize].to_vec())
            );
            command(&mut socket, read(2, types::MAX_READ as u64, u32::MAX));
            assert_eq!(
                blob_reply(&mut socket),
                (2, expected[types::MAX_READ as usize..].to_vec())
            );
            // Past the end is empty, not an error.
            command(&mut socket, read(3, u64::MAX, 10));
            assert_eq!(blob_reply(&mut socket), (3, Vec::new()));
            // Once released, reads go to the provider.
            command(
                &mut socket,
                Command::ReleaseBlob {
                    blob: "shared-1".into(),
                },
            );
            command(&mut socket, read(4, 0, 10));
            while socket.read().is_ok() {}
        });
        assert_eq!(cx.share(bytes), "shared-1");
        let next = cx.next_command(Some(Duration::from_secs(10))).unwrap();
        assert!(
            matches!(next, Some(Command::ReadBlob { request: 4, .. })),
            "{next:?}"
        );
        drop(cx);
        server.join().unwrap();
    }

    #[test]
    fn shared_blobs_are_kept_until_removed() {
        let mut shared = Shared::default();
        let a = shared.insert(vec![0; 6]);
        let b = shared.insert(vec![0; 4]);
        assert_ne!(a, b);
        assert!(shared.remove(&a));
        assert!(!shared.remove(&a));
        assert!(shared.blobs.contains_key(&b));
    }

    #[test]
    fn long_ids_are_refused_before_sending() {
        assert!(check_id(&"é".repeat(types::MAX_ID)).is_err());
        assert!(check_id(&"a".repeat(types::MAX_ID)).is_ok());
    }
}
