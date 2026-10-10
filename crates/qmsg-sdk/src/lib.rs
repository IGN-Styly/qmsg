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
//!
//! # Staying responsive
//!
//! Providers run on one thread, so they must not block on their platform
//! while commands wait. [`Context::wait`] sleeps until a command arrives, a
//! blob read started with [`Context::start_read`] is answered, or one of the
//! provider's own sockets can be read or written. Use non-blocking sockets,
//! look names up with [`Context::start_lookup`], connect with
//! [`start_connect`], and keep slow work, such as an upload or
//! a send waiting for the platform's answer, as state to finish later,
//! rather than waiting for it in place. Then autocomplete, other commands and
//! `Shutdown` are answered while it runs:
//!
//! ```ignore
//! loop {
//!     let mut sources = vec![Source::read(platform.as_fd())];
//!     if platform.has_output() {
//!         sources.push(Source::write(platform.as_fd()));
//!     }
//!     cx.wait(&sources, None)?;
//!     while let Some(event) = platform.try_read()? {
//!         // Emit it, or finish the pending send it answers.
//!     }
//!     while let Some(command) = cx.next_command(Some(Duration::ZERO))? {
//!         // Answer it, or start it and keep it as pending work.
//!     }
//!     // Take blob reads that arrived, and write what the platform takes.
//! }
//! ```
//!
//! The orchestrator sends [`Command::Cancel`] once it stops waiting for a
//! request. Commands still queued here are skipped and answered with
//! [`CommandError::Cancelled`]; a skipped [`Command::Send`], or call of a
//! declared [`ActionKind::SendMessage`] in a channel, with a nonce is
//! reported as [`MessageEvent::NotSent`] first. For a command already
//! started, check [`Context::is_cancelled`] before the step that can't be
//! undone. Once that step is done, finish as usual: cancelling doesn't undo
//! it. Answer every request either way, since the orchestrator counts it
//! against its limits until then.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, Read};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub use qmsg_types::{
    self as types, ActionKind, Arguments, Channel, ChannelKind, ChannelRef, Choice, Command,
    CommandError, CompletionItem, CompletionPage, CompletionRequest, Content, ContentKind,
    ContentRule, DirectoryUpdate, Field, FormattedText, Function, FunctionCall, FunctionContext,
    FunctionInput, FunctionInputRef, FunctionKind, HistoryPage, InputHint, InputIssue, LogLevel,
    LoginStep, Media, MediaSource, Mention, Message, MessageEvent, MessageLimits, Organization,
    Presence, ProviderConfig, ProviderStatus, Reaction, Reply, Span, Style, TextUnit, User, Value,
    ValueType, Verification, VerificationRequest, Violation, inputs,
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
    /// Commands that arrived while waiting for something else. The
    /// orchestrator limits how many requests wait, which bounds this.
    pending: VecDeque<Command>,
    /// Started requests the orchestrator stopped waiting for, newest last.
    cancelled: VecDeque<u64>,
    /// Blob reads started with [`Context::start_read`], by blob and offset.
    reads: HashMap<(String, u64), BlobRead>,
    /// Lookups started with [`Context::start_lookup`], by name.
    lookups: HashMap<String, Lookup>,
    /// Ids of the provider's [`ActionKind::SendMessage`] functions, to report
    /// skipped calls as [`MessageEvent::NotSent`].
    send_functions: HashSet<String>,
    shared: Shared,
}

/// A blob read waiting for its answer, or holding it.
enum BlobRead {
    Waiting {
        len: u32,
    },
    /// Dropped when the answer comes.
    Forgotten,
    Done(std::result::Result<Vec<u8>, String>),
}

/// A name lookup waiting for its answer, or holding it.
enum Lookup {
    Waiting,
    /// Dropped when the answer comes.
    Forgotten,
    Done(std::result::Result<Vec<SocketAddr>, String>),
}

/// The most cancelled requests [`Context::is_cancelled`] remembers.
const MAX_CANCELLED: usize = 256;

/// What [`Context::wait`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wake {
    /// A command is waiting for [`Context::next_command`].
    pub command: bool,
    /// Which of the sources are ready, in order. A readable socket can
    /// still hold only part of a message, or be closed; a failed one is
    /// ready for both.
    pub sources: Vec<bool>,
}

/// A socket [`Context::wait`] waits on.
#[cfg(any(unix, target_os = "wasi"))]
#[derive(Debug, Clone, Copy)]
pub struct Source<'a> {
    fd: std::os::fd::BorrowedFd<'a>,
    events: std::ffi::c_short,
}

#[cfg(any(unix, target_os = "wasi"))]
impl<'a> Source<'a> {
    /// Ready once the socket can be read.
    pub fn read(fd: std::os::fd::BorrowedFd<'a>) -> Self {
        Self {
            fd,
            events: libc::POLLIN,
        }
    }

    /// Ready once the socket takes more output, or has connected.
    pub fn write(fd: std::os::fd::BorrowedFd<'a>) -> Self {
        Self {
            fd,
            events: libc::POLLOUT,
        }
    }
}

/// Blobs answered without involving the provider, kept until released.
#[derive(Default)]
struct Shared {
    blobs: HashMap<String, Arc<Vec<u8>>>,
    next: u64,
}

impl Shared {
    fn insert(&mut self, bytes: Arc<Vec<u8>>) -> String {
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
            cancelled: VecDeque::new(),
            reads: HashMap::new(),
            lookups: HashMap::new(),
            send_functions: HashSet::new(),
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

    /// Reports where the provider is in connecting to its platform. Report
    /// `Syncing`, the directory, then `Ready`, and the next login step
    /// before answering a login function.
    pub fn status(&mut self, status: ProviderStatus) -> Result {
        self.send(&ProviderMessage::Status(status))
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
        self.cancelled.retain(|&r| r != request);
        if let Ok(Reply::Functions(functions)) = &result {
            let sends = functions.iter().filter(|f| {
                matches!(
                    f.kind,
                    FunctionKind::Action {
                        action: ActionKind::SendMessage,
                        ..
                    }
                )
            });
            self.send_functions.extend(sends.map(|f| f.id.clone()));
        }
        self.send(&ProviderMessage::Reply { request, result })
    }

    /// Whether the orchestrator stopped waiting for a request the provider
    /// has started. Check before a step that can't be undone. If it was
    /// cancelled, skip that step and answer [`CommandError::Cancelled`],
    /// reporting a send with a nonce as [`MessageEvent::NotSent`].
    pub fn is_cancelled(&mut self, request: u64) -> Result<bool> {
        self.drain()?;
        Ok(self.cancelled.contains(&request))
    }

    /// Makes `bytes` readable by the orchestrator as a [`MediaSource::Blob`],
    /// returning its id. [`Context::next_command`] answers its reads until the
    /// orchestrator releases it or [`Context::unshare`] is called.
    ///
    /// Each id belongs to the one event or reply it is sent in. To send the
    /// same file again, such as in history, share it again: an `Arc` of it
    /// is not copied.
    ///
    /// For files the provider has to fetch from the platform, use an id of
    /// its own instead and answer [`Command::ReadBlob`] itself.
    pub fn share(&mut self, bytes: impl Into<Arc<Vec<u8>>>) -> String {
        self.shared.insert(bytes.into())
    }

    pub fn unshare(&mut self, id: &str) {
        self.shared.remove(id);
    }

    /// Reads up to `len` bytes, at most [`types::MAX_READ`], of a blob the
    /// orchestrator sent, starting at `offset`. Shorter than `len` only at
    /// the end of the blob.
    ///
    /// This waits for the answer, however slow the orchestrator's source;
    /// use [`Context::start_read`] to keep answering commands meanwhile.
    pub fn read_blob(&mut self, blob: &str, offset: u64, len: u32) -> Result<Vec<u8>> {
        self.start_read(blob, offset, len)?;
        loop {
            if let Some(result) = self.read_result(blob, offset) {
                return Ok(result?);
            }
            match self.receive()? {
                HostMessage::Command(command) => self.accept(command)?,
                other => self.answered(other)?,
            }
        }
    }

    /// Starts reading up to `len` bytes, at most [`types::MAX_READ`], of a
    /// blob the orchestrator sent, starting at `offset`, without waiting.
    /// Take the answer with [`Context::take_read`]; [`Context::wait`] returns
    /// once it has come. One read of a blob at an offset can run at a time,
    /// and the orchestrator fails reads over [`types::MAX_BLOB_READS`] at
    /// once, counting forgotten ones until their answer comes.
    pub fn start_read(&mut self, blob: &str, offset: u64, len: u32) -> Result {
        check_id(blob)?;
        let key = (blob.to_owned(), offset);
        if self.reads.contains_key(&key) {
            return Err(format!("`{blob}` is already being read at {offset}").into());
        }
        let len = len.min(types::MAX_READ);
        self.send(&ProviderMessage::ReadBlob {
            blob: blob.to_owned(),
            offset,
            len,
        })?;
        self.reads.insert(key, BlobRead::Waiting { len });
        Ok(())
    }

    /// The answer to a read started with [`Context::start_read`], or `None`
    /// if it hasn't come yet. The bytes are shorter than asked only at the
    /// end of the blob; the inner error says why the read itself failed.
    pub fn take_read(
        &mut self,
        blob: &str,
        offset: u64,
    ) -> Result<Option<std::result::Result<Vec<u8>, String>>> {
        self.drain()?;
        Ok(self.read_result(blob, offset))
    }

    /// Stops a read started with [`Context::start_read`]: its answer is
    /// dropped when it comes.
    pub fn forget_read(&mut self, blob: &str, offset: u64) {
        let key = (blob.to_owned(), offset);
        match self.reads.get(&key) {
            Some(BlobRead::Waiting { .. }) => {
                self.reads.insert(key, BlobRead::Forgotten);
            }
            Some(_) => {
                self.reads.remove(&key);
            }
            None => {}
        }
    }

    fn read_result(
        &mut self,
        blob: &str,
        offset: u64,
    ) -> Option<std::result::Result<Vec<u8>, String>> {
        let key = (blob.to_owned(), offset);
        match self.reads.remove(&key)? {
            BlobRead::Done(result) => Some(result),
            other => {
                self.reads.insert(key, other);
                None
            }
        }
    }

    /// Starts looking up the addresses of `name`, a host and port such as
    /// `example.com:443`, without waiting. The orchestrator looks it up,
    /// since a lookup here would block the provider. Take the answer with
    /// [`Context::take_lookup`]; [`Context::wait`] returns once it has come.
    /// One lookup of a name can run at a time, and the orchestrator fails
    /// lookups over [`types::MAX_LOOKUPS`] at once, counting forgotten ones
    /// until their answer comes. It fails ones that take over its timeout,
    /// so an answer always comes. Looking up a forgotten name again takes
    /// the answer to the lookup still running.
    pub fn start_lookup(&mut self, name: &str) -> Result {
        check_id(name)?;
        match self.lookups.get_mut(name) {
            Some(Lookup::Waiting) => {
                return Err(format!("`{name}` is already being looked up").into());
            }
            Some(lookup @ Lookup::Forgotten) => {
                *lookup = Lookup::Waiting;
                return Ok(());
            }
            _ => {}
        }
        self.send(&ProviderMessage::Lookup {
            name: name.to_owned(),
        })?;
        self.lookups.insert(name.to_owned(), Lookup::Waiting);
        Ok(())
    }

    /// The answer to a lookup started with [`Context::start_lookup`], or
    /// `None` if it hasn't come yet. The inner error says why the lookup
    /// failed, such as an unknown name or a timeout.
    pub fn take_lookup(
        &mut self,
        name: &str,
    ) -> Result<Option<std::result::Result<Vec<SocketAddr>, String>>> {
        self.drain()?;
        match self.lookups.remove(name) {
            Some(Lookup::Done(result)) => Ok(Some(result)),
            Some(other) => {
                self.lookups.insert(name.to_owned(), other);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    /// Stops a lookup started with [`Context::start_lookup`]: its answer is
    /// dropped when it comes.
    pub fn forget_lookup(&mut self, name: &str) {
        match self.lookups.get_mut(name) {
            Some(lookup @ Lookup::Waiting) => *lookup = Lookup::Forgotten,
            Some(_) => {
                self.lookups.remove(name);
            }
            None => {}
        }
    }

    /// Keeps the answer to a started blob read or lookup. Anything else is
    /// unexpected here.
    fn answered(&mut self, message: HostMessage) -> Result {
        let (blob, offset, result) = match message {
            HostMessage::Blob {
                blob,
                offset,
                result,
            } => (blob, offset, result),
            HostMessage::Addresses { name, result } => {
                return match self.lookups.remove(&name) {
                    Some(Lookup::Waiting) => {
                        self.lookups.insert(name, Lookup::Done(result));
                        Ok(())
                    }
                    Some(Lookup::Forgotten) => Ok(()),
                    _ => Err(format!("unexpected lookup of `{name}`").into()),
                };
            }
            message => return Err(unexpected(message)),
        };
        let key = (blob, offset);
        match self.reads.remove(&key) {
            Some(BlobRead::Waiting { len }) => {
                let result = result.and_then(|bytes| match bytes.len() <= len as usize {
                    true => Ok(bytes),
                    false => Err("the orchestrator sent more than was asked".into()),
                });
                self.reads.insert(key, BlobRead::Done(result));
                Ok(())
            }
            Some(BlobRead::Forgotten) => Ok(()),
            _ => Err(format!("unexpected read of `{}` at {}", key.0, key.1).into()),
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
    /// `Some(Duration::ZERO)` only takes a command that has already arrived.
    ///
    /// Reads and releases of [shared](Context::share) blobs, and
    /// cancellations, are handled here rather than returned.
    pub fn next_command(&mut self, timeout: Option<Duration>) -> Result<Option<Command>> {
        let deadline = timeout.map(|t| Instant::now() + t);
        loop {
            // Read ahead, so commands cancelled while queued are skipped.
            self.drain()?;
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
            let received = self.receive();
            self.set_read_timeout(None)?;
            match received {
                Ok(HostMessage::Command(command)) => self.accept(command)?,
                Ok(other) => self.answered(other)?,
                Err(e) if is_timeout(&e) => return Ok(None),
                Err(e) => return Err(e),
            }
        }
    }

    /// Waits until a command arrives, a started blob read or lookup is
    /// answered, one
    /// of `sources` is ready, or `timeout` elapses. Data the provider already
    /// buffered, such as in a `BufReader`, doesn't count: read that before
    /// waiting.
    #[cfg(any(unix, target_os = "wasi"))]
    pub fn wait(&mut self, sources: &[Source<'_>], timeout: Option<Duration>) -> Result<Wake> {
        use std::os::fd::AsFd;
        self.drain()?;
        let answered = self.reads.values().any(|r| matches!(r, BlobRead::Done(_)))
            || self.lookups.values().any(|l| matches!(l, Lookup::Done(_)));
        let timeout = match self.pending.is_empty() && !answered {
            true => timeout,
            false => Some(Duration::ZERO),
        };
        let mut fds = vec![Source::read(self.socket.get_ref().as_fd())];
        fds.extend_from_slice(sources);
        let ready = poll::poll(&fds, timeout)?;
        if ready[0] {
            self.drain()?;
        }
        Ok(Wake {
            command: !self.pending.is_empty(),
            sources: ready[1..].to_vec(),
        })
    }

    /// Takes every message that has already arrived, without blocking.
    fn drain(&mut self) -> Result {
        let mut arrived = Vec::new();
        self.socket.get_mut().set_nonblocking(true)?;
        let result = loop {
            match self.receive() {
                Ok(message) => arrived.push(message),
                Err(e) if is_timeout(&e) => break Ok(()),
                Err(e) => break Err(e),
            }
        };
        self.socket.get_mut().set_nonblocking(false)?;
        result?;
        // Handled once blocking again, since answers can be large writes.
        arrived.into_iter().try_for_each(|message| match message {
            HostMessage::Command(command) => self.accept(command),
            other => self.answered(other),
        })
    }

    /// Handles what the SDK can on its own, and queues the rest.
    fn accept(&mut self, command: Command) -> Result {
        match command {
            Command::ReadBlob {
                request,
                blob,
                offset,
                len,
            } if self.shared.blobs.contains_key(&blob) => {
                let bytes = self.shared.blobs[&blob].clone();
                let start = usize::try_from(offset).map_or(bytes.len(), |o| o.min(bytes.len()));
                let len = len.min(types::MAX_READ) as usize;
                let end = start + len.min(bytes.len() - start);
                let chunk = bytes[start..end].to_vec();
                self.reply(request, Ok(Reply::Blob(chunk)))?;
            }
            Command::ReleaseBlob { blob } if self.shared.remove(&blob) => {}
            Command::Cancel { request } => {
                let queued = self
                    .pending
                    .iter()
                    .position(|c| c.request() == Some(request));
                match queued.and_then(|i| self.pending.remove(i)) {
                    Some(command) => {
                        // It never reached the platform.
                        if let Some((channel, nonce)) = self.unsent(command) {
                            self.emit(MessageEvent::NotSent {
                                channel,
                                nonce,
                                error: CommandError::Cancelled,
                            })?;
                        }
                        self.reply(request, Err(CommandError::Cancelled))?;
                    }
                    None => {
                        if self.cancelled.len() == MAX_CANCELLED {
                            self.cancelled.pop_front();
                        }
                        self.cancelled.push_back(request);
                    }
                }
            }
            command => self.pending.push_back(command),
        }
        Ok(())
    }

    /// The channel and nonce of a send, by command or by a declared
    /// `SendMessage` function, if it has a nonce.
    fn unsent(&self, command: Command) -> Option<(ChannelRef, String)> {
        match command {
            Command::Send {
                channel,
                nonce: Some(nonce),
                ..
            } => Some((channel, nonce)),
            Command::Call { mut call, .. } if self.send_functions.contains(&call.function) => {
                match (call.context, call.arguments.remove(inputs::NONCE)) {
                    (FunctionContext::Channel(channel), Some(Value::Text(nonce))) => {
                        Some((channel, nonce))
                    }
                    _ => None,
                }
            }
            _ => None,
        }
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

    /// Waits for the answer to a request, saving commands and blob reads
    /// that arrive first.
    fn answer(&mut self) -> Result<HostMessage> {
        loop {
            match self.receive()? {
                HostMessage::Command(command) => self.accept(command)?,
                answer @ (HostMessage::Blob { .. } | HostMessage::Addresses { .. }) => {
                    self.answered(answer)?
                }
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

#[cfg(any(unix, target_os = "wasi"))]
mod poll {
    use std::ffi::c_int;
    use std::io;
    use std::os::fd::AsRawFd;
    use std::time::Duration;

    /// Which of `sources` are ready, or have closed or failed. WASI's libc
    /// polls WASI streams.
    pub fn poll(sources: &[super::Source<'_>], timeout: Option<Duration>) -> io::Result<Vec<bool>> {
        let mut fds: Vec<_> = sources
            .iter()
            .map(|source| libc::pollfd {
                fd: source.fd.as_raw_fd(),
                events: source.events,
                revents: 0,
            })
            .collect();
        // Rounded up, so a short wait doesn't spin.
        let timeout = timeout.map_or(-1, |t| {
            t.as_nanos().div_ceil(1_000_000).min(c_int::MAX as u128) as c_int
        });
        loop {
            // SAFETY: `fds` is a valid array of `fds.len()` entries, and the
            // descriptors are borrowed for the call.
            let n = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout) };
            if n >= 0 {
                return Ok(fds.iter().map(|fd| fd.revents != 0).collect());
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
}

/// Starts connecting to `addr` without waiting for it. The stream is
/// non-blocking. Wait for [`Source::write`] on it, then check
/// [`TcpStream::take_error`]: `None` means it connected.
///
/// Look names up first, with [`Context::start_lookup`].
///
/// Only on WASI, where providers run, and Linux, for testing. Elsewhere it
/// fails with [`io::ErrorKind::Unsupported`] rather than wait.
#[cfg(any(target_os = "linux", target_os = "wasi"))]
pub fn start_connect(addr: SocketAddr) -> io::Result<TcpStream> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    let family = match addr {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    };
    // SAFETY: plain FFI calls; the descriptor is owned once created, and the
    // addresses are valid for their sizes.
    unsafe {
        let fd = libc::socket(family, libc::SOCK_STREAM | libc::SOCK_NONBLOCK, 0);
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let socket = OwnedFd::from_raw_fd(fd);
        let connected = match addr {
            SocketAddr::V4(addr) => {
                let mut raw: libc::sockaddr_in = std::mem::zeroed();
                raw.sin_family = libc::AF_INET as libc::sa_family_t;
                raw.sin_port = addr.port().to_be();
                raw.sin_addr.s_addr = u32::from_ne_bytes(addr.ip().octets());
                let len = size_of::<libc::sockaddr_in>() as libc::socklen_t;
                libc::connect(
                    socket.as_raw_fd(),
                    (&raw as *const libc::sockaddr_in).cast(),
                    len,
                )
            }
            SocketAddr::V6(addr) => {
                let mut raw: libc::sockaddr_in6 = std::mem::zeroed();
                raw.sin6_family = libc::AF_INET6 as libc::sa_family_t;
                raw.sin6_port = addr.port().to_be();
                raw.sin6_flowinfo = addr.flowinfo();
                raw.sin6_addr.s6_addr = addr.ip().octets();
                raw.sin6_scope_id = addr.scope_id();
                let len = size_of::<libc::sockaddr_in6>() as libc::socklen_t;
                libc::connect(
                    socket.as_raw_fd(),
                    (&raw as *const libc::sockaddr_in6).cast(),
                    len,
                )
            }
        };
        if connected < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EINPROGRESS) {
                return Err(error);
            }
        }
        Ok(TcpStream::from(socket))
    }
}

/// Fails: see the other platforms' `start_connect`.
#[cfg(not(any(target_os = "linux", target_os = "wasi")))]
pub fn start_connect(addr: SocketAddr) -> io::Result<TcpStream> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("can't connect to {addr} without waiting on this platform"),
    ))
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
            // Commands sent together arrive together.
            stream.set_nodelay(true).unwrap();
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
        let a = shared.insert(Arc::new(vec![0; 6]));
        let b = shared.insert(Arc::new(vec![0; 4]));
        assert_ne!(a, b);
        assert!(shared.remove(&a));
        assert!(!shared.remove(&a));
        assert!(shared.blobs.contains_key(&b));
    }

    fn provider_message(socket: &mut WebSocket<TcpStream>) -> ProviderMessage {
        loop {
            if let Frame::Binary(bytes) = socket.read().unwrap() {
                return types::decode(&bytes).unwrap();
            }
        }
    }

    fn send(request: u64, nonce: &str) -> Command {
        Command::Send {
            request,
            channel: ChannelRef::new(None, "c"),
            reply_to: None,
            content: vec![Content::Text("hi".into())],
            nonce: Some(nonce.into()),
        }
    }

    #[test]
    fn cancelled_commands_are_skipped_or_flagged() {
        let (ready_tx, ready) = std::sync::mpsc::channel();
        let (mut cx, server) = connect(move |mut socket| {
            command(&mut socket, send(1, "a"));
            command(&mut socket, send(2, "b"));
            command(&mut socket, Command::Cancel { request: 1 });
            ready_tx.send(()).unwrap();
            // The queued send never reached the platform, which comes
            // before its answer.
            assert_eq!(
                provider_message(&mut socket),
                ProviderMessage::Message(MessageEvent::NotSent {
                    channel: ChannelRef::new(None, "c"),
                    nonce: "a".into(),
                    error: CommandError::Cancelled,
                })
            );
            assert_eq!(
                provider_message(&mut socket),
                ProviderMessage::Reply {
                    request: 1,
                    result: Err(CommandError::Cancelled)
                }
            );
            // Cancelling one the provider started only flags it.
            command(&mut socket, Command::Cancel { request: 2 });
            ready_tx.send(()).unwrap();
            assert!(matches!(
                provider_message(&mut socket),
                ProviderMessage::Reply { request: 2, .. }
            ));
            while socket.read().is_ok() {}
        });
        ready.recv().unwrap();
        let next = cx.next_command(None).unwrap();
        assert!(
            matches!(next, Some(Command::Send { request: 2, .. })),
            "{next:?}"
        );
        assert!(!cx.is_cancelled(2).unwrap());
        ready.recv().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !cx.is_cancelled(2).unwrap() {
            assert!(Instant::now() < deadline, "the cancel never arrived");
            thread::sleep(Duration::from_millis(5));
        }
        // Done anyway, so it is answered as usual and forgotten.
        cx.reply(2, Ok(Reply::Sent { id: "1".into() })).unwrap();
        assert!(!cx.is_cancelled(2).unwrap());
        drop(cx);
        server.join().unwrap();
    }

    #[test]
    fn wait_wakes_for_commands_and_sources() {
        use std::io::Write;
        use std::os::fd::AsFd;

        let platform = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut remote = TcpStream::connect(platform.local_addr().unwrap()).unwrap();
        let (local, _) = platform.accept().unwrap();
        let (go_tx, go) = std::sync::mpsc::channel::<()>();
        let (mut cx, server) = connect(move |mut socket| {
            go.recv().unwrap();
            command(&mut socket, Command::Shutdown);
            while socket.read().is_ok() {}
        });

        let started = Instant::now();
        let wake = cx
            .wait(
                &[Source::read(local.as_fd())],
                Some(Duration::from_millis(50)),
            )
            .unwrap();
        assert_eq!(
            wake,
            Wake {
                command: false,
                sources: vec![false]
            }
        );
        assert!(started.elapsed() >= Duration::from_millis(50));

        remote.write_all(b"event\n").unwrap();
        let wake = cx.wait(&[Source::read(local.as_fd())], None).unwrap();
        assert_eq!(wake.sources, [true]);
        let wake = cx.wait(&[Source::write(local.as_fd())], None).unwrap();
        assert_eq!(wake.sources, [true]);

        go_tx.send(()).unwrap();
        let mut wake = cx.wait(&[], Some(Duration::from_secs(10))).unwrap();
        // A partly arrived frame can wake it early.
        while !wake.command {
            wake = cx.wait(&[], Some(Duration::from_secs(10))).unwrap();
        }
        assert_eq!(
            cx.next_command(Some(Duration::ZERO)).unwrap(),
            Some(Command::Shutdown)
        );
        drop(cx);
        server.join().unwrap();
    }

    #[test]
    fn skipped_send_functions_are_not_sent() {
        let channel = ChannelRef::new(None, "c");
        let call = |request, function: &str| Command::Call {
            request,
            call: FunctionCall {
                function: function.into(),
                context: FunctionContext::Channel(ChannelRef::new(None, "c")),
                arguments: [
                    (inputs::CONTENT.into(), Value::Content(vec![])),
                    (inputs::NONCE.into(), Value::Text(format!("n{request}"))),
                ]
                .into(),
            },
        };
        let (ready_tx, ready) = std::sync::mpsc::channel();
        let (mut cx, server) = connect(move |mut socket| {
            command(
                &mut socket,
                Command::Functions {
                    request: 1,
                    context: FunctionContext::Channel(channel.clone()),
                },
            );
            assert!(matches!(
                provider_message(&mut socket),
                ProviderMessage::Reply { request: 1, .. }
            ));
            // Only calls of a declared `SendMessage` are sends.
            command(&mut socket, call(2, "custom"));
            command(&mut socket, call(3, "send"));
            command(&mut socket, Command::Cancel { request: 2 });
            command(&mut socket, Command::Cancel { request: 3 });
            command(&mut socket, Command::Shutdown);
            ready_tx.send(()).unwrap();
            let skipped = |request| ProviderMessage::Reply {
                request,
                result: Err(CommandError::Cancelled),
            };
            assert_eq!(provider_message(&mut socket), skipped(2));
            assert_eq!(
                provider_message(&mut socket),
                ProviderMessage::Message(MessageEvent::NotSent {
                    channel,
                    nonce: "n3".into(),
                    error: CommandError::Cancelled,
                })
            );
            assert_eq!(provider_message(&mut socket), skipped(3));
            while socket.read().is_ok() {}
        });
        let Some(Command::Functions { request, .. }) = cx.next_command(None).unwrap() else {
            panic!("expected discovery");
        };
        let mut custom = Function::action("custom", "Custom", ActionKind::Custom("x".into()));
        custom.inputs = Function::action("send", "Send", ActionKind::SendMessage).inputs;
        let send = Function::action("send", "Send", ActionKind::SendMessage);
        cx.reply(request, Ok(Reply::Functions(vec![custom, send])))
            .unwrap();
        ready.recv().unwrap();
        // Both calls were skipped while queued.
        assert_eq!(cx.next_command(None).unwrap(), Some(Command::Shutdown));
        drop(cx);
        server.join().unwrap();
    }

    #[test]
    fn blob_reads_run_while_commands_are_answered() {
        let (mut cx, server) = connect(move |mut socket| {
            let read = |socket: &mut WebSocket<TcpStream>| match provider_message(socket) {
                ProviderMessage::ReadBlob { blob, offset, len } => (blob, offset, len),
                other => panic!("expected a read, got {other:?}"),
            };
            assert_eq!(read(&mut socket), ("b".into(), 0, 4));
            assert_eq!(read(&mut socket), ("b".into(), 4, 4));
            // A command arrives before the answers.
            command(&mut socket, Command::Shutdown);
            for (offset, bytes) in [(4, vec![5]), (0, vec![1, 2, 3, 4])] {
                let answer = HostMessage::Blob {
                    blob: "b".into(),
                    offset,
                    result: Ok(bytes),
                };
                let answer = types::encode(&answer).unwrap();
                socket.send(Frame::Binary(answer.into())).unwrap();
            }
            while socket.read().is_ok() {}
        });
        cx.start_read("b", 0, 4).unwrap();
        assert!(cx.start_read("b", 0, 4).is_err(), "already reading");
        cx.start_read("b", 4, 4).unwrap();
        assert_eq!(cx.take_read("b", 0).unwrap(), None);
        cx.forget_read("b", 4);
        assert_eq!(cx.next_command(None).unwrap(), Some(Command::Shutdown));
        let deadline = Instant::now() + Duration::from_secs(10);
        let bytes = loop {
            cx.wait(&[], Some(Duration::from_secs(10))).unwrap();
            if let Some(bytes) = cx.take_read("b", 0).unwrap() {
                break bytes.unwrap();
            }
            assert!(Instant::now() < deadline, "the answer never came");
        };
        assert_eq!(bytes, [1, 2, 3, 4]);
        // The forgotten one was dropped.
        assert!(cx.reads.is_empty());
        drop(cx);
        server.join().unwrap();
    }

    #[test]
    fn lookups_run_while_commands_are_answered() {
        let (lookups_tx, lookups) = std::sync::mpsc::channel();
        let (mut cx, server) = connect(move |mut socket| {
            let looked_up = |socket: &mut WebSocket<TcpStream>| match provider_message(socket) {
                ProviderMessage::Lookup { name } => name,
                other => panic!("expected a lookup, got {other:?}"),
            };
            assert_eq!(looked_up(&mut socket), "slow:1");
            assert_eq!(looked_up(&mut socket), "gone:1");
            assert_eq!(looked_up(&mut socket), "bad:1");
            // Commands come while the lookups are stuck.
            command(&mut socket, Command::Shutdown);
            lookups.recv().unwrap();
            let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
            for (name, result) in [
                ("gone:1", Ok(vec![addr])),
                ("bad:1", Err("unknown".into())),
                ("slow:1", Ok(vec![addr])),
            ] {
                let answer = HostMessage::Addresses {
                    name: name.into(),
                    result,
                };
                let answer = types::encode(&answer).unwrap();
                socket.send(Frame::Binary(answer.into())).unwrap();
            }
            while socket.read().is_ok() {}
        });
        cx.start_lookup("slow:1").unwrap();
        assert!(cx.start_lookup("slow:1").is_err(), "already looking up");
        cx.start_lookup("gone:1").unwrap();
        cx.start_lookup("bad:1").unwrap();
        cx.forget_lookup("gone:1");
        // Looking up again takes the answer of the one running.
        cx.forget_lookup("bad:1");
        cx.start_lookup("bad:1").unwrap();
        let wake = cx.wait(&[], Some(Duration::from_secs(10))).unwrap();
        assert!(wake.command);
        assert_eq!(cx.next_command(None).unwrap(), Some(Command::Shutdown));
        assert_eq!(cx.take_lookup("slow:1").unwrap(), None);
        lookups_tx.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut answers = Vec::new();
        while answers.len() < 2 {
            cx.wait(&[], Some(Duration::from_secs(10))).unwrap();
            for name in ["slow:1", "bad:1"] {
                if let Some(result) = cx.take_lookup(name).unwrap() {
                    answers.push((name, result));
                }
            }
            assert!(Instant::now() < deadline, "the answers never came");
        }
        answers.sort_by_key(|(name, _)| *name);
        let addr = "127.0.0.1:1".parse().unwrap();
        assert_eq!(
            answers,
            [("bad:1", Err("unknown".into())), ("slow:1", Ok(vec![addr]))]
        );
        // The forgotten one was dropped.
        assert!(cx.lookups.is_empty());
        drop(cx);
        server.join().unwrap();
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn connecting_is_refused_where_it_would_wait() {
        let error = start_connect("127.0.0.1:1".parse().unwrap()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn connecting_does_not_wait() {
        use std::os::fd::AsFd;
        let (mut cx, server) = connect(|mut socket| while socket.read().is_ok() {});
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stream = start_connect(addr).unwrap();
        let wake = cx
            .wait(
                &[Source::write(stream.as_fd())],
                Some(Duration::from_secs(10)),
            )
            .unwrap();
        assert_eq!(wake.sources, [true]);
        assert!(stream.take_error().unwrap().is_none());
        assert_eq!(stream.peer_addr().unwrap(), addr);
        // Refused once nothing listens.
        drop(listener);
        let stream = start_connect(addr).unwrap();
        cx.wait(
            &[Source::write(stream.as_fd())],
            Some(Duration::from_secs(10)),
        )
        .unwrap();
        assert!(stream.take_error().unwrap().is_some());
        drop(cx);
        server.join().unwrap();
    }

    #[test]
    fn long_ids_are_refused_before_sending() {
        assert!(check_id(&"é".repeat(types::MAX_ID)).is_err());
        assert!(check_id(&"a".repeat(types::MAX_ID)).is_ok());
    }
}
