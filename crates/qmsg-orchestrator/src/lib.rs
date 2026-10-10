//! Runs qmsg providers as `wasm32-wasip2` components.
//!
//! Each provider gets its own OS thread, which creates the provider's wasmtime
//! `Store` and loads its Wasm. Providers own their network connections and have
//! full TCP and UDP access.
//!
//! Providers talk to the orchestrator over a WebSocket. The orchestrator serves
//! it on localhost and gives each provider a URL with its own session token.
//!
//! Providers report the organizations, users and channels they are part of as
//! [`ProviderEvent::Directory`]; [`Directory`] keeps track of them.
//!
//! Commands go through [`ProviderHandle`], whose methods wait for the
//! provider's answer. Files too big for one message, in either direction, are
//! [blobs](Blob) read in pieces.
//!
//! Each provider gets one connection. A provider that sends an id or key
//! over [`MAX_ID`] bytes, or a message that doesn't decode, is disconnected.
//!
//! Providers' secrets live in a SQLite database in the data directory; see
//! [`Encryption`] for how they are protected.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::Duration;

use anyhow::{Context as _, bail};
use futures_util::stream::FuturesUnordered;
use futures_util::{SinkExt, StreamExt};
use qmsg_types::{
    ABI_VERSION, Arguments, ChannelRef, Command, CommandError, CompletionPage, CompletionRequest,
    Content, DirectoryUpdate, Function, FunctionCall, FunctionContext, FunctionKind, HistoryPage,
    HostMessage, InputIssue, LogLevel, MAX_BLOB_READS, MAX_FRAME, MAX_ID, MAX_LOOKUPS, MAX_READ,
    MediaSource, Message, MessageEvent, ProviderConfig, ProviderMessage, ProviderStatus, Reply,
    Value, Verification, VerificationRequest,
};
use serde::Deserialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message as Frame;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store, UpdateDeadline};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

mod blobs;
mod directory;
mod secrets;

use blobs::Blobs;
pub use blobs::{Blob, BlobSource, FileBlob};
pub use directory::{ApplyError, Directory, OrganizationEntry, Scope};
pub use secrets::Encryption;
use secrets::Secrets;

mod bindings {
    wasmtime::component::bindgen!({
        path: "../../wit",
        world: "provider",
        exports: { default: async },
    });
}

/// How often running providers are made to yield to the executor.
const EPOCH_TICK: Duration = Duration::from_millis(10);
/// Replies waiting to be written to a provider. A provider that keeps asking
/// without reading only stalls its own connection. Each can be a blob read of
/// up to `MAX_READ` bytes, so this bounds what a provider can make the
/// orchestrator hold.
const REPLY_QUEUE: usize = 4;
/// How long [`ProviderHandle`] requests wait for an answer by default.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// The most requests one provider can have unanswered at once, counting
/// ones no longer awaited until the provider answers or skips them. This
/// bounds the commands queued for a stalled provider, and the work a busy
/// one holds.
pub const MAX_REQUESTS: usize = 64;
/// The most bytes of encoded commands one provider can have unanswered at
/// once, counted like [`MAX_REQUESTS`]. A single command up to [`MAX_FRAME`]
/// always fits when none is unanswered.
pub const MAX_QUEUED: usize = 2 * MAX_FRAME;
/// The most blob reads and name lookups one orchestrator runs at once, for
/// all its providers. Each runs on a blocking thread, which can't be stopped,
/// so a stuck one keeps its place until it returns, even after the provider
/// that asked has gone. Reads and lookups over this fail at once.
pub const MAX_BLOCKING: usize = 16;
/// How long a provider waits for a blob read or name lookup before it fails.
/// The work itself keeps running, and counts against [`MAX_BLOCKING`], until
/// it returns.
pub const BLOCKING_TIMEOUT: Duration = Duration::from_secs(30);

/// Looks up the addresses of a host and port for providers; see
/// [`Orchestrator::set_resolver`].
pub type Resolver = Arc<dyn Fn(&str) -> io::Result<Vec<SocketAddr>> + Send + Sync>;

/// Runs work that can block, such as blob sources and name lookups, on
/// blocking threads, at most a fixed number at once.
struct Blocking {
    slots: Arc<Semaphore>,
    timeout: Duration,
}

impl Default for Blocking {
    fn default() -> Self {
        Self::new(MAX_BLOCKING, BLOCKING_TIMEOUT)
    }
}

impl Blocking {
    fn new(slots: usize, timeout: Duration) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(slots)),
            timeout,
        }
    }

    /// Runs `work`, failing at once when every slot is taken, or once it
    /// takes longer than the timeout. Its slot stays taken until it returns,
    /// even when no one waits for it any more.
    async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, String> {
        let Ok(slot) = self.slots.clone().try_acquire_owned() else {
            return Err("too many blob reads and lookups are running".into());
        };
        let task = tokio::task::spawn_blocking(move || {
            // Freed when the work returns or panics.
            let _slot = slot;
            work()
        });
        match tokio::time::timeout(self.timeout, task).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err("failed".into()),
            Err(_) => Err("timed out".into()),
        }
    }
}

/// A provider to run.
#[derive(Debug, Clone, Deserialize)]
pub struct ProviderSpec {
    pub name: String,
    pub wasm: PathBuf,
    #[serde(default)]
    pub settings: BTreeMap<String, String>,
}

/// Identifies one spawn, even when providers reuse a name or their events
/// come from different orchestrators in this process. Not a persistent id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProviderId {
    /// The configured name, also used for persistent secrets.
    pub name: String,
    /// Unique across spawns in this process.
    pub instance: u64,
}

// Process-wide so consumers can merge events from several orchestrators.
static NEXT_INSTANCE: AtomicU64 = AtomicU64::new(0);

/// Something a provider instance did. `Exited` is its last event. There is
/// no ordering between instances, including ones that reuse a name.
#[derive(Debug)]
pub enum ProviderEvent {
    Message {
        provider: ProviderId,
        event: MessageEvent,
    },
    /// A change to the organizations, users and channels the provider is part
    /// of.
    Directory {
        provider: ProviderId,
        update: DirectoryUpdate,
    },
    /// Where the provider is in connecting to its platform, such as needing
    /// a login.
    Status {
        provider: ProviderId,
        status: ProviderStatus,
    },
    Exited {
        provider: ProviderId,
        result: Result<(), String>,
    },
}

/// Sessions by token.
type Sessions = Arc<Mutex<HashMap<String, Arc<Session>>>>;

/// The orchestrator's side of one provider's WebSocket.
struct Session {
    id: ProviderId,
    /// Encoded `HostMessage::Command`s.
    commands: tokio::sync::Mutex<mpsc::UnboundedReceiver<Vec<u8>>>,
    /// For releasing blobs in answers no one waits for. Weak, so the
    /// commands end when the handle is dropped.
    release: mpsc::WeakUnboundedSender<Vec<u8>>,
    status: watch::Sender<ProviderStatus>,
    events: mpsc::Sender<ProviderEvent>,
    /// Weak, so dropping the orchestrator releases the data directory even
    /// while providers are still running.
    secrets: Weak<Secrets>,
    blobs: Weak<Blobs>,
    /// Identifies this provider's blobs.
    token: String,
    blocking: Arc<Blocking>,
    resolver: Resolver,
    requests: Arc<Requests>,
    /// Set once the provider is killed.
    killed: watch::Receiver<bool>,
    /// Open connections, counted from the handshake until every frame the
    /// provider sent has been handled. At most one.
    connections: watch::Sender<usize>,
    /// Set, under the sessions lock, once the provider has stopped running.
    /// No connection is accepted after.
    exiting: AtomicBool,
}

impl Session {
    /// Waits until the provider's connections have been read to the end.
    ///
    /// Only called once the provider's store is dropped, which closes its
    /// sockets, so every connection is already on its way to finishing.
    async fn drained(&self) {
        let mut connections = self.connections.subscribe();
        let _ = connections.wait_for(|&n| n == 0).await;
    }

    /// Sends an event, unless the provider is killed first: then its events
    /// are dropped, so a full channel can't keep it from exiting.
    async fn forward(&self, event: ProviderEvent) {
        let mut killed = self.killed.clone();
        tokio::select! {
            // Checked first, so nothing more is sent once it is set.
            biased;
            _ = killed.wait_for(|&k| k) => {}
            _ = self.events.send(event) => {}
        }
    }
}

/// Counts the provider's connection for as long as it is alive.
struct ConnectionGuard(Arc<Session>);

impl ConnectionGuard {
    /// Fails if the provider already has a connection, or has stopped
    /// running. Called under the sessions lock, which `exiting` is set under.
    fn new(session: Arc<Session>) -> Option<Self> {
        if session.exiting.load(Ordering::Relaxed) {
            return None;
        }
        let first = session.connections.send_if_modified(|n| {
            let first = *n == 0;
            if first {
                *n += 1;
            }
            first
        });
        // Only built when counted, since dropping one uncounts it.
        first.then(|| Self(session))
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.connections.send_modify(|n| *n -= 1);
    }
}

type Answer = Result<Reply, CommandError>;

/// Commands the provider hasn't answered yet, shared by the session and the
/// handle.
struct Requests {
    next: AtomicU64,
    pending: Mutex<HashMap<u64, Pending>>,
    /// [`MAX_QUEUED`], but smaller in tests.
    max_queued: usize,
}

impl Default for Requests {
    fn default() -> Self {
        Self {
            next: AtomicU64::default(),
            pending: Mutex::default(),
            max_queued: MAX_QUEUED,
        }
    }
}

/// A command the provider hasn't answered.
struct Pending {
    /// `None` once the requester gave up. The command still counts until the
    /// provider answers or skips it, since it may still be queued or running.
    waiter: Option<oneshot::Sender<Answer>>,
    /// Its encoded size, counted against [`MAX_QUEUED`].
    size: usize,
}

impl Pending {
    fn new(waiter: oneshot::Sender<Answer>) -> Self {
        Self {
            waiter: Some(waiter),
            size: 0,
        }
    }
}

/// Gives up on a request that wasn't answered, cancelling it in the provider.
struct PendingGuard<'a> {
    handle: &'a ProviderHandle,
    request: u64,
    /// Taken once the answer is read.
    answer: Option<oneshot::Receiver<Answer>>,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        let mut pending = self.handle.requests.pending.lock().unwrap();
        match pending.get_mut(&self.request) {
            Some(waiting) => {
                waiting.waiter = None;
                drop(pending);
                // Nothing to cancel once the provider has stopped.
                let _ = self.handle.queue(Command::Cancel {
                    request: self.request,
                });
            }
            None => {
                drop(pending);
                // Answered just as the requester gave up, so no one will
                // read the blobs in it.
                if let Some(mut answer) = self.answer.take()
                    && let Ok(Ok(reply)) = answer.try_recv()
                {
                    release_reply(&self.handle.commands.downgrade(), &reply);
                }
            }
        }
    }
}

/// Why a [`ProviderHandle`] request failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestError {
    /// The provider exited or was killed before answering. The command may
    /// have taken effect; this does not mean it is safe to retry.
    NotRunning,
    /// The command is over [`MAX_FRAME`] encoded. Send big files as a
    /// [`Blob`] instead.
    TooLarge { size: usize },
    /// The provider didn't answer in time. It may still carry out the
    /// command.
    TimedOut,
    /// The provider couldn't carry out the command.
    Failed(CommandError),
    /// The provider's answer doesn't fit the command or its declaration.
    BadReply(String),
    /// [`MAX_REQUESTS`] requests, or [`MAX_QUEUED`] bytes of them, are
    /// unanswered, including ones given up on that the provider hasn't
    /// answered or skipped yet. Nothing was sent.
    Busy,
    /// The request doesn't fit the function declaration it was given, such
    /// as a lookup passed to `verify`. Nothing was sent.
    WrongFunction(String),
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRunning => write!(f, "the provider is not running"),
            Self::TooLarge { size } => {
                write!(
                    f,
                    "the command is {size} bytes, over the {MAX_FRAME} byte limit"
                )
            }
            Self::TimedOut => write!(f, "the provider didn't answer in time"),
            Self::Failed(e) => write!(f, "{e}"),
            Self::BadReply(reason) => write!(f, "bad reply: {reason}"),
            Self::Busy => write!(f, "too many requests are waiting"),
            Self::WrongFunction(reason) => write!(f, "wrong function: {reason}"),
        }
    }
}

impl std::error::Error for RequestError {}

pub struct Orchestrator {
    engine: Engine,
    linker: Arc<Linker<WasiState>>,
    sessions: Sessions,
    secrets: Arc<Secrets>,
    blobs: Arc<Blobs>,
    blocking: Arc<Blocking>,
    resolver: Resolver,
    addr: SocketAddr,
    server: JoinHandle<()>,
}

impl Drop for Orchestrator {
    /// Stops the WebSocket server and closes every provider's connection.
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Orchestrator {
    /// Creates the engine and starts the WebSocket server on localhost.
    ///
    /// Secrets are kept in `data_dir`, which is created if needed.
    pub async fn new(data_dir: &Path, encryption: Encryption) -> anyhow::Result<Self> {
        let data_dir = data_dir.to_owned();
        // Opening may wait on the OS keychain.
        let secrets =
            tokio::task::spawn_blocking(move || Secrets::open(&data_dir, encryption)).await??;

        let mut config = Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config)?;

        let mut linker = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

        let weak = engine.weak();
        thread::spawn(move || {
            while let Some(engine) = weak.upgrade() {
                engine.increment_epoch();
                drop(engine);
                thread::sleep(EPOCH_TICK);
            }
        });

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let sessions = Sessions::default();
        let server = tokio::spawn(serve(listener, sessions.clone()));

        Ok(Self {
            engine,
            linker: Arc::new(linker),
            sessions,
            secrets: Arc::new(secrets),
            blobs: Arc::default(),
            blocking: Arc::default(),
            resolver: Arc::new(|name| Ok(name.to_socket_addrs()?.collect())),
            addr,
            server,
        })
    }

    /// Replaces how names that providers look up are resolved, such as to
    /// use a proxy, for providers spawned after. It runs on a blocking thread,
    /// counted against [`MAX_BLOCKING`] and given up on after
    /// [`BLOCKING_TIMEOUT`]. By default it asks the OS.
    pub fn set_resolver(
        &mut self,
        resolver: impl Fn(&str) -> io::Result<Vec<SocketAddr>> + Send + Sync + 'static,
    ) {
        self.resolver = Arc::new(resolver);
    }

    /// Starts a provider on its own thread.
    ///
    /// Events from the provider, including its exit, are sent to `events`.
    /// When `events` is full the provider's messages wait, which in turn
    /// slows the provider down. Drain events on a separate task from requests:
    /// answers can wait behind events. Keep request timeouts enabled to bound
    /// the wait if the event consumer stalls.
    ///
    /// Failing to load the Wasm is reported as an `Exited` event. Fails if a
    /// provider with the same name is still running or draining its connection.
    /// Once drained, its name is free even if `Exited` is waiting for room.
    /// Each spawn has a distinct [`ProviderId`]; a replacement never waits for
    /// an old instance's `Exited`.
    pub fn spawn(
        &self,
        spec: ProviderSpec,
        events: mpsc::Sender<ProviderEvent>,
    ) -> anyhow::Result<ProviderHandle> {
        let token = new_token()?;
        // Build before registering the session so failure needs no cleanup.
        let runtime = ProviderRuntime(Some(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
        ));
        let mut sessions = self.sessions.lock().unwrap();
        // Secrets are keyed by name. Hold the name until the old connection
        // has drained, including all of its secret operations.
        if sessions.values().any(|s| s.id.name == spec.name) {
            bail!("a provider named `{}` is already running", spec.name);
        }
        let instance = NEXT_INSTANCE
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| anyhow::anyhow!("provider instance ids exhausted"))?;
        let id = ProviderId {
            name: spec.name.clone(),
            instance,
        };
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let (kill, killed) = watch::channel(false);
        let (status, status_rx) = watch::channel(ProviderStatus::Connecting);
        let session = Arc::new(Session {
            id: id.clone(),
            commands: tokio::sync::Mutex::new(commands_rx),
            release: commands_tx.downgrade(),
            status,
            events: events.clone(),
            secrets: Arc::downgrade(&self.secrets),
            blobs: Arc::downgrade(&self.blobs),
            token: token.clone(),
            blocking: self.blocking.clone(),
            resolver: self.resolver.clone(),
            requests: Arc::default(),
            killed: killed.clone(),
            connections: watch::Sender::new(0),
            exiting: AtomicBool::new(false),
        });
        let handle = ProviderHandle {
            id: id.clone(),
            commands: commands_tx,
            requests: session.requests.clone(),
            blobs: Arc::downgrade(&self.blobs),
            token: token.clone(),
            timeout: Some(REQUEST_TIMEOUT),
            status: status_rx,
            kill,
        };
        sessions.insert(token.clone(), session.clone());
        drop(sessions);

        let engine = self.engine.clone();
        let linker = self.linker.clone();
        let sessions = self.sessions.clone();
        let host_url = format!("ws://{}/{token}", self.addr);
        let session_token = token.clone();
        let mut killed = killed;

        let spawned = thread::Builder::new()
            .name(format!("provider-{}", spec.name))
            .spawn(move || {
                runtime.block_on(async {
                    // Losing the race drops the provider's store, which
                    // closes its sockets.
                    let result = tokio::select! {
                        result = run(engine, &linker, spec, host_url) => result,
                        _ = killed.wait_for(|&k| k) => Err(anyhow::anyhow!("killed")),
                    };
                    // Refuse late connections, then handle what the provider
                    // sent before reporting the exit, so `Exited` is always
                    // its last event.
                    {
                        let _sessions = sessions.lock().unwrap();
                        session.exiting.store(true, Ordering::Relaxed);
                    }
                    session.drained().await;
                    // Fail new requests, then the ones still waiting.
                    session.commands.lock().await.close();
                    session.requests.pending.lock().unwrap().clear();
                    // Free the name before waiting for channel space. Only
                    // this instance's Exited remains; no secret work can run.
                    sessions.lock().unwrap().remove(&token);
                    let _ = events
                        .send(ProviderEvent::Exited {
                            provider: id,
                            result: result.map_err(|e| format!("{e:#}")),
                        })
                        .await;
                })
            });
        if let Err(e) = spawned {
            // The thread never started, so nothing else will remove the session.
            self.sessions.lock().unwrap().remove(&session_token);
            return Err(e.into());
        }
        Ok(handle)
    }
}

/// The runtime a provider's thread runs on.
struct ProviderRuntime(Option<tokio::runtime::Runtime>);

impl ProviderRuntime {
    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.0
            .as_ref()
            .expect("only taken on drop")
            .block_on(future)
    }
}

impl Drop for ProviderRuntime {
    /// A runtime can't be dropped normally in async code, which is where
    /// `spawn` drops it when it fails.
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

/// Controls a running provider. Dropping it kills the provider.
///
/// Requests wait for the provider's answer, for [`REQUEST_TIMEOUT`] by
/// default. A request that is dropped or times out is cancelled: the
/// provider skips it if it hasn't started it. One already started can still
/// be carried out, such as a message being sent, so delivery is unknown. Send
/// with a nonce to learn the outcome from later events. Either way it counts
/// against [`MAX_REQUESTS`] until the provider answers or skips it.
pub struct ProviderHandle {
    id: ProviderId,
    commands: mpsc::UnboundedSender<Vec<u8>>,
    requests: Arc<Requests>,
    blobs: Weak<Blobs>,
    /// The session's token, which owns its blobs.
    token: String,
    timeout: Option<Duration>,
    status: watch::Receiver<ProviderStatus>,
    kill: watch::Sender<bool>,
}

impl ProviderHandle {
    /// This spawn's identity, also carried by each of its events.
    pub fn id(&self) -> &ProviderId {
        &self.id
    }

    /// How long requests wait for an answer, or `None` to wait until the
    /// provider exits.
    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
    }

    /// The status the provider reported last, or `Connecting` before any.
    /// It can be ahead of the events read so far.
    pub fn status(&self) -> ProviderStatus {
        self.status.borrow().clone()
    }

    /// Discovers the functions available now in a context, including helpers
    /// linked from inputs. An empty list means none are available.
    ///
    /// Fails with [`RequestError::BadReply`] unless the declarations pass
    /// [`check_declarations`](qmsg_types::check_declarations), so standard
    /// actions can be trusted to follow their contract.
    pub async fn functions(&self, context: FunctionContext) -> Result<Vec<Function>, RequestError> {
        let reply = self
            .request(|request| Command::Functions {
                request,
                context: context.clone(),
            })
            .await?;
        match reply {
            Reply::Functions(functions) => {
                qmsg_types::check_declarations(&context, &functions)
                    .map_err(RequestError::BadReply)?;
                Ok(functions)
            }
            other => Err(self.bad_reply(other)),
        }
    }

    /// Calls a declared action or lookup.
    ///
    /// A standard action called in a context its contract doesn't allow is
    /// a [`RequestError::WrongFunction`]. The arguments are checked against
    /// the declaration first, and a
    /// failing check returns its issues as
    /// [`CommandError::InvalidInput`] without asking the provider. Providers
    /// still recheck inputs and permissions. A result of the wrong type is a
    /// [`RequestError::BadReply`]. Events come before the answer, as with
    /// `send_message`.
    pub async fn call_function(
        &self,
        function: &Function,
        context: FunctionContext,
        arguments: Arguments,
    ) -> Result<Value, RequestError> {
        if !matches!(
            function.kind,
            FunctionKind::Action { .. } | FunctionKind::Lookup(_)
        ) {
            return Err(RequestError::WrongFunction(format!(
                "`{}` is not an action or lookup",
                function.id
            )));
        }
        // Such as a message action called in a channel.
        function
            .check_contract(&context)
            .map_err(RequestError::WrongFunction)?;
        function
            .check(&arguments)
            .map_err(|issues| RequestError::Failed(CommandError::InvalidInput(issues)))?;
        let call = FunctionCall {
            function: function.id.clone(),
            context,
            arguments,
        };
        match self
            .request(|request| Command::Call { request, call })
            .await?
        {
            Reply::Value(value) if function.value_type().accepts(&value) => Ok(value),
            Reply::Value(value) => Err(self.bad_reply(Reply::Value(value))),
            other => Err(self.bad_reply(other)),
        }
    }

    /// Runs a verification function, either linked to an input or standalone.
    /// An invalid value is a successful `Verification::Invalid` result, while
    /// failure to check it is a request error. Checking does not run an action.
    ///
    /// A value of the wrong type for `helper` is invalid without asking the
    /// provider.
    pub async fn verify(
        &self,
        helper: &Function,
        verification: VerificationRequest,
    ) -> Result<Verification, RequestError> {
        let FunctionKind::Verification(value_type) = &helper.kind else {
            return Err(RequestError::WrongFunction(format!(
                "`{}` is not a verification",
                helper.id
            )));
        };
        check_helper(helper, &verification.call)?;
        if !value_type.accepts(&verification.value) {
            return Ok(Verification::Invalid(vec![InputIssue {
                input: verification.input.map(|i| i.input),
                code: "wrong_type".into(),
                message: format!("Expected {value_type:?}"),
            }]));
        }
        match self
            .request(|request| Command::Verify {
                request,
                verification: Box::new(verification),
            })
            .await?
        {
            Reply::Verified(Verification::Invalid(issues)) if issues.is_empty() => Err(
                RequestError::BadReply("invalid verification has no issues".into()),
            ),
            Reply::Verified(result) => Ok(result),
            other => Err(self.bad_reply(other)),
        }
    }

    /// Runs an autocomplete function. Labels are for display; use the item's
    /// value as the input. This also works outside actions. No action runs.
    /// Items of the wrong type for `helper` are a [`RequestError::BadReply`].
    pub async fn complete(
        &self,
        helper: &Function,
        completion: CompletionRequest,
    ) -> Result<CompletionPage, RequestError> {
        let FunctionKind::Completion(value_type) = &helper.kind else {
            return Err(RequestError::WrongFunction(format!(
                "`{}` is not a completion",
                helper.id
            )));
        };
        check_helper(helper, &completion.call)?;
        let limit = completion.limit;
        match self
            .request(|request| Command::Complete {
                request,
                completion: Box::new(completion),
            })
            .await?
        {
            Reply::Completed(page) if page.items.len() as u64 > u64::from(limit) => {
                self.bad_reply(Reply::Completed(page));
                Err(RequestError::BadReply(
                    "completion exceeds requested limit".into(),
                ))
            }
            Reply::Completed(page) if page.items.iter().any(|i| !value_type.accepts(&i.value)) => {
                Err(self.bad_reply(Reply::Completed(page)))
            }
            Reply::Completed(page) => Ok(page),
            other => Err(self.bad_reply(other)),
        }
    }

    /// Reads a page of a channel's messages, oldest first, starting with the
    /// newest page. Pass the page's `next_cursor` for older ones.
    ///
    /// History can overlap messages already received as events; keep
    /// messages by channel and id so repeats replace each other.
    pub async fn history(
        &self,
        channel: ChannelRef,
        cursor: Option<String>,
        limit: u32,
    ) -> Result<HistoryPage, RequestError> {
        let reply = self
            .request(|request| Command::History {
                request,
                channel: channel.clone(),
                cursor,
                limit,
            })
            .await?;
        match reply {
            Reply::History(page)
                if page.messages.len() as u64 <= u64::from(limit)
                    && page.messages.iter().all(|m| m.channel == channel) =>
            {
                Ok(page)
            }
            other => Err(self.bad_reply(other)),
        }
    }

    /// Reads one message, such as one an edit or a reply refers to.
    pub async fn get_message(
        &self,
        channel: ChannelRef,
        id: String,
    ) -> Result<Message, RequestError> {
        let reply = self
            .request(|request| Command::GetMessage {
                request,
                channel: channel.clone(),
                id: id.clone(),
            })
            .await?;
        match reply {
            Reply::Message(message) if message.channel == channel && message.id == id => {
                Ok(message)
            }
            other => Err(self.bad_reply(other)),
        }
    }

    /// Offers `source` to this provider, to send as a [`Blob::media`]. It can
    /// read it until the returned [`Blob`] is dropped.
    ///
    /// To send the same file to several providers, add an `Arc` of it to
    /// each.
    pub fn add_blob(&self, source: impl BlobSource) -> anyhow::Result<Blob> {
        let blobs = self
            .blobs
            .upgrade()
            .context("the orchestrator has shut down")?;
        Ok(Blob::new(&blobs, new_token()?, self.token.clone(), source))
    }

    /// Sends a message, returning its id.
    ///
    /// Check the content with [`Channel::check`](qmsg_types::Channel::check)
    /// first to learn of the channel's limits without asking the provider.
    ///
    /// When the answer doesn't come, such as on a timeout, a `nonce` unique
    /// to this send settles it later: a [`Message`] with the nonce means it
    /// was sent, a [`MessageEvent::NotSent`] that it never reached the
    /// platform. Without either, delivery stays unknown.
    pub async fn send_message(
        &self,
        channel: ChannelRef,
        reply_to: Option<String>,
        content: Vec<Content>,
        nonce: Option<String>,
    ) -> Result<String, RequestError> {
        let reply = self
            .request(|request| Command::Send {
                request,
                channel,
                reply_to,
                content,
                nonce,
            })
            .await?;
        match reply {
            Reply::Sent { id } => Ok(id),
            other => Err(self.bad_reply(other)),
        }
    }

    /// Opens a conversation with `members`, or finds the one already open.
    ///
    /// The provider reports the channel first, so its
    /// [`ProviderEvent::Directory`] is queued before this returns, though the
    /// task receiving events may not have handled it yet.
    pub async fn open_channel(
        &self,
        organization: Option<String>,
        members: Vec<String>,
    ) -> Result<ChannelRef, RequestError> {
        let reply = self
            .request(|request| Command::OpenChannel {
                request,
                organization,
                members,
            })
            .await?;
        match reply {
            Reply::Opened(channel) => Ok(channel),
            other => Err(self.bad_reply(other)),
        }
    }

    /// Reads up to `len` bytes, at most [`MAX_READ`](qmsg_types::MAX_READ),
    /// of a blob the provider sent, starting at `offset`. Shorter than `len`
    /// only at the end of the blob.
    pub async fn read_blob(
        &self,
        blob: &str,
        offset: u64,
        len: u32,
    ) -> Result<Vec<u8>, RequestError> {
        let reply = self
            .request(|request| Command::ReadBlob {
                request,
                blob: blob.to_owned(),
                offset,
                len,
            })
            .await?;
        match reply {
            Reply::Blob(bytes) if bytes.len() <= len.min(MAX_READ) as usize => Ok(bytes),
            Reply::Blob(bytes) => Err(RequestError::BadReply(format!(
                "{} bytes for a read of {len}",
                bytes.len()
            ))),
            other => Err(self.bad_reply(other)),
        }
    }

    /// Tells the provider the orchestrator is done with a blob it sent.
    pub fn release_blob(&self, blob: &str) -> Result<(), RequestError> {
        self.queue(Command::ReleaseBlob {
            blob: blob.to_owned(),
        })
    }

    /// Asks the provider to stop. It exits when it is done.
    pub fn shutdown(&self) -> Result<(), RequestError> {
        self.queue(Command::Shutdown)
    }

    async fn request(&self, command: impl FnOnce(u64) -> Command) -> Result<Reply, RequestError> {
        let request = self.requests.next.fetch_add(1, Ordering::Relaxed);
        let bytes = encode_command(command(request))?;
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.requests.pending.lock().unwrap();
            let queued: usize = pending.values().map(|p| p.size).sum();
            if pending.len() >= MAX_REQUESTS
                || (!pending.is_empty() && queued + bytes.len() > self.requests.max_queued)
            {
                return Err(RequestError::Busy);
            }
            let size = bytes.len();
            self.commands
                .send(bytes)
                .map_err(|_| RequestError::NotRunning)?;
            // Under the lock, so the answer can't be handled before this.
            pending.insert(
                request,
                Pending {
                    size,
                    ..Pending::new(tx)
                },
            );
        }
        let mut guard = PendingGuard {
            handle: self,
            request,
            answer: Some(rx),
        };
        let rx = guard.answer.as_mut().expect("just set");
        let answer = match self.timeout {
            Some(timeout) => tokio::time::timeout(timeout, rx)
                .await
                .map_err(|_| RequestError::TimedOut)?,
            None => rx.await,
        };
        guard.answer = None;
        answer
            .map_err(|_| RequestError::NotRunning)?
            .map_err(RequestError::Failed)
    }

    /// Queues a command no one waits on. These don't count against
    /// [`MAX_REQUESTS`], so cancelling, releasing and stopping always work.
    fn queue(&self, command: Command) -> Result<(), RequestError> {
        self.commands
            .send(encode_command(command)?)
            .map_err(|_| RequestError::NotRunning)
    }

    /// Refuses a reply, releasing the provider's blobs in it, since no one
    /// will read them.
    fn bad_reply(&self, reply: Reply) -> RequestError {
        release_reply(&self.commands.downgrade(), &reply);
        bad_reply(reply)
    }

    /// Stops the provider immediately, even if it is blocked on I/O.
    ///
    /// Its `Exited` event fails with `killed`. Events it sent that are still
    /// waiting for room in the channel are dropped. Pending requests fail
    /// with [`RequestError::NotRunning`]; their delivery status is unknown.
    /// Already queued events remain. `Exited` may wait for room, but that
    /// cannot hold the name once the connection has drained.
    pub fn kill(&self) {
        self.kill.send_replace(true);
    }
}

impl Drop for ProviderHandle {
    /// Without a handle nothing can send the provider commands or stop it.
    fn drop(&mut self) {
        self.kill();
    }
}

async fn run(
    engine: Engine,
    linker: &Linker<WasiState>,
    spec: ProviderSpec,
    host_url: String,
) -> anyhow::Result<()> {
    let component = Component::from_file(&engine, &spec.wasm)
        .map_err(anyhow::Error::from)
        .with_context(|| format!("loading {}", spec.wasm.display()))?;
    let config = ProviderConfig {
        name: spec.name,
        host_url,
        settings: spec.settings,
    };

    let state = WasiState {
        wasi: wasi_ctx(),
        table: ResourceTable::new(),
    };
    let mut store = Store::new(&engine, state);
    store.set_epoch_deadline(1);
    store.epoch_deadline_callback(|_| Ok(UpdateDeadline::Yield(1)));

    let provider = bindings::Provider::instantiate_async(&mut store, &component, linker).await?;
    let version = provider.call_abi_version(&mut store).await?;
    if version != ABI_VERSION {
        bail!("provider uses ABI version {version}, expected {ABI_VERSION}");
    }

    let config = qmsg_types::encode(&config)?;
    provider
        .call_run(&mut store, &config)
        .await?
        .map_err(|e| anyhow::anyhow!(e))
}

struct WasiState {
    wasi: WasiCtx,
    table: ResourceTable,
}

impl WasiView for WasiState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

fn wasi_ctx() -> WasiCtx {
    WasiCtx::builder()
        .inherit_stderr()
        .inherit_network()
        .allow_tcp(true)
        .allow_udp(true)
        .allow_ip_name_lookup(true)
        .build()
}

fn bad_reply(reply: Reply) -> RequestError {
    let kind = match reply {
        Reply::Functions(_) => "Functions",
        Reply::Value(_) => "Value",
        Reply::Verified(_) => "Verified",
        Reply::Completed(_) => "Completed",
        Reply::Sent { .. } => "Sent",
        Reply::Opened(_) => "Opened",
        Reply::History(_) => "History",
        Reply::Message(_) => "Message",
        Reply::Blob(_) => "Blob",
    };
    RequestError::BadReply(format!("unexpected {kind}"))
}

fn encode_command(command: Command) -> Result<Vec<u8>, RequestError> {
    let bytes = qmsg_types::encode(&HostMessage::Command(command)).expect("commands always encode");
    match bytes.len() {
        size if size > MAX_FRAME => Err(RequestError::TooLarge { size }),
        _ => Ok(bytes),
    }
}

/// Releases the provider's blobs in a reply no one will read.
///
/// Each blob id in a reply or event belongs to it alone (see
/// [`MediaSource::Blob`]), so this never takes a file from another reader.
fn release_reply(commands: &mpsc::WeakUnboundedSender<Vec<u8>>, reply: &Reply) {
    let Some(commands) = commands.upgrade() else {
        return;
    };
    for blob in reply_blobs(reply) {
        if let Ok(bytes) = encode_command(Command::ReleaseBlob { blob }) {
            let _ = commands.send(bytes);
        }
    }
}

fn check_helper(helper: &Function, call: &FunctionCall) -> Result<(), RequestError> {
    if call.function == helper.id {
        Ok(())
    } else {
        Err(RequestError::WrongFunction(format!(
            "the request is for `{}`, not `{}`",
            call.function, helper.id
        )))
    }
}

/// The provider's blobs in a reply.
fn reply_blobs(reply: &Reply) -> Vec<String> {
    fn content(content: &[Content], blobs: &mut Vec<String>) {
        for part in content {
            if let Content::Image(m) | Content::Video(m) | Content::Audio(m) | Content::File(m) =
                part
                && let MediaSource::Blob(id) = &m.source
            {
                blobs.push(id.clone());
            }
        }
    }
    fn value(v: &Value, blobs: &mut Vec<String>) {
        match v {
            Value::Content(c) => content(c, blobs),
            Value::Message(m) => content(&m.content, blobs),
            Value::List(items) => items.iter().for_each(|v| value(v, blobs)),
            Value::Record(fields) => fields.values().for_each(|v| value(v, blobs)),
            _ => {}
        }
    }
    let mut blobs = Vec::new();
    match reply {
        Reply::Value(v) => value(v, &mut blobs),
        Reply::Completed(page) => page.items.iter().for_each(|i| value(&i.value, &mut blobs)),
        Reply::History(page) => page
            .messages
            .iter()
            .for_each(|m| content(&m.content, &mut blobs)),
        Reply::Message(m) => content(&m.content, &mut blobs),
        _ => {}
    }
    blobs
}

fn new_token() -> anyhow::Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| anyhow::anyhow!("generating session token: {e}"))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

async fn serve(listener: TcpListener, sessions: Sessions) {
    // Owning the connection tasks means aborting `serve` also closes them.
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    connections.spawn(connection(stream, sessions.clone()));
                }
                Err(e) => tracing::warn!("accepting provider connection: {e}"),
            },
            Some(_) = connections.join_next() => {}
        }
    }
}

/// Serves one provider's WebSocket until the provider closes it.
// tungstenite's handshake callback fixes the error type.
#[allow(clippy::result_large_err)]
async fn connection(stream: TcpStream, sessions: Sessions) {
    let mut guard = None;
    let limits = WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME))
        .max_frame_size(Some(MAX_FRAME));
    let callback = |request: &Request, response| {
        let token = request.uri().path().trim_start_matches('/');
        // Counted before the handshake completes, so the provider can't
        // finish and be reported as exited before this connection counts.
        guard = sessions
            .lock()
            .unwrap()
            .get(token)
            .cloned()
            .and_then(ConnectionGuard::new);
        match guard {
            Some(_) => Ok(response),
            None => Err(not_found()),
        }
    };
    let accept = tokio_tungstenite::accept_hdr_async_with_config(stream, callback, Some(limits));
    let socket = match accept.await {
        Ok(socket) => socket,
        Err(e) => {
            tracing::debug!("rejected provider connection: {e}");
            return;
        }
    };
    let Some(guard) = guard else { return };
    let session = &guard.0;

    // Reading and writing run independently, so a provider that is busy
    // writing a large message can't deadlock against a large command.
    let (mut sink, mut stream) = socket.split();
    let (replies_tx, mut replies) = mpsc::channel(REPLY_QUEUE);

    let reader = async move {
        // Blob reads run beside the rest, so a slow source doesn't hold up
        // the provider's answers and events.
        let mut reads = FuturesUnordered::new();
        let mut lookups = FuturesUnordered::new();
        loop {
            let frame = tokio::select! {
                Some(reply) = reads.next() => {
                    if replies_tx.send(reply).await.is_err() {
                        break;
                    }
                    continue;
                }
                Some(reply) = lookups.next() => {
                    if replies_tx.send(reply).await.is_err() {
                        break;
                    }
                    continue;
                }
                frame = stream.next() => match frame {
                    Some(frame) => frame,
                    None => break,
                },
            };
            match frame {
                Ok(Frame::Binary(bytes)) => match qmsg_types::decode(&bytes) {
                    Ok(message) if has_long_id(&message) => {
                        tracing::warn!(provider = session.id.name, "id over {MAX_ID} bytes");
                        break;
                    }
                    Ok(ProviderMessage::ReadBlob { blob, offset, len }) => {
                        if reads.len() < MAX_BLOB_READS {
                            reads.push(session.read_blob(blob, offset, len));
                            continue;
                        }
                        // Refused without reading, so stalled sources can't
                        // pile up or keep the connection from being read.
                        let reply = HostMessage::Blob {
                            blob,
                            offset,
                            result: Err(format!("over {MAX_BLOB_READS} blob reads at once")),
                        };
                        if replies_tx.send(reply).await.is_err() {
                            break;
                        }
                    }
                    Ok(ProviderMessage::Lookup { name }) => {
                        if lookups.len() < MAX_LOOKUPS {
                            lookups.push(session.lookup(name));
                            continue;
                        }
                        let reply = HostMessage::Addresses {
                            name,
                            result: Err(format!("over {MAX_LOOKUPS} lookups at once")),
                        };
                        if replies_tx.send(reply).await.is_err() {
                            break;
                        }
                    }
                    Ok(message) => {
                        if let Some(reply) = session.handle(message).await
                            && replies_tx.send(reply).await.is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(provider = session.id.name, "invalid message: {e}");
                        break;
                    }
                },
                Ok(Frame::Close(_)) => break,
                Ok(_) => {}
                // The SDK refuses to send these, so the provider bypassed it.
                Err(e @ WsError::Capacity(_)) => {
                    tracing::warn!(provider = session.id.name, "connection error: {e}");
                    break;
                }
                Err(e) => {
                    tracing::debug!(provider = session.id.name, "connection error: {e}");
                    break;
                }
            }
        }
        // Dropping `replies_tx` here ends the writer.
    };

    let writer = async {
        // Held for the connection's lifetime, so a second connection for the
        // same provider gets no commands until the first one closes.
        let mut commands = session.commands.lock().await;
        let mut commands_open = true;
        loop {
            let bytes = tokio::select! {
                command = commands.recv(), if commands_open => match command {
                    Some(bytes) => bytes,
                    None => {
                        commands_open = false;
                        continue;
                    }
                },
                // Replies are small: secrets, blob reads and the ids they
                // repeat are all limited far below `MAX_FRAME`.
                reply = replies.recv() => match reply {
                    Some(reply) => qmsg_types::encode(&reply).expect("host messages always encode"),
                    None => break,
                },
            };
            if sink.send(Frame::Binary(bytes.into())).await.is_err() {
                break;
            }
        }
    };

    tokio::join!(reader, writer);
}

impl Session {
    /// Handles a message from the provider, returning the reply if it needs one.
    async fn handle(&self, message: ProviderMessage) -> Option<HostMessage> {
        match message {
            ProviderMessage::Log { level, message } => {
                let provider = &self.id.name;
                match level {
                    LogLevel::Error => tracing::error!(provider, "{message}"),
                    LogLevel::Warn => tracing::warn!(provider, "{message}"),
                    LogLevel::Info => tracing::info!(provider, "{message}"),
                    LogLevel::Debug => tracing::debug!(provider, "{message}"),
                    LogLevel::Trace => tracing::trace!(provider, "{message}"),
                }
                None
            }
            ProviderMessage::Message(event) => {
                self.forward(ProviderEvent::Message {
                    provider: self.id.clone(),
                    event,
                })
                .await;
                None
            }
            ProviderMessage::Status(status) => {
                self.status.send_replace(status.clone());
                self.forward(ProviderEvent::Status {
                    provider: self.id.clone(),
                    status,
                })
                .await;
                None
            }
            ProviderMessage::Reply { request, result } => {
                // A kill may have discarded events before this answer. Do not
                // report a terminal answer that would imply they were queued.
                if *self.killed.borrow() {
                    return None;
                }
                let pending = self.requests.pending.lock().unwrap().remove(&request);
                // The requester may have stopped waiting, even just now.
                let unread = match pending.and_then(|p| p.waiter) {
                    Some(tx) => tx.send(result).err(),
                    None => {
                        tracing::debug!(provider = self.id.name, request, "unawaited reply");
                        Some(result)
                    }
                };
                // No one will read or release its blobs.
                if let Some(Ok(reply)) = unread {
                    release_reply(&self.release, &reply);
                }
                None
            }
            ProviderMessage::ReadBlob { blob, offset, len } => {
                Some(self.read_blob(blob, offset, len).await)
            }
            ProviderMessage::Lookup { name } => Some(self.lookup(name).await),
            ProviderMessage::Directory(update) => {
                self.forward(ProviderEvent::Directory {
                    provider: self.id.clone(),
                    update,
                })
                .await;
                None
            }
            ProviderMessage::SecretGet { key } => {
                let result = self.secret(&key, |s, name, key| s.get(name, key)).await;
                Some(HostMessage::Secret { key, result })
            }
            ProviderMessage::SecretSet { key, value } => {
                let result = self
                    .secret(&key, move |s, name, key| s.set(name, key, &value))
                    .await;
                Some(HostMessage::SecretStored { key, result })
            }
            ProviderMessage::SecretDelete { key } => {
                let result = self.secret(&key, |s, name, key| s.delete(name, key)).await;
                Some(HostMessage::SecretStored { key, result })
            }
        }
    }

    async fn read_blob(&self, blob: String, offset: u64, len: u32) -> HostMessage {
        // A slow source mustn't keep a killed provider from exiting.
        let mut killed = self.killed.clone();
        let result = tokio::select! {
            // Checked first, so no read starts once it is set. A read
            // already running finishes on its own, after the provider.
            biased;
            _ = killed.wait_for(|&k| k) => Err("killed".into()),
            result = blobs::read(&self.blobs, &self.blocking, &self.token, &blob, offset, len) => result,
        };
        HostMessage::Blob {
            blob,
            offset,
            result,
        }
    }

    async fn lookup(&self, name: String) -> HostMessage {
        let resolver = self.resolver.clone();
        let host = name.clone();
        let result = match self.blocking.run(move || resolver(&host)).await {
            Ok(result) => result.map_err(|e| e.to_string()),
            Err(e) => Err(format!("looking up: {e}")),
        };
        HostMessage::Addresses { name, result }
    }

    /// Runs a secret storage operation off the async threads, since SQLite
    /// blocks.
    async fn secret<T: Send + 'static>(
        &self,
        key: &str,
        op: impl FnOnce(&Secrets, &str, &str) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        let secrets = self
            .secrets
            .upgrade()
            .ok_or("the orchestrator has shut down")?;
        let name = self.id.name.clone();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || op(&secrets, &name, &key))
            .await
            .expect("secret storage panicked")
    }
}

/// Whether the message has an id or key the orchestrator would repeat back
/// that is over [`MAX_ID`] bytes.
fn has_long_id(message: &ProviderMessage) -> bool {
    let id = match message {
        ProviderMessage::SecretGet { key }
        | ProviderMessage::SecretSet { key, .. }
        | ProviderMessage::SecretDelete { key } => key,
        ProviderMessage::ReadBlob { blob, .. } => blob,
        ProviderMessage::Lookup { name } => name,
        _ => return false,
    };
    id.len() > MAX_ID
}

fn not_found() -> ErrorResponse {
    let mut response = ErrorResponse::new(None);
    *response.status_mut() = StatusCode::NOT_FOUND;
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> Arc<Session> {
        session_with_events(mpsc::channel(1).0).0
    }

    /// Also returns the session's kill switch, whose dropping kills it, and
    /// its command sender, which the handle would hold.
    fn session_with_events(
        events: mpsc::Sender<ProviderEvent>,
    ) -> (
        Arc<Session>,
        watch::Sender<bool>,
        mpsc::UnboundedSender<Vec<u8>>,
    ) {
        session_with(events, Weak::new())
    }

    fn session_with(
        events: mpsc::Sender<ProviderEvent>,
        blobs: Weak<Blobs>,
    ) -> (
        Arc<Session>,
        watch::Sender<bool>,
        mpsc::UnboundedSender<Vec<u8>>,
    ) {
        let resolver = Arc::new(|_: &str| Err(io::ErrorKind::NotFound.into()));
        session_on(events, blobs, Arc::default(), resolver)
    }

    /// A session sharing `blocking`, which looks names up with `resolver`.
    fn session_on(
        events: mpsc::Sender<ProviderEvent>,
        blobs: Weak<Blobs>,
        blocking: Arc<Blocking>,
        resolver: Resolver,
    ) -> (
        Arc<Session>,
        watch::Sender<bool>,
        mpsc::UnboundedSender<Vec<u8>>,
    ) {
        let (commands_tx, commands) = mpsc::unbounded_channel();
        let (kill, killed) = watch::channel(false);
        let session = Arc::new(Session {
            id: ProviderId {
                name: "p".into(),
                instance: 0,
            },
            commands: tokio::sync::Mutex::new(commands),
            release: commands_tx.downgrade(),
            status: watch::Sender::new(ProviderStatus::Connecting),
            events,
            secrets: Weak::new(),
            blobs,
            token: "token".into(),
            blocking,
            resolver,
            requests: Arc::default(),
            killed,
            connections: watch::Sender::new(0),
            exiting: AtomicBool::new(false),
        });
        (session, kill, commands_tx)
    }

    /// A handle with no provider behind it, and what it queues.
    fn handle() -> (ProviderHandle, mpsc::UnboundedReceiver<Vec<u8>>) {
        let (commands, queued) = mpsc::unbounded_channel();
        (handle_with(commands, Arc::default()), queued)
    }

    fn handle_with(
        commands: mpsc::UnboundedSender<Vec<u8>>,
        requests: Arc<Requests>,
    ) -> ProviderHandle {
        ProviderHandle {
            id: ProviderId {
                name: "p".into(),
                instance: 0,
            },
            commands,
            requests,
            blobs: Weak::new(),
            token: "token".into(),
            timeout: None,
            status: watch::channel(ProviderStatus::Connecting).1,
            kill: watch::channel(false).0,
        }
    }

    fn file(blob: &str) -> Content {
        Content::File(qmsg_types::Media {
            name: None,
            mime: None,
            size: None,
            source: MediaSource::Blob(blob.into()),
        })
    }

    #[test]
    fn a_provider_gets_one_connection() {
        let session = session();
        let first = ConnectionGuard::new(session.clone()).unwrap();
        // Refusing doesn't uncount the open connection.
        for _ in 0..3 {
            assert!(ConnectionGuard::new(session.clone()).is_none());
        }
        assert_eq!(*session.connections.borrow(), 1);
        drop(first);
        assert_eq!(*session.connections.borrow(), 0);

        session.exiting.store(true, Ordering::Relaxed);
        assert!(ConnectionGuard::new(session.clone()).is_none());
    }

    #[tokio::test]
    async fn a_kill_cannot_deliver_an_answer_after_discarding_events() {
        let (events, mut received) = mpsc::channel(1);
        let (session, kill, _commands) = session_with_events(events);
        let update = || {
            ProviderMessage::Directory(DirectoryUpdate::Me {
                organization: None,
                id: Some("me".into()),
            })
        };
        session.handle(update()).await;
        let blocked = session.handle(update());
        tokio::pin!(blocked);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut blocked)
                .await
                .is_err()
        );
        let (answer, mut result) = oneshot::channel();
        session
            .requests
            .pending
            .lock()
            .unwrap()
            .insert(0, Pending::new(answer));

        kill.send_replace(true);
        blocked.await;
        session
            .handle(ProviderMessage::Reply {
                request: 0,
                result: Ok(Reply::Sent { id: "1".into() }),
            })
            .await;
        assert!(matches!(
            result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(received.try_recv().is_ok());
        assert!(received.try_recv().is_err());
        // The exit path cancels pending requests once the connection drains.
        session.requests.pending.lock().unwrap().clear();
        assert!(result.await.is_err());
    }

    #[tokio::test]
    async fn instance_ids_are_unique_across_orchestrators() {
        let a_dir = tempfile::tempdir().unwrap();
        let b_dir = tempfile::tempdir().unwrap();
        let a = Orchestrator::new(a_dir.path(), Encryption::Plaintext)
            .await
            .unwrap();
        let b = Orchestrator::new(b_dir.path(), Encryption::Plaintext)
            .await
            .unwrap();
        let spec = ProviderSpec {
            name: "p".into(),
            wasm: a_dir.path().join("missing.wasm"),
            settings: BTreeMap::new(),
        };
        let (events, mut received) = mpsc::channel(2);
        let first = a.spawn(spec.clone(), events.clone()).unwrap();
        let second = b.spawn(spec, events).unwrap();
        assert_ne!(first.id(), second.id());
        assert_eq!(first.id().name, second.id().name);
        for _ in 0..2 {
            let event = tokio::time::timeout(Duration::from_secs(5), received.recv())
                .await
                .unwrap();
            let Some(ProviderEvent::Exited { provider, result }) = event else {
                panic!("expected a load failure");
            };
            assert!(result.is_err());
            assert!(&provider == first.id() || &provider == second.id());
        }
    }

    fn decode_command(bytes: &[u8]) -> Command {
        let HostMessage::Command(command) = qmsg_types::decode(bytes).unwrap() else {
            panic!("expected a command");
        };
        command
    }

    /// Answers the next queued request with `reply`.
    async fn answer(
        handle: &ProviderHandle,
        queued: &mut mpsc::UnboundedReceiver<Vec<u8>>,
        reply: Reply,
    ) -> Command {
        let command = decode_command(&queued.recv().await.unwrap());
        let request = command.request().unwrap();
        let pending = handle.requests.pending.lock().unwrap().remove(&request);
        pending.unwrap().waiter.unwrap().send(Ok(reply)).unwrap();
        command
    }

    #[tokio::test]
    async fn dropped_requests_are_forgotten_and_cancelled() {
        let (handle, mut queued) = handle();
        let read = handle.read_blob("b", 0, 10);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), read)
                .await
                .is_err()
        );
        let request = decode_command(&queued.try_recv().unwrap())
            .request()
            .unwrap();
        assert_eq!(
            decode_command(&queued.try_recv().unwrap()),
            Command::Cancel { request }
        );
        // It counts until the provider answers or skips it.
        assert!(
            handle.requests.pending.lock().unwrap()[&request]
                .waiter
                .is_none()
        );
        handle.requests.pending.lock().unwrap().clear();
        // An answered request isn't cancelled.
        let (read, _) = tokio::join!(
            handle.read_blob("b", 0, 10),
            answer(&handle, &mut queued, Reply::Blob(vec![1]))
        );
        assert_eq!(read, Ok(vec![1]));
        assert!(queued.try_recv().is_err());
    }

    #[tokio::test]
    async fn too_many_waiting_requests_are_refused() {
        let (handle, mut queued) = handle();
        for request in 0..MAX_REQUESTS as u64 {
            handle
                .requests
                .pending
                .lock()
                .unwrap()
                .insert(request + 1000, Pending::new(oneshot::channel().0));
        }
        assert_eq!(handle.read_blob("b", 0, 1).await, Err(RequestError::Busy));
        assert!(queued.try_recv().is_err());
    }

    #[tokio::test]
    async fn given_up_requests_count_until_the_provider_answers_them() {
        let (session, _kill, commands) = session_with_events(mpsc::channel(1).0);
        let mut handle = handle_with(commands, session.requests.clone());
        handle.set_timeout(Some(Duration::from_millis(1)));
        // Nothing reads the commands, like a stalled provider, while
        // requesters keep giving up.
        let mut timed_out = 0;
        for _ in 0..3 * MAX_REQUESTS {
            let big = vec![Content::Text("x".repeat(1000))];
            match handle
                .send_message(ChannelRef::new(None, "c"), None, big, None)
                .await
            {
                Err(RequestError::TimedOut) => timed_out += 1,
                Err(RequestError::Busy) => {}
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(timed_out, MAX_REQUESTS);
        let mut queued = session.commands.lock().await;
        let count = |queued: &mut mpsc::UnboundedReceiver<Vec<u8>>| {
            std::iter::from_fn(|| queued.try_recv().ok())
                .map(|bytes| decode_command(&bytes))
                .collect::<Vec<_>>()
        };
        // Each given-up send and its cancel; refused ones queue nothing.
        let commands = count(&mut queued);
        assert_eq!(commands.len(), 2 * MAX_REQUESTS);
        assert!(matches!(commands[1], Command::Cancel { request: 0 }));
        // Cancelling, releasing and stopping don't need room.
        handle.release_blob("b").unwrap();
        handle.shutdown().unwrap();
        assert_eq!(count(&mut queued).len(), 2);
        assert_eq!(handle.read_blob("b", 0, 1).await, Err(RequestError::Busy));

        // Once the provider skips one, there is room for another.
        session
            .handle(ProviderMessage::Reply {
                request: 0,
                result: Err(CommandError::Cancelled),
            })
            .await;
        assert_eq!(
            handle.read_blob("b", 0, 1).await,
            Err(RequestError::TimedOut)
        );
        assert!(matches!(
            count(&mut queued)[..],
            [Command::ReadBlob { .. }, Command::Cancel { .. }]
        ));
    }

    /// Blocks every read until opened, counting reads at once and in all.
    #[derive(Default)]
    struct Stalled {
        open: Mutex<bool>,
        opened: std::sync::Condvar,
        running: std::sync::atomic::AtomicUsize,
        most: std::sync::atomic::AtomicUsize,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl Stalled {
        fn stall(&self) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let running = self.running.fetch_add(1, Ordering::SeqCst) + 1;
            self.most.fetch_max(running, Ordering::SeqCst);
            let open = self.open.lock().unwrap();
            drop(self.opened.wait_while(open, |open| !*open).unwrap());
            self.running.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl BlobSource for Stalled {
        fn size(&self) -> u64 {
            1
        }
        fn read_at(&self, _: u64, _: usize) -> std::io::Result<Vec<u8>> {
            self.stall();
            Ok(vec![0])
        }
    }

    /// Opens the source even when the test fails, since the runtime waits
    /// for its blocked reads.
    struct Opener(Arc<Stalled>);

    impl Drop for Opener {
        fn drop(&mut self) {
            *self.0.open.lock().unwrap_or_else(|e| e.into_inner()) = true;
            self.0.opened.notify_all();
        }
    }

    #[tokio::test]
    async fn queued_bytes_are_limited_until_the_provider_answers() {
        let (mut session, _kill, commands) = session_with_events(mpsc::channel(1).0);
        let send = |request, text: &str| Command::Send {
            request,
            channel: ChannelRef::new(None, "c"),
            reply_to: None,
            content: vec![Content::Text(text.into())],
            nonce: None,
        };
        // Room for exactly two small sends. Ids up to 127 encode alike.
        let small = encode_command(send(0, "x")).unwrap().len();
        Arc::get_mut(&mut session).unwrap().requests = Arc::new(Requests {
            max_queued: 2 * small,
            ..Requests::default()
        });
        let mut handle = handle_with(commands, session.requests.clone());
        // Requesters give up at once, like after a timeout; their commands
        // still count until the provider answers them.
        handle.set_timeout(Some(Duration::from_millis(1)));
        let c = || ChannelRef::new(None, "c");
        let small_send = || handle.send_message(c(), None, vec![Content::Text("x".into())], None);
        let mut queued = session.commands.lock().await;
        let mut sent = || {
            std::iter::from_fn(|| queued.try_recv().ok())
                .map(|bytes| decode_command(&bytes))
                .filter(|c| !matches!(c, Command::Cancel { .. }))
                .collect::<Vec<_>>()
        };
        let answer = |request| {
            session.handle(ProviderMessage::Reply {
                request,
                result: Err(CommandError::Cancelled),
            })
        };

        assert_eq!(small_send().await, Err(RequestError::TimedOut));
        // Exactly at the limit still fits.
        assert_eq!(small_send().await, Err(RequestError::TimedOut));
        assert_eq!(small_send().await, Err(RequestError::Busy));
        assert_eq!(sent(), [send(0, "x"), send(1, "x")]);
        // Stopping and releasing don't count.
        handle.release_blob("b").unwrap();
        handle.shutdown().unwrap();
        assert_eq!(sent().len(), 2);

        // An answer makes room for one more, but not two.
        answer(0).await;
        assert_eq!(small_send().await, Err(RequestError::TimedOut));
        assert_eq!(small_send().await, Err(RequestError::Busy));
        assert_eq!(sent(), [send(3, "x")]);

        // Once nothing is unanswered, one command over the limit still goes,
        // and then nothing else does.
        answer(1).await;
        answer(3).await;
        let big = "x".repeat(4 * small);
        let big_send = handle.send_message(c(), None, vec![Content::Text(big.clone())], None);
        assert_eq!(big_send.await, Err(RequestError::TimedOut));
        assert_eq!(small_send().await, Err(RequestError::Busy));
        assert_eq!(sent(), [send(5, &big)]);
        answer(5).await;
        assert_eq!(small_send().await, Err(RequestError::TimedOut));
    }

    #[tokio::test]
    async fn stuck_blocking_work_stays_bounded_across_reconnects() {
        use tokio::time::timeout;

        const SLOTS: usize = 6;
        let blocking = Arc::new(Blocking::new(SLOTS, Duration::from_millis(200)));
        let blobs = Arc::new(Blobs::default());
        let source = Arc::new(Stalled::default());
        let _opener = Opener(source.clone());
        let _blob = Blob::new(&blobs, "slow".into(), "token".into(), source.clone());
        let resolver: Resolver = {
            let source = source.clone();
            Arc::new(move |_| {
                source.stall();
                Err(io::ErrorKind::NotFound.into())
            })
        };
        let (events_tx, mut events) = mpsc::channel(8);
        let (session, _kill, commands) = session_on(
            events_tx,
            Arc::downgrade(&blobs),
            blocking.clone(),
            resolver,
        );
        let handle = handle_with(commands, session.requests.clone());
        let sessions = Sessions::default();
        sessions
            .lock()
            .unwrap()
            .insert("token".into(), session.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let wait = Duration::from_secs(5);
        let send =
            |message: ProviderMessage| Frame::Binary(qmsg_types::encode(&message).unwrap().into());

        let mut timed_out = 0;
        let mut served = None;
        for round in 0..5 {
            let (stream, _) = tokio::join!(
                tokio_tungstenite::connect_async(format!("ws://{address}/token")),
                async {
                    let (accepted, _) = listener.accept().await.unwrap();
                    served = Some(tokio::spawn(connection(accepted, sessions.clone())));
                }
            );
            let mut socket = stream.unwrap().0;
            // As many of each as one connection may run.
            for n in 0..4 {
                socket
                    .send(send(ProviderMessage::ReadBlob {
                        blob: "slow".into(),
                        offset: n,
                        len: 1,
                    }))
                    .await
                    .unwrap();
                socket
                    .send(send(ProviderMessage::Lookup {
                        name: format!("host{n}:1"),
                    }))
                    .await
                    .unwrap();
            }
            for _ in 0..8 {
                let Some(Ok(Frame::Binary(bytes))) = timeout(wait, socket.next()).await.unwrap()
                else {
                    panic!("expected an answer");
                };
                let (HostMessage::Blob { result: Err(e), .. }
                | HostMessage::Addresses { result: Err(e), .. }) =
                    qmsg_types::decode(&bytes).unwrap()
                else {
                    panic!("expected a failure");
                };
                match e.ends_with("timed out") {
                    true => timed_out += 1,
                    false => assert!(e.contains("too many"), "{e}"),
                }
            }
            // The connection is still served.
            socket
                .send(send(ProviderMessage::Status(ProviderStatus::Ready)))
                .await
                .unwrap();
            let event = timeout(wait, events.recv()).await.unwrap();
            assert!(
                matches!(event, Some(ProviderEvent::Status { .. })),
                "{event:?}"
            );
            if round == 4 {
                handle.shutdown().unwrap();
                let Some(Ok(Frame::Binary(bytes))) = timeout(wait, socket.next()).await.unwrap()
                else {
                    panic!("expected a command");
                };
                assert_eq!(decode_command(&bytes), Command::Shutdown);
            }
            socket.close(None).await.unwrap();
            let served = served.take().unwrap();
            timeout(wait, served).await.unwrap().unwrap();
        }
        // Only the first connection's work ever started; the rest failed
        // at once, without a thread.
        assert_eq!(timed_out, SLOTS);
        assert_eq!(source.calls.load(Ordering::SeqCst), SLOTS);
        assert_eq!(source.most.load(Ordering::SeqCst), SLOTS);
        assert_eq!(blocking.slots.available_permits(), 0);

        // Slots come back once the stuck work returns.
        drop(_opener);
        timeout(wait, async {
            while blocking.slots.available_permits() < SLOTS {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the slots come back");
    }

    #[tokio::test]
    async fn stalled_blob_reads_dont_stop_the_connection() {
        use tokio::time::timeout;

        let blobs = Arc::new(Blobs::default());
        let source = Arc::new(Stalled::default());
        let _opener = Opener(source.clone());
        let _blob = Blob::new(&blobs, "slow".into(), "token".into(), source.clone());
        let (events_tx, mut events) = mpsc::channel(8);
        let (session, _kill, commands) = session_with(events_tx, Arc::downgrade(&blobs));
        let handle = handle_with(commands, session.requests.clone());
        let sessions = Sessions::default();
        sessions
            .lock()
            .unwrap()
            .insert("token".into(), session.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let served = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            connection(stream, sessions).await;
        });
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/token"))
            .await
            .unwrap();
        let wait = Duration::from_secs(5);
        let send =
            |message: ProviderMessage| Frame::Binary(qmsg_types::encode(&message).unwrap().into());
        let read = |offset| {
            send(ProviderMessage::ReadBlob {
                blob: "slow".into(),
                offset,
                len: 1,
            })
        };

        for offset in 0..MAX_BLOB_READS as u64 {
            socket.send(read(offset)).await.unwrap();
        }
        timeout(wait, async {
            while source.running.load(Ordering::SeqCst) < MAX_BLOB_READS {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the reads start");
        // One more fails at once, without starting.
        socket.send(read(99)).await.unwrap();
        let Some(Ok(Frame::Binary(bytes))) = timeout(wait, socket.next()).await.unwrap() else {
            panic!("expected an answer");
        };
        let refused = qmsg_types::decode::<HostMessage>(&bytes).unwrap();
        assert!(
            matches!(
                &refused,
                HostMessage::Blob {
                    offset: 99,
                    result: Err(_),
                    ..
                }
            ),
            "{refused:?}"
        );

        // Statuses still arrive.
        socket
            .send(send(ProviderMessage::Status(ProviderStatus::Ready)))
            .await
            .unwrap();
        let event = timeout(wait, events.recv()).await.unwrap();
        assert!(
            matches!(
                event,
                Some(ProviderEvent::Status {
                    status: ProviderStatus::Ready,
                    ..
                })
            ),
            "{event:?}"
        );

        // Commands still go out and their answers come back.
        let answer = async {
            let Some(Ok(Frame::Binary(bytes))) = socket.next().await else {
                panic!("expected a command");
            };
            let HostMessage::Command(Command::ReadBlob { request, .. }) =
                qmsg_types::decode(&bytes).unwrap()
            else {
                panic!("expected a blob read");
            };
            let reply = ProviderMessage::Reply {
                request,
                result: Ok(Reply::Blob(vec![7])),
            };
            socket.send(send(reply)).await.unwrap();
        };
        let (result, ()) = timeout(wait, async {
            tokio::join!(handle.read_blob("b", 0, 1), answer)
        })
        .await
        .unwrap();
        assert_eq!(result, Ok(vec![7]));

        // And closing ends the connection while the reads are stuck.
        socket.close(None).await.unwrap();
        timeout(wait, served).await.unwrap().unwrap();
        assert_eq!(source.most.load(Ordering::SeqCst), MAX_BLOB_READS);
    }

    #[tokio::test]
    async fn standard_actions_are_called_only_in_their_context() {
        let (handle, mut queued) = handle();
        let send = Function::action("send", "Send", qmsg_types::ActionKind::SendMessage);
        let arguments: Arguments = [(
            qmsg_types::inputs::CONTENT.into(),
            Value::Content(vec![Content::Text("hi".into())]),
        )]
        .into();
        let message = FunctionContext::Message {
            channel: ChannelRef::new(None, "c"),
            id: "1".into(),
        };
        for context in [FunctionContext::Provider, message] {
            let result = handle
                .call_function(&send, context, arguments.clone())
                .await;
            assert!(
                matches!(result, Err(RequestError::WrongFunction(_))),
                "{result:?}"
            );
        }
        assert!(queued.try_recv().is_err(), "nothing is sent");
        let channel = FunctionContext::Channel(ChannelRef::new(None, "c"));
        let (result, command) = tokio::join!(
            handle.call_function(&send, channel, arguments),
            answer(&handle, &mut queued, Reply::Value(Value::Text("1".into())))
        );
        assert!(matches!(command, Command::Call { .. }));
        assert_eq!(result, Ok(Value::Text("1".into())));
    }

    #[tokio::test]
    async fn an_answer_that_comes_as_the_requester_gives_up_releases_its_blobs() {
        use futures_util::FutureExt;
        let (handle, mut queued) = handle();
        let channel = ChannelRef::new(None, "c");
        let mut message = Message::new("1", channel.clone(), "a", 0, vec![]);
        message.content = vec![file("raced")];
        {
            let get = handle.get_message(channel, "1".into());
            tokio::pin!(get);
            assert!((&mut get).now_or_never().is_none());
            let request = decode_command(&queued.try_recv().unwrap())
                .request()
                .unwrap();
            // Answered, as the session would, but never read.
            let pending = handle.requests.pending.lock().unwrap().remove(&request);
            let answer = Ok(Reply::Message(message));
            pending.unwrap().waiter.unwrap().send(answer).unwrap();
        }
        assert_eq!(
            decode_command(&queued.try_recv().unwrap()),
            Command::ReleaseBlob {
                blob: "raced".into()
            }
        );
        assert!(
            queued.try_recv().is_err(),
            "an answered request isn't cancelled"
        );
    }

    #[tokio::test]
    async fn unawaited_replies_release_their_blobs() {
        let (session, _kill, _commands) = session_with_events(mpsc::channel(1).0);
        let mut commands = session.commands.lock().await;
        let mut message = Message::new("1", ChannelRef::new(None, "c"), "a", 0, vec![]);
        message.content = vec![file("late")];
        session
            .handle(ProviderMessage::Reply {
                request: 7,
                result: Ok(Reply::History(HistoryPage {
                    messages: vec![message.clone()],
                    next_cursor: None,
                })),
            })
            .await;
        assert_eq!(
            decode_command(&commands.try_recv().unwrap()),
            Command::ReleaseBlob {
                blob: "late".into()
            }
        );
        // Or the requester gave up just before the answer was handed over.
        let (waiter, gone) = oneshot::channel();
        drop(gone);
        session
            .requests
            .pending
            .lock()
            .unwrap()
            .insert(8, Pending::new(waiter));
        message.content = vec![file("dropped")];
        session
            .handle(ProviderMessage::Reply {
                request: 8,
                result: Ok(Reply::Message(message)),
            })
            .await;
        assert_eq!(
            decode_command(&commands.try_recv().unwrap()),
            Command::ReleaseBlob {
                blob: "dropped".into()
            }
        );
        assert!(session.requests.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn history_from_another_channel_is_refused_and_released() {
        let (handle, mut queued) = handle();
        let here = ChannelRef::new(None, "here");
        let mut message = Message::new("1", ChannelRef::new(None, "there"), "a", 0, vec![]);
        message.content = vec![file("b1")];
        let page = HistoryPage {
            messages: vec![message.clone()],
            next_cursor: None,
        };
        let (result, command) = tokio::join!(
            handle.history(here.clone(), None, 10),
            answer(&handle, &mut queued, Reply::History(page))
        );
        assert!(matches!(command, Command::History { limit: 10, .. }));
        assert!(
            matches!(result, Err(RequestError::BadReply(_))),
            "{result:?}"
        );
        assert_eq!(
            decode_command(&queued.try_recv().unwrap()),
            Command::ReleaseBlob { blob: "b1".into() }
        );
        let (result, _) = tokio::join!(
            handle.get_message(here, "1".into()),
            answer(&handle, &mut queued, Reply::Message(message))
        );
        assert!(
            matches!(result, Err(RequestError::BadReply(_))),
            "{result:?}"
        );
    }

    fn helper(id: &str, kind: FunctionKind) -> Function {
        Function {
            id: id.into(),
            label: id.into(),
            description: String::new(),
            kind,
            inputs: vec![],
        }
    }

    fn call(function: &str) -> FunctionCall {
        FunctionCall {
            function: function.into(),
            context: FunctionContext::Provider,
            arguments: Default::default(),
        }
    }

    #[tokio::test]
    async fn requests_are_checked_against_declarations_before_sending() {
        use qmsg_types::{FunctionInput, ValueType};
        let (handle, mut queued) = handle();
        let mut lookup = helper("find", FunctionKind::Lookup(ValueType::Text));
        lookup.inputs.push(FunctionInput {
            required: true,
            ..FunctionInput::new("name", "Name", ValueType::Text)
        });
        let result = handle
            .call_function(&lookup, FunctionContext::Provider, Default::default())
            .await;
        assert!(matches!(result,
            Err(RequestError::Failed(CommandError::InvalidInput(issues))) if issues[0].code == "required"));
        let check = helper("check", FunctionKind::Verification(ValueType::Text));
        let result = handle
            .verify(
                &check,
                VerificationRequest {
                    call: call("check"),
                    input: Some(qmsg_types::FunctionInputRef {
                        function: "find".into(),
                        input: "name".into(),
                    }),
                    value: Value::Integer(1),
                },
            )
            .await;
        assert!(matches!(result,
            Ok(Verification::Invalid(issues)) if issues[0].input.as_deref() == Some("name")));
        // The declaration must be the one the request is for, and fit it.
        let request = VerificationRequest {
            call: call("other"),
            input: None,
            value: Value::Text("x".into()),
        };
        assert!(matches!(
            handle.verify(&check, request.clone()).await,
            Err(RequestError::WrongFunction(_))
        ));
        assert!(matches!(
            handle.verify(&lookup, request).await,
            Err(RequestError::WrongFunction(_))
        ));
        assert!(matches!(
            handle
                .call_function(&check, FunctionContext::Provider, Default::default())
                .await,
            Err(RequestError::WrongFunction(_))
        ));
        assert!(queued.try_recv().is_err(), "nothing is sent");
    }

    #[tokio::test]
    async fn replies_that_dont_fit_are_refused() {
        let (handle, mut queued) = handle();
        let answers = [Reply::Blob(vec![0; 11]), Reply::Sent { id: "1".into() }];
        for answer in answers {
            let answer_it = async {
                let bytes = queued.recv().await.unwrap();
                let HostMessage::Command(Command::ReadBlob { request, .. }) =
                    qmsg_types::decode(&bytes).unwrap()
                else {
                    panic!("expected a blob read");
                };
                let pending = handle.requests.pending.lock().unwrap().remove(&request);
                pending.unwrap().waiter.unwrap().send(Ok(answer)).unwrap();
            };
            let (read, ()) = tokio::join!(handle.read_blob("b", 0, 10), answer_it);
            assert!(matches!(read, Err(RequestError::BadReply(_))), "{read:?}");
        }
    }

    #[tokio::test]
    async fn malformed_function_replies_are_refused() {
        use qmsg_types::{CompletionItem, ValueType};
        let (handle, mut queued) = handle();
        let lookup = helper("find", FunctionKind::Lookup(ValueType::Text));
        let check = helper("check", FunctionKind::Verification(ValueType::Text));
        let suggest = helper("suggest", FunctionKind::Completion(ValueType::Text));
        let item = |value| CompletionItem {
            label: "item".into(),
            description: None,
            value,
        };
        let page = |items| {
            Reply::Completed(CompletionPage {
                items,
                next_cursor: None,
            })
        };
        let cases = [
            (0, Reply::Value(Value::Null)),
            // Duplicate ids.
            (0, Reply::Functions(vec![lookup.clone(), lookup.clone()])),
            (1, Reply::Functions(vec![])),
            // A result of the wrong type.
            (1, Reply::Value(Value::Integer(1))),
            (2, Reply::Verified(Verification::Invalid(vec![]))),
            (2, Reply::Value(Value::Null)),
            // Over the limit of one.
            (3, page(vec![item(Value::Content(vec![file("over")])); 2])),
            // Suggestions of the wrong type.
            (3, page(vec![item(Value::Integer(1))])),
            (3, Reply::Value(Value::Null)),
        ];
        let mut released = Vec::new();
        for (method, reply) in cases {
            let run = async {
                match method {
                    0 => handle
                        .functions(FunctionContext::Provider)
                        .await
                        .map(|_| ()),
                    1 => handle
                        .call_function(&lookup, FunctionContext::Provider, Default::default())
                        .await
                        .map(|_| ()),
                    2 => handle
                        .verify(
                            &check,
                            VerificationRequest {
                                call: call("check"),
                                input: None,
                                value: Value::Text("input".into()),
                            },
                        )
                        .await
                        .map(|_| ()),
                    _ => handle
                        .complete(
                            &suggest,
                            CompletionRequest {
                                call: call("suggest"),
                                input: None,
                                query: "q".into(),
                                cursor: None,
                                limit: 1,
                            },
                        )
                        .await
                        .map(|_| ()),
                }
            };
            let (result, command) = tokio::join!(run, answer(&handle, &mut queued, reply));
            let expected = match command {
                Command::Functions { .. } => 0,
                Command::Call { call, .. } => {
                    assert_eq!(call.function, "find");
                    1
                }
                Command::Verify { verification, .. } => {
                    assert_eq!(verification.value, Value::Text("input".into()));
                    2
                }
                Command::Complete { completion, .. } => {
                    assert_eq!((completion.query.as_str(), completion.limit), ("q", 1));
                    3
                }
                other => panic!("unexpected {other:?}"),
            };
            assert_eq!(method, expected);
            assert!(
                matches!(result, Err(RequestError::BadReply(_))),
                "{result:?}"
            );
            released.extend(std::iter::from_fn(|| queued.try_recv().ok()));
        }
        // No one reads a refused reply's blobs.
        let released: Vec<_> = released.iter().map(|b| decode_command(b)).collect();
        let over = || Command::ReleaseBlob {
            blob: "over".into(),
        };
        assert_eq!(released, [over(), over()]);
    }
}
