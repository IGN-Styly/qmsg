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
//! Providers' secrets live in a SQLite database in the data directory; see
//! [`Encryption`] for how they are protected.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::Duration;

use anyhow::{Context as _, bail};
use futures_util::{SinkExt, StreamExt};
use qmsg_types::{
    ABI_VERSION, ChannelRef, Command, CommandError, Content, DirectoryUpdate, HostMessage,
    LogLevel, MAX_FRAME, MessageEvent, ProviderConfig, ProviderMessage, Reply,
};
use serde::Deserialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc, oneshot, watch};
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
/// without reading only stalls its own connection.
const REPLY_QUEUE: usize = 64;

/// A provider to run.
#[derive(Debug, Clone, Deserialize)]
pub struct ProviderSpec {
    pub name: String,
    pub wasm: PathBuf,
    #[serde(default)]
    pub settings: BTreeMap<String, String>,
}

/// Something a provider did. `Exited` is always a provider's last event, and
/// its name can't be used by another provider until `Exited` is delivered.
#[derive(Debug)]
pub enum ProviderEvent {
    Message {
        provider: String,
        event: MessageEvent,
    },
    /// A change to the organizations, users and channels the provider is part
    /// of.
    Directory {
        provider: String,
        update: DirectoryUpdate,
    },
    Exited {
        provider: String,
        result: Result<(), String>,
    },
}

/// Sessions by token.
type Sessions = Arc<Mutex<HashMap<String, Arc<Session>>>>;

/// The orchestrator's side of one provider's WebSocket.
struct Session {
    name: String,
    /// Encoded `HostMessage::Command`s.
    commands: tokio::sync::Mutex<mpsc::UnboundedReceiver<Vec<u8>>>,
    events: mpsc::Sender<ProviderEvent>,
    /// Weak, so dropping the orchestrator releases the data directory even
    /// while providers are still running.
    secrets: Weak<Secrets>,
    blobs: Weak<Blobs>,
    requests: Arc<Requests>,
    /// Open connections, counted from the handshake until every frame the
    /// provider sent has been handled.
    connections: watch::Sender<usize>,
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
}

/// Counts one connection for as long as it is alive.
struct ConnectionGuard(Arc<Session>);

impl ConnectionGuard {
    fn new(session: Arc<Session>) -> Self {
        session.connections.send_modify(|n| *n += 1);
        Self(session)
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.connections.send_modify(|n| *n -= 1);
    }
}

type Answer = Result<Reply, CommandError>;

/// Commands waiting for the provider's answer, shared by the session and the
/// handle.
#[derive(Default)]
struct Requests {
    next: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<Answer>>>,
}

/// Forgets a request once it is answered or no longer awaited.
struct PendingGuard<'a> {
    requests: &'a Requests,
    request: u64,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.requests.pending.lock().unwrap().remove(&self.request);
    }
}

/// Why a [`ProviderHandle`] request failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestError {
    /// The provider exited, or exited before answering.
    NotRunning,
    /// The command is over [`MAX_FRAME`] encoded. Send big files as a
    /// [`Blob`] instead.
    TooLarge { size: usize },
    /// The provider couldn't carry out the command.
    Failed(CommandError),
    /// The provider answered with a reply for a different command.
    UnexpectedReply(Reply),
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
            Self::Failed(e) => write!(f, "{e}"),
            Self::UnexpectedReply(reply) => write!(f, "unexpected reply {reply:?}"),
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
            addr,
            server,
        })
    }

    /// Offers `source` to providers, to send as a [`Blob::media`]. Providers
    /// can read it until the returned [`Blob`] is dropped.
    pub fn add_blob(&self, source: impl BlobSource) -> anyhow::Result<Blob> {
        Ok(Blob::new(&self.blobs, new_token()?, source))
    }

    /// Starts a provider on its own thread.
    ///
    /// Events from the provider, including its exit, are sent to `events`.
    /// When `events` is full the provider's messages wait, which in turn
    /// slows the provider down.
    /// Failing to load the Wasm is reported as an `Exited` event.
    ///
    /// Fails if a provider with the same name hasn't delivered its `Exited`
    /// yet.
    pub fn spawn(
        &self,
        spec: ProviderSpec,
        events: mpsc::Sender<ProviderEvent>,
    ) -> anyhow::Result<ProviderHandle> {
        let token = new_token()?;
        let mut sessions = self.sessions.lock().unwrap();
        // Secrets are keyed by name, so two running providers with the same
        // name would share them.
        if sessions.values().any(|s| s.name == spec.name) {
            bail!("a provider named `{}` is already running", spec.name);
        }
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let session = Arc::new(Session {
            name: spec.name.clone(),
            commands: tokio::sync::Mutex::new(commands_rx),
            events: events.clone(),
            secrets: Arc::downgrade(&self.secrets),
            blobs: Arc::downgrade(&self.blobs),
            requests: Arc::default(),
            connections: watch::Sender::new(0),
        });
        let requests = session.requests.clone();
        sessions.insert(token.clone(), session.clone());
        drop(sessions);

        let kill = Arc::new(Notify::new());
        let killed = kill.clone();
        let engine = self.engine.clone();
        let linker = self.linker.clone();
        let sessions = self.sessions.clone();
        let host_url = format!("ws://{}/{token}", self.addr);
        let name = spec.name.clone();
        let session_token = token.clone();

        let spawned = thread::Builder::new()
            .name(format!("provider-{}", spec.name))
            .spawn(move || {
                let exited = |result: anyhow::Result<()>| ProviderEvent::Exited {
                    provider: name,
                    result: result.map_err(|e| format!("{e:#}")),
                };
                match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime.block_on(async {
                        // Losing the race drops the provider's store, which
                        // closes its sockets.
                        let result = tokio::select! {
                            result = run(engine, &linker, spec, host_url) => result,
                            () = killed.notified() => Err(anyhow::anyhow!("killed")),
                        };
                        // Handle what the provider sent before reporting the
                        // exit, so `Exited` is always its last event.
                        session.drained().await;
                        // Fail new requests, then the ones still waiting.
                        session.commands.lock().await.close();
                        session.requests.pending.lock().unwrap().clear();
                        let permit = events.reserve().await;
                        // Freeing the name and sending `Exited` together means
                        // a new provider with this name can't send events
                        // first, and can be spawned as soon as `Exited` is
                        // received.
                        let mut sessions = sessions.lock().unwrap();
                        sessions.remove(&token);
                        if let Ok(permit) = permit {
                            permit.send(exited(result));
                        }
                    }),
                    Err(e) => {
                        // The provider never ran, so it sent no other events.
                        sessions.lock().unwrap().remove(&token);
                        let _ = events.blocking_send(exited(Err(e.into())));
                    }
                }
            });
        if let Err(e) = spawned {
            // The thread never started, so nothing else will remove the session.
            self.sessions.lock().unwrap().remove(&session_token);
            return Err(e.into());
        }

        Ok(ProviderHandle {
            commands: commands_tx,
            requests,
            kill,
        })
    }
}

/// Controls a running provider. Dropping it kills the provider.
pub struct ProviderHandle {
    commands: mpsc::UnboundedSender<Vec<u8>>,
    requests: Arc<Requests>,
    kill: Arc<Notify>,
}

impl ProviderHandle {
    /// Sends a message, returning its id.
    ///
    /// Check the content with [`Channel::check`](qmsg_types::Channel::check)
    /// first to learn of the channel's limits without asking the provider.
    pub async fn send_message(
        &self,
        channel: ChannelRef,
        reply_to: Option<String>,
        content: Vec<Content>,
    ) -> Result<String, RequestError> {
        let reply = self
            .request(|request| Command::Send {
                request,
                channel,
                reply_to,
                content,
            })
            .await?;
        match reply {
            Reply::Sent { id } => Ok(id),
            other => Err(RequestError::UnexpectedReply(other)),
        }
    }

    /// Opens a conversation with `members`, or finds the one already open.
    /// The provider reports the channel before this returns.
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
            other => Err(RequestError::UnexpectedReply(other)),
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
            Reply::Blob(bytes) => Ok(bytes),
            other => Err(RequestError::UnexpectedReply(other)),
        }
    }

    /// Asks the provider to stop. It exits when it is done.
    pub fn shutdown(&self) -> Result<(), RequestError> {
        self.queue(Command::Shutdown)
    }

    async fn request(&self, command: impl FnOnce(u64) -> Command) -> Result<Reply, RequestError> {
        let request = self.requests.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.requests.pending.lock().unwrap().insert(request, tx);
        let _guard = PendingGuard {
            requests: &self.requests,
            request,
        };
        self.queue(command(request))?;
        rx.await
            .map_err(|_| RequestError::NotRunning)?
            .map_err(RequestError::Failed)
    }

    fn queue(&self, command: Command) -> Result<(), RequestError> {
        let bytes =
            qmsg_types::encode(&HostMessage::Command(command)).expect("commands always encode");
        if bytes.len() > MAX_FRAME {
            return Err(RequestError::TooLarge { size: bytes.len() });
        }
        self.commands
            .send(bytes)
            .map_err(|_| RequestError::NotRunning)
    }

    /// Stops the provider immediately, even if it is blocked on I/O.
    ///
    /// Its `Exited` event fails with `killed`.
    pub fn kill(&self) {
        self.kill.notify_one();
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
            .map(ConnectionGuard::new);
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
        while let Some(frame) = stream.next().await {
            match frame {
                Ok(Frame::Binary(bytes)) => match qmsg_types::decode(&bytes) {
                    Ok(message) => {
                        if let Some(reply) = session.handle(message).await
                            && replies_tx.send(reply).await.is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(provider = session.name, "invalid message: {e}");
                        break;
                    }
                },
                Ok(Frame::Close(_)) => break,
                Ok(_) => {}
                // The SDK refuses to send these, so the provider bypassed it.
                Err(e @ WsError::Capacity(_)) => {
                    tracing::warn!(provider = session.name, "connection error: {e}");
                    break;
                }
                Err(e) => {
                    tracing::debug!(provider = session.name, "connection error: {e}");
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
                // Replies are small: secrets and blob reads are limited far
                // below `MAX_FRAME`.
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
                let provider = &self.name;
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
                let _ = self
                    .events
                    .send(ProviderEvent::Message {
                        provider: self.name.clone(),
                        event,
                    })
                    .await;
                None
            }
            ProviderMessage::Reply { request, result } => {
                let pending = self.requests.pending.lock().unwrap().remove(&request);
                match pending {
                    // The requester may have stopped waiting.
                    Some(tx) => {
                        let _ = tx.send(result);
                    }
                    None => tracing::debug!(provider = self.name, request, "unawaited reply"),
                }
                None
            }
            ProviderMessage::ReadBlob { blob, offset, len } => {
                let result = blobs::read(&self.blobs, &blob, offset, len).await;
                Some(HostMessage::Blob {
                    blob,
                    offset,
                    result,
                })
            }
            ProviderMessage::Directory(update) => {
                let _ = self
                    .events
                    .send(ProviderEvent::Directory {
                        provider: self.name.clone(),
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
        let name = self.name.clone();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || op(&secrets, &name, &key))
            .await
            .expect("secret storage panicked")
    }
}

fn not_found() -> ErrorResponse {
    let mut response = ErrorResponse::new(None);
    *response.status_mut() = StatusCode::NOT_FOUND;
    response
}
