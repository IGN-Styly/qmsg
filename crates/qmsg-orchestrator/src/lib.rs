//! Runs qmsg providers as `wasm32-wasip2` components.
//!
//! Each provider gets its own OS thread, which creates the provider's wasmtime
//! `Store` and loads its Wasm. Providers own their network connections and have
//! full TCP and UDP access.
//!
//! Providers talk to the orchestrator over a WebSocket. The orchestrator serves
//! it on localhost and gives each provider a URL with its own session token.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::{Context as _, bail};
use futures_util::{SinkExt, StreamExt};
use qmsg_types::{
    ABI_VERSION, Command, HostMessage, LogLevel, Message, ProviderConfig, ProviderMessage,
};
use serde::Deserialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::tungstenite::Message as Frame;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request};
use tokio_tungstenite::tungstenite::http::StatusCode;
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder, UpdateDeadline};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

mod bindings {
    wasmtime::component::bindgen!({
        path: "../../wit",
        world: "provider",
        exports: { default: async },
    });
}

/// How often running providers are made to yield to the executor.
const EPOCH_TICK: Duration = Duration::from_millis(10);
const MEMORY_LIMIT: usize = 64 * 1024 * 1024;

/// A provider to run.
#[derive(Debug, Clone, Deserialize)]
pub struct ProviderSpec {
    pub name: String,
    pub wasm: PathBuf,
    #[serde(default)]
    pub settings: BTreeMap<String, String>,
}

#[derive(Debug)]
pub enum ProviderEvent {
    Message {
        provider: String,
        message: Message,
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
    commands: tokio::sync::Mutex<mpsc::UnboundedReceiver<Command>>,
    events: mpsc::UnboundedSender<ProviderEvent>,
    kv: Mutex<HashMap<String, Vec<u8>>>,
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

pub struct Orchestrator {
    engine: Engine,
    linker: Arc<Linker<WasiState>>,
    sessions: Sessions,
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
    pub async fn new() -> anyhow::Result<Self> {
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
            addr,
            server,
        })
    }

    /// Starts a provider on its own thread.
    ///
    /// Events from the provider, including its exit, are sent to `events`.
    /// Failing to load the Wasm is reported as an `Exited` event.
    pub fn spawn(
        &self,
        spec: ProviderSpec,
        events: mpsc::UnboundedSender<ProviderEvent>,
    ) -> anyhow::Result<ProviderHandle> {
        let token = new_token()?;
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let session = Arc::new(Session {
            name: spec.name.clone(),
            commands: tokio::sync::Mutex::new(commands_rx),
            events: events.clone(),
            kv: Mutex::default(),
            connections: watch::Sender::new(0),
        });
        self.sessions
            .lock()
            .unwrap()
            .insert(token.clone(), session.clone());

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
                let result = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime.block_on(async {
                        let finished = async {
                            let result = run(engine, &linker, spec, host_url).await;
                            // The provider's socket closed when its store was
                            // dropped; handle what it sent before reporting
                            // the exit, so `Exited` is always its last event.
                            session.drained().await;
                            result
                        };
                        tokio::select! {
                            result = finished => Some(result),
                            () = killed.notified() => None,
                        }
                    }),
                    Err(e) => Some(Err(e.into())),
                };
                sessions.lock().unwrap().remove(&token);
                if let Some(result) = result {
                    let _ = events.send(ProviderEvent::Exited {
                        provider: name,
                        result: result.map_err(|e| format!("{e:#}")),
                    });
                }
            });
        if let Err(e) = spawned {
            // The thread never started, so nothing else will remove the session.
            self.sessions.lock().unwrap().remove(&session_token);
            return Err(e.into());
        }

        Ok(ProviderHandle {
            commands: commands_tx,
            kill,
        })
    }
}

/// Controls a running provider. Dropping it kills the provider.
pub struct ProviderHandle {
    commands: mpsc::UnboundedSender<Command>,
    kill: Arc<Notify>,
}

impl ProviderHandle {
    /// Queues a command. Fails if the provider has already exited.
    pub fn send(&self, command: Command) -> anyhow::Result<()> {
        self.commands
            .send(command)
            .map_err(|_| anyhow::anyhow!("provider is not running"))
    }

    /// Stops the provider immediately, even if it is blocked on I/O.
    ///
    /// No `Exited` event is sent for a killed provider.
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
        limits: StoreLimitsBuilder::new()
            .memory_size(MEMORY_LIMIT)
            // `memory_size` applies to each memory, so allow only one.
            .memories(1)
            .build(),
    };
    let mut store = Store::new(&engine, state);
    store.limiter(|state| &mut state.limits);
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
    limits: StoreLimits,
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
    let accept = tokio_tungstenite::accept_hdr_async(stream, |request: &Request, response| {
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
    });
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
    let (replies_tx, mut replies) = mpsc::unbounded_channel();

    let reader = async move {
        while let Some(frame) = stream.next().await {
            match frame {
                Ok(Frame::Binary(bytes)) => match qmsg_types::decode(&bytes) {
                    Ok(message) => {
                        if let Some(reply) = session.handle(message) {
                            let _ = replies_tx.send(reply);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(provider = session.name, "invalid message: {e}");
                        break;
                    }
                },
                Ok(Frame::Close(_)) => break,
                Ok(_) => {}
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
            let message = tokio::select! {
                command = commands.recv(), if commands_open => match command {
                    Some(command) => HostMessage::Command(command),
                    None => {
                        commands_open = false;
                        continue;
                    }
                },
                reply = replies.recv() => match reply {
                    Some(reply) => reply,
                    None => break,
                },
            };
            let bytes = qmsg_types::encode(&message).expect("host messages always encode");
            if sink.send(Frame::Binary(bytes.into())).await.is_err() {
                break;
            }
        }
    };

    tokio::join!(reader, writer);
}

impl Session {
    /// Handles a message from the provider, returning the reply if it needs one.
    fn handle(&self, message: ProviderMessage) -> Option<HostMessage> {
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
            ProviderMessage::Emit(message) => {
                let _ = self.events.send(ProviderEvent::Message {
                    provider: self.name.clone(),
                    message,
                });
                None
            }
            ProviderMessage::KvGet { key } => {
                let value = self.kv.lock().unwrap().get(&key).cloned();
                Some(HostMessage::Value { key, value })
            }
            ProviderMessage::KvSet { key, value } => {
                self.kv.lock().unwrap().insert(key, value);
                None
            }
        }
    }
}

fn not_found() -> ErrorResponse {
    let mut response = ErrorResponse::new(None);
    *response.status_mut() = StatusCode::NOT_FOUND;
    response
}
