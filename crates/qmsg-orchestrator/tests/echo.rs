//! Runs the echo provider against a local TCP server.

use std::path::PathBuf;
use std::process::Command as Process;
use std::sync::OnceLock;
use std::time::Duration;

use qmsg_orchestrator::{Orchestrator, ProviderEvent, ProviderSpec};
use qmsg_types::{Command, Message};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::timeout;

fn echo_wasm() -> &'static PathBuf {
    static WASM: OnceLock<PathBuf> = OnceLock::new();
    WASM.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let status = Process::new(env!("CARGO"))
            .args([
                "build",
                "-p",
                "qmsg-provider-echo",
                "--target",
                "wasm32-wasip2",
            ])
            .current_dir(&root)
            .status()
            .expect("failed to run cargo");
        assert!(status.success(), "building the echo provider failed");
        let target = std::env::var_os("CARGO_TARGET_DIR")
            .map(|dir| root.join(dir))
            .unwrap_or_else(|| root.join("target"));
        target.join("wasm32-wasip2/debug/qmsg_provider_echo.wasm")
    })
}

/// Greets each client, then answers every line with `echo: <line>`.
async fn start_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (read, mut write) = socket.into_split();
                write.write_all(b"hello from server\n").await.unwrap();
                let mut lines = BufReader::new(read).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    write
                        .write_all(format!("echo: {line}\n").as_bytes())
                        .await
                        .unwrap();
                }
            });
        }
    });
    port
}

fn spec(port: u16) -> ProviderSpec {
    ProviderSpec {
        name: "echo".into(),
        wasm: echo_wasm().clone(),
        settings: [("server".to_owned(), format!("127.0.0.1:{port}"))].into(),
    }
}

async fn next(events: &mut mpsc::Receiver<ProviderEvent>) -> ProviderEvent {
    timeout(Duration::from_secs(30), events.recv())
        .await
        .expect("timed out waiting for provider event")
        .expect("event channel closed")
}

#[tokio::test]
async fn provider_owns_its_connection() {
    let port = start_server().await;
    let orchestrator = Orchestrator::new().await.unwrap();
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();

    let message = match next(&mut events).await {
        ProviderEvent::Message { message, .. } => message,
        other => panic!("expected the greeting, got {other:?}"),
    };
    assert_eq!(message.body, "hello from server");

    provider
        .send(Command::Send {
            chat: "general".into(),
            body: "ping".into(),
        })
        .unwrap();
    let ProviderEvent::Message {
        provider: name,
        message,
    } = next(&mut events).await
    else {
        panic!("expected the reply");
    };
    assert_eq!(name, "echo");
    assert_eq!(
        message,
        Message {
            chat: "general".into(),
            author: format!("127.0.0.1:{port}"),
            body: "echo: ping".into(),
        }
    );

    provider.send(Command::Shutdown).unwrap();
    // Sent right before the provider returns, so it must still beat `Exited`.
    let message = match next(&mut events).await {
        ProviderEvent::Message { message, .. } => message,
        other => panic!("expected the goodbye, got {other:?}"),
    };
    assert_eq!(message.body, "goodbye");
    let ProviderEvent::Exited { result, .. } = next(&mut events).await else {
        panic!("expected the provider to exit");
    };
    assert_eq!(result, Ok(()));
}

#[tokio::test]
async fn kill_stops_a_blocked_provider() {
    let port = start_server().await;
    let orchestrator = Orchestrator::new().await.unwrap();
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    next(&mut events).await; // greeting; the provider now waits for commands

    provider.kill();
    // A killed provider drops its sender without sending `Exited`.
    let closed = timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap();
    assert!(closed.is_none());
}

#[tokio::test]
async fn dropping_the_handle_stops_the_provider() {
    let port = start_server().await;
    let orchestrator = Orchestrator::new().await.unwrap();
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    next(&mut events).await; // greeting; the provider now waits for commands

    drop(provider);
    let closed = timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap();
    assert!(closed.is_none());
}
