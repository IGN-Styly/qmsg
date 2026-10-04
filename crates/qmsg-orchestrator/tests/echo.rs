//! Runs the echo provider against a local TCP server.

use std::path::PathBuf;
use std::process::Command as Process;
use std::sync::OnceLock;
use std::time::Duration;

use qmsg_orchestrator::{Directory, Encryption, Orchestrator, ProviderEvent, ProviderSpec};
use qmsg_types::{
    Channel, ChannelKind, Command, Content, ContentKind, DirectoryUpdate, Media, MediaSource,
    Message,
};
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

/// An orchestrator with its own data directory, which lives as long as it does.
async fn orchestrator() -> (Orchestrator, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    // Tests must not touch the real OS keychain.
    let orchestrator = Orchestrator::new(dir.path(), Encryption::Plaintext)
        .await
        .unwrap();
    (orchestrator, dir)
}

async fn next(events: &mut mpsc::Receiver<ProviderEvent>) -> ProviderEvent {
    timeout(Duration::from_secs(30), events.recv())
        .await
        .expect("timed out waiting for provider event")
        .expect("event channel closed")
}

/// Skips the organization the provider reports first, and its greeting.
async fn started(events: &mut mpsc::Receiver<ProviderEvent>) {
    let ProviderEvent::Directory { .. } = next(events).await else {
        panic!("expected the organization");
    };
    let ProviderEvent::Message { .. } = next(events).await else {
        panic!("expected the greeting");
    };
}

fn text(text: &str) -> Vec<Content> {
    vec![Content::Text(text.into())]
}

fn send(channel: &str, content: Vec<Content>) -> Command {
    Command::Send {
        organization: None,
        channel: channel.into(),
        content,
    }
}

#[tokio::test]
async fn provider_reports_its_organization() {
    let port = start_server().await;
    let server = format!("127.0.0.1:{port}");
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let _provider = orchestrator.spawn(spec(port), events_tx).unwrap();

    let mut directory = Directory::new();
    let ProviderEvent::Directory { provider, update } = next(&mut events).await else {
        panic!("expected the organization");
    };
    assert!(matches!(update, DirectoryUpdate::OrganizationSet(_)));
    directory.apply(&provider, update);

    let channel = directory.channel("echo", Some(&server), &server).unwrap();
    assert_eq!(
        channel,
        &Channel {
            id: server.clone(),
            name: server.clone(),
            kind: ChannelKind::Text,
            inputs: vec![ContentKind::Text],
        }
    );
    assert!(channel.accepts(&Content::Text("hi".into())));
    assert!(!channel.accepts(&Content::Image(Media {
        name: None,
        mime: Some("image/png".into()),
        source: MediaSource::Url("https://example.com/a.png".into()),
    })));

    // The greeting comes from the organization's channel.
    let ProviderEvent::Message { message, .. } = next(&mut events).await else {
        panic!("expected the greeting");
    };
    assert_eq!(
        message,
        Message {
            organization: Some(server.clone()),
            channel: server.clone(),
            author: server,
            content: text("hello from server"),
        }
    );
}

#[tokio::test]
async fn provider_owns_its_connection() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    provider.send(send("general", text("ping"))).unwrap();
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
            organization: Some(format!("127.0.0.1:{port}")),
            channel: "general".into(),
            author: format!("127.0.0.1:{port}"),
            content: text("echo: ping"),
        }
    );

    // Each line gets its own reply, all in the command's channel. Content the
    // server can't take is skipped.
    let image = Content::Image(Media {
        name: Some("cat.png".into()),
        mime: Some("image/png".into()),
        source: MediaSource::Bytes(vec![0x89, b'P', b'N', b'G']),
    });
    let content = vec![
        Content::Text("a\r\nb".into()),
        image,
        Content::Text("c".into()),
    ];
    provider.send(send("general", content)).unwrap();
    for expected in ["echo: a", "echo: b", "echo: c"] {
        let message = match next(&mut events).await {
            ProviderEvent::Message { message, .. } => message,
            other => panic!("expected a reply, got {other:?}"),
        };
        assert_eq!(
            (message.channel.as_str(), message.content),
            ("general", text(expected))
        );
    }

    provider.send(Command::Shutdown).unwrap();
    // Sent right before the provider returns, so it must still beat `Exited`.
    let message = match next(&mut events).await {
        ProviderEvent::Message { message, .. } => message,
        other => panic!("expected the goodbye, got {other:?}"),
    };
    assert_eq!(message.content, text("goodbye"));
    let ProviderEvent::Exited { result, .. } = next(&mut events).await else {
        panic!("expected the provider to exit");
    };
    assert_eq!(result, Ok(()));
}

#[tokio::test]
async fn kill_stops_a_blocked_provider() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await; // the provider now waits for commands

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
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await; // the provider now waits for commands

    drop(provider);
    let closed = timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap();
    assert!(closed.is_none());
}

#[tokio::test]
async fn running_providers_have_unique_names() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx.clone()).unwrap();
    started(&mut events).await;

    // It would share the running provider's secrets.
    assert!(orchestrator.spawn(spec(port), events_tx.clone()).is_err());

    provider.send(Command::Shutdown).unwrap();
    next(&mut events).await; // goodbye
    let ProviderEvent::Exited { .. } = next(&mut events).await else {
        panic!("expected the provider to exit");
    };
    // The name is free again once the provider has exited.
    let _provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;
}

#[tokio::test]
async fn dropping_the_orchestrator_releases_the_data_directory() {
    let port = start_server().await;
    let (orchestrator, dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let _provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await; // the provider is still running

    drop(orchestrator);
    Orchestrator::new(dir.path(), Encryption::Plaintext)
        .await
        .unwrap();
}
