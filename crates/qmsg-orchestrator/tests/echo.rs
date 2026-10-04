//! Runs the echo provider against a local TCP server.

use std::path::PathBuf;
use std::process::Command as Process;
use std::sync::OnceLock;
use std::time::Duration;

use qmsg_orchestrator::{Directory, Encryption, Orchestrator, ProviderEvent, ProviderSpec};
use qmsg_types::{
    Channel, ChannelKind, ChannelRef, Command, Content, ContentKind, DirectoryUpdate, MAX_FRAME,
    Media, MediaSource, Message, MessageEvent,
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
    received(events).await;
}

async fn received(events: &mut mpsc::Receiver<ProviderEvent>) -> Message {
    match next(events).await {
        ProviderEvent::Message {
            event: MessageEvent::Received(message),
            ..
        } => message,
        other => panic!("expected a message, got {other:?}"),
    }
}

async fn sent(events: &mut mpsc::Receiver<ProviderEvent>) -> (u64, Result<String, String>) {
    match next(events).await {
        ProviderEvent::Sent {
            request, result, ..
        } => (request, result),
        other => panic!("expected a send result, got {other:?}"),
    }
}

async fn exited(events: &mut mpsc::Receiver<ProviderEvent>) -> Result<(), String> {
    match next(events).await {
        ProviderEvent::Exited { result, .. } => result,
        other => panic!("expected the provider to exit, got {other:?}"),
    }
}

fn text(text: &str) -> Vec<Content> {
    vec![Content::Text(text.into())]
}

/// The echo server's only channel.
fn home(port: u16) -> ChannelRef {
    let server = format!("127.0.0.1:{port}");
    ChannelRef::new(Some(&server), server.clone())
}

fn send(request: u64, channel: ChannelRef, content: Vec<Content>) -> Command {
    Command::Send {
        request,
        channel,
        reply_to: None,
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
    assert!(matches!(update, DirectoryUpdate::OrganizationUpserted(_)));
    directory.apply(&provider, update).unwrap();

    let channel = directory.channel("echo", &home(port)).unwrap();
    assert_eq!(
        channel,
        &Channel {
            accepted_content: vec![ContentKind::Text],
            ..Channel::new(&server, &server, ChannelKind::Text)
        }
    );
    assert!(channel.accepts(&Content::Text("hi".into())));
    assert!(!channel.accepts(&Content::Image(Media {
        name: None,
        mime: Some("image/png".into()),
        source: MediaSource::Url("https://example.com/a.png".into()),
    })));
    let scope = directory.scope("echo", Some(&server)).unwrap();
    assert_eq!(scope.me(), Some("me"));

    // The greeting comes from the organization's channel, by a known user.
    let greeting = received(&mut events).await;
    assert_eq!(greeting.channel, home(port));
    assert_eq!(greeting.content, text("hello from server"));
    assert_eq!(directory.author("echo", &greeting).unwrap().id, server);
}

#[tokio::test]
async fn provider_owns_its_connection() {
    let port = start_server().await;
    let server = format!("127.0.0.1:{port}");
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    provider.send(send(1, home(port), text("ping"))).unwrap();
    let (request, result) = sent(&mut events).await;
    assert_eq!(request, 1);
    let id = result.unwrap();
    // The sent message is reported like any other, by the account.
    let mine = received(&mut events).await;
    assert_eq!(
        (mine.id.as_str(), mine.author.as_str()),
        (id.as_str(), "me")
    );
    assert_eq!(mine.content, text("ping"));

    let reply = received(&mut events).await;
    assert_eq!(
        (
            &reply.channel,
            reply.author.as_str(),
            reply.reply_to.as_deref()
        ),
        (&home(port), server.as_str(), Some(id.as_str()))
    );
    assert_eq!(reply.content, text("echo: ping"));
    assert_ne!(reply.id, id);

    // Each line gets its own reply.
    let content = vec![Content::Text("a\r\nb".into()), Content::Text("c".into())];
    provider.send(send(2, home(port), content)).unwrap();
    assert_eq!(sent(&mut events).await.0, 2);
    received(&mut events).await; // the sent message
    for expected in ["echo: a", "echo: b", "echo: c"] {
        assert_eq!(received(&mut events).await.content, text(expected));
    }

    provider.send(Command::Shutdown).unwrap();
    // Sent right before the provider returns, so it must still beat `Exited`.
    assert_eq!(received(&mut events).await.content, text("goodbye"));
    assert_eq!(exited(&mut events).await, Ok(()));
}

#[tokio::test]
async fn sends_the_channel_cannot_take_fail() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    // A channel the provider never reported.
    let elsewhere = ChannelRef::new(None, "general");
    provider.send(send(1, elsewhere, text("ping"))).unwrap();
    let (request, result) = sent(&mut events).await;
    assert_eq!(request, 1);
    assert!(result.is_err());

    // Content the channel doesn't accept.
    let image = Content::Image(Media {
        name: Some("cat.png".into()),
        mime: Some("image/png".into()),
        source: MediaSource::Bytes(vec![0x89, b'P', b'N', b'G']),
    });
    provider
        .send(send(
            2,
            home(port),
            vec![Content::Text("look".into()), image],
        ))
        .unwrap();
    let (request, result) = sent(&mut events).await;
    assert_eq!(request, 2);
    assert!(result.is_err());

    // Nothing was sent, so the next event is the goodbye.
    provider.send(Command::Shutdown).unwrap();
    assert_eq!(received(&mut events).await.content, text("goodbye"));
}

#[tokio::test]
async fn commands_over_the_limit_are_refused() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    let huge = Content::File(Media {
        name: None,
        mime: None,
        source: MediaSource::Bytes(vec![0; MAX_FRAME]),
    });
    assert!(provider.send(send(1, home(port), vec![huge])).is_err());

    // The connection is still up.
    provider.send(send(2, home(port), text("ping"))).unwrap();
    assert_eq!(sent(&mut events).await, (2, Ok("2".into())));
}

#[tokio::test]
async fn kill_stops_a_blocked_provider() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await; // the provider now waits for commands

    provider.kill();
    assert_eq!(exited(&mut events).await, Err("killed".into()));
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
    assert_eq!(exited(&mut events).await, Err("killed".into()));
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

    provider.kill();
    assert_eq!(exited(&mut events).await, Err("killed".into()));
    // The name is free as soon as `Exited` is received.
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
