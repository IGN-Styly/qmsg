//! Runs the echo provider against a local TCP server.

use std::io;
use std::path::PathBuf;
use std::process::Command as Process;
use std::sync::{Mutex, OnceLock, mpsc as std_mpsc};
use std::time::Duration;

use qmsg_orchestrator::{
    BlobSource, Directory, Encryption, Orchestrator, ProviderEvent, ProviderSpec, RequestError,
};
use qmsg_types::{
    ChannelKind, ChannelRef, CommandError, Content, ContentKind, DirectoryUpdate, MAX_FRAME,
    MAX_READ, Media, MediaSource, Message, MessageEvent, Violation,
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
    start_server_answering(usize::MAX).await
}

/// Like [`start_server`], but hangs up on a client once it has answered
/// `answers` lines.
async fn start_server_answering(answers: usize) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (read, mut write) = socket.into_split();
                write.write_all(b"hello from server\n").await.unwrap();
                let mut lines = BufReader::new(read).lines();
                for _ in 0..answers {
                    let Ok(Some(line)) = lines.next_line().await else {
                        return;
                    };
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
    named("echo", port)
}

fn named(name: &str, port: u16) -> ProviderSpec {
    ProviderSpec {
        name: name.into(),
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

/// Long, since every test compiles the provider at once.
async fn next(events: &mut mpsc::Receiver<ProviderEvent>) -> ProviderEvent {
    timeout(Duration::from_secs(120), events.recv())
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

fn file(name: &str, source: MediaSource) -> Content {
    Content::File(Media {
        name: Some(name.into()),
        mime: None,
        size: None,
        source,
    })
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
    assert_eq!(channel.kind, ChannelKind::Text);
    assert_eq!(channel.limits.max_attachments, Some(1));
    assert_eq!(
        directory.scope("echo", Some(&server)).unwrap().me(),
        Some("me")
    );
    // The orchestrator can check content against the channel's limits itself.
    assert_eq!(channel.check(&text("hi")), Ok(()));
    assert_eq!(
        channel.check(&[Content::Text("x".repeat(2001))]),
        Err(Violation::TextTooLong {
            length: 2001,
            max: 2000
        })
    );

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

    let id = provider
        .send_message(home(port), None, text("ping"))
        .await
        .unwrap();
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
    provider
        .send_message(home(port), None, content)
        .await
        .unwrap();
    received(&mut events).await; // the sent message
    for expected in ["echo: a", "echo: b", "echo: c"] {
        assert_eq!(received(&mut events).await.content, text(expected));
    }

    provider.shutdown().unwrap();
    // Sent right before the provider returns, so it must still beat `Exited`.
    assert_eq!(received(&mut events).await.content, text("goodbye"));
    assert_eq!(exited(&mut events).await, Ok(()));
}

#[tokio::test]
async fn a_send_the_server_drops_fails() {
    let port = start_server_answering(1).await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    let sent = provider.send_message(home(port), None, text("a\nb")).await;
    let Err(RequestError::Failed(CommandError::Failed(error))) = sent else {
        panic!("expected the send to fail, got {sent:?}");
    };
    assert!(error.contains("1 of 2 lines"), "{error}");
    // The server's answer still arrives, but the message isn't reported as
    // sent.
    let reply = received(&mut events).await;
    assert_eq!((reply.content, reply.reply_to), (text("echo: a"), None));
    assert!(exited(&mut events).await.is_err());
}

#[tokio::test]
async fn sends_the_channel_cannot_take_fail() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    let elsewhere = ChannelRef::new(None, "general");
    let image = Content::Image(Media {
        name: None,
        mime: Some("image/png".into()),
        size: None,
        source: MediaSource::Bytes(vec![0x89, b'P', b'N', b'G']),
    });
    let two_files = vec![
        file("a", MediaSource::Bytes(vec![1])),
        file("b", MediaSource::Bytes(vec![2])),
    ];
    let cases = [
        (
            elsewhere.clone(),
            text("ping"),
            CommandError::UnknownChannel(elsewhere),
        ),
        (
            home(port),
            vec![Content::Text("look".into()), image],
            Violation::Unsupported {
                part: 1,
                kind: ContentKind::Image,
            }
            .into(),
        ),
        (
            home(port),
            two_files,
            Violation::TooManyAttachments { count: 2, max: 1 }.into(),
        ),
        (
            home(port),
            vec![file("c", MediaSource::Url("https://example.com/c".into()))],
            CommandError::Unsupported,
        ),
    ];
    for (channel, content, error) in cases {
        assert_eq!(
            provider.send_message(channel, None, content).await,
            Err(RequestError::Failed(error))
        );
    }

    // Nothing was sent, so the next event is the goodbye.
    provider.shutdown().unwrap();
    assert_eq!(received(&mut events).await.content, text("goodbye"));
}

#[tokio::test]
async fn blobs_are_read_in_pieces_both_ways() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    // Bigger than one read, so it takes several.
    let bytes: Vec<u8> = (0..MAX_READ as usize + 1000).map(|i| i as u8).collect();
    let blob = provider.add_blob(bytes.clone()).unwrap();
    let content = vec![Content::File(blob.media(Some("big.bin".into()), None))];
    provider
        .send_message(home(port), None, content)
        .await
        .unwrap();

    // The provider holds its copy of the sent file.
    let mine = received(&mut events).await;
    let [Content::File(media)] = mine.content.as_slice() else {
        panic!("expected a file, got {:?}", mine.content);
    };
    assert_eq!(media.size, Some(bytes.len() as u64));
    let MediaSource::Blob(id) = &media.source else {
        panic!("expected a blob, got {:?}", media.source);
    };
    let mut copy = Vec::new();
    loop {
        let piece = provider
            .read_blob(id, copy.len() as u64, MAX_READ)
            .await
            .unwrap();
        copy.extend_from_slice(&piece);
        if piece.len() < MAX_READ as usize {
            break;
        }
    }
    assert_eq!(copy, bytes);

    let reply = received(&mut events).await;
    let expected = format!("echo: file big.bin: {} bytes", bytes.len());
    assert_eq!(reply.content, text(&expected));

    // Once released, the provider frees its copy.
    provider.release_blob(id).unwrap();
    assert_eq!(
        provider.read_blob(id, 0, 10).await,
        Err(RequestError::Failed(CommandError::UnknownBlob(id.clone())))
    );

    // A dropped blob can't be read any more.
    let gone = blob.id().to_owned();
    drop(blob);
    let content = vec![file("gone", MediaSource::Blob(gone))];
    assert!(matches!(
        provider.send_message(home(port), None, content).await,
        Err(RequestError::Failed(CommandError::Failed(_)))
    ));
}

#[tokio::test]
async fn providers_only_read_their_own_blobs() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let a = orchestrator
        .spawn(named("a", port), events_tx.clone())
        .unwrap();
    started(&mut events).await;
    let b = orchestrator.spawn(named("b", port), events_tx).unwrap();
    started(&mut events).await;

    let blob = a.add_blob(vec![1, 2, 3]).unwrap();
    let content = vec![Content::File(blob.media(None, None))];
    assert!(matches!(
        b.send_message(home(port), None, content.clone()).await,
        Err(RequestError::Failed(CommandError::Failed(_)))
    ));
    a.send_message(home(port), None, content).await.unwrap();
}

#[tokio::test]
async fn files_over_the_limit_are_refused_by_the_provider() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    // Without a size, the channel's limits can't catch it; the provider
    // finds out while reading it.
    let max = 16 * 1024 * 1024;
    let blob = provider.add_blob(vec![0u8; max + 1]).unwrap();
    let content = vec![
        Content::Text("big".into()),
        Content::File(Media {
            size: None,
            ..blob.media(None, None)
        }),
    ];
    assert_eq!(
        provider.send_message(home(port), None, content).await,
        Err(RequestError::Failed(
            Violation::TooLarge {
                part: 1,
                size: max as u64 + 1,
                max: max as u64,
            }
            .into()
        ))
    );
}

/// A blob source whose reads say they started, then wait until `release`
/// is dropped.
struct Stall {
    started: Mutex<std_mpsc::Sender<()>>,
    release: Mutex<std_mpsc::Receiver<()>>,
}

impl BlobSource for Stall {
    fn size(&self) -> u64 {
        1
    }

    fn read_at(&self, _: u64, _: usize) -> io::Result<Vec<u8>> {
        let _ = self.started.lock().unwrap().send(());
        let _ = self.release.lock().unwrap().recv();
        Ok(vec![0])
    }
}

#[tokio::test]
async fn requests_end_on_timeout_or_exit() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let mut provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    // The provider stays stuck reading this, so it answers nothing.
    let (started_tx, started) = std_mpsc::channel();
    let (release, stalled) = std_mpsc::channel();
    let blob = provider
        .add_blob(Stall {
            started: Mutex::new(started_tx),
            release: Mutex::new(stalled),
        })
        .unwrap();
    let content = vec![Content::File(blob.media(None, None))];
    provider.set_timeout(Some(Duration::from_secs(2)));
    assert_eq!(
        provider.send_message(home(port), None, content).await,
        Err(RequestError::TimedOut)
    );
    // It timed out because the read stalled, not for some other reason.
    started.try_recv().expect("the read never started");

    // A request still waiting when the provider exits fails. `join!` polls
    // in order, so the request is queued before the kill.
    provider.set_timeout(None);
    let both = async {
        tokio::join!(
            provider.send_message(home(port), None, text("ping")),
            async {
                tokio::task::yield_now().await;
                provider.kill();
            }
        )
    };
    let (sent, ()) = timeout(Duration::from_secs(60), both).await.unwrap();
    assert_eq!(sent, Err(RequestError::NotRunning));
    assert_eq!(exited(&mut events).await, Err("killed".into()));
    drop(release);
}

#[tokio::test]
async fn a_killed_provider_frees_its_name_while_no_one_reads() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    // Room for the organization only, and nothing reads it for now.
    let (events_tx, mut events) = mpsc::channel(1);
    let old = orchestrator.spawn(spec(port), events_tx.clone()).unwrap();
    timeout(Duration::from_secs(120), async {
        while events.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    old.kill();
    let new = timeout(Duration::from_secs(30), async {
        loop {
            match orchestrator.spawn(spec(port), events_tx.clone()) {
                Ok(new) => return new,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("the name was never freed");

    // The old provider's `Exited` still comes before anything from the new.
    let ProviderEvent::Directory { .. } = next(&mut events).await else {
        panic!("expected the old organization");
    };
    assert_eq!(exited(&mut events).await, Err("killed".into()));
    started(&mut events).await;
    drop(new);
}

#[tokio::test]
async fn kill_drops_events_waiting_for_room() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    // Room for the organization only, so the greeting waits.
    let (events_tx, mut events) = mpsc::channel(1);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    timeout(Duration::from_secs(120), async {
        while events.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // Give the greeting time to start waiting behind it. Whether or not it
    // has, nothing but `Exited` may follow the organization once killed.
    tokio::time::sleep(Duration::from_millis(200)).await;

    provider.kill();
    let ProviderEvent::Directory { .. } = next(&mut events).await else {
        panic!("expected the organization");
    };
    assert_eq!(exited(&mut events).await, Err("killed".into()));
}

#[tokio::test]
async fn opens_channels_with_known_users() {
    let port = start_server().await;
    let server = format!("127.0.0.1:{port}");
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    let opened = provider
        .open_channel(Some(server.clone()), vec![server.clone()])
        .await;
    assert_eq!(opened, Ok(home(port)));
    let opened = provider
        .open_channel(Some(server.clone()), vec!["bob".into()])
        .await;
    assert_eq!(
        opened,
        Err(RequestError::Failed(CommandError::UnknownUser(
            "bob".into()
        )))
    );
    // The server is only a user in its own organization.
    let opened = provider
        .open_channel(Some("elsewhere".into()), vec![server.clone()])
        .await;
    assert_eq!(
        opened,
        Err(RequestError::Failed(CommandError::UnknownOrganization(
            "elsewhere".into()
        )))
    );
    let opened = provider.open_channel(None, vec![server.clone()]).await;
    assert_eq!(
        opened,
        Err(RequestError::Failed(CommandError::UnknownUser(server)))
    );
}

#[tokio::test]
async fn commands_over_the_limit_are_refused() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    let huge = file("huge", MediaSource::Bytes(vec![0; MAX_FRAME]));
    assert!(matches!(
        provider.send_message(home(port), None, vec![huge]).await,
        Err(RequestError::TooLarge { .. })
    ));

    // The connection is still up.
    let sent = provider.send_message(home(port), None, text("ping")).await;
    assert_eq!(sent, Ok("2".into()));
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
    // Requests fail rather than wait forever.
    let sent = provider.send_message(home(port), None, text("ping")).await;
    assert_eq!(sent, Err(RequestError::NotRunning));
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
