//! Runs the echo provider against a local TCP server.

use std::io;
use std::path::PathBuf;
use std::process::Command as Process;
use std::sync::{Mutex, OnceLock, mpsc as std_mpsc};
use std::time::Duration;

use qmsg_orchestrator::{
    BlobSource, Directory, Encryption, Orchestrator, ProviderEvent, ProviderHandle, ProviderSpec,
    REQUEST_TIMEOUT, RequestError,
};
use qmsg_types::{
    ActionKind, Arguments, ChannelKind, ChannelRef, CommandError, CompletionRequest, Content,
    ContentKind, DirectoryUpdate, FormattedText, Function, FunctionCall, FunctionContext,
    FunctionInputRef, FunctionKind, InputHint, MAX_FRAME, MAX_READ, Media, MediaSource, Message,
    MessageEvent, ProviderStatus, Reaction, Span, Style, Value, ValueType, Verification,
    VerificationRequest, Violation,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::net::tcp::OwnedReadHalf;
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
                let mut lines = provider_lines(read);
                for _ in 0..answers {
                    let Some(line) = lines.recv().await else {
                        return;
                    };
                    write
                        .write_all(format!("echo: {line}\n").as_bytes())
                        .await
                        .unwrap();
                }
                // Hang up cleanly. Closing with lines unread would reset the
                // connection, which can discard answers already sent.
                let _ = write.shutdown().await;
                while lines.recv().await.is_some() {}
            });
        }
    });
    port
}

/// The lines the provider writes, without the bytes that follow a file's
/// line.
fn provider_lines(read: OwnedReadHalf) -> mpsc::UnboundedReceiver<String> {
    let (lines, received) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut read = BufReader::new(read);
        loop {
            let mut line = String::new();
            if read.read_line(&mut line).await.unwrap_or(0) == 0 {
                return;
            }
            let line = line.trim_end_matches(['\r', '\n']).to_owned();
            let size = line
                .strip_prefix("file ")
                .and_then(|l| l.strip_suffix(" bytes"))
                .and_then(|l| l.rsplit(": ").next())
                .and_then(|size| size.parse::<u64>().ok());
            if let Some(size) = size {
                let file = (&mut read).take(size);
                match tokio::io::copy(&mut { file }, &mut tokio::io::sink()).await {
                    Ok(n) if n == size => {}
                    _ => return,
                }
            }
            if lines.send(line).is_err() {
                return;
            }
        }
    });
    received
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

async fn status(events: &mut mpsc::Receiver<ProviderEvent>) -> ProviderStatus {
    match next(events).await {
        ProviderEvent::Status { status, .. } => status,
        other => panic!("expected a status, got {other:?}"),
    }
}

async fn directory(events: &mut mpsc::Receiver<ProviderEvent>) -> DirectoryUpdate {
    match next(events).await {
        ProviderEvent::Directory { update, .. } => update,
        other => panic!("expected a directory update, got {other:?}"),
    }
}

/// Checks a sync from `Syncing` to `Ready`: a reset, then the organization.
async fn synced(events: &mut mpsc::Receiver<ProviderEvent>) {
    assert_eq!(status(events).await, ProviderStatus::Syncing);
    assert_eq!(directory(events).await, DirectoryUpdate::Reset);
    let DirectoryUpdate::OrganizationUpserted(_) = directory(events).await else {
        panic!("expected the organization");
    };
    assert_eq!(status(events).await, ProviderStatus::Ready);
}

/// Checks the provider connects and syncs, then skips its greeting.
async fn started(events: &mut mpsc::Receiver<ProviderEvent>) {
    assert_eq!(status(events).await, ProviderStatus::Connecting);
    synced(events).await;
    assert_eq!(received(events).await.content, text("hello from server"));
}

/// The message event that comes next.
async fn message_event(events: &mut mpsc::Receiver<ProviderEvent>) -> MessageEvent {
    match next(events).await {
        ProviderEvent::Message { event, .. } => event,
        other => panic!("expected a message event, got {other:?}"),
    }
}

/// The event already queued, which must come before an answer.
fn queued(events: &mut mpsc::Receiver<ProviderEvent>) -> ProviderEvent {
    events
        .try_recv()
        .expect("the event must come before the answer")
}

fn args<const N: usize>(arguments: [(&str, Value); N]) -> Arguments {
    arguments.into_iter().map(|(k, v)| (k.into(), v)).collect()
}

fn find<'a>(functions: &'a [Function], id: &str) -> &'a Function {
    functions
        .iter()
        .find(|f| f.id == id)
        .unwrap_or_else(|| panic!("no function `{id}` in {functions:?}"))
}

/// What a test tells [`start_scripted_server`] to do.
enum Control {
    /// Say a line of its own, as another user would.
    Say(String),
    /// Hold answers until `Hold(false)`, like a slow platform.
    Hold(bool),
}

/// Like [`start_server`], one client at a time, but under the test's control.
async fn start_scripted_server() -> (u16, mpsc::UnboundedSender<Control>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (control_tx, mut control) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let (read, mut write) = socket.into_split();
            write.write_all(b"hello from server\n").await.unwrap();
            let mut lines = provider_lines(read);
            let mut hold = false;
            let mut held = Vec::new();
            loop {
                let out = tokio::select! {
                    line = lines.recv() => match line {
                        Some(line) if hold => {
                            held.push(line);
                            continue;
                        }
                        Some(line) => vec![line],
                        None => break,
                    },
                    control = control.recv() => match control {
                        Some(Control::Say(line)) => {
                            write.write_all(format!("{line}\n").as_bytes()).await.unwrap();
                            continue;
                        }
                        Some(Control::Hold(on)) => {
                            hold = on;
                            if on { continue } else { std::mem::take(&mut held) }
                        }
                        None => return,
                    },
                };
                for line in out {
                    write
                        .write_all(format!("echo: {line}\n").as_bytes())
                        .await
                        .unwrap();
                }
            }
        }
    });
    (port, control_tx)
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
async fn functions_offer_actions_lookups_verification_and_completion() {
    let port = start_server().await;
    let server = format!("127.0.0.1:{port}");
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;
    // The host checks declarations, so standard actions follow their contract.
    let functions = provider.functions(FunctionContext::Provider).await.unwrap();
    let users = find(&functions, "users");
    assert_eq!(users.inputs[0].verification.as_deref(), Some("verify-user"));
    assert_eq!(users.inputs[0].completion.as_deref(), Some("complete-user"));
    let FunctionKind::Lookup(ValueType::List(record)) = &users.kind else {
        panic!("expected a list lookup");
    };
    let ValueType::Record(fields) = record.as_ref() else {
        panic!("expected records");
    };
    assert_eq!(
        fields.iter().map(|f| f.id.as_str()).collect::<Vec<_>>(),
        ["id", "name"]
    );
    let open = find(&functions, "open-channel");
    assert!(matches!(
        open.kind,
        FunctionKind::Action {
            action: ActionKind::OpenChannel,
            ..
        }
    ));
    let verify_user = find(&functions, "verify-user");
    let complete_user = find(&functions, "complete-user");
    assert_eq!(
        verify_user.kind,
        FunctionKind::Verification(ValueType::Text)
    );
    assert_eq!(
        complete_user.kind,
        FunctionKind::Completion(ValueType::Text)
    );

    let in_channel = FunctionContext::Channel(home(port));
    let Value::List(found) = provider
        .call_function(users, in_channel.clone(), Arguments::new())
        .await
        .unwrap()
    else {
        panic!("expected users");
    };
    assert_eq!(found.len(), 2);
    // Inputs are checked before the provider is asked.
    assert!(
        matches!(provider.call_function(open, FunctionContext::Provider, Arguments::new()).await,
        Err(RequestError::Failed(CommandError::InvalidInput(issues))) if issues[0].code == "required")
    );
    let members = args([("members", Value::List(vec![Value::Text(server.clone())]))]);
    assert_eq!(
        provider
            .call_function(open, FunctionContext::Provider, members)
            .await
            .unwrap(),
        Value::Channel(home(port))
    );
    let wrong = args([("user", Value::Boolean(true))]);
    assert!(
        matches!(provider.call_function(users, in_channel.clone(), wrong).await,
        Err(RequestError::Failed(CommandError::InvalidInput(issues))) if issues[0].code == "wrong_type")
    );
    let call = |function: &str| FunctionCall {
        function: function.into(),
        context: FunctionContext::Provider,
        arguments: Default::default(),
    };
    let input = FunctionInputRef {
        function: "users".into(),
        input: "user".into(),
    };
    for (value, valid) in [
        (Value::Text(server.clone()), true),
        (Value::Text("missing".into()), false),
    ] {
        let result = provider
            .verify(
                verify_user,
                VerificationRequest {
                    call: call("verify-user"),
                    input: Some(input.clone()),
                    value,
                },
            )
            .await
            .unwrap();
        if valid {
            assert_eq!(result, Verification::Valid);
        } else {
            assert!(matches!(result, Verification::Invalid(issues)
            if issues[0].input.as_deref() == Some("user") && issues[0].code == "unknown_user"));
        }
    }
    // Helpers also work independently of actions or declared inputs, and
    // values of the wrong type are invalid without asking.
    for (value, valid) in [(Value::Text("me".into()), true), (Value::Integer(1), false)] {
        let result = provider
            .verify(
                verify_user,
                VerificationRequest {
                    call: call("verify-user"),
                    input: None,
                    value,
                },
            )
            .await
            .unwrap();
        assert_eq!(result == Verification::Valid, valid, "{result:?}");
    }
    let mut completion = CompletionRequest {
        call: call("complete-user"),
        input: Some(input),
        query: String::new(),
        cursor: None,
        limit: 1,
    };
    let first = provider
        .complete(complete_user, completion.clone())
        .await
        .unwrap();
    assert_eq!(first.items.len(), 1);
    assert_eq!(first.items[0].value, Value::Text(server.clone()));
    completion.cursor = first.next_cursor;
    assert!(completion.cursor.is_some());
    let second = provider
        .complete(complete_user, completion.clone())
        .await
        .unwrap();
    assert_eq!(second.items[0].label, "qmsg");
    assert_eq!(second.items[0].value, Value::Text("me".into()));
    assert!(second.next_cursor.is_none());
    completion.input = None;
    completion.cursor = None;
    completion.query = "q".into();
    assert_eq!(
        provider
            .complete(complete_user, completion.clone())
            .await
            .unwrap()
            .items,
        second.items
    );
    completion.query = "no-match".into();
    let empty = provider
        .complete(complete_user, completion.clone())
        .await
        .unwrap();
    assert!(empty.items.is_empty() && empty.next_cursor.is_none());
    completion.cursor = Some("bad".into());
    assert!(matches!(provider.complete(complete_user, completion).await,
        Err(RequestError::Failed(CommandError::InvalidInput(issues))) if issues[0].code == "bad_cursor"));
    // A shared helper can adapt to the parent action using the input
    // reference, here one item of a list input.
    let action_input = FunctionInputRef {
        function: "open-channel".into(),
        input: "members".into(),
    };
    assert!(matches!(provider.verify(verify_user, VerificationRequest {
        call: call("verify-user"), input: Some(action_input.clone()), value: Value::Text("me".into()),
    }).await.unwrap(), Verification::Invalid(issues) if issues[0].code == "no_conversation"));
    let action_items = provider
        .complete(
            complete_user,
            CompletionRequest {
                call: call("complete-user"),
                input: Some(action_input),
                query: String::new(),
                cursor: None,
                limit: 10,
            },
        )
        .await
        .unwrap();
    assert_eq!(action_items.items.len(), 1);
    assert_eq!(action_items.items[0].value, Value::Text(server));
    let missing = Function {
        id: "missing".into(),
        ..users.clone()
    };
    assert!(
        matches!(provider.call_function(&missing, FunctionContext::Provider, Arguments::new()).await,
        Err(RequestError::Failed(CommandError::UnknownFunction(id))) if id == "missing")
    );
    assert!(
        matches!(provider.functions(FunctionContext::Organization("missing".into())).await,
        Err(RequestError::Failed(CommandError::UnknownOrganization(id))) if id == "missing")
    );
    assert!(
        events.try_recv().is_err(),
        "lookups and input helpers must not emit message events"
    );
    provider.shutdown().unwrap();
    received(&mut events).await;
    assert!(exited(&mut events).await.is_ok());
}

#[tokio::test]
async fn message_actions_follow_standard_contracts() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    let in_channel = FunctionContext::Channel(home(port));
    let functions = provider.functions(in_channel.clone()).await.unwrap();
    let send = find(&functions, "send");
    let sent = provider
        .call_function(
            send,
            in_channel,
            args([
                ("content", Value::Content(text("hi"))),
                ("nonce", Value::Text("n1".into())),
            ]),
        )
        .await
        .unwrap();
    let Value::Text(id) = sent else {
        panic!("a send returns the message id, got {sent:?}");
    };
    // Both are queued before the answer.
    let ProviderEvent::Message {
        event: MessageEvent::Received(mine),
        ..
    } = queued(&mut events)
    else {
        panic!("expected the sent message");
    };
    assert_eq!(
        (mine.id.as_str(), mine.nonce.as_deref()),
        (id.as_str(), Some("n1"))
    );
    let reply = received(&mut events).await;
    assert_eq!(reply.reply_to.as_deref(), Some(id.as_str()));

    let on = |id: &str| FunctionContext::Message {
        channel: home(port),
        id: id.into(),
    };
    let mine_functions = provider.functions(on(&id)).await.unwrap();
    let ids: Vec<_> = mine_functions.iter().map(|f| f.id.as_str()).collect();
    assert_eq!(
        ids,
        [
            "edit",
            "delete",
            "react",
            "unreact",
            "complete-reaction",
            "mark-read",
            "mark-unread"
        ]
    );
    // Only the account's own messages can be edited.
    let server_functions = provider.functions(on(&reply.id)).await.unwrap();
    assert!(
        server_functions
            .iter()
            .all(|f| f.id != "edit" && f.id != "delete")
    );

    let bold = Content::Formatted(FormattedText {
        text: "hi there".into(),
        spans: vec![Span {
            start: 3,
            end: 8,
            style: Style::Bold,
        }],
    });
    let edit = find(&mine_functions, "edit");
    let edited = provider
        .call_function(
            edit,
            on(&id),
            args([("content", Value::Content(vec![bold.clone()]))]),
        )
        .await;
    assert_eq!(edited, Ok(Value::Null));
    let ProviderEvent::Message {
        event: MessageEvent::Edited {
            content, edited_at, ..
        },
        ..
    } = queued(&mut events)
    else {
        panic!("expected the edit");
    };
    assert_eq!(content, vec![bold.clone()]);
    let message = provider.get_message(home(port), id.clone()).await.unwrap();
    assert_eq!(
        (message.content, message.edited_at),
        (vec![bold], Some(edited_at))
    );

    let react = find(&mine_functions, "react");
    let unreact = find(&mine_functions, "unreact");
    let suggestions = provider
        .complete(
            find(&mine_functions, "complete-reaction"),
            CompletionRequest {
                call: FunctionCall {
                    function: "complete-reaction".into(),
                    context: on(&id),
                    arguments: Arguments::new(),
                },
                input: None,
                query: String::new(),
                cursor: None,
                limit: 10,
            },
        )
        .await
        .unwrap();
    let thumbs = suggestions.items[0].value.clone();
    assert_eq!(thumbs, Value::Text("👍".into()));
    let reaction = || args([("reaction", thumbs.clone())]);
    provider
        .call_function(react, on(&id), reaction())
        .await
        .unwrap();
    assert!(matches!(queued(&mut events), ProviderEvent::Message {
        event: MessageEvent::Reacted { key, added: true, .. }, ..
    } if key == "👍"));
    let message = provider.get_message(home(port), id.clone()).await.unwrap();
    assert_eq!(
        message.reactions,
        [Reaction {
            key: "👍".into(),
            count: 1,
            me: true
        }]
    );
    provider
        .call_function(unreact, on(&id), reaction())
        .await
        .unwrap();
    assert!(matches!(
        queued(&mut events),
        ProviderEvent::Message {
            event: MessageEvent::Reacted { added: false, .. },
            ..
        }
    ));
    assert!(
        matches!(provider.call_function(unreact, on(&id), reaction()).await,
        Err(RequestError::Failed(CommandError::InvalidInput(issues))) if issues[0].code == "not_reacted")
    );

    // Unread from the reply on means read up to the message before it.
    let mark_unread = find(&server_functions, "mark-unread");
    provider
        .call_function(mark_unread, on(&reply.id), Arguments::new())
        .await
        .unwrap();
    assert!(matches!(queued(&mut events), ProviderEvent::Message {
        event: MessageEvent::Read { up_to: Some(up_to), user, .. }, ..
    } if up_to == id && user == "me"));

    let delete = find(&mine_functions, "delete");
    provider
        .call_function(delete, on(&id), Arguments::new())
        .await
        .unwrap();
    assert!(matches!(queued(&mut events), ProviderEvent::Message {
        event: MessageEvent::Deleted { id: deleted, .. }, ..
    } if deleted == id));
    assert_eq!(
        provider.get_message(home(port), id.clone()).await,
        Err(RequestError::Failed(CommandError::UnknownMessage(id)))
    );
}

#[tokio::test]
async fn history_is_paged_newest_first() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    assert_eq!(status(&mut events).await, ProviderStatus::Connecting);
    synced(&mut events).await;
    let mut live = vec![received(&mut events).await];
    provider
        .send_message(home(port), None, text("a\nb"), None)
        .await
        .unwrap();
    for _ in 0..3 {
        live.push(received(&mut events).await);
    }

    // Pages are oldest first, newest page first, and hold what was received
    // live: clients replace repeats by channel and id.
    let mut paged = Vec::new();
    let mut cursor = None;
    loop {
        let page = provider.history(home(port), cursor, 3).await.unwrap();
        assert!(page.messages.len() <= 3);
        paged.splice(0..0, page.messages);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(paged, live);
    assert!(
        matches!(provider.history(home(port), Some("bad".into()), 3).await,
        Err(RequestError::Failed(CommandError::InvalidInput(issues))) if issues[0].code == "bad_cursor")
    );
    let elsewhere = ChannelRef::new(None, "elsewhere");
    assert_eq!(
        provider.history(elsewhere.clone(), None, 3).await,
        Err(RequestError::Failed(CommandError::UnknownChannel(
            elsewhere
        )))
    );
    assert_eq!(
        provider.get_message(home(port), live[1].id.clone()).await,
        Ok(live[1].clone())
    );
}

#[tokio::test]
async fn live_events_arrive_while_idle_and_during_slow_sends() {
    let (port, server) = start_scripted_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    // The provider is waiting for commands, and still hears the server.
    server.send(Control::Say("news: hi".into())).unwrap();
    let news = received(&mut events).await;
    assert_eq!(
        (news.content, news.author),
        (text("news: hi"), format!("127.0.0.1:{port}"))
    );

    server.send(Control::Hold(true)).unwrap();
    let send = provider.send_message(home(port), None, text("slow"), Some("n1".into()));
    tokio::pin!(send);
    assert!(
        timeout(Duration::from_millis(300), &mut send)
            .await
            .is_err()
    );
    // Autocomplete is answered while the send waits for the server.
    let functions = provider.functions(FunctionContext::Provider).await.unwrap();
    let completion = CompletionRequest {
        call: FunctionCall {
            function: "complete-user".into(),
            context: FunctionContext::Provider,
            arguments: Arguments::new(),
        },
        input: None,
        query: "q".into(),
        cursor: None,
        limit: 5,
    };
    let page = timeout(
        Duration::from_secs(5),
        provider.complete(find(&functions, "complete-user"), completion),
    )
    .await
    .expect("autocomplete waited for the send")
    .unwrap();
    assert_eq!(page.items[0].value, Value::Text("me".into()));
    server.send(Control::Say("news: meanwhile".into())).unwrap();
    assert_eq!(received(&mut events).await.content, text("news: meanwhile"));

    server.send(Control::Hold(false)).unwrap();
    let id = timeout(Duration::from_secs(30), &mut send)
        .await
        .unwrap()
        .unwrap();
    let mine = received(&mut events).await;
    assert_eq!(
        (mine.id.as_str(), mine.nonce.as_deref()),
        (id.as_str(), Some("n1"))
    );
    let reply = received(&mut events).await;
    assert_eq!(
        (reply.content, reply.reply_to),
        (text("echo: slow"), Some(id))
    );
}

#[tokio::test]
async fn timed_out_sends_are_settled_by_later_events() {
    let (port, server) = start_scripted_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let mut provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    server.send(Control::Hold(true)).unwrap();
    provider.set_timeout(Some(Duration::from_millis(500)));
    let sent = provider
        .send_message(home(port), None, text("late"), Some("n2".into()))
        .await;
    assert_eq!(sent, Err(RequestError::TimedOut));
    provider.set_timeout(Some(REQUEST_TIMEOUT));
    // The line was already written, so the cancel can't call it off. The
    // message with the nonce settles it.
    server.send(Control::Hold(false)).unwrap();
    let mine = received(&mut events).await;
    assert_eq!(
        (mine.content, mine.nonce.as_deref()),
        (text("late"), Some("n2"))
    );
    assert_eq!(received(&mut events).await.content, text("echo: late"));

    // A send that never reached the platform says so before the answer.
    let elsewhere = ChannelRef::new(None, "elsewhere");
    let sent = provider
        .send_message(elsewhere.clone(), None, text("x"), Some("n3".into()))
        .await;
    assert_eq!(
        sent,
        Err(RequestError::Failed(CommandError::UnknownChannel(
            elsewhere.clone()
        )))
    );
    assert!(matches!(queued(&mut events), ProviderEvent::Message {
        event: MessageEvent::NotSent { nonce, channel, .. }, ..
    } if nonce == "n3" && channel == elsewhere));
}

#[tokio::test]
async fn shutdown_is_not_blocked_by_a_slow_send() {
    let (port, server) = start_scripted_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    server.send(Control::Hold(true)).unwrap();
    let send = provider.send_message(home(port), None, text("stuck"), Some("n4".into()));
    let stop = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        provider.shutdown().unwrap();
    };
    let (sent, ()) = timeout(Duration::from_secs(30), async { tokio::join!(send, stop) })
        .await
        .expect("shutdown waited for the send");
    // It may have reached the server, so its delivery is unknown.
    assert_eq!(sent, Err(RequestError::NotRunning));
    assert_eq!(received(&mut events).await.content, text("goodbye"));
    assert_eq!(exited(&mut events).await, Ok(()));
}

async fn call_login(
    provider: &ProviderHandle,
    input: &str,
    value: &str,
) -> Result<Value, RequestError> {
    let functions = provider.functions(FunctionContext::Provider).await.unwrap();
    let [login] = functions.as_slice() else {
        panic!("expected only the login step, got {functions:?}");
    };
    assert!(matches!(
        login.kind,
        FunctionKind::Action {
            action: ActionKind::Login,
            ..
        }
    ));
    let arguments = args([(input, Value::Text(value.into()))]);
    provider
        .call_function(login, FunctionContext::Provider, arguments)
        .await
}

#[tokio::test]
async fn login_steps_then_a_kept_session_until_logout() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let mut spec = spec(port);
    spec.settings.insert("password".into(), "hunter2".into());
    spec.settings.insert("code".into(), "123456".into());
    let provider = orchestrator.spawn(spec.clone(), events_tx.clone()).unwrap();

    assert_eq!(status(&mut events).await, ProviderStatus::Connecting);
    let ProviderStatus::LoginRequired(step) = status(&mut events).await else {
        panic!("expected a login step");
    };
    assert_eq!(step.function.as_deref(), Some("login-password"));
    assert!(matches!(
        provider.status(),
        ProviderStatus::LoginRequired(_)
    ));
    let functions = provider.functions(FunctionContext::Provider).await.unwrap();
    assert_eq!(functions[0].inputs[0].hint, InputHint::Secret);
    assert_eq!(
        provider
            .send_message(home(port), None, text("hi"), None)
            .await,
        Err(RequestError::Failed(CommandError::LoginRequired))
    );
    assert!(matches!(call_login(&provider, "password", "wrong").await,
        Err(RequestError::Failed(CommandError::InvalidInput(issues)))
            if issues[0].input.as_deref() == Some("password") && issues[0].code == "wrong"));
    // Each step's answer comes after the next status.
    assert_eq!(
        call_login(&provider, "password", "hunter2").await,
        Ok(Value::Null)
    );
    let ProviderEvent::Status {
        status: ProviderStatus::LoginRequired(step),
        ..
    } = queued(&mut events)
    else {
        panic!("expected the code step");
    };
    assert_eq!(step.function.as_deref(), Some("login-code"));
    assert_eq!(
        call_login(&provider, "code", "123456").await,
        Ok(Value::Null)
    );
    assert!(matches!(
        queued(&mut events),
        ProviderEvent::Status {
            status: ProviderStatus::Syncing,
            ..
        }
    ));
    assert_eq!(directory(&mut events).await, DirectoryUpdate::Reset);
    directory(&mut events).await;
    assert_eq!(status(&mut events).await, ProviderStatus::Ready);
    received(&mut events).await;

    // The session is kept as a secret, so a restart doesn't ask again.
    provider.kill();
    assert!(exited(&mut events).await.is_err());
    let provider = timeout(Duration::from_secs(30), async {
        loop {
            match orchestrator.spawn(spec.clone(), events_tx.clone()) {
                Ok(provider) => return provider,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .unwrap();
    started(&mut events).await;
    assert_eq!(provider.status(), ProviderStatus::Ready);

    let functions = provider.functions(FunctionContext::Provider).await.unwrap();
    let logout = find(&functions, "logout");
    assert_eq!(
        provider
            .call_function(logout, FunctionContext::Provider, Arguments::new())
            .await,
        Ok(Value::Null)
    );
    assert!(matches!(
        queued(&mut events),
        ProviderEvent::Directory {
            update: DirectoryUpdate::Reset,
            ..
        }
    ));
    assert_eq!(status(&mut events).await, ProviderStatus::Connecting);
    assert!(matches!(
        status(&mut events).await,
        ProviderStatus::LoginRequired(_)
    ));
}

#[tokio::test]
async fn provider_reports_its_organization() {
    let port = start_server().await;
    let server = format!("127.0.0.1:{port}");
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let _provider = orchestrator.spawn(spec(port), events_tx).unwrap();

    let mut directory = Directory::new();
    assert_eq!(status(&mut events).await, ProviderStatus::Connecting);
    assert_eq!(status(&mut events).await, ProviderStatus::Syncing);
    let mut organization = None;
    for _ in 0..2 {
        let ProviderEvent::Directory { provider, update } = next(&mut events).await else {
            panic!("expected the directory");
        };
        assert_eq!(&provider, _provider.id());
        organization = Some(provider.clone());
        directory.apply(&provider, update).unwrap();
    }
    let provider = organization.unwrap();
    assert_eq!(status(&mut events).await, ProviderStatus::Ready);

    let channel = directory.channel(&provider, &home(port)).unwrap();
    assert_eq!(channel.kind, ChannelKind::Text);
    assert_eq!(channel.limits.max_attachments, Some(1));
    assert_eq!(
        directory.scope(&provider, Some(&server)).unwrap().me(),
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
    assert_eq!(directory.author(&provider, &greeting).unwrap().id, server);
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
        .send_message(home(port), None, text("ping"), None)
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
        .send_message(home(port), None, content, None)
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
async fn a_lost_server_fails_sends_then_reconnects() {
    let port = start_server_answering(1).await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    let sent = provider
        .send_message(home(port), None, text("a\nb"), Some("n".into()))
        .await;
    let Err(RequestError::Failed(CommandError::Provider {
        code,
        message: error,
        details,
    })) = sent
    else {
        panic!("expected the send to fail, got {sent:?}");
    };
    assert_eq!(code, "send_incomplete");
    assert_eq!(
        details.as_deref(),
        Some(&Value::Record(
            [
                ("confirmed_lines".into(), Value::Integer(1)),
                ("total_lines".into(), Value::Integer(2)),
                ("delivery_unknown".into(), Value::Boolean(true)),
            ]
            .into()
        ))
    );
    assert!(error.contains("1 of 2 lines"), "{error}");
    // The server's answer still arrives, but the message isn't reported as
    // sent, and isn't reported as not sent either: that is unknown.
    let reply = received(&mut events).await;
    assert_eq!((reply.content, reply.reply_to), (text("echo: a"), None));

    // It reconnects, starts its directory over, and says it may have missed
    // messages.
    assert!(matches!(
        status(&mut events).await,
        ProviderStatus::Disconnected { .. }
    ));
    assert_eq!(status(&mut events).await, ProviderStatus::Connecting);
    assert_eq!(status(&mut events).await, ProviderStatus::Syncing);
    assert_eq!(directory(&mut events).await, DirectoryUpdate::Reset);
    directory(&mut events).await;
    assert_eq!(
        message_event(&mut events).await,
        MessageEvent::Gap {
            channel: home(port)
        }
    );
    assert_eq!(status(&mut events).await, ProviderStatus::Ready);
    assert_eq!(
        received(&mut events).await.content,
        text("hello from server")
    );
    // History made before the reconnect is still there.
    let page = provider.history(home(port), None, 10).await.unwrap();
    assert_eq!(page.messages.len(), 3);
    provider
        .send_message(home(port), None, text("again"), None)
        .await
        .unwrap();
}

#[tokio::test]
async fn partial_replies_are_queued_before_failure_even_when_events_fill() {
    let port = start_server_answering(2).await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(1);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    let sent = provider.send_message(home(port), None, text("a\nb\nc"), None);
    tokio::pin!(sent);
    // The second reply cannot fit. A terminal answer must wait for it.
    assert!(
        timeout(Duration::from_millis(200), &mut sent)
            .await
            .is_err()
    );
    assert_eq!(received(&mut events).await.content, text("echo: a"));
    let result = timeout(Duration::from_secs(30), &mut sent).await.unwrap();
    let Err(RequestError::Failed(CommandError::Provider {
        code,
        message: error,
        details,
    })) = result
    else {
        panic!("expected partial failure, got {result:?}");
    };
    assert_eq!(code, "send_incomplete");
    assert_eq!(
        details.as_deref(),
        Some(&Value::Record(
            [
                ("confirmed_lines".into(), Value::Integer(2)),
                ("total_lines".into(), Value::Integer(3)),
                ("delivery_unknown".into(), Value::Boolean(true)),
            ]
            .into()
        ))
    );
    assert!(error.contains("2 of 3 lines"), "{error}");
    provider.kill();
    // A kill after the answer cannot discard the already queued reply.
    let reply = received(&mut events).await;
    assert_eq!((reply.content, reply.reply_to), (text("echo: b"), None));
    assert!(exited(&mut events).await.is_err());
    assert!(events.recv().await.is_none());
}

#[tokio::test]
async fn success_queues_all_events_before_the_handle_is_dropped() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(2);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    let id = provider
        .send_message(home(port), None, text("ping"), None)
        .await
        .unwrap();
    // Both the sent message and server reply must already be in the queue.
    assert_eq!(events.len(), 2);
    drop(provider);
    assert_eq!(received(&mut events).await.id, id);
    assert_eq!(received(&mut events).await.content, text("echo: ping"));
    assert!(exited(&mut events).await.is_err());
    assert!(events.recv().await.is_none());
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
            provider.send_message(channel, None, content, None).await,
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
        .send_message(home(port), None, content, None)
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
        provider.send_message(home(port), None, content, None).await,
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
        b.send_message(home(port), None, content.clone(), None)
            .await,
        Err(RequestError::Failed(CommandError::Failed(_)))
    ));
    a.send_message(home(port), None, content, None)
        .await
        .unwrap();
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
        provider.send_message(home(port), None, content, None).await,
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
        provider.send_message(home(port), None, content, None).await,
        Err(RequestError::TimedOut)
    );
    // It timed out because the read stalled, not for some other reason.
    started.try_recv().expect("the read never started");

    // A request still waiting when the provider exits fails. `join!` polls
    // in order, so the request is queued before the kill.
    provider.set_timeout(None);
    let both = async {
        tokio::join!(
            provider.send_message(home(port), None, text("ping"), None),
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
    // Room for the first status only, and nothing reads it for now.
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

    assert_ne!(old.id(), new.id());
    assert_eq!(old.id().name, new.id().name);
    // Shutdown requests fail before anyone makes room for the old Exited.
    assert_eq!(old.shutdown(), Err(RequestError::NotRunning));
    let old_id = old.id().clone();
    drop(old); // Its kill switch must not affect the replacement.

    let mut old_exited = false;
    let mut new_directory = false;
    let mut new_greeting = false;
    while !(old_exited && new_directory && new_greeting) {
        match next(&mut events).await {
            ProviderEvent::Status { provider, .. } | ProviderEvent::Directory { provider, .. }
                if provider == old_id =>
            {
                assert!(!old_exited);
            }
            ProviderEvent::Exited { provider, result } if provider == old_id => {
                assert!(!old_exited);
                assert_eq!(result, Err("killed".into()));
                old_exited = true;
            }
            ProviderEvent::Status { provider, .. } if &provider == new.id() => {}
            ProviderEvent::Directory {
                provider,
                update: DirectoryUpdate::OrganizationUpserted(_),
            } if &provider == new.id() => {
                new_directory = true;
            }
            ProviderEvent::Directory { provider, .. } if &provider == new.id() => {}
            ProviderEvent::Message { provider, .. } if &provider == new.id() => {
                assert!(new_directory);
                new_greeting = true;
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
    let server = format!("127.0.0.1:{port}");
    assert_eq!(
        new.open_channel(Some(server.clone()), vec![server]).await,
        Ok(home(port))
    );
    new.kill();
    let ProviderEvent::Exited { provider, result } = next(&mut events).await else {
        panic!("expected the replacement's exit");
    };
    assert_eq!(&provider, new.id());
    assert_eq!(result, Err("killed".into()));
    drop(events_tx);
    assert!(events.recv().await.is_none());
}

#[tokio::test]
async fn a_late_exit_on_another_channel_keeps_the_replacement_directory() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    // The old provider's channel has room for its statuses, reset and
    // organization only, and no one reads it until the replacement is live,
    // so Exited must wait.
    let (old_tx, mut old_events) = mpsc::channel(4);
    let old = orchestrator.spawn(spec(port), old_tx).unwrap();
    timeout(Duration::from_secs(120), async {
        while old_events.len() < 4 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    old.kill();

    let (new_tx, mut new_events) = mpsc::channel(16);
    let new = timeout(Duration::from_secs(30), async {
        loop {
            match orchestrator.spawn(spec(port), new_tx.clone()) {
                Ok(new) => return new,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("the name was never freed");
    let mut directory = Directory::new();
    let new_id = new.id().clone();
    assert_ne!(&new_id, old.id());
    // Apply every update in order, as a client would.
    let mut apply = async |events: &mut mpsc::Receiver<ProviderEvent>, id: &_| loop {
        match next(events).await {
            ProviderEvent::Directory { provider, update } => {
                assert_eq!(&provider, id);
                let organization = matches!(update, DirectoryUpdate::OrganizationUpserted(_));
                directory.apply(&provider, update).unwrap();
                if organization {
                    return;
                }
            }
            ProviderEvent::Status { provider, .. } => assert_eq!(&provider, id),
            other => panic!("unexpected {other:?}"),
        }
    };
    apply(&mut new_events, &new_id).await;
    // Merge the delayed old events only after the new instance is live.
    let old_id = old.id().clone();
    apply(&mut old_events, &old_id).await;
    let ProviderEvent::Exited { provider, result } = next(&mut old_events).await else {
        panic!("expected the old exit");
    };
    assert_eq!(provider, old_id);
    assert_eq!(result, Err("killed".into()));
    directory.remove_provider(&provider);
    assert!(directory.channel(&new_id, &home(port)).is_some());
    assert!(directory.channel(&old_id, &home(port)).is_none());
    assert!(old_events.recv().await.is_none());
}

#[tokio::test]
async fn kill_drops_events_waiting_for_room() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    // Room for the first status only, so everything else waits.
    let (events_tx, mut events) = mpsc::channel(1);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    timeout(Duration::from_secs(120), async {
        while events.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // Give the next event time to start waiting behind it. Whether or not
    // it has, nothing but `Exited` may follow the first status once killed.
    tokio::time::sleep(Duration::from_millis(200)).await;

    provider.kill();
    assert_eq!(status(&mut events).await, ProviderStatus::Connecting);
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
        provider
            .send_message(home(port), None, vec![huge], None)
            .await,
        Err(RequestError::TooLarge { .. })
    ));

    // The connection is still up.
    let sent = provider
        .send_message(home(port), None, text("ping"), None)
        .await;
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
    let sent = provider
        .send_message(home(port), None, text("ping"), None)
        .await;
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

/// Asks for user suggestions, as a client typing would, within `limit`.
async fn autocomplete(provider: &ProviderHandle, limit: Duration) -> Result<(), RequestError> {
    let helper = Function {
        id: "complete-user".into(),
        label: "Suggest users".into(),
        description: String::new(),
        kind: FunctionKind::Completion(ValueType::Text),
        inputs: vec![],
    };
    let completion = CompletionRequest {
        call: FunctionCall {
            function: "complete-user".into(),
            context: FunctionContext::Provider,
            arguments: Arguments::new(),
        },
        input: None,
        query: String::new(),
        cursor: None,
        limit: 5,
    };
    timeout(limit, provider.complete(&helper, completion))
        .await
        .expect("autocomplete waited")
        .map(drop)
}

#[tokio::test]
async fn a_server_that_stops_reading_blocks_nothing_but_the_upload() {
    // The server greets, then never reads, with as little room as it can.
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(1).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = socket.local_addr().unwrap().port();
    let listener = socket.listen(1).unwrap();
    let (held_tx, held) = tokio::sync::oneshot::channel();
    let (writing_tx, writing) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(b"hello from server\n").await.unwrap();
        // The provider fetches a file whole before writing any of it, so
        // once some arrives, the rest is waiting for room.
        socket.peek(&mut [0]).await.unwrap();
        let _ = writing_tx.send(());
        let _ = held.await;
        drop(socket);
    });
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    // Far more than the connection holds without the server reading.
    let blob = provider.add_blob(vec![7u8; 8 * 1024 * 1024]).unwrap();
    let content = vec![Content::File(blob.media(Some("big".into()), None))];
    let send = provider.send_message(home(port), None, content, Some("big".into()));
    tokio::pin!(send);
    tokio::select! {
        sent = &mut send => panic!("the send finished: {sent:?}"),
        _ = writing => {}
    }
    autocomplete(&provider, Duration::from_secs(5))
        .await
        .unwrap();
    let page = provider.history(home(port), None, 10).await.unwrap();
    assert_eq!(page.messages.len(), 1, "only the greeting");

    provider.shutdown().unwrap();
    let (sent, ()) = timeout(Duration::from_secs(10), async {
        tokio::join!(&mut send, async {
            // Part of it was written, so it isn't reported as not sent.
            assert_eq!(received(&mut events).await.content, text("goodbye"));
            assert_eq!(exited(&mut events).await, Ok(()));
        })
    })
    .await
    .expect("shutdown waited for the upload");
    assert_eq!(sent, Err(RequestError::NotRunning));
    let _ = held_tx.send(());
}

#[tokio::test]
async fn connections_left_writing_are_bounded() {
    // Each connection is greeted, never read, then hung up on once the
    // provider writes to it. It fetches a file whole before writing any of
    // it, so by then more is waiting than the connection holds.
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(1).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = socket.local_addr().unwrap().port();
    let listener = socket.listen(1).unwrap();
    let (unread_tx, mut unread) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let (mut read, mut write) = socket.into_split();
            write.write_all(b"hello from server\n").await.unwrap();
            read.peek(&mut [0]).await.unwrap();
            let _ = unread_tx.send(read);
            // Ends only the server's side of writing.
            drop(write);
        }
    });
    let (orchestrator, _dir) = orchestrator().await;

    const MAX_ABANDONED: usize = 4;
    // Each run frees its connections, so the next one starts over.
    for _ in 0..2 {
        let (events_tx, mut events) = mpsc::channel(16);
        let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
        started(&mut events).await;
        for left in 1..=MAX_ABANDONED {
            let blob = provider.add_blob(vec![7u8; 8 * 1024 * 1024]).unwrap();
            let content = vec![Content::File(blob.media(Some("big".into()), None))];
            let sent = provider.send_message(home(port), None, content, None);
            assert!(sent.await.is_err());
            if left == MAX_ABANDONED {
                break;
            }
            // Left open, but it reconnects.
            assert!(matches!(
                status(&mut events).await,
                ProviderStatus::Disconnected { .. }
            ));
            assert_eq!(status(&mut events).await, ProviderStatus::Connecting);
            assert_eq!(status(&mut events).await, ProviderStatus::Syncing);
            assert_eq!(directory(&mut events).await, DirectoryUpdate::Reset);
            directory(&mut events).await;
            assert!(matches!(
                message_event(&mut events).await,
                MessageEvent::Gap { .. }
            ));
            assert_eq!(status(&mut events).await, ProviderStatus::Ready);
            assert_eq!(
                received(&mut events).await.content,
                text("hello from server")
            );
        }
        // Rather than keep more open, it fails, and exiting frees them.
        assert!(matches!(
            status(&mut events).await,
            ProviderStatus::Failed(_)
        ));
        assert!(exited(&mut events).await.is_err());
        for _ in 0..MAX_ABANDONED {
            let mut read = unread.try_recv().expect("a connection per send");
            timeout(Duration::from_secs(10), async {
                let mut buf = vec![0; 64 * 1024];
                while let Ok(1..) = read.read(&mut buf).await {}
            })
            .await
            .expect("the connection was left open");
        }
        assert!(unread.try_recv().is_err(), "no more connections");
    }
}

#[tokio::test]
async fn a_stalled_blob_source_blocks_nothing_but_its_send() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let mut provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    let (started_tx, read_started) = std_mpsc::channel();
    let (release, stalled) = std_mpsc::channel();
    let blob = provider
        .add_blob(Stall {
            started: Mutex::new(started_tx),
            release: Mutex::new(stalled),
        })
        .unwrap();
    let content = vec![Content::File(blob.media(None, None))];
    provider.set_timeout(Some(Duration::from_secs(1)));
    let sent = provider
        .send_message(home(port), None, content, Some("stalled".into()))
        .await;
    assert_eq!(sent, Err(RequestError::TimedOut));
    read_started.try_recv().expect("the read never started");
    provider.set_timeout(Some(REQUEST_TIMEOUT));

    // Nothing was written, so the cancel calls it off.
    assert_eq!(
        message_event(&mut events).await,
        MessageEvent::NotSent {
            channel: home(port),
            nonce: "stalled".into(),
            error: CommandError::Cancelled,
        }
    );
    autocomplete(&provider, Duration::from_secs(5))
        .await
        .unwrap();
    let id = timeout(
        Duration::from_secs(10),
        provider.send_message(home(port), None, text("next"), None),
    )
    .await
    .expect("the stalled send held up the next one")
    .unwrap();
    assert_eq!(received(&mut events).await.id, id);
    assert_eq!(received(&mut events).await.content, text("echo: next"));

    provider.shutdown().unwrap();
    assert_eq!(received(&mut events).await.content, text("goodbye"));
    assert_eq!(exited(&mut events).await, Ok(()));
    drop(release);
}

#[tokio::test]
async fn connecting_answers_commands_and_shutdown() {
    // A full backlog drops the provider's connection attempt, so it hangs.
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = socket.local_addr().unwrap().port();
    let _listener = socket.listen(0).unwrap();
    let _filler = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    assert_eq!(status(&mut events).await, ProviderStatus::Connecting);

    let page = timeout(
        Duration::from_secs(5),
        provider.history(home(port), None, 10),
    )
    .await
    .expect("history waited for the connection")
    .unwrap();
    assert!(page.messages.is_empty());
    let completed = autocomplete(&provider, Duration::from_secs(5)).await;
    assert!(
        matches!(&completed, Err(RequestError::Failed(CommandError::Provider { code, .. })) if code == "not_connected"),
        "{completed:?}"
    );
    let sent = provider
        .send_message(home(port), None, text("early"), Some("early".into()))
        .await;
    assert!(matches!(sent, Err(RequestError::Failed(_))), "{sent:?}");
    assert!(matches!(queued(&mut events), ProviderEvent::Message {
        event: MessageEvent::NotSent { nonce, .. }, ..
    } if nonce == "early"));
    assert_eq!(provider.status(), ProviderStatus::Connecting);

    provider.shutdown().unwrap();
    timeout(Duration::from_secs(5), async {
        assert_eq!(received(&mut events).await.content, text("goodbye"));
        assert_eq!(exited(&mut events).await, Ok(()));
    })
    .await
    .expect("shutdown waited for the connection");
}

#[tokio::test]
async fn reconnecting_answers_commands_and_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (hang_up, hung_up) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(b"hello from server\n").await.unwrap();
        let _ = hung_up.await;
        // Nothing listens any more, so reconnecting fails and retries.
    });
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    hang_up.send(()).unwrap();
    assert!(matches!(
        status(&mut events).await,
        ProviderStatus::Disconnected { .. }
    ));
    assert_eq!(status(&mut events).await, ProviderStatus::Connecting);
    let page = timeout(
        Duration::from_secs(1),
        provider.history(home(port), None, 10),
    )
    .await
    .expect("history waited for the reconnect")
    .unwrap();
    assert_eq!(page.messages[0].content, text("hello from server"));
    provider.shutdown().unwrap();
    timeout(Duration::from_secs(1), async {
        assert_eq!(received(&mut events).await.content, text("goodbye"));
        assert_eq!(exited(&mut events).await, Ok(()));
    })
    .await
    .expect("shutdown waited for the reconnect");
}

#[tokio::test]
async fn each_lookup_shares_files_under_its_own_blob_ids() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let mut provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    let content = vec![file("f", MediaSource::Bytes(vec![1, 2, 3]))];
    let id = provider
        .send_message(home(port), None, content, None)
        .await
        .unwrap();
    let blob_of = |message: &Message| match &message.content[..] {
        [
            Content::File(Media {
                source: MediaSource::Blob(blob),
                ..
            }),
        ] => blob.clone(),
        other => panic!("expected a file, got {other:?}"),
    };
    let sent = blob_of(&received(&mut events).await);
    received(&mut events).await;

    // Lookups given up on release their own copies only.
    provider.set_timeout(Some(Duration::ZERO));
    for _ in 0..10 {
        let _ = provider.get_message(home(port), id.clone()).await;
        let _ = provider.history(home(port), None, 10).await;
    }
    provider.set_timeout(Some(REQUEST_TIMEOUT));
    assert_eq!(provider.read_blob(&sent, 0, 10).await, Ok(vec![1, 2, 3]));

    let looked_up = blob_of(&provider.get_message(home(port), id.clone()).await.unwrap());
    assert_ne!(looked_up, sent);
    provider.release_blob(&sent).unwrap();
    assert_eq!(
        provider.read_blob(&looked_up, 0, 10).await,
        Ok(vec![1, 2, 3])
    );
    provider.release_blob(&looked_up).unwrap();
    // History still holds the file, under yet another id.
    let page = provider.history(home(port), None, 10).await.unwrap();
    let again = blob_of(page.messages.iter().find(|m| m.id == id).unwrap());
    assert_eq!(provider.read_blob(&again, 0, 10).await, Ok(vec![1, 2, 3]));
    assert!(matches!(
        provider.read_blob(&sent, 0, 10).await,
        Err(RequestError::Failed(CommandError::UnknownBlob(_)))
    ));
}

#[tokio::test]
async fn empty_sends_are_refused_as_not_sent() {
    let port = start_server().await;
    let (orchestrator, _dir) = orchestrator().await;
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(spec(port), events_tx).unwrap();
    started(&mut events).await;

    let empty = Err(RequestError::Failed(Violation::Empty.into()));
    let not_sent = |events: &mut mpsc::Receiver<ProviderEvent>, expected: &str| {
        assert!(matches!(queued(events), ProviderEvent::Message {
            event: MessageEvent::NotSent { nonce, error, .. }, ..
        } if nonce == expected && error == CommandError::Rejected(Violation::Empty)));
    };
    let sent = provider
        .send_message(home(port), None, vec![], Some("e1".into()))
        .await;
    assert_eq!(sent, empty.clone().map(|()| String::new()));
    not_sent(&mut events, "e1");

    let channel = FunctionContext::Channel(home(port));
    let functions = provider.functions(channel.clone()).await.unwrap();
    let arguments = args([
        ("content", Value::Content(vec![])),
        ("nonce", Value::Text("e2".into())),
    ]);
    let called = provider
        .call_function(find(&functions, "send"), channel, arguments)
        .await;
    assert_eq!(called, empty.map(|()| Value::Null));
    not_sent(&mut events, "e2");
    // Nothing was written, so the next event is the goodbye.
    provider.shutdown().unwrap();
    assert_eq!(received(&mut events).await.content, text("goodbye"));
}

/// An echo provider for `server`, a name its orchestrator looks up.
fn looked_up(server: &str) -> ProviderSpec {
    ProviderSpec {
        name: "echo".into(),
        wasm: echo_wasm().clone(),
        settings: [("server".to_owned(), server.to_owned())].into(),
    }
}

#[tokio::test]
async fn looking_up_answers_commands_and_shutdown() {
    let (mut orchestrator, _dir) = orchestrator().await;
    let (started_tx, lookup_started) = std_mpsc::channel();
    let (release, stalled) = std_mpsc::channel();
    let stall = Stall {
        started: Mutex::new(started_tx),
        release: Mutex::new(stalled),
    };
    orchestrator.set_resolver(move |_| {
        stall.read_at(0, 0)?;
        Err(io::ErrorKind::NotFound.into())
    });
    let server = "stalled.test:1";
    let (events_tx, mut events) = mpsc::channel(16);
    let mut provider = orchestrator.spawn(looked_up(server), events_tx).unwrap();
    assert_eq!(status(&mut events).await, ProviderStatus::Connecting);
    tokio::task::spawn_blocking(move || lookup_started.recv_timeout(Duration::from_secs(120)))
        .await
        .unwrap()
        .expect("the lookup never started");

    let home = ChannelRef::new(Some(server), server);
    let limit = Duration::from_secs(5);
    let page = timeout(limit, provider.history(home.clone(), None, 10))
        .await
        .expect("history waited for the lookup")
        .unwrap();
    assert!(page.messages.is_empty());
    let completed = autocomplete(&provider, limit).await;
    assert!(
        matches!(&completed, Err(RequestError::Failed(CommandError::Provider { code, .. })) if code == "not_connected"),
        "{completed:?}"
    );
    // A request given up on is cancelled, and the next is still answered.
    provider.set_timeout(Some(Duration::from_nanos(1)));
    let given_up = provider.history(home.clone(), None, 10).await;
    assert!(matches!(given_up, Ok(_) | Err(RequestError::TimedOut)));
    provider.set_timeout(Some(REQUEST_TIMEOUT));
    let sent = timeout(
        limit,
        provider.send_message(home.clone(), None, text("early"), Some("early".into())),
    )
    .await
    .expect("the send waited for the lookup");
    assert!(matches!(sent, Err(RequestError::Failed(_))), "{sent:?}");
    assert!(matches!(queued(&mut events), ProviderEvent::Message {
        event: MessageEvent::NotSent { nonce, .. }, ..
    } if nonce == "early"));

    provider.shutdown().unwrap();
    timeout(limit, async {
        assert_eq!(received(&mut events).await.content, text("goodbye"));
        assert_eq!(exited(&mut events).await, Ok(()));
    })
    .await
    .expect("shutdown waited for the lookup");
    drop(release);
}

#[tokio::test]
async fn failed_lookups_are_retried() {
    let port = start_server().await;
    let server = format!("echo.test:{port}");
    let (mut orchestrator, _dir) = orchestrator().await;
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    orchestrator.set_resolver({
        let calls = calls.clone();
        let server = server.clone();
        move |name| {
            assert_eq!(name, server);
            match calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                0 | 1 => Err(io::Error::other("no such host")),
                _ => Ok(vec![([127, 0, 0, 1], port).into()]),
            }
        }
    });
    let (events_tx, mut events) = mpsc::channel(16);
    let provider = orchestrator.spawn(looked_up(&server), events_tx).unwrap();
    started(&mut events).await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);

    let home = ChannelRef::new(Some(&server), &server);
    let id = provider
        .send_message(home, None, text("hi"), None)
        .await
        .unwrap();
    assert_eq!(received(&mut events).await.id, id);
    assert_eq!(received(&mut events).await.content, text("echo: hi"));
    provider.shutdown().unwrap();
    assert_eq!(received(&mut events).await.content, text("goodbye"));
    assert_eq!(exited(&mut events).await, Ok(()));
}
