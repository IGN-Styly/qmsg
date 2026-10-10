//! Example provider that talks a line-based protocol over TCP.
//!
//! It connects to the `server` setting and reports the server as an
//! organization with one channel, named after the server, and two users: the
//! server and the account, `me`. The channel takes text, formatted text and
//! one file per message, within limits.
//!
//! The server answers each line it gets with `echo: <line>`, in order. A file
//! is a line `file <name>: <size> bytes` followed by that many bytes, which
//! the server reads before answering the line. Any other line from the
//! server, such as its greeting, is a message from the server, which can come
//! at any time.
//!
//! The provider never blocks on the server or the orchestrator: it connects,
//! reads files the orchestrator offers, and writes to the server without
//! waiting, and keeps each send as state to finish later. So it reports the
//! server's messages as they come, and answers commands, including
//! autocomplete and `Shutdown`, while a send is uploading or waiting for the
//! server, and while it looks up the server's name and connects. A lookup
//! that fails or times out is retried like a failed connection.
//!
//! For each `Send` it:
//!
//! - checks the content against the channel's limits,
//! - reads the files the orchestrator offers as blobs, one send at a time,
//! - checks the send is still wanted, the last moment it can be called off,
//! - writes each line of text, and each file, to the server,
//! - once the server has answered every line, reports the sent message, with
//!   the send's nonce and each file shared back as a blob,
//! - emits each of the server's answers as a reply to the sent message,
//! - then answers success, after those events on the host connection.
//!
//! A send that fails or is cancelled before writing anything is reported as
//! `NotSent` when it has a nonce. If the server goes away after, the provider
//! emits the answers it got, without a `reply_to`, then answers with the
//! count of confirmed lines; the rest may have reached the server too, so
//! their delivery is unknown. Then it reconnects: it resets and resends its
//! directory, and reports a `Gap`, since it can't tell what it missed.
//!
//! The runtime can't close a socket the server stopped reading while writes
//! are pending, even after a shutdown: it waits for the write it has taken,
//! and nothing calls that off. So such a connection stays open until the
//! provider exits, which closes it. After a few, the provider fails, rather
//! than keep more open.
//!
//! With a `password` setting, the account must log in first, with the
//! password and then the `code` setting. The session is kept as a secret, so
//! a restart doesn't ask again, until `logout`.
//!
//! The provider keeps its channel's history in memory, with its files, for
//! `History`, `GetMessage`, and edits, deletions, reactions and read markers
//! through functions. The server has no such things, so these change only the
//! provider's copy. Each time it returns a file, it shares it under a new
//! blob id, so releasing one never takes it from another reader. On
//! `Shutdown` it emits a goodbye and returns; sends the server hasn't
//! answered are not answered, so their delivery is unknown.

use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::fd::AsFd;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use qmsg_sdk::{
    Channel, ChannelKind, ChannelRef, Command, CommandError, Content, ContentKind, ContentRule,
    Context, DirectoryUpdate, HistoryPage, InputIssue, LogLevel, Media, MediaSource, Message,
    MessageEvent, MessageLimits, Organization, Provider, ProviderStatus, Reply, Result, Source,
    User, Value, inputs,
};

mod functions;

const ME: &str = "me";
const MAX_TEXT: u64 = 2000;
const MAX_FILE: u64 = 16 * 1024 * 1024;
/// Messages kept for history, oldest dropped first.
const MAX_HISTORY: usize = 1000;
/// Bytes of files kept for history, oldest messages dropped first.
const MAX_KEPT: usize = 64 * 1024 * 1024;
const SESSION: &str = "session";
const CONNECT_ATTEMPTS: u32 = 5;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Connections left open because the server stopped reading them, before
/// the provider fails so the runtime frees them.
const MAX_ABANDONED: usize = 4;

struct Echo;

impl Provider for Echo {
    fn run(cx: &mut Context) -> Result {
        let server = cx.setting("server")?.to_owned();
        let login = cx.config().settings.get("password").map(|password| {
            let code = cx.config().settings.get("code").cloned();
            (password.clone(), code.unwrap_or_default())
        });
        let mut history = History {
            channel: ChannelRef::new(Some(&server), &server),
            next_id: 0,
            messages: VecDeque::new(),
            kept: 0,
        };
        let mut synced = false;
        let mut abandoned = 0;
        cx.status(ProviderStatus::Connecting)?;
        loop {
            let Some(conn) = connect(cx, &server, &mut history)? else {
                return Ok(());
            };
            match &login {
                Some((password, code)) if cx.secret_get(SESSION)?.is_none() => {
                    if !functions::log_in(cx, password, code)? {
                        return Ok(());
                    }
                }
                _ => cx.status(ProviderStatus::Syncing)?,
            }
            let mut session = Session {
                server: server.clone(),
                channel: channel(&server),
                conn: BufReader::new(conn),
                partial: Vec::new(),
                out: VecDeque::new(),
                uploads: VecDeque::new(),
                sends: VecDeque::new(),
                history: &mut history,
                login: login.is_some(),
            };
            session.sync(cx, synced)?;
            synced = true;
            let end = session.run(cx)?;
            if !session.close(cx)? {
                abandoned += 1;
            }
            match end {
                End::Shutdown => return Ok(()),
                _ if abandoned == MAX_ABANDONED => {
                    let error = format!("{MAX_ABANDONED} connections stopped being read");
                    cx.status(ProviderStatus::Failed(error.clone()))?;
                    return Err(error.into());
                }
                End::LoggedOut => {}
                End::Lost(reason) => {
                    cx.log(LogLevel::Warn, &reason);
                    cx.status(ProviderStatus::Disconnected { reason })?;
                    cx.status(ProviderStatus::Connecting)?;
                }
            }
        }
    }
}

/// Connects, trying a few times before giving up for good, and answering
/// commands meanwhile. `None` on shutdown.
fn connect(cx: &mut Context, server: &str, history: &mut History) -> Result<Option<TcpStream>> {
    let mut attempt = 0;
    loop {
        let error = match try_connect(cx, server, history)? {
            Connected::Conn(conn) => {
                cx.log(LogLevel::Info, format!("connected to {server}"));
                return Ok(Some(conn));
            }
            Connected::Shutdown => return Ok(None),
            Connected::Failed(error) => error,
        };
        attempt += 1;
        if attempt == CONNECT_ATTEMPTS {
            cx.status(ProviderStatus::Failed(error.to_string()))?;
            return Err(error.into());
        }
        cx.log(LogLevel::Warn, format!("connecting to {server}: {error}"));
        let retry = Instant::now() + Duration::from_millis(100 << attempt);
        if wait_offline(cx, history, &[], retry)?.is_none() {
            return Ok(None);
        }
    }
}

enum Connected {
    Conn(TcpStream),
    Shutdown,
    Failed(io::Error),
}

fn try_connect(cx: &mut Context, server: &str, history: &mut History) -> Result<Connected> {
    let addrs = match server.parse::<SocketAddr>() {
        Ok(addr) => vec![addr],
        Err(_) => match look_up(cx, server, history)? {
            None => return Ok(Connected::Shutdown),
            Some(Ok(addrs)) => addrs,
            Some(Err(e)) => return Ok(Connected::Failed(io::Error::other(e))),
        },
    };
    let mut error = io::Error::new(io::ErrorKind::NotFound, "no address");
    for addr in addrs {
        let conn = match qmsg_sdk::start_connect(addr) {
            Ok(conn) => conn,
            Err(e) => {
                error = e;
                continue;
            }
        };
        let deadline = Instant::now() + CONNECT_TIMEOUT;
        match wait_offline(cx, history, &[Source::write(conn.as_fd())], deadline)? {
            None => return Ok(Connected::Shutdown),
            Some(false) => error = io::Error::new(io::ErrorKind::TimedOut, "timed out"),
            Some(true) => {
                let connected = conn
                    .take_error()
                    .and_then(|e| e.map_or(Ok(()), Err))
                    .and_then(|()| conn.peer_addr().map(drop));
                match connected {
                    Ok(()) => return Ok(Connected::Conn(conn)),
                    Err(e) => error = e,
                }
            }
        }
    }
    Ok(Connected::Failed(error))
}

/// Looks `server` up, answering commands without a server meanwhile. The
/// orchestrator always answers, if only with a timeout. `None` on shutdown.
fn look_up(
    cx: &mut Context,
    server: &str,
    history: &mut History,
) -> Result<Option<std::result::Result<Vec<SocketAddr>, String>>> {
    cx.start_lookup(server)?;
    loop {
        if let Some(result) = cx.take_lookup(server)? {
            return Ok(Some(result));
        }
        while let Some(command) = cx.next_command(Some(Duration::ZERO))? {
            if offline(cx, history, command)? {
                cx.forget_lookup(server);
                return Ok(None);
            }
        }
        cx.wait(&[], None)?;
    }
}

/// Answers commands without a server until `deadline` or one of `sources` is
/// ready, which it returns. `None` on shutdown.
fn wait_offline(
    cx: &mut Context,
    history: &mut History,
    sources: &[Source<'_>],
    deadline: Instant,
) -> Result<Option<bool>> {
    loop {
        while let Some(command) = cx.next_command(Some(Duration::ZERO))? {
            if offline(cx, history, command)? {
                return Ok(None);
            }
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Ok(Some(false));
        }
        if cx.wait(sources, Some(left))?.sources.contains(&true) {
            return Ok(Some(true));
        }
    }
}

/// Answers a command without a server, returning whether to shut down.
fn offline(cx: &mut Context, history: &mut History, command: Command) -> Result<bool> {
    match command {
        Command::Shutdown => {
            let server = history.channel.channel.clone();
            let goodbye = history.make(cx, &server, None, text("goodbye".into()), None, vec![]);
            cx.emit(goodbye)?;
            return Ok(true);
        }
        command @ (Command::History { .. } | Command::GetMessage { .. }) => {
            history.answer(cx, command)?;
        }
        Command::ReleaseBlob { .. } | Command::Cancel { .. } => {}
        command => {
            let error = CommandError::Provider {
                code: "not_connected".into(),
                message: "not connected to the server".into(),
                details: None,
            };
            refuse(cx, &command, error)?;
        }
    }
    Ok(false)
}

/// Answers a command with `error`, reporting a send with a nonce as
/// `NotSent`, since it never reached the server.
fn refuse(cx: &mut Context, command: &Command, error: CommandError) -> Result {
    let Some(request) = command.request() else {
        return Ok(());
    };
    if let Some((channel, nonce)) = unsent(command) {
        cx.emit(MessageEvent::NotSent {
            channel,
            nonce,
            error: error.clone(),
        })?;
    }
    cx.reply(request, Err(error))
}

/// The channel and nonce of a send, by command or by the `send` function.
fn unsent(command: &Command) -> Option<(ChannelRef, String)> {
    match command {
        Command::Send {
            channel,
            nonce: Some(nonce),
            ..
        } => Some((channel.clone(), nonce.clone())),
        Command::Call { call, .. } if call.function == functions::SEND => {
            match (&call.context, call.arguments.get(inputs::NONCE)) {
                (qmsg_sdk::FunctionContext::Channel(channel), Some(Value::Text(nonce))) => {
                    Some((channel.clone(), nonce.clone()))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn channel(server: &str) -> Channel {
    Channel {
        accepted_content: vec![
            ContentRule::new(ContentKind::Text).max_size(MAX_TEXT),
            ContentRule::new(ContentKind::Formatted),
            ContentRule::new(ContentKind::File).max_size(MAX_FILE),
        ],
        limits: MessageLimits {
            max_attachments: Some(1),
            max_total_size: None,
        },
        ..Channel::new(server, server, ChannelKind::Text)
    }
}

/// Why a session with the server ended.
enum End {
    Shutdown,
    LoggedOut,
    Lost(String),
}

/// How a send is answered: `Command::Send`, or a `SendMessage` function.
#[derive(Clone, Copy)]
enum Answer {
    Sent,
    Value,
}

/// A send not written yet.
struct Upload {
    send: Outgoing,
    /// Its files read so far, in order.
    files: Vec<Arc<Vec<u8>>>,
    /// The blob of the file being read and what has arrived, whose next
    /// read is started.
    reading: Option<(String, Vec<u8>)>,
}

impl Upload {
    /// Reads the rest of its files without waiting: `Ok(true)` once all
    /// have arrived.
    fn fetch(&mut self, cx: &mut Context) -> Result<std::result::Result<bool, CommandError>> {
        loop {
            let mut files = (0u32..)
                .zip(&self.send.content)
                .filter_map(|(i, part)| match part {
                    Content::File(media) => Some((i, media)),
                    _ => None,
                });
            let Some((part, media)) = files.nth(self.files.len()) else {
                return Ok(Ok(true));
            };
            let blob = match &media.source {
                MediaSource::Bytes(bytes) => {
                    self.files.push(Arc::new(bytes.clone()));
                    continue;
                }
                // The provider has no HTTP client.
                MediaSource::Url(_) => return Ok(Err(CommandError::Unsupported)),
                MediaSource::Blob(blob) => blob,
            };
            // At most one more byte than allowed, to notice a wrong size.
            let ask = |have: usize| {
                (MAX_FILE + 1 - have as u64).min(qmsg_sdk::types::MAX_READ as u64) as u32
            };
            let Some((_, bytes)) = &mut self.reading else {
                if let Err(e) = cx.start_read(blob, 0, ask(0)) {
                    return Ok(Err(CommandError::Failed(e.to_string())));
                }
                self.reading = Some((blob.clone(), Vec::new()));
                return Ok(Ok(false));
            };
            let asked = ask(bytes.len()) as usize;
            let chunk = match cx.take_read(blob, bytes.len() as u64)? {
                None => return Ok(Ok(false)),
                Some(Ok(chunk)) => chunk,
                Some(Err(e)) => {
                    self.reading = None;
                    return Ok(Err(CommandError::Failed(e)));
                }
            };
            let done = chunk.len() < asked;
            bytes.extend_from_slice(&chunk);
            let size = bytes.len() as u64;
            if size > MAX_FILE {
                self.reading = None;
                let too_large = qmsg_sdk::Violation::TooLarge {
                    part,
                    size,
                    max: MAX_FILE,
                };
                return Ok(Err(CommandError::Rejected(too_large)));
            }
            if done {
                let (_, bytes) = self.reading.take().expect("read above");
                self.files.push(Arc::new(bytes));
                continue;
            }
            if let Err(e) = cx.start_read(blob, size, ask(bytes.len())) {
                self.reading = None;
                return Ok(Err(CommandError::Failed(e.to_string())));
            }
            return Ok(Ok(false));
        }
    }

    /// Calls it off before anything is written.
    fn refuse(self, cx: &mut Context, error: CommandError) -> Result {
        if let Some((blob, bytes)) = &self.reading {
            cx.forget_read(blob, bytes.len() as u64);
        }
        let send = self.send;
        if let Some(nonce) = send.nonce {
            cx.emit(MessageEvent::NotSent {
                channel: send.channel,
                nonce,
                error: error.clone(),
            })?;
        }
        cx.reply(send.request, Err(error))
    }
}

/// A send written to the server, waiting for its answers.
struct PendingSend {
    request: u64,
    answer: Answer,
    reply_to: Option<String>,
    nonce: Option<String>,
    content: Vec<Content>,
    files: Vec<Arc<Vec<u8>>>,
    lines: usize,
    replies: Vec<String>,
}

/// One connection to the server.
struct Session<'a> {
    server: String,
    channel: Channel,
    /// Non-blocking.
    conn: BufReader<TcpStream>,
    /// The start of a line that hasn't fully arrived.
    partial: Vec<u8>,
    /// What is still to be written, in order, with how much of each is.
    out: VecDeque<(Arc<Vec<u8>>, usize)>,
    /// Sends not written yet, oldest first. Only the first reads its files.
    uploads: VecDeque<Upload>,
    /// Sends written, oldest first, as the server answers.
    sends: VecDeque<PendingSend>,
    history: &'a mut History,
    login: bool,
}

impl Session<'_> {
    /// Reports the directory from scratch, then that the provider is ready.
    fn sync(&mut self, cx: &mut Context, reconnected: bool) -> Result {
        cx.directory(DirectoryUpdate::Reset)?;
        cx.directory(DirectoryUpdate::OrganizationUpserted(Organization {
            id: self.server.clone(),
            name: self.server.clone(),
            me: Some(ME.into()),
            users: vec![
                User {
                    id: self.server.clone(),
                    name: self.server.clone(),
                },
                User {
                    id: ME.into(),
                    name: "qmsg".into(),
                },
            ],
            channels: vec![self.channel.clone()],
        }))?;
        if reconnected {
            // Whatever the server said while we were away is lost.
            cx.emit(MessageEvent::Gap {
                channel: self.history.channel.clone(),
            })?;
        }
        cx.status(ProviderStatus::Ready)
    }

    fn run(&mut self, cx: &mut Context) -> Result<End> {
        loop {
            let (lines, closed) = match read_lines(&mut self.conn, &mut self.partial) {
                Ok(read) => read,
                Err(e) => return self.lost(cx, e.to_string()),
            };
            for line in lines {
                self.line(cx, line)?;
            }
            if closed {
                return self.lost(cx, "the server closed the connection".into());
            }
            while let Some(command) = cx.next_command(Some(Duration::ZERO))? {
                if let Some(end) = self.command(cx, command)? {
                    return Ok(end);
                }
            }
            self.upload(cx)?;
            if let Err(e) = self.flush() {
                return self.lost(cx, e.to_string());
            }
            // `read_lines` emptied the buffer, so the socket says it all.
            let conn = self.conn.get_ref().as_fd();
            let mut sources = vec![Source::read(conn)];
            if !self.out.is_empty() {
                sources.push(Source::write(conn));
            }
            cx.wait(&sources, None)?;
        }
    }

    /// Handles a line from the server.
    fn line(&mut self, cx: &mut Context, line: String) -> Result {
        let Some(send) = self
            .sends
            .front_mut()
            .filter(|_| line.starts_with("echo: "))
        else {
            let message = self
                .history
                .make(cx, &self.server, None, text(line), None, vec![]);
            return cx.emit(message);
        };
        send.replies.push(line);
        if send.replies.len() < send.lines {
            return Ok(());
        }
        let send = self.sends.pop_front().expect("found above");
        let sent = self
            .history
            .make(cx, ME, send.reply_to, send.content, send.nonce, send.files);
        let id = sent.id.clone();
        cx.emit(sent)?;
        for reply in send.replies {
            let reply = self.history.make(
                cx,
                &self.server,
                Some(id.clone()),
                text(reply),
                None,
                vec![],
            );
            cx.emit(reply)?;
        }
        // Events come before the answer on the host connection.
        let reply = match send.answer {
            Answer::Sent => Reply::Sent { id },
            Answer::Value => Reply::Value(Value::Text(id)),
        };
        cx.reply(send.request, Ok(reply))
    }

    /// Fails the sends still waiting, and ends the session.
    fn lost(&mut self, cx: &mut Context, reason: String) -> Result<End> {
        self.fail_sends(cx, &reason)?;
        Ok(End::Lost(reason))
    }

    fn fail_sends(&mut self, cx: &mut Context, reason: &str) -> Result {
        // Nothing of these was written.
        for upload in self.uploads.drain(..) {
            let error = CommandError::Provider {
                code: "not_connected".into(),
                message: reason.into(),
                details: None,
            };
            upload.refuse(cx, error)?;
        }
        self.out.clear();
        for send in self.sends.drain(..) {
            let confirmed = send.replies.len();
            // The server did answer these, though the message they answer
            // was never sent whole.
            for reply in send.replies {
                let reply = self
                    .history
                    .make(cx, &self.server, None, text(reply), None, vec![]);
                cx.emit(reply)?;
            }
            // Events precede the answer, so a caller can kill or drop the
            // provider after this failure without losing them.
            let error = CommandError::Provider {
                code: "send_incomplete".into(),
                message: format!(
                    "the server answered {confirmed} of {} lines: {reason}",
                    send.lines
                ),
                details: Some(Box::new(Value::Record(
                    [
                        ("confirmed_lines".into(), Value::Integer(confirmed as i64)),
                        ("total_lines".into(), Value::Integer(send.lines as i64)),
                        // The other lines may have reached the server.
                        ("delivery_unknown".into(), Value::Boolean(true)),
                    ]
                    .into(),
                ))),
            };
            cx.reply(send.request, Err(error))?;
        }
        Ok(())
    }

    /// Handles a command, returning how the session ends, if it does.
    fn command(&mut self, cx: &mut Context, command: Command) -> Result<Option<End>> {
        match command {
            command @ (Command::Functions { .. }
            | Command::Call { .. }
            | Command::Verify { .. }
            | Command::Complete { .. }) => return functions::handle(self, cx, command),
            Command::Send {
                request,
                channel,
                reply_to,
                content,
                nonce,
            } => {
                let send = Outgoing {
                    request,
                    answer: Answer::Sent,
                    channel,
                    reply_to,
                    content,
                    nonce,
                };
                self.send(cx, send)?;
            }
            command @ (Command::History { .. } | Command::GetMessage { .. }) => {
                self.history.answer(cx, command)?;
            }
            Command::OpenChannel {
                request,
                organization,
                members,
            } => {
                let result = self.open_channel(organization.as_deref(), &members);
                cx.reply(request, result.map(Reply::Opened))?;
            }
            // Shared blobs and cancellations are handled by the SDK, and
            // there are no other blobs.
            Command::ReadBlob { request, blob, .. } => {
                cx.reply(request, Err(CommandError::UnknownBlob(blob)))?;
            }
            Command::ReleaseBlob { .. } | Command::Cancel { .. } => {}
            Command::Shutdown => {
                for upload in self.uploads.drain(..) {
                    upload.refuse(cx, CommandError::Cancelled)?;
                }
                let goodbye =
                    self.history
                        .make(cx, &self.server, None, text("goodbye".into()), None, vec![]);
                cx.emit(goodbye)?;
                return Ok(Some(End::Shutdown));
            }
        }
        Ok(None)
    }

    fn open_channel(
        &self,
        organization: Option<&str>,
        members: &[String],
    ) -> std::result::Result<ChannelRef, CommandError> {
        let server = self.server.as_str();
        match (organization, members.iter().find(|m| *m != server)) {
            // Every user is in the server's organization.
            (Some(org), _) if org != server => Err(CommandError::UnknownOrganization(org.into())),
            (None, _) => Err(CommandError::UnknownUser(
                members.first().cloned().unwrap_or_default(),
            )),
            (_, Some(member)) => Err(CommandError::UnknownUser(member.clone())),
            (_, None) if members.is_empty() => Err(CommandError::Failed("no members".into())),
            (_, None) => Ok(self.history.channel.clone()),
        }
    }

    /// Starts a send. It is written in [`Session::upload`] once its files
    /// have arrived, and finishes in [`Session::line`] once the server has
    /// answered every line.
    fn send(&mut self, cx: &mut Context, send: Outgoing) -> Result {
        let refused = if send.channel != self.history.channel {
            Some(CommandError::UnknownChannel(send.channel.clone()))
        } else {
            // This refuses empty content too, so every send has a line.
            self.channel.check(&send.content).err().map(Into::into)
        };
        let upload = Upload {
            send,
            files: Vec::new(),
            reading: None,
        };
        match refused {
            Some(error) => upload.refuse(cx, error),
            None => {
                self.uploads.push_back(upload);
                Ok(())
            }
        }
    }

    /// Moves sends along: reads the first one's files, and writes it once
    /// they are here and the server has taken what came before. Sends no
    /// longer awaited are called off unless written.
    fn upload(&mut self, cx: &mut Context) -> Result {
        let mut i = 0;
        while i < self.uploads.len() {
            if cx.is_cancelled(self.uploads[i].send.request)? {
                let upload = self.uploads.remove(i).expect("in range");
                upload.refuse(cx, CommandError::Cancelled)?;
            } else {
                i += 1;
            }
        }
        while let Some(upload) = self.uploads.front_mut() {
            match upload.fetch(cx)? {
                Ok(true) => {}
                Ok(false) => return Ok(()),
                Err(error) => {
                    let upload = self.uploads.pop_front().expect("found above");
                    upload.refuse(cx, error)?;
                    continue;
                }
            }
            // One at a time, so at most one file waits to be written.
            if !self.out.is_empty() {
                return Ok(());
            }
            let upload = self.uploads.pop_front().expect("found above");
            // The last moment it can still be called off.
            if cx.is_cancelled(upload.send.request)? {
                upload.refuse(cx, CommandError::Cancelled)?;
                continue;
            }
            self.write(upload);
        }
        Ok(())
    }

    /// Queues a send's lines and files to be written, as the account sent it.
    fn write(&mut self, upload: Upload) {
        let Upload { send, files, .. } = upload;
        let mut lines = 0;
        let mut line = |out: &mut VecDeque<_>, line: &str| {
            out.push_back((Arc::new(format!("{line}\n").into_bytes()), 0));
            lines += 1;
        };
        let mut content = Vec::new();
        let mut bytes = files.iter();
        for part in send.content {
            match part {
                Content::Text(text) => {
                    text.split('\n').for_each(|l| line(&mut self.out, l));
                    content.push(Content::Text(text));
                }
                // The server only takes plain text.
                Content::Formatted(formatted) => {
                    formatted
                        .text
                        .split('\n')
                        .for_each(|l| line(&mut self.out, l));
                    content.push(Content::Formatted(formatted));
                }
                Content::File(media) => {
                    let file = bytes.next().expect("fetched").clone();
                    let name = media.name.clone().unwrap_or_default().replace('\n', " ");
                    line(&mut self.out, &format!("file {name}: {} bytes", file.len()));
                    let size = file.len() as u64;
                    self.out.push_back((file, 0));
                    content.push(Content::File(Media {
                        size: Some(size),
                        // Shared under a new id each time it is shown.
                        source: MediaSource::Blob(String::new()),
                        ..media
                    }));
                }
                other => content.push(other),
            }
        }
        self.sends.push_back(PendingSend {
            request: send.request,
            answer: send.answer,
            reply_to: send.reply_to,
            nonce: send.nonce,
            content,
            files,
            lines,
            replies: Vec::new(),
        });
    }

    /// Closes the connection, returning whether it did. The runtime finishes
    /// writes it has taken before it closes a socket, even after a shutdown,
    /// which never happens if the server stopped reading, so a connection
    /// still writing is left open instead, until the provider exits.
    fn close(self, cx: &mut Context) -> Result<bool> {
        let conn = self.conn.into_inner();
        let wake = cx.wait(&[Source::write(conn.as_fd())], Some(Duration::ZERO))?;
        if !wake.sources[0] {
            std::mem::forget(conn);
        }
        Ok(wake.sources[0])
    }

    /// Writes what the server takes now, without waiting.
    fn flush(&mut self) -> io::Result<()> {
        while let Some((bytes, written)) = self.out.front_mut() {
            match self.conn.get_mut().write(&bytes[*written..]) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => {
                    *written += n;
                    if *written == bytes.len() {
                        self.out.pop_front();
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

/// A send to start, from `Command::Send` or a `SendMessage` function.
struct Outgoing {
    request: u64,
    answer: Answer,
    channel: ChannelRef,
    reply_to: Option<String>,
    content: Vec<Content>,
    nonce: Option<String>,
}

/// Reads the lines that have arrived, without blocking, and whether the
/// server closed the connection.
fn read_lines(
    conn: &mut BufReader<TcpStream>,
    partial: &mut Vec<u8>,
) -> io::Result<(Vec<String>, bool)> {
    let mut lines = Vec::new();
    loop {
        match conn.read_until(b'\n', partial) {
            Ok(0) => return Ok((lines, true)),
            Ok(_) if partial.ends_with(b"\n") => {
                let line = String::from_utf8_lossy(partial);
                lines.push(line.trim_end().to_owned());
                partial.clear();
            }
            // The last line ended without a newline.
            Ok(_) => return Ok((lines, true)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok((lines, false)),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}

/// A message in history, with the files it holds, in order.
struct Kept {
    message: Message,
    files: Vec<Arc<Vec<u8>>>,
}

/// The messages in the server's channel, oldest first.
struct History {
    channel: ChannelRef,
    next_id: u64,
    messages: VecDeque<Kept>,
    /// Bytes of files kept.
    kept: usize,
}

impl History {
    /// Makes a new message and keeps it, with `files` for its file parts,
    /// returning it as shown.
    fn make(
        &mut self,
        cx: &mut Context,
        author: &str,
        reply_to: Option<String>,
        content: Vec<Content>,
        nonce: Option<String>,
        files: Vec<Arc<Vec<u8>>>,
    ) -> Message {
        self.next_id += 1;
        let message = Message {
            reply_to,
            nonce,
            ..Message::new(
                self.next_id.to_string(),
                self.channel.clone(),
                author,
                now(),
                content,
            )
        };
        self.kept += files.iter().map(|f| f.len()).sum::<usize>();
        self.messages.push_back(Kept { message, files });
        while self.messages.len() > MAX_HISTORY || (self.kept > MAX_KEPT && self.messages.len() > 1)
        {
            self.remove(0);
        }
        show(cx, self.messages.back().expect("just added"))
    }

    fn position(&self, id: &str) -> Option<usize> {
        self.messages.iter().position(|m| m.message.id == id)
    }

    fn get(&self, id: &str) -> Option<&Message> {
        self.position(id).map(|i| &self.messages[i].message)
    }

    fn message_mut(&mut self, position: usize) -> &mut Message {
        &mut self.messages[position].message
    }

    /// Replaces a message's content with text, dropping its files.
    fn set_text(&mut self, position: usize, content: Vec<Content>) {
        let kept = &mut self.messages[position];
        self.kept -= kept.files.drain(..).map(|f| f.len()).sum::<usize>();
        kept.message.content = content;
    }

    fn remove(&mut self, position: usize) {
        if let Some(kept) = self.messages.remove(position) {
            self.kept -= kept.files.iter().map(|f| f.len()).sum::<usize>();
        }
    }

    /// Answers `History` or `GetMessage`.
    fn answer(&self, cx: &mut Context, command: Command) -> Result {
        match command {
            Command::History {
                request,
                channel,
                cursor,
                limit,
            } => {
                let page = match channel == self.channel {
                    true => self.page(cx, cursor.as_deref(), limit),
                    false => Err(CommandError::UnknownChannel(channel)),
                };
                cx.reply(request, page.map(Reply::History))
            }
            Command::GetMessage {
                request,
                channel,
                id,
            } => {
                let message = match channel == self.channel {
                    true => match self.position(&id) {
                        Some(i) => Ok(show(cx, &self.messages[i])),
                        None => Err(CommandError::UnknownMessage(id)),
                    },
                    false => Err(CommandError::UnknownChannel(channel)),
                };
                cx.reply(request, message.map(Reply::Message))
            }
            _ => unreachable!("only history commands"),
        }
    }

    /// Up to `limit` messages before `cursor`, the id of the oldest message
    /// of the previous page.
    fn page(
        &self,
        cx: &mut Context,
        cursor: Option<&str>,
        limit: u32,
    ) -> std::result::Result<HistoryPage, CommandError> {
        let end = match cursor {
            None => self.messages.len(),
            Some(cursor) => {
                let cursor: u64 = cursor.parse().map_err(|_| {
                    CommandError::InvalidInput(vec![InputIssue {
                        input: None,
                        code: "bad_cursor".into(),
                        message: "Invalid history cursor".into(),
                    }])
                })?;
                self.messages
                    .partition_point(|m| m.message.id.parse::<u64>().is_ok_and(|id| id < cursor))
            }
        };
        let start = end.saturating_sub(limit as usize);
        let messages: Vec<_> = self
            .messages
            .range(start..end)
            .map(|kept| show(cx, kept))
            .collect();
        let next_cursor = (start > 0 && !messages.is_empty()).then(|| messages[0].id.clone());
        Ok(HistoryPage {
            messages,
            next_cursor,
        })
    }
}

/// A kept message as the orchestrator sees it, its files shared under new
/// blob ids that belong to this copy alone.
fn show(cx: &mut Context, kept: &Kept) -> Message {
    let mut message = kept.message.clone();
    let mut files = kept.files.iter();
    for part in &mut message.content {
        if let Content::File(media) = part
            && let Some(file) = files.next()
        {
            media.source = MediaSource::Blob(cx.share(file.clone()));
        }
    }
    message
}

fn text(text: String) -> Vec<Content> {
    vec![Content::Text(text)]
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

qmsg_sdk::export_provider!(Echo);
