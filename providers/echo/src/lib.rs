//! Example provider that talks a line-based protocol over TCP.
//!
//! It connects to the `server` setting and reports the server as an
//! organization with one channel, named after the server, and two users: the
//! server and the account, `me`. The channel takes text and one file per
//! message, within limits. The provider emits the server's greeting, then for
//! each `Send`:
//!
//! - checks the content against the channel's limits,
//! - writes each line of text, and a line describing each file, to the server,
//! - once the server has answered every line, reports the sent message,
//!   sharing each file back as a blob,
//! - emits each of the server's replies as a reply to the sent message,
//! - then answers success, after those events on the host connection.
//!
//! On partial failure it emits every reply received, without a `reply_to`,
//! then answers with the count of confirmed lines and exits. The failed line
//! may have reached the server too. Both success and failure wait for event
//! queue space; callers must drain events on a separate task and use request
//! timeouts. After an answer, killing or dropping the handle cannot lose those
//! queued replies. A timeout or kill before the answer leaves delivery unknown
//! and can discard events. This does not provide durable delivery.
//!
//! `OpenChannel` with just the server finds the channel. On `Shutdown` it
//! emits a goodbye and returns.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::{SystemTime, UNIX_EPOCH};

use qmsg_sdk::{
    Channel, ChannelKind, ChannelRef, Command, CommandError, Content, ContentKind, ContentRule,
    Context, DirectoryUpdate, LogLevel, Media, MediaSource, Message, MessageLimits, Organization,
    Provider, Reply, Result, User, Value,
};

const ME: &str = "me";
mod functions;
const MAX_TEXT: u64 = 2000;
const MAX_FILE: u64 = 16 * 1024 * 1024;

struct Echo;

impl Provider for Echo {
    fn run(cx: &mut Context) -> Result {
        let server = cx.setting("server")?.to_owned();
        let mut conn = BufReader::new(TcpStream::connect(&server)?);
        cx.log(LogLevel::Info, format!("connected to {server}"));

        let channel = Channel {
            accepted_content: vec![
                ContentRule::new(ContentKind::Text).max_size(MAX_TEXT),
                ContentRule::new(ContentKind::File).max_size(MAX_FILE),
            ],
            limits: MessageLimits {
                max_attachments: Some(1),
                max_total_size: None,
            },
            ..Channel::new(&server, &server, ChannelKind::Text)
        };
        cx.directory(DirectoryUpdate::OrganizationUpserted(Organization {
            id: server.clone(),
            name: server.clone(),
            me: Some(ME.into()),
            users: vec![
                User {
                    id: server.clone(),
                    name: server.clone(),
                },
                User {
                    id: ME.into(),
                    name: "qmsg".into(),
                },
            ],
            channels: vec![channel.clone()],
        }))?;
        let mut messages = Messages {
            channel: ChannelRef::new(Some(&server), &server),
            next_id: 0,
        };

        let greeting = read_line(&mut conn)?;
        cx.emit(messages.make(&server, None, text(greeting)))?;

        while let Some(command) = cx.next_command(None)? {
            match command {
                command @ (Command::Functions { .. }
                | Command::Call { .. }
                | Command::Verify { .. }
                | Command::Complete { .. }) => {
                    functions::handle(cx, command, &server, &messages.channel)?;
                }
                Command::Send {
                    request,
                    channel: to,
                    reply_to,
                    content,
                } => {
                    if to != messages.channel {
                        cx.reply(request, Err(CommandError::UnknownChannel(to)))?;
                        continue;
                    }
                    if let Err(violation) = channel.check(&content) {
                        cx.reply(request, Err(violation.into()))?;
                        continue;
                    }
                    let (lines, sent_content) = match prepare(cx, content) {
                        Ok(prepared) => prepared,
                        Err(e) => {
                            cx.reply(request, Err(e))?;
                            continue;
                        }
                    };
                    // Deliver everything before answering, so success means
                    // the server has it all. It answers each line, so send
                    // them one at a time.
                    let mut replies = Vec::new();
                    for line in &lines {
                        let reply = writeln!(conn.get_mut(), "{line}")
                            .map_err(Into::into)
                            .and_then(|()| read_line(&mut conn));
                        match reply {
                            Ok(reply) => replies.push(reply),
                            Err(e) => {
                                let confirmed = replies.len();
                                let error = format!(
                                    "the server answered {} of {} lines: {e}",
                                    replies.len(),
                                    lines.len()
                                );
                                // The server did answer these, though the
                                // message they answer was never sent whole.
                                for reply in replies {
                                    cx.emit(messages.make(&server, None, text(reply)))?;
                                }
                                // Events precede the answer on the host connection.
                                // Thus a caller can kill/drop us after this failure
                                // without losing replies. A full event channel delays
                                // the answer: callers must drain it on another task
                                // and use request timeouts (not reorder the failure).
                                cx.reply(
                                    request,
                                    Err(CommandError::Provider {
                                        code: "send_incomplete".into(),
                                        message: error.clone(),
                                        details: Some(Box::new(Value::Record(
                                            [
                                                (
                                                    "confirmed_lines".into(),
                                                    Value::Integer(confirmed as i64),
                                                ),
                                                (
                                                    "total_lines".into(),
                                                    Value::Integer(lines.len() as i64),
                                                ),
                                                (
                                                    "last_line_delivery_unknown".into(),
                                                    Value::Boolean(true),
                                                ),
                                            ]
                                            .into(),
                                        ))),
                                    }),
                                )?;
                                // Without the server there is nothing left to do.
                                return Err(error.into());
                            }
                        }
                    }
                    let sent = messages.make(ME, reply_to, sent_content);
                    cx.emit(sent.clone())?;
                    for reply in replies {
                        cx.emit(messages.make(&server, Some(sent.id.clone()), text(reply)))?;
                    }
                    // Apply the same event-before-answer rule to success.
                    cx.reply(request, Ok(Reply::Sent { id: sent.id }))?;
                }
                Command::OpenChannel {
                    request,
                    organization,
                    members,
                } => {
                    let result = match (organization, members.iter().find(|m| **m != server)) {
                        // Every user is in the server's organization.
                        (Some(org), _) if org != server => {
                            Err(CommandError::UnknownOrganization(org))
                        }
                        (None, _) => Err(CommandError::UnknownUser(
                            members.first().cloned().unwrap_or_default(),
                        )),
                        (_, Some(member)) => Err(CommandError::UnknownUser(member.clone())),
                        (_, None) if members.is_empty() => {
                            Err(CommandError::Failed("no members".into()))
                        }
                        (_, None) => Ok(Reply::Opened(messages.channel.clone())),
                    };
                    cx.reply(request, result)?;
                }
                // Shared blobs are handled by `next_command`, and there are no
                // others.
                Command::ReadBlob { request, blob, .. } => {
                    cx.reply(request, Err(CommandError::UnknownBlob(blob)))?;
                }
                Command::ReleaseBlob { .. } => {}
                Command::Shutdown => {
                    cx.emit(messages.make(&server, None, text("goodbye".into())))?;
                    break;
                }
            }
        }
        Ok(())
    }
}

/// Returns the lines to send the server, and the content as the account sent
/// it, with files held here.
fn prepare(
    cx: &mut Context,
    content: Vec<Content>,
) -> std::result::Result<(Vec<String>, Vec<Content>), CommandError> {
    let mut lines = Vec::new();
    let mut sent = Vec::new();
    for (index, part) in (0u32..).zip(content) {
        match part {
            Content::Text(text) => {
                lines.extend(text.split('\n').map(str::to_owned));
                sent.push(Content::Text(text));
            }
            Content::File(media) => {
                let bytes = fetch(cx, index, &media)?;
                let name = media.name.clone().unwrap_or_default();
                lines.push(format!("file {name}: {} bytes", bytes.len()));
                sent.push(Content::File(Media {
                    size: Some(bytes.len() as u64),
                    source: MediaSource::Blob(cx.share(bytes)),
                    ..media
                }));
            }
            other => sent.push(other),
        }
    }
    Ok((lines, sent))
}

/// Reads a file the orchestrator sent as part `part`, whatever its source.
fn fetch(cx: &mut Context, part: u32, media: &Media) -> std::result::Result<Vec<u8>, CommandError> {
    match &media.source {
        MediaSource::Bytes(bytes) => Ok(bytes.clone()),
        MediaSource::Blob(blob) => {
            let mut bytes = Vec::new();
            // At most one more byte than allowed, to notice a wrong size.
            cx.blob_reader(blob.as_str())
                .take(MAX_FILE + 1)
                .read_to_end(&mut bytes)
                .map_err(|e| CommandError::Failed(e.to_string()))?;
            match bytes.len() as u64 {
                size if size > MAX_FILE => {
                    Err(CommandError::Rejected(qmsg_sdk::Violation::TooLarge {
                        part,
                        size,
                        max: MAX_FILE,
                    }))
                }
                _ => Ok(bytes),
            }
        }
        // The provider has no HTTP client.
        MediaSource::Url(_) => Err(CommandError::Unsupported),
    }
}

/// Makes the messages in the server's channel.
struct Messages {
    channel: ChannelRef,
    next_id: u64,
}

impl Messages {
    fn make(&mut self, author: &str, reply_to: Option<String>, content: Vec<Content>) -> Message {
        self.next_id += 1;
        Message {
            id: self.next_id.to_string(),
            channel: self.channel.clone(),
            author: author.to_owned(),
            sent_at: now(),
            reply_to,
            content,
        }
    }
}

fn text(text: String) -> Vec<Content> {
    vec![Content::Text(text)]
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn read_line(conn: &mut BufReader<TcpStream>) -> Result<String> {
    let mut line = String::new();
    if conn.read_line(&mut line)? == 0 {
        return Err("server closed the connection".into());
    }
    Ok(line.trim_end().to_owned())
}

qmsg_sdk::export_provider!(Echo);
