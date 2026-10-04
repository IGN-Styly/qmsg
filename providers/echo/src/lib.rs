//! Example provider that talks a line-based protocol over TCP.
//!
//! It connects to the `server` setting and reports the server as an
//! organization with one channel, named after the server, and two users: the
//! server and the account, `me`. The channel takes text and one file per
//! message, within limits. The provider emits the server's greeting, then for
//! each `Send`:
//!
//! - checks the content against the channel's limits,
//! - reports the sent message, sharing each file back as a blob,
//! - writes each line of text, and a line describing each file, to the server,
//! - and emits each of the server's replies as a reply to the sent message.
//!
//! `OpenChannel` with just the server finds the channel. On `Shutdown` it
//! emits a goodbye and returns.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::{SystemTime, UNIX_EPOCH};

use qmsg_sdk::{
    Channel, ChannelKind, ChannelRef, Command, CommandError, Content, ContentKind, ContentRule,
    Context, DirectoryUpdate, LogLevel, Media, MediaSource, Message, MessageLimits, Organization,
    Provider, Reply, Result, User,
};

const ME: &str = "me";
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
                    let sent = messages.make(ME, reply_to, sent_content);
                    cx.reply(
                        request,
                        Ok(Reply::Sent {
                            id: sent.id.clone(),
                        }),
                    )?;
                    cx.emit(sent.clone())?;
                    // The server answers each line, so send them one at a
                    // time.
                    for line in lines {
                        writeln!(conn.get_mut(), "{line}")?;
                        let reply = read_line(&mut conn)?;
                        cx.emit(messages.make(&server, Some(sent.id.clone()), text(reply)))?;
                    }
                }
                Command::OpenChannel {
                    request, members, ..
                } => {
                    let result = match members.iter().find(|m| **m != server) {
                        None if !members.is_empty() => Ok(Reply::Opened(messages.channel.clone())),
                        Some(member) => Err(CommandError::UnknownUser(member.clone())),
                        None => Err(CommandError::Failed("no members".into())),
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
