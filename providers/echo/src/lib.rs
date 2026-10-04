//! Example provider that talks a line-based protocol over TCP.
//!
//! It connects to the `server` setting and reports the server as an
//! organization with one text channel, named after the server, and two users:
//! the server and the account, `me`. It emits the server's greeting, then for
//! each `Send` command reports the sent message, writes each line of its text
//! and emits each reply as a reply to it. On `Shutdown` it emits a goodbye and
//! returns.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::{SystemTime, UNIX_EPOCH};

use qmsg_sdk::{
    Channel, ChannelKind, ChannelRef, Command, Content, ContentKind, Context, DirectoryUpdate,
    LogLevel, Message, Organization, Provider, Result, User,
};

const ME: &str = "me";

struct Echo;

impl Provider for Echo {
    fn run(cx: &mut Context) -> Result {
        let server = cx.setting("server")?.to_owned();
        let mut conn = BufReader::new(TcpStream::connect(&server)?);
        cx.log(LogLevel::Info, format!("connected to {server}"));

        let channel = Channel {
            accepted_content: vec![ContentKind::Text],
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
                        let error = format!("no channel `{}` in {to:?}", to.channel);
                        cx.sent(request, Err(error))?;
                        continue;
                    }
                    if let Some(part) = content.iter().find(|part| !channel.accepts(part)) {
                        let error = format!("the channel can't take {:?}", part.kind());
                        cx.sent(request, Err(error))?;
                        continue;
                    }
                    let sent = messages.make(ME, reply_to, content);
                    cx.sent(request, Ok(sent.id.clone()))?;
                    cx.emit(sent.clone())?;
                    // The server answers each line, so send the lines one at
                    // a time.
                    for part in &sent.content {
                        let Content::Text(text_part) = part else {
                            continue;
                        };
                        for line in text_part.split('\n') {
                            writeln!(conn.get_mut(), "{line}")?;
                            let reply = read_line(&mut conn)?;
                            cx.emit(messages.make(&server, Some(sent.id.clone()), text(reply)))?;
                        }
                    }
                }
                Command::Shutdown => {
                    cx.emit(messages.make(&server, None, text("goodbye".into())))?;
                    break;
                }
            }
        }
        Ok(())
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
