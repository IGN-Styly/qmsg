//! Example provider that talks a line-based protocol over TCP.
//!
//! It connects to the `server` setting and reports the server as an
//! organization with one text channel, named after the server. It then emits
//! the server's greeting, and for each `Send` command writes each line of the
//! text and emits each reply. On `Shutdown` it emits a goodbye and returns.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;

use qmsg_sdk::{
    Channel, ChannelKind, Command, Content, ContentKind, Context, DirectoryUpdate, LogLevel,
    Message, Organization, Provider, Result, User,
};

struct Echo;

impl Provider for Echo {
    fn run(cx: &mut Context) -> Result {
        let server = cx.setting("server")?.to_owned();
        let mut conn = BufReader::new(TcpStream::connect(&server)?);
        cx.log(LogLevel::Info, format!("connected to {server}"));

        cx.directory(DirectoryUpdate::OrganizationSet(Organization {
            id: server.clone(),
            name: server.clone(),
            users: vec![User {
                id: server.clone(),
                name: server.clone(),
            }],
            channels: vec![Channel {
                id: server.clone(),
                name: server.clone(),
                kind: ChannelKind::Text,
                inputs: vec![ContentKind::Text],
            }],
        }))?;
        let message = |channel: &str, text: String| Message {
            organization: Some(server.clone()),
            channel: channel.to_owned(),
            author: server.clone(),
            content: vec![Content::Text(text)],
        };

        let greeting = read_line(&mut conn)?;
        cx.emit(message(&server, greeting))?;

        while let Some(command) = cx.next_command(None)? {
            match command {
                // The server answers each line, so send the lines one at a
                // time to keep every reply with its command's channel.
                Command::Send {
                    channel, content, ..
                } => {
                    for part in content {
                        let Content::Text(text) = part else {
                            cx.log(
                                LogLevel::Warn,
                                format!("can't send {:?} to a line server", part.kind()),
                            );
                            continue;
                        };
                        for line in text.split('\n') {
                            writeln!(conn.get_mut(), "{line}")?;
                            let reply = read_line(&mut conn)?;
                            cx.emit(message(&channel, reply))?;
                        }
                    }
                }
                Command::Shutdown => {
                    cx.emit(message(&server, "goodbye".into()))?;
                    break;
                }
            }
        }
        Ok(())
    }
}

fn read_line(conn: &mut BufReader<TcpStream>) -> Result<String> {
    let mut line = String::new();
    if conn.read_line(&mut line)? == 0 {
        return Err("server closed the connection".into());
    }
    Ok(line.trim_end().to_owned())
}

qmsg_sdk::export_provider!(Echo);
