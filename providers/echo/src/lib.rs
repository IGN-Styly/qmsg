//! Example provider that talks a line-based protocol over TCP.
//!
//! It connects to the `server` setting, emits the server's greeting, then for
//! each `Send` command writes each line of the body and emits each reply. On
//! `Shutdown` it emits a goodbye and returns.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;

use qmsg_sdk::{Command, Context, LogLevel, Message, Provider, Result};

struct Echo;

impl Provider for Echo {
    fn run(cx: &mut Context) -> Result {
        let server = cx.setting("server")?.to_owned();
        let mut conn = BufReader::new(TcpStream::connect(&server)?);
        cx.log(LogLevel::Info, format!("connected to {server}"));

        let greeting = read_line(&mut conn)?;
        cx.emit(Message {
            chat: server.clone(),
            author: server.clone(),
            body: greeting,
        })?;

        while let Some(command) = cx.next_command(None)? {
            match command {
                // The server answers each line, so send the lines one at a
                // time to keep every reply with its command's chat.
                Command::Send { chat, body } => {
                    for line in body.split('\n') {
                        writeln!(conn.get_mut(), "{line}")?;
                        let reply = read_line(&mut conn)?;
                        cx.emit(Message {
                            chat: chat.clone(),
                            author: server.clone(),
                            body: reply,
                        })?;
                    }
                }
                Command::Shutdown => {
                    cx.emit(Message {
                        chat: server.clone(),
                        author: server.clone(),
                        body: "goodbye".into(),
                    })?;
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
