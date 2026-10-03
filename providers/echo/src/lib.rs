//! Example provider that talks a line-based protocol over TCP.
//!
//! It connects to the `server` setting, emits the server's greeting, then for
//! each `Send` command writes the body as a line and emits the reply.

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
                Command::Send { chat, body } => {
                    writeln!(conn.get_mut(), "{body}")?;
                    let reply = read_line(&mut conn)?;
                    cx.emit(Message {
                        chat,
                        author: server.clone(),
                        body: reply,
                    })?;
                }
                Command::Shutdown => break,
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
