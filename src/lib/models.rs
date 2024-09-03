use crate::models::Command::{Echo, Get, Keys, Ping, Set, Unknown};
use std::str::FromStr;
use tokio::io::{AsyncBufReadExt, BufStream};
use tokio::net::TcpStream;

#[derive(Debug)]
pub enum Command {
    Unknown(String),
    Ping,
    Echo(String),
    Keys(String),
    Get(String),
    Set(String, String),
}

#[derive(Debug)]
pub struct Request {
    pub command: Command,
}

#[derive(Debug)]
pub struct BulkString {
    pub payload: String,
}

#[derive(Debug)]
struct Array {
    payload: Vec<BulkString>,
}

impl Into<Vec<u8>> for BulkString {
    fn into(self) -> Vec<u8> {
        let length = self.payload.len();
        format!("{}{}\r\n{}\r\n", "$", length, self.payload).into_bytes()
    }
}

pub async fn to_command(buf_stream: &mut BufStream<&mut TcpStream>) -> Option<Request> {
    let result = read_cmd_part(buf_stream).await;

    if let Some(part) = result {
        let num_of_elems = u8::from_str(&part[1usize..2usize]).unwrap();

        read_cmd_part(buf_stream).await;

        let command = read_cmd_part(buf_stream).await;
        let mut args = Vec::with_capacity((num_of_elems - 1) as usize);
        for _ in 0..num_of_elems - 1 {
            read_cmd_part(buf_stream).await;
            let arg = read_cmd_part(buf_stream).await;
            args.push(arg.unwrap());
        }

        return Some(Request {
            command: match command.unwrap().to_lowercase().as_str() {
                "ping" => Ping,
                "echo" => Echo(args[0].clone()),
                "keys" => Keys(args[0].clone()),
                "get" => Get(args[0].clone()),
                "set" => Set(args[0].clone(), args[1].clone()),
                unknown => Unknown(unknown.to_string()),
            },
        });
    }
    None
}

async fn read_cmd_part(buf_stream: &mut BufStream<&mut TcpStream>) -> Option<String> {
    let mut command = "".to_owned();
    let bytes_read = buf_stream
        .read_line(&mut command)
        .await
        .expect("Failed to read from stream");

    if bytes_read > 0 {
        // Remove \r\n
        Some(command[0..command.len() - 2].to_owned())
    } else {
        None
    }
}
