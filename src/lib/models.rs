use crate::models::Command::{Config, Echo, Get, Info, Keys, Ping, Set, Unknown};
use anyhow::Context;
use clap::Parser;
use std::str::FromStr;
use tokio::io::{AsyncBufReadExt, BufStream};
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct SetParams {
    pub key: String,
    pub value: String,

    pub px: Option<u32>,
}

#[derive(Debug)]
pub enum Command {
    Unknown(String),
    Ping,
    Save,
    Info(String),
    Echo(String),
    Keys(String),
    Get(String),
    Set(SetParams),
    Config(String),
}

#[derive(Debug)]
pub struct Request {
    pub command: Command,
}

#[derive(Debug)]
pub struct BulkString {
    pub payload: Option<String>,
}

#[derive(Debug)]
pub struct Array {
    pub payload: Vec<BulkString>,
}

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(long)]
    pub dir: Option<String>,

    #[arg(long)]
    pub dbfilename: Option<String>,

    #[arg(long, default_value_t = 6379)]
    pub port: u16,

    #[arg(long)]
    pub replicaof: Option<String>,
}

#[derive(Debug)]
pub struct BaseError {
    pub message: String,
}

impl Into<Vec<u8>> for BaseError {
    fn into(self) -> Vec<u8> {
        let redis_err = format!("-{}", self.message);
        redis_err.into_bytes()
    }
}

impl Into<Vec<u8>> for Array {
    fn into(self) -> Vec<u8> {
        let mut bytes = format!("*{}\r\n", self.payload.len()).as_bytes().to_vec();

        for bulk_string in self.payload {
            let bulk_string_bytes: Vec<u8> = bulk_string.into();
            bytes.extend(bulk_string_bytes);
        }

        bytes
    }
}

impl Into<Vec<u8>> for BulkString {
    fn into(self) -> Vec<u8> {
        if let Some(payload) = self.payload {
            let length = payload.len();
            format!("{}{}\r\n{}\r\n", "$", length, payload).into_bytes()
        } else {
            "$-1\r\n".to_owned().into_bytes()
        }
    }
}

pub async fn to_command(
    buf_stream: &mut BufStream<&mut TcpStream>,
) -> anyhow::Result<Option<Request>> {
    let result = read_cmd_part(buf_stream).await;

    if let Some(part) = result {
        let num_of_elems = u8::from_str(&part[1usize..2usize])
            .with_context(|| "Expecting length of elements, such as *2")?;

        read_cmd_part(buf_stream).await;

        let command = read_cmd_part(buf_stream)
            .await
            .with_context(|| "Expecting a base command, such as GET")?;

        let mut args = Vec::with_capacity((num_of_elems - 1) as usize);
        for _ in 0..num_of_elems - 1 {
            read_cmd_part(buf_stream).await;
            let arg = read_cmd_part(buf_stream).await;
            args.push(arg.unwrap().to_lowercase());
        }

        return Ok(Some(Request {
            command: match command.to_lowercase().as_str() {
                "ping" => Ping,
                "info" => Info(args[0].clone()),
                "echo" => Echo(args[0].clone()),
                "keys" => Keys(args[0].clone()),
                "get" => Get(args[0].clone()),
                "set" => Set(build_set_params(args)),
                "config" => Config(args[1].clone()),
                unknown => Unknown(unknown.to_string()),
            },
        }));
    }
    Ok(None)
}

fn build_set_params(args: Vec<String>) -> SetParams {
    let mut px = None;
    for i in 2..args.len() {
        if args[i] == "px" {
            px = Some(u32::from_str(args[i + 1].as_str()).unwrap());
        }
    }

    SetParams {
        key: args[0].to_owned(),
        value: args[1].to_owned(),
        px,
    }
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
