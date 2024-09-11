use crate::models::Command::*;
use anyhow::Context;
use clap::Parser;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncBufReadExt, BufStream};
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
pub struct SetParams {
    pub key: String,
    pub value: String,

    pub px: Option<u32>,
}

#[derive(Debug, Clone)]
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
    Wait(u32, u32),
    ReplConf(String, String),
    PSync(String, String),
}

#[derive(Debug)]
pub struct Request {
    pub command: Command,
}

#[derive(Debug)]
pub struct SimpleString {
    pub value: String,
}

#[derive(Debug)]
pub struct RespInteger {
    pub value: i64,
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

impl Into<Vec<u8>> for Command {
    fn into(self) -> Vec<u8> {
        let mut bulk_strings = vec![];
        match self {
            Unknown(_) => panic!("Cannot convert an UNKNOWN command"),
            Ping => bulk_strings.push(BulkString {
                payload: Some("PING".to_string()),
            }),
            Save => bulk_strings.push(BulkString {
                payload: Some("SAVE".to_string()),
            }),
            Info(key) => {
                bulk_strings.push(BulkString {
                    payload: Some("INFO".to_string()),
                });
                bulk_strings.push(BulkString {
                    payload: Some(key.clone()),
                })
            }
            Echo(value) => {
                bulk_strings.push(BulkString {
                    payload: Some("ECHO".to_string()),
                });
                bulk_strings.push(BulkString {
                    payload: Some(value.clone()),
                })
            }
            Keys(pattern) => {
                bulk_strings.push(BulkString {
                    payload: Some("KEYS".to_string()),
                });
                bulk_strings.push(BulkString {
                    payload: Some(pattern.clone()),
                })
            }
            Get(key) => {
                bulk_strings.push(BulkString {
                    payload: Some("GET".to_string()),
                });
                bulk_strings.push(BulkString {
                    payload: Some(key.clone()),
                })
            }
            Set(params) => {
                bulk_strings.push(BulkString {
                    payload: Some("SET".to_string()),
                });
                bulk_strings.push(BulkString {
                    payload: Some(params.key.clone()),
                });
                bulk_strings.push(BulkString {
                    payload: Some(params.value.clone()),
                });
                if let Some(px) = params.px {
                    bulk_strings.push(BulkString {
                        payload: Some(px.to_string()),
                    });
                }
            }
            Config(key) => {
                bulk_strings.push(BulkString {
                    payload: Some("CONFIG".to_string()),
                });
                bulk_strings.push(BulkString {
                    payload: Some(key.clone()),
                })
            }
            ReplConf(key, value) => {
                bulk_strings.push(BulkString {
                    payload: Some("REPLCONF".to_string()),
                });
                bulk_strings.push(BulkString {
                    payload: Some(key.clone()),
                });
                bulk_strings.push(BulkString {
                    payload: Some(value.clone()),
                });
            }
            PSync(repl_id, offset) => {
                bulk_strings.push(BulkString {
                    payload: Some("PSYNC".to_string()),
                });
                bulk_strings.push(BulkString {
                    payload: Some(repl_id.clone()),
                });
                bulk_strings.push(BulkString {
                    payload: Some(offset.clone()),
                });
            }
            Wait(replicas, timeout) => {
                bulk_strings.push(BulkString {
                    payload: Some("WAIT".to_string()),
                });
                bulk_strings.push(BulkString {
                    payload: Some(replicas.to_string()),
                });
                bulk_strings.push(BulkString {
                    payload: Some(timeout.to_string()),
                });
            }
        }
        let array = Array {
            payload: bulk_strings,
        };
        array.into()
    }
}

impl Into<Vec<u8>> for SimpleString {
    fn into(self) -> Vec<u8> {
        format!("+{}\r\n", self.value).as_bytes().to_vec()
    }
}

impl Into<Vec<u8>> for RespInteger {
    fn into(self) -> Vec<u8> {
        format!(":{}\r\n", self.value).as_bytes().to_vec()
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
    mut buf_stream: &mut BufStream<TcpStream>,
    offset: &AtomicUsize,
) -> anyhow::Result<Option<Request>> {
    let mut read_so_far = 0;
    if let Some((part, bytes_read)) = read_cmd_part(&mut buf_stream).await {
        read_so_far += bytes_read;
        let num_of_elems = u8::from_str(&part[1usize..2usize])
            .with_context(|| "Expecting length of elements, such as *2")?;

        let (_, bytes_read) = read_cmd_part(&mut buf_stream).await.unwrap();
        read_so_far += bytes_read;

        let (command, bytes_read) = read_cmd_part(&mut buf_stream)
            .await
            .with_context(|| "Expecting a base command, such as GET")?;
        read_so_far += bytes_read;

        let mut args = Vec::with_capacity((num_of_elems - 1) as usize);
        for _ in 0..num_of_elems - 1 {
            let (_, bytes_read) = read_cmd_part(&mut buf_stream).await.unwrap();
            read_so_far += bytes_read;

            let (arg, bytes_read) = read_cmd_part(&mut buf_stream).await.unwrap();
            read_so_far += bytes_read;

            args.push(arg.to_lowercase());
        }

        let command = match command.to_lowercase().as_str() {
            "ping" => Ping,
            "wait" => Wait(args[0].parse()?, args[1].parse()?),
            "info" => Info(args[0].clone()),
            "echo" => Echo(args[0].clone()),
            "keys" => Keys(args[0].clone()),
            "get" => Get(args[0].clone()),
            "set" => Set(build_set_params(args)),
            "config" => Config(args[1].clone()),
            "replconf" => ReplConf(args[0].clone(), args[1].clone()),
            "psync" => PSync(args[0].clone(), args[1].clone()),
            unknown => Unknown(unknown.to_string()),
        };

        match command {
            // REPLCONF is added after processing because reasons *shrug*
            ReplConf(_, _) => {}
            _ => {
                offset.fetch_add(read_so_far, Ordering::Relaxed);
            }
        }

        return Ok(Some(Request { command }));
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

async fn read_cmd_part(buf_stream: &mut BufStream<TcpStream>) -> Option<(String, usize)> {
    let mut command = "".to_owned();
    let bytes_read = buf_stream
        .read_line(&mut command)
        .await
        .expect("Failed to read from stream");

    if bytes_read > 0 {
        // Remove \r\n
        Some((command[0..command.len() - 2].to_owned(), bytes_read))
    } else {
        None
    }
}
