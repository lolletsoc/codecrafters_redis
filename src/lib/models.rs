use crate::models::Command::*;
use anyhow::Context;
use clap::Parser;
use std::str::FromStr;
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
) -> anyhow::Result<Option<Request>> {
    let result = read_cmd_part(&mut buf_stream).await;

    if let Some(part) = result {
        let num_of_elems = u8::from_str(&part[1usize..2usize])
            .with_context(|| "Expecting length of elements, such as *2")?;

        read_cmd_part(&mut buf_stream).await;

        let command = read_cmd_part(&mut buf_stream)
            .await
            .with_context(|| "Expecting a base command, such as GET")?;

        let mut args = Vec::with_capacity((num_of_elems - 1) as usize);
        for _ in 0..num_of_elems - 1 {
            read_cmd_part(&mut buf_stream).await;
            let arg = read_cmd_part(&mut buf_stream).await;
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
                "replconf" => ReplConf(args[0].clone(), args[1].clone()),
                "psync" => PSync(args[0].clone(), args[1].clone()),
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

async fn read_cmd_part(buf_stream: &mut BufStream<TcpStream>) -> Option<String> {
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
