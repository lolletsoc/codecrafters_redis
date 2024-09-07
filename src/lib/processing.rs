use crate::models::*;
use dashmap::DashMap;
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncWriteExt, BufStream};
use tokio::net::TcpStream;

pub async fn process_command(
    command: Command,
    args: &Arc<Args>,
    map: &Arc<DashMap<String, (String, Option<SystemTime>)>>,
    buf_stream: &mut BufStream<&mut TcpStream>,
) {
    match command {
        Command::Config(field) => match field.as_str() {
            "dir" => {
                let array = Array {
                    payload: vec![
                        BulkString {
                            payload: Some("dir".to_owned()),
                        },
                        BulkString {
                            payload: Some(args.dir.clone().unwrap()),
                        },
                    ],
                };

                write_and_flush(buf_stream, array).await;
            }
            "dbfilename" => {
                let array = Array {
                    payload: vec![
                        BulkString {
                            payload: Some("dbfilename".to_owned()),
                        },
                        BulkString {
                            payload: Some(args.dbfilename.clone().unwrap()),
                        },
                    ],
                };

                write_and_flush(buf_stream, array).await;
            }
            unknown => {
                write_and_flush(
                    buf_stream,
                    BaseError {
                        message: format!("Config key '{}' unknown", unknown),
                    },
                )
                .await
            }
        },
        Command::Info(_) => {
            let replication = BulkString {
                payload: Some("role:master".to_string()),
            };
            write_and_flush(buf_stream, replication).await;
        }
        Command::Ping => {
            write_and_flush(buf_stream, "+PONG\r\n").await;
        }
        Command::Echo(ref message) => {
            write_and_flush(
                buf_stream,
                BulkString {
                    payload: Some(message.to_string()),
                },
            )
            .await;
        }
        Command::Get(ref key) => {
            let result = map.get(key);
            if let Some(result) = result {
                let value = match result.to_owned().1 {
                    Some(expire_at) => {
                        let now = SystemTime::now();
                        if now >= expire_at {
                            None
                        } else {
                            Some(result.to_owned().0)
                        }
                    }
                    None => Some(result.to_owned().0),
                };

                write_and_flush(buf_stream, BulkString { payload: value }).await;
            } else {
                write_and_flush(buf_stream, BulkString { payload: None }).await;
            }
        }
        Command::Set(params) => {
            let value = params.value.to_string();
            let expire_at = match params.px {
                Some(_) => {
                    Some(SystemTime::now().add(Duration::from_millis(params.px.unwrap() as u64)))
                }
                None => None,
            };

            map.insert(params.key.to_string(), (value, expire_at));
            write_and_flush(
                buf_stream,
                BulkString {
                    payload: Some("OK".to_string()),
                },
            )
            .await;
        }
        Command::Keys(_) => {
            // Assuming a wildcard ('*') for now
            let bulk_strings = map
                .iter()
                .map(|e| BulkString {
                    payload: Some(e.key().to_string()),
                })
                .collect();

            write_and_flush(
                buf_stream,
                Array {
                    payload: bulk_strings,
                },
            )
            .await;
        }
        Command::Unknown(_) => {
            eprintln!("");
        }
        Command::Save => todo!(),
    }
}

pub async fn write_and_flush<T>(buf_stream: &mut BufStream<&mut TcpStream>, into_bytes: T)
where
    T: Into<Vec<u8>>,
{
    let bytes: Vec<u8> = into_bytes.into();

    buf_stream
        .write(bytes.as_slice())
        .await
        .expect("Failed to send bytes");

    buf_stream.flush().await.unwrap();
}
