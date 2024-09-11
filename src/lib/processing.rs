use crate::models::*;
use crate::replication::MasterReplicationInfo;
use anyhow::Context;
use base64::{engine::general_purpose, Engine as _};
use dashmap::DashMap;
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

pub async fn process_command(
    command: Command,
    args: &Arc<Args>,
    rep_ref: &Arc<MasterReplicationInfo>,
    map: &Arc<DashMap<String, (String, Option<SystemTime>)>>,
    buf_stream: Arc<Mutex<TcpStream>>,
    replicas: &Arc<Mutex<Vec<Arc<Mutex<TcpStream>>>>>,
    tx: &Arc<Sender<Command>>,
    rx: &Arc<Mutex<Receiver<Command>>>,
) {
    println!(
        "Processing {:?} as replica: {}",
        command,
        args.replicaof.is_some()
    );
    let mut guard = buf_stream.lock().await;
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

                write_and_flush(&mut guard, array).await;
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

                write_and_flush(&mut guard, array).await;
            }
            unknown => {
                write_and_flush(
                    &mut guard,
                    BaseError {
                        message: format!("Config key '{}' unknown", unknown),
                    },
                )
                .await;
            }
        },
        Command::Info(_) => {
            let master_or_slave = match args.replicaof {
                Some(_) => "slave",
                None => "master",
            };

            let replication = BulkString {
                payload: Some(
                    format!(
                        "role:{}\rmaster_replid:{}\rmaster_repl_offset:{}",
                        master_or_slave, rep_ref.replid, rep_ref.repl_offset
                    )
                    .to_string(),
                ),
            };
            write_and_flush(&mut guard, replication).await;
        }
        Command::Ping => {
            write_and_flush(&mut guard, "+PONG\r\n").await;
        }
        Command::Echo(ref message) => {
            write_and_flush(
                &mut guard,
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

                write_and_flush(&mut guard, BulkString { payload: value }).await;
            } else {
                write_and_flush(&mut guard, BulkString { payload: None }).await;
            }
        }
        Command::Set(ref params) => {
            let value = params.value.to_string();
            let expire_at = match params.px {
                Some(_) => {
                    Some(SystemTime::now().add(Duration::from_millis(params.px.unwrap() as u64)))
                }
                None => None,
            };

            map.insert(params.key.to_string(), (value, expire_at));
            tx.send(command.clone())
                .await
                .expect("Failed to send Command to TX");

            if args.replicaof.is_none() {
                write_and_flush(
                    &mut guard,
                    BulkString {
                        payload: Some("OK".to_string()),
                    },
                )
                .await;
            }
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
                &mut guard,
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
        Command::ReplConf(_, _) => {
            if let Some(_) = &args.replicaof {
                write_and_flush(
                    &mut guard,
                    Command::ReplConf("ACK".to_string(), "0".to_string()),
                )
                .await;
            } else {
                send_ack(&mut guard).await;
            }
        }
        Command::PSync(_, _) => {
            assert!(
                &args.replicaof.is_none(),
                "Configured as replica. I should never receive this command."
            );

            write_and_flush(
                &mut guard,
                SimpleString {
                    value: format!("FULLRESYNC {} 0", rep_ref.replid),
                },
            )
            .await;

            let empty_rdb = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==".as_bytes();
            let empty_rdb_bytes = general_purpose::STANDARD.decode(empty_rdb).unwrap();

            println!("len of empty: {}", empty_rdb_bytes.len());

            let bytes = empty_rdb_bytes.as_slice();
            write_and_flush(&mut guard, format!("${}\r\n", bytes.len())).await;
            write_and_flush(&mut guard, bytes).await;

            println!("Adding replica");
            replicas.lock().await.push(buf_stream.clone());

            let replicas = replicas.clone();
            let rx = rx.clone();
            tokio::spawn(async move {
                println!("Replication started on master");
                loop {
                    let mut rx = rx.lock().await;
                    for command in rx.recv().await {
                        println!("Received command for replication: {:?}", command);
                        let mut guard = replicas.lock().await;
                        for replica_stream in guard.iter_mut() {
                            let mut stream = replica_stream.lock().await;
                            let bytes_written = write_and_flush(&mut stream, command.clone()).await;
                            println!("Wrote {} bytes for replication", bytes_written);
                        }
                    }
                }
            });
        }
    }
}

pub async fn write_and_flush<T>(tcp_stream: &mut TcpStream, into_bytes: T) -> usize
where
    T: Into<Vec<u8>>,
{
    let bytes: Vec<u8> = into_bytes.into();

    let bytes = tcp_stream
        .write(bytes.as_slice())
        .await
        .expect("Failed to send bytes");

    tcp_stream.flush().await.unwrap();

    bytes
}

pub async fn send_ack(buf_stream: &mut TcpStream) {
    buf_stream
        .write("+OK\r\n".as_bytes())
        .await
        .expect("Failed to send bytes");

    buf_stream.flush().await.unwrap();
}

pub async fn receive_ack(tcp_stream: &mut TcpStream) -> anyhow::Result<()> {
    let mut ack = String::new();
    let mut bytes = [0; 5];
    tcp_stream
        .read_exact(&mut bytes)
        .await
        .with_context(|| "Failed to receive bytes")?;

    if !ack.eq("+OK\r") {
        Ok(())
    } else {
        Err(anyhow::Error::msg(format!(
            "Received unexpected ACK: {}",
            ack
        )))
    }
}
