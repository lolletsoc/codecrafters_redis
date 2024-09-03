use dashmap::DashMap;
use redis_starter_rust::models::{to_command, BulkString, Command};
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncWriteExt, BufStream};
use tokio::net::TcpListener;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    let map: Arc<DashMap<String, (String, Option<SystemTime>)>> = Arc::new(DashMap::new());
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;
        let map_ref = map.clone();

        tokio::spawn(async move {
            let mut buf_stream = BufStream::new(&mut stream);

            loop {
                let request = to_command(&mut buf_stream).await;
                if let None = request {
                    break;
                }

                match request.unwrap().command {
                    Command::Ping => {
                        buf_stream
                            .write("+PONG\r\n".to_owned().as_bytes())
                            .await
                            .expect("Failed to send bytes");

                        buf_stream.flush().await.unwrap();
                    }
                    Command::Echo(ref message) => {
                        let bulk_string = BulkString {
                            payload: Some(message.to_string()),
                        };

                        let bytes: Vec<u8> = bulk_string.into();

                        buf_stream
                            .write(bytes.as_slice())
                            .await
                            .expect("Failed to send bytes");

                        buf_stream.flush().await.unwrap();
                    }
                    Command::Get(ref key) => {
                        let result = map_ref.get(key);
                        if let Some(result) = result {
                            let value;
                            if let Some(expire_at) = result.to_owned().1 {
                                let now = SystemTime::now();
                                if now >= expire_at {
                                    value = None
                                } else {
                                    value = Some(result.to_owned().0)
                                }
                            } else {
                                value = Some(result.to_owned().0)
                            }

                            let bulk_string = BulkString { payload: value };

                            let bytes: Vec<u8> = bulk_string.into();

                            buf_stream
                                .write(bytes.as_slice())
                                .await
                                .expect("Failed to send bytes");

                            buf_stream.flush().await.unwrap();
                        } else {
                            todo!();
                        }
                    }
                    Command::Set(params) => {
                        let value = params.value.to_string();
                        let expire_at;
                        if params.px.is_some() {
                            expire_at = Some(
                                SystemTime::now()
                                    .add(Duration::from_millis(params.px.unwrap() as u64)),
                            );
                        } else {
                            expire_at = None;
                        }

                        map_ref.insert(params.key.to_string(), (value, expire_at));
                        let bulk_string = BulkString {
                            payload: Some("OK".to_string()),
                        };

                        let bytes: Vec<u8> = bulk_string.into();

                        buf_stream
                            .write(bytes.as_slice())
                            .await
                            .expect("Failed to send bytes");

                        buf_stream.flush().await.unwrap();
                    }
                    Command::Keys(_) => {}
                    Command::Unknown(_) => {
                        eprintln!("");
                    }
                }
            }
        });
    }
}
