use redis_starter_rust::models::{to_command, BulkString, Command};
use tokio::io::{AsyncWriteExt, BufStream};
use tokio::net::TcpListener;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;
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
                            payload: message.to_string(),
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
