use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufStream};
use tokio::net::TcpListener;

enum Command {
    PING,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut command = "".to_owned();
            let mut buf_stream = BufStream::new(&mut stream);

            loop {
                let num_bytes = buf_stream
                    .read_line(&mut command)
                    .await
                    .expect("Failed to read from stream");

                println!("Read {} bytes", num_bytes);
                println!("Received: {}", command);

                if num_bytes == 0 {
                    break;
                }

                if command.ends_with("PING\r\n") {
                    buf_stream
                        .write("+PONG\r\n".to_owned().as_bytes())
                        .await
                        .expect("Failed to send bytes");

                    buf_stream.flush().await.unwrap();
                }
                command.clear();
            }
        });
    }
}
