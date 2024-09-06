use clap::Parser;
use dashmap::DashMap;
use redis_starter_rust::models::{to_command, Args, BaseError};
use redis_starter_rust::processing::{process_command, write_and_flush};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::BufStream;
use tokio::net::TcpListener;
use redis_starter_rust::rdb::read_rdb;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    let args = Arc::new(Args::parse());
    let map: Arc<DashMap<String, (String, Option<SystemTime>)>> = Arc::new(DashMap::new());

    if let (Some(dir), Some(filename)) = (&args.dir, &args.dbfilename) {
        read_rdb(dir, filename, map.clone()).await?;
    }

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;
        let map_ref = map.clone();
        let args_ref = args.clone();

        tokio::spawn(async move {
            let mut buf_stream = BufStream::new(&mut stream);

            loop {
                let request = to_command(&mut buf_stream).await;
                match request {
                    Ok(Some(request)) => {
                        process_command(request.command, &args_ref, &map_ref, &mut buf_stream)
                            .await;
                    }
                    Ok(None) => {
                        // EOF
                        break;
                    }
                    Err(err) => {
                        write_and_flush(
                            &mut buf_stream,
                            BaseError {
                                message: err.to_string(),
                            },
                        )
                        .await
                    }
                }
            }
        });
    }
}
