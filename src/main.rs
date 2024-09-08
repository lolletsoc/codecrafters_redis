use clap::Parser;
use dashmap::DashMap;
use redis_starter_rust::models::{to_command, Args, BaseError};
use redis_starter_rust::processing::{process_command, write_and_flush};
use redis_starter_rust::rdb::read_rdb;
use redis_starter_rust::replication::{init_replication, MasterReplicationInfo};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::BufStream;
use tokio::net::TcpListener;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    let args = Arc::new(Args::parse());
    let map: Arc<DashMap<String, (String, Option<SystemTime>)>> = Arc::new(DashMap::new());
    let master_rep_info = Arc::new(MasterReplicationInfo::new());

    if let (Some(dir), Some(filename)) = (&args.dir, &args.dbfilename) {
        read_rdb(dir, filename, map.clone()).await?;
    }

    if let Some(repinfo) = &args.replicaof {
        init_replication(repinfo, &args)
            .await
            .expect("Replication init failed");
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await?;

    loop {
        let (mut stream, _) = listener.accept().await?;
        let map_ref = map.clone();
        let args_ref = args.clone();
        let rep_ref = master_rep_info.clone();

        tokio::spawn(async move {
            let mut buf_stream = BufStream::new(&mut stream);

            loop {
                let request = to_command(&mut buf_stream).await;
                match request {
                    Ok(Some(request)) => {
                        process_command(
                            request.command,
                            &args_ref,
                            &rep_ref,
                            &map_ref,
                            &mut buf_stream,
                        )
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
