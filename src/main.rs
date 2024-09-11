use clap::Parser;
use dashmap::DashMap;
use redis_starter_rust::models::{to_command, Args, BaseError, Command};
use redis_starter_rust::processing::{process_command, write_and_flush};
use redis_starter_rust::rdb::read_rdb;
use redis_starter_rust::replication::{init_replication, MasterReplicationInfo};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::BufStream;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    let args = Arc::new(Args::parse());
    let map: Arc<DashMap<String, (String, Option<SystemTime>)>> = Arc::new(DashMap::new());
    let master_rep_info = Arc::new(MasterReplicationInfo::new());
    let replicas = Arc::new(Mutex::new(Vec::new()));
    let (tx, rx): (Sender<Command>, Receiver<Command>) = mpsc::channel(100);
    let tx = Arc::new(tx);
    let rx = Arc::new(Mutex::new(rx));

    if let (Some(dir), Some(filename)) = (&args.dir, &args.dbfilename) {
        read_rdb(dir, filename, map.clone()).await?;
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await?;
    println!("Listening on {}", args.port);

    if let Some(repinfo) = &args.replicaof {
        let replication_stream = init_replication(repinfo, &args)
            .await
            .expect("Replication init failed");

        let map_ref = map.clone();
        let args_ref = args.clone();
        let rep_ref = master_rep_info.clone();
        let replicas = replicas.clone();
        let tx = tx.clone();
        let rx = rx.clone();

        let std_stream = replication_stream.into_std().unwrap();
        let cloned_stream = std_stream.try_clone().unwrap();

        let cloned_tcp_stream = TcpStream::from_std(cloned_stream).unwrap();
        let cloned_buf_stream = BufStream::new(cloned_tcp_stream);
        let buf_stream = Arc::new(Mutex::new(cloned_buf_stream));

        let arc_stream = Arc::new(Mutex::new(TcpStream::from_std(std_stream).unwrap()));
        tokio::spawn(async move {
            loop {
                let binding = buf_stream.clone();
                let mut guard = binding.lock().await;
                let request = to_command(&mut guard).await;
                match request {
                    Ok(Some(request)) => {
                        process_command(
                            request.command,
                            &args_ref,
                            &rep_ref,
                            &map_ref,
                            arc_stream.clone(),
                            &replicas,
                            &tx,
                            &rx,
                        )
                        .await;
                    }
                    Ok(None) => {
                        // EOF
                        println!("No more data");
                        break;
                    }
                    Err(err) => {
                        let arc = arc_stream.clone();
                        let mut stream_guard = arc.lock().await;
                        write_and_flush(
                            &mut stream_guard,
                            BaseError {
                                message: err.to_string(),
                            },
                        )
                        .await;
                    }
                }
            }
        });

        println!("Initialized replication with master");
    }

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("New connection from {}", addr);

                let map_ref = map.clone();
                let args_ref = args.clone();
                let rep_ref = master_rep_info.clone();
                let replicas = replicas.clone();
                let tx = tx.clone();
                let rx = rx.clone();

                let std_stream = stream.into_std().unwrap();
                let cloned_stream = std_stream.try_clone().unwrap();

                let cloned_tcp_stream = TcpStream::from_std(cloned_stream).unwrap();
                let cloned_buf_stream = BufStream::new(cloned_tcp_stream);
                let buf_stream = Arc::new(Mutex::new(cloned_buf_stream));

                let arc_stream = Arc::new(Mutex::new(TcpStream::from_std(std_stream).unwrap()));
                tokio::spawn(async move {
                    loop {
                        let binding = buf_stream.clone();
                        let mut guard = binding.lock().await;
                        let request = to_command(&mut guard).await;
                        match request {
                            Ok(Some(request)) => {
                                process_command(
                                    request.command,
                                    &args_ref,
                                    &rep_ref,
                                    &map_ref,
                                    arc_stream.clone(),
                                    &replicas,
                                    &tx,
                                    &rx,
                                )
                                .await;
                            }
                            Ok(None) => {
                                // EOF
                                println!("No more data");
                                break;
                            }
                            Err(err) => {
                                let arc = arc_stream.clone();
                                let mut stream_guard = arc.lock().await;
                                write_and_flush(
                                    &mut stream_guard,
                                    BaseError {
                                        message: err.to_string(),
                                    },
                                )
                                .await;
                            }
                        }
                    }
                });
            }
            Err(err) => {
                println!("Error establishing connection: {}", err);
            }
        }
    }
}
