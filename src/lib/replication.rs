use crate::models::Command::{PSync, Ping, ReplConf};
use crate::models::{Args, Array, BulkString};
use crate::processing::{receive_ack, write_and_flush};
use anyhow::Context;
use rand::distr::Alphanumeric;
use rand::Rng;
use std::sync::Arc;
use tokio::io::BufStream;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

pub struct MasterReplicationInfo {
    pub replid: String,
    pub repl_offset: u8,
}

impl MasterReplicationInfo {
    pub fn new() -> MasterReplicationInfo {
        let replid: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(40)
            .map(char::from)
            .collect();

        MasterReplicationInfo {
            replid,
            repl_offset: 0,
        }
    }
}

pub async fn init_replication(replicaof: &str, args: &Arc<Args>) -> anyhow::Result<()> {
    let split: Vec<&str> = replicaof.split(' ').collect();

    let mut tcp_stream = TcpStream::connect(format!("{}:{}", split[0], split[1]))
        .await
        .with_context(|| "Failed to connect to replica")?;
    let tcp_stream = Mutex::new(&mut tcp_stream);

    let guard = &mut tcp_stream.lock().await;
    write_and_flush(guard, Ping).await;
    receive_ack(guard).await?;

    write_and_flush(
        guard,
        ReplConf("listening-port".to_string(), args.port.to_string()),
    )
    .await;
    receive_ack(guard).await?;

    write_and_flush(guard, ReplConf("capa".to_string(), "psync2".to_string())).await;
    receive_ack(guard).await?;

    write_and_flush(guard, PSync("?".to_string(), "-1".to_string())).await;
    receive_ack(guard).await?;

    Ok(())
}
