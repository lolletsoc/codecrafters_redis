use crate::models::Args;
use crate::models::Command::{PSync, Ping, ReplConf};
use crate::processing::{receive_ack, write_and_flush};
use crate::rdb::read_rdb_from_bytes;
use anyhow::Context;
use dashmap::DashMap;
use rand::distr::Alphanumeric;
use rand::Rng;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::AsyncReadExt;
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

pub async fn init_replication(replicaof: &str, args: &Arc<Args>) -> anyhow::Result<TcpStream> {
    let split: Vec<&str> = replicaof.split(' ').collect();

    let mut tcp_stream = TcpStream::connect(format!("{}:{}", split[0], split[1]))
        .await
        .with_context(|| "Failed to connect to replica")?;

    write_and_flush(&mut tcp_stream, Ping).await;
    receive_ack(&mut tcp_stream).await?;

    write_and_flush(
        &mut tcp_stream,
        ReplConf("listening-port".to_string(), args.port.to_string()),
    )
    .await;
    receive_ack(&mut tcp_stream).await?;

    write_and_flush(
        &mut tcp_stream,
        ReplConf("capa".to_string(), "psync2".to_string()),
    )
    .await;
    receive_ack(&mut tcp_stream).await?;

    write_and_flush(&mut tcp_stream, PSync("?".to_string(), "-1".to_string())).await;

    // This is a hack to deal with FULLRESYNC and the empty RDB file.
    // Will go back to this at a later stage - only requires proper use of BufStream
    let mut bytes = [0; 58];
    tcp_stream
        .read_exact(&mut bytes)
        .await
        .with_context(|| "Failed to receive bytes")?;

    let mut bytes = [0; 93];
    tcp_stream
        .read_exact(&mut bytes)
        .await
        .with_context(|| "Failed to receive bytes")?;

    // From here on, we will receive all replication commands

    Ok(tcp_stream)
}
