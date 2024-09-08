use crate::models::Command::{PSync, Ping, ReplConf};
use crate::models::{Args, Array, BulkString};
use crate::processing::{receive_ack, write_and_flush};
use anyhow::Context;
use rand::distr::Alphanumeric;
use rand::Rng;
use std::sync::Arc;
use tokio::io::BufStream;
use tokio::net::TcpStream;

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

    let tcp_stream = &mut TcpStream::connect(format!("{}:{}", split[0], split[1]))
        .await
        .with_context(|| "Failed to connect to replica")?;

    let mut stream = BufStream::new(tcp_stream);
    write_and_flush(&mut stream, Ping).await;
    receive_ack(&mut stream).await?;

    write_and_flush(
        &mut stream,
        ReplConf("listening-port".to_string(), args.port.to_string()),
    )
    .await;
    receive_ack(&mut stream).await?;

    write_and_flush(
        &mut stream,
        ReplConf("capa".to_string(), "psync2".to_string()),
    )
    .await;
    receive_ack(&mut stream).await?;

    write_and_flush(&mut stream, PSync("?".to_string(), "-1".to_string())).await;

    Ok(())
}
