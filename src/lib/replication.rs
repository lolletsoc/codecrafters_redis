use crate::models::{Args, Array, BulkString};
use crate::processing::write_and_flush;
use anyhow::Context;
use rand::distr::Alphanumeric;
use rand::Rng;
use std::future::Future;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufStream};
use tokio::net::{TcpSocket, TcpStream};

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

pub async fn init_replication(replicaof: &str) -> anyhow::Result<()> {
    let split: Vec<&str> = replicaof.split(' ').collect();

    let tcp_stream = &mut TcpStream::connect(format!("{}:{}", split[0], split[1]))
        .await
        .with_context(|| "Failed to connect to replica")?;

    let mut stream = BufStream::new(tcp_stream);

    let array = Array {
        payload: vec![BulkString {
            payload: Some("PING".to_string()),
        }],
    };

    write_and_flush(&mut stream, array).await;
    Ok(())
}
