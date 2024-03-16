use std::fmt::{Formatter};
use anyhow::{bail, Context, Result};
use std::str;
use tokio::{
    io::{AsyncWriteExt, AsyncReadExt},
    net::{TcpListener, TcpStream},
};

use crate::resptype::*;
use crate::flags::*;
use crate::info::*;

pub async fn handshake(host_addr: &str, host_port: &str, local_port: &str) -> Result<()> {
    let bind_addr = host_addr.to_string() + ":" + host_port;
    let mut stream = TcpStream::connect(&bind_addr).await.unwrap();

    let ping = Type::Array(vec!(Type::BulkString("ping".to_string()))).serialize();
    stream.write_all(&ping[..]).await.unwrap();

    let replconf = Type::Array(vec![
        Type::BulkString("replconf".to_string()),
        Type::BulkString("listening-port".to_string()),
        Type::BulkString(local_port.to_string())
    ]).serialize();
    stream.write_all(&replconf[..]).await.unwrap();

    let replconf = Type::Array(vec![
        Type::BulkString("replconf".to_string()),
        Type::BulkString("capa".to_string()),
        Type::BulkString("psync".to_string())
    ]).serialize();
    stream.write_all(&replconf[..]).await.unwrap();

    Ok(())
}
