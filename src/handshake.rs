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

pub async fn handshake(host: &str, port: &str) -> Result<()> {
    let bind_addr = host.to_string() + ":" + port;
    let mut stream = TcpStream::connect(&bind_addr).await.unwrap();
    let ping = Type::Array(vec!(Type::BulkString("ping".to_string()))).serialize();

    stream.write_all(&ping[..]).await.unwrap();
    Ok(())
}
