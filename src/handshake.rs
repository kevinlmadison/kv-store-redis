use crate::resptype::*;
use anyhow::{bail, Context, Result};
use std::str;
use tokio::{io, io::AsyncReadExt, io::AsyncWriteExt, net::TcpStream};

type WriteHalf = io::WriteHalf<TcpStream>;
type ReadHalf = io::ReadHalf<TcpStream>;

async fn send_and_receive(msg: Vec<u8>, rd: &mut ReadHalf, wr: &mut WriteHalf) -> Result<()> {
    if let Ok(_) = wr.write_all(&msg[..]).await {
        let mut buffer: [u8; 1024] = [0; 1024];

        let len = rd.read(&mut buffer).await?;

        if len == 0 {
            bail!("Nothing read from read buffer")
        }

        println!(
            "Handshake: {:?} Received",
            str::from_utf8(&buffer[..len]).unwrap()
        );
    }
    Ok(())
}

pub async fn handshake(host_addr: &str, host_port: &str, local_port: &str) -> Result<()> {
    let bind_addr: String = host_addr.to_string() + ":" + host_port;
    loop {
        let Ok(stream) = TcpStream::connect(&bind_addr).await else {
            continue;
        };
        let (mut rd, mut wr) = io::split(stream);

        let mut handshake_args: Vec<Vec<u8>> = Vec::new();
        handshake_args.push(Type::Array(vec![Type::BulkString("ping".to_string())]).serialize());

        handshake_args.push(
            Type::Array(vec![
                Type::BulkString("replconf".to_string()),
                Type::BulkString("listening-port".to_string()),
                Type::BulkString(local_port.to_string()),
            ])
            .serialize(),
        );

        handshake_args.push(
            Type::Array(vec![
                Type::BulkString("replconf".to_string()),
                Type::BulkString("capa".to_string()),
                Type::BulkString("psync".to_string()),
            ])
            .serialize(),
        );

        handshake_args.push(
            Type::Array(vec![
                Type::BulkString("psync".to_string()),
                Type::BulkString("?".to_string()),
                Type::BulkString("-1".to_string()),
            ])
            .serialize(),
        );

        for arg in handshake_args.into_iter() {
            let _ = send_and_receive(arg.clone(), &mut rd, &mut wr).await;
        }

        // Here we're waiting for RBD file after receiving the FULLRESYNC from
        // the master instance.

        let mut buffer: [u8; 1024] = [0; 1024];

        let len = rd.read(&mut buffer).await?;

        if len == 0 {
            println!("Nothing read from read buffer");
            return Ok(());
        }

        println!(
            "Handshake: {:?} Received",
            str::from_utf8(&buffer[..len]).unwrap()
        );

        return Ok(());
    }
}
