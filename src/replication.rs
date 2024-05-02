use crate::frame::*;
use crate::response::*;
use crate::resptype::*;
use crate::server::*;
use anyhow::{bail, Result};
use std::str;
use std::sync::{Arc, Mutex};
use std::{thread, time};
use tokio::{io, io::AsyncReadExt, io::AsyncWriteExt, net::TcpStream};

type WriteHalf = io::WriteHalf<TcpStream>;
type ReadHalf = io::ReadHalf<TcpStream>;

pub fn replicate(frame: Frame, server_info: Arc<Mutex<ServerInfo>>) {
    let mut server_info = server_info.lock().unwrap();
    let msg = frame.bytes_vec();
    println!("Replicatiing: {:?}", msg);
    for replica in server_info.replicas.iter_mut() {
        let _ = my_send(replica, &msg[..]);
        // let _ = replica_c.write_all(&msg).await.unwrap();
        // let _ = replica_c.flush().await.unwrap();
    }
}

pub async fn my_send(stream: &mut TcpStream, msg: &[u8]) {
    stream.write_all(&msg).await.unwrap();
}

fn sync_replica_db(/* info_db: &InfoDb, db: &Db */) -> Result<()> {
    // For now we just add an arbitrary wait to simulate syncing the replica
    // with the rdb file
    let ten_millis = time::Duration::from_millis(3000);
    thread::sleep(ten_millis);
    Ok(())
}

async fn send_and_receive(msg: Vec<u8>, rd: &mut ReadHalf, wr: &mut WriteHalf) -> Result<()> {
    if let Ok(_) = wr.write_all(&msg[..]).await {
        let mut buffer: [u8; 1024] = [0; 1024];

        let len = rd.read(&mut buffer).await?;

        if len == 0 {
            bail!("Nothing read from read buffer loop")
        }

        println!(
            "Handshake: {:?} Received",
            // str::from_utf8(&buffer[..len]).unwrap()
            &buffer[..len]
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

        // let _ = sync_replica_db();

        println!(
            "Handshake Post: {:?} Received",
            // str::from_utf8(&buffer[..len]).unwrap()
            &buffer[..len]
        );

        return Ok(());
    }
}
