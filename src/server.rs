use crate::command::*;
use crate::flags::*;
use crate::frame::*;
use crate::info::*;
use crate::response::*;
use crate::resptype::*;
use anyhow::{bail, Context, Result};
use itertools::Itertools;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{str, thread, time};
use tokio::{
    io,
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[derive(Debug, Clone)]
pub struct DbEntry {
    value: String,
    expiry: Option<Instant>,
}

impl DbEntry {
    pub fn new(s: String, ex: Option<Duration>) -> Self {
        if let Some(dur) = ex {
            return Self {
                value: s,
                expiry: Some(Instant::now() + dur),
            };
        } else {
            return Self {
                value: s,
                expiry: None,
            };
        }
    }
    pub fn value(self) -> String {
        self.value.clone()
    }
}

#[derive(Default, Debug, Clone)]
pub struct Database {
    db: HashMap<String, DbEntry>,
}

impl Database {
    pub fn insert(&mut self, key: String, val: DbEntry) -> Result<()> {
        self.db.insert(key, val);
        Ok(())
    }

    pub fn get(&self, key: String) -> Result<DbEntry> {
        if let Some(val) = self.db.get(&key) {
            if let Some(exp) = val.expiry {
                if exp <= Instant::now() {
                    // Entry has expired
                    return Ok(DbEntry::new("(nil)".into(), None));
                }
            }
            // Valid entry (no expiry or not expired)
            return Ok(val.clone());
        }
        // Key not found
        return Ok(DbEntry::new("(nil)".into(), None));
    }

    pub fn get_all(&self) -> Result<Vec<String>> {
        Ok(self
            .db
            .clone()
            .into_iter()
            .map(|(k, v)| k.to_owned() + ":" + v.value().as_str() + "\n")
            .collect::<Vec<String>>())
    }
}
#[derive(Debug, Clone)]
pub enum Role {
    Master,
    Slave(SocketAddr),
}

#[derive(Debug)]
pub struct ServerInfo {
    pub role: Role,
    pub addr: SocketAddr,
    pub replicas: Vec<TcpStream>,
}

#[derive(Debug)]
pub struct Server {
    redis_db: Arc<Mutex<Database>>,
    info_db: Arc<Mutex<Database>>,
    server_info: Arc<tokio::sync::Mutex<ServerInfo>>,
}

impl Server {
    pub fn new(args: &Args) -> Self {
        let bind_addr: SocketAddr = format!("{}:{}", args.addr, args.port).parse().unwrap();
        match &args.replicaof {
            Some(tokens) => {
                let (host, port) = tokens
                    .into_iter()
                    .collect_tuple()
                    .context("parsing arguments for --replicaof flag")
                    .unwrap();
                println!("{}:{}", host, port);
                // let master_addr: SocketAddr = format!("{}:{}", host, port)
                let master_addr: SocketAddr = format!("127.0.0.1:{}", port)
                    .parse()
                    .expect("failed to parse dis addy doh");
                return Self {
                    server_info: Arc::new(tokio::sync::Mutex::new(ServerInfo {
                        replicas: Vec::default(),
                        role: Role::Slave(master_addr),
                        addr: bind_addr,
                    })),
                    redis_db: Arc::new(Mutex::new(Database::default())),
                    info_db: Arc::new(Mutex::new(Database::default())),
                };
            }
            None => {
                return Self {
                    server_info: Arc::new(tokio::sync::Mutex::new(ServerInfo {
                        replicas: Vec::default(),
                        role: Role::Master,
                        addr: bind_addr,
                    })),
                    redis_db: Arc::new(Mutex::new(Database::default())),
                    info_db: Arc::new(Mutex::new(Database::default())),
                };
            }
        }
    }

    pub async fn start(&self) -> Result<()> {
        let role = self.server_info.lock().await.role.clone();
        match role {
            Role::Master => {
                let _ = self.call_init_info_db().await?;
            }

            Role::Slave(bind_addr) => {
                let _ = self.call_init_info_db().await?;
                let port = self.server_info.lock().await.addr.clone().port();
                let stream = self.handshake(bind_addr, port).await.unwrap();
                let db = self.redis_db.clone();
                let info_db = self.info_db.clone();
                let server_info = self.server_info.clone();
                tokio::spawn(async move { stream_handler(stream, db, info_db, server_info).await });
                println!("Tokio thread spawned for replication");
                // Ok(())
            }
        }

        let bind_addr = self.server_info.lock().await.addr.clone();
        let listener = TcpListener::bind(&bind_addr).await.unwrap();
        println!("STARTING");
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let db = self.redis_db.clone();
                    let info_db = self.info_db.clone();
                    let server_info = self.server_info.clone();
                    tokio::spawn(
                        async move { stream_handler(stream, db, info_db, server_info).await },
                    );
                    println!("Tokio thread spawned");
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }

    pub async fn handshake(&self, bind_addr: SocketAddr, local_port: u16) -> Result<TcpStream> {
        // let bind_addr: String = host_addr.to_string() + ":" + host_port;
        loop {
            let Ok(stream) = TcpStream::connect(&bind_addr).await else {
                continue;
            };
            let (mut rd, mut wr) = io::split(stream);

            let mut handshake_args: Vec<Vec<u8>> = Vec::new();
            handshake_args
                .push(Type::Array(vec![Type::BulkString("ping".to_string())]).serialize());

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
                return Ok(rd.unsplit(wr));
            }

            // let _ = sync_replica_db();

            println!(
                "Handshake Post: {:?} Received",
                // str::from_utf8(&buffer[..len]).unwrap()
                &buffer[..len]
            );

            return Ok(rd.unsplit(wr));
        }
    }
    pub async fn call_init_info_db(&self) -> Result<()> {
        let info_db = &self.info_db;
        let role = self.server_info.lock().await.role.clone();
        let port = self.server_info.lock().await.addr.port();
        match &role {
            &Role::Master => {
                let _ = init_info_db(&info_db, None, port, role);
            }
            &Role::Slave(master_addr) => {
                let _ = init_info_db(&info_db, Some(master_addr), port, role);
            }
        }
        Ok(())
    }
}

type WriteHalf = io::WriteHalf<TcpStream>;
type ReadHalf = io::ReadHalf<TcpStream>;

pub async fn replicate(frame: Frame, server_info: Arc<tokio::sync::Mutex<ServerInfo>>) {
    let msg = frame.bytes_vec();
    // println!("Replicatiing: {:?}", msg);
    let replicas = &mut server_info.lock().await.replicas;
    for replica in replicas.iter_mut() {
        // println!("Replicating this replica: {:?}", replica);
        let msg = msg.clone();
        replica
            .write_all(&msg)
            .await
            .expect("failed to write to stream");
        // stream.flush().await.unwrap();
        let ten_millis = time::Duration::from_millis(10);
        thread::sleep(ten_millis);
        // let _ = replicate_send_and_receive(replica, msg).await.unwrap();
    }
}

// pub async fn replicate_send_and_receive(stream: &mut TcpStream, msg: Vec<u8>) -> Result<()> {
//     let (mut rd, mut wr) = io::split(stream);
//     if let Ok(_) = wr.write_all(&msg[..]).await {
//         let mut buffer: [u8; 1024] = [0; 1024];
//
//         let len = rd.read(&mut buffer).await?;
//
//         if len == 0 {
//             bail!("Nothing read from read buffer loop")
//         }
//
//         println!(
//             "Response: {:?}",
//             str::from_utf8(&buffer[..len]).unwrap() // &buffer[..len]
//         );
//     }
//     rd.unsplit(wr);
//     return Ok(());
// }

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
            "Response: {:?}",
            // str::from_utf8(&buffer[..len]).unwrap()
            &buffer[..len]
        );
    }
    Ok(())
}

// pub type Db = Arc<Mutex<Database>>;

async fn stream_handler(
    mut stream: TcpStream,
    db: Arc<Mutex<Database>>,
    info_db: Arc<Mutex<Database>>,
    server_info: Arc<tokio::sync::Mutex<ServerInfo>>,
) -> Result<()> {
    let mut buffer: [u8; 1024] = [0; 1024];
    loop {
        if let Ok(len) = stream.read(&mut buffer).await {
            if len == 0 {
                bail!("No bytes read from stream!");
            }

            let frame = Frame::new(&buffer, len)
                .context("creating frame from buffer")
                .unwrap();

            let frame_c = frame.clone();

            let responses = create_response(frame, &db, &info_db)
                .context("getting response from frame")
                .expect("failed to create response");

            for response in responses.into_iter() {
                let response_slice = &response[..];
                stream
                    .write_all(response_slice)
                    .await
                    .expect("failed to write to stream");
                // stream.flush().await.unwrap();
                let ten_millis = time::Duration::from_millis(10);
                thread::sleep(ten_millis);
            }
            match frame_c.command() {
                Command::Set => {
                    println!("Command SET");
                    let _ = replicate(frame_c, server_info.clone()).await;
                }
                Command::PSync => {
                    println!("Command PSYNC");
                    let mut server_info = server_info.lock().await;
                    server_info.replicas.push(stream);
                    return Ok(());
                }
                _ => {
                    println!("Command NOT FOUND");
                }
            }
        }
    }
}
