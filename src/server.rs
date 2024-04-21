use crate::command::*;
use crate::flags::*;
use crate::frame::*;
use crate::replication::*;
use crate::response::*;
use anyhow::{bail, Context, Result};
use itertools::Itertools;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{thread, time};
use tokio::{
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
    pub fn insert(mut self, key: String, val: DbEntry) -> Result<()> {
        self.db.insert(key, val);
        Ok(())
    }

    pub fn get(self, key: String) -> Result<DbEntry> {
        if let Some(val) = self.db.get(&key) {
            return Ok(val.clone());
        } else {
            return Ok(DbEntry::new("(nil)".into(), None));
        }
    }

    pub fn get_all(self) -> Result<Vec<String>> {
        Ok(self
            .db
            .clone()
            .into_iter()
            .map(|(k, v)| k.to_owned() + ":" + v.value().as_str() + "\n")
            .collect::<Vec<String>>())
    }
}
#[derive(Debug)]
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
    server_info: Arc<Mutex<ServerInfo>>,
}

impl Server {
    pub fn new(addr: SocketAddr, role: Role) -> Self {
        Self {
            server_info: Arc::new(Mutex::new(ServerInfo {
                replicas: Vec::default(),
                role,
                addr,
            })),
            redis_db: Arc::new(Mutex::new(Database::default())),
            info_db: Arc::new(Mutex::new(Database::default())),
        }
    }

    pub async fn start(self) -> Result<()> {
        let bind_addr = self.server_info.lock().unwrap().addr.clone();
        let listener = TcpListener::bind(&bind_addr).await.unwrap();
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
}

// pub type Db = Arc<Mutex<Database>>;

async fn stream_handler(
    mut stream: TcpStream,
    db: Arc<Mutex<Database>>,
    info_db: Arc<Mutex<Database>>,
    server_info: Arc<Mutex<ServerInfo>>,
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
                .unwrap();

            for response in responses.into_iter() {
                let response_slice = &response[..];
                stream.write_all(response_slice).await.unwrap();
                // stream.flush().await.unwrap();
                let ten_millis = time::Duration::from_millis(10);
                thread::sleep(ten_millis);
            }
            match frame_c.command() {
                Command::Set => {
                    println!("Command SET");
                    let _ = replicate(frame_c, &server_info).await;
                }
                Command::PSync => {
                    println!("Command PSYNC");
                    let mut server_info = server_info.lock().unwrap();
                    server_info.replicas.push(stream);
                    return Ok(());
                }
                _ => {
                    println!("Command PSYNC");
                }
            }
        }
    }
}
