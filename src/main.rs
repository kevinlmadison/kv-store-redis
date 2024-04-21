use anyhow::{bail, Context, Result};
use clap::Parser;
use itertools::Itertools;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use std::{thread, time};

mod command;
mod flags;
mod frame;
mod info;
mod replication;
mod response;
mod resptype;
mod server;

use command::*;
use flags::*;
use frame::*;
use info::*;
use replication::*;
use response::*;
use server::*;

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let args = Args::parse();
    let bind_addr: SocketAddr = format!("{}:{}", args.addr, args.port).parse().unwrap();

    let db = Arc::new(Mutex::new(Database::default()));
    let info_db = Arc::new(Mutex::new(Database::default()));
    let _: () = init_info_db(&info_db, &args).unwrap();

    // let listener = TcpListener::bind(&bind_addr).await.unwrap();
    println!("Listening at {}", &bind_addr);

    match &args.replicaof {
        Some(tokens) => {
            let (host, port) = tokens
                .into_iter()
                .collect_tuple()
                .context("parsing arguments for --replicaof flag")
                .unwrap();
            let master_addr: SocketAddr = format!("{}:{}", host, port).parse().unwrap();
            let server = Server::new(bind_addr, Role::Slave(master_addr));
        }
        None => {
            let server = Server::new(bind_addr, Role::Master);
        }
    }
    let _ = handshake(host, port, &args.port).await.unwrap();
}
