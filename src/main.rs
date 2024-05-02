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

    println!("Listening at {}", &bind_addr);

    let server = Server::new(&args);
    // let stream = server.handshake(bind_addr, &args.port).await.unwrap();
    let _ = server.start().await.unwrap();

    // match &args.replicaof {
    //     Some(tokens) => {
    //         let (host, port) = tokens
    //             .into_iter()
    //             .collect_tuple()
    //             .context("parsing arguments for --replicaof flag")
    //             .unwrap();
    //         let master_addr: SocketAddr = format!("{}:{}", host, port).parse().unwrap();
    //         let server = Server::new(bind_addr, Role::Slave(master_addr));
    //         let _ = server.start(Some(stream), args);
    //     }
    //     None => {
    //         let server = Server::new(bind_addr, Role::Master);
    //         let _ = server.start(None, args);
    //     }
    // }
    // let _ = handshake(host, port, &args.port).await.unwrap();
}
