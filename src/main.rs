use anyhow::{bail, Context, Result};
use clap::Parser;
use itertools::Itertools;
use std::collections::HashMap;
use std::str;
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod command;
mod flags;
mod frame;
mod handshake;
mod info;
mod response;
mod resptype;

use flags::*;
use frame::*;
use handshake::*;
use info::*;
use response::*;

async fn stream_handler(mut stream: TcpStream, db: Db, info_db: InfoDb) -> Result<()> {
    let mut buffer: [u8; 1024] = [0; 1024];
    loop {
        if let Ok(len) = stream.read(&mut buffer).await {
            if len == 0 {
                bail!("No bytes read from stream!");
            }

            let frame = Frame::new(&buffer)
                .context("creating frame from buffer")
                .unwrap();

            let response = create_response(frame, &db, &info_db)
                .context("getting response from frame")
                .unwrap();

            let response_slice = &response[..];
            // println!("response: {:?}", str::from_utf8(response_slice).unwrap());
            stream.write_all(response_slice).await.unwrap();
        }
    }
}

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let args = Args::parse();
    let bind_addr = format!("{}:{}", args.addr, args.port);

    let db = Arc::new(Mutex::new(HashMap::new()));
    let info_db = Arc::new(Mutex::new(HashMap::new()));
    let _: () = init_info_db(&info_db, &args).unwrap();

    let listener = TcpListener::bind(&bind_addr).await.unwrap();
    println!("Listening at {}", &bind_addr);

    if let Some(tokens) = &args.replicaof {
        let (host, port) = tokens
            .into_iter()
            .collect_tuple()
            .context("parsing arguments for --replicaof flag")
            .unwrap();
        let _ = handshake(host, port, &args.port).await.unwrap();
    }

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let db = db.clone();
                let info_db = info_db.clone();
                tokio::spawn(async move { stream_handler(stream, db, info_db).await });
                println!("Tokio thread spawned");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
