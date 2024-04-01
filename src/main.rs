use anyhow::{bail, Context, Result};
use clap::Parser;
use itertools::Itertools;
use std::collections::HashMap;
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

use command::*;
use flags::*;
use frame::*;
use info::*;
use replication::*;
use response::*;

async fn stream_handler(
    mut stream: TcpStream,
    db: Db,
    info_db: InfoDb,
    repl_streams: StreamVec,
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
                    let _ = replicate(frame_c, &repl_streams);
                }
                Command::PSync => {
                    let mut repl_streams = repl_streams.lock().unwrap();
                    repl_streams.push(stream);
                    return Ok(());
                }
                _ => (),
            }
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
    let repl_streams = Arc::new(Mutex::new(Vec::new()));
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
                let repl_streams = repl_streams.clone();
                tokio::spawn(
                    async move { stream_handler(stream, db, info_db, repl_streams).await },
                );
                println!("Tokio thread spawned");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
