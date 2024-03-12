// use anyhow::{anyhow, Error};
use std::str;
use std::error::Error;
use bytes::BytesMut;
use tokio::{
    io::{AsyncWriteExt, AsyncReadExt},
    net::{TcpListener, TcpStream},
};


type Cursor = usize;


#[derive(Debug)]
enum Type {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Type>),
}


impl Type {
    pub fn serialize(self) -> Vec<u8> {
        match self {
            Type::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            Type::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s).into_bytes(),
            Type::Array(elems) => elems.into_iter().flat_map(|v| v.serialize()).collect(),
        }
    }
}


#[derive(Debug)]
enum Command {
    Ping,
    Echo,
}


impl Command {
    pub fn from_string(s: &String) -> Option<Command> {
        match s.to_lowercase().as_str() {
            "ping" => Some(Command::Ping),
            "echo" => Some(Command::Echo),
            _ => None,
        }
    }
}


fn parse_simple_string(buffer: &[u8]) -> (Type, Cursor) {
    let (val, cursor) = parse_crlf(buffer);
    return (
        Type::SimpleString(str::from_utf8(&val).unwrap().to_string()),
        cursor
    );
}


fn parse_bulk_string(buffer: &[u8]) -> (Type, Cursor) {
    let (len_raw, cursor) = parse_crlf(buffer);
    let len = parse_usize(len_raw);
    let val = &buffer[cursor..(cursor + len)];
    println!("BULK TEST val: {:?}, len: {:?}", str::from_utf8(&val).unwrap().to_string(), len);
    return(
        Type::BulkString(str::from_utf8(&val).unwrap().to_string()),
        cursor + len + 2
    );
}


fn parse_array(buffer: &[u8]) -> (Type, Cursor) {
    let (num_elems_raw, mut cursor) = parse_crlf(buffer);
    let num_elems = parse_usize(num_elems_raw);
    println!("ARRAY TEST: {:?}", num_elems);
    let mut rv = Vec::<Type>::with_capacity(num_elems);
    for _ in 0..num_elems {
        let (elem, cursor_new) = parse_resp(&buffer[cursor..]);
        cursor += cursor_new + 1;
        rv.push(elem);
    }
    return(
        Type::Array(rv),
        cursor
    );
}


fn parse_crlf(buffer: &[u8]) -> (&[u8], Cursor) {
    let mut i: usize = 0;
    while i < buffer.len() && buffer[i] != b'\r' {
        i += 1;
    }
    return (&buffer[..i], (i + 2).min(buffer.len()));
}


fn parse_usize(buffer: &[u8]) -> usize {
    let num_elems_str = str::from_utf8(buffer).expect("parse usize: from utf8");
    // println!("num_elems_str: {:?}", num_elems_str);
    num_elems_str.parse::<usize>().expect("parse usize: rv")
}


fn parse_resp(buffer: &[u8]) -> (Type, Cursor) {
    println!("parse_resp: {:?}", str::from_utf8(&buffer[0..20]).expect("parse usize: from utf8"));
    match buffer[0] {
        b'+' => return parse_simple_string(&buffer[1..]),
        b'$' => return parse_bulk_string(&buffer[1..]),
        b'*' => return parse_array(&buffer[1..]),
        x => panic!("Invalid RESP Type: {:?}", x),
    };
}


fn parse_command(resp: &Type) -> Option<Command> {
    match resp {
        Type::SimpleString(s) => Command::from_string(s),
        Type::BulkString(s) => Command::from_string(s),
        Type::Array(elems) => parse_command(&elems[0]),
    }
}


fn create_response(t: Type, cmd: Command) -> Option<Vec<u8>> {
    match cmd {
        Command::Ping => Some(Type::SimpleString("PONG".to_string()).serialize()),
        Command::Echo => {
            if let Type::Array(mut myvec) = t {
                let rv = myvec.pop().unwrap();
                Some(rv.serialize())
            }
            else { None }
        }
    }
}


async fn stream_handler(mut stream: TcpStream) {
    let mut buffer: [u8; 1024] = [0; 1024];
    if let Ok(len) = stream.read(&mut buffer).await {
        if len == 0 { 
            println!("failed to parse any characters");
            return; 
        }
        let (resp, _) = parse_resp(&buffer);
        println!("resp: {:?}", resp);
        if let Some(command) = parse_command(&resp) {
            println!("command: {:?}", command);
            if let Some(response) = create_response(resp, command) {
                println!("response: {:?}", response);
                stream.write_all(&response[..]).await.unwrap();
            }

        }
    }
    ()
}


#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(async move { stream_handler(stream).await }); 
                println!("Handled the input");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
