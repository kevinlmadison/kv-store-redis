use anyhow::{anyhow, Result};
use std::str;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncWriteExt, AsyncReadExt},
    net::{TcpListener, TcpStream},
};


type Cursor = usize;
type Db = Arc<Mutex<HashMap<String, String>>>;


#[derive(Debug, Clone)]
struct Frame {
    command: Type,
    args: Vec<Type>,
    resp: Type,
}


impl Frame {
    pub fn new(buffer: &[u8]) -> Option<Self> {
        let (resp, _) = parse_resp(buffer);
        let resp_clone = resp.clone();

        if let Type::Array(cmd_args) = resp_clone {
            let cmd = cmd_args[0].clone(); 
            let args: Vec<Type> = cmd_args[1..].to_vec();
            Some(Self {
                command: cmd,
                args: args,
                resp: resp,
            })
        } else { None }
    }

    pub fn resp_as_type(&self) -> Option<Type> {
        Some(self.resp.clone())
    }
    
    pub fn resp_as_bytes(&self) -> Option<Vec<u8>> {
        Some(self.resp.clone().serialize())
    }

    pub fn args_as_vec_type(&self) -> Option<Vec<Type>> {
        Some(self.args.clone())
    }

    pub fn args_as_vec(&self) -> Option<Vec<Vec<u8>>> {
        Some(self.args.clone().into_iter().map(|v| v.serialize()).collect())
    }

    pub fn cmd_as_command(&self) -> Option<Command> {
        parse_command(&self.command.clone())
    }

    pub fn cmd_as_type(&self) -> Option<Type> {
        Some(self.command.clone())
    }

    pub fn cmd_as_bytes(&self) -> Option<Vec<u8>> {
        Some(self.command.clone().serialize())
    }

    pub fn cmd_as_string(&self) -> Option<String> {
        match self.cmd_as_command() {
            Some(Command::Ping) => Some(String::from("ping")),
            Some(Command::Echo) => Some(String::from("echo")),
            Some(Command::Get)  => Some(String::from("get")),
            Some(Command::Set)  => Some(String::from("set")),
            _ => None,
        }
    }
}


fn parse_resp(buffer: &[u8]) -> (Type, Cursor) {
    match buffer[0] {
        b'+' => return parse_simple_string(&buffer[1..]),
        b'$' => return parse_bulk_string(&buffer[1..]),
        b'*' => return parse_array(&buffer[1..]),
        x => panic!("Invalid RESP Type: {:?}", x),
    };
}


#[derive(Debug, Clone)]
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
            Type::Array(elems) => {
                let mut prefix = format!("*{}\r\n", elems.len()).into_bytes();
                let s: Vec<u8> = elems.into_iter().flat_map(|v| v.serialize()).collect();
                prefix.extend(s);
                prefix
            },
        }
    }
}


#[derive(Debug)]
enum Command {
    Ping,
    Echo,
    Get,
    Set,
}


impl Command {
    pub fn from_string(s: &String) -> Option<Command> {
        match s.to_lowercase().as_str() {
            "ping" => Some(Command::Ping),
            "echo" => Some(Command::Echo),
            "get"  => Some(Command::Get),
            "set"  => Some(Command::Set),
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
    return(
        Type::BulkString(str::from_utf8(&val).unwrap().to_string()),
        cursor + len + 2
    );
}


fn parse_array(buffer: &[u8]) -> (Type, Cursor) {
    let (num_elems_raw, mut cursor) = parse_crlf(buffer);
    let num_elems = parse_usize(num_elems_raw);
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
    num_elems_str.parse::<usize>().expect("parse usize: rv")
}


fn parse_command(resp: &Type) -> Option<Command> {
    match resp {
        Type::SimpleString(s) => Command::from_string(s),
        Type::BulkString(s) => Command::from_string(s),
        Type::Array(elems) => parse_command(&elems[0]),
    }
}


async fn stream_handler(mut stream: TcpStream, db: Db) -> Result<(), anyhow::Error> {
    let mut buffer: [u8; 1024] = [0; 1024];
    loop {
        if let Ok(len) = stream.read(&mut buffer).await {
            if len == 0 { return Err(anyhow!("No bytes read from stream!")); }

            let Some(frame) = Frame::new(&buffer) else {
                return Err(anyhow!("Could not create new frame from buffer"));
            };
            let Some(response) = (match frame.cmd_as_command() {

                Some(Command::Ping) => Some(Type::SimpleString("PONG".to_string()).serialize()),
                Some(Command::Echo) => {
                    let Some(args) = frame.args_as_vec_type() else {
                        return Err(anyhow!("Could not get frame args as Vec<Type>"));
                    };
                    Some(Type::Array(args).serialize())
                }
                Some(Command::Get)=> {
                    let db = db.lock().unwrap();
                    let Some(mut args) = frame.args_as_vec_type() else {
                        return Err(anyhow!("Could not get frame args as Vec<Type>"));
                    };
                    let Some(Type::BulkString(key)) = args.pop() else {
                        return Err(anyhow!("Could not pop arg from vec"));
                    };
                    let Some(val) = db.get(&key) else {
                        return Err(anyhow!("Could not get item from kv store"));
                    };
                    println!("{:?}", val);
                    Some(Type::BulkString(val.to_string()).serialize())
                },
                Some(Command::Set) => {
                    let mut db = db.lock().unwrap();
                    let Some(mut args) = frame.args_as_vec_type() else {
                        return Err(anyhow!("Could not get frame args as Vec<Type>"));
                    };
                    let Some(Type::BulkString(val)) = args.pop() else {
                        return Err(anyhow!("Could not pop val from vec"));
                    };
                    let Some(Type::BulkString(key)) = args.pop() else {
                        return Err(anyhow!("Could not pop key from vec"));
                    };
                    db.insert(key, val);
                    Some(Type::SimpleString("OK".to_string()).serialize())
                },
                _ => None,
            }) else {
                return Err(anyhow!("Could not match on frame arguments"));
            };
            println!("response: {:?}", str::from_utf8(&response[..]).unwrap());
            stream.write_all(&response[..]).await.unwrap();
        }
    }
}


#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let db = db.clone();
                tokio::spawn(async move { stream_handler(stream, db).await }); 
                println!("Tokio thread spawned");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
