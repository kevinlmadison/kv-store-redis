use itertools::Itertools;
use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use std::fmt::{Formatter, Display};
use std::str;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncWriteExt, AsyncReadExt},
    net::{TcpListener, TcpStream},
};


type Cursor = usize;
type Db = Arc<Mutex<HashMap<String, SetValue>>>;

#[derive(Debug, Clone)]
enum Command {
    Ping,
    Echo,
    Get,
    Set,
}

impl TryFrom<&Type> for Command {
    type Error = anyhow::Error;

    fn try_from(value: &Type) -> Result<Self> {
        match value {
            Type::BulkString(s) => {
                let s = s.to_lowercase();
                if s == "ping" {
                    Ok(Command::Ping)
                } else if s == "echo" {
                    Ok(Command::Echo)
                } else if s == "set" {
                    Ok(Command::Set)
                } else if s == "get" {
                    Ok(Command::Get)
                } else {
                    bail!("Command not supported: {}", s)
                }
            }
            _ => bail!("Command parse error: {}", value.to_string()),
        }
    }
}


impl TryFrom<Type> for String {
    type Error = anyhow::Error;
    fn try_from(value: Type) -> Result<Self> {
        match value {
            Type::BulkString(s) => {
                let s = s.to_lowercase();
                Ok(String::from(s))
            },
            Type::SimpleString(s) => {
                let s = s.to_lowercase();
                Ok(String::from(s))
            },
            _ => bail!("Command parse error: {}", value.to_string()),
        }
    }
}


#[derive(Debug, Clone)]
struct Frame {
    command: Command,
    args: Option<Vec<String>>,
    resp: Type,
}


impl Frame {
    pub fn new(buffer: &[u8]) -> Result<Self> {
        let (resp, _) = parse_resp(buffer);
        let resp_clone = resp.clone();

        let Type::Array(tokens) = resp_clone else {
            bail!("unable to parse tokens from array")
        };
        // let cmd = cmd_args[0].clone(); 
        let cmd = tokens.first().context("parsing first token for command")?;
        let cmd: Command = cmd.try_into().context("parsing command string")?;
        match cmd {
            Command::Ping => {
                Ok(Self {
                    command: cmd,
                    args: None,
                    resp: resp,
                })
            },
            Command::Echo => {
                let (_, arg) = tokens
                    .into_iter()
                    .collect_tuple()
                    .context("parsing argument for echo command")?;
                let arg = arg.try_into().context("parsing arg from Type")?;

                Ok(Self {
                    command: cmd,
                    args: Some(vec!(arg)),
                    resp: resp,
                })
            }
            Command::Get => {
                let (_, arg) = tokens
                    .into_iter()
                    .collect_tuple()
                    .context("parsing argument for get command")?;
                let arg = arg.try_into().context("parsing arg from Type")?;

                Ok(Self {
                    command: cmd,
                    args: Some(vec!(arg)),
                    resp: resp,
                })
            }
            Command::Set => {
                if tokens.len() == 3 {
                    let (_, key, val) = tokens
                        .into_iter()
                        .collect_tuple()
                        .context("parsing argument for set command")?;
                    let key = key.try_into().context("parsing key from Type")?;
                    let val = val.try_into().context("parsing val from Type")?;

                    Ok(Self {
                        command: cmd,
                        args: Some(vec![key, val]),
                        resp: resp,
                    })
                }
                else if tokens.len() == 5 {
                    let (_, key, val, px, dur) = tokens
                        .into_iter()
                        .collect_tuple()
                        .context("parsing argument for set command")?;
                    let key = key.try_into().context("parsing key from Type")?;
                    let val = val.try_into().context("parsing val from Type")?;
                    let px = px.try_into().context("parsing px from Type")?;
                    let dur = dur.try_into().context("parsing duration from Type")?;

                    Ok(Self {
                        command: cmd,
                        args: Some(vec![key, val, px, dur]),
                        resp: resp,
                    })
                }
                else {
                    bail!("Set command can only handle 2 or 4 arguments currently");
                }
            }
            _ => bail!("Failed to parse a command"),
        }
    }

    pub fn command(&self) -> Command {
        self.command.clone()
    }

    pub fn resp(&self) -> Type {
        self.resp.clone()
    }

    pub fn args(&self) -> Option<Vec<String>> {
        self.args.clone()
    }
}


#[derive(Debug, Clone)]
enum Type {
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    Integer(String),
    Array(Vec<Type>),
}


impl Display for Type {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    match self {
      Type::Array(items) => {
        let elements: String = items.iter().map(|e| e.to_string()).collect();
        f.write_fmt(format_args!("*{}\r\n{}", items.len(), elements))
      }
      Type::SimpleString(s) => f.write_fmt(format_args!("+{}\r\n", s)),
      Type::BulkString(s) => f.write_fmt(format_args!("${}\r\n{}\r\n", s.len(), s)),
      Type::NullBulkString => f.write_fmt(format_args!("$-1\r\n")),
      Type::Integer(i) => f.write_fmt(format_args!(":{}\r\n", i)),
    }
  }
}

impl Type {
    pub fn serialize(self) -> Vec<u8> {
        match self {
            Type::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            Type::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s).into_bytes(),
            Type::NullBulkString => format!("$-1\r\n").into_bytes(),
            Type::Integer(s) => format!(":{}\r\n", s).into_bytes(),
            Type::Array(elems) => {
                let mut prefix = format!("*{}\r\n", elems.len()).into_bytes();
                let s: Vec<u8> = elems.into_iter().flat_map(|v| v.serialize()).collect();
                prefix.extend(s);
                prefix
            },
        }
    }
}


#[derive(Debug, Clone)]
struct SetValue {
    value: String,
    expiry: Option<Instant>,
}

impl SetValue {
    pub fn new(s: String) -> Self {
        Self {
            value: s,
            expiry: None,
        }
    }

    pub fn new_with_expiry(s: String, ex: Duration) -> Self {
        Self {
            value: s,
            expiry: Some(Instant::now() + ex),
        }
    }
}


fn parse_integer(buffer: &[u8]) -> (Type, Cursor) {
    let (val, cursor) = parse_crlf(buffer);
    let val = str::from_utf8(&val).unwrap();
    return (
        Type::Integer(val.to_string()),
        cursor
    );
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


fn parse_resp(buffer: &[u8]) -> (Type, Cursor) {
    match buffer[0] {
        b'+' => return parse_simple_string(&buffer[1..]),
        b'$' => return parse_bulk_string(&buffer[1..]),
        b':' => return parse_integer(&buffer[1..]),
        b'*' => return parse_array(&buffer[1..]),
        x => panic!("Invalid RESP Type: {:?}", x),
    };
}


fn handle_get(frame: Frame, db: &Db) -> Result<Vec<u8>> {
    let db = db.lock().unwrap();
    let Some(args) = frame.args() else {
        bail!("Could not get frame args as Vec<Type>");
    };
    if args.len() > 1 {
        return Ok(Type::BulkString(
                "(error) Incorrect number of arguments for get".to_string()
                ).serialize());
    }

    let key = args.first().context("getting get key")?;
    let Some(val) = db.get(key) else {
        return Ok(Type::NullBulkString.serialize());
    };

    match val.expiry {
        Some(expiry) => {
            if expiry <= Instant::now() {
                return Ok(Type::NullBulkString.serialize());
            } else {
                return Ok(Type::BulkString(val.value.to_string()).serialize());
            }
        }
        None => {
            return Ok(Type::BulkString(val.value.to_string()).serialize());
        }
    }
}


fn handle_set(frame: Frame, db: &Db) -> Result<Vec<u8>> {

    println!("handling set command");
    let mut db = db.lock().unwrap();
    let Some(args) = frame.args() else {
        return Err(anyhow!("Could not get frame args as Vec<Type>"));
    };
    if args.len() == 2 {
        let (key, val) = args
            .into_iter()
            .collect_tuple()
            .context("parsing argument for set command")?;
        let set_val = SetValue::new(val);
        db.insert(key, set_val);
    }
    else if args.len() == 4 {
        let (key, val, px, dur) = args
            .into_iter()
            .collect_tuple()
            .context("parsing argument for set command")?;
        if px.to_lowercase().to_string() != "px" {
            bail!("can only support px as extra command for set");
        } 
        let dur = dur.parse::<u64>().context("parsing u64 from string")?;
        let set_val = SetValue::new_with_expiry(val, Duration::from_millis(dur));
        db.insert(key, set_val);
    }
    else {
        println!("incorrect arg count");
    }
    Ok(Type::SimpleString("OK".to_string()).serialize())
}


fn create_response(frame: Frame, db: &Db) -> Result<Vec<u8>> {
    match frame.command() {

        Command::Ping => {
            return Ok(Type::SimpleString("PONG".to_string()).serialize());
        },

        Command::Echo => {
            let Some(args) = frame.args() else {
                bail!("Could not get frame args as Vec<Type>");
            };
            if args.len() > 1 {
                return Ok(Type::BulkString(
                        "(error) Incorrect number of arguments for echo".to_string())
                        .serialize());
            } else {
                let arg = args.first().context("getting echo arg")?;
                return Ok(Type::BulkString(arg.to_string()).serialize());
            }
        },

        Command::Get=> {
            return handle_get(frame, db);
        },

        Command::Set => {
            return handle_set(frame, db);
        },
    }
}

async fn stream_handler(mut stream: TcpStream, db: Db) -> Result<()> {
    let mut buffer: [u8; 1024] = [0; 1024];
    loop {
        if let Ok(len) = stream.read(&mut buffer).await {
            if len == 0 { return Err(anyhow!("No bytes read from stream!")); }

            let frame = Frame::new(&buffer).context("creating frame from buffer")?;
            let response = create_response(frame, &db).context("getting response from frame")?;

            let response_slice = &response[..];
            println!("response: {:?}", str::from_utf8(response_slice).unwrap());
            stream.write_all(response_slice).await.unwrap();
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
