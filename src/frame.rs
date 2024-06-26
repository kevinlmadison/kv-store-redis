use crate::command::*;
use crate::resptype::*;
use anyhow::{bail, Context, Result};
use itertools::Itertools;
use std::str;

pub type Cursor = usize;

#[derive(Debug, Clone)]
pub struct Frame {
    command: Command,
    args: Option<Vec<String>>,
    bytes_vec: Vec<u8>,
}

impl Frame {
    pub fn new(buffer: &[u8], len: usize) -> Result<Self> {
        let mut bytes_vec: Vec<u8> = Vec::new();
        for val in &buffer[..len] {
            let val_c = val;
            bytes_vec.push(*val_c);
        }

        let (resp, _) = parse_resp(buffer);

        let Type::Array(tokens) = resp else {
            bail!("unable to parse tokens from array")
        };
        let cmd = tokens.first().context("parsing first token for command")?;
        let cmd: Command = cmd.try_into().context("parsing command string")?;
        match cmd {
            Command::Ping => Ok(Self {
                command: cmd,
                args: None,
                bytes_vec,
            }),
            Command::Echo => {
                let (_, arg) = tokens
                    .into_iter()
                    .collect_tuple()
                    .context("parsing argument for echo command")?;
                let arg = arg.try_into().context("parsing arg from Type")?;

                Ok(Self {
                    command: cmd,
                    args: Some(vec![arg]),
                    bytes_vec,
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
                    args: Some(vec![arg]),
                    bytes_vec,
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
                        bytes_vec,
                    })
                } else if tokens.len() == 5 {
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
                        bytes_vec,
                    })
                } else {
                    bail!("Set command can only handle 2 or 4 arguments currently");
                }
            }
            Command::Info => {
                if tokens.len() == 1 {
                    Ok(Self {
                        command: cmd,
                        args: None,
                        bytes_vec,
                    })
                } else if tokens.len() == 2 {
                    let (_, arg) = tokens
                        .into_iter()
                        .collect_tuple()
                        .context("parsing argument for info command")?;
                    let arg = arg.try_into().context("parsing arg from Type")?;

                    Ok(Self {
                        command: cmd,
                        args: Some(vec![arg]),
                        bytes_vec,
                    })
                } else {
                    bail!("Info command can only handle 0 or 1 arguments currently");
                }
            }
            Command::ReplConf => {
                if tokens.len() == 3 {
                    let (_, arg1, arg2) = tokens
                        .into_iter()
                        .collect_tuple()
                        .context("parsing argument for replconf command")?;
                    let arg1 = arg1.try_into().context("parsing arg from Type")?;
                    let arg2 = arg2.try_into().context("parsing arg from Type")?;

                    Ok(Self {
                        command: cmd,
                        args: Some(vec![arg1, arg2]),
                        bytes_vec,
                    })
                } else {
                    bail!("ReplConf command can only handle 2 arguments currently");
                }
            }
            Command::PSync => {
                if tokens.len() == 3 {
                    let (_, arg1, arg2) = tokens
                        .into_iter()
                        .collect_tuple()
                        .context("parsing argument for psync command")?;
                    let arg1 = arg1.try_into().context("parsing arg from Type")?;
                    let arg2 = arg2.try_into().context("parsing arg from Type")?;

                    Ok(Self {
                        command: cmd,
                        args: Some(vec![arg1, arg2]),
                        bytes_vec,
                    })
                } else {
                    bail!("PSync command can only handle 2 arguments currently");
                }
            }
        }
    }

    pub fn command(&self) -> Command {
        self.command.clone()
    }

    pub fn args(&self) -> Option<Vec<String>> {
        self.args.clone()
    }

    pub fn bytes_vec(&self) -> Vec<u8> {
        self.bytes_vec.clone()
    }

    pub fn serialize(&self) -> Option<Vec<u8>> {
        None
    }
}

fn parse_integer(buffer: &[u8]) -> (Type, Cursor) {
    let (val, cursor) = parse_crlf(buffer);
    let val = str::from_utf8(&val).unwrap();
    return (Type::Integer(val.to_string()), cursor);
}

fn parse_simple_string(buffer: &[u8]) -> (Type, Cursor) {
    let (val, cursor) = parse_crlf(buffer);
    return (
        Type::SimpleString(str::from_utf8(&val).unwrap().to_string()),
        cursor,
    );
}

fn parse_bulk_string(buffer: &[u8]) -> (Type, Cursor) {
    let (len_raw, cursor) = parse_crlf(buffer);
    let len = parse_usize(len_raw);
    let val = &buffer[cursor..(cursor + len)];
    return (
        Type::BulkString(str::from_utf8(&val).unwrap().to_string()),
        cursor + len + 2,
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
    return (Type::Array(rv), cursor);
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
