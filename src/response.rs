use crate::frame::*;
use crate::command::*;
use crate::resptype::*;
use crate::info::*;
use itertools::Itertools;
use anyhow::{anyhow, bail, Context, Result};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub type Db = Arc<Mutex<HashMap<String, SetValue>>>;


#[derive(Debug, Clone)]
pub struct SetValue {
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


pub fn create_response(frame: Frame, db: &Db, info_db: &InfoDb) -> Result<Vec<u8>> {
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

        Command::Info => {
            return handle_info(frame, info_db);
        },

        Command::ReplConf => {
            Ok(Type::SimpleString("OK".to_string()).serialize())
        }
    }
}

