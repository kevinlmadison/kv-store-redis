use crate::command::*;
use crate::frame::*;
use crate::info::*;
use crate::resptype::*;
use anyhow::{anyhow, bail, Context, Result};
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub type Db = Arc<Mutex<HashMap<String, SetValue>>>;
pub type Response = Vec<Vec<u8>>;

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
        return Ok(
            Type::BulkString("(error) Incorrect number of arguments for get".to_string())
                .serialize(),
        );
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
    } else if args.len() == 4 {
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
    } else {
        println!("incorrect arg count");
    }
    Ok(Type::SimpleString("OK".to_string()).serialize())
}

fn handle_replconf(frame: Frame, info_db: &InfoDb) -> Result<Vec<u8>> {
    let mut info_db = info_db.lock().unwrap();
    let Some(args) = frame.args() else {
        return Err(anyhow!("Could not get frame args as Vec<Type>"));
    };
    if args.len() == 2 {
        let (key, val) = args
            .into_iter()
            .collect_tuple()
            .context("parsing arguments for replconf command")?;
        if key.to_lowercase().to_string() != "listening-port"
            && key.to_lowercase().to_string() != "capa"
        {
            bail!(
                "can only support listening-port or capa as extra command for replconf: key: {:?}",
                key
            );
        }
        info_db.insert(key.clone(), val);
        // println!("GETTING HERE IN REPLCONF: {:?}", info_db.get(&key).unwrap());
    } else {
        println!("incorrect arg count");
    }
    Ok(Type::SimpleString("OK".to_string()).serialize())
}

fn handle_psync(frame: Frame, info_db: &InfoDb) -> Result<Vec<u8>> {
    let info_db = info_db.lock().unwrap();
    let Some(args) = frame.args() else {
        return Err(anyhow!("Could not get frame args as Vec<Type>"));
    };
    if args.len() == 2 {
        let (id, offset) = args
            .into_iter()
            .collect_tuple()
            .context("parsing arguments for replconf command")?;
        if id.to_lowercase().to_string() != "?" && offset.to_lowercase().to_string() != "-1" {
            bail!(
                "can only support '?' or '-1' as args for psync: arg1: {:?} arg2: {:?}",
                id,
                offset,
            );
        }
        let rv_id: &str = info_db.get("master_replid").unwrap();
        let rv_offset: &str = info_db.get("master_repl_offset").unwrap();
        // println!("GETTING HERE IN REPLCONF: {:?}", rv_id);
        return Ok(
            Type::SimpleString("FULLRESYNC ".to_string() + rv_id + " " + rv_offset).serialize(),
        );
    } else {
        println!("incorrect arg count");
    }
    Ok(Type::SimpleString("OK".to_string()).serialize())
}

pub fn create_response(frame: Frame, db: &Db, info_db: &InfoDb) -> Result<Response> {
    match frame.command() {
        Command::Ping => {
            return Ok(vec![Type::SimpleString("PONG".to_string()).serialize()]);
        }

        Command::Echo => {
            let Some(args) = frame.args() else {
                bail!("Could not get frame args as Vec<Type>");
            };
            if args.len() > 1 {
                return Ok(vec![Type::BulkString(
                    "(error) Incorrect number of arguments for echo".to_string(),
                )
                .serialize()]);
            } else {
                let arg = args.first().context("getting echo arg")?;
                return Ok(vec![Type::BulkString(arg.to_string()).serialize()]);
            }
        }

        Command::Get => {
            let rv = handle_get(frame, db)?;
            return Ok(vec![rv]);
        }

        Command::Set => {
            let rv = handle_set(frame, db)?;
            return Ok(vec![rv]);
        }

        Command::Info => {
            let rv = handle_info(frame, info_db)?;
            return Ok(vec![rv]);
        }

        Command::ReplConf => {
            let rv = handle_replconf(frame, info_db)?;
            return Ok(vec![rv]);
        }

        Command::PSync => {
            let rv = handle_psync(frame, info_db)?;
            let rdb = Type::RDBSyncString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2".to_string()).serialize();
            return Ok(vec![rv, rdb]);
        }
    }
}
