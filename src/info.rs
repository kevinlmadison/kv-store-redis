use crate::flags::*;
use crate::frame::*;
use crate::resptype::*;
use crate::server::*;
use anyhow::{bail, Context, Result};
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const MASTER_DEFAULTS: [(&str, &str); 5] = [
    ("role", "master"),
    ("tcp_port", "6379"),
    ("connected_slaves", "0"),
    ("master_replid", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
    ("master_repl_offset", "0"),
];

const SLAVE_DEFAULTS: [(&str, &str); 7] = [
    ("role", "slave"),
    ("tcp_port", "6380"),
    ("master_host", "127.0.0.1"),
    ("master_port", "6379"),
    ("connected_slaves", "0"),
    ("master_replid", "?"),
    ("master_repl_offset", "-1"),
];

const ALL_ARGS: [&str; 7] = [
    "role",
    "tcp_port",
    "master_host",
    "master_port",
    "connected_slaves",
    "master_replid",
    "master_repl_offset",
];

const REPLICATION_ARGS: [&str; 7] = [
    "role",
    "tcp_port",
    "master_host",
    "master_port",
    "connected_slaves",
    "master_replid",
    "master_repl_offset",
];

pub type Db = Arc<Mutex<Database>>;

pub fn init_info_db(info_db: &Db, args: &Args) -> Result<()> {
    let defaults: Vec<(&str, &str)> = match args.replicaof {
        Some(_) => SLAVE_DEFAULTS.to_vec(),
        None => MASTER_DEFAULTS.to_vec(),
    };
    let mut info_db = info_db.lock().unwrap();

    for (k, v) in defaults {
        let db_entry: DbEntry = DbEntry::new(v.to_owned(), None);
        info_db.insert(k.to_owned(), db_entry);
    }
    if let Some(tokens) = &args.replicaof {
        let (host, port) = tokens
            .into_iter()
            .collect_tuple()
            .context("parsing arguments for --replicaof flag")?;

        let host: String = host.try_into().context("parsing host from &str")?;
        let db_entry: DbEntry = DbEntry::new(host.to_owned(), None);
        info_db.insert("master_host".to_owned(), db_entry);

        let port: String = port.try_into().context("parsing port from &str")?;
        let db_entry: DbEntry = DbEntry::new(port.to_owned(), None);
        info_db.insert("master_port".to_owned(), db_entry);
    }

    let db_entry: DbEntry = DbEntry::new(args.port.to_owned(), None);
    info_db.insert("tcp_port".to_owned(), db_entry);

    Ok(())
}

enum InfoQuery {
    Replication,
    All,
    Test,
}

impl TryFrom<String> for InfoQuery {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self> {
        match value.as_str() {
            "replication" => Ok(InfoQuery::Replication),
            "all" => Ok(InfoQuery::All),
            "test" => Ok(InfoQuery::Test),
            _ => Ok(InfoQuery::All),
        }
    }
}

impl TryFrom<&str> for InfoQuery {
    type Error = anyhow::Error;
    fn try_from(value: &str) -> Result<Self> {
        match value {
            "replication" => Ok(InfoQuery::Replication),
            "all" => Ok(InfoQuery::All),
            "test" => Ok(InfoQuery::Test),
            _ => Ok(InfoQuery::Test),
        }
    }
}

fn info_query(query: InfoQuery, info_db: &Db) -> Result<Vec<u8>> {
    match query {
        InfoQuery::Replication => {
            let rv: Vec<String> = REPLICATION_ARGS
                .to_vec()
                .iter()
                .map(|elem| elem.to_string())
                .collect::<Vec<String>>();

            let info_db = info_db.lock().unwrap();

            let rv = rv
                .iter()
                .map(|k| {
                    k.to_owned() + ":" + info_db.get(k.to_owned()).unwrap().value().as_str() + "\n"
                })
                .collect::<Vec<String>>();

            let rv = rv
                .iter()
                .map(|elem| elem.to_string())
                .reduce(|cur, nxt| cur.to_owned() + &nxt)
                .unwrap()
                .to_string();
            Ok(Type::BulkString(rv).serialize())
        }
        InfoQuery::All => {
            let rv: Vec<String> = ALL_ARGS
                .to_vec()
                .iter()
                .map(|elem| elem.to_string())
                .collect::<Vec<String>>();
            let info_db = info_db.lock().unwrap();

            let rv = rv
                .iter()
                .map(|k| {
                    k.to_owned() + ":" + info_db.get(k.to_owned()).unwrap().value().as_str() + "\n"
                })
                .collect::<Vec<String>>();

            let rv = rv
                .iter()
                .map(|elem| elem.to_string())
                .reduce(|cur, nxt| cur.to_owned() + &nxt)
                .unwrap()
                .to_string();
            Ok(Type::BulkString(rv).serialize())
        }
        InfoQuery::Test => {
            let info_db = info_db.lock().unwrap();

            let rv = info_db.get_all().unwrap();

            let rv = rv
                .iter()
                .map(|elem| elem.to_string())
                .reduce(|cur, nxt| cur.to_owned() + &nxt)
                .unwrap()
                .to_string();
            Ok(Type::BulkString(rv).serialize())
        }
    }
}

pub fn handle_info(frame: Frame, info_db: &Db) -> Result<Vec<u8>> {
    println!("handling info command");
    // let mut info_db = info_db.lock().unwrap();
    if let Some(mut args) = frame.args() {
        if args.len() == 1 {
            let query = args.pop().context("parsing argument for info command")?;
            match query.to_lowercase().as_str() {
                "replication" => {
                    return info_query(query.try_into()?, info_db);
                }
                "all" => {
                    return info_query(query.try_into()?, info_db);
                }
                "test" => {
                    return info_query(query.try_into()?, info_db);
                }
                _ => {
                    bail!("can only support replication as arg for info");
                }
            }
        } else {
            return info_query("all".try_into()?, info_db);
        }
    } else {
        return info_query("all".try_into()?, info_db);
    }
}
