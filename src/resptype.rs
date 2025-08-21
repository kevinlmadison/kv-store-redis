use anyhow::{bail, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use std::fmt::{Display, Formatter};
use std::num::ParseIntError;

const EMPTY_RDB_BASE64: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

fn get_rdb() -> Vec<u8> {
    STANDARD
        .decode(EMPTY_RDB_BASE64)
        .expect("Failed to decode RDB")
}

#[derive(Debug, Clone)]
pub enum Type {
    SimpleString(String),
    BulkString(String),
    RDBSyncString(Option<Vec<u8>>),
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
            Type::RDBSyncString(_) => {
                let s = get_rdb();
                f.write_fmt(format_args!("${}\r\n{:?}", s.len(), s))
            }
            Type::NullBulkString => f.write_fmt(format_args!("$-1\r\n")),
            Type::Integer(i) => f.write_fmt(format_args!(":{}\r\n", i)),
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
            }
            Type::SimpleString(s) => {
                let s = s.to_lowercase();
                Ok(String::from(s))
            }
            _ => bail!("Command parse error: {}", value.to_string()),
        }
    }
}

impl Type {
    pub fn serialize(self) -> Vec<u8> {
        match self {
            Type::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            Type::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s).into_bytes(),
            Type::RDBSyncString(s) => {
                let mut rv = Vec::new();
                let rdb = s.unwrap_or_else(get_rdb);
                let prefix: Vec<u8> = format!("${}\r\n", rdb.len()).into_bytes().to_vec();
                rv.extend(prefix);
                rv.extend(rdb);
                return rv;
            }
            Type::NullBulkString => format!("$-1\r\n").into_bytes(),
            Type::Integer(s) => format!(":{}\r\n", s).into_bytes(),
            Type::Array(elems) => {
                let mut prefix = format!("*{}\r\n", elems.len()).into_bytes();
                let s: Vec<u8> = elems.into_iter().flat_map(|v| v.serialize()).collect();
                prefix.extend(s);
                prefix
            }
        }
    }
}
