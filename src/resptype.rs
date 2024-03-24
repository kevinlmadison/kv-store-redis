use anyhow::{bail, Result};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub enum Type {
    SimpleString(String),
    BulkString(String),
    RDBSyncString(String),
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
            Type::RDBSyncString(s) => f.write_fmt(format_args!("${}\r\n{}", s.len(), s)),
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
            Type::RDBSyncString(s) => format!("${}\r\n{}", s.len(), s).into_bytes(),
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
