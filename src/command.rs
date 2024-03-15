use crate::resptype::*;
use anyhow::{bail, Context, Result};


#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Echo,
    Get,
    Set,
    Info,
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
                } else if s == "info" {
                    Ok(Command::Info)
                } else {
                    bail!("Command not supported: {}", s)
                }
            }
            _ => bail!("Command parse error: {}", value.to_string()),
        }
    }
}
