use anyhow::{bail, Context, Result};
use clap::Parser;
use itertools::Itertools;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, default_value_t = String::from("127.0.0.1"))]
    pub addr: String,

    #[arg(short, long, default_value_t = String::from("6379"))]
    pub port: String,

    #[arg(required = false, short, long, num_args = 1)]
    pub replicaof: Option<String>,
}
