extern crate kvs;

use std::env::current_dir;
use std::process::exit;

use clap::{Parser, Subcommand};
use kvs::{KvStore, KvsError};

#[derive(Debug, Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// set <KEY> <VALUE>
    #[clap(arg_required_else_help = true)]
    Set { key: String, val: String },

    /// get <KEY>
    #[clap(arg_required_else_help = true)]
    Get { key: String },

    /// rm <KEY>
    #[clap(arg_required_else_help = true)]
    #[clap(name = "rm")]
    Remove { key: String },
}

fn main() {
    let args = Cli::parse();
    let mut kv_store = KvStore::open(current_dir().unwrap().as_path()).unwrap();

    match args.command {
        Command::Get { key } => {
            if let Some(val) = kv_store.get(key).unwrap() {
                println!("{}", val);
            } else {
                println!("Key not found");
            }
        }
        Command::Set { key, val } => {
            if let Err(err) = kv_store.set(key, val) {
                eprintln!("{:?}", err);
            }
        }
        Command::Remove { key } => {
            if let Err(KvsError::KeyNotFound) = kv_store.remove(key) {
                println!("Key not found");
                exit(1);
            }
        }
    }
}
