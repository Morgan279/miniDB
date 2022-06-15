extern crate failure;
extern crate serde;

pub use kv::error::{KvsError, Result};
pub use kv::kv_store::KvStore;

pub mod kv;
