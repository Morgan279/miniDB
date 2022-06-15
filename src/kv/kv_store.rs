use std::path::Path;

use super::error::Result;
use super::storage::{SimplifiedBitcask, Storage};

pub struct KvStore {
    storage: Box<dyn Storage>,
}

impl KvStore {
    pub fn open(path: &Path) -> Result<KvStore> {
        let storage = SimplifiedBitcask::open(path.to_path_buf())?;
        Ok(KvStore {
            storage: Box::new(storage),
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.storage.get(key)
    }

    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        self.storage.put(key, val)
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.storage.remove(key)
    }
}
