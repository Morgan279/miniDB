use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use serde_repr::*;

use super::error::{KvsError, Result};

const STORAGE_FILE_PREFIX: &str = "miniDB";
const COMPACTION_THRESHOLD: u64 = 1 << 16;
const USIZE_LEN: usize = std::mem::size_of::<usize>();
const ENTRY_HEAD_LEN: usize = USIZE_LEN * 2 + 1;

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug)]
#[repr(u8)]
pub enum CmdKind {
    PUT = 1,
    DEL = 2,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Entry {
    key_len: usize,

    value_len: usize,

    key: String,

    value: String,

    kind: CmdKind,
}

impl Entry {
    pub fn new(key: String, value: String, kind: CmdKind) -> Entry {
        Entry {
            key_len: key.as_bytes().len(),
            value_len: value.as_bytes().len(),
            key,
            value,
            kind,
        }
    }

    pub fn size(&self) -> usize {
        ENTRY_HEAD_LEN + self.key_len + self.value_len
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0; self.size()];
        // encode key len
        buf[0..USIZE_LEN].copy_from_slice(&self.key_len.to_be_bytes());

        // encode value length
        buf[USIZE_LEN..USIZE_LEN * 2].copy_from_slice(&self.value_len.to_be_bytes());

        // encode kind
        buf[USIZE_LEN * 2..ENTRY_HEAD_LEN]
            .copy_from_slice(bincode::serialize(&self.kind).unwrap().as_slice());

        // encode key
        buf[ENTRY_HEAD_LEN..ENTRY_HEAD_LEN + self.key_len].copy_from_slice(self.key.as_bytes());

        // encode value
        buf[ENTRY_HEAD_LEN + self.key_len..].copy_from_slice(self.value.as_bytes());

        buf
    }

    pub fn decode(b: &[u8; ENTRY_HEAD_LEN]) -> Result<Entry> {
        let key_len = usize::from_be_bytes(b[0..USIZE_LEN].try_into()?);
        let value_len = usize::from_be_bytes(b[USIZE_LEN..USIZE_LEN * 2].try_into()?);
        let kind: CmdKind = bincode::deserialize(&b[USIZE_LEN * 2..ENTRY_HEAD_LEN])?;
        Ok(Entry {
            key_len,
            value_len,
            kind,
            key: String::new(),
            value: String::new(),
        })
    }
}

pub trait Storage {
    fn get(&mut self, key: String) -> Result<Option<String>>;

    fn put(&mut self, key: String, val: String) -> Result<()>;

    fn remove(&mut self, key: String) -> Result<()>;
}

pub struct SimplifiedBitcask {
    data_path_buf: PathBuf,

    reader: BufReaderWithPos<File>,

    writer: BufWriterWithPos<File>,

    index: HashMap<String, u64>,

    pending_compact: u64,
}

impl Storage for SimplifiedBitcask {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.read(&key) {
            Ok(e) => Ok(Some(e.value)),
            Err(KvsError::KeyNotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn put(&mut self, key: String, val: String) -> Result<()> {
        let e = Entry::new(key, val, CmdKind::PUT);
        self.write(e)?;
        if self.pending_compact >= COMPACTION_THRESHOLD {
            self.merge()?;
        }
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let e = Entry::new(key.clone(), String::new(), CmdKind::DEL);
            self.write(e)?;
            self.index.remove(&key);
            return Ok(());
        }

        Err(KvsError::KeyNotFound)
    }
}

impl SimplifiedBitcask {
    pub fn open(path_buf: PathBuf) -> Result<SimplifiedBitcask> {
        let data_path_buf = path_buf.join(STORAGE_FILE_PREFIX.to_string() + ".data");
        let writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(data_path_buf.as_path())?,
        )?;
        let reader = BufReaderWithPos::new(File::open(data_path_buf.as_path())?)?;
        let mut instance = SimplifiedBitcask {
            data_path_buf,
            reader,
            writer,
            index: HashMap::new(),
            pending_compact: 0,
        };
        instance.load_index()?;
        Ok(instance)
    }

    fn write(&mut self, entry: Entry) -> Result<()> {
        let key = entry.key.clone();
        if let Some(old_pos) = self.index.insert(key, self.writer.pos) {
            self.pending_compact += self.read_at(old_pos).unwrap().size() as u64;
        }
        let buf = entry.encode();
        self.writer.write(&buf)?;
        self.writer.flush()?;
        Ok(())
    }

    fn read(&mut self, key: &str) -> Result<Entry> {
        if let Some(offset) = self.index.get(key) {
            let pos = *offset;
            return self.read_at(pos);
        };

        Err(KvsError::KeyNotFound)
    }

    fn read_at(&mut self, offset: u64) -> Result<Entry> {
        self.reader.seek(SeekFrom::Start(offset))?;
        let mut buf: [u8; ENTRY_HEAD_LEN] = [0; ENTRY_HEAD_LEN];
        let len = self.reader.read(&mut buf)?;
        if len == 0 {
            return Err(KvsError::EOF);
        }
        let mut e = Entry::decode(&buf)?;

        let mut key_buf = vec![0; e.key_len];
        self.reader.read_exact(key_buf.as_mut_slice())?;
        e.key = String::from_utf8(key_buf)?;

        let mut val_buf = vec![0; e.value_len];
        self.reader.read_exact(val_buf.as_mut_slice())?;
        e.value = String::from_utf8(val_buf)?;

        Ok(e)
    }

    fn load_index(&mut self) -> Result<()> {
        let mut offset = 0;
        loop {
            match self.read_at(offset) {
                Ok(e) => {
                    let size = e.size() as u64;
                    match e.kind {
                        CmdKind::DEL => self.index.remove(&e.key),
                        CmdKind::PUT => self.index.insert(e.key, offset),
                    };
                    offset += size;
                }
                Err(KvsError::EOF) => {
                    self.writer.pos = offset;
                    return Ok(());
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    fn merge(&mut self) -> Result<()> {
        let mut offset = 0;
        let mut valid_entry = Vec::new();
        loop {
            match self.read_at(offset) {
                Ok(e) => {
                    let size = e.size() as u64;
                    if let Some(valid_pos) = self.index.get(&e.key) {
                        if e.kind == CmdKind::PUT && *valid_pos == offset {
                            valid_entry.push(e);
                        }
                    }
                    offset += size;
                }
                Err(KvsError::EOF) => {
                    break;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if !valid_entry.is_empty() {
            let mut data_path_ancestors = self.data_path_buf.ancestors();
            data_path_ancestors.next();
            let merge_path_buf = data_path_ancestors
                .next()
                .ok_or(KvsError::InvalidDataPath)?
                .join(STORAGE_FILE_PREFIX.to_string() + ".merge");
            let merge_file = File::create(merge_path_buf.as_path())?;
            let mut write_buf = BufWriterWithPos::new(merge_file)?;

            for e in &valid_entry {
                let key = e.key.clone();
                self.index.insert(key, write_buf.pos);
                write_buf.write(&e.encode())?;
            }

            self.writer = write_buf;
            self.reader = BufReaderWithPos::new(File::open(merge_path_buf.as_path())?)?;
            std::fs::remove_file(self.data_path_buf.as_path())?;
            std::fs::rename(merge_path_buf.as_path(), self.data_path_buf.as_path())?;
        }

        self.pending_compact = 0;
        Ok(())
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
