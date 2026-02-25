//! Write-Ahead Log or WAL.
//!
//! Entry types:
//!   0x01 = CreateBucket  (payload = bincode Schema)
//!   0x02 = Insert        (payload = bincode WalInsert)
//!   0x03 = Commit        (payload = empty)
//!   0x04 = Checkpoint    (payload = empty)
//!
//! On startup, any entries after the last Commit are replayed to recover.
//! After enough entries accumulate, a Checkpoint is written and the WAL
//! is truncated.

use crate::error::{ArcaneError, Result};
use crate::storage::{Schema, Value};
use crc32fast::Hasher as Crc32;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const WAL_MAGIC: &[u8; 8] = b"ARCWAL01";

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EntryType {
    CreateBucket = 0x01,
    Insert = 0x02,
    Commit = 0x03,
    Checkpoint = 0x04,
}

impl TryFrom<u8> for EntryType {
    type Error = ArcaneError;
    fn try_from(v: u8) -> Result<Self> {
        match v {
            0x01 => Ok(Self::CreateBucket),
            0x02 => Ok(Self::Insert),
            0x03 => Ok(Self::Commit),
            0x04 => Ok(Self::Checkpoint),
            _ => Err(ArcaneError::Wal(format!("Unknown entry type: {}", v))),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WalInsert {
    pub bucket: String,
    pub hash: u64,
    pub fields: Vec<Value>,
}

#[derive(Debug)]
pub enum WalEntry {
    CreateBucket(Schema),
    Insert(WalInsert),
    Commit,
    Checkpoint,
}

/// Thread-safe WAL writer.
pub struct Wal {
    file: Mutex<File>,
    seq: AtomicU64,
    path: std::path::PathBuf,
}

impl Wal {
    pub fn open(dir: &Path) -> Result<Self> {
        let path = dir.join("arcane.wal");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let wal = Wal {
            file: Mutex::new(file),
            seq: AtomicU64::new(0),
            path,
        };
        {
            let mut f = wal.file.lock();
            let len = f.seek(SeekFrom::End(0))?;
            if len == 0 {
                f.write_all(WAL_MAGIC)?;
                f.flush()?;
            } else {
                let last_seq = Self::scan_last_seq(&mut f)?;
                wal.seq.store(last_seq, Ordering::Relaxed);
            }
        }

        Ok(wal)
    }

    pub fn append_create_bucket(&self, schema: &Schema) -> Result<u64> {
        let payload = bincode::serialize(schema)?;
        self.write_entry(EntryType::CreateBucket, &payload)
    }

    pub fn append_insert(&self, insert: &WalInsert) -> Result<u64> {
        let payload = bincode::serialize(insert)?;
        self.write_entry(EntryType::Insert, &payload)
    }

    pub fn append_commit(&self) -> Result<u64> {
        self.write_entry(EntryType::Commit, &[])
    }

    pub fn append_checkpoint(&self) -> Result<u64> {
        self.write_entry(EntryType::Checkpoint, &[])
    }

    fn write_entry(&self, ty: EntryType, payload: &[u8]) -> Result<u64> {
        let seq = self.seq.fetch_add(1, Ordering::AcqRel) + 1;
        let crc = Self::crc(seq, ty as u8, payload);
        let len = payload.len() as u32;

        let mut buf = Vec::with_capacity(17 + payload.len());
        buf.extend_from_slice(&seq.to_le_bytes());
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.push(ty as u8);
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(payload);

        let mut f = self.file.lock();
        f.seek(SeekFrom::End(0))?;
        f.write_all(&buf)?;
        f.flush()?;

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            libc_fdatasync(f.as_raw_fd());
        }

        Ok(seq)
    }

    /// Replay WAL entries for recovery. Returns entries since last Checkpoint.
    pub fn replay(dir: &Path) -> Result<Vec<WalEntry>> {
        let path = dir.join("arcane.wal");
        if !path.exists() {
            return Ok(vec![]);
        }

        let mut f = File::open(&path)?;
        let mut magic = [0u8; 8];
        if f.read_exact(&mut magic).is_err() || &magic != WAL_MAGIC {
            return Ok(vec![]);
        }

        let mut entries = Vec::new();
        let mut last_checkpoint = 0usize;

        loop {
            // Well...it's seq:8 + crc:4 + type:1 + len:4
            let mut hdr = [0u8; 17];

            if f.read_exact(&mut hdr).is_err() {
                break;
            }

            let seq = u64::from_le_bytes(hdr[0..8].try_into().unwrap());
            let stored_crc = u32::from_le_bytes(hdr[8..12].try_into().unwrap());
            let ty_byte = hdr[12];
            let len = u32::from_le_bytes(hdr[13..17].try_into().unwrap()) as usize;
            let mut payload = vec![0u8; len];

            if f.read_exact(&mut payload).is_err() {
                break;
            }

            let ty = match EntryType::try_from(ty_byte) {
                Ok(t) => t,
                Err(_) => break,
            };

            let computed = Self::crc(seq, ty_byte, &payload);

            if computed != stored_crc {
                // Ah shit, looks like a torn write. Stop replay here.
                break;
            }

            let entry = match ty {
                EntryType::CreateBucket => {
                    let schema: Schema = bincode::deserialize(&payload)?;
                    WalEntry::CreateBucket(schema)
                }
                EntryType::Insert => {
                    let ins: WalInsert = bincode::deserialize(&payload)?;
                    WalEntry::Insert(ins)
                }
                EntryType::Commit => WalEntry::Commit,
                EntryType::Checkpoint => {
                    last_checkpoint = entries.len();
                    WalEntry::Checkpoint
                }
            };
            entries.push(entry);
        }
        Ok(entries.into_iter().skip(last_checkpoint).collect())
    }

    /// Truncate WAL (called after checkpoint flush to data files).
    pub fn truncate(&self) -> Result<()> {
        let mut f = self.file.lock();
        f.seek(SeekFrom::Start(0))?;
        f.set_len(0)?;
        f.write_all(WAL_MAGIC)?;
        f.flush()?;
        Ok(())
    }

    /// Compute CRC32 checksum for WAL entry.
    fn crc(seq: u64, ty: u8, payload: &[u8]) -> u32 {
        let mut h = Crc32::new();
        h.update(&seq.to_le_bytes());
        h.update(&[ty]);
        h.update(payload);
        h.finalize()
    }

    /// Scan WAL file for last sequence number.
    fn scan_last_seq(f: &mut File) -> Result<u64> {
        let mut seq = 0u64;
        f.seek(SeekFrom::Start(8))?;
        loop {
            let mut hdr = [0u8; 17];
            if f.read_exact(&mut hdr).is_err() {
                break;
            }
            let s = u64::from_le_bytes(hdr[0..8].try_into().unwrap());
            let len = u32::from_le_bytes(hdr[13..17].try_into().unwrap()) as i64;
            if f.seek(SeekFrom::Current(len)).is_err() {
                break;
            }
            seq = s;
        }
        Ok(seq)
    }
}

#[cfg(unix)]
fn libc_fdatasync(fd: i32) {
    unsafe {
        libc::fdatasync(fd);
    }
}
