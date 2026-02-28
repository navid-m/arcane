use crate::error::{ArcaneError, Result};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const MAGIC: &[u8; 8] = b"ARCANE01";
pub const HEADER_SIZE: u64 = 64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldType {
    String,
    Int,
    Float,
    Bool,
    Bytes,
}

impl FieldType {
    pub fn name(&self) -> &'static str {
        match self {
            FieldType::String => "string",
            FieldType::Int => "int",
            FieldType::Float => "float",
            FieldType::Bool => "bool",
            FieldType::Bytes => "bytes",
        }
    }
}

impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: String,
    pub ty: FieldType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub bucket_name: String,
    pub fields: Vec<FieldDef>,
}

impl Schema {
    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Bytes(Vec<u8>),
    Null,
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::Int(_) => "int",
            Value::Float(_) => "float",
            Value::Bool(_) => "bool",
            Value::Bytes(_) => "bytes",
            Value::Null => "null",
        }
    }

    /// Raw bytes fed into the hash function.
    pub fn hash_bytes(&self) -> Vec<u8> {
        match self {
            Value::String(s) => s.as_bytes().to_vec(),
            Value::Int(i) => i.to_le_bytes().to_vec(),
            Value::Float(f) => f.to_bits().to_le_bytes().to_vec(),
            Value::Bool(b) => vec![*b as u8],
            Value::Bytes(b) => b.clone(),
            Value::Null => vec![0xFF],
        }
    }

    /// Whether the type matches.
    pub fn matches_type(&self, ty: &FieldType) -> bool {
        matches!(
            (self, ty),
            (Value::String(_), FieldType::String)
                | (Value::Int(_), FieldType::Int)
                | (Value::Float(_), FieldType::Float)
                | (Value::Bool(_), FieldType::Bool)
                | (Value::Bytes(_), FieldType::Bytes)
                | (Value::Null, _)
        )
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(s) => write!(f, "{}", s),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(v) => write!(f, "{}", v),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Bytes(b) => write!(f, "<{} bytes>", b.len()),
            Value::Null => write!(f, "__null__"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Record {
    pub hash: u64,
    pub fields: Vec<Value>,
}

impl Record {
    pub fn new(fields: Vec<Value>) -> Self {
        let hash = compute_hash(&fields);
        Record { hash, fields }
    }

    pub fn get_field(&self, idx: usize) -> Option<&Value> {
        self.fields.get(idx)
    }

    /// Returns (field_index, value) pairs for non-null values for serialization.
    pub fn serialized_fields(&self) -> Vec<(usize, &Value)> {
        self.fields
            .iter()
            .enumerate()
            .filter(|(_, v)| !matches!(v, Value::Null))
            .map(|(i, v)| (i, v))
            .collect()
    }

    /// Reconstruct full fields vector from deserialized (index, value) pairs.
    pub fn reconstruct_fields(num_fields: usize, sparse: Vec<(usize, Value)>) -> Vec<Value> {
        let mut fields = vec![Value::Null; num_fields];
        for (idx, val) in sparse {
            if idx < num_fields {
                fields[idx] = val;
            }
        }
        fields
    }
}

pub fn compute_hash(fields: &[Value]) -> u64 {
    use xxhash_rust::xxh3::Xxh3;
    let mut h = Xxh3::new();
    for v in fields {
        if matches!(v, Value::Null) {
            continue;
        }
        h.update(&v.hash_bytes());
        h.update(&[0x00]);
    }
    h.digest()
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed)]
struct IndexEntry {
    hash: u64,
    offset: u64,
}

const INDEX_ENTRY_SIZE: usize = std::mem::size_of::<IndexEntry>();

pub struct BucketStore {
    /// The schema of the bucket.
    pub schema: Schema,

    /// Path to the index file.
    idx_path: PathBuf,

    /// In-memory index: sorted Vec of (hash, file_offset).
    /// It's protected by the bucket-level RwLock on the BucketStore itself.
    index: Vec<(u64, u64)>,

    /// The data file handle.
    data_file: File,

    /// Current write position in the data file.
    write_pos: u64,

    /// The record count of the bucket.
    record_count: u64,
}

impl BucketStore {
    /// Create a brand-new bucket on disk.
    pub fn create(dir: &Path, schema: Schema) -> Result<Self> {
        let name = &schema.bucket_name;
        let data_path = dir.join(format!("{}.arc", name));
        let idx_path = dir.join(format!("{}.arc.idx", name));

        if data_path.exists() {
            return Err(ArcaneError::BucketExists(name.clone()));
        }

        let mut data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&data_path)?;

        let header = Self::build_header(0, HEADER_SIZE, 0);
        data_file.write_all(&header)?;

        let schema_bytes = bincode::serialize(&schema)?;
        let schema_off = HEADER_SIZE;
        data_file.write_all(&schema_bytes)?;

        let data_off = schema_off + schema_bytes.len() as u64;
        let mut header = Self::build_header(0, data_off, 0);

        header[24..32].copy_from_slice(&schema_off.to_le_bytes());
        data_file.seek(SeekFrom::Start(0))?;
        data_file.write_all(&header)?;
        data_file.seek(SeekFrom::Start(data_off))?;

        File::create(&idx_path)?;

        Ok(BucketStore {
            schema,
            idx_path,
            index: Vec::new(),
            data_file,
            write_pos: data_off,
            record_count: 0,
        })
    }

    /// Open an existing bucket from disk.
    pub fn open(dir: &Path, bucket_name: &str) -> Result<Self> {
        let data_path = dir.join(format!("{}.arc", bucket_name));
        let idx_path = dir.join(format!("{}.arc.idx", bucket_name));

        if !data_path.exists() {
            return Err(ArcaneError::BucketNotFound(bucket_name.to_string()));
        }

        let mut data_file = OpenOptions::new().read(true).write(true).open(&data_path)?;
        let mut hdr = [0u8; HEADER_SIZE as usize];

        data_file.read_exact(&mut hdr)?;

        if &hdr[0..8] != MAGIC {
            return Err(ArcaneError::Other("Invalid magic bytes".into()));
        }

        let schema_off = u64::from_le_bytes(hdr[24..32].try_into().unwrap());
        let data_off = u64::from_le_bytes(hdr[32..40].try_into().unwrap());
        let record_count = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
        let schema_len = (data_off - schema_off) as usize;

        data_file.seek(SeekFrom::Start(schema_off))?;

        let mut schema_bytes = vec![0u8; schema_len];
        data_file.read_exact(&mut schema_bytes)?;
        let schema: Schema = bincode::deserialize(&schema_bytes)?;

        let index = if idx_path.exists() {
            Self::load_index(&idx_path)?
        } else {
            Vec::new()
        };

        let write_pos = data_file.seek(SeekFrom::End(0))?;
        let mut store = BucketStore {
            schema,
            idx_path,
            index,
            data_file,
            write_pos,
            record_count,
        };

        if store.index.is_empty() && record_count > 0 {
            store.rebuild_index(data_off)?;
        }

        Ok(store)
    }

    fn load_index(path: &Path) -> Result<Vec<(u64, u64)>> {
        let mut f = File::open(path)?;
        let meta = f.metadata()?;
        let n = meta.len() as usize / INDEX_ENTRY_SIZE;
        let mut idx = Vec::with_capacity(n);
        let mut buf = [0u8; INDEX_ENTRY_SIZE];
        for _ in 0..n {
            if f.read_exact(&mut buf).is_err() {
                break;
            }
            let hash = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let offset = u64::from_le_bytes(buf[8..16].try_into().unwrap());
            idx.push((hash, offset));
        }
        idx.sort_unstable_by_key(|e| e.0);
        Ok(idx)
    }

    fn rebuild_index(&mut self, data_off: u64) -> Result<()> {
        self.data_file.seek(SeekFrom::Start(data_off))?;
        let mut index = Vec::new();
        let mut pos = data_off;
        let file_len = self.data_file.seek(SeekFrom::End(0))?;
        self.data_file.seek(SeekFrom::Start(data_off))?;

        while pos < file_len {
            let mut rec_hdr = [0u8; 13];

            if self.data_file.read_exact(&mut rec_hdr).is_err() {
                break;
            }

            let hash = u64::from_le_bytes(rec_hdr[0..8].try_into().unwrap());
            let alive = rec_hdr[8];
            let len = u32::from_le_bytes(rec_hdr[9..13].try_into().unwrap()) as u64;

            if alive == 0x01 {
                index.push((hash, pos));
            }
            self.data_file.seek(SeekFrom::Current(len as i64))?;
            pos += 13 + len;
        }
        index.sort_unstable_by_key(|e| e.0);
        self.index = index;
        self.flush_index()?;
        Ok(())
    }

    /// Insert a record. Returns its hash.
    pub fn insert(&mut self, record: Record) -> Result<u64> {
        if self.index_contains(record.hash) {
            return Err(ArcaneError::DuplicateRecord(
                record.hash,
                self.schema.bucket_name.clone(),
            ));
        }

        let offset = self.write_pos;
        let serialized: Vec<(usize, Value)> = record
            .serialized_fields()
            .into_iter()
            .map(|(i, v)| (i, v.clone()))
            .collect();
        let field_bytes = bincode::serialize(&serialized)?;
        let len = field_bytes.len() as u32;
        let mut buf = Vec::with_capacity(13 + field_bytes.len());

        buf.extend_from_slice(&record.hash.to_le_bytes());
        buf.push(0x01);
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&field_bytes);

        self.data_file.seek(SeekFrom::Start(offset))?;
        self.data_file.write_all(&buf)?;
        self.write_pos += buf.len() as u64;
        self.record_count += 1;

        let pos = self.index.partition_point(|e| e.0 < record.hash);
        self.index.insert(pos, (record.hash, offset));

        let mut idx_file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.idx_path)?;

        let mut entry = [0u8; INDEX_ENTRY_SIZE];
        entry[0..8].copy_from_slice(&record.hash.to_le_bytes());
        entry[8..16].copy_from_slice(&offset.to_le_bytes());
        idx_file.write_all(&entry)?;

        self.data_file.seek(SeekFrom::Start(8))?;
        self.data_file.write_all(&self.record_count.to_le_bytes())?;

        Ok(record.hash)
    }

    /// Bulk insert multiple records efficiently.
    ///
    /// Returns the number of records inserted.
    ///
    /// This method batches I/O operations for performance on large datasets.
    pub fn bulk_insert(&mut self, records: Vec<Record>) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }

        for record in &records {
            if self.index_contains(record.hash) {
                return Err(ArcaneError::DuplicateRecord(
                    record.hash,
                    self.schema.bucket_name.clone(),
                ));
            }
        }

        let mut data_buffer = Vec::new();
        let mut index_entries = Vec::with_capacity(records.len());
        let mut offset = self.write_pos;

        for record in &records {
            let serialized: Vec<(usize, Value)> = record
                .serialized_fields()
                .into_iter()
                .map(|(i, v)| (i, v.clone()))
                .collect();
            let field_bytes = bincode::serialize(&serialized)?;
            let len = field_bytes.len() as u32;

            data_buffer.extend_from_slice(&record.hash.to_le_bytes());
            data_buffer.push(0x01);
            data_buffer.extend_from_slice(&len.to_le_bytes());
            data_buffer.extend_from_slice(&field_bytes);
            index_entries.push((record.hash, offset));

            offset += (13 + field_bytes.len()) as u64;
        }

        self.data_file.seek(SeekFrom::Start(self.write_pos))?;
        self.data_file.write_all(&data_buffer)?;
        self.write_pos = offset;
        self.record_count += records.len() as u64;

        for (hash, off) in &index_entries {
            let pos = self.index.partition_point(|e| e.0 < *hash);
            self.index.insert(pos, (*hash, *off));
        }

        let mut idx_file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.idx_path)?;

        for (hash, off) in &index_entries {
            let mut entry = [0u8; INDEX_ENTRY_SIZE];
            entry[0..8].copy_from_slice(&hash.to_le_bytes());
            entry[8..16].copy_from_slice(&off.to_le_bytes());
            idx_file.write_all(&entry)?;
        }

        self.data_file.seek(SeekFrom::Start(8))?;
        self.data_file.write_all(&self.record_count.to_le_bytes())?;

        Ok(records.len())
    }

    /// Delete a record by hash (mark as deleted)
    pub fn delete(&mut self, hash: u64) -> Result<bool> {
        match self.index.binary_search_by_key(&hash, |e| e.0) {
            Ok(idx) => {
                let offset = self.index[idx].1;
                self.data_file.seek(SeekFrom::Start(offset + 8))?;
                self.data_file.write_all(&[0x00])?;
                self.index.remove(idx);
                self.record_count = self.record_count.saturating_sub(1);
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    /// Truncate all records
    pub fn truncate(&mut self) -> Result<()> {
        self.index.clear();
        self.record_count = 0;
        let data_off = self.data_off()?;
        self.data_file.set_len(data_off)?;
        self.write_pos = data_off;
        self.data_file.seek(SeekFrom::Start(8))?;
        self.data_file.write_all(&0u64.to_le_bytes())?;
        self.flush_index()?;
        Ok(())
    }

    /// Read a record by its file offset.
    pub fn read_at(&mut self, offset: u64) -> Result<Option<Record>> {
        self.data_file.seek(SeekFrom::Start(offset))?;
        let mut hdr = [0u8; 13];
        if self.data_file.read_exact(&mut hdr).is_err() {
            return Ok(None);
        }
        let hash = u64::from_le_bytes(hdr[0..8].try_into().unwrap());
        let alive = hdr[8];
        let len = u32::from_le_bytes(hdr[9..13].try_into().unwrap()) as usize;

        if alive != 0x01 {
            return Ok(None);
        }

        let mut field_bytes = vec![0u8; len];
        self.data_file.read_exact(&mut field_bytes)?;
        let sparse: Vec<(usize, Value)> = bincode::deserialize(&field_bytes)?;
        let fields = Record::reconstruct_fields(self.schema.fields.len(), sparse);
        Ok(Some(Record { hash, fields }))
    }

    /// Scan all live records.
    pub fn scan_all(&mut self) -> Result<Vec<Record>> {
        let offsets: Vec<u64> = self.index.iter().map(|e| e.1).collect();
        let mut records = Vec::with_capacity(offsets.len());
        for off in offsets {
            if let Some(r) = self.read_at(off)? {
                records.push(r);
            }
        }
        Ok(records)
    }

    /// Lookup by hash — O(log n).
    pub fn get_by_hash(&mut self, hash: u64) -> Result<Option<Record>> {
        match self.index.binary_search_by_key(&hash, |e| e.0) {
            Ok(i) => self.read_at(self.index[i].1),
            Err(_) => Ok(None),
        }
    }

    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    pub fn index_contains(&self, hash: u64) -> bool {
        self.index.binary_search_by_key(&hash, |e| e.0).is_ok()
    }

    /// Flush the entire in-memory index to disk (used after bulk loads).
    fn flush_index(&self) -> Result<()> {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.idx_path)?;
        for (hash, off) in &self.index {
            let mut entry = [0u8; INDEX_ENTRY_SIZE];
            entry[0..8].copy_from_slice(&hash.to_le_bytes());
            entry[8..16].copy_from_slice(&off.to_le_bytes());
            f.write_all(&entry)?;
        }
        Ok(())
    }

    fn build_header(record_count: u64, data_off: u64, flags: u16) -> Vec<u8> {
        let mut h = vec![0u8; HEADER_SIZE as usize];
        h[0..8].copy_from_slice(MAGIC);
        h[8..16].copy_from_slice(&record_count.to_le_bytes());
        h[16..18].copy_from_slice(&1u16.to_le_bytes());
        h[18..20].copy_from_slice(&flags.to_le_bytes());
        h[32..40].copy_from_slice(&data_off.to_le_bytes());
        h
    }

    pub fn data_off(&mut self) -> Result<u64> {
        let mut hdr = [0u8; HEADER_SIZE as usize];
        self.data_file.seek(SeekFrom::Start(0))?;
        self.data_file.read_exact(&mut hdr)?;
        Ok(u64::from_le_bytes(hdr[32..40].try_into().unwrap()))
    }

    /// Add a new field to the schema. Existing rows implicitly have Value::Null for this field.
    pub fn add_field(&mut self, field: FieldDef) -> Result<()> {
        self.schema.fields.push(field);
        let new_schema_bytes = bincode::serialize(&self.schema)?;
        let mut hdr = [0u8; HEADER_SIZE as usize];
        self.data_file.seek(SeekFrom::Start(0))?;
        self.data_file.read_exact(&mut hdr)?;

        let old_schema_off = u64::from_le_bytes(hdr[24..32].try_into().unwrap());
        let old_data_off = u64::from_le_bytes(hdr[32..40].try_into().unwrap());
        let old_schema_len = old_data_off - old_schema_off;

        if new_schema_bytes.len() as u64 > old_schema_len {
            let new_data_off = old_schema_off + new_schema_bytes.len() as u64;
            let data_len = self.write_pos - old_data_off;
            let mut data_buf = vec![0u8; data_len as usize];

            self.data_file.seek(SeekFrom::Start(old_data_off))?;
            self.data_file.read_exact(&mut data_buf)?;
            self.data_file.seek(SeekFrom::Start(old_schema_off))?;
            self.data_file.write_all(&new_schema_bytes)?;
            self.data_file.write_all(&data_buf)?;
            self.write_pos = new_data_off + data_len;

            hdr[32..40].copy_from_slice(&new_data_off.to_le_bytes());

            self.data_file.seek(SeekFrom::Start(0))?;
            self.data_file.write_all(&hdr)?;
            self.rebuild_index(new_data_off)?;
        } else {
            self.data_file.seek(SeekFrom::Start(old_schema_off))?;
            self.data_file.write_all(&new_schema_bytes)?;
        }

        self.data_file.flush()?;
        Ok(())
    }

    /// Flush all in-memory data to disk.
    ///
    /// This includes the index and ensures the data file is synced.
    pub fn flush(&mut self) -> Result<()> {
        self.flush_index()?;
        self.data_file.flush()?;

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            unsafe {
                libc::fdatasync(self.data_file.as_raw_fd());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_value_type_matching() {
        assert!(Value::String("test".into()).matches_type(&FieldType::String));
        assert!(Value::Int(42).matches_type(&FieldType::Int));
        assert!(Value::Float(3.14).matches_type(&FieldType::Float));
        assert!(Value::Bool(true).matches_type(&FieldType::Bool));
        assert!(Value::Null.matches_type(&FieldType::String));
        assert!(!Value::String("test".into()).matches_type(&FieldType::Int));
    }

    #[test]
    fn test_compute_hash() {
        let fields1 = vec![Value::String("Alice".into()), Value::Int(30)];
        let fields2 = vec![Value::String("Alice".into()), Value::Int(30)];
        let fields3 = vec![Value::String("Bob".into()), Value::Int(30)];

        assert_eq!(compute_hash(&fields1), compute_hash(&fields2));
        assert_ne!(compute_hash(&fields1), compute_hash(&fields3));
    }

    #[test]
    fn test_compute_hash_ignores_null() {
        let fields1 = vec![Value::String("Alice".into()), Value::Null];
        let fields2 = vec![Value::String("Alice".into())];
        assert_eq!(compute_hash(&fields1), compute_hash(&fields2));
    }

    #[test]
    fn test_record_serialized_fields() {
        let fields = vec![Value::String("Alice".into()), Value::Null, Value::Int(30)];
        let record = Record::new(fields);
        let serialized = record.serialized_fields();

        assert_eq!(serialized.len(), 2);
        assert_eq!(serialized[0].0, 0);
        assert_eq!(serialized[1].0, 2);
    }

    #[test]
    fn test_record_reconstruct_fields() {
        let sparse = vec![(0, Value::String("Alice".into())), (2, Value::Int(30))];
        let fields = Record::reconstruct_fields(3, sparse);

        assert_eq!(fields.len(), 3);
        assert!(matches!(fields[0], Value::String(_)));
        assert!(matches!(fields[1], Value::Null));
        assert!(matches!(fields[2], Value::Int(30)));
    }

    #[test]
    fn test_bucket_create_and_open() {
        let dir = TempDir::new().unwrap();
        let schema = Schema {
            bucket_name: "TestBucket".into(),
            fields: vec![
                FieldDef {
                    name: "name".into(),
                    ty: FieldType::String,
                },
                FieldDef {
                    name: "age".into(),
                    ty: FieldType::Int,
                },
            ],
        };

        let store = BucketStore::create(dir.path(), schema.clone()).unwrap();
        drop(store);

        let store = BucketStore::open(dir.path(), "TestBucket").unwrap();
        assert_eq!(store.schema.bucket_name, "TestBucket");
        assert_eq!(store.schema.fields.len(), 2);
    }

    #[test]
    fn test_bucket_insert_and_read() {
        let dir = TempDir::new().unwrap();
        let schema = Schema {
            bucket_name: "TestBucket".into(),
            fields: vec![FieldDef {
                name: "data".into(),
                ty: FieldType::String,
            }],
        };

        let mut store = BucketStore::create(dir.path(), schema).unwrap();
        let record = Record::new(vec![Value::String("test data".into())]);
        let hash = store.insert(record.clone()).unwrap();

        let retrieved = store.get_by_hash(hash).unwrap().unwrap();
        assert_eq!(retrieved.hash, record.hash);
        assert_eq!(retrieved.fields.len(), 1);
    }

    #[test]
    fn test_bucket_duplicate_detection() {
        let dir = TempDir::new().unwrap();
        let schema = Schema {
            bucket_name: "TestBucket".into(),
            fields: vec![FieldDef {
                name: "data".into(),
                ty: FieldType::String,
            }],
        };

        let mut store = BucketStore::create(dir.path(), schema).unwrap();
        let record = Record::new(vec![Value::String("test".into())]);

        store.insert(record.clone()).unwrap();
        let result = store.insert(record);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ArcaneError::DuplicateRecord(_, _)
        ));
    }

    #[test]
    fn test_bucket_scan_all() {
        let dir = TempDir::new().unwrap();
        let schema = Schema {
            bucket_name: "TestBucket".into(),
            fields: vec![FieldDef {
                name: "id".into(),
                ty: FieldType::Int,
            }],
        };

        let mut store = BucketStore::create(dir.path(), schema).unwrap();

        for i in 0..10 {
            let record = Record::new(vec![Value::Int(i)]);
            store.insert(record).unwrap();
        }

        let records = store.scan_all().unwrap();
        assert_eq!(records.len(), 10);
    }

    #[test]
    fn test_schema_evolution() {
        let dir = TempDir::new().unwrap();
        let schema = Schema {
            bucket_name: "TestBucket".into(),
            fields: vec![FieldDef {
                name: "name".into(),
                ty: FieldType::String,
            }],
        };

        let mut store = BucketStore::create(dir.path(), schema).unwrap();
        let record1 = Record::new(vec![Value::String("Alice".into())]);
        store.insert(record1).unwrap();

        store
            .add_field(FieldDef {
                name: "age".into(),
                ty: FieldType::Int,
            })
            .unwrap();

        assert_eq!(store.schema.fields.len(), 2);

        let record2 = Record::new(vec![Value::String("Bob".into()), Value::Int(30)]);
        store.insert(record2).unwrap();
        let records = store.scan_all().unwrap();
        assert_eq!(records.len(), 2);

        let alice_record = records
            .iter()
            .find(|r| matches!(r.fields[0], Value::String(ref s) if s == "Alice"))
            .expect("Alice record not found");

        assert_eq!(alice_record.fields.len(), 2);
        assert!(matches!(alice_record.fields[1], Value::Null));

        let bob_record = records
            .iter()
            .find(|r| matches!(r.fields[0], Value::String(ref s) if s == "Bob"))
            .expect("Bob record not found");

        assert_eq!(bob_record.fields.len(), 2);
        assert!(matches!(bob_record.fields[1], Value::Int(30)));
    }

    #[test]
    fn test_schema_field_index() {
        let schema = Schema {
            bucket_name: "Test".into(),
            fields: vec![
                FieldDef {
                    name: "first".into(),
                    ty: FieldType::String,
                },
                FieldDef {
                    name: "second".into(),
                    ty: FieldType::Int,
                },
            ],
        };

        assert_eq!(schema.field_index("first"), Some(0));
        assert_eq!(schema.field_index("second"), Some(1));
        assert_eq!(schema.field_index("nonexistent"), None);
    }
}
