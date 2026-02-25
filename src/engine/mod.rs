use crate::error::{ArcaneError, Result};
use crate::parser::{self, Filter, Projection, Statement};
use crate::storage::{BucketStore, FieldDef, Record, Schema, Value};
use crate::wal::{Wal, WalInsert};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Config {
    /// How many WAL entries before an automatic checkpoint.
    pub checkpoint_interval: u64,

    /// Disable fsync for benchmarks (NOT crash-safe).
    pub no_sync: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            checkpoint_interval: 10_000,
            no_sync: false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum QueryResult {
    BucketCreated {
        name: String,
    },
    Inserted {
        bucket: String,
        hash: u64,
    },
    Rows {
        schema: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    Hashes(Vec<u64>),
    Empty,
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryResult::BucketCreated { name } => {
                writeln!(f, "Bucket '{}' created.", name)
            }
            QueryResult::Inserted { bucket, hash } => {
                writeln!(f, "Inserted into '{}' with hash {:#016x}.", bucket, hash)
            }
            QueryResult::Rows { schema, rows } => {
                if rows.is_empty() {
                    return writeln!(f, "(0 rows)");
                }
                let mut widths: Vec<usize> = schema.iter().map(|s| s.len()).collect();
                for row in rows {
                    for (i, cell) in row.iter().enumerate() {
                        if i < widths.len() {
                            widths[i] = widths[i].max(cell.len());
                        }
                    }
                }
                let header: Vec<String> = schema
                    .iter()
                    .enumerate()
                    .map(|(i, s)| format!("{:<width$}", s, width = widths[i]))
                    .collect();

                writeln!(f, "{}", header.join("  "))?;
                writeln!(f, "{}", "─".repeat(header.join("  ").len()))?;

                for row in rows {
                    let cells: Vec<String> = row
                        .iter()
                        .enumerate()
                        .map(|(i, s)| {
                            let w = widths.get(i).copied().unwrap_or(s.len());
                            format!("{:<width$}", s, width = w)
                        })
                        .collect();
                    writeln!(f, "{}", cells.join("  "))?;
                }
                writeln!(f, "({} rows)", rows.len())
            }
            QueryResult::Hashes(hashes) => {
                for h in hashes {
                    writeln!(f, "{:#016x}", h)?;
                }
                Ok(())
            }
            QueryResult::Empty => Ok(()),
        }
    }
}

/// It's a per-bucket RwLock
type BucketHandle = Arc<RwLock<BucketStore>>;

pub struct Database {
    dir: PathBuf,
    buckets: DashMap<String, BucketHandle>,
    wal: Arc<Wal>,
}

impl Database {
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Arc<Self>> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let wal = Arc::new(Wal::open(&dir)?);
        let buckets: DashMap<String, BucketHandle> = DashMap::new();

        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "arc") {
                if path.to_str().map_or(false, |s| s.ends_with(".arc.idx")) {
                    continue;
                }
                if let Some(stem) = path.file_stem() {
                    let name = stem.to_string_lossy().to_string();
                    let store = BucketStore::open(&dir, &name)?;
                    buckets.insert(name, Arc::new(RwLock::new(store)));
                }
            }
        }

        let db = Arc::new(Database {
            dir: dir.clone(),
            buckets,
            wal,
        });

        db.recover()?;

        Ok(db)
    }

    /// Execute a single-line AQL statement.
    pub fn execute(&self, line: &str) -> Result<QueryResult> {
        let stmt = parser::parse_statement(line)?;
        self.execute_stmt(stmt)
    }

    /// Execute a pre-parsed statement (useful when embedding).
    pub fn execute_stmt(&self, stmt: Statement) -> Result<QueryResult> {
        match stmt {
            Statement::CreateBucket { name, fields } => self.create_bucket(name, fields),
            Statement::Insert { bucket, values } => self.insert(bucket, values),
            Statement::Get {
                bucket,
                projection,
                filter,
            } => self.get(bucket, projection, filter),
        }
    }

    /// Execute all statements in an .arc script.
    pub fn execute_script(&self, src: &str) -> Vec<Result<QueryResult>> {
        parser::parse_script(src)
            .into_iter()
            .map(|r| r.and_then(|s| self.execute_stmt(s)))
            .collect()
    }

    fn create_bucket(&self, name: String, fields: Vec<FieldDef>) -> Result<QueryResult> {
        if self.buckets.contains_key(&name) {
            return Err(ArcaneError::BucketExists(name));
        }

        let schema = Schema {
            bucket_name: name.clone(),
            fields,
        };

        self.wal.append_create_bucket(&schema)?;
        self.wal.append_commit()?;

        let store = BucketStore::create(&self.dir, schema)?;
        self.buckets
            .insert(name.clone(), Arc::new(RwLock::new(store)));

        Ok(QueryResult::BucketCreated { name })
    }

    fn insert(&self, bucket: String, values: Vec<(Option<String>, Value)>) -> Result<QueryResult> {
        let handle = self
            .buckets
            .get(&bucket)
            .ok_or_else(|| ArcaneError::BucketNotFound(bucket.clone()))?;

        let all_named = values.iter().all(|(name, _)| name.is_some());
        if all_named {
            let mut store = handle.write();
            for (name, val) in &values {
                let field_name = name.as_ref().unwrap();
                if store.schema.field_index(field_name).is_none() {
                    let ty = match val {
                        Value::String(_) => crate::storage::FieldType::String,
                        Value::Int(_) => crate::storage::FieldType::Int,
                        Value::Float(_) => crate::storage::FieldType::Float,
                        Value::Bool(_) => crate::storage::FieldType::Bool,
                        Value::Bytes(_) => crate::storage::FieldType::Bytes,
                        Value::Null => crate::storage::FieldType::String,
                    };
                    store.add_field(FieldDef {
                        name: field_name.clone(),
                        ty,
                    })?;
                }
            }
        }

        let fields_values = {
            let store = handle.read();
            let schema = &store.schema;
            Self::resolve_values(schema, &values)?
        };

        {
            let store = handle.read();
            let schema = &store.schema;
            for (i, val) in fields_values.iter().enumerate() {
                let field = &schema.fields[i];
                if !val.matches_type(&field.ty) && !matches!(val, Value::Null) {
                    return Err(ArcaneError::TypeError {
                        field: field.name.clone(),
                        expected: field.ty.to_string(),
                        got: val.type_name().to_string(),
                    });
                }
            }
        }

        let record = Record::new(fields_values.clone());
        let wal_entry = WalInsert {
            bucket: bucket.clone(),
            hash: record.hash,
            fields: fields_values,
        };

        self.wal.append_insert(&wal_entry)?;
        self.wal.append_commit()?;

        let hash = {
            let mut store = handle.write();
            store.insert(record)?
        };

        Ok(QueryResult::Inserted { bucket, hash })
    }

    fn get(
        &self,
        bucket: String,
        projection: Projection,
        filter: Option<Filter>,
    ) -> Result<QueryResult> {
        let handle = self
            .buckets
            .get(&bucket)
            .ok_or_else(|| ArcaneError::BucketNotFound(bucket.clone()))?;

        let mut store = handle.write();
        let schema = store.schema.clone();
        let mut records: Vec<Record> = store.scan_all()?;

        if let Some(f) = &filter {
            let idx = schema
                .field_index(&f.field)
                .ok_or_else(|| ArcaneError::UnknownField(f.field.clone()))?;
            records.retain(|r| r.fields.get(idx).map_or(false, |v| v == &f.value));
        }

        match projection {
            Projection::Hash => {
                let hashes: Vec<u64> = records.iter().map(|r| r.hash).collect();
                return Ok(QueryResult::Hashes(hashes));
            }
            Projection::Head(n) => {
                records.truncate(n);
            }
            Projection::Tail(n) => {
                let skip = records.len().saturating_sub(n);
                records = records.into_iter().skip(skip).collect();
            }
            Projection::Star => {}
        }

        let mut col_names = vec!["__hash__".to_string()];
        col_names.extend(schema.fields.iter().map(|f| f.name.clone()));

        let rows: Vec<Vec<String>> = records
            .iter()
            .map(|r| {
                let mut row = vec![format!("{:#016x}", r.hash)];
                row.extend(r.fields.iter().map(|v| v.to_string()));
                row
            })
            .collect();

        Ok(QueryResult::Rows {
            schema: col_names,
            rows,
        })
    }

    fn resolve_values(schema: &Schema, values: &[(Option<String>, Value)]) -> Result<Vec<Value>> {
        let n = schema.fields.len();

        let all_named = values.iter().all(|(name, _)| name.is_some());
        let all_pos = values.iter().all(|(name, _)| name.is_none());

        if all_named {
            let map: HashMap<&str, &Value> = values
                .iter()
                .map(|(name, val)| (name.as_deref().unwrap(), val))
                .collect();
            let mut out = Vec::with_capacity(n);
            for field in &schema.fields {
                match map.get(field.name.as_str()) {
                    Some(v) => out.push((*v).clone()),
                    None => out.push(Value::Null),
                }
            }
            Ok(out)
        } else if all_pos {
            if values.len() != n {
                return Err(ArcaneError::SchemaMismatch {
                    expected: n,
                    got: values.len(),
                });
            }
            Ok(values.iter().map(|(_, v)| v.clone()).collect())
        } else {
            Err(ArcaneError::Other(
                "Cannot mix positional and named values in insert".into(),
            ))
        }
    }

    /// WAL replay on startup.
    fn recover(&self) -> Result<()> {
        use crate::wal::WalEntry;
        let entries = Wal::replay(&self.dir)?;
        if entries.is_empty() {
            return Ok(());
        }

        tracing::info!("Replaying {} WAL entries for crash recovery", entries.len());

        for entry in entries {
            match entry {
                WalEntry::CreateBucket(schema) => {
                    let name = schema.bucket_name.clone();
                    if !self.buckets.contains_key(&name) {
                        let store = BucketStore::create(&self.dir, schema)?;
                        self.buckets.insert(name, Arc::new(RwLock::new(store)));
                    }
                }
                WalEntry::Insert(ins) => {
                    if let Some(handle) = self.buckets.get(&ins.bucket) {
                        let record = Record {
                            hash: ins.hash,
                            fields: ins.fields,
                        };
                        let mut store = handle.write();
                        let _ = store.insert(record);
                    }
                }
                WalEntry::Commit | WalEntry::Checkpoint => {}
            }
        }
        Ok(())
    }

    /// List all bucket names.
    pub fn buckets(&self) -> Vec<String> {
        self.buckets.iter().map(|e| e.key().clone()).collect()
    }

    /// Get schema for a bucket.
    pub fn schema(&self, bucket: &str) -> Option<Schema> {
        self.buckets.get(bucket).map(|h| h.read().schema.clone())
    }
}
