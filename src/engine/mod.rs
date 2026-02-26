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

    /// Disable WAL entirely (NOT crash-safe, but fastest).
    pub no_wal: bool,

    /// Batch commits: only fsync every N commits (trades durability for performance).
    /// 1 = fsync every commit (safest), higher = less frequent fsync.
    pub fsync_interval: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            checkpoint_interval: 10_000,
            no_sync: false,
            no_wal: false,
            fsync_interval: 1,
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
    BatchInserted {
        bucket: String,
        count: usize,
    },
    BulkCompleted {
        count: usize,
        errors: usize,
    },
    Deleted {
        bucket: String,
        count: usize,
    },
    Updated {
        bucket: String,
        count: usize,
    },
    Truncated {
        bucket: String,
    },
    Rows {
        schema: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    Hashes(Vec<u64>),
    Committed,
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
            QueryResult::BatchInserted { bucket, count } => {
                writeln!(f, "Batch inserted {} rows into '{}'.", count, bucket)
            }
            QueryResult::BulkCompleted { count, errors } => {
                if *errors == 0 {
                    writeln!(f, "Bulk completed: {} statements executed.", count)
                } else {
                    writeln!(
                        f,
                        "Bulk completed: {} statements, {} errors.",
                        count, errors
                    )
                }
            }
            QueryResult::Deleted { bucket, count } => {
                writeln!(f, "Deleted {} rows from '{}'.", count, bucket)
            }
            QueryResult::Updated { bucket, count } => {
                writeln!(f, "Updated {} rows in '{}'.", count, bucket)
            }
            QueryResult::Truncated { bucket } => {
                writeln!(f, "Truncated bucket '{}'.", bucket)
            }
            QueryResult::Committed => {
                writeln!(f, "Committed.")
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
    config: Config,
}

impl Database {
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Arc<Self>> {
        Self::open_with_config(dir, Config::default())
    }

    pub fn open_with_config<P: AsRef<Path>>(dir: P, config: Config) -> Result<Arc<Self>> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let wal = Arc::new(Wal::open_with_config(
            &dir,
            config.no_sync,
            config.fsync_interval,
        )?);
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
            config,
        });

        if !db.config.no_wal {
            db.recover()?;
        }

        Ok(db)
    }

    /// Execute a single-line AQL statement.
    pub fn execute(&self, line: &str) -> Result<QueryResult> {
        let stmt = parser::parse_statement(line)?;
        self.execute_stmt(stmt)
    }

    /// Execute a pre-parsed statement.
    pub fn execute_stmt(&self, stmt: Statement) -> Result<QueryResult> {
        match stmt {
            Statement::CreateBucket {
                name,
                fields,
                unique,
                forced,
            } => self.create_bucket(name, fields, unique, forced),
            Statement::Insert { bucket, values } => self.insert(bucket, values),
            Statement::BatchInsert { bucket, rows } => self.batch_insert(bucket, rows),
            Statement::Bulk { statements } => self.bulk(statements),
            Statement::Delete { bucket, filter } => self.delete(bucket, filter),
            Statement::Set {
                bucket,
                values,
                filter,
            } => self.set(bucket, values, filter),
            Statement::Truncate { bucket } => self.truncate(bucket),
            Statement::Get {
                bucket,
                projection,
                filter,
            } => self.get(bucket, projection, filter),
            Statement::Commit => self.commit(),
        }
    }

    /// Execute all statements in an .arc script.
    pub fn execute_script(&self, src: &str) -> Vec<Result<QueryResult>> {
        parser::parse_script(src)
            .into_iter()
            .map(|r| r.and_then(|s| self.execute_stmt(s)))
            .collect()
    }

    fn create_bucket(
        &self,
        name: String,
        fields: Vec<FieldDef>,
        unique: bool,
        forced: bool,
    ) -> Result<QueryResult> {
        if self.buckets.contains_key(&name) {
            if unique && !forced {
                return Err(ArcaneError::BucketExists(name));
            }
            if forced {
                self.buckets.remove(&name);
                std::fs::remove_file(self.dir.join(format!("{}.arc", name)))?;
                std::fs::remove_file(self.dir.join(format!("{}.arc.idx", name)))?;
            } else {
                return Err(ArcaneError::BucketExists(name));
            }
        }

        let schema = Schema {
            bucket_name: name.clone(),
            fields,
        };

        if !self.config.no_wal {
            self.wal.append_create_bucket(&schema)?;
        }

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

        let record = Record::new(fields_values);
        let wal_entry = WalInsert {
            bucket: bucket.clone(),
            hash: record.hash,
            fields: record.fields.clone(),
        };

        if !self.config.no_wal {
            self.wal.append_insert(&wal_entry)?;
        }

        let hash = {
            let mut store = handle.write();
            store.insert(record)?
        };

        Ok(QueryResult::Inserted { bucket, hash })
    }

    fn batch_insert(
        &self,
        bucket: String,
        rows: Vec<Vec<(Option<String>, Value)>>,
    ) -> Result<QueryResult> {
        let handle = self
            .buckets
            .get(&bucket)
            .ok_or_else(|| ArcaneError::BucketNotFound(bucket.clone()))?;

        let mut records = Vec::with_capacity(rows.len());

        for values in rows.clone() {
            let fields_values = {
                let store = handle.read();
                Self::resolve_values(&store.schema, &values)?
            };

            {
                let store = handle.read();
                for (i, val) in fields_values.iter().enumerate() {
                    let field = &store.schema.fields[i];
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

            if !self.config.no_wal {
                let wal_entry = WalInsert {
                    bucket: bucket.clone(),
                    hash: record.hash,
                    fields: fields_values,
                };
                self.wal.append_insert(&wal_entry)?;
            }

            records.push(record);
        }

        let count = {
            let mut store = handle.write();
            for record in records {
                store.insert(record)?;
            }
            rows.len()
        };

        Ok(QueryResult::BatchInserted { bucket, count })
    }

    fn bulk(&self, statements: Vec<Statement>) -> Result<QueryResult> {
        let mut count = 0;
        let mut errors = 0;

        for stmt in statements {
            match self.execute_stmt(stmt) {
                Ok(_) => count += 1,
                Err(_) => errors += 1,
            }
        }

        Ok(QueryResult::BulkCompleted { count, errors })
    }

    fn delete(&self, bucket: String, filter: Filter) -> Result<QueryResult> {
        let handle = self
            .buckets
            .get(&bucket)
            .ok_or_else(|| ArcaneError::BucketNotFound(bucket.clone()))?;

        let mut store = handle.write();
        let schema = store.schema.clone();
        let idx = schema
            .field_index(&filter.field)
            .ok_or_else(|| ArcaneError::UnknownField(filter.field.clone()))?;
        let records: Vec<Record> = store.scan_all()?;
        let mut count = 0;

        for record in records {
            if record.fields.get(idx).map_or(false, |v| {
                Self::compare_values(v, &filter.op, &filter.value)
            }) {
                store.delete(record.hash)?;
                count += 1;
            }
        }

        Ok(QueryResult::Deleted { bucket, count })
    }

    fn set(
        &self,
        bucket: String,
        values: Vec<(Option<String>, Value)>,
        filter: Filter,
    ) -> Result<QueryResult> {
        let handle = self
            .buckets
            .get(&bucket)
            .ok_or_else(|| ArcaneError::BucketNotFound(bucket.clone()))?;
        let mut store = handle.write();
        let schema = store.schema.clone();
        let filter_idx = schema
            .field_index(&filter.field)
            .ok_or_else(|| ArcaneError::UnknownField(filter.field.clone()))?;
        let records: Vec<Record> = store.scan_all()?;
        let mut count = 0;

        for old_record in records {
            if old_record.fields.get(filter_idx).map_or(false, |v| {
                Self::compare_values(v, &filter.op, &filter.value)
            }) {
                store.delete(old_record.hash)?;

                let mut new_fields = old_record.fields.clone();
                
                let all_named = values.iter().all(|(name, _)| name.is_some());
                if all_named {
                    for (name, val) in &values {
                        let field_name = name.as_ref().unwrap();
                        if let Some(idx) = schema.field_index(field_name) {
                            new_fields[idx] = val.clone();
                        }
                    }
                } else {
                    if values.len() != schema.fields.len() {
                        return Err(ArcaneError::SchemaMismatch {
                            expected: schema.fields.len(),
                            got: values.len(),
                        });
                    }
                    new_fields = values.iter().map(|(_, v)| v.clone()).collect();
                }

                for (i, val) in new_fields.iter().enumerate() {
                    let field = &schema.fields[i];
                    if !val.matches_type(&field.ty) && !matches!(val, Value::Null) {
                        return Err(ArcaneError::TypeError {
                            field: field.name.clone(),
                            expected: field.ty.to_string(),
                            got: val.type_name().to_string(),
                        });
                    }
                }

                let new_record = Record::new(new_fields.clone());
                
                if !self.config.no_wal {
                    let wal_entry = WalInsert {
                        bucket: bucket.clone(),
                        hash: new_record.hash,
                        fields: new_fields,
                    };
                    self.wal.append_insert(&wal_entry)?;
                }

                store.insert(new_record)?;
                count += 1;
            }
        }

        Ok(QueryResult::Updated { bucket, count })
    }

    fn compare_values(left: &Value, op: &parser::CompareOp, right: &Value) -> bool {
        use parser::CompareOp::*;
        match op {
            Eq => left == right,
            Ne => left != right,
            Lt => match (left, right) {
                (Value::Int(a), Value::Int(b)) => a < b,
                (Value::Float(a), Value::Float(b)) => a < b,
                (Value::Int(a), Value::Float(b)) => (*a as f64) < *b,
                (Value::Float(a), Value::Int(b)) => *a < (*b as f64),
                _ => false,
            },
            Le => match (left, right) {
                (Value::Int(a), Value::Int(b)) => a <= b,
                (Value::Float(a), Value::Float(b)) => a <= b,
                (Value::Int(a), Value::Float(b)) => (*a as f64) <= *b,
                (Value::Float(a), Value::Int(b)) => *a <= (*b as f64),
                _ => false,
            },
            Gt => match (left, right) {
                (Value::Int(a), Value::Int(b)) => a > b,
                (Value::Float(a), Value::Float(b)) => a > b,
                (Value::Int(a), Value::Float(b)) => (*a as f64) > *b,
                (Value::Float(a), Value::Int(b)) => *a > (*b as f64),
                _ => false,
            },
            Ge => match (left, right) {
                (Value::Int(a), Value::Int(b)) => a >= b,
                (Value::Float(a), Value::Float(b)) => a >= b,
                (Value::Int(a), Value::Float(b)) => (*a as f64) >= *b,
                (Value::Float(a), Value::Int(b)) => *a >= (*b as f64),
                _ => false,
            },
        }
    }

    fn truncate(&self, bucket: String) -> Result<QueryResult> {
        let handle = self
            .buckets
            .get(&bucket)
            .ok_or_else(|| ArcaneError::BucketNotFound(bucket.clone()))?;

        let mut store = handle.write();
        store.truncate()?;

        Ok(QueryResult::Truncated { bucket })
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
            records.retain(|r| {
                r.fields
                    .get(idx)
                    .map_or(false, |v| Self::compare_values(v, &f.op, &f.value))
            });
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

    fn commit(&self) -> Result<QueryResult> {
        if !self.config.no_wal {
            self.wal.append_commit()?;
            self.wal.force_sync()?;
        }
        Ok(QueryResult::Committed)
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_db() -> (Arc<Database>, TempDir) {
        let dir = TempDir::new().unwrap();
        let db = Database::open(dir.path()).unwrap();
        (db, dir)
    }

    #[test]
    fn test_create_bucket() {
        let (db, _dir) = setup_db();
        let result = db
            .execute("create bucket Users (name: string, age: int)")
            .unwrap();

        match result {
            QueryResult::BucketCreated { name } => assert_eq!(name, "Users"),
            _ => panic!("Expected BucketCreated"),
        }

        assert!(db.buckets().contains(&"Users".to_string()));
    }

    #[test]
    fn test_create_duplicate_bucket() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string)").unwrap();
        let result = db.execute("create bucket Users (name: string)");

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ArcaneError::BucketExists(_)));
    }

    #[test]
    fn test_insert_positional() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        let result = db.execute("insert into Users (\"Alice\", 30)").unwrap();

        match result {
            QueryResult::Inserted { bucket, hash } => {
                assert_eq!(bucket, "Users");
                assert_ne!(hash, 0);
            }
            _ => panic!("Expected Inserted"),
        }
    }

    #[test]
    fn test_insert_named() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        let result = db
            .execute("insert into Users (name: \"Alice\", age: 30)")
            .unwrap();

        match result {
            QueryResult::Inserted { .. } => {}
            _ => panic!("Expected Inserted"),
        }
    }

    #[test]
    fn test_insert_schema_mismatch() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        let result = db.execute("insert into Users (\"Alice\")");

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ArcaneError::SchemaMismatch { .. }
        ));
    }

    #[test]
    fn test_insert_type_error() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        let result = db.execute("insert into Users (\"Alice\", \"not a number\")");

        assert!(result.is_err());
    }

    #[test]
    fn test_insert_duplicate() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string)").unwrap();
        db.execute("insert into Users (\"Alice\")").unwrap();
        let result = db.execute("insert into Users (\"Alice\")");

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ArcaneError::DuplicateRecord(_, _)
        ));
    }

    #[test]
    fn test_batch_insert() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        let result = db
            .execute("insert into Users ([name: \"Alice\", age: 30], [name: \"Bob\", age: 25])")
            .unwrap();

        match result {
            QueryResult::BatchInserted { bucket, count } => {
                assert_eq!(bucket, "Users");
                assert_eq!(count, 2);
            }
            _ => panic!("Expected BatchInserted"),
        }
    }

    #[test]
    fn test_bulk() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string)").unwrap();
        let result = db
            .execute("bulk { insert into Users (\"Alice\") insert into Users (\"Bob\") }")
            .unwrap();

        match result {
            QueryResult::BulkCompleted { count, errors } => {
                assert_eq!(count, 2);
                assert_eq!(errors, 0);
            }
            _ => panic!("Expected BulkCompleted"),
        }
    }

    #[test]
    fn test_get_star() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string)").unwrap();
        db.execute("insert into Users (\"Alice\")").unwrap();
        db.execute("insert into Users (\"Bob\")").unwrap();

        let result = db.execute("get * from Users").unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 2); // __hash__ + name
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_get_head() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (id: int)").unwrap();
        for i in 0..10 {
            db.execute(&format!("insert into Users ({})", i)).unwrap();
        }

        let result = db.execute("get head(3) from Users").unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_get_tail() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (id: int)").unwrap();
        for i in 0..10 {
            db.execute(&format!("insert into Users ({})", i)).unwrap();
        }

        let result = db.execute("get tail(3) from Users").unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_get_with_filter() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (\"Alice\", 30)").unwrap();
        db.execute("insert into Users (\"Bob\", 25)").unwrap();
        db.execute("insert into Users (\"Alice\", 35)").unwrap();

        let result = db
            .execute("get * from Users where name = \"Alice\"")
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_get_hash_projection() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string)").unwrap();
        db.execute("insert into Users (\"Alice\")").unwrap();

        let result = db.execute("get __hash__ from Users").unwrap();

        match result {
            QueryResult::Hashes(hashes) => {
                assert_eq!(hashes.len(), 1);
            }
            _ => panic!("Expected Hashes"),
        }
    }

    #[test]
    fn test_schema_evolution_on_insert() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string)").unwrap();
        db.execute("insert into Users (name: \"Alice\")").unwrap();
        db.execute("insert into Users (name: \"Bob\", age: 30)")
            .unwrap();

        let schema = db.schema("Users").unwrap();
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fields[1].name, "age");
    }

    #[test]
    fn test_execute_script() {
        let (db, _dir) = setup_db();
        let script = r#"
            create bucket Users (name: string);
            insert into Users ("Alice");
            insert into Users ("Bob");
            get * from Users;
        "#;

        let results = db.execute_script(script);
        assert_eq!(results.len(), 4);
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn test_bucket_not_found() {
        let (db, _dir) = setup_db();
        let result = db.execute("insert into NonExistent (\"data\")");

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ArcaneError::BucketNotFound(_)
        ));
    }

    #[test]
    fn test_unknown_field() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string)").unwrap();
        db.execute("insert into Users (\"Alice\")").unwrap();
        let result = db.execute("get * from Users where nonexistent = \"value\"");

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ArcaneError::UnknownField(_)));
    }

    #[test]
    fn test_persistence() {
        let dir = TempDir::new().unwrap();
        {
            let db = Database::open(dir.path()).unwrap();
            db.execute("create bucket Users (name: string)").unwrap();
            db.execute("insert into Users (\"Alice\")").unwrap();
        }
        {
            let db = Database::open(dir.path()).unwrap();
            assert!(db.buckets().contains(&"Users".to_string()));

            let result = db.execute("get * from Users").unwrap();
            match result {
                QueryResult::Rows { rows, .. } => {
                    assert_eq!(rows.len(), 1);
                }
                _ => panic!("Expected Rows"),
            }
        }
    }
}
