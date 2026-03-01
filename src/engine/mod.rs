//! The storage engine of ArcaneDB.
//!
//! This module provides the core functionality for managing and interacting with the storage engine.

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
    Described {
        bucket: String,
        fields: Vec<(String, String)>,
        row_count: u64,
    },
    Rows {
        schema: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    Hashes(Vec<u64>),
    Exported {
        bucket: String,
        path: String,
        count: usize,
    },
    Printed {
        message: String,
    },
    Committed,
    Empty,
    BucketList {
        buckets: Vec<String>,
    },
    ColumnDropped {
        bucket: String,
        column: String,
    },
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
            QueryResult::Described {
                bucket,
                fields,
                row_count,
            } => {
                writeln!(f, "\nBucket: {}", bucket)?;
                writeln!(f)?;

                if fields.is_empty() {
                    writeln!(f, "No fields defined.")?;
                } else {
                    let max_name_len = fields
                        .iter()
                        .map(|(name, _)| name.len())
                        .max()
                        .unwrap_or(4)
                        .max(5);
                    let max_type_len = fields
                        .iter()
                        .map(|(_, ty)| ty.len())
                        .max()
                        .unwrap_or(4)
                        .max(4);
                    for (name, ty) in fields {
                        writeln!(
                            f,
                            "{:<width_name$} : {:<width_type$}",
                            name,
                            ty,
                            width_name = max_name_len,
                            width_type = max_type_len
                        )?;
                    }
                }

                writeln!(f)?;
                writeln!(f, "Total rows: {}", row_count)
            }
            QueryResult::Committed => {
                writeln!(f, "Committed.")
            }
            QueryResult::Exported {
                bucket,
                path,
                count,
            } => {
                writeln!(
                    f,
                    "Exported {} rows from '{}' to '{}'.",
                    count, bucket, path
                )
            }
            QueryResult::Printed { message } => {
                writeln!(f, "{}", message)
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
            QueryResult::BucketList { buckets } => {
                if buckets.is_empty() {
                    writeln!(f, "No buckets found.")
                } else {
                    writeln!(f, "\nAvailable buckets:")?;
                    writeln!(f)?;
                    for bucket in buckets {
                        writeln!(f, "  {}", bucket)?;
                    }
                    writeln!(f)?;
                    writeln!(f, "({} buckets)", buckets.len())
                }
            }
            QueryResult::ColumnDropped { bucket, column } => {
                writeln!(f, "Dropped column '{}' from bucket '{}'.", column, bucket)
            }
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
            Statement::CreateBucketFromCsv {
                name,
                csv_path,
                unique,
                forced,
                drop_columns,
            } => self.create_bucket_from_csv(name, csv_path, unique, forced, drop_columns),
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
            Statement::Describe { bucket } => self.describe(bucket),
            Statement::Get {
                bucket,
                projection,
                filter,
                order_by,
            } => self.get(bucket, projection, filter, order_by),
            Statement::Export { bucket, csv_path } => self.export_csv(bucket, csv_path),
            Statement::Print { message } => Ok(QueryResult::Printed { message }),
            Statement::Commit => self.commit(),
            Statement::Checkpoint => {
                self.checkpoint()?;
                Ok(QueryResult::Printed {
                    message: "Checkpoint completed.".to_string(),
                })
            }
            Statement::ShowBuckets => self.show_buckets(),
            Statement::DropColumn { column, bucket } => self.drop_column(column, bucket),
        }
    }

    /// Execute all statements in an .arc script.
    pub fn execute_script(&self, src: &str) -> Vec<Result<QueryResult>> {
        parser::parse_script(src)
            .into_iter()
            .map(|r| r.and_then(|s| self.execute_stmt(s)))
            .collect()
    }

    /// The checkpoint call on forced bucket creation ensures data integrity by flushing WAL.
    ///
    /// Do NOT remove this call.
    fn create_bucket(
        &self,
        name: String,
        fields: Vec<FieldDef>,
        unique: bool,
        forced: bool,
    ) -> Result<QueryResult> {
        let bucket_existed = self.buckets.contains_key(&name);

        if bucket_existed {
            if unique && !forced {
                return Err(ArcaneError::BucketExists(name));
            }
            if forced {
                self.checkpoint()?;
                self.buckets.remove(&name);
                std::fs::remove_file(self.dir.join(format!("{}.arc", name)))?;
                std::fs::remove_file(self.dir.join(format!("{}.arc.idx", name)))?;
            } else {
                return Err(ArcaneError::BucketExists(name));
            }
        }

        let mut seen_names = std::collections::HashSet::new();
        for field in &fields {
            if !seen_names.insert(field.name.as_str()) {
                return Err(ArcaneError::Other(format!(
                    "Duplicate column name '{}' in bucket schema",
                    field.name
                )));
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

        if count >= 1000 {
            self.checkpoint()?;
        }

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
        let records: Vec<Record> = store.scan_all()?;
        let mut count = 0;

        for record in records {
            if Self::evaluate_filter(&filter, &record, &schema)? {
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
        let all_named = values.iter().all(|(name, _)| name.is_some());

        if all_named {
            for (name, _) in &values {
                let field_name = name.as_ref().unwrap();
                if schema.field_index(field_name).is_none() {
                    return Err(ArcaneError::UnknownField(field_name.clone()));
                }
            }
        }

        let records: Vec<Record> = store.scan_all()?;
        let mut count = 0;

        for old_record in records {
            if Self::evaluate_filter(&filter, &old_record, &schema)? {
                store.delete(old_record.hash)?;

                let mut new_fields = old_record.fields.clone();

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
            Like => match (left, right) {
                (Value::String(text), Value::String(pattern)) => {
                    Self::match_like_pattern(text, pattern)
                }
                _ => false,
            },
            IsNull => matches!(left, Value::Null),
            IsNotNull => !matches!(left, Value::Null),
        }
    }

    fn match_like_pattern(text: &str, pattern: &str) -> bool {
        let text_chars: Vec<char> = text.chars().collect();
        let pattern_chars: Vec<char> = pattern.chars().collect();
        Self::match_like_recursive(&text_chars, &pattern_chars, 0, 0)
    }

    fn match_like_recursive(
        text: &[char],
        pattern: &[char],
        text_idx: usize,
        pattern_idx: usize,
    ) -> bool {
        if pattern_idx >= pattern.len() {
            return text_idx >= text.len();
        }

        if pattern[pattern_idx] == '%' {
            if Self::match_like_recursive(text, pattern, text_idx, pattern_idx + 1) {
                return true;
            }
            if text_idx < text.len() {
                return Self::match_like_recursive(text, pattern, text_idx + 1, pattern_idx);
            }
            return false;
        }

        if text_idx >= text.len() {
            return false;
        }
        if pattern[pattern_idx] == '_' {
            return Self::match_like_recursive(text, pattern, text_idx + 1, pattern_idx + 1);
        }
        if text[text_idx] == pattern[pattern_idx] {
            return Self::match_like_recursive(text, pattern, text_idx + 1, pattern_idx + 1);
        }

        false
    }

    fn evaluate_filter(filter: &Filter, record: &Record, schema: &Schema) -> Result<bool> {
        match filter {
            Filter::Simple { field, op, value } => {
                if field == "__hash__" {
                    let hash_value = Value::Int(record.hash as i64);
                    return Ok(Self::compare_values(&hash_value, op, value));
                }

                if matches!(op, parser::CompareOp::IsNull | parser::CompareOp::IsNotNull) {
                    let field_value = match schema.field_index(field) {
                        Some(idx) => record.fields.get(idx).unwrap_or(&Value::Null),
                        None => &Value::Null,
                    };
                    Ok(Self::compare_values(field_value, op, value))
                } else {
                    let idx = schema
                        .field_index(field)
                        .ok_or_else(|| ArcaneError::UnknownField(field.clone()))?;
                    Ok(record
                        .fields
                        .get(idx)
                        .map_or(false, |v| Self::compare_values(v, op, value)))
                }
            }
            Filter::And(left, right) => {
                let left_result = Self::evaluate_filter(left, record, schema)?;
                let right_result = Self::evaluate_filter(right, record, schema)?;
                Ok(left_result && right_result)
            }
            Filter::Or(left, right) => {
                let left_result = Self::evaluate_filter(left, record, schema)?;
                let right_result = Self::evaluate_filter(right, record, schema)?;
                Ok(left_result || right_result)
            }
        }
    }

    fn compare_values_for_sort(left: &Value, right: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (left, right) {
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Int(a), Value::Float(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Float(a), Value::Int(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            _ => Ordering::Equal,
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

    fn describe(&self, bucket: String) -> Result<QueryResult> {
        let handle = self
            .buckets
            .get(&bucket)
            .ok_or_else(|| ArcaneError::BucketNotFound(bucket.clone()))?;

        let store = handle.read();
        let schema = &store.schema;
        let row_count = store.record_count();

        let fields: Vec<(String, String)> = schema
            .fields
            .iter()
            .map(|f| (f.name.clone(), f.ty.to_string()))
            .collect();

        Ok(QueryResult::Described {
            bucket,
            fields,
            row_count,
        })
    }

    fn show_buckets(&self) -> Result<QueryResult> {
        let mut buckets: Vec<String> = self
            .buckets
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        buckets.sort();

        Ok(QueryResult::BucketList { buckets })
    }

    fn drop_column(&self, column: String, bucket: String) -> Result<QueryResult> {
        let handle = self
            .buckets
            .get(&bucket)
            .ok_or_else(|| ArcaneError::BucketNotFound(bucket.clone()))?;

        let mut store = handle.write();
        store.drop_column(&column)?;

        Ok(QueryResult::ColumnDropped { bucket, column })
    }

    fn get(
        &self,
        bucket: String,
        projection: Projection,
        filter: Option<Filter>,
        order_by: Option<parser::OrderBy>,
    ) -> Result<QueryResult> {
        let handle = self
            .buckets
            .get(&bucket)
            .ok_or_else(|| ArcaneError::BucketNotFound(bucket.clone()))?;

        let mut store = handle.write();
        let schema = store.schema.clone();
        let mut records: Vec<Record> = store.scan_all()?;

        if let Some(f) = &filter {
            let mut filtered_records = Vec::new();
            for record in records {
                if Self::evaluate_filter(f, &record, &schema)? {
                    filtered_records.push(record);
                }
            }
            records = filtered_records;
        }

        if let Some(ref order) = order_by {
            if order.field == "__hash__" {
                records.sort_by(|a, b| {
                    let cmp = a.hash.cmp(&b.hash);
                    match order.order {
                        parser::SortOrder::Asc => cmp,
                        parser::SortOrder::Desc => cmp.reverse(),
                    }
                });
            } else {
                let sort_idx = schema
                    .field_index(&order.field)
                    .ok_or_else(|| ArcaneError::UnknownField(order.field.clone()))?;

                records.sort_by(|a, b| {
                    let val_a = &a.fields[sort_idx];
                    let val_b = &b.fields[sort_idx];
                    let cmp = Self::compare_values_for_sort(val_a, val_b);

                    match order.order {
                        parser::SortOrder::Asc => cmp,
                        parser::SortOrder::Desc => cmp.reverse(),
                    }
                });
            }
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
            Projection::Aggregates(ref agg_funcs) => {
                use crate::parser::AggregateFunc;

                let mut col_names = Vec::new();
                let mut values = Vec::new();

                for agg in agg_funcs {
                    if matches!(agg, AggregateFunc::Count(None)) {
                        col_names.push("count(*)".to_string());
                        values.push(records.len().to_string());
                        continue;
                    }

                    let (field_name, func_name) = match agg {
                        AggregateFunc::Avg(f) => (f, "avg"),
                        AggregateFunc::Sum(f) => (f, "sum"),
                        AggregateFunc::Min(f) => (f, "min"),
                        AggregateFunc::Max(f) => (f, "max"),
                        AggregateFunc::Median(f) => (f, "median"),
                        AggregateFunc::Stddev(f) => (f, "stddev"),
                        AggregateFunc::Count(Some(f)) => (f, "count"),
                        AggregateFunc::Count(None) => unreachable!(),
                    };

                    let field_idx = schema
                        .field_index(field_name)
                        .ok_or_else(|| ArcaneError::UnknownField(field_name.clone()))?;

                    col_names.push(format!("{}({})", func_name, field_name));

                    let result = match agg {
                        AggregateFunc::Avg(_) => {
                            let mut sum = 0.0;
                            let mut count = 0;
                            for record in &records {
                                match &record.fields[field_idx] {
                                    Value::Int(i) => {
                                        sum += *i as f64;
                                        count += 1;
                                    }
                                    Value::Float(f) => {
                                        sum += f;
                                        count += 1;
                                    }
                                    Value::Null => {}
                                    _ => {
                                        return Err(ArcaneError::Other(format!(
                                            "Cannot compute average on non-numeric field '{}'",
                                            field_name
                                        )))
                                    }
                                }
                            }
                            if count == 0 {
                                "NULL".to_string()
                            } else {
                                format!("{:.2}", sum / count as f64)
                            }
                        }
                        AggregateFunc::Sum(_) => {
                            let mut sum = 0.0;
                            for record in &records {
                                match &record.fields[field_idx] {
                                    Value::Int(i) => sum += *i as f64,
                                    Value::Float(f) => sum += f,
                                    Value::Null => {}
                                    _ => {
                                        return Err(ArcaneError::Other(format!(
                                            "Cannot compute sum on non-numeric field '{}'",
                                            field_name
                                        )))
                                    }
                                }
                            }
                            format!("{:.2}", sum)
                        }
                        AggregateFunc::Min(_) => {
                            let mut min_val: Option<Value> = None;
                            for record in &records {
                                let val = &record.fields[field_idx];
                                if matches!(val, Value::Null) {
                                    continue;
                                }
                                match &min_val {
                                    None => min_val = Some(val.clone()),
                                    Some(current_min) => {
                                        if Self::compare_values_for_sort(val, current_min)
                                            == std::cmp::Ordering::Less
                                        {
                                            min_val = Some(val.clone());
                                        }
                                    }
                                }
                            }
                            min_val
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "NULL".to_string())
                        }
                        AggregateFunc::Max(_) => {
                            let mut max_val: Option<Value> = None;
                            for record in &records {
                                let val = &record.fields[field_idx];
                                if matches!(val, Value::Null) {
                                    continue;
                                }
                                match &max_val {
                                    None => max_val = Some(val.clone()),
                                    Some(current_max) => {
                                        if Self::compare_values_for_sort(val, current_max)
                                            == std::cmp::Ordering::Greater
                                        {
                                            max_val = Some(val.clone());
                                        }
                                    }
                                }
                            }
                            max_val
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "NULL".to_string())
                        }
                        AggregateFunc::Median(_) => {
                            let mut numeric_values: Vec<f64> = Vec::new();
                            for record in &records {
                                match &record.fields[field_idx] {
                                    Value::Int(i) => numeric_values.push(*i as f64),
                                    Value::Float(f) => numeric_values.push(*f),
                                    Value::Null => {}
                                    _ => {
                                        return Err(ArcaneError::Other(format!(
                                            "Cannot compute median on non-numeric field '{}'",
                                            field_name
                                        )))
                                    }
                                }
                            }
                            if numeric_values.is_empty() {
                                "NULL".to_string()
                            } else {
                                numeric_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
                                let len = numeric_values.len();
                                let median = if len % 2 == 0 {
                                    (numeric_values[len / 2 - 1] + numeric_values[len / 2]) / 2.0
                                } else {
                                    numeric_values[len / 2]
                                };
                                format!("{:.2}", median)
                            }
                        }
                        AggregateFunc::Stddev(_) => {
                            let mut numeric_values: Vec<f64> = Vec::new();
                            for record in &records {
                                match &record.fields[field_idx] {
                                    Value::Int(i) => numeric_values.push(*i as f64),
                                    Value::Float(f) => numeric_values.push(*f),
                                    Value::Null => {}
                                    _ => {
                                        return Err(ArcaneError::Other(format!(
                                            "Cannot compute stddev on non-numeric field '{}'",
                                            field_name
                                        )))
                                    }
                                }
                            }
                            if numeric_values.is_empty() {
                                "NULL".to_string()
                            } else {
                                let mean = numeric_values.iter().sum::<f64>()
                                    / numeric_values.len() as f64;
                                let variance = numeric_values
                                    .iter()
                                    .map(|v| (v - mean).powi(2))
                                    .sum::<f64>()
                                    / numeric_values.len() as f64;
                                format!("{:.2}", variance.sqrt())
                            }
                        }
                        AggregateFunc::Count(Some(_)) => {
                            let mut count = 0;
                            for record in &records {
                                if !matches!(&record.fields[field_idx], Value::Null) {
                                    count += 1;
                                }
                            }
                            count.to_string()
                        }
                        AggregateFunc::Count(None) => unreachable!(),
                    };

                    values.push(result);
                }

                return Ok(QueryResult::Rows {
                    schema: col_names,
                    rows: vec![values],
                });
            }
            Projection::Fields(ref field_names) => {
                let mut field_indices = Vec::new();
                for field_name in field_names {
                    let idx = schema
                        .field_index(field_name)
                        .ok_or_else(|| ArcaneError::UnknownField(field_name.clone()))?;
                    field_indices.push(idx);
                }
                let col_names: Vec<String> = field_names.clone();
                let rows: Vec<Vec<String>> = records
                    .iter()
                    .map(|r| {
                        field_indices
                            .iter()
                            .map(|&idx| r.fields[idx].to_string())
                            .collect()
                    })
                    .collect();

                return Ok(QueryResult::Rows {
                    schema: col_names,
                    rows,
                });
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

    /// Checkpoint: Flush all bucket data to disk and truncate WAL.
    ///
    /// This should be called after large imports or periodically to prevent WAL
    /// from growing too large.
    pub fn checkpoint(&self) -> Result<()> {
        if self.config.no_wal {
            return Ok(());
        }

        for bucket_ref in self.buckets.iter() {
            let handle = bucket_ref.value();
            let mut store = handle.write();
            store.flush()?;
        }

        self.wal.append_checkpoint()?;
        self.wal.force_sync()?;
        self.wal.truncate()?;

        Ok(())
    }

    fn export_csv(&self, bucket: String, csv_path: String) -> Result<QueryResult> {
        let handle = self
            .buckets
            .get(&bucket)
            .ok_or_else(|| ArcaneError::BucketNotFound(bucket.clone()))?;

        let mut store = handle.write();
        let schema = store.schema.clone();
        let records: Vec<Record> = store.scan_all()?;
        let mut csv_content = String::new();
        let header: Vec<String> = schema.fields.iter().map(|f| f.name.clone()).collect();

        csv_content.push_str(&header.join(","));
        csv_content.push('\n');

        for record in &records {
            let row: Vec<String> = record
                .fields
                .iter()
                .map(|v| Self::value_to_csv_field(v))
                .collect();
            csv_content.push_str(&row.join(","));
            csv_content.push('\n');
        }

        std::fs::write(&csv_path, csv_content)?;

        Ok(QueryResult::Exported {
            bucket,
            path: csv_path,
            count: records.len(),
        })
    }

    fn value_to_csv_field(value: &Value) -> String {
        match value {
            Value::String(s) => {
                if s.contains(',') || s.contains('"') || s.contains('\n') {
                    format!("\"{}\"", s.replace('"', "\"\""))
                } else {
                    s.clone()
                }
            }
            Value::Null => String::new(),
            other => other.to_string(),
        }
    }

    fn create_bucket_from_csv(
        &self,
        name: String,
        csv_path: String,
        unique: bool,
        forced: bool,
        drop_columns: Option<Vec<usize>>,
    ) -> Result<QueryResult> {
        let csv_content = std::fs::read_to_string(&csv_path).map_err(|e| {
            ArcaneError::Other(format!("Failed to read CSV file '{}': {}", csv_path, e))
        })?;
        let mut lines = csv_content.lines();
        let header_line = lines
            .next()
            .ok_or_else(|| ArcaneError::Other(format!("CSV file '{}' is empty", csv_path)))?;
        let mut field_names = Self::parse_csv_line(header_line);

        if field_names.is_empty() {
            return Err(ArcaneError::Other(format!(
                "CSV file '{}' has no columns",
                csv_path
            )));
        }

        let mut seen_names = std::collections::HashSet::new();
        let mut duplicates = Vec::new();
        for (idx, name) in field_names.iter().enumerate() {
            if !seen_names.insert(name.as_str()) {
                duplicates.push((idx, name.as_str()));
            }
        }

        if !duplicates.is_empty() && drop_columns.is_none() {
            let dup_list: Vec<String> = duplicates
                .iter()
                .map(|(idx, name)| format!("'{}' at index {}", name, idx))
                .collect();
            return Err(ArcaneError::Other(format!(
                "CSV file '{}' has duplicate column names: {}. Use 'drop columns (...)' to specify which columns to drop by 0 based index.",
                csv_path,
                dup_list.join(", ")
            )));
        }

        if let Some(ref indices) = drop_columns {
            for &idx in indices {
                if idx >= field_names.len() {
                    return Err(ArcaneError::Other(format!(
                        "Column index {} is out of bounds (CSV has {} columns)",
                        idx,
                        field_names.len()
                    )));
                }
            }

            let mut sorted_indices = indices.clone();
            sorted_indices.sort_unstable();
            sorted_indices.reverse();
            for &idx in &sorted_indices {
                field_names.remove(idx);
            }

            let mut seen_after_drop = std::collections::HashSet::new();
            for name in &field_names {
                if !seen_after_drop.insert(name.as_str()) {
                    return Err(ArcaneError::Other(format!(
                        "Duplicate column name '{}' still exists after dropping specified columns",
                        name
                    )));
                }
            }
        }

        if field_names.is_empty() {
            return Err(ArcaneError::Other(format!(
                "CSV file '{}' has no columns remaining after dropping",
                csv_path
            )));
        }

        let first_data_line = lines.next().ok_or_else(|| {
            ArcaneError::Other(format!("CSV file '{}' has no data rows", csv_path))
        })?;
        let mut first_row_values = Self::parse_csv_line(first_data_line);

        if let Some(ref indices) = drop_columns {
            let mut sorted_indices = indices.clone();
            sorted_indices.sort_unstable();
            sorted_indices.reverse();
            for &idx in &sorted_indices {
                if idx < first_row_values.len() {
                    first_row_values.remove(idx);
                }
            }
        }

        if first_row_values.len() != field_names.len() {
            return Err(ArcaneError::Other(format!(
                "CSV file '{}' has mismatched column count",
                csv_path
            )));
        }

        let mut fields = Vec::new();

        for (i, field_name) in field_names.iter().enumerate() {
            let value_str = &first_row_values[i];
            let field_type = Self::infer_type(value_str);
            fields.push(FieldDef {
                name: field_name.clone(),
                ty: field_type,
            });
        }

        self.create_bucket(name.clone(), fields.clone(), unique, forced)?;

        let mut all_records = Vec::new();
        let mut wal_entries = Vec::new();
        let mut seen_hashes = std::collections::HashSet::new();

        let first_row_parsed: Vec<Value> = field_names
            .iter()
            .zip(first_row_values.iter())
            .map(|(name, val_str)| {
                let field_def = fields.iter().find(|f| &f.name == name).unwrap();
                Self::parse_value(val_str, &field_def.ty)
            })
            .collect();

        let first_record = Record::new(first_row_parsed);

        if seen_hashes.insert(first_record.hash) {
            if !self.config.no_wal {
                wal_entries.push(WalInsert {
                    bucket: name.clone(),
                    hash: first_record.hash,
                    fields: first_record.fields.clone(),
                });
            }
            all_records.push(first_record);
        }

        for line in lines {
            if line.trim().is_empty() {
                continue;
            }

            let mut row_values = Self::parse_csv_line(line);

            if let Some(ref indices) = drop_columns {
                let mut sorted_indices = indices.clone();
                sorted_indices.sort_unstable();
                sorted_indices.reverse();
                for &idx in &sorted_indices {
                    if idx < row_values.len() {
                        row_values.remove(idx);
                    }
                }
            }

            if row_values.len() != field_names.len() {
                continue;
            }

            let row_parsed: Vec<Value> = field_names
                .iter()
                .zip(row_values.iter())
                .map(|(name, val_str)| {
                    let field_def = fields.iter().find(|f| &f.name == name).unwrap();
                    Self::parse_value(val_str, &field_def.ty)
                })
                .collect();

            let record = Record::new(row_parsed);

            if seen_hashes.insert(record.hash) {
                if !self.config.no_wal {
                    wal_entries.push(WalInsert {
                        bucket: name.clone(),
                        hash: record.hash,
                        fields: record.fields.clone(),
                    });
                }
                all_records.push(record);
            }
        }

        if !self.config.no_wal {
            for entry in wal_entries {
                self.wal.append_insert(&entry)?;
            }
        }

        let handle = self
            .buckets
            .get(&name)
            .ok_or_else(|| ArcaneError::BucketNotFound(name.clone()))?;

        let record_count = all_records.len();
        let mut store = handle.write();
        store.bulk_insert(all_records)?;
        drop(store);

        if record_count >= 1000 {
            self.checkpoint()?;
        }

        Ok(QueryResult::BucketCreated { name })
    }

    fn parse_csv_line(line: &str) -> Vec<String> {
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                '"' => {
                    if in_quotes {
                        if chars.peek() == Some(&'"') {
                            current_field.push('"');
                            chars.next();
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        in_quotes = true;
                    }
                }
                ',' if !in_quotes => {
                    fields.push(current_field.trim().to_string());
                    current_field.clear();
                }
                _ => {
                    current_field.push(c);
                }
            }
        }

        fields.push(current_field.trim().to_string());
        fields
    }

    fn infer_type(value_str: &str) -> crate::storage::FieldType {
        if value_str.is_empty() {
            return crate::storage::FieldType::String;
        }

        if value_str.parse::<i64>().is_ok() {
            return crate::storage::FieldType::Int;
        }

        if value_str.parse::<f64>().is_ok() {
            return crate::storage::FieldType::Float;
        }

        match value_str.to_lowercase().as_str() {
            "true" | "false" => return crate::storage::FieldType::Bool,
            _ => {}
        }

        crate::storage::FieldType::String
    }

    fn parse_value(value_str: &str, field_type: &crate::storage::FieldType) -> Value {
        if value_str.is_empty() {
            return Value::Null;
        }

        match field_type {
            crate::storage::FieldType::String => Value::String(value_str.to_string()),
            crate::storage::FieldType::Int => Value::Int(value_str.parse().unwrap_or(0)),
            crate::storage::FieldType::Float => Value::Float(value_str.parse().unwrap_or(0.0)),
            crate::storage::FieldType::Bool => Value::Bool(value_str.to_lowercase() == "true"),
            crate::storage::FieldType::Bytes => Value::Bytes(value_str.as_bytes().to_vec()),
        }
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

    #[test]
    fn test_set_update_single_field() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (\"Alice\", 30)").unwrap();
        db.execute("insert into Users (\"Bob\", 25)").unwrap();

        let result = db
            .execute("set Users (age: 31) where name = \"Alice\"")
            .unwrap();

        match result {
            QueryResult::Updated { bucket, count } => {
                assert_eq!(bucket, "Users");
                assert_eq!(count, 1);
            }
            _ => panic!("Expected Updated"),
        }

        let result = db
            .execute("get * from Users where name = \"Alice\"")
            .unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][2], "31"); // age field
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_set_update_multiple_fields() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int, city: string)")
            .unwrap();
        db.execute("insert into Users (\"Alice\", 30, \"NYC\")")
            .unwrap();

        let result = db
            .execute("set Users (name: \"Alicia\", age: 31, city: \"LA\") where name = \"Alice\"")
            .unwrap();

        match result {
            QueryResult::Updated { count, .. } => {
                assert_eq!(count, 1);
            }
            _ => panic!("Expected Updated"),
        }

        let result = db.execute("get * from Users").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], "Alicia");
                assert_eq!(rows[0][2], "31");
                assert_eq!(rows[0][3], "LA");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_set_update_multiple_rows() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (\"Alice\", 30)").unwrap();
        db.execute("insert into Users (\"Bob\", 30)").unwrap();
        db.execute("insert into Users (\"Charlie\", 25)").unwrap();

        let result = db.execute("set Users (age: 31) where age = 30").unwrap();

        match result {
            QueryResult::Updated { count, .. } => {
                assert_eq!(count, 2);
            }
            _ => panic!("Expected Updated"),
        }

        let result = db.execute("get * from Users where age = 31").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_set_unknown_field() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (\"Alice\", 30)").unwrap();

        let result = db.execute(
            "set Users (name: \"Bob\", random_bullshit_column: 12345) where name = \"Alice\"",
        );

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ArcaneError::UnknownField(_)));
    }

    #[test]
    fn test_set_type_mismatch() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (\"Alice\", 30)").unwrap();

        let result = db.execute("set Users (age: \"not a number\") where name = \"Alice\"");

        assert!(result.is_err());
    }

    #[test]
    fn test_set_no_matches() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (\"Alice\", 30)").unwrap();

        let result = db
            .execute("set Users (age: 40) where name = \"Bob\"")
            .unwrap();

        match result {
            QueryResult::Updated { count, .. } => {
                assert_eq!(count, 0);
            }
            _ => panic!("Expected Updated"),
        }
    }

    #[test]
    fn test_set_positional() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (\"Alice\", 30)").unwrap();

        let result = db
            .execute("set Users (\"Bob\", 35) where name = \"Alice\"")
            .unwrap();

        match result {
            QueryResult::Updated { count, .. } => {
                assert_eq!(count, 1);
            }
            _ => panic!("Expected Updated"),
        }

        let result = db.execute("get * from Users").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], "Bob");
                assert_eq!(rows[0][2], "35");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_set_bucket_not_found() {
        let (db, _dir) = setup_db();
        let result = db.execute("set NonExistent (name: \"test\") where id = 1");

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ArcaneError::BucketNotFound(_)
        ));
    }

    #[test]
    fn test_get_single_field_projection() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int, city: string)")
            .unwrap();
        db.execute("insert into Users (\"Alice\", 30, \"NYC\")")
            .unwrap();
        db.execute("insert into Users (\"Bob\", 25, \"LA\")")
            .unwrap();

        let result = db.execute("get name from Users").unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 1);
                assert_eq!(schema[0], "name");
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].len(), 1);
                assert_eq!(rows[0][0], "Alice");
                assert_eq!(rows[1][0], "Bob");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_get_multiple_field_projection() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int, city: string)")
            .unwrap();
        db.execute("insert into Users (\"Alice\", 30, \"NYC\")")
            .unwrap();
        db.execute("insert into Users (\"Bob\", 25, \"LA\")")
            .unwrap();

        let result = db.execute("get name, city from Users").unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 2);
                assert_eq!(schema[0], "name");
                assert_eq!(schema[1], "city");
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], "Alice");
                assert_eq!(rows[0][1], "NYC");
                assert_eq!(rows[1][0], "Bob");
                assert_eq!(rows[1][1], "LA");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_get_field_projection_with_filter() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (\"Alice\", 30)").unwrap();
        db.execute("insert into Users (\"Bob\", 25)").unwrap();
        db.execute("insert into Users (\"Charlie\", 35)").unwrap();

        let result = db.execute("get name from Users where age > 26").unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 1);
                assert_eq!(schema[0], "name");
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], "Alice");
                assert_eq!(rows[1][0], "Charlie");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_get_field_projection_unknown_field() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (\"Alice\", 30)").unwrap();

        let result = db.execute("get name, nonexistent from Users");

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ArcaneError::UnknownField(_)));
    }

    #[test]
    fn test_get_order_by_asc() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (\"Charlie\", 35)").unwrap();
        db.execute("insert into Users (\"Alice\", 30)").unwrap();
        db.execute("insert into Users (\"Bob\", 25)").unwrap();

        let result = db.execute("get * from Users order by age asc").unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0][2], "25"); // Bob
                assert_eq!(rows[1][2], "30"); // Alice
                assert_eq!(rows[2][2], "35"); // Charlie
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_get_order_by_desc() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (\"Charlie\", 35)").unwrap();
        db.execute("insert into Users (\"Alice\", 30)").unwrap();
        db.execute("insert into Users (\"Bob\", 25)").unwrap();

        let result = db.execute("get * from Users order by age desc").unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0][2], "35"); // Charlie
                assert_eq!(rows[1][2], "30"); // Alice
                assert_eq!(rows[2][2], "25"); // Bob
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_get_order_by_string() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (\"Charlie\", 35)").unwrap();
        db.execute("insert into Users (\"Alice\", 30)").unwrap();
        db.execute("insert into Users (\"Bob\", 25)").unwrap();

        let result = db.execute("get * from Users order by name asc").unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0][1], "Alice");
                assert_eq!(rows[1][1], "Bob");
                assert_eq!(rows[2][1], "Charlie");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_get_order_by_with_filter() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (\"Charlie\", 35)").unwrap();
        db.execute("insert into Users (\"Alice\", 30)").unwrap();
        db.execute("insert into Users (\"Bob\", 25)").unwrap();
        db.execute("insert into Users (\"Diana\", 40)").unwrap();

        let result = db
            .execute("get * from Users where age > 26 order by age asc")
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0][1], "Alice"); // 30
                assert_eq!(rows[1][1], "Charlie"); // 35
                assert_eq!(rows[2][1], "Diana"); // 40
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_get_order_by_unknown_field() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (\"Alice\", 30)").unwrap();

        let result = db.execute("get * from Users order by nonexistent asc");

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ArcaneError::UnknownField(_)));
    }

    #[test]
    fn test_get_order_by_with_field_projection() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int, city: string)")
            .unwrap();
        db.execute("insert into Users (\"Charlie\", 35, \"NYC\")")
            .unwrap();
        db.execute("insert into Users (\"Alice\", 30, \"LA\")")
            .unwrap();
        db.execute("insert into Users (\"Bob\", 25, \"SF\")")
            .unwrap();

        let result = db
            .execute("get name, age from Users order by age desc")
            .unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 2);
                assert_eq!(schema[0], "name");
                assert_eq!(schema[1], "age");
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0][0], "Charlie");
                assert_eq!(rows[0][1], "35");
                assert_eq!(rows[1][0], "Alice");
                assert_eq!(rows[2][0], "Bob");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_like_pattern_prefix() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float)")
            .unwrap();
        db.execute("insert into Products (name: \"Widget\", price: 9.99)")
            .unwrap();
        db.execute("insert into Products (name: \"Gadget\", price: 19.99)")
            .unwrap();
        db.execute("insert into Products (name: \"Doohickey\", price: 4.99)")
            .unwrap();

        let result = db
            .execute("get * from Products where name like \"D%\"")
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert!(rows[0][1].contains("Doohickey"));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_like_pattern_suffix() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string)").unwrap();
        db.execute("insert into Products (name: \"Widget\")")
            .unwrap();
        db.execute("insert into Products (name: \"Gadget\")")
            .unwrap();
        db.execute("insert into Products (name: \"Doohickey\")")
            .unwrap();

        let result = db
            .execute("get * from Products where name like \"%et\"")
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_like_pattern_contains() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string)").unwrap();
        db.execute("insert into Products (name: \"Widget\")")
            .unwrap();
        db.execute("insert into Products (name: \"Gadget\")")
            .unwrap();
        db.execute("insert into Products (name: \"Doohickey\")")
            .unwrap();

        let result = db
            .execute("get * from Products where name like \"%dg%\"")
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_like_pattern_underscore() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string)").unwrap();
        db.execute("insert into Products (name: \"Cat\")").unwrap();
        db.execute("insert into Products (name: \"Bat\")").unwrap();
        db.execute("insert into Products (name: \"Hat\")").unwrap();
        db.execute("insert into Products (name: \"Boat\")").unwrap();

        let result = db
            .execute("get * from Products where name like \"_at\"")
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3); // Cat, Bat, Hat (not Boat)
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_like_pattern_exact_match() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string)").unwrap();
        db.execute("insert into Products (name: \"Widget\")")
            .unwrap();
        db.execute("insert into Products (name: \"Gadget\")")
            .unwrap();

        let result = db
            .execute("get * from Products where name like \"Widget\"")
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert!(rows[0][1].contains("Widget"));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_like_with_complex_filter() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float, in_stock: bool)")
            .unwrap();
        db.execute("insert into Products (name: \"Doohickey\", price: 4.49, in_stock: true)")
            .unwrap();
        db.execute("insert into Products (name: \"Doohickey2\", price: 4.49, in_stock: false)")
            .unwrap();
        db.execute("insert into Products (name: \"Widget\", price: 9.99, in_stock: true)")
            .unwrap();

        let result = db
            .execute(
                "get name from Products where name like \"D%\" and price > 4 and in_stock = true",
            )
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "Doohickey");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_describe_bucket() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float, in_stock: bool)")
            .unwrap();
        db.execute("insert into Products (name: \"Widget\", price: 9.99, in_stock: true)")
            .unwrap();
        db.execute("insert into Products (name: \"Gadget\", price: 19.99, in_stock: false)")
            .unwrap();

        let result = db.execute("describe Products").unwrap();

        match result {
            QueryResult::Described {
                bucket,
                fields,
                row_count,
            } => {
                assert_eq!(bucket, "Products");
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].0, "name");
                assert_eq!(fields[0].1, "string");
                assert_eq!(fields[1].0, "price");
                assert_eq!(fields[1].1, "float");
                assert_eq!(fields[2].0, "in_stock");
                assert_eq!(fields[2].1, "bool");
                assert_eq!(row_count, 2);
            }
            _ => panic!("Expected Described"),
        }
    }

    #[test]
    fn test_describe_empty_bucket() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();

        let result = db.execute("describe Users").unwrap();

        match result {
            QueryResult::Described {
                bucket,
                fields,
                row_count,
            } => {
                assert_eq!(bucket, "Users");
                assert_eq!(fields.len(), 2);
                assert_eq!(row_count, 0);
            }
            _ => panic!("Expected Described"),
        }
    }

    #[test]
    fn test_describe_bucket_not_found() {
        let (db, _dir) = setup_db();
        let result = db.execute("describe NonExistent");

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ArcaneError::BucketNotFound(_)
        ));
    }

    #[test]
    fn test_describe_after_truncate() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (name: \"Alice\", age: 30)")
            .unwrap();
        db.execute("insert into Users (name: \"Bob\", age: 25)")
            .unwrap();
        db.execute("truncate Users").unwrap();

        let result = db.execute("describe Users").unwrap();

        match result {
            QueryResult::Described { row_count, .. } => {
                assert_eq!(row_count, 0);
            }
            _ => panic!("Expected Described"),
        }
    }

    #[test]
    fn test_aggregate_avg() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float)")
            .unwrap();
        db.execute("insert into Products (name: \"Widget\", price: 10.0)")
            .unwrap();
        db.execute("insert into Products (name: \"Gadget\", price: 20.0)")
            .unwrap();
        db.execute("insert into Products (name: \"Doohickey\", price: 30.0)")
            .unwrap();

        let result = db.execute("get avg(price) from Products").unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 1);
                assert_eq!(schema[0], "avg(price)");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "20.00");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_aggregate_count_star() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float)")
            .unwrap();
        db.execute("insert into Products (name: \"Widget\", price: 10.0)")
            .unwrap();
        db.execute("insert into Products (name: \"Gadget\", price: 20.0)")
            .unwrap();
        db.execute("insert into Products (name: \"Doohickey\", price: 30.0)")
            .unwrap();

        let result = db.execute("get count(*) from Products").unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 1);
                assert_eq!(schema[0], "count(*)");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "3");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_aggregate_count_field() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float)")
            .unwrap();
        db.execute("insert into Products (name: \"Widget\", price: 10.0)")
            .unwrap();
        db.execute("insert into Products (name: \"Gadget\")")
            .unwrap();
        db.execute("insert into Products (name: \"Doohickey\", price: 30.0)")
            .unwrap();

        let result = db.execute("get count(price) from Products").unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 1);
                assert_eq!(schema[0], "count(price)");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "2");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_aggregate_sum() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float)")
            .unwrap();
        db.execute("insert into Products (name: \"Widget\", price: 10.0)")
            .unwrap();
        db.execute("insert into Products (name: \"Gadget\", price: 20.0)")
            .unwrap();
        db.execute("insert into Products (name: \"Doohickey\", price: 30.0)")
            .unwrap();

        let result = db.execute("get sum(price) from Products").unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 1);
                assert_eq!(schema[0], "sum(price)");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "60.00");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_aggregate_min() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float)")
            .unwrap();
        db.execute("insert into Products (name: \"Widget\", price: 10.0)")
            .unwrap();
        db.execute("insert into Products (name: \"Gadget\", price: 20.0)")
            .unwrap();
        db.execute("insert into Products (name: \"Doohickey\", price: 5.0)")
            .unwrap();

        let result = db.execute("get min(price) from Products").unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 1);
                assert_eq!(schema[0], "min(price)");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "5");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_aggregate_max() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float)")
            .unwrap();
        db.execute("insert into Products (name: \"Widget\", price: 10.0)")
            .unwrap();
        db.execute("insert into Products (name: \"Gadget\", price: 20.0)")
            .unwrap();
        db.execute("insert into Products (name: \"Doohickey\", price: 5.0)")
            .unwrap();

        let result = db.execute("get max(price) from Products").unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 1);
                assert_eq!(schema[0], "max(price)");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "20");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_aggregate_median_odd_count() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float)")
            .unwrap();
        db.execute("insert into Products (name: \"A\", price: 10.0)")
            .unwrap();
        db.execute("insert into Products (name: \"B\", price: 20.0)")
            .unwrap();
        db.execute("insert into Products (name: \"C\", price: 30.0)")
            .unwrap();

        let result = db.execute("get median(price) from Products").unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 1);
                assert_eq!(schema[0], "median(price)");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "20.00");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_aggregate_median_even_count() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float)")
            .unwrap();
        db.execute("insert into Products (name: \"A\", price: 10.0)")
            .unwrap();
        db.execute("insert into Products (name: \"B\", price: 20.0)")
            .unwrap();
        db.execute("insert into Products (name: \"C\", price: 30.0)")
            .unwrap();
        db.execute("insert into Products (name: \"D\", price: 40.0)")
            .unwrap();

        let result = db.execute("get median(price) from Products").unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 1);
                assert_eq!(schema[0], "median(price)");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "25.00");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_aggregate_stddev() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float)")
            .unwrap();
        db.execute("insert into Products (name: \"A\", price: 10.0)")
            .unwrap();
        db.execute("insert into Products (name: \"B\", price: 20.0)")
            .unwrap();
        db.execute("insert into Products (name: \"C\", price: 30.0)")
            .unwrap();

        let result = db.execute("get stddev(price) from Products").unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 1);
                assert_eq!(schema[0], "stddev(price)");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "8.16");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_aggregate_multiple_functions() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float)")
            .unwrap();
        db.execute("insert into Products (name: \"Widget\", price: 10.0)")
            .unwrap();
        db.execute("insert into Products (name: \"Gadget\", price: 20.0)")
            .unwrap();
        db.execute("insert into Products (name: \"Doohickey\", price: 30.0)")
            .unwrap();

        let result = db
            .execute("get avg(price), sum(price), min(price), max(price) from Products")
            .unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 4);
                assert_eq!(schema[0], "avg(price)");
                assert_eq!(schema[1], "sum(price)");
                assert_eq!(schema[2], "min(price)");
                assert_eq!(schema[3], "max(price)");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "20.00");
                assert_eq!(rows[0][1], "60.00");
                assert_eq!(rows[0][2], "10");
                assert_eq!(rows[0][3], "30");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_aggregate_with_filter() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float, in_stock: bool)")
            .unwrap();
        db.execute("insert into Products (name: \"Widget\", price: 10.0, in_stock: true)")
            .unwrap();
        db.execute("insert into Products (name: \"Gadget\", price: 20.0, in_stock: false)")
            .unwrap();
        db.execute("insert into Products (name: \"Doohickey\", price: 30.0, in_stock: true)")
            .unwrap();

        let result = db
            .execute("get avg(price), sum(price) from Products where in_stock = true")
            .unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 2);
                assert_eq!(schema[0], "avg(price)");
                assert_eq!(schema[1], "sum(price)");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "20.00");
                assert_eq!(rows[0][1], "40.00");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_aggregate_with_int_field() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (name: \"Alice\", age: 25)")
            .unwrap();
        db.execute("insert into Users (name: \"Bob\", age: 30)")
            .unwrap();
        db.execute("insert into Users (name: \"Charlie\", age: 35)")
            .unwrap();

        let result = db.execute("get avg(age), sum(age) from Users").unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 2);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "30.00");
                assert_eq!(rows[0][1], "90.00");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_aggregate_empty_result() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float)")
            .unwrap();

        let result = db.execute("get avg(price) from Products").unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 1);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "NULL");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_aggregate_with_null_values() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float)")
            .unwrap();
        db.execute("insert into Products (name: \"Widget\", price: 10.0)")
            .unwrap();
        db.execute("insert into Products (name: \"Gadget\")")
            .unwrap();
        db.execute("insert into Products (name: \"Doohickey\", price: 30.0)")
            .unwrap();

        let result = db
            .execute("get avg(price), sum(price) from Products")
            .unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 2);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "20.00");
                assert_eq!(rows[0][1], "40.00");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_aggregate_unknown_field() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float)")
            .unwrap();
        db.execute("insert into Products (name: \"Widget\", price: 10.0)")
            .unwrap();

        let result = db.execute("get avg(nonexistent) from Products");

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ArcaneError::UnknownField(_)));
    }

    #[test]
    fn test_aggregate_non_numeric_field() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float)")
            .unwrap();
        db.execute("insert into Products (name: \"Widget\", price: 10.0)")
            .unwrap();

        let result = db.execute("get avg(name) from Products");

        assert!(result.is_err());
    }

    #[test]
    fn test_aggregate_min_max_string() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Products (name: string)").unwrap();
        db.execute("insert into Products (name: \"Zebra\")")
            .unwrap();
        db.execute("insert into Products (name: \"Apple\")")
            .unwrap();
        db.execute("insert into Products (name: \"Mango\")")
            .unwrap();

        let result = db
            .execute("get min(name), max(name) from Products")
            .unwrap();

        match result {
            QueryResult::Rows { schema, rows } => {
                assert_eq!(schema.len(), 2);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "Apple");
                assert_eq!(rows[0][1], "Zebra");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_string_function_upper() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string)").unwrap();
        db.execute("insert into Users (name: \"alice\")").unwrap();

        let result = db
            .execute("set Users (name: upper(\"billy\")) where name = \"alice\"")
            .unwrap();

        match result {
            QueryResult::Updated { count, .. } => {
                assert_eq!(count, 1);
            }
            _ => panic!("Expected Updated"),
        }

        let result = db.execute("get * from Users").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], "BILLY");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_string_function_lower() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string)").unwrap();
        db.execute("insert into Users (name: \"ALICE\")").unwrap();

        let result = db
            .execute("set Users (name: lower(\"BOBBY\")) where name = \"ALICE\"")
            .unwrap();

        match result {
            QueryResult::Updated { count, .. } => {
                assert_eq!(count, 1);
            }
            _ => panic!("Expected Updated"),
        }

        let result = db.execute("get * from Users").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], "bobby");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_string_function_title() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string)").unwrap();
        db.execute("insert into Users (name: \"alice smith\")")
            .unwrap();

        let result = db
            .execute("set Users (name: title(\"john doe\")) where name = \"alice smith\"")
            .unwrap();

        match result {
            QueryResult::Updated { count, .. } => {
                assert_eq!(count, 1);
            }
            _ => panic!("Expected Updated"),
        }

        let result = db.execute("get * from Users").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], "John Doe");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_string_function_title_from_uppercase() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string)").unwrap();
        db.execute("insert into Users (name: \"ALICE\")").unwrap();

        let result = db
            .execute("set Users (name: title(\"JOHN DOE\")) where name = \"ALICE\"")
            .unwrap();

        match result {
            QueryResult::Updated { count, .. } => {
                assert_eq!(count, 1);
            }
            _ => panic!("Expected Updated"),
        }

        let result = db.execute("get * from Users").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], "John Doe");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_string_function_in_where_clause() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string)").unwrap();
        db.execute("insert into Users (name: \"Alice\")").unwrap();
        db.execute("insert into Users (name: \"Bob\")").unwrap();
        db.execute("insert into Users (name: \"Eve\")").unwrap();

        let result = db
            .execute("get * from Users where name = title(\"eve\")")
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], "Eve");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_string_function_in_insert() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string)").unwrap();
        db.execute("insert into Users (name: upper(\"alice\"))")
            .unwrap();

        let result = db.execute("get * from Users").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], "ALICE");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_string_function_nested() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string)").unwrap();
        db.execute("insert into Users (name: upper(lower(\"ALICE\")))")
            .unwrap();

        let result = db.execute("get * from Users").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], "ALICE");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_is_null_non_existent_field() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (name: \"Alice\", age: 30)")
            .unwrap();
        db.execute("insert into Users (name: \"Bob\", age: 25)")
            .unwrap();

        let result = db
            .execute("get * from Users where non_existent_field is null")
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_is_not_null_non_existent_field() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (name: \"Alice\", age: 30)")
            .unwrap();
        db.execute("insert into Users (name: \"Bob\", age: 25)")
            .unwrap();

        let result = db
            .execute("get * from Users where non_existent_field is not null")
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 0);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_is_null_existing_field_with_null_values() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (name: \"Alice\", age: 30)")
            .unwrap();
        db.execute("insert into Users (name: \"Bob\")").unwrap();

        let result = db.execute("get * from Users where age is null").unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], "Bob");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_is_not_null_existing_field() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (name: \"Alice\", age: 30)")
            .unwrap();
        db.execute("insert into Users (name: \"Bob\")").unwrap();

        let result = db
            .execute("get * from Users where age is not null")
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], "Alice");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_is_null_with_and_filter() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int, city: string)")
            .unwrap();
        db.execute("insert into Users (name: \"Alice\", age: 30, city: \"NYC\")")
            .unwrap();
        db.execute("insert into Users (name: \"Bob\", age: 25)")
            .unwrap();
        db.execute("insert into Users (name: \"Charlie\")").unwrap();

        let result = db
            .execute("get * from Users where city is null and age is not null")
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], "Bob");
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_is_null_with_or_filter() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int, city: string)")
            .unwrap();
        db.execute("insert into Users (name: \"Alice\", age: 30, city: \"NYC\")")
            .unwrap();
        db.execute("insert into Users (name: \"Bob\", age: 25)")
            .unwrap();
        db.execute("insert into Users (name: \"Charlie\")").unwrap();

        let result = db
            .execute("get * from Users where city is null or age is null")
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_is_null_all_null_values() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (name: \"Alice\")").unwrap();
        db.execute("insert into Users (name: \"Bob\")").unwrap();

        let result = db.execute("get * from Users where age is null").unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_is_not_null_no_null_values() {
        let (db, _dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();
        db.execute("insert into Users (name: \"Alice\", age: 30)")
            .unwrap();
        db.execute("insert into Users (name: \"Bob\", age: 25)")
            .unwrap();

        let result = db
            .execute("get * from Users where age is not null")
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_print_command() {
        let (db, _dir) = setup_db();
        let result = db.execute("print \"Hello, World!\"").unwrap();

        match result {
            QueryResult::Printed { message } => {
                assert_eq!(message, "Hello, World!");
            }
            _ => panic!("Expected Printed"),
        }
    }

    #[test]
    fn test_print_with_special_characters() {
        let (db, _dir) = setup_db();
        let result = db.execute("print \"Line 1\\nLine 2\\tTabbed\"").unwrap();

        match result {
            QueryResult::Printed { message } => {
                assert_eq!(message, "Line 1\nLine 2\tTabbed");
            }
            _ => panic!("Expected Printed"),
        }
    }

    #[test]
    fn test_print_empty_string() {
        let (db, _dir) = setup_db();
        let result = db.execute("print \"\"").unwrap();

        match result {
            QueryResult::Printed { message } => {
                assert_eq!(message, "");
            }
            _ => panic!("Expected Printed"),
        }
    }

    #[test]
    fn test_export_csv() {
        let (db, dir) = setup_db();
        db.execute("create bucket Products (name: string, price: float, in_stock: bool)")
            .unwrap();
        db.execute("insert into Products (name: \"Widget\", price: 9.99, in_stock: true)")
            .unwrap();
        db.execute("insert into Products (name: \"Gadget\", price: 19.99, in_stock: false)")
            .unwrap();

        let csv_path = dir.path().join("products.csv");
        let csv_path_str = csv_path.to_string_lossy().replace('\\', "/");
        let result = db
            .execute(&format!("export Products to csv(\"{}\")", csv_path_str))
            .unwrap();

        match result {
            QueryResult::Exported {
                bucket,
                path,
                count,
            } => {
                assert_eq!(bucket, "Products");
                assert_eq!(count, 2);
                assert!(std::path::Path::new(&path).exists());
            }
            _ => panic!("Expected Exported"),
        }

        let csv_content = std::fs::read_to_string(&csv_path).unwrap();
        assert!(csv_content.contains("name,price,in_stock"));
        assert!(csv_content.contains("Widget"));
        assert!(csv_content.contains("Gadget"));
    }

    #[test]
    fn test_export_csv_empty_bucket() {
        let (db, dir) = setup_db();
        db.execute("create bucket Users (name: string, age: int)")
            .unwrap();

        let csv_path = dir.path().join("users.csv");
        let csv_path_str = csv_path.to_string_lossy().replace('\\', "/");
        let result = db
            .execute(&format!("export Users to csv(\"{}\")", csv_path_str))
            .unwrap();

        match result {
            QueryResult::Exported { count, .. } => {
                assert_eq!(count, 0);
            }
            _ => panic!("Expected Exported"),
        }

        let csv_content = std::fs::read_to_string(&csv_path).unwrap();
        assert!(csv_content.contains("name,age"));
    }

    #[test]
    fn test_export_csv_with_special_characters() {
        let (db, dir) = setup_db();
        db.execute("create bucket Products (name: string, description: string)")
            .unwrap();
        db.execute(
            "insert into Products (name: \"Widget\", description: \"A great, useful item\")",
        )
        .unwrap();
        db.execute("insert into Products (name: \"Gadget\", description: \"Has \\\"quotes\\\"\")")
            .unwrap();

        let csv_path = dir.path().join("products.csv");
        let csv_path_str = csv_path.to_string_lossy().replace('\\', "/");
        db.execute(&format!("export Products to csv(\"{}\")", csv_path_str))
            .unwrap();

        let csv_content = std::fs::read_to_string(&csv_path).unwrap();
        assert!(csv_content.contains("\"A great, useful item\""));
    }

    #[test]
    fn test_export_csv_bucket_not_found() {
        let (db, dir) = setup_db();
        let csv_path = dir.path().join("nonexistent.csv");
        let csv_path_str = csv_path.to_string_lossy().replace('\\', "/");
        let result = db.execute(&format!("export NonExistent to csv(\"{}\")", csv_path_str));

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ArcaneError::BucketNotFound(_)
        ));
    }

    #[test]
    fn test_create_bucket_from_csv() {
        let (db, dir) = setup_db();
        let csv_path = dir.path().join("test.csv");
        let csv_content = "name,age,active\nAlice,30,true\nBob,25,false\nCharlie,35,true\n";

        std::fs::write(&csv_path, csv_content).unwrap();

        let csv_path_str = csv_path.to_string_lossy().replace('\\', "/");
        let result = db
            .execute(&format!(
                "create bucket Users from csv(\"{}\")",
                csv_path_str
            ))
            .unwrap();

        match result {
            QueryResult::BucketCreated { name } => {
                assert_eq!(name, "Users");
            }
            _ => panic!("Expected BucketCreated"),
        }

        let result = db.execute("get * from Users").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_create_bucket_from_csv_with_types() {
        let (db, dir) = setup_db();
        let csv_path = dir.path().join("products.csv");
        let csv_content = "name,price,quantity\nWidget,9.99,100\nGadget,19.99,50\n";

        std::fs::write(&csv_path, csv_content).unwrap();

        let csv_path_str = csv_path.to_string_lossy().replace('\\', "/");
        db.execute(&format!(
            "create bucket Products from csv(\"{}\")",
            csv_path_str
        ))
        .unwrap();

        let schema = db.schema("Products").unwrap();
        assert_eq!(schema.fields.len(), 3);
        assert_eq!(schema.fields[0].name, "name");
        assert_eq!(schema.fields[1].name, "price");
        assert_eq!(schema.fields[2].name, "quantity");
    }

    #[test]
    fn test_create_bucket_from_csv_empty_file() {
        let (db, dir) = setup_db();
        let csv_path = dir.path().join("empty.csv");

        std::fs::write(&csv_path, "").unwrap();

        let csv_path_str = csv_path.to_string_lossy().replace('\\', "/");
        let result = db.execute(&format!(
            "create bucket Empty from csv(\"{}\")",
            csv_path_str
        ));

        assert!(result.is_err());
    }

    #[test]
    fn test_create_bucket_from_csv_file_not_found() {
        let (db, _dir) = setup_db();
        let result = db.execute("create bucket Users from csv(\"nonexistent.csv\")");
        assert!(result.is_err());
    }

    #[test]
    fn test_export_and_import_roundtrip() {
        let (db, dir) = setup_db();

        db.execute("create bucket Original (name: string, value: int, active: bool)")
            .unwrap();
        db.execute("insert into Original (name: \"Test1\", value: 100, active: true)")
            .unwrap();
        db.execute("insert into Original (name: \"Test2\", value: 200, active: false)")
            .unwrap();

        let csv_path = dir.path().join("export.csv");
        let csv_path_str = csv_path.to_string_lossy().replace('\\', "/");

        db.execute(&format!("export Original to csv(\"{}\")", csv_path_str))
            .unwrap();
        db.execute(&format!(
            "create bucket Imported from csv(\"{}\")",
            csv_path_str
        ))
        .unwrap();

        let original_result = db.execute("get * from Original").unwrap();
        let imported_result = db.execute("get * from Imported").unwrap();

        match (original_result, imported_result) {
            (
                QueryResult::Rows {
                    rows: original_rows,
                    ..
                },
                QueryResult::Rows {
                    rows: imported_rows,
                    ..
                },
            ) => {
                assert_eq!(original_rows.len(), imported_rows.len());
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_csv_with_null_values() {
        let (db, dir) = setup_db();
        let csv_path = dir.path().join("nulls.csv");
        let csv_content = "name,age,city\nAlice,30,NYC\nBob,,LA\nCharlie,35,\n";

        std::fs::write(&csv_path, csv_content).unwrap();

        let csv_path_str = csv_path.to_string_lossy().replace('\\', "/");
        db.execute(&format!(
            "create bucket Users from csv(\"{}\")",
            csv_path_str
        ))
        .unwrap();

        let result = db.execute("get * from Users where age is null").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], "Bob");
            }
            _ => panic!("Expected Rows"),
        }
    }
}
