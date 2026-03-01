//! Arcane - Copyright (C) Navid Momtahen 2026
//!
//! License: GPL-3.0-only

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ArcaneError {
    #[error("Bucket '{0}' already exists")]
    BucketExists(String),

    #[error("Bucket '{0}' not found")]
    BucketNotFound(String),

    #[error("Duplicate record: hash {0:#016x} already exists in bucket '{1}'")]
    DuplicateRecord(u64, String),

    #[error("Schema mismatch: expected {expected} fields, got {got}")]
    SchemaMismatch { expected: usize, got: usize },

    #[error("Unknown field '{0}'")]
    UnknownField(String),

    #[error("Type error: field '{field}' expects {expected}, got {got}")]
    TypeError {
        field: String,
        expected: String,
        got: String,
    },

    #[error("Parse error at position {pos}: {msg}")]
    ParseError { pos: usize, msg: String },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialize(#[from] bincode::Error),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("Lock poisoned")]
    LockPoisoned,

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, ArcaneError>;
