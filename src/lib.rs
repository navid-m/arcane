//! Arcane - Copyright (C) Navid Momtahen 2026
//!
//! A high-performance, hash-bucketed database engine designed for extreme throughput
//! on concurrent reads and writes. Arcane can be embedded as a library (like SQLite)
//! or run as a standalone server.

pub mod engine;
pub mod error;
pub mod parser;
pub mod server;
pub mod storage;
pub mod wal;

pub use engine::{Config, Database, QueryResult};
pub use error::ArcaneError;
