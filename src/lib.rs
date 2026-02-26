//! Arcane - Copyright (C) Navid Momtahen 2026
//!
//! License: GPL-3.0-only

pub mod engine;
pub mod error;
pub mod meta;
pub mod parser;
pub mod server;
pub mod storage;
pub mod wal;

pub use engine::{Config, Database, QueryResult};
pub use error::ArcaneError;
