//! Real-time Advanced Analytics Engine for the Arcane DBMS
//!
//! This module provides streaming analytics, window functions, time-series analysis,
//! and statistical computations for real-time data processing.

use crate::error::{ArcaneError, Result};
use crate::storage::{Record, Schema, Value};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub mod stats;
pub mod streaming;
pub mod time_series;
pub mod window;

pub use stats::{Distribution, StatisticalAnalyzer};
pub use streaming::{StreamProcessor, StreamWindow};
pub use time_series::{TimeSeriesAnalyzer, Trend};
pub use window::{WindowFunction, WindowType};

/// Analytics engine that processes records in real-time
pub struct AnalyticsEngine {
    processors: Arc<RwLock<HashMap<String, StreamProcessor>>>,
    analyzers: Arc<RwLock<HashMap<String, TimeSeriesAnalyzer>>>,
}

impl AnalyticsEngine {
    pub fn new() -> Self {
        AnalyticsEngine {
            processors: Arc::new(RwLock::new(HashMap::new())),
            analyzers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new stream processor for a bucket
    pub fn create_stream(&self, name: String, window_size: usize) -> Result<()> {
        let processor = StreamProcessor::new(window_size);
        self.processors.write().insert(name, processor);
        Ok(())
    }

    /// Process a record through the analytics pipeline
    pub fn process_record(
        &self,
        stream_name: &str,
        record: &Record,
        schema: &Schema,
    ) -> Result<()> {
        if let Some(processor) = self.processors.write().get_mut(stream_name) {
            processor.add_record(record.clone(), schema)?;
        }
        Ok(())
    }

    /// Get real-time metrics for a stream
    pub fn get_metrics(&self, stream_name: &str, field: &str) -> Result<StreamMetrics> {
        let processors = self.processors.read();
        let processor = processors
            .get(stream_name)
            .ok_or_else(|| ArcaneError::Other(format!("Stream '{}' not found", stream_name)))?;

        processor.compute_metrics(field)
    }

    /// Create a time-series analyzer
    pub fn create_timeseries(&self, name: String) -> Result<()> {
        let analyzer = TimeSeriesAnalyzer::new();
        self.analyzers.write().insert(name, analyzer);
        Ok(())
    }

    /// Analyze time-series data
    pub fn analyze_timeseries(&self, name: &str, values: Vec<f64>) -> Result<TimeSeriesResult> {
        let mut analyzers = self.analyzers.write();
        let analyzer = analyzers
            .get_mut(name)
            .ok_or_else(|| ArcaneError::Other(format!("Analyzer '{}' not found", name)))?;

        analyzer.analyze(values)
    }
}

impl Default for AnalyticsEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Real-time metrics computed from streaming data
#[derive(Debug, Clone)]
pub struct StreamMetrics {
    pub count: usize,
    pub sum: f64,
    pub mean: f64,
    pub min: f64,
    pub max: f64,
    pub stddev: f64,
    pub variance: f64,
    pub moving_avg: f64,
    pub rate_of_change: f64,
}

/// Time-series analysis results
#[derive(Debug, Clone)]
pub struct TimeSeriesResult {
    pub trend: Trend,
    pub seasonality: Option<f64>,
    pub forecast: Vec<f64>,
    pub anomalies: Vec<usize>,
    pub correlation: f64,
}

/// Extract numeric value from a Value enum
pub fn extract_numeric(value: &Value) -> Result<f64> {
    match value {
        Value::Int(i) => Ok(*i as f64),
        Value::Float(f) => Ok(*f),
        Value::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
        _ => Err(ArcaneError::TypeError {
            field: "value".to_string(),
            expected: "numeric".to_string(),
            got: value.type_name().to_string(),
        }),
    }
}
