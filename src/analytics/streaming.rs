//! Streaming analytics for real-time data processing

use crate::analytics::{extract_numeric, StreamMetrics};
use crate::error::{ArcaneError, Result};
use crate::storage::{Record, Schema};
use std::collections::VecDeque;

/// Stream processor with sliding window
pub struct StreamProcessor {
    window_size: usize,
    records: VecDeque<Record>,
    schemas: VecDeque<Schema>,
}

impl StreamProcessor {
    pub fn new(window_size: usize) -> Self {
        StreamProcessor {
            window_size,
            records: VecDeque::with_capacity(window_size),
            schemas: VecDeque::with_capacity(window_size),
        }
    }

    /// Add a record to the stream
    pub fn add_record(&mut self, record: Record, schema: &Schema) -> Result<()> {
        if self.records.len() >= self.window_size {
            self.records.pop_front();
            self.schemas.pop_front();
        }
        self.records.push_back(record);
        self.schemas.push_back(schema.clone());
        Ok(())
    }

    /// Compute real-time metrics for a field
    pub fn compute_metrics(&self, field: &str) -> Result<StreamMetrics> {
        if self.records.is_empty() {
            return Err(ArcaneError::Other("No records in stream".to_string()));
        }

        let mut values = Vec::new();

        for (record, schema) in self.records.iter().zip(self.schemas.iter()) {
            if let Some(idx) = schema.field_index(field) {
                if let Some(value) = record.fields.get(idx) {
                    values.push(extract_numeric(value)?);
                }
            }
        }

        if values.is_empty() {
            return Err(ArcaneError::Other(format!(
                "No numeric values for field '{}'",
                field
            )));
        }

        let count = values.len();
        let sum: f64 = values.iter().sum();
        let mean = sum / count as f64;
        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / count as f64;
        let stddev = variance.sqrt();
        let ma_window = (count / 2).max(1);
        let moving_avg = values.iter().rev().take(ma_window).sum::<f64>() / ma_window as f64;
        let rate_of_change = if count > 1 {
            let first = values[0];
            let last = values[count - 1];
            if first != 0.0 {
                ((last - first) / first) * 100.0
            } else {
                0.0
            }
        } else {
            0.0
        };

        Ok(StreamMetrics {
            count,
            sum,
            mean,
            min,
            max,
            stddev,
            variance,
            moving_avg,
            rate_of_change,
        })
    }

    /// Get current window size
    pub fn window_size(&self) -> usize {
        self.records.len()
    }
}

/// Sliding window for stream processing
#[derive(Debug, Clone)]
pub struct StreamWindow {
    pub size: usize,
    pub slide: usize,
}

impl StreamWindow {
    pub fn new(size: usize, slide: usize) -> Self {
        StreamWindow { size, slide }
    }

    pub fn tumbling(size: usize) -> Self {
        StreamWindow { size, slide: size }
    }

    pub fn sliding(size: usize, slide: usize) -> Self {
        StreamWindow { size, slide }
    }
}
