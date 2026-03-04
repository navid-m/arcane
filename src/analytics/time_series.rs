//! Time-series analysis and forecasting

use crate::analytics::TimeSeriesResult;
use crate::error::{ArcaneError, Result};

/// Time-series analyzer with trend detection and forecasting
pub struct TimeSeriesAnalyzer {
    history: Vec<f64>,
}

impl TimeSeriesAnalyzer {
    pub fn new() -> Self {
        TimeSeriesAnalyzer {
            history: Vec::new(),
        }
    }

    /// Analyze time-series data
    pub fn analyze(&mut self, values: Vec<f64>) -> Result<TimeSeriesResult> {
        if values.is_empty() {
            return Err(ArcaneError::Other("No values to analyze".to_string()));
        }

        self.history.extend(values.iter());

        let trend = self.detect_trend(&values);
        let seasonality = self.detect_seasonality(&values);
        let forecast = self.forecast(&values, 5);
        let anomalies = self.detect_anomalies(&values);
        let correlation = self.compute_autocorrelation(&values, 1);

        Ok(TimeSeriesResult {
            trend,
            seasonality,
            forecast,
            anomalies,
            correlation,
        })
    }

    /// Detect trend using linear regression
    fn detect_trend(&self, values: &[f64]) -> Trend {
        if values.len() < 2 {
            return Trend::Stable;
        }

        let n = values.len() as f64;
        let x_mean = (n - 1.0) / 2.0;
        let y_mean = values.iter().sum::<f64>() / n;
        let mut numerator = 0.0;
        let mut denominator = 0.0;

        for (i, &y) in values.iter().enumerate() {
            let x = i as f64;
            numerator += (x - x_mean) * (y - y_mean);
            denominator += (x - x_mean).powi(2);
        }

        let slope = if denominator != 0.0 {
            numerator / denominator
        } else {
            0.0
        };

        if slope > 0.01 {
            Trend::Increasing
        } else if slope < -0.01 {
            Trend::Decreasing
        } else {
            Trend::Stable
        }
    }

    /// Detect seasonality using autocorrelation
    fn detect_seasonality(&self, values: &[f64]) -> Option<f64> {
        if values.len() < 4 {
            return None;
        }

        let max_lag = (values.len() / 2).min(12);
        let mut max_corr = 0.0;
        let mut best_lag = 0;

        for lag in 2..=max_lag {
            let corr = self.compute_autocorrelation(values, lag).abs();
            if corr > max_corr {
                max_corr = corr;
                best_lag = lag;
            }
        }

        if max_corr > 0.5 {
            Some(best_lag as f64)
        } else {
            None
        }
    }

    /// Simple exponential smoothing forecast
    fn forecast(&self, values: &[f64], periods: usize) -> Vec<f64> {
        if values.is_empty() {
            return vec![];
        }

        let alpha = 0.3;
        let mut forecast = Vec::with_capacity(periods);
        let mut last = values[values.len() - 1];
        let trend = if values.len() > 1 {
            (values[values.len() - 1] - values[0]) / (values.len() - 1) as f64
        } else {
            0.0
        };

        for _i in 0..periods {
            last = alpha * last + (1.0 - alpha) * (last + trend);
            forecast.push(last);
        }

        forecast
    }

    /// Detect anomalies using z-score
    fn detect_anomalies(&self, values: &[f64]) -> Vec<usize> {
        if values.len() < 3 {
            return vec![];
        }

        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
        let stddev = variance.sqrt();
        let threshold = 2.5;

        values
            .iter()
            .enumerate()
            .filter_map(|(i, &v)| {
                let z_score = ((v - mean) / stddev).abs();
                if z_score > threshold {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Compute autocorrelation at given lag
    fn compute_autocorrelation(&self, values: &[f64], lag: usize) -> f64 {
        if values.len() <= lag {
            return 0.0;
        }

        let n = values.len() - lag;
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let mut numerator = 0.0;
        let mut denominator = 0.0;

        for i in 0..n {
            numerator += (values[i] - mean) * (values[i + lag] - mean);
        }

        for &v in values {
            denominator += (v - mean).powi(2);
        }

        if denominator != 0.0 {
            numerator / denominator
        } else {
            0.0
        }
    }
}

impl Default for TimeSeriesAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

/// Trend direction
#[derive(Debug, Clone, PartialEq)]
pub enum Trend {
    Increasing,
    Decreasing,
    Stable,
}
