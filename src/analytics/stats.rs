//! Statistical analysis functions

use crate::error::{ArcaneError, Result};

/// Statistical analyzer for data distributions
pub struct StatisticalAnalyzer;

impl StatisticalAnalyzer {
    /// Compute percentile
    pub fn percentile(values: &[f64], p: f64) -> Result<f64> {
        if values.is_empty() {
            return Err(ArcaneError::Other("No values provided".to_string()));
        }
        if !(0.0..=100.0).contains(&p) {
            return Err(ArcaneError::Other(
                "Percentile must be between 0 and 100".to_string(),
            ));
        }

        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let index = (p / 100.0) * (sorted.len() - 1) as f64;
        let lower = index.floor() as usize;
        let upper = index.ceil() as usize;
        let fraction = index - lower as f64;

        Ok(sorted[lower] * (1.0 - fraction) + sorted[upper] * fraction)
    }

    /// Compute median
    pub fn median(values: &[f64]) -> Result<f64> {
        Self::percentile(values, 50.0)
    }

    /// Compute mode
    pub fn mode(values: &[f64]) -> Result<f64> {
        if values.is_empty() {
            return Err(ArcaneError::Other("No values provided".to_string()));
        }

        let mut counts = std::collections::HashMap::new();
        for &v in values {
            *counts.entry(v.to_bits()).or_insert(0) += 1;
        }

        let max_count = counts.values().max().unwrap();
        let mode_bits = counts
            .iter()
            .find(|(_, &count)| count == *max_count)
            .map(|(&bits, _)| bits)
            .unwrap();

        Ok(f64::from_bits(mode_bits))
    }

    /// Compute skewness
    pub fn skewness(values: &[f64]) -> Result<f64> {
        if values.len() < 3 {
            return Err(ArcaneError::Other(
                "Need at least 3 values for skewness".to_string(),
            ));
        }

        let n = values.len() as f64;
        let mean = values.iter().sum::<f64>() / n;
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
        let stddev = variance.sqrt();

        if stddev == 0.0 {
            return Ok(0.0);
        }

        let skew = values
            .iter()
            .map(|v| ((v - mean) / stddev).powi(3))
            .sum::<f64>()
            / n;

        Ok(skew)
    }

    /// Compute kurtosis
    pub fn kurtosis(values: &[f64]) -> Result<f64> {
        if values.len() < 4 {
            return Err(ArcaneError::Other(
                "Need at least 4 values for kurtosis".to_string(),
            ));
        }

        let n = values.len() as f64;
        let mean = values.iter().sum::<f64>() / n;
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
        let stddev = variance.sqrt();

        if stddev == 0.0 {
            return Ok(0.0);
        }

        let kurt = values
            .iter()
            .map(|v| ((v - mean) / stddev).powi(4))
            .sum::<f64>()
            / n;

        Ok(kurt - 3.0)
    }

    /// Compute covariance between two datasets
    pub fn covariance(x: &[f64], y: &[f64]) -> Result<f64> {
        if x.len() != y.len() {
            return Err(ArcaneError::Other(
                "Datasets must have same length".to_string(),
            ));
        }
        if x.is_empty() {
            return Err(ArcaneError::Other("No values provided".to_string()));
        }

        let n = x.len() as f64;
        let mean_x = x.iter().sum::<f64>() / n;
        let mean_y = y.iter().sum::<f64>() / n;

        let cov = x
            .iter()
            .zip(y.iter())
            .map(|(&xi, &yi)| (xi - mean_x) * (yi - mean_y))
            .sum::<f64>()
            / n;

        Ok(cov)
    }

    /// Compute Pearson correlation coefficient
    pub fn correlation(x: &[f64], y: &[f64]) -> Result<f64> {
        if x.len() != y.len() {
            return Err(ArcaneError::Other(
                "Datasets must have same length".to_string(),
            ));
        }
        if x.is_empty() {
            return Err(ArcaneError::Other("No values provided".to_string()));
        }

        let n = x.len() as f64;
        let mean_x = x.iter().sum::<f64>() / n;
        let mean_y = y.iter().sum::<f64>() / n;
        let var_x = x.iter().map(|&v| (v - mean_x).powi(2)).sum::<f64>();
        let var_y = y.iter().map(|&v| (v - mean_y).powi(2)).sum::<f64>();

        if var_x == 0.0 || var_y == 0.0 {
            return Ok(0.0);
        }

        let cov = x
            .iter()
            .zip(y.iter())
            .map(|(&xi, &yi)| (xi - mean_x) * (yi - mean_y))
            .sum::<f64>();

        Ok(cov / (var_x * var_y).sqrt())
    }

    /// Detect distribution type
    pub fn detect_distribution(values: &[f64]) -> Result<Distribution> {
        if values.len() < 10 {
            return Ok(Distribution::Unknown);
        }

        let skew = Self::skewness(values)?;
        let kurt = Self::kurtosis(values)?;

        // Simple heuristics
        if skew.abs() < 0.5 && kurt.abs() < 0.5 {
            Ok(Distribution::Normal)
        } else if skew > 1.0 {
            Ok(Distribution::RightSkewed)
        } else if skew < -1.0 {
            Ok(Distribution::LeftSkewed)
        } else if kurt > 1.0 {
            Ok(Distribution::Leptokurtic)
        } else if kurt < -1.0 {
            Ok(Distribution::Platykurtic)
        } else {
            Ok(Distribution::Unknown)
        }
    }
}

/// Distribution types
#[derive(Debug, Clone, PartialEq)]
pub enum Distribution {
    Normal,
    RightSkewed,
    LeftSkewed,
    Leptokurtic,
    Platykurtic,
    Unknown,
}
