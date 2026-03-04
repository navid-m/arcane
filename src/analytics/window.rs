//! Window functions for analytics

use crate::error::Result;

/// Window function types
#[derive(Debug, Clone)]
pub enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    Lag(usize),
    Lead(usize),
    FirstValue,
    LastValue,
    NthValue(usize),
}

/// Window type for partitioning
#[derive(Debug, Clone)]
pub enum WindowType {
    Rows(usize),
    Range(f64),
    Unbounded,
}

/// Window function processor
pub struct WindowProcessor {
    function: WindowFunction,
}

impl WindowProcessor {
    pub fn new(function: WindowFunction) -> Self {
        WindowProcessor { function }
    }

    /// Apply window function to values
    pub fn apply(&self, values: &[f64]) -> Result<Vec<f64>> {
        match &self.function {
            WindowFunction::RowNumber => Ok(self.row_number(values)),
            WindowFunction::Rank => Ok(self.rank(values)),
            WindowFunction::DenseRank => Ok(self.dense_rank(values)),
            WindowFunction::PercentRank => Ok(self.percent_rank(values)),
            WindowFunction::Lag(n) => Ok(self.lag(values, *n)),
            WindowFunction::Lead(n) => Ok(self.lead(values, *n)),
            WindowFunction::FirstValue => Ok(self.first_value(values)),
            WindowFunction::LastValue => Ok(self.last_value(values)),
            WindowFunction::NthValue(n) => Ok(self.nth_value(values, *n)),
        }
    }

    fn row_number(&self, values: &[f64]) -> Vec<f64> {
        (1..=values.len()).map(|i| i as f64).collect()
    }

    fn rank(&self, values: &[f64]) -> Vec<f64> {
        let mut indexed: Vec<(usize, f64)> =
            values.iter().enumerate().map(|(i, &v)| (i, v)).collect();

        indexed.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        let mut ranks = vec![0.0; values.len()];
        let mut current_rank = 1;

        for (i, (idx, val)) in indexed.iter().enumerate() {
            if i > 0 && indexed[i - 1].1 != *val {
                current_rank = i + 1;
            }
            ranks[*idx] = current_rank as f64;
        }

        ranks
    }

    fn dense_rank(&self, values: &[f64]) -> Vec<f64> {
        let mut indexed: Vec<(usize, f64)> =
            values.iter().enumerate().map(|(i, &v)| (i, v)).collect();

        indexed.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        let mut ranks = vec![0.0; values.len()];
        let mut current_rank = 1;

        for (i, (idx, val)) in indexed.iter().enumerate() {
            if i > 0 && indexed[i - 1].1 != *val {
                current_rank += 1;
            }
            ranks[*idx] = current_rank as f64;
        }

        ranks
    }

    fn percent_rank(&self, values: &[f64]) -> Vec<f64> {
        if values.len() <= 1 {
            return vec![0.0; values.len()];
        }

        let ranks = self.rank(values);
        let n = values.len() as f64;

        ranks.iter().map(|&r| (r - 1.0) / (n - 1.0)).collect()
    }

    fn lag(&self, values: &[f64], n: usize) -> Vec<f64> {
        let mut result = Vec::with_capacity(values.len());

        for i in 0..values.len() {
            if i < n {
                result.push(f64::NAN);
            } else {
                result.push(values[i - n]);
            }
        }

        result
    }

    fn lead(&self, values: &[f64], n: usize) -> Vec<f64> {
        let mut result = Vec::with_capacity(values.len());

        for i in 0..values.len() {
            if i + n >= values.len() {
                result.push(f64::NAN);
            } else {
                result.push(values[i + n]);
            }
        }

        result
    }

    fn first_value(&self, values: &[f64]) -> Vec<f64> {
        if values.is_empty() {
            return vec![];
        }
        vec![values[0]; values.len()]
    }

    fn last_value(&self, values: &[f64]) -> Vec<f64> {
        if values.is_empty() {
            return vec![];
        }
        let last = values[values.len() - 1];
        vec![last; values.len()]
    }

    fn nth_value(&self, values: &[f64], n: usize) -> Vec<f64> {
        if n >= values.len() {
            return vec![f64::NAN; values.len()];
        }
        vec![values[n]; values.len()]
    }
}
