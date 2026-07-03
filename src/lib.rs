pub mod input;
pub mod report;
pub mod row_diff;
pub mod schema_diff;
pub mod value;

use std::path::PathBuf;

use anyhow::{Result, bail};

use report::DiffReport;
use value::Comparator;

pub struct DiffConfig {
    pub left: PathBuf,
    pub right: PathBuf,
    /// Key column(s) used to match rows; inferred when None.
    pub key: Option<Vec<String>>,
    pub tol_abs: Option<f64>,
    pub tol_rel: Option<f64>,
    /// Stop scanning after this many row differences.
    pub fail_fast: Option<usize>,
    /// Max example rows kept per category in the report.
    pub max_samples: usize,
}

pub fn run_diff(cfg: &DiffConfig) -> Result<DiffReport> {
    let left = input::read_table(&cfg.left)?;
    let right = input::read_table(&cfg.right)?;
    let schema = schema_diff::diff_schemas(&left.schema, &right.schema);
    if schema.mutual.is_empty() {
        bail!("tables share no columns; nothing to compare");
    }
    let cmp = Comparator::new(cfg.tol_abs, cfg.tol_rel);
    row_diff::diff_rows(&left, &right, schema, cfg, &cmp)
}
