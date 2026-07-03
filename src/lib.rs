pub mod input;
pub mod report;
pub mod row_diff;
pub mod schema_diff;
pub mod sort;
pub mod value;

use std::path::PathBuf;

use anyhow::{Context, Result, bail};

use report::DiffReport;
use value::Comparator;

/// Rows sampled from each side when inferring a key column.
const KEY_INFER_SAMPLE_ROWS: usize = 100_000;

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
    /// Sort-buffer budget before spilling to disk.
    pub memory_mb: usize,
}

pub fn run_diff(cfg: &DiffConfig) -> Result<DiffReport> {
    let lschema = input::probe_schema(&cfg.left)
        .with_context(|| format!("failed to read {}", cfg.left.display()))?;
    let rschema = input::probe_schema(&cfg.right)
        .with_context(|| format!("failed to read {}", cfg.right.display()))?;
    let schema = schema_diff::diff_schemas(&lschema, &rschema);
    if schema.mutual.is_empty() {
        bail!("tables share no columns; nothing to compare");
    }

    let (key_cols, inferred) = match &cfg.key {
        Some(k) => {
            row_diff::validate_key(k, &schema)?;
            (k.clone(), false)
        }
        None => {
            let lsample =
                input::read_sample(&cfg.left, &lschema, &schema.mutual, KEY_INFER_SAMPLE_ROWS)?;
            let rsample =
                input::read_sample(&cfg.right, &rschema, &schema.mutual, KEY_INFER_SAMPLE_ROWS)?;
            (row_diff::infer_key(&lsample, &rsample, &schema.mutual)?, true)
        }
    };

    let cmp = Comparator::new(cfg.tol_abs, cfg.tol_rel);
    let left = input::open_batches(&cfg.left, &lschema, &schema.mutual)?;
    let right = input::open_batches(&cfg.right, &rschema, &schema.mutual)?;
    row_diff::diff_streams(left, right, key_cols, inferred, schema, cfg, &cmp)
}
