pub mod hash;
pub mod input;
pub mod rename;
pub mod report;
pub mod row_diff;
pub mod schema_diff;
pub mod sort;
pub mod value;

use std::path::PathBuf;

use anyhow::{Context, Result, bail};

use input::FileFormat;
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
    /// Match rows by whole-row content hash instead of a key.
    pub keyless: bool,
    /// Inputs are already sorted by the key: stream directly, skipping the
    /// sort phase entirely; order is verified on the fly.
    pub assume_sorted: bool,
    /// Where spill files go (defaults to the system temp dir).
    pub spill_dir: Option<PathBuf>,
    /// Force the input format for both sides instead of using file
    /// extensions (needed for extension-less temp files, e.g. from git).
    pub input_format: Option<FileFormat>,
}

pub fn run_diff(cfg: &DiffConfig) -> Result<DiffReport> {
    let lschema = input::probe_schema(&cfg.left, cfg.input_format)
        .with_context(|| format!("failed to read {}", cfg.left.display()))?;
    let rschema = input::probe_schema(&cfg.right, cfg.input_format)
        .with_context(|| format!("failed to read {}", cfg.right.display()))?;
    let mut schema = schema_diff::diff_schemas(&lschema, &rschema);
    if schema.mutual.is_empty() {
        bail!("tables share no columns; nothing to compare");
    }

    // Keyed with a resolved key, or keyless (auto = fell back from inference).
    enum Mode {
        Keyed(Vec<String>, bool),
        Keyless { auto: bool },
    }
    let has_tol = cfg.tol_abs.is_some() || cfg.tol_rel.is_some();
    let mode = if cfg.keyless {
        if cfg.key.is_some() {
            bail!("--keyless and --key are mutually exclusive");
        }
        if has_tol {
            bail!("float tolerances are not supported in keyless mode (rows are matched by exact content hash)");
        }
        if cfg.assume_sorted {
            bail!("--assume-sorted requires a key (keyless mode orders rows by content hash, not file order)");
        }
        Mode::Keyless { auto: false }
    } else if let Some(k) = &cfg.key {
        row_diff::validate_key(k, &schema)?;
        Mode::Keyed(k.clone(), false)
    } else {
        let lsample = input::read_sample(
            &cfg.left,
            &lschema,
            &schema.mutual,
            KEY_INFER_SAMPLE_ROWS,
            cfg.input_format,
        )?;
        let rsample = input::read_sample(
            &cfg.right,
            &rschema,
            &schema.mutual,
            KEY_INFER_SAMPLE_ROWS,
            cfg.input_format,
        )?;
        if cfg.assume_sorted {
            bail!("--assume-sorted requires an explicit --key");
        }
        match row_diff::infer_key(&lsample, &rsample, &schema.mutual) {
            Ok(k) => Mode::Keyed(k, true),
            // Without tolerances, keyless is a safe drop-in; with them the
            // user must decide, so surface the inference failure instead.
            Err(_) if !has_tol => Mode::Keyless { auto: true },
            Err(e) => {
                return Err(e.context(
                    "key inference failed and keyless mode cannot honor float tolerances; \
                     pass --key or drop --tol-abs/--tol-rel",
                ));
            }
        }
    };

    match &mode {
        Mode::Keyed(k, _) => {
            rename::detect_renames(cfg, &lschema, &rschema, &mut schema, Some(k))?
        }
        Mode::Keyless { .. } => {
            rename::detect_renames(cfg, &lschema, &rschema, &mut schema, None)?
        }
    }
    // Renamed columns ride along under their side-local names.
    let mut lproj = schema.mutual.clone();
    let mut rproj = schema.mutual.clone();
    for r in &schema.renamed {
        lproj.push(r.left.clone());
        rproj.push(r.right.clone());
    }

    let left = input::open_batches(&cfg.left, &lschema, &lproj, cfg.input_format)?;
    let right = input::open_batches(&cfg.right, &rschema, &rproj, cfg.input_format)?;
    match mode {
        Mode::Keyed(key_cols, inferred) => {
            let cmp = Comparator::new(cfg.tol_abs, cfg.tol_rel);
            row_diff::diff_streams(left, right, key_cols, inferred, schema, cfg, &cmp)
        }
        Mode::Keyless { auto } => row_diff::diff_streams_keyless(left, right, auto, schema, cfg),
    }
}
