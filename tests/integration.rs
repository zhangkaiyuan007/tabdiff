use std::path::{Path, PathBuf};

use tabdiff::{DiffConfig, run_diff};

fn data(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join(name)
}

fn cfg(left: &str, right: &str) -> DiffConfig {
    DiffConfig {
        left: data(left),
        right: data(right),
        key: None,
        tol_abs: None,
        tol_rel: None,
        fail_fast: None,
        max_samples: 10,
    }
}

#[test]
fn csv_basic_diff() {
    let report = run_diff(&cfg("left.csv", "right.csv")).unwrap();
    assert_eq!(report.key.columns, vec!["id"]);
    assert!(report.key.inferred);
    assert_eq!(report.diff.added, 1);
    assert_eq!(report.diff.removed, 1);
    assert_eq!(report.diff.modified, 2);
    assert_eq!(report.schema.added.len(), 1); // email
    assert_eq!(report.schema.removed.len(), 1); // legacy
    assert_eq!(report.columns_changed.get("amount"), Some(&1));
    assert_eq!(report.columns_changed.get("status"), Some(&1));
    assert!(report.has_differences());
}

#[test]
fn float_tolerance_suppresses_noise() {
    let mut c = cfg("left.csv", "right.csv");
    c.tol_abs = Some(0.001);
    let report = run_diff(&c).unwrap();
    assert_eq!(report.diff.modified, 1); // only the status change remains
    assert_eq!(report.columns_changed.get("amount"), None);
}

#[test]
fn csv_vs_parquet_mixed() {
    let table = tabdiff::input::read_table(&data("right.csv")).unwrap();
    let path = std::env::temp_dir().join(format!("tabdiff-test-{}.parquet", std::process::id()));
    let file = std::fs::File::create(&path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(file, table.schema.clone(), None).unwrap();
    w.write(&table.batch).unwrap();
    w.close().unwrap();

    let mut c = cfg("left.csv", "right.csv");
    c.right = path.clone();
    let report = run_diff(&c);
    std::fs::remove_file(&path).ok();
    let report = report.unwrap();
    assert_eq!(report.diff.added, 1);
    assert_eq!(report.diff.removed, 1);
    assert_eq!(report.diff.modified, 2);
}

#[test]
fn duplicate_key_is_an_error() {
    let mut c = cfg("dup.csv", "dup.csv");
    c.key = Some(vec!["id".into()]);
    let err = run_diff(&c).unwrap_err().to_string();
    assert!(err.contains("not unique"), "unexpected error: {err}");
}

#[test]
fn identical_tables_have_no_differences() {
    let report = run_diff(&cfg("left.csv", "left.csv")).unwrap();
    assert!(!report.has_differences());
}

#[test]
fn fail_fast_truncates_scan() {
    let mut c = cfg("left.csv", "right.csv");
    c.fail_fast = Some(1);
    let report = run_diff(&c).unwrap();
    assert!(report.truncated);
}

#[test]
fn explicit_composite_key() {
    let mut c = cfg("left.csv", "right.csv");
    c.key = Some(vec!["id".into(), "name".into()]);
    let report = run_diff(&c).unwrap();
    assert!(!report.key.inferred);
    assert_eq!(report.diff.modified, 2);
}
