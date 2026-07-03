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
        memory_mb: 256,
        keyless: false,
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

/// Generates ~60k-row tables and diffs them with a zero memory budget so
/// every batch spills to disk, exercising the external-sort merge path.
#[test]
fn spilling_produces_same_results_as_in_memory() {
    let dir = std::env::temp_dir();
    let lp = dir.join(format!("tabdiff-big-l-{}.csv", std::process::id()));
    let rp = dir.join(format!("tabdiff-big-r-{}.csv", std::process::id()));

    let mut l = String::from("id,val,tag\n");
    let mut r = String::from("id,val,tag\n");
    for id in 0..60_000u64 {
        l.push_str(&format!("{id},{},a\n", id * 2));
        if (100..110).contains(&id) {
            continue; // 10 rows removed on the right
        }
        let val = if id % 1000 == 0 { id * 2 + 1 } else { id * 2 }; // 60 modified
        r.push_str(&format!("{id},{val},a\n"));
    }
    for id in 60_000..60_005u64 {
        r.push_str(&format!("{id},{},a\n", id * 2)); // 5 rows added
    }
    std::fs::write(&lp, l).unwrap();
    std::fs::write(&rp, r).unwrap();

    let mut expected = None;
    for memory_mb in [0, 256] {
        let c = DiffConfig {
            left: lp.clone(),
            right: rp.clone(),
            key: Some(vec!["id".into()]),
            tol_abs: None,
            tol_rel: None,
            fail_fast: None,
            max_samples: 3,
            memory_mb,
            keyless: false,
        };
        let report = run_diff(&c).unwrap();
        assert_eq!(report.diff.added, 5, "memory_mb={memory_mb}");
        assert_eq!(report.diff.removed, 10, "memory_mb={memory_mb}");
        assert_eq!(report.diff.modified, 60, "memory_mb={memory_mb}");
        assert_eq!(report.rows.left, 60_000);
        assert_eq!(report.rows.right, 59_995);
        let counts = (report.diff.added, report.diff.removed, report.diff.modified);
        assert_eq!(*expected.get_or_insert(counts), counts);
    }
    std::fs::remove_file(&lp).ok();
    std::fs::remove_file(&rp).ok();
}

// events_*.csv have no unique column: `10:00,A,21.5` appears twice on the
// left. Right side: one duplicate dropped, 21.6 edited to 21.7, one row added.
#[test]
fn keyless_multiset_diff() {
    let mut c = cfg("events_left.csv", "events_right.csv");
    c.keyless = true;
    let report = run_diff(&c).unwrap();
    assert!(report.keyless);
    assert!(!report.key.inferred);
    assert_eq!(report.diff.added, 2); // edited row's new version + the new row
    assert_eq!(report.diff.removed, 2); // dropped duplicate + edited row's old version
    assert_eq!(report.diff.modified, 0);
}

#[test]
fn keyless_fallback_when_no_unique_key() {
    let report = run_diff(&cfg("events_left.csv", "events_right.csv")).unwrap();
    assert!(report.keyless, "should fall back to keyless automatically");
    assert!(report.key.inferred, "fallback should be marked as automatic");
    assert_eq!(report.diff.added, 2);
    assert_eq!(report.diff.removed, 2);
}

#[test]
fn keyless_identical_files_with_duplicates() {
    let mut c = cfg("events_left.csv", "events_left.csv");
    c.keyless = true;
    let report = run_diff(&c).unwrap();
    assert!(!report.has_differences());
}

#[test]
fn keyless_rejects_tolerances() {
    let mut c = cfg("events_left.csv", "events_right.csv");
    c.keyless = true;
    c.tol_abs = Some(0.1);
    let err = run_diff(&c).unwrap_err().to_string();
    assert!(err.contains("not supported"), "unexpected error: {err}");
}

#[test]
fn explicit_composite_key() {
    let mut c = cfg("left.csv", "right.csv");
    c.key = Some(vec!["id".into(), "name".into()]);
    let report = run_diff(&c).unwrap();
    assert!(!report.key.inferred);
    assert_eq!(report.diff.modified, 2);
}
