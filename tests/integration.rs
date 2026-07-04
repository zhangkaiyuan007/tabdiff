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
        assume_sorted: false,
        spill_dir: None,
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
            assume_sorted: false,
            spill_dir: None,
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
fn assume_sorted_matches_normal_path() {
    // left.csv / right.csv are written sorted by id.
    let mut c = cfg("left.csv", "right.csv");
    c.key = Some(vec!["id".into()]);
    c.assume_sorted = true;
    let report = run_diff(&c).unwrap();
    assert_eq!(report.diff.added, 1);
    assert_eq!(report.diff.removed, 1);
    assert_eq!(report.diff.modified, 2);
    assert_eq!(report.rows.left, 5);
    assert_eq!(report.rows.right, 5);
}

#[test]
fn assume_sorted_rejects_unsorted_input() {
    let path = std::env::temp_dir().join(format!("tabdiff-unsorted-{}.csv", std::process::id()));
    std::fs::write(&path, "id,v\n2,a\n1,b\n").unwrap();
    let mut c = cfg("left.csv", "right.csv");
    c.left = path.clone();
    c.right = path.clone();
    c.key = Some(vec!["id".into()]);
    c.assume_sorted = true;
    let err = run_diff(&c).unwrap_err().to_string();
    std::fs::remove_file(&path).ok();
    assert!(err.contains("not sorted"), "unexpected error: {err}");
}

/// An Int64 key on one side and a Float64 key on the other must still match
/// (unified encoding), mirroring data-diff-era cross-type key complaints.
#[test]
fn cross_type_keys_unify() {
    let table = tabdiff::input::read_table(&data("right.csv")).unwrap();
    let id_idx = table.schema.index_of("id").unwrap();
    let mut columns = table.batch.columns().to_vec();
    columns[id_idx] =
        arrow::compute::cast(&columns[id_idx], &arrow::datatypes::DataType::Float64).unwrap();
    let mut fields: Vec<arrow::datatypes::Field> = table
        .schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields[id_idx] = fields[id_idx]
        .clone()
        .with_data_type(arrow::datatypes::DataType::Float64);
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));
    let batch = arrow::array::RecordBatch::try_new(schema.clone(), columns).unwrap();

    let path = std::env::temp_dir().join(format!("tabdiff-f64key-{}.parquet", std::process::id()));
    let file = std::fs::File::create(&path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();

    let mut c = cfg("left.csv", "right.csv");
    c.right = path.clone();
    c.key = Some(vec!["id".into()]);
    let report = run_diff(&c);
    std::fs::remove_file(&path).ok();
    let report = report.unwrap();
    // Same result as the plain csv-vs-csv diff: keys still line up.
    assert_eq!(report.diff.added, 1);
    assert_eq!(report.diff.removed, 1);
    assert_eq!(report.diff.modified, 2);
    assert_eq!(report.schema.type_changed.len(), 1); // id: Int64 -> Float64
}

/// rename_left/right: `amount` renamed to `amt` with identical values except
/// one edited row (id=7). Keyed detection must pair the columns and keep the
/// column in the row diff under the display name `amount→amt`.
#[test]
fn keyed_rename_detection() {
    let report = run_diff(&cfg("rename_left.csv", "rename_right.csv")).unwrap();
    assert_eq!(report.schema.renamed.len(), 1);
    assert_eq!(report.schema.renamed[0].left, "amount");
    assert_eq!(report.schema.renamed[0].right, "amt");
    assert!(report.schema.added.is_empty());
    assert!(report.schema.removed.is_empty());
    assert_eq!(report.diff.modified, 1); // the id=7 edit
    assert_eq!(report.columns_changed.get("amount→amt"), Some(&1));
}

#[test]
fn keyless_rename_detection_with_high_cardinality() {
    let dir = std::env::temp_dir();
    let lp = dir.join(format!("tabdiff-ren-l-{}.csv", std::process::id()));
    let rp = dir.join(format!("tabdiff-ren-r-{}.csv", std::process::id()));
    // `code` renamed to `token`, 30 distinct values, no unique key (dup rows).
    let mut l = String::from("code,grp\n");
    let mut r = String::from("token,grp\n");
    for i in 0..30 {
        for _ in 0..2 {
            l.push_str(&format!("c{i},g\n"));
            r.push_str(&format!("c{i},g\n"));
        }
    }
    std::fs::write(&lp, &l).unwrap();
    std::fs::write(&rp, &r).unwrap();
    let mut c = cfg("left.csv", "right.csv");
    c.left = lp.clone();
    c.right = rp.clone();
    c.keyless = true;
    let report = run_diff(&c).unwrap();
    std::fs::remove_file(&lp).ok();
    std::fs::remove_file(&rp).ok();
    assert_eq!(report.schema.renamed.len(), 1);
    assert_eq!(report.schema.renamed[0].left, "code");
    assert_eq!(report.schema.renamed[0].right, "token");
    // Renamed column hashes at an aligned position: rows still match.
    assert_eq!(report.diff.added, 0);
    assert_eq!(report.diff.removed, 0);
}

/// Low-cardinality columns must never be claimed as renames in keyless mode.
#[test]
fn keyless_rename_guard_low_cardinality() {
    let dir = std::env::temp_dir();
    let lp = dir.join(format!("tabdiff-grd-l-{}.csv", std::process::id()));
    let rp = dir.join(format!("tabdiff-grd-r-{}.csv", std::process::id()));
    let mut l = String::from("flag,v\n");
    let mut r = String::from("active,v\n");
    for i in 0..40 {
        l.push_str(&format!("{},x{}\n", i % 2 == 0, i));
        r.push_str(&format!("{},x{}\n", i % 2 == 0, i));
    }
    std::fs::write(&lp, &l).unwrap();
    std::fs::write(&rp, &r).unwrap();
    let mut c = cfg("left.csv", "right.csv");
    c.left = lp.clone();
    c.right = rp.clone();
    c.keyless = true;
    let report = run_diff(&c).unwrap();
    std::fs::remove_file(&lp).ok();
    std::fs::remove_file(&rp).ok();
    assert!(report.schema.renamed.is_empty(), "boolean columns must not pair");
    assert_eq!(report.schema.added.len(), 1);
    assert_eq!(report.schema.removed.len(), 1);
}

#[test]
fn explicit_composite_key() {
    let mut c = cfg("left.csv", "right.csv");
    c.key = Some(vec!["id".into(), "name".into()]);
    let report = run_diff(&c).unwrap();
    assert!(!report.key.inferred);
    assert_eq!(report.diff.modified, 2);
}
