use std::cmp::Ordering;
use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};

use crate::DiffConfig;
use crate::hash::{HASH_COL, with_row_hash};
use crate::input::{BatchIter, Table};
use crate::report::{
    Change, DiffCounts, DiffReport, KeyInfo, KeyVal, ModifiedRow, RowCounts, RowSample, Samples,
};
use crate::schema_diff::SchemaDiff;
use crate::sort::{Row, SortedSource, SpillDir, sort_into_runs};
use crate::value::{Cell, Comparator, cmp_keys, extract};

pub fn diff_streams(
    left: BatchIter,
    right: BatchIter,
    key_cols: Vec<String>,
    inferred: bool,
    schema: SchemaDiff,
    cfg: &DiffConfig,
    cmp: &Comparator,
) -> Result<DiffReport> {
    let lschema = left.schema.clone();
    let rschema = right.schema.clone();

    let budget = cfg.memory_mb.saturating_mul(1024 * 1024);
    let mut spill = SpillDir::new();
    let (lruns, lrows) = sort_into_runs(left, &key_cols, budget, &mut spill)?;
    let (rruns, rrows) = sort_into_runs(right, &key_cols, budget, &mut spill)?;
    let mut ls = SortedSource::new(lruns, &key_cols, cfg.left.display().to_string())?;
    let mut rs = SortedSource::new(rruns, &key_cols, cfg.right.display().to_string())?;

    // (column name, index in left projected schema, index in right)
    let value_cols: Vec<(String, usize, usize)> = schema
        .mutual
        .iter()
        .filter(|c| !key_cols.contains(c))
        .map(|c| Ok((c.clone(), lschema.index_of(c)?, rschema.index_of(c)?)))
        .collect::<Result<_>>()?;

    let mut counts = DiffCounts { added: 0, removed: 0, modified: 0 };
    let mut samples = Samples { added: vec![], removed: vec![], modified: vec![] };
    let mut columns_changed: BTreeMap<String, usize> = BTreeMap::new();
    let mut truncated = false;

    enum Act {
        Left,
        Right,
        Both,
    }

    loop {
        if cfg
            .fail_fast
            .is_some_and(|n| counts.added + counts.removed + counts.modified >= n)
        {
            truncated = true;
            break;
        }
        let act = match (ls.peek(), rs.peek()) {
            (None, None) => break,
            (Some(lrow), None) => {
                record_removed(lrow, &key_cols, cfg, &mut counts, &mut samples);
                Act::Left
            }
            (None, Some(rrow)) => {
                record_added(rrow, &key_cols, cfg, &mut counts, &mut samples);
                Act::Right
            }
            (Some(lrow), Some(rrow)) => match cmp_keys(&lrow.key, &rrow.key) {
                Ordering::Less => {
                    record_removed(lrow, &key_cols, cfg, &mut counts, &mut samples);
                    Act::Left
                }
                Ordering::Greater => {
                    record_added(rrow, &key_cols, cfg, &mut counts, &mut samples);
                    Act::Right
                }
                Ordering::Equal => {
                    let mut changes = vec![];
                    for (name, li, ri) in &value_cols {
                        let lv = extract(lrow.batch.column(*li).as_ref(), lrow.row)?;
                        let rv = extract(rrow.batch.column(*ri).as_ref(), rrow.row)?;
                        if !cmp.eq(&lv, &rv) {
                            *columns_changed.entry(name.clone()).or_insert(0) += 1;
                            changes.push(Change {
                                column: name.clone(),
                                left: lv.render_typed(),
                                right: rv.render_typed(),
                            });
                        }
                    }
                    if !changes.is_empty() {
                        counts.modified += 1;
                        if samples.modified.len() < cfg.max_samples {
                            samples.modified.push(ModifiedRow {
                                key: render_key(&key_cols, &lrow.key),
                                changes,
                            });
                        }
                    }
                    Act::Both
                }
            },
        };
        match act {
            Act::Left => ls.advance()?,
            Act::Right => rs.advance()?,
            Act::Both => {
                ls.advance()?;
                rs.advance()?;
            }
        }
    }

    Ok(DiffReport {
        schema,
        key: KeyInfo { columns: key_cols, inferred },
        keyless: false,
        rows: RowCounts { left: lrows, right: rrows },
        diff: counts,
        columns_changed,
        samples,
        truncated,
    })
}

/// Keyless diff: rows are matched by whole-row content hash and compared as
/// multisets, so duplicate rows are legal and edits appear as remove + add.
/// `auto` marks that this mode was a fallback after key inference failed.
pub fn diff_streams_keyless(
    left: BatchIter,
    right: BatchIter,
    auto: bool,
    schema: SchemaDiff,
    cfg: &DiffConfig,
) -> Result<DiffReport> {
    let mutual = schema.mutual.clone();
    let left = with_row_hash(left, &mutual)?;
    let right = with_row_hash(right, &mutual)?;
    let lschema = left.schema.clone();
    let rschema = right.schema.clone();
    let key_cols = vec![HASH_COL.to_string()];

    let budget = cfg.memory_mb.saturating_mul(1024 * 1024);
    let mut spill = SpillDir::new();
    let (lruns, lrows) = sort_into_runs(left, &key_cols, budget, &mut spill)?;
    let (rruns, rrows) = sort_into_runs(right, &key_cols, budget, &mut spill)?;
    let mut ls = SortedSource::new(lruns, &key_cols, cfg.left.display().to_string())?;
    let mut rs = SortedSource::new(rruns, &key_cols, cfg.right.display().to_string())?;

    let lcols = sample_columns(&mutual, &lschema)?;
    let rcols = sample_columns(&mutual, &rschema)?;

    let mut counts = DiffCounts { added: 0, removed: 0, modified: 0 };
    let mut samples = Samples { added: vec![], removed: vec![], modified: vec![] };
    let mut truncated = false;

    loop {
        if cfg
            .fail_fast
            .is_some_and(|n| counts.added + counts.removed >= n)
        {
            truncated = true;
            break;
        }
        enum Act {
            Removed(Vec<KeyVal>),
            Added(Vec<KeyVal>),
            Matched(Vec<KeyVal>, Vec<KeyVal>),
        }
        let act = match (ls.peek(), rs.peek()) {
            (None, None) => break,
            (Some(lrow), None) => Act::Removed(render_row(lrow, &lcols)?),
            (None, Some(rrow)) => Act::Added(render_row(rrow, &rcols)?),
            (Some(lrow), Some(rrow)) => match cmp_keys(&lrow.key, &rrow.key) {
                Ordering::Less => Act::Removed(render_row(lrow, &lcols)?),
                Ordering::Greater => Act::Added(render_row(rrow, &rcols)?),
                Ordering::Equal => {
                    Act::Matched(render_row(lrow, &lcols)?, render_row(rrow, &rcols)?)
                }
            },
        };
        match act {
            Act::Removed(row) => {
                let n = ls.advance_group()?;
                counts.removed += n;
                push_sample(&mut samples.removed, row, n, cfg.max_samples);
            }
            Act::Added(row) => {
                let n = rs.advance_group()?;
                counts.added += n;
                push_sample(&mut samples.added, row, n, cfg.max_samples);
            }
            Act::Matched(lrow, rrow) => {
                let nl = ls.advance_group()?;
                let nr = rs.advance_group()?;
                if nl > nr {
                    counts.removed += nl - nr;
                    push_sample(&mut samples.removed, lrow, nl - nr, cfg.max_samples);
                } else if nr > nl {
                    counts.added += nr - nl;
                    push_sample(&mut samples.added, rrow, nr - nl, cfg.max_samples);
                }
            }
        }
    }

    Ok(DiffReport {
        schema,
        key: KeyInfo { columns: vec![], inferred: auto },
        keyless: true,
        rows: RowCounts { left: lrows, right: rrows },
        diff: counts,
        columns_changed: BTreeMap::new(),
        samples,
        truncated,
    })
}

fn sample_columns(mutual: &[String], schema: &arrow::datatypes::SchemaRef) -> Result<Vec<(String, usize)>> {
    mutual
        .iter()
        .map(|c| Ok((c.clone(), schema.index_of(c)?)))
        .collect()
}

fn render_row(row: &Row, cols: &[(String, usize)]) -> Result<Vec<KeyVal>> {
    cols.iter()
        .map(|(name, i)| {
            Ok(KeyVal {
                column: name.clone(),
                value: extract(row.batch.column(*i).as_ref(), row.row)?.render(),
            })
        })
        .collect()
}

fn push_sample(dst: &mut Vec<RowSample>, row: Vec<KeyVal>, count: usize, max: usize) {
    if dst.len() < max {
        dst.push(RowSample { row, count });
    }
}

fn record_removed(
    row: &Row,
    key_cols: &[String],
    cfg: &DiffConfig,
    counts: &mut DiffCounts,
    samples: &mut Samples,
) {
    counts.removed += 1;
    if samples.removed.len() < cfg.max_samples {
        samples.removed.push(RowSample {
            row: render_key(key_cols, &row.key),
            count: 1,
        });
    }
}

fn record_added(
    row: &Row,
    key_cols: &[String],
    cfg: &DiffConfig,
    counts: &mut DiffCounts,
    samples: &mut Samples,
) {
    counts.added += 1;
    if samples.added.len() < cfg.max_samples {
        samples.added.push(RowSample {
            row: render_key(key_cols, &row.key),
            count: 1,
        });
    }
}

pub fn validate_key(key: &[String], schema: &SchemaDiff) -> Result<()> {
    for k in key {
        if !schema.mutual.contains(k) {
            bail!(
                "key column `{k}` is not present in both tables (shared columns: {})",
                schema.mutual.join(", ")
            );
        }
    }
    Ok(())
}

/// Infers a key from row samples of both sides. Uniqueness here is only a
/// sample-level heuristic; the merge verifies it over the full data.
pub fn infer_key(left: &Table, right: &Table, mutual: &[String]) -> Result<Vec<String>> {
    // Heuristic order: id-like names first, then remaining shared columns.
    let mut cands: Vec<&String> = vec![];
    for c in mutual {
        if c.eq_ignore_ascii_case("id") {
            cands.push(c);
        }
    }
    for c in mutual {
        let lc = c.to_ascii_lowercase();
        if (lc.ends_with("_id") || lc == "key" || lc == "pk" || lc == "uuid") && !cands.contains(&c)
        {
            cands.push(c);
        }
    }
    for c in mutual {
        if !cands.contains(&c) {
            cands.push(c);
        }
    }
    for c in cands {
        let cols = vec![c.clone()];
        if key_is_unique(left, &cols)? && key_is_unique(right, &cols)? {
            return Ok(cols);
        }
    }
    bail!("could not infer a key column that is unique and non-null on both sides; pass --key <column[,column]>")
}

fn key_is_unique(t: &Table, cols: &[String]) -> Result<bool> {
    let keys = extract_keys(t, cols)?;
    if keys
        .iter()
        .any(|k| k.iter().any(|c| matches!(c, Cell::Null)))
    {
        return Ok(false);
    }
    let mut idx: Vec<usize> = (0..keys.len()).collect();
    idx.sort_unstable_by(|&a, &b| cmp_keys(&keys[a], &keys[b]));
    Ok(!idx
        .windows(2)
        .any(|w| cmp_keys(&keys[w[0]], &keys[w[1]]) == Ordering::Equal))
}

fn extract_keys(t: &Table, cols: &[String]) -> Result<Vec<Vec<Cell>>> {
    let arrays = cols
        .iter()
        .map(|c| {
            t.batch
                .column_by_name(c)
                .cloned()
                .with_context(|| format!("column `{c}` missing"))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut keys = Vec::with_capacity(t.batch.num_rows());
    for row in 0..t.batch.num_rows() {
        let key = arrays
            .iter()
            .map(|a| extract(a.as_ref(), row))
            .collect::<Result<Vec<_>>>()?;
        keys.push(key);
    }
    Ok(keys)
}

fn render_key(cols: &[String], cells: &[Cell]) -> Vec<KeyVal> {
    cols.iter()
        .zip(cells)
        .map(|(c, v)| KeyVal {
            column: c.clone(),
            value: v.render(),
        })
        .collect()
}
