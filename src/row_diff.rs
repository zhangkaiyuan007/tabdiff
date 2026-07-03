use std::cmp::Ordering;
use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};

use crate::DiffConfig;
use crate::input::{BatchIter, Table};
use crate::report::{
    Change, DiffCounts, DiffReport, KeyInfo, KeyVal, ModifiedRow, RowCounts, Samples,
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
        rows: RowCounts { left: lrows, right: rrows },
        diff: counts,
        columns_changed,
        samples,
        truncated,
    })
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
        samples.removed.push(render_key(key_cols, &row.key));
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
        samples.added.push(render_key(key_cols, &row.key));
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
