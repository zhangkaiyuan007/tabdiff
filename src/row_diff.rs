use std::cmp::Ordering;
use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use arrow::array::ArrayRef;

use crate::DiffConfig;
use crate::input::Table;
use crate::report::{
    Change, DiffCounts, DiffReport, KeyInfo, KeyVal, ModifiedRow, RowCounts, Samples,
};
use crate::schema_diff::SchemaDiff;
use crate::value::{Cell, Comparator, cmp_keys, extract};

pub fn diff_rows(
    left: &Table,
    right: &Table,
    schema: SchemaDiff,
    cfg: &DiffConfig,
    cmp: &Comparator,
) -> Result<DiffReport> {
    let (key_cols, inferred) = match &cfg.key {
        Some(k) => {
            validate_key(k, &schema)?;
            (k.clone(), false)
        }
        None => (infer_key(left, right, &schema.mutual)?, true),
    };

    let lkeys = extract_keys(left, &key_cols)?;
    let rkeys = extract_keys(right, &key_cols)?;
    let lidx = sorted_indices(&lkeys);
    let ridx = sorted_indices(&rkeys);
    ensure_unique(&lkeys, &lidx, &key_cols, &cfg.left.display().to_string())?;
    ensure_unique(&rkeys, &ridx, &key_cols, &cfg.right.display().to_string())?;

    let value_cols: Vec<(String, ArrayRef, ArrayRef)> = schema
        .mutual
        .iter()
        .filter(|c| !key_cols.contains(c))
        .map(|c| Ok((c.clone(), column(left, c)?, column(right, c)?)))
        .collect::<Result<_>>()?;

    let mut counts = DiffCounts {
        added: 0,
        removed: 0,
        modified: 0,
    };
    let mut samples = Samples {
        added: vec![],
        removed: vec![],
        modified: vec![],
    };
    let mut columns_changed: BTreeMap<String, usize> = BTreeMap::new();
    let mut truncated = false;

    let (mut i, mut j) = (0, 0);
    while i < lidx.len() || j < ridx.len() {
        if cfg
            .fail_fast
            .is_some_and(|n| counts.added + counts.removed + counts.modified >= n)
        {
            truncated = true;
            break;
        }
        let ord = if i >= lidx.len() {
            Ordering::Greater
        } else if j >= ridx.len() {
            Ordering::Less
        } else {
            cmp_keys(&lkeys[lidx[i]], &rkeys[ridx[j]])
        };
        match ord {
            Ordering::Less => {
                counts.removed += 1;
                if samples.removed.len() < cfg.max_samples {
                    samples.removed.push(render_key(&key_cols, &lkeys[lidx[i]]));
                }
                i += 1;
            }
            Ordering::Greater => {
                counts.added += 1;
                if samples.added.len() < cfg.max_samples {
                    samples.added.push(render_key(&key_cols, &rkeys[ridx[j]]));
                }
                j += 1;
            }
            Ordering::Equal => {
                let (li, rj) = (lidx[i], ridx[j]);
                let mut changes = vec![];
                for (name, la, ra) in &value_cols {
                    let lv = extract(la.as_ref(), li)?;
                    let rv = extract(ra.as_ref(), rj)?;
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
                            key: render_key(&key_cols, &lkeys[li]),
                            changes,
                        });
                    }
                }
                i += 1;
                j += 1;
            }
        }
    }

    Ok(DiffReport {
        schema,
        key: KeyInfo {
            columns: key_cols,
            inferred,
        },
        rows: RowCounts {
            left: left.batch.num_rows(),
            right: right.batch.num_rows(),
        },
        diff: counts,
        columns_changed,
        samples,
        truncated,
    })
}

fn column(t: &Table, name: &str) -> Result<ArrayRef> {
    t.batch
        .column_by_name(name)
        .cloned()
        .with_context(|| format!("column `{name}` missing"))
}

fn validate_key(key: &[String], schema: &SchemaDiff) -> Result<()> {
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

fn extract_keys(t: &Table, cols: &[String]) -> Result<Vec<Vec<Cell>>> {
    let arrays = cols
        .iter()
        .map(|c| column(t, c))
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

fn sorted_indices(keys: &[Vec<Cell>]) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..keys.len()).collect();
    idx.sort_unstable_by(|&a, &b| cmp_keys(&keys[a], &keys[b]));
    idx
}

fn has_adjacent_dup(keys: &[Vec<Cell>], idx: &[usize]) -> bool {
    idx.windows(2)
        .any(|w| cmp_keys(&keys[w[0]], &keys[w[1]]) == Ordering::Equal)
}

fn ensure_unique(
    keys: &[Vec<Cell>],
    idx: &[usize],
    key_cols: &[String],
    side: &str,
) -> Result<()> {
    let mut dups = 0usize;
    let mut example = None;
    for w in idx.windows(2) {
        if cmp_keys(&keys[w[0]], &keys[w[1]]) == Ordering::Equal {
            dups += 1;
            example.get_or_insert_with(|| {
                key_cols
                    .iter()
                    .zip(&keys[w[0]])
                    .map(|(c, v)| format!("{c}={}", v.render()))
                    .collect::<Vec<_>>()
                    .join(", ")
            });
        }
    }
    if dups > 0 {
        bail!(
            "key ({}) is not unique in {side}: {dups} duplicate(s), e.g. {}",
            key_cols.join(", "),
            example.unwrap()
        );
    }
    Ok(())
}

fn infer_key(left: &Table, right: &Table, mutual: &[String]) -> Result<Vec<String>> {
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
    let idx = sorted_indices(&keys);
    Ok(!has_adjacent_dup(&keys, &idx))
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
