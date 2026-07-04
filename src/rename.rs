//! Column rename detection: pairs removed (left-only) columns with added
//! (right-only) columns by content similarity, so a rename is reported as
//! one schema change and the column still participates in the row diff.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;

use crate::DiffConfig;
use crate::hash::write_cell;
use crate::input::{Table, read_sample};
use crate::schema_diff::{RenamedColumn, SchemaDiff};
use crate::value::{Comparator, extract};

const SAMPLE_ROWS: usize = 10_000;
/// Keyed detection: minimum key-matched sample rows to attempt a claim.
const MIN_MATCHED_ROWS: usize = 20;
/// Keyed detection: fraction of matched rows whose values must agree.
const KEYED_MATCH_RATE: f64 = 0.95;
/// Keyless detection: minimum distinct values, so low-cardinality columns
/// (booleans, small enums) can never masquerade as renames of each other.
const MIN_DISTINCT: usize = 8;
const JACCARD_THRESHOLD: f64 = 0.85;

/// Detects renames among `schema`'s added/removed columns and moves the
/// matched pairs into `schema.renamed`. With a key, values are compared on
/// key-matched sample rows; without one, by value-set overlap.
pub fn detect_renames(
    cfg: &DiffConfig,
    lschema: &SchemaRef,
    rschema: &SchemaRef,
    schema: &mut SchemaDiff,
    key: Option<&[String]>,
) -> Result<()> {
    if schema.added.is_empty() || schema.removed.is_empty() {
        return Ok(());
    }
    let removed: Vec<String> = schema.removed.iter().map(|c| c.name.clone()).collect();
    let added: Vec<String> = schema.added.iter().map(|c| c.name.clone()).collect();
    let candidates = match key {
        Some(k) => keyed_candidates(cfg, lschema, rschema, &removed, &added, k)?,
        None => jaccard_candidates(cfg, lschema, rschema, &removed, &added)?,
    };
    apply(schema, candidates);
    Ok(())
}

fn keyed_candidates(
    cfg: &DiffConfig,
    lschema: &SchemaRef,
    rschema: &SchemaRef,
    removed: &[String],
    added: &[String],
    key: &[String],
) -> Result<Vec<(String, String, f64)>> {
    let mut lcols = key.to_vec();
    lcols.extend(removed.iter().cloned());
    let mut rcols = key.to_vec();
    rcols.extend(added.iter().cloned());
    let lsample = read_sample(&cfg.left, lschema, &lcols, SAMPLE_ROWS, cfg.input_format)?;
    let rsample = read_sample(&cfg.right, rschema, &rcols, SAMPLE_ROWS, cfg.input_format)?;

    let lmap = key_map(&lsample, key)?;
    let rmap = key_map(&rsample, key)?;
    let matched: Vec<(usize, usize)> = lmap
        .iter()
        .filter_map(|(k, &li)| rmap.get(k).map(|&ri| (li, ri)))
        .collect();
    if matched.len() < MIN_MATCHED_ROWS {
        return Ok(vec![]);
    }

    let cmp = Comparator::default();
    let mut candidates = vec![];
    for rem in removed {
        let la = column(&lsample, rem)?;
        for add in added {
            let ra = column(&rsample, add)?;
            let mut eq = 0usize;
            for &(li, ri) in &matched {
                if cmp.eq(
                    &extract(la.as_ref(), li)?,
                    &extract(ra.as_ref(), ri)?,
                ) {
                    eq += 1;
                }
            }
            let rate = eq as f64 / matched.len() as f64;
            if rate >= KEYED_MATCH_RATE {
                candidates.push((rem.clone(), add.clone(), rate));
            }
        }
    }
    Ok(candidates)
}

fn jaccard_candidates(
    cfg: &DiffConfig,
    lschema: &SchemaRef,
    rschema: &SchemaRef,
    removed: &[String],
    added: &[String],
) -> Result<Vec<(String, String, f64)>> {
    let lsample = read_sample(&cfg.left, lschema, removed, SAMPLE_ROWS, cfg.input_format)?;
    let rsample = read_sample(&cfg.right, rschema, added, SAMPLE_ROWS, cfg.input_format)?;
    let mut candidates = vec![];
    for rem in removed {
        let lset = value_set(&lsample, rem)?;
        if lset.len() < MIN_DISTINCT {
            continue;
        }
        for add in added {
            let rset = value_set(&rsample, add)?;
            if rset.len() < MIN_DISTINCT {
                continue;
            }
            let inter = lset.intersection(&rset).count();
            let union = lset.len() + rset.len() - inter;
            let jaccard = inter as f64 / union as f64;
            if jaccard >= JACCARD_THRESHOLD {
                candidates.push((rem.clone(), add.clone(), jaccard));
            }
        }
    }
    Ok(candidates)
}

/// Greedy mutual-best assignment: highest similarity wins, each column
/// participates in at most one rename.
fn apply(schema: &mut SchemaDiff, mut candidates: Vec<(String, String, f64)>) {
    candidates.sort_by(|a, b| b.2.total_cmp(&a.2));
    let mut used_l = HashSet::new();
    let mut used_r = HashSet::new();
    for (left, right, similarity) in candidates {
        if used_l.contains(&left) || used_r.contains(&right) {
            continue;
        }
        used_l.insert(left.clone());
        used_r.insert(right.clone());
        schema.removed.retain(|c| c.name != left);
        schema.added.retain(|c| c.name != right);
        schema.renamed.push(RenamedColumn {
            left,
            right,
            similarity,
        });
    }
}

fn column(t: &Table, name: &str) -> Result<ArrayRef> {
    t.batch
        .column_by_name(name)
        .cloned()
        .with_context(|| format!("column `{name}` missing from sample"))
}

fn key_map(t: &Table, key: &[String]) -> Result<HashMap<Vec<u8>, usize>> {
    let arrays = key
        .iter()
        .map(|c| column(t, c))
        .collect::<Result<Vec<_>>>()?;
    let mut map = HashMap::with_capacity(t.batch.num_rows());
    let mut buf = vec![];
    for row in 0..t.batch.num_rows() {
        buf.clear();
        for a in &arrays {
            write_cell(&mut buf, &extract(a.as_ref(), row)?);
        }
        map.entry(buf.clone()).or_insert(row);
    }
    Ok(map)
}

fn value_set(t: &Table, col: &str) -> Result<HashSet<Vec<u8>>> {
    let array = column(t, col)?;
    let mut set = HashSet::new();
    let mut buf = vec![];
    for row in 0..t.batch.num_rows() {
        buf.clear();
        write_cell(&mut buf, &extract(array.as_ref(), row)?);
        set.insert(buf.clone());
    }
    Ok(set)
}
