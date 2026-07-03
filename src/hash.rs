//! Whole-row content hashing for keyless mode: appends a synthetic hash
//! column so the external-sort/merge pipeline can treat it as the key.

use std::sync::Arc;

use anyhow::{Result, bail};
use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use twox_hash::XxHash3_128;

use crate::input::BatchIter;
use crate::value::{Cell, extract};

pub const HASH_COL: &str = "__tabdiff_row_hash";

/// Wraps a batch stream, appending a hex hash column computed over `cols`
/// in the given canonical order (must match on both sides of the diff).
pub fn with_row_hash(src: BatchIter, cols: &[String]) -> Result<BatchIter> {
    if src.schema.index_of(HASH_COL).is_ok() {
        bail!("input already contains a `{HASH_COL}` column");
    }
    let idx = cols
        .iter()
        .map(|c| Ok(src.schema.index_of(c)?))
        .collect::<Result<Vec<usize>>>()?;
    let mut fields: Vec<Field> = src
        .schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(HASH_COL, DataType::Utf8, false));
    let schema = Arc::new(Schema::new(fields));
    let out_schema = schema.clone();
    let iter = src.iter.map(move |batch| {
        let batch = batch?;
        append_hash(&batch, &idx, &schema).map_err(|e| ArrowError::ComputeError(e.to_string()))
    });
    Ok(BatchIter {
        schema: out_schema,
        iter: Box::new(iter),
    })
}

fn append_hash(batch: &RecordBatch, idx: &[usize], schema: &Arc<Schema>) -> Result<RecordBatch> {
    let mut buf = Vec::with_capacity(64);
    let mut hashes = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        buf.clear();
        for &i in idx {
            write_cell(&mut buf, &extract(batch.column(i).as_ref(), row)?);
        }
        hashes.push(format!("{:032x}", XxHash3_128::oneshot(&buf)));
    }
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from(hashes)));
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

/// Serializes a cell for hashing with the same normalization the keyed
/// comparator applies: integral floats hash like ints (so CSV `1` matches
/// Parquet `1.0`), NaN is canonical, and -0.0 folds into 0.
fn write_cell(buf: &mut Vec<u8>, c: &Cell) {
    match c {
        Cell::Null => buf.push(0),
        Cell::Bool(b) => {
            buf.push(1);
            buf.push(*b as u8);
        }
        Cell::Int(i) => {
            buf.push(2);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        Cell::Float(f) => {
            let as_int = *f as i64;
            if as_int as f64 == *f {
                buf.push(2);
                buf.extend_from_slice(&as_int.to_le_bytes());
            } else if f.is_nan() {
                buf.push(3);
                buf.extend_from_slice(&f64::NAN.to_bits().to_le_bytes());
            } else {
                buf.push(3);
                buf.extend_from_slice(&f.to_bits().to_le_bytes());
            }
        }
        Cell::Str(s) => {
            buf.push(4);
            buf.extend_from_slice(&(s.len() as u64).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash_of(cells: &[Cell]) -> u128 {
        let mut buf = vec![];
        for c in cells {
            write_cell(&mut buf, c);
        }
        XxHash3_128::oneshot(&buf)
    }

    #[test]
    fn integral_float_hashes_like_int() {
        assert_eq!(hash_of(&[Cell::Int(1)]), hash_of(&[Cell::Float(1.0)]));
        assert_ne!(hash_of(&[Cell::Int(1)]), hash_of(&[Cell::Float(1.5)]));
    }

    #[test]
    fn negative_zero_folds_into_zero() {
        assert_eq!(hash_of(&[Cell::Float(-0.0)]), hash_of(&[Cell::Int(0)]));
    }

    #[test]
    fn int_and_string_hash_differently() {
        assert_ne!(hash_of(&[Cell::Int(1)]), hash_of(&[Cell::Str("1".into())]));
    }

    #[test]
    fn cell_boundaries_matter() {
        // ("ab", "c") must not collide with ("a", "bc")
        assert_ne!(
            hash_of(&[Cell::Str("ab".into()), Cell::Str("c".into())]),
            hash_of(&[Cell::Str("a".into()), Cell::Str("bc".into())])
        );
    }
}
