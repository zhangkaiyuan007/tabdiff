use std::cmp::Ordering;

use anyhow::Result;
use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeStringArray, StringArray, StringViewArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::util::display::array_value_to_string;

/// A single table cell normalized into tabdiff's comparison domain.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

impl Cell {
    pub fn render(&self) -> String {
        match self {
            Cell::Null => "NULL".to_string(),
            Cell::Bool(b) => b.to_string(),
            Cell::Int(i) => i.to_string(),
            Cell::Float(f) => f.to_string(),
            Cell::Str(s) => s.clone(),
        }
    }

    /// Like `render`, but quotes strings so `1` and `"1"` stay distinguishable.
    pub fn render_typed(&self) -> String {
        match self {
            Cell::Str(s) => format!("{s:?}"),
            other => other.render(),
        }
    }
}

pub fn extract(array: &dyn Array, row: usize) -> Result<Cell> {
    if array.is_null(row) {
        return Ok(Cell::Null);
    }
    macro_rules! int {
        ($t:ty) => {{
            let a = array.as_any().downcast_ref::<$t>().unwrap();
            Cell::Int(a.value(row) as i64)
        }};
    }
    macro_rules! string {
        ($t:ty) => {{
            let a = array.as_any().downcast_ref::<$t>().unwrap();
            Cell::Str(a.value(row).to_string())
        }};
    }
    Ok(match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Cell::Bool(a.value(row))
        }
        DataType::Int8 => int!(Int8Array),
        DataType::Int16 => int!(Int16Array),
        DataType::Int32 => int!(Int32Array),
        DataType::Int64 => int!(Int64Array),
        DataType::UInt8 => int!(UInt8Array),
        DataType::UInt16 => int!(UInt16Array),
        DataType::UInt32 => int!(UInt32Array),
        DataType::UInt64 => {
            let v = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row);
            match i64::try_from(v) {
                Ok(i) => Cell::Int(i),
                Err(_) => Cell::Str(v.to_string()),
            }
        }
        DataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Cell::Float(a.value(row) as f64)
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Cell::Float(a.value(row))
        }
        DataType::Utf8 => string!(StringArray),
        DataType::LargeUtf8 => string!(LargeStringArray),
        DataType::Utf8View => string!(StringViewArray),
        // Dates, timestamps, decimals, etc. fall back to Arrow's canonical
        // rendering, which normalizes formatting across file formats.
        _ => Cell::Str(array_value_to_string(array, row)?),
    })
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Comparator {
    pub tol_abs: Option<f64>,
    pub tol_rel: Option<f64>,
}

impl Comparator {
    pub fn new(tol_abs: Option<f64>, tol_rel: Option<f64>) -> Self {
        Self { tol_abs, tol_rel }
    }

    pub fn eq(&self, a: &Cell, b: &Cell) -> bool {
        match (a, b) {
            (Cell::Null, Cell::Null) => true,
            (Cell::Bool(x), Cell::Bool(y)) => x == y,
            (Cell::Int(x), Cell::Int(y)) => x == y,
            (Cell::Float(x), Cell::Float(y)) => self.float_eq(*x, *y),
            (Cell::Int(x), Cell::Float(y)) | (Cell::Float(y), Cell::Int(x)) => {
                self.float_eq(*x as f64, *y)
            }
            (Cell::Str(x), Cell::Str(y)) => x == y,
            _ => false,
        }
    }

    fn float_eq(&self, a: f64, b: f64) -> bool {
        if a == b || (a.is_nan() && b.is_nan()) {
            return true;
        }
        let tol = f64::max(
            self.tol_abs.unwrap_or(0.0),
            self.tol_rel.unwrap_or(0.0) * f64::max(a.abs(), b.abs()),
        );
        tol > 0.0 && (a - b).abs() <= tol
    }
}

fn rank(c: &Cell) -> u8 {
    match c {
        Cell::Null => 0,
        Cell::Bool(_) => 1,
        Cell::Int(_) | Cell::Float(_) => 2,
        Cell::Str(_) => 3,
    }
}

/// Total order used for sorting keys; values of unrelated types order by rank.
pub fn cmp_cells(a: &Cell, b: &Cell) -> Ordering {
    match (a, b) {
        (Cell::Null, Cell::Null) => Ordering::Equal,
        (Cell::Bool(x), Cell::Bool(y)) => x.cmp(y),
        (Cell::Int(x), Cell::Int(y)) => x.cmp(y),
        (Cell::Float(x), Cell::Float(y)) => x.total_cmp(y),
        (Cell::Int(x), Cell::Float(y)) => (*x as f64).total_cmp(y),
        (Cell::Float(x), Cell::Int(y)) => x.total_cmp(&(*y as f64)),
        (Cell::Str(x), Cell::Str(y)) => x.cmp(y),
        _ => rank(a).cmp(&rank(b)),
    }
}

pub fn cmp_keys(a: &[Cell], b: &[Cell]) -> Ordering {
    for (x, y) in a.iter().zip(b) {
        let o = cmp_cells(x, y);
        if o != Ordering::Equal {
            return o;
        }
    }
    a.len().cmp(&b.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_by_default() {
        let c = Comparator::default();
        assert!(!c.eq(&Cell::Float(1.0), &Cell::Float(1.0000001)));
        assert!(c.eq(&Cell::Float(1.5), &Cell::Float(1.5)));
    }

    #[test]
    fn int_float_compare_numerically() {
        let c = Comparator::default();
        assert!(c.eq(&Cell::Int(1), &Cell::Float(1.0)));
        assert!(!c.eq(&Cell::Int(1), &Cell::Float(1.1)));
    }

    #[test]
    fn int_str_never_equal() {
        let c = Comparator::default();
        assert!(!c.eq(&Cell::Int(1), &Cell::Str("1".into())));
    }

    #[test]
    fn absolute_tolerance() {
        let c = Comparator::new(Some(0.01), None);
        assert!(c.eq(&Cell::Float(1.0), &Cell::Float(1.005)));
        assert!(!c.eq(&Cell::Float(1.0), &Cell::Float(1.02)));
    }

    #[test]
    fn relative_tolerance() {
        let c = Comparator::new(None, Some(0.01));
        assert!(c.eq(&Cell::Float(100.0), &Cell::Float(100.5)));
        assert!(!c.eq(&Cell::Float(100.0), &Cell::Float(102.0)));
    }

    #[test]
    fn nan_equals_nan() {
        let c = Comparator::default();
        assert!(c.eq(&Cell::Float(f64::NAN), &Cell::Float(f64::NAN)));
    }

    #[test]
    fn key_ordering_is_lexicographic() {
        let a = vec![Cell::Int(1), Cell::Str("b".into())];
        let b = vec![Cell::Int(1), Cell::Str("c".into())];
        assert_eq!(cmp_keys(&a, &b), Ordering::Less);
        assert_eq!(cmp_keys(&a, &a.clone()), Ordering::Equal);
    }
}
