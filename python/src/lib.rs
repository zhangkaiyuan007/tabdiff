//! Python bindings: `tabdiff.diff(left, right, ...)` returns the diff
//! report as a dict (the JSON report plus a `has_differences` bool), the
//! shape pytest assertions want.

use std::path::PathBuf;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};

use tabdiff::DiffConfig;
use tabdiff::input::FileFormat;

#[pyfunction]
#[pyo3(signature = (
    left,
    right,
    *,
    key = None,
    keyless = false,
    tol_abs = None,
    tol_rel = None,
    fail_fast = None,
    samples = 10,
    memory_mb = 256,
    assume_sorted = false,
    spill_dir = None,
    input_format = None,
    r#where = None,
))]
#[allow(clippy::too_many_arguments)]
fn diff(
    py: Python<'_>,
    left: PathBuf,
    right: PathBuf,
    key: Option<Vec<String>>,
    keyless: bool,
    tol_abs: Option<f64>,
    tol_rel: Option<f64>,
    fail_fast: Option<usize>,
    samples: usize,
    memory_mb: usize,
    assume_sorted: bool,
    spill_dir: Option<PathBuf>,
    input_format: Option<String>,
    r#where: Option<String>,
) -> PyResult<Py<PyDict>> {
    let input_format = match input_format.as_deref() {
        None => None,
        Some("csv") => Some(FileFormat::Csv),
        Some("parquet") => Some(FileFormat::Parquet),
        Some(other) => {
            return Err(PyValueError::new_err(format!(
                "input_format must be 'csv' or 'parquet', got {other:?}"
            )));
        }
    };
    let cfg = DiffConfig {
        left,
        right,
        key,
        tol_abs,
        tol_rel,
        fail_fast,
        max_samples: samples,
        memory_mb,
        keyless,
        assume_sorted,
        spill_dir,
        input_format,
        where_expr: r#where,
    };
    let report = tabdiff::run_diff(&cfg).map_err(|e| PyRuntimeError::new_err(format!("{e:#}")))?;
    let dict: Py<PyDict> = PyModule::import(py, "json")?
        .call_method1("loads", (report.to_json(),))?
        .extract()?;
    dict.bind(py).set_item("has_differences", report.has_differences())?;
    Ok(dict)
}

#[pymodule]
#[pyo3(name = "tabdiff")]
fn tabdiff_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(diff, m)?)?;
    Ok(())
}
