use std::fs::File;
use std::io::Seek;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::compute::concat_batches;
use arrow::csv::ReaderBuilder;
use arrow::csv::reader::Format;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const BATCH_ROWS: usize = 8192;
const CSV_INFER_ROWS: usize = 10_000;

/// A schema plus a stream of record batches conforming to it.
pub struct BatchIter {
    pub schema: SchemaRef,
    pub iter: Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>>>,
}

/// A fully materialized table; used for samples and small inputs.
pub struct Table {
    pub schema: SchemaRef,
    pub batch: RecordBatch,
}

/// Reads only the schema (CSV: inferred from a sample; Parquet: metadata).
pub fn probe_schema(path: &Path) -> Result<SchemaRef> {
    let schema = match extension(path)? {
        Kind::Csv => {
            let mut file = File::open(path)?;
            let format = Format::default().with_header(true);
            let (schema, _) = format.infer_schema(&mut file, Some(CSV_INFER_ROWS))?;
            Arc::new(schema)
        }
        Kind::Parquet => {
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
            builder.schema().clone()
        }
    };
    Ok(schema)
}

/// Opens a batch stream projected to `columns` (order-independent).
pub fn open_batches(path: &Path, full: &SchemaRef, columns: &[String]) -> Result<BatchIter> {
    // Ascending indices: both readers then agree with `Schema::project`.
    let mut indices = columns
        .iter()
        .map(|c| Ok(full.index_of(c)?))
        .collect::<Result<Vec<usize>>>()?;
    indices.sort_unstable();

    let batches = match extension(path)? {
        Kind::Csv => {
            let mut file = File::open(path)?;
            let format = Format::default().with_header(true);
            file.rewind()?;
            let reader = ReaderBuilder::new(full.clone())
                .with_format(format)
                .with_batch_size(BATCH_ROWS)
                .with_projection(indices.clone())
                .build(file)?;
            BatchIter {
                schema: Arc::new(full.project(&indices)?),
                iter: Box::new(reader),
            }
        }
        Kind::Parquet => {
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
            let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
            let reader = builder
                .with_batch_size(BATCH_ROWS)
                .with_projection(mask)
                .build()?;
            BatchIter {
                schema: reader.schema(),
                iter: Box::new(reader),
            }
        }
    };
    Ok(batches)
}

/// Materializes up to `max_rows` from the start of the file.
pub fn read_sample(
    path: &Path,
    full: &SchemaRef,
    columns: &[String],
    max_rows: usize,
) -> Result<Table> {
    let src = open_batches(path, full, columns)?;
    let mut batches = vec![];
    let mut rows = 0;
    for batch in src.iter {
        let batch = batch?;
        rows += batch.num_rows();
        batches.push(batch);
        if rows >= max_rows {
            break;
        }
    }
    Ok(Table {
        batch: concat_batches(&src.schema, &batches)?,
        schema: src.schema,
    })
}

/// Materializes a whole file; convenience for tests and tools.
pub fn read_table(path: &Path) -> Result<Table> {
    let full = probe_schema(path).with_context(|| format!("failed to read {}", path.display()))?;
    let all: Vec<String> = full.fields().iter().map(|f| f.name().clone()).collect();
    read_sample(path, &full, &all, usize::MAX)
        .with_context(|| format!("failed to read {}", path.display()))
}

enum Kind {
    Csv,
    Parquet,
}

fn extension(path: &Path) -> Result<Kind> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "csv" => Ok(Kind::Csv),
        "parquet" | "pq" => Ok(Kind::Parquet),
        other => bail!(
            "unsupported file extension `{other}` for {} (expected .csv or .parquet)",
            path.display()
        ),
    }
}
