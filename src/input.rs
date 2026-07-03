use std::fs::File;
use std::io::Seek;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::csv::ReaderBuilder;
use arrow::csv::reader::Format;
use arrow::datatypes::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub struct Table {
    pub schema: SchemaRef,
    pub batch: RecordBatch,
}

// v0 materializes the whole file; the streaming core will replace this.
pub fn read_table(path: &Path) -> Result<Table> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let table = match ext.as_str() {
        "csv" => read_csv(path),
        "parquet" | "pq" => read_parquet(path),
        other => bail!("unsupported file extension `{other}` (expected .csv or .parquet)"),
    };
    table.with_context(|| format!("failed to read {}", path.display()))
}

fn read_csv(path: &Path) -> Result<Table> {
    let mut file = File::open(path)?;
    let format = Format::default().with_header(true);
    let (schema, _) = format.infer_schema(&mut file, Some(10_000))?;
    file.rewind()?;
    let schema = Arc::new(schema);
    let reader = ReaderBuilder::new(schema.clone())
        .with_format(format)
        .build(file)?;
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok(Table {
        batch: concat_batches(&schema, &batches)?,
        schema,
    })
}

fn read_parquet(path: &Path) -> Result<Table> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok(Table {
        batch: concat_batches(&schema, &batches)?,
        schema,
    })
}
