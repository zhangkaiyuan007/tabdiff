//! External sort: turns an unsorted batch stream into a globally key-sorted
//! row stream. Input within the memory budget stays in RAM; anything larger
//! spills sorted runs to Arrow IPC files that are k-way merged on read.
//!
//! Keys are encoded with arrow-row into byte-comparable form, so all ordering
//! and equality checks are memcmp. Both sides of a diff must build their
//! codecs from one [`KeySpec`] so the encodings are mutually comparable.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::{cast, concat_batches, take};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::row::{RowConverter, Rows, SortField};

use crate::input::BatchIter;
use crate::value::extract;

/// Rows per batch inside a spilled run, so merging k runs only holds
/// k small batches in memory rather than k whole runs.
const RUN_CHUNK_ROWS: usize = 8192;

pub struct Row {
    /// arrow-row encoded key; byte order == logical key order.
    pub key: Vec<u8>,
    pub batch: RecordBatch,
    pub row: usize,
}

/// Key columns plus the unified comparison types both sides agree on.
pub struct KeySpec {
    pub key_cols: Vec<String>,
    types: Vec<DataType>,
}

impl KeySpec {
    pub fn new(key_cols: &[String], left: &SchemaRef, right: &SchemaRef) -> Result<Self> {
        let types = key_cols
            .iter()
            .map(|c| {
                Ok(unify(
                    left.field_with_name(c)?.data_type(),
                    right.field_with_name(c)?.data_type(),
                ))
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            key_cols: key_cols.to_vec(),
            types,
        })
    }

    /// Builds this side's codec. Encodings from the two codecs of one spec
    /// are byte-comparable because they share the same sort fields.
    pub fn codec(&self, schema: &SchemaRef) -> Result<KeyCodec> {
        let key_idx = self
            .key_cols
            .iter()
            .map(|c| Ok(schema.index_of(c)?))
            .collect::<Result<Vec<_>>>()?;
        let converter = RowConverter::new(
            self.types
                .iter()
                .map(|t| SortField::new(t.clone()))
                .collect(),
        )?;
        Ok(KeyCodec {
            converter,
            key_idx,
            key_cols: self.key_cols.clone(),
            types: self.types.clone(),
        })
    }
}

fn base_type(t: &DataType) -> DataType {
    match t {
        DataType::Dictionary(_, v) => (**v).clone(),
        other => other.clone(),
    }
}

/// Cross-side key type unification: identical types stay as-is, numeric
/// pairs widen to Float64, everything else meets at Utf8 (so e.g. a UUID
/// column can match a text column, a top data-diff-era complaint).
fn unify(l: &DataType, r: &DataType) -> DataType {
    let l = base_type(l);
    let r = base_type(r);
    if l == r {
        l
    } else if l.is_numeric() && r.is_numeric() {
        DataType::Float64
    } else {
        DataType::Utf8
    }
}

pub struct KeyCodec {
    converter: RowConverter,
    key_idx: Vec<usize>,
    key_cols: Vec<String>,
    types: Vec<DataType>,
}

impl KeyCodec {
    pub fn key_idx(&self) -> &[usize] {
        &self.key_idx
    }

    fn encode(&self, batch: &RecordBatch) -> Result<Rows> {
        let cols = self
            .key_idx
            .iter()
            .zip(&self.types)
            .map(|(&i, t)| {
                let a = batch.column(i);
                if a.data_type() == t {
                    Ok(a.clone())
                } else {
                    Ok(cast(a.as_ref(), t)?)
                }
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        Ok(self.converter.convert_columns(&cols)?)
    }

    /// Renders the (original, un-cast) key cells of a row for messages.
    fn render_key(&self, batch: &RecordBatch, row: usize) -> String {
        self.key_cols
            .iter()
            .zip(&self.key_idx)
            .map(|(c, &i)| match extract(batch.column(i).as_ref(), row) {
                Ok(v) => format!("{c}={}", v.render()),
                Err(_) => format!("{c}=?"),
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Lazily created spill directory, removed on drop.
pub struct SpillDir {
    parent: Option<PathBuf>,
    path: Option<PathBuf>,
    counter: usize,
}

impl SpillDir {
    pub fn new(parent: Option<PathBuf>) -> Self {
        Self {
            parent,
            path: None,
            counter: 0,
        }
    }

    fn next_file(&mut self) -> Result<PathBuf> {
        if self.path.is_none() {
            let dir = self
                .parent
                .clone()
                .unwrap_or_else(std::env::temp_dir)
                .join(format!(
                    "tabdiff-spill-{}-{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                ));
            std::fs::create_dir_all(&dir)
                .with_context(|| format!("cannot create spill dir {}", dir.display()))?;
            self.path = Some(dir);
        }
        self.counter += 1;
        Ok(self
            .path
            .as_ref()
            .unwrap()
            .join(format!("run-{}.arrow", self.counter)))
    }
}

impl Drop for SpillDir {
    fn drop(&mut self) {
        if let Some(p) = &self.path {
            std::fs::remove_dir_all(p).ok();
        }
    }
}

pub enum RunSource {
    Memory(SchemaRef, Vec<RecordBatch>),
    File(SchemaRef, PathBuf),
}

impl RunSource {
    fn open(&self) -> Result<BatchIter> {
        match self {
            RunSource::Memory(schema, chunks) => Ok(BatchIter {
                schema: schema.clone(),
                iter: Box::new(chunks.clone().into_iter().map(Ok)),
            }),
            RunSource::File(schema, path) => {
                let reader = StreamReader::try_new(BufReader::new(File::open(path)?), None)?;
                Ok(BatchIter {
                    schema: schema.clone(),
                    iter: Box::new(reader),
                })
            }
        }
    }
}

/// Consumes a batch stream, producing sorted runs plus the total row count.
pub fn sort_into_runs(
    src: BatchIter,
    codec: &KeyCodec,
    budget_bytes: usize,
    spill: &mut SpillDir,
) -> Result<(Vec<RunSource>, usize)> {
    let schema = src.schema.clone();

    let mut runs = vec![];
    let mut pending: Vec<RecordBatch> = vec![];
    let mut pending_bytes = 0usize;
    let mut total_rows = 0usize;

    for batch in src.iter {
        let batch = batch?;
        total_rows += batch.num_rows();
        pending_bytes += batch.get_array_memory_size();
        pending.push(batch);
        if pending_bytes >= budget_bytes {
            let chunks = sort_chunks(&schema, std::mem::take(&mut pending), codec)?;
            runs.push(spill_run(&schema, chunks, spill)?);
            pending_bytes = 0;
        }
    }

    if runs.is_empty() {
        // Everything fit in the budget: keep the single run in memory.
        let chunks = sort_chunks(&schema, pending, codec)?;
        runs.push(RunSource::Memory(schema, chunks));
    } else if !pending.is_empty() {
        let chunks = sort_chunks(&schema, std::mem::take(&mut pending), codec)?;
        runs.push(spill_run(&schema, chunks, spill)?);
    }
    Ok((runs, total_rows))
}

/// Sorts accumulated batches by key and re-slices into small chunks.
/// Already-sorted input (common for keyed exports) skips the permutation.
fn sort_chunks(
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    codec: &KeyCodec,
) -> Result<Vec<RecordBatch>> {
    let merged = concat_batches(schema, &batches)?;
    let n = merged.num_rows();
    if n == 0 {
        return Ok(vec![]);
    }
    let rows = codec.encode(&merged)?;
    let sorted = if (1..n).all(|i| rows.row(i - 1) <= rows.row(i)) {
        merged
    } else {
        let mut idx: Vec<u32> = (0..n as u32).collect();
        idx.sort_unstable_by(|&a, &b| rows.row(a as usize).cmp(&rows.row(b as usize)));
        let idx = UInt32Array::from(idx);
        let columns = merged
            .columns()
            .iter()
            .map(|c| Ok(take(c.as_ref(), &idx, None)?))
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(schema.clone(), columns)?
    };
    Ok((0..n)
        .step_by(RUN_CHUNK_ROWS)
        .map(|off| sorted.slice(off, RUN_CHUNK_ROWS.min(n - off)))
        .collect())
}

fn spill_run(
    schema: &SchemaRef,
    chunks: Vec<RecordBatch>,
    spill: &mut SpillDir,
) -> Result<RunSource> {
    let path = spill.next_file()?;
    let file = File::create(&path)
        .with_context(|| format!("cannot create spill file {}", path.display()))?;
    let mut writer = StreamWriter::try_new(file, schema)?;
    for chunk in &chunks {
        writer.write(chunk)?;
    }
    writer.finish()?;
    Ok(RunSource::File(schema.clone(), path))
}

struct RunCursor {
    iter: Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>>>,
    codec: Arc<KeyCodec>,
    batch: Option<RecordBatch>,
    rows: Option<Rows>,
    pos: usize,
}

impl RunCursor {
    fn new(src: BatchIter, codec: Arc<KeyCodec>) -> Result<Self> {
        let mut cursor = Self {
            iter: src.iter,
            codec,
            batch: None,
            rows: None,
            pos: 0,
        };
        cursor.load()?;
        Ok(cursor)
    }

    fn load(&mut self) -> Result<()> {
        loop {
            match self.iter.next() {
                None => {
                    self.batch = None;
                    self.rows = None;
                    return Ok(());
                }
                Some(batch) => {
                    let batch = batch?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    self.rows = Some(self.codec.encode(&batch)?);
                    self.batch = Some(batch);
                    self.pos = 0;
                    return Ok(());
                }
            }
        }
    }

    fn peek_key(&self) -> Option<&[u8]> {
        self.rows.as_ref().map(|r| r.row(self.pos).data())
    }

    fn take_row(&mut self) -> Result<Row> {
        let batch = self.batch.clone().expect("take_row on exhausted cursor");
        let key = self
            .rows
            .as_ref()
            .expect("rows present with batch")
            .row(self.pos)
            .as_ref()
            .to_vec();
        let row = Row {
            key,
            batch,
            row: self.pos,
        };
        self.pos += 1;
        if self.pos >= row.batch.num_rows() {
            self.load()?;
        }
        Ok(row)
    }
}

struct KWayMerge {
    cursors: Vec<RunCursor>,
    heap: BinaryHeap<Reverse<(Vec<u8>, usize)>>,
}

impl KWayMerge {
    fn new(cursors: Vec<RunCursor>) -> Self {
        let mut heap = BinaryHeap::new();
        for (i, cursor) in cursors.iter().enumerate() {
            if let Some(k) = cursor.peek_key() {
                heap.push(Reverse((k.to_vec(), i)));
            }
        }
        Self { cursors, heap }
    }

    fn next(&mut self) -> Result<Option<Row>> {
        let Some(Reverse((_, i))) = self.heap.pop() else {
            return Ok(None);
        };
        let row = self.cursors[i].take_row()?;
        if let Some(k) = self.cursors[i].peek_key() {
            self.heap.push(Reverse((k.to_vec(), i)));
        }
        Ok(Some(row))
    }
}

/// Globally sorted row stream over one side of the diff.
pub struct SortedSource {
    merge: KWayMerge,
    codec: Arc<KeyCodec>,
    current: Option<Row>,
    label: String,
    /// Verify ascending key order while streaming (--assume-sorted path).
    validate_order: bool,
    consumed: usize,
    total_hint: Option<usize>,
}

impl SortedSource {
    pub fn from_runs(
        runs: Vec<RunSource>,
        codec: Arc<KeyCodec>,
        label: String,
        total_rows: usize,
    ) -> Result<Self> {
        let cursors = runs
            .iter()
            .map(|r| RunCursor::new(r.open()?, codec.clone()))
            .collect::<Result<Vec<_>>>()?;
        Self::start(
            KWayMerge::new(cursors),
            codec,
            label,
            false,
            Some(total_rows),
        )
    }

    /// Streams directly from the reader without sorting or spilling; key
    /// order is verified on the fly and violations are an error.
    pub fn from_stream(src: BatchIter, codec: Arc<KeyCodec>, label: String) -> Result<Self> {
        let cursor = RunCursor::new(src, codec.clone())?;
        Self::start(KWayMerge::new(vec![cursor]), codec, label, true, None)
    }

    fn start(
        merge: KWayMerge,
        codec: Arc<KeyCodec>,
        label: String,
        validate_order: bool,
        total_hint: Option<usize>,
    ) -> Result<Self> {
        let mut source = Self {
            merge,
            codec,
            current: None,
            label,
            validate_order,
            consumed: 0,
            total_hint,
        };
        source.current = source.pull()?;
        Ok(source)
    }

    fn pull(&mut self) -> Result<Option<Row>> {
        let next = self.merge.next()?;
        if let Some(row) = &next {
            self.consumed += 1;
            if self.validate_order
                && let Some(cur) = &self.current
                && row.key < cur.key
            {
                bail!(
                    "{} is not sorted by the key (row with {} arrived after {}); \
                     drop --assume-sorted",
                    self.label,
                    self.codec.render_key(&row.batch, row.row),
                    self.codec.render_key(&cur.batch, cur.row),
                );
            }
        }
        Ok(next)
    }

    pub fn peek(&self) -> Option<&Row> {
        self.current.as_ref()
    }

    /// Total rows on this side: exact when pre-counted by the sort phase,
    /// otherwise rows consumed so far (a lower bound if the scan stopped early).
    pub fn total_rows(&self) -> usize {
        self.total_hint.unwrap_or(self.consumed)
    }

    /// Advances past every row sharing the current key and returns the group
    /// size. Used by keyless mode, where duplicate keys are expected data
    /// rather than an error.
    pub fn advance_group(&mut self) -> Result<usize> {
        let Some(key) = self.current.as_ref().map(|r| r.key.clone()) else {
            return Ok(0);
        };
        let mut n = 0;
        while let Some(cur) = &self.current {
            if cur.key != key {
                break;
            }
            n += 1;
            self.current = self.pull()?;
        }
        Ok(n)
    }

    pub fn advance(&mut self) -> Result<()> {
        let next = self.pull()?;
        if let (Some(cur), Some(nxt)) = (&self.current, &next)
            && cur.key == nxt.key
        {
            bail!(
                "key ({}) is not unique in {}: e.g. {}; pick a different --key or use --keyless",
                self.codec.key_cols.join(", "),
                self.label,
                self.codec.render_key(&cur.batch, cur.row),
            );
        }
        self.current = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unify_keeps_identical_types() {
        assert_eq!(unify(&DataType::Int64, &DataType::Int64), DataType::Int64);
        assert_eq!(unify(&DataType::Utf8, &DataType::Utf8), DataType::Utf8);
    }

    #[test]
    fn unify_widens_numeric_pairs() {
        assert_eq!(
            unify(&DataType::Int64, &DataType::Float64),
            DataType::Float64
        );
        assert_eq!(
            unify(&DataType::UInt32, &DataType::Int16),
            DataType::Float64
        );
    }

    #[test]
    fn unify_falls_back_to_utf8() {
        assert_eq!(unify(&DataType::Int64, &DataType::Utf8), DataType::Utf8);
        assert_eq!(unify(&DataType::Date32, &DataType::Utf8), DataType::Utf8);
    }
}
