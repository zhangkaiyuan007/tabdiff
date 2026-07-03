//! External sort: turns an unsorted batch stream into a globally key-sorted
//! row stream. Input within the memory budget stays in RAM; anything larger
//! spills sorted runs to Arrow IPC files that are k-way merged on read.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use arrow::array::{RecordBatch, UInt32Array};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;

use crate::input::BatchIter;
use crate::value::{Cell, cmp_keys, extract};

/// Rows per batch inside a spilled run, so merging k runs only holds
/// k small batches in memory rather than k whole runs.
const RUN_CHUNK_ROWS: usize = 8192;

pub struct Row {
    pub key: Vec<Cell>,
    pub batch: RecordBatch,
    pub row: usize,
}

/// Lazily created spill directory, removed on drop.
pub struct SpillDir {
    path: Option<PathBuf>,
    counter: usize,
}

impl SpillDir {
    pub fn new() -> Self {
        Self { path: None, counter: 0 }
    }

    fn next_file(&mut self) -> Result<PathBuf> {
        if self.path.is_none() {
            let dir = std::env::temp_dir().join(format!(
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
        Ok(self.path.as_ref().unwrap().join(format!("run-{}.arrow", self.counter)))
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
    key_cols: &[String],
    budget_bytes: usize,
    spill: &mut SpillDir,
) -> Result<(Vec<RunSource>, usize)> {
    let schema = src.schema.clone();
    let key_idx = key_indices(&schema, key_cols)?;

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
            let chunks = sort_chunks(&schema, std::mem::take(&mut pending), &key_idx)?;
            runs.push(spill_run(&schema, chunks, spill)?);
            pending_bytes = 0;
        }
    }

    if runs.is_empty() {
        // Everything fit in the budget: keep the single run in memory.
        let chunks = sort_chunks(&schema, pending, &key_idx)?;
        runs.push(RunSource::Memory(schema, chunks));
    } else if !pending.is_empty() {
        let chunks = sort_chunks(&schema, std::mem::take(&mut pending), &key_idx)?;
        runs.push(spill_run(&schema, chunks, spill)?);
    }
    Ok((runs, total_rows))
}

pub fn key_indices(schema: &SchemaRef, key_cols: &[String]) -> Result<Vec<usize>> {
    key_cols
        .iter()
        .map(|c| Ok(schema.index_of(c)?))
        .collect()
}

fn batch_keys(batch: &RecordBatch, key_idx: &[usize]) -> Result<Vec<Vec<Cell>>> {
    let mut keys = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let key = key_idx
            .iter()
            .map(|&i| extract(batch.column(i).as_ref(), row))
            .collect::<Result<Vec<_>>>()?;
        keys.push(key);
    }
    Ok(keys)
}

/// Sorts accumulated batches by key and re-slices into small chunks.
fn sort_chunks(
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    key_idx: &[usize],
) -> Result<Vec<RecordBatch>> {
    let merged = concat_batches(schema, &batches)?;
    if merged.num_rows() == 0 {
        return Ok(vec![]);
    }
    let keys = batch_keys(&merged, key_idx)?;
    let mut idx: Vec<u32> = (0..merged.num_rows() as u32).collect();
    idx.sort_unstable_by(|&a, &b| cmp_keys(&keys[a as usize], &keys[b as usize]));
    let idx = UInt32Array::from(idx);
    let columns = merged
        .columns()
        .iter()
        .map(|c| Ok(take(c.as_ref(), &idx, None)?))
        .collect::<Result<Vec<_>>>()?;
    let sorted = RecordBatch::try_new(schema.clone(), columns)?;
    Ok((0..sorted.num_rows())
        .step_by(RUN_CHUNK_ROWS)
        .map(|off| sorted.slice(off, RUN_CHUNK_ROWS.min(sorted.num_rows() - off)))
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

/// Key wrapper giving `Vec<Cell>` the total order needed by the merge heap.
struct KeyOrd(Vec<Cell>);

impl PartialEq for KeyOrd {
    fn eq(&self, other: &Self) -> bool {
        cmp_keys(&self.0, &other.0) == Ordering::Equal
    }
}
impl Eq for KeyOrd {}
impl PartialOrd for KeyOrd {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for KeyOrd {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_keys(&self.0, &other.0)
    }
}

struct RunCursor {
    iter: Box<dyn Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>>,
    key_idx: Vec<usize>,
    batch: Option<RecordBatch>,
    keys: Vec<Vec<Cell>>,
    pos: usize,
}

impl RunCursor {
    fn new(src: BatchIter, key_cols: &[String]) -> Result<Self> {
        let key_idx = key_indices(&src.schema, key_cols)?;
        let mut cursor = Self {
            iter: src.iter,
            key_idx,
            batch: None,
            keys: vec![],
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
                    return Ok(());
                }
                Some(batch) => {
                    let batch = batch?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    self.keys = batch_keys(&batch, &self.key_idx)?;
                    self.batch = Some(batch);
                    self.pos = 0;
                    return Ok(());
                }
            }
        }
    }

    fn peek_key(&self) -> Option<&Vec<Cell>> {
        self.batch.as_ref().map(|_| &self.keys[self.pos])
    }

    fn take_row(&mut self) -> Result<Row> {
        let batch = self.batch.clone().expect("take_row on exhausted cursor");
        let key = std::mem::take(&mut self.keys[self.pos]);
        let row = Row { key, batch, row: self.pos };
        self.pos += 1;
        if self.pos >= self.keys.len() {
            self.load()?;
        }
        Ok(row)
    }
}

struct KWayMerge {
    cursors: Vec<RunCursor>,
    heap: BinaryHeap<Reverse<(KeyOrd, usize)>>,
}

impl KWayMerge {
    fn new(runs: &[RunSource], key_cols: &[String]) -> Result<Self> {
        let mut cursors = vec![];
        let mut heap = BinaryHeap::new();
        for run in runs {
            let cursor = RunCursor::new(run.open()?, key_cols)?;
            if let Some(k) = cursor.peek_key() {
                heap.push(Reverse((KeyOrd(k.clone()), cursors.len())));
            }
            cursors.push(cursor);
        }
        Ok(Self { cursors, heap })
    }

    fn next(&mut self) -> Result<Option<Row>> {
        let Some(Reverse((_, i))) = self.heap.pop() else {
            return Ok(None);
        };
        let row = self.cursors[i].take_row()?;
        if let Some(k) = self.cursors[i].peek_key() {
            self.heap.push(Reverse((KeyOrd(k.clone()), i)));
        }
        Ok(Some(row))
    }
}

/// Globally sorted row stream over one side of the diff. Enforces key
/// uniqueness (adjacent duplicates) while streaming.
pub struct SortedSource {
    merge: KWayMerge,
    current: Option<Row>,
    key_cols: Vec<String>,
    label: String,
}

impl SortedSource {
    pub fn new(runs: Vec<RunSource>, key_cols: &[String], label: String) -> Result<Self> {
        let mut merge = KWayMerge::new(&runs, key_cols)?;
        let current = merge.next()?;
        Ok(Self {
            merge,
            current,
            key_cols: key_cols.to_vec(),
            label,
        })
    }

    pub fn peek(&self) -> Option<&Row> {
        self.current.as_ref()
    }

    pub fn advance(&mut self) -> Result<()> {
        let next = self.merge.next()?;
        if let (Some(cur), Some(nxt)) = (&self.current, &next) {
            if cmp_keys(&cur.key, &nxt.key) == Ordering::Equal {
                let rendered = self
                    .key_cols
                    .iter()
                    .zip(&cur.key)
                    .map(|(c, v)| format!("{c}={}", v.render()))
                    .collect::<Vec<_>>()
                    .join(", ");
                bail!(
                    "key ({}) is not unique in {}: e.g. {}",
                    self.key_cols.join(", "),
                    self.label,
                    rendered
                );
            }
        }
        self.current = next;
        Ok(())
    }
}
