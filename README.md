# tabdiff

Semantic diff for CSV and Parquet tables. Single binary, type-aware, CI-friendly.

Unlike text-based `diff`, tabdiff matches rows by key and compares **values**, not bytes:
`1.0` equals `1.00`, floats can have tolerances, column order doesn't matter, and you
can diff a CSV against a Parquet file directly. Tables without a unique key (logs,
event streams, transaction exports) are compared as row multisets instead — duplicates
included, automatically.

> **Status**: early development. The streaming core (external sort + k-way merge) is in
> place: memory use is bounded by `--memory-mb` regardless of input size. Informal
> benchmark: two 2M-row / 54 MB CSVs diff in ~1.4 s with 68 MB peak RSS at an 8 MB
> sort budget.

## Usage

```console
$ tabdiff old.csv new.parquet
Schema
  + email (Utf8)
  - legacy (Utf8)
Key: id (inferred)
Rows: 5 → 5
  + 1 added
  - 1 removed
  ~ 2 modified  (amount: 1, status: 1)

~ id=2  amount: 20.5 → 20.50001
~ id=3  status: "closed" → "open"
+ id=6
- id=5
```

When no column uniquely identifies rows (or with `--keyless`), rows are matched by
whole-row content hash and compared as multisets — edits then appear as `- old / + new`:

```console
$ tabdiff old_events.csv new_events.csv
Schema
  identical
Key: none — keyless mode [auto: no unique key column found] (rows matched by content; edits appear as - old / + new)
Rows: 4 → 4
  + 2 added
  - 2 removed
  ~ 0 modified

+ ts=10:10, sensor=C, temp=18
+ ts=10:05, sensor=A, temp=21.7
- ts=10:00, sensor=A, temp=21.5
- ts=10:05, sensor=A, temp=21.6
```

Common flags:

| Flag | Meaning |
|---|---|
| `--key id` / `--key a,b` | match rows on these column(s); inferred when omitted |
| `--keyless` | force whole-row content matching (multiset diff) |
| `--tol-abs X` / `--tol-rel X` | float comparison tolerance |
| `--format json` | machine-readable report |
| `--fail-fast N` | stop after N row differences (fast CI gate) |
| `--samples N` | show up to N example rows per category (default 10) |
| `--memory-mb N` | sort-buffer budget before spilling to temp files (default 256) |

Exit codes follow `diff`/`cmp` convention: `0` no differences, `1` differences found, `2` error.

## Why not …

- **`diff`/`git diff`** — text-based: row order, float formatting, and column order all produce noise.
- **datacompy** — a pandas library, not a CLI; requires writing Python for every comparison.
- **daff** — JavaScript; struggles with large files.
- **data-diff** — archived by its vendor in 2024.

## Roadmap

See [docs/MVP-requirements.md](docs/MVP-requirements.md). Highlights: column rename
detection, git diff driver for Parquet, `--where` row filtering, Python bindings.
Performance track: byte-comparable key encoding (arrow-row), already-sorted input
detection, `--spill-dir` for choosing the spill location.

## Development

```console
cargo test
cargo run -- testdata/left.csv testdata/right.csv
```

## License

MIT OR Apache-2.0
