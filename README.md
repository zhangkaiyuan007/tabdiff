# tabdiff

Semantic diff for CSV and Parquet tables. Single binary, type-aware, CI-friendly.

Unlike text-based `diff`, tabdiff matches rows by key and compares **values**, not bytes:
`1.0` equals `1.00`, floats can have tolerances, column order doesn't matter, and you
can diff a CSV against a Parquet file directly. Renamed columns are detected by content
similarity and keep participating in the row diff. Tables without a unique key (logs,
event streams, transaction exports) are compared as row multisets instead — duplicates
included, automatically.

> **Status**: early development. The streaming core (external sort + k-way merge,
> arrow-row byte-comparable keys) bounds memory by `--memory-mb` regardless of input
> size. Informal benchmark, two 2M-row / 54 MB CSVs at an 8 MB sort budget: keyed diff
> ~1.0 s / 29 MB peak RSS, `--assume-sorted` ~0.7 s, keyless ~4.4 s.

## Usage

```console
$ tabdiff old.csv new.parquet
Schema
  + email (Utf8)
  - legacy (Utf8)
  ~ amount → amt (renamed, 97% content match)
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
| `--assume-sorted` | inputs already sorted by `--key`: skip sorting entirely, verify order on the fly |
| `--spill-dir DIR` | where spill files go (default: system temp dir) |

Exit codes follow `diff`/`cmp` convention: `0` no differences, `1` differences found, `2` error.

## Why not …

- **`diff`/`git diff`** — text-based: row order, float formatting, and column order all produce noise.
- **datacompy** — a pandas library, not a CLI; requires writing Python for every comparison.
- **daff** — JavaScript; struggles with large files.
- **data-diff** — archived by its vendor in 2024.

## Roadmap

See [docs/MVP-requirements.md](docs/MVP-requirements.md). Highlights: git diff driver
for Parquet, `--where` row filtering, Python bindings.
Performance track: keyless-mode throughput (hash-sort currently shuffles whole rows).

Cross-type keys unify automatically: an `Int64` id on one side matches a `Float64` id
on the other, and e.g. UUID-as-binary meets UUID-as-text at Utf8.

## Development

```console
cargo test
cargo run -- testdata/left.csv testdata/right.csv
```

## License

MIT OR Apache-2.0
