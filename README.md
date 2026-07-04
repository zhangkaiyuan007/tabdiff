# tabdiff

Semantic diff for CSV and Parquet tables. Single binary, type-aware, CI-friendly.

![tabdiff demo](assets/demo.gif)

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

## Install

Prebuilt binaries and a PyPI package are planned. For now, build from source
(needs a recent stable Rust):

```console
$ cargo install --path .
```

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
| `--where "region = 'EU' AND amount >= 100"` | filter rows on both sides before diffing (`= != < <= > >=`, `AND`) |
| `--input-format csv\|parquet` | force the format instead of trusting file extensions |

Exit codes follow `diff`/`cmp` convention: `0` no differences, `1` differences found, `2` error.

Keys with different types on each side unify automatically: an `Int64` id matches a
`Float64` id, and UUID-as-binary meets UUID-as-text at Utf8.

## git integration

Make `git diff` render semantic table diffs for CSV/Parquet files tracked in a repo:

```console
$ git config --global diff.tabdiff.command "tabdiff --git"
$ echo '*.parquet diff=tabdiff' >> .gitattributes
$ echo '*.csv diff=tabdiff' >> .gitattributes
$ git diff data.csv
tabdiff data.csv
Schema
  identical
Key: id (inferred)
...
```

All regular flags combine with `--git`, e.g. `tabdiff --key id --tol-rel 1e-9 --git`.

## Why not …

- **`diff`/`git diff`** — text-based: row order, float formatting, and column order all produce noise.
- **datacompy** — a pandas library, not a CLI; requires writing Python for every comparison.
- **daff** — JavaScript; struggles with large files.
- **data-diff** — archived by its vendor in 2024.

## Roadmap

- Prebuilt binaries and a PyPI package
- S3 and stdin inputs
- Sampling mode: estimate the difference rate of huge tables quickly
- Keyless-mode throughput (the hash sort currently shuffles whole rows)
- Database sources through an embedded engine (DuckDB), keeping the single binary

## Python

The `python/` crate exposes the same engine as a Python module (built with maturin,
PyPI release pending). The report comes back as a dict — the shape pytest wants:

```python
import tabdiff

report = tabdiff.diff("expected.parquet", "actual.parquet",
                      key=["id"], tol_rel=1e-9, where="region = 'EU'")
assert not report["has_differences"], report["samples"]
```

## Development

```console
cargo test
cargo run -- testdata/left.csv testdata/right.csv
```

## License

Licensed under either of the [Apache License 2.0](LICENSE-APACHE) or the
[MIT License](LICENSE-MIT), at your option.
