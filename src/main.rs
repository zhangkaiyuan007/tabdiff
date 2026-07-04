use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, ValueEnum};
use tabdiff::DiffConfig;
use tabdiff::input::{FileFormat, detect_format};

/// Semantic diff for CSV and Parquet tables.
#[derive(Parser)]
#[command(name = "tabdiff", version)]
struct Cli {
    /// Old/left table (.csv or .parquet)
    #[arg(required_unless_present = "git")]
    left: Option<PathBuf>,
    /// New/right table (.csv or .parquet)
    #[arg(required_unless_present = "git")]
    right: Option<PathBuf>,
    /// Key column(s) to match rows on, comma-separated; inferred when omitted
    #[arg(short, long, value_delimiter = ',')]
    key: Option<Vec<String>>,
    /// Match rows by whole-row content instead of a key (edits appear as -/+)
    #[arg(long, conflicts_with_all = ["key", "tol_abs", "tol_rel"])]
    keyless: bool,
    /// Absolute tolerance when comparing floats
    #[arg(long)]
    tol_abs: Option<f64>,
    /// Relative tolerance when comparing floats
    #[arg(long)]
    tol_rel: Option<f64>,
    /// Stop scanning after N row differences
    #[arg(long, value_name = "N")]
    fail_fast: Option<usize>,
    /// Max example rows shown per category
    #[arg(long, default_value_t = 10)]
    samples: usize,
    /// Sort-buffer memory budget in MB before spilling to disk
    #[arg(long, default_value_t = 256)]
    memory_mb: usize,
    /// Inputs are already sorted by --key: skip sorting, verify order on the fly
    #[arg(long, requires = "key", conflicts_with = "keyless")]
    assume_sorted: bool,
    /// Directory for spill files (default: system temp dir)
    #[arg(long, value_name = "DIR")]
    spill_dir: Option<PathBuf>,
    /// Force input format for both sides (default: by file extension)
    #[arg(long, value_enum, value_name = "FORMAT")]
    input_format: Option<InputFormat>,
    /// Run as a git diff driver: expects git's 7 args
    /// (path old-file old-hex old-mode new-file new-hex new-mode).
    /// Set up with: git config diff.tabdiff.command "tabdiff --git"
    /// and `*.parquet diff=tabdiff` in .gitattributes.
    #[arg(long, num_args = 7, value_name = "GIT_ARG", allow_hyphen_values = true)]
    git: Option<Vec<String>>,
    /// Output format
    #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
    format: OutputFormat,
}

#[derive(Clone, Copy, ValueEnum)]
enum OutputFormat {
    Human,
    Json,
}

#[derive(Clone, Copy, ValueEnum)]
enum InputFormat {
    Csv,
    Parquet,
}

impl From<InputFormat> for FileFormat {
    fn from(f: InputFormat) -> Self {
        match f {
            InputFormat::Csv => FileFormat::Csv,
            InputFormat::Parquet => FileFormat::Parquet,
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Some(git_args) = cli.git.clone() {
        return run_git_mode(&cli, &git_args);
    }
    let cfg = config(&cli);
    match tabdiff::run_diff(&cfg) {
        Ok(report) => {
            match cli.format {
                OutputFormat::Human => {
                    print!("{}", report.render_human(std::io::stdout().is_terminal()))
                }
                OutputFormat::Json => println!("{}", report.to_json()),
            }
            if report.has_differences() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(e) => {
            eprintln!("tabdiff: error: {e:#}");
            ExitCode::from(2)
        }
    }
}

fn config(cli: &Cli) -> DiffConfig {
    DiffConfig {
        left: cli.left.clone().unwrap_or_default(),
        right: cli.right.clone().unwrap_or_default(),
        key: cli.key.clone(),
        tol_abs: cli.tol_abs,
        tol_rel: cli.tol_rel,
        fail_fast: cli.fail_fast,
        max_samples: cli.samples,
        memory_mb: cli.memory_mb,
        keyless: cli.keyless,
        assume_sorted: cli.assume_sorted,
        spill_dir: cli.spill_dir.clone(),
        input_format: cli.input_format.map(Into::into),
    }
}

/// Git invokes the driver with `path old-file old-hex old-mode new-file
/// new-hex new-mode`; temp files carry no extension, so the format comes
/// from the repo path. Git aborts the whole diff on a non-zero exit, so
/// this mode always exits 0 and reports problems inline.
fn run_git_mode(cli: &Cli, args: &[String]) -> ExitCode {
    let (path, old, new) = (&args[0], &args[1], &args[4]);
    let color = std::io::stdout().is_terminal();
    let bold = if color { "\x1b[1m" } else { "" };
    let reset = if color { "\x1b[0m" } else { "" };
    println!("{bold}tabdiff {path}{reset}");

    let is_absent = |p: &str| p == "/dev/null" || p == "nul";
    if is_absent(old) || is_absent(new) {
        println!("({} file)", if is_absent(old) { "new" } else { "deleted" });
        return ExitCode::SUCCESS;
    }
    let format = match detect_format(Path::new(path)) {
        Ok(f) => f,
        Err(e) => {
            println!("(skipped: {e})");
            return ExitCode::SUCCESS;
        }
    };
    let mut cfg = config(cli);
    cfg.left = PathBuf::from(old);
    cfg.right = PathBuf::from(new);
    cfg.input_format = Some(format);
    match tabdiff::run_diff(&cfg) {
        Ok(report) => print!("{}", report.render_human(color)),
        Err(e) => println!("(tabdiff error: {e:#})"),
    }
    ExitCode::SUCCESS
}
