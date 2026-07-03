use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, ValueEnum};
use tabdiff::DiffConfig;

/// Semantic diff for CSV and Parquet tables.
#[derive(Parser)]
#[command(name = "tabdiff", version)]
struct Cli {
    /// Old/left table (.csv or .parquet)
    left: PathBuf,
    /// New/right table (.csv or .parquet)
    right: PathBuf,
    /// Key column(s) to match rows on, comma-separated; inferred when omitted
    #[arg(short, long, value_delimiter = ',')]
    key: Option<Vec<String>>,
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
    /// Output format
    #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
    format: OutputFormat,
}

#[derive(Clone, Copy, ValueEnum)]
enum OutputFormat {
    Human,
    Json,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let cfg = DiffConfig {
        left: cli.left,
        right: cli.right,
        key: cli.key,
        tol_abs: cli.tol_abs,
        tol_rel: cli.tol_rel,
        fail_fast: cli.fail_fast,
        max_samples: cli.samples,
    };
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
