use arcane::engine::Database;
use clap::{Parser, Subcommand};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "arcane-cli", about = "ArcaneDB Commandline Interface")]
struct Args {
    /// Database directory
    #[arg(short, long, default_value = "./arcane_data")]
    data: String,

    /// Command to execute
    #[command(subcommand)]
    command: Option<Cmd>,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Execute an .arc script file
    Run {
        /// Path to the .arc script
        file: PathBuf,
    },

    /// Start an interactive REPL
    Repl,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("warn"))
        .init();

    let args = Args::parse();
    let db = Database::open(&args.data).expect("Failed to open database");

    match args.command.unwrap_or(Cmd::Repl) {
        Cmd::Run { file } => {
            if file.extension().map_or(false, |e| e != "arc") {
                eprintln!("Warning: expected .arc file extension");
            }
            let src = std::fs::read_to_string(&file).unwrap_or_else(|e| {
                eprintln!("Cannot read file: {}", e);
                std::process::exit(1)
            });
            let results = db.execute_script(&src);
            for result in results {
                match result {
                    Ok(r) => print!("{}", r),
                    Err(e) => eprintln!("Error: {}", e),
                }
            }
        }
        Cmd::Repl => {
            repl(db);
        }
    }
}

fn repl(db: Arc<Database>) {
    println!("Arcane DBMS v0.1.0 — type AQL statements, one per line. Ctrl-D to exit.");
    println!("Data directory: connected");
    println!();

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let line = line.trim().to_string();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match db.execute(&line) {
            Ok(result) => print!("{}", result),
            Err(e) => eprintln!("! {}", e),
        }
        stdout.flush().ok();
    }
}
