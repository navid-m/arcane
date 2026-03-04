//! Arcane - Copyright (C) Navid Momtahen 2026
//!
//! License: GPL-3.0-only

use arcane::authentication::{decrypt_database, AuthManager};
use arcane::engine::Database;
use arcane::meta::{get_version, show_version};
use clap::{Parser, Subcommand};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "arcc", about = "ArcaneDB Commandline Interface")]
struct Args {
    /// Database directory
    #[arg(short, long, default_value = "./arcane_data")]
    data: String,

    /// Command to execute
    #[command(subcommand)]
    command: Option<Cmd>,

    /// Show the version
    #[arg(short, long, alias = "about")]
    version: bool,
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

    if args.version {
        show_version();
        return;
    }

    let db_path = Path::new(&args.data);
    let auth_manager = AuthManager::load(db_path).expect("Failed to load auth manager");
    let password_opt = if auth_manager.has_users() {
        println!("Authentication required for this database.");
        print!("Username: ");
        io::stdout().flush().unwrap();
        let mut username = String::new();
        io::stdin()
            .read_line(&mut username)
            .expect("Failed to read username");
        let username = username.trim();

        print!("Password: ");
        io::stdout().flush().unwrap();
        let password = rpassword::read_password().expect("Failed to read password");

        match auth_manager.verify(username, &password) {
            Ok(true) => {
                println!("Authentication successful.\n");

                if let Err(e) = decrypt_database(db_path, &password) {
                    eprintln!("Error: Failed to decrypt database: {}", e);
                    eprintln!("This may indicate:");
                    eprintln!("  - Wrong password");
                    eprintln!("  - Corrupted database files");
                    eprintln!("  - Database was encrypted with a different password");
                    std::process::exit(1);
                }

                Some(password)
            }
            Ok(false) => {
                eprintln!("Error: Authentication failed - invalid credentials");
                std::process::exit(1);
            }
            Err(e) => {
                eprintln!("Error: Authentication error: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    let db = Database::open(&args.data).expect("Failed to open database");

    if let Some(password) = password_opt {
        db.set_encryption_key(password)
            .expect("Failed to set encryption key");
    }

    match args.command.unwrap_or(Cmd::Repl) {
        Cmd::Run { file } => {
            if file.extension().map_or(false, |e| e != "aql") {
                eprintln!("Warning: expected .aql file extension");
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

            if let Err(e) = db.shutdown() {
                eprintln!("Warning: Failed to flush database: {}", e);
            }

            drop(db);
        }
        Cmd::Repl => {
            repl(db.clone());

            if let Err(e) = db.shutdown() {
                eprintln!("Warning: Failed to flush database: {}", e);
            }

            drop(db);
        }
    }
}

fn repl(db: Arc<Database>) {
    println!(
        "Arcane v{} — Type AQL statements, one per line. Ctrl-D to exit.",
        get_version()
    );
    println!("DDIR: Connected.\n");

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
