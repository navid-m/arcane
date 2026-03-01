//! Arcane User Management CLI
//!
//! Tool for managing database users and authentication.

use arcane::auth::AuthManager;
use clap::{Parser, Subcommand};
use std::io::{self, Write};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "arcane-user", about = "ArcaneDB User Management")]
struct Args {
    /// Database directory
    #[arg(short, long, default_value = "./arcane_data")]
    data: String,

    /// The user management command
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Add a new user
    Add {
        /// Username
        username: String,
    },

    /// Remove a user
    Remove {
        /// Username
        username: String,
    },

    /// List all users
    List,
}

fn main() {
    let args = Args::parse();
    let db_path = PathBuf::from(&args.data);

    std::fs::create_dir_all(&db_path).expect("Failed to create database directory");

    let mut auth_manager = AuthManager::load(&db_path).expect("Failed to load auth manager");

    match args.command {
        Command::Add { username } => {
            print!("Enter password for '{}': ", username);
            io::stdout().flush().unwrap();
            let password = rpassword::read_password().expect("Failed to read password");

            if password.is_empty() {
                eprintln!("Error: Password cannot be empty");
                std::process::exit(1);
            }

            match auth_manager.add_user(username.clone(), &password) {
                Ok(_) => {
                    println!("User '{}' added successfully", username);
                    println!("\nAuthentication is now enabled for this database.");
                    println!("Clients must connect using: arcane://{};password", username);
                }
                Err(e) => {
                    eprintln!("Error adding user: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Command::Remove { username } => match auth_manager.remove_user(&username) {
            Ok(_) => {
                println!("User '{}' removed successfully", username);
                if !auth_manager.has_users() {
                    println!("\nNo users remaining. Authentication is now disabled.");
                }
            }
            Err(e) => {
                eprintln!("Error removing user: {}", e);
                std::process::exit(1);
            }
        },
        Command::List => {
            let users = auth_manager.list_users();
            if users.is_empty() {
                println!("No users configured. Authentication is disabled.");
            } else {
                println!("Configured users:");
                for user in users {
                    println!("  - {}", user);
                }
                println!("\nAuthentication is enabled.");
            }
        }
    }
}
