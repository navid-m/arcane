use crate::authentication::{decrypt_database, parse_connection_string, AuthManager};
use crate::engine::Database;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

/// Run the server.
pub async fn run(bind: &str, db_path: &str) -> std::io::Result<()> {
    let auth_manager = AuthManager::load(Path::new(db_path)).expect("Failed to load auth manager");
    let auth_enabled = auth_manager.has_users();

    let db_opt = if !auth_enabled {
        Some(Database::open(db_path).expect("Failed to open database"))
    } else {
        None
    };

    if auth_enabled {
        tracing::info!(
            "Arcane listening on {} (authentication enabled - database will open after first authentication)",
            bind
        );
    } else {
        tracing::info!("Arcane listening on {} (authentication disabled)", bind);
    }

    let listener = TcpListener::bind(bind).await?;
    let db_path_arc = Arc::new(db_path.to_string());

    loop {
        let (stream, addr) = listener.accept().await?;
        tracing::debug!("Connection from {}", addr);
        let db_clone = db_opt.clone();
        let db_path_clone = db_path_arc.clone();
        let auth_manager =
            AuthManager::load(Path::new(db_path)).expect("Failed to load auth manager");

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, db_clone, db_path_clone, auth_manager).await {
                tracing::warn!("Connection error: {}", e);
            }
        });
    }
}

/// Handle some given connection asynchronously.
async fn handle_connection(
    mut stream: TcpStream,
    db_opt: Option<Arc<Database>>,
    db_path: Arc<String>,
    auth_manager: AuthManager,
) -> std::io::Result<()> {
    let (reader, mut writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let mut authenticated = !auth_manager.has_users();
    let mut db = db_opt;

    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if !authenticated {
            if trimmed.starts_with("arcane://") {
                match parse_connection_string(trimmed) {
                    Ok((username, password)) => match auth_manager.verify(&username, &password) {
                        Ok(true) => {
                            let db_path_ref = Path::new(db_path.as_str());
                            if let Err(e) = decrypt_database(db_path_ref, &password) {
                                writer
                                    .write_all(
                                        format!("ERR Failed to decrypt database: {}\n", e)
                                            .as_bytes(),
                                    )
                                    .await?;
                                writer.flush().await?;
                                continue;
                            }
                            let database = match Database::open(db_path_ref) {
                                Ok(d) => d,
                                Err(e) => {
                                    writer
                                        .write_all(
                                            format!("ERR Failed to open database: {}\n", e)
                                                .as_bytes(),
                                        )
                                        .await?;
                                    writer.flush().await?;
                                    continue;
                                }
                            };

                            if let Err(e) = database.set_encryption_key(password.clone()) {
                                writer
                                    .write_all(
                                        format!("ERR Failed to set encryption key: {}\n", e)
                                            .as_bytes(),
                                    )
                                    .await?;
                                writer.flush().await?;
                                continue;
                            }

                            db = Some(database);
                            authenticated = true;

                            writer
                                .write_all(b"OK\nAuthenticated successfully\nEND\n")
                                .await?;
                        }
                        Ok(false) => {
                            writer
                                .write_all(b"ERR Authentication failed: Invalid credentials\n")
                                .await?;
                        }
                        Err(e) => {
                            writer
                                .write_all(format!("ERR Authentication error: {}\n", e).as_bytes())
                                .await?;
                        }
                    },
                    Err(e) => {
                        writer.write_all(format!("ERR {}\n", e).as_bytes()).await?;
                    }
                }
            } else {
                writer
                    .write_all(b"ERR Authentication required. Send connection string: arcane://username;password\n")
                    .await?;
            }
            writer.flush().await?;
            continue;
        }

        if let Some(ref database) = db {
            match database.execute(trimmed) {
                Ok(result) => {
                    writer
                        .write_all(format!("OK\n{}END\n", result).as_bytes())
                        .await?;
                }
                Err(e) => {
                    writer.write_all(format!("ERR {}\n", e).as_bytes()).await?;
                }
            }
        } else {
            writer.write_all(b"ERR Database not initialized\n").await?;
        }
        writer.flush().await?;
    }

    Ok(())
}
