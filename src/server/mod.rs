use crate::authentication::{parse_connection_string, AuthManager};
use crate::engine::Database;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

/// Run the server.
pub async fn run(bind: &str, db_path: &str) -> std::io::Result<()> {
    let db = Database::open(db_path).expect("Failed to open database");
    let auth_manager =
        AuthManager::load(std::path::Path::new(db_path)).expect("Failed to load auth manager");
    let auth_enabled = auth_manager.has_users();

    if auth_enabled {
        tracing::info!("Arcane listening on {} (authentication enabled)", bind);
    } else {
        tracing::info!("Arcane listening on {} (authentication disabled)", bind);
    }

    let listener = TcpListener::bind(bind).await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        tracing::debug!("Connection from {}", addr);
        let db = db.clone();
        let auth_manager =
            AuthManager::load(std::path::Path::new(db_path)).expect("Failed to load auth manager");

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, db, auth_manager).await {
                tracing::warn!("Connection error: {}", e);
            }
        });
    }
}

/// Handle some given connection asynchronously.
async fn handle_connection(
    mut stream: TcpStream,
    db: Arc<Database>,
    auth_manager: AuthManager,
) -> std::io::Result<()> {
    let (reader, mut writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let mut authenticated = !auth_manager.has_users();

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
                            let db_path_string = db.dir.to_string_lossy().to_string();
                            let db_path_ref = std::path::Path::new(&db_path_string);

                            if let Err(e) =
                                crate::authentication::decrypt_database(db_path_ref, &password)
                            {
                                writer
                                    .write_all(
                                        format!("ERR Failed to decrypt database: {}\n", e)
                                            .as_bytes(),
                                    )
                                    .await?;
                                writer.flush().await?;
                                continue;
                            }

                            authenticated = true;

                            if let Err(e) = db.set_encryption_key(password.clone()) {
                                writer
                                    .write_all(
                                        format!("ERR Failed to set encryption key: {}\n", e)
                                            .as_bytes(),
                                    )
                                    .await?;
                                writer.flush().await?;
                                continue;
                            }

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

        match db.execute(trimmed) {
            Ok(result) => {
                writer
                    .write_all(format!("OK\n{}END\n", result).as_bytes())
                    .await?;
            }
            Err(e) => {
                writer.write_all(format!("ERR {}\n", e).as_bytes()).await?;
            }
        }
        writer.flush().await?;
    }

    Ok(())
}
