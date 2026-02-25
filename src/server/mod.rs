use crate::engine::Database;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

pub async fn run(bind: &str, db_path: &str) -> std::io::Result<()> {
    let db = Database::open(db_path).expect("Failed to open database");
    let listener = TcpListener::bind(bind).await?;
    tracing::info!("Arcane server listening on {}", bind);

    loop {
        let (stream, addr) = listener.accept().await?;
        tracing::debug!("Connection from {}", addr);
        let db = db.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, db).await {
                tracing::warn!("Connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream, db: Arc<Database>) -> std::io::Result<()> {
    let (reader, mut writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match db.execute(trimmed) {
            Ok(result) => {
                let msg = format!("OK\n{}END\n", result);
                writer.write_all(msg.as_bytes()).await?;
            }
            Err(e) => {
                let msg = format!("ERR {}\n", e);
                writer.write_all(msg.as_bytes()).await?;
            }
        }
        writer.flush().await?;
    }
    Ok(())
}
