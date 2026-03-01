//! Database file encryption utilities
//!
//! Handles encryption/decryption of entire database files when authentication is enabled.

use super::crypto;
use crate::error::{ArcaneError, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

const ENCRYPTED_MARKER: &[u8] = b"ARCENC01";
const PLAIN_MARKER: &[u8] = b"ARCANE01";

/// Check if a file is encrypted by reading its magic bytes
pub fn is_file_encrypted(path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }

    let mut file = File::open(path)?;
    let mut magic = [0u8; 8];

    if file.read_exact(&mut magic).is_err() {
        return Ok(false);
    }

    Ok(&magic == ENCRYPTED_MARKER)
}

/// Encrypt a database file in place
pub fn encrypt_file(path: &Path, password: &str) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let mut file = File::open(path)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;
    drop(file);

    if data.is_empty() {
        tracing::debug!("File {:?} is empty, skipping encryption", path);
        return Ok(());
    }

    let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    if filename.ends_with(".idx") && data.len() < 16 {
        tracing::debug!(
            "Index file {:?} too small ({} bytes), skipping encryption",
            path,
            data.len()
        );
        return Ok(());
    }

    if data.len() == 8 && &data[0..8] == b"ARCWAL01" {
        tracing::debug!(
            "WAL file {:?} only contains header, skipping encryption",
            path
        );
        return Ok(());
    }

    if data.len() >= 8 {
        if &data[0..8] != PLAIN_MARKER && &data[0..8] != b"ARCWAL01" {
            tracing::debug!("File {:?} appears already encrypted, skipping", path);
            return Ok(());
        }
    }

    if data.len() >= 8 {
        if &data[0..8] == PLAIN_MARKER {
            data[0..8].copy_from_slice(ENCRYPTED_MARKER);
        } else if &data[0..8] == b"ARCWAL01" {
            data[0..8].copy_from_slice(b"ARCWALE1");
        }
    }

    let encrypted = crypto::encrypt(&data, password)?;
    let mut file = OpenOptions::new().write(true).truncate(true).open(path)?;

    file.write_all(&encrypted)?;
    file.flush()?;

    Ok(())
}

/// Decrypt a database file in place
pub fn decrypt_file(path: &Path, password: &str) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let mut file = File::open(path)?;
    let mut encrypted_data = Vec::new();
    file.read_to_end(&mut encrypted_data)?;
    drop(file);

    if encrypted_data.is_empty() {
        return Ok(());
    }

    let decrypted = crypto::decrypt(&encrypted_data, password)?;
    let mut data = decrypted;

    if data.len() >= 8 {
        if &data[0..8] == ENCRYPTED_MARKER {
            data[0..8].copy_from_slice(PLAIN_MARKER);
        } else if &data[0..8] == b"ARCWALE1" {
            data[0..8].copy_from_slice(b"ARCWAL01");
        }
    }

    let mut file = OpenOptions::new().write(true).truncate(true).open(path)?;
    file.write_all(&data)?;
    file.flush()?;

    Ok(())
}

/// Encrypt all database files in a directory
pub fn encrypt_database(db_path: &Path, password: &str) -> Result<()> {
    for entry in fs::read_dir(db_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

            if filename.ends_with(".arc") || filename == "arcane.wal" {
                tracing::debug!("Encrypting file: {:?}", path);
                encrypt_file(&path, password)?;
            }
        }
    }

    Ok(())
}

/// Decrypt all database files in a directory
pub fn decrypt_database(db_path: &Path, password: &str) -> Result<()> {
    for entry in fs::read_dir(db_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

            if filename.ends_with(".arc") || filename == "arcane.wal" {
                let mut file = File::open(&path)?;
                let mut file_data = Vec::new();
                file.read_to_end(&mut file_data)?;
                drop(file);

                if file_data.is_empty() {
                    tracing::debug!("File {:?} is empty, skipping", path);
                    continue;
                }

                let is_plain = if file_data.len() >= 8 {
                    &file_data[0..8] == PLAIN_MARKER || &file_data[0..8] == b"ARCWAL01"
                } else {
                    false
                };

                if is_plain {
                    tracing::debug!("File {:?} is already decrypted, skipping", path);
                    continue;
                }

                let looks_encrypted = file_data.len() >= 45;

                if !looks_encrypted {
                    tracing::debug!(
                        "File {:?} too small to be encrypted ({} bytes), skipping",
                        path,
                        file_data.len()
                    );
                    continue;
                }

                tracing::debug!(
                    "Attempting to decrypt file: {:?} ({} bytes)",
                    path,
                    file_data.len()
                );
                if let Err(e) = decrypt_file(&path, password) {
                    return Err(ArcaneError::Other(format!(
                        "Failed to decrypt {:?}: {}.\nThis usually means one of the following:\n  \
                        - Wrong password (you entered a different password than was used to encrypt)\n  \
                        - File was encrypted in a previous session with a different password\n  \
                        - File is corrupted\n\n\
                        To fix: Delete the encrypted files and recreate the database, or use the correct password.",
                        path, e
                    )));
                }
            }
        }
    }

    Ok(())
}

/// Check if database directory has encrypted files
pub fn is_database_encrypted(db_path: &Path) -> Result<bool> {
    for entry in fs::read_dir(db_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

            if filename.ends_with(".arc") || filename == "arcane.wal" {
                let mut file = File::open(&path)?;
                let mut magic = [0u8; 8];

                if file.read_exact(&mut magic).is_ok() {
                    if &magic != PLAIN_MARKER {
                        return Ok(true);
                    }
                }
            }
        }
    }

    Ok(false)
}
