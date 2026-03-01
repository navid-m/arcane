//! Encryption key management for Arcane.
//!
//! Manages encryption keys derived from user passwords.

use crate::authentication::{crypto, AuthManager};
use crate::error::{ArcaneError, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::RwLock;

/// Manages encryption for database files
pub struct EncryptionManager {
    /// Derived encryption key from authenticated user's password
    encryption_key: RwLock<Option<String>>,

    /// Path to database directory
    db_path: std::path::PathBuf,
}

impl EncryptionManager {
    pub fn new(db_path: &Path) -> Self {
        EncryptionManager {
            encryption_key: RwLock::new(None),
            db_path: db_path.to_path_buf(),
        }
    }

    /// Set encryption key after successful authentication
    pub fn set_key(&self, password: String) -> Result<()> {
        let mut key = self
            .encryption_key
            .write()
            .map_err(|_| ArcaneError::LockPoisoned)?;
        *key = Some(password);
        Ok(())
    }

    /// Check if encryption is enabled (i.e., if users exist)
    pub fn is_enabled(&self) -> bool {
        AuthManager::load(&self.db_path)
            .map(|auth| auth.has_users())
            .unwrap_or(false)
    }

    /// Get the current encryption key
    pub fn get_key(&self) -> Result<Option<String>> {
        let key = self
            .encryption_key
            .read()
            .map_err(|_| ArcaneError::LockPoisoned)?;
        Ok(key.clone())
    }

    /// Encrypt data if encryption is enabled
    pub fn encrypt_if_enabled(&self, data: &[u8]) -> Result<Vec<u8>> {
        if !self.is_enabled() {
            return Ok(data.to_vec());
        }

        let key = self
            .encryption_key
            .read()
            .map_err(|_| ArcaneError::LockPoisoned)?;
        match key.as_ref() {
            Some(password) => crypto::encrypt(data, password),
            None => Err(ArcaneError::Other(
                "Encryption enabled but no key set (not authenticated)".to_string(),
            )),
        }
    }

    /// Decrypt data if encryption is enabled
    pub fn decrypt_if_enabled(&self, data: &[u8]) -> Result<Vec<u8>> {
        if !self.is_enabled() {
            return Ok(data.to_vec());
        }

        let key = self
            .encryption_key
            .read()
            .map_err(|_| ArcaneError::LockPoisoned)?;
        match key.as_ref() {
            Some(password) => crypto::decrypt(data, password),
            None => Err(ArcaneError::Other(
                "Encryption enabled but no key set (not authenticated)".to_string(),
            )),
        }
    }

    /// Encrypt a file in place
    pub fn encrypt_file(&self, file_path: &Path) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        let mut file = File::open(file_path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let encrypted = self.encrypt_if_enabled(&data)?;

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(file_path)?;
        file.write_all(&encrypted)?;
        file.flush()?;

        Ok(())
    }

    /// Decrypt a file in place
    pub fn decrypt_file(&self, file_path: &Path) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        let mut file = File::open(file_path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let decrypted = self.decrypt_if_enabled(&data)?;

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(file_path)?;
        file.write_all(&decrypted)?;
        file.flush()?;

        Ok(())
    }
}
