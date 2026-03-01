//! Encryption module for ArcaneDB
//!
//! Provides AES-256-GCM encryption/decryption for data at rest.

use crate::error::{ArcaneError, Result};
use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use argon2::Argon2;
use rand::RngCore;

const NONCE_SIZE: usize = 12;
const SALT_SIZE: usize = 16;

/// Derive encryption key from password using Argon2
fn derive_key(password: &str, salt: &[u8]) -> Result<[u8; 32]> {
    let mut key = [0u8; 32];
    Argon2::default()
        .hash_password_into(password.as_bytes(), salt, &mut key)
        .map_err(|e| ArcaneError::Other(format!("Key derivation failed: {}", e)))?;
    Ok(key)
}

/// Encrypt data with AES-256-GCM
pub fn encrypt(data: &[u8], password: &str) -> Result<Vec<u8>> {
    let mut salt = [0u8; SALT_SIZE];
    OsRng.fill_bytes(&mut salt);

    let key = derive_key(password, &salt)?;
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|e| ArcaneError::Other(format!("Cipher creation failed: {}", e)))?;
    let mut nonce_bytes = [0u8; NONCE_SIZE];
    OsRng.fill_bytes(&mut nonce_bytes);

    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, data)
        .map_err(|e| ArcaneError::Other(format!("Encryption failed: {}", e)))?;
    let mut result = Vec::with_capacity(SALT_SIZE + NONCE_SIZE + ciphertext.len());

    result.extend_from_slice(&salt);
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);

    Ok(result)
}

/// Decrypt data with AES-256-GCM
pub fn decrypt(encrypted: &[u8], password: &str) -> Result<Vec<u8>> {
    if encrypted.len() < SALT_SIZE + NONCE_SIZE {
        return Err(ArcaneError::Other("Invalid encrypted data".to_string()));
    }

    let salt = &encrypted[..SALT_SIZE];
    let nonce_bytes = &encrypted[SALT_SIZE..SALT_SIZE + NONCE_SIZE];
    let ciphertext = &encrypted[SALT_SIZE + NONCE_SIZE..];
    let key = derive_key(password, salt)?;
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|e| ArcaneError::Other(format!("Cipher creation failed: {}", e)))?;
    let nonce = Nonce::from_slice(nonce_bytes);
    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| ArcaneError::Other(format!("Decryption failed: {}", e)))?;

    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt() {
        let data = b"Hello, ArcaneDB!";
        let password = "supersecret";

        let encrypted = encrypt(data, password).unwrap();
        assert_ne!(encrypted.as_slice(), data);

        let decrypted = decrypt(&encrypted, password).unwrap();
        assert_eq!(decrypted.as_slice(), data);
    }

    #[test]
    fn test_wrong_password() {
        let data = b"Secret data";
        let encrypted = encrypt(data, "correct").unwrap();
        assert!(decrypt(&encrypted, "wrong").is_err());
    }
}
