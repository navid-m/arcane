//! Authentication module for ArcaneDB
//!
//! Manages user credentials and authentication state.

use crate::error::{ArcaneError, Result};
use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub password_hash: String,
}

#[derive(Debug)]
pub struct AuthManager {
    users: HashMap<String, User>,
    auth_file: std::path::PathBuf,
}

impl AuthManager {
    /// Load or create auth manager from directory
    pub fn load(dir: &Path) -> Result<Self> {
        let auth_file = dir.join("arcane.auth");
        let users = if auth_file.exists() {
            let mut file = File::open(&auth_file)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            serde_json::from_str(&contents).unwrap_or_default()
        } else {
            HashMap::new()
        };

        Ok(AuthManager { users, auth_file })
    }

    /// Save users to disk
    fn save(&self) -> Result<()> {
        let json = serde_json::to_string_pretty(&self.users)
            .map_err(|e| ArcaneError::Other(format!("Failed to serialize users: {}", e)))?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.auth_file)?;
        file.write_all(json.as_bytes())?;
        file.flush()?;
        Ok(())
    }

    /// Add a new user
    pub fn add_user(&mut self, username: String, password: &str) -> Result<()> {
        if self.users.contains_key(&username) {
            return Err(ArcaneError::Other(format!(
                "User '{}' already exists",
                username
            )));
        }

        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let password_hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| ArcaneError::Other(format!("Failed to hash password: {}", e)))?
            .to_string();

        self.users.insert(
            username.clone(),
            User {
                username,
                password_hash,
            },
        );
        self.save()?;
        Ok(())
    }

    /// Verify user credentials
    pub fn verify(&self, username: &str, password: &str) -> Result<bool> {
        let user = self
            .users
            .get(username)
            .ok_or_else(|| ArcaneError::Other(format!("User '{}' not found", username)))?;

        let parsed_hash = PasswordHash::new(&user.password_hash)
            .map_err(|e| ArcaneError::Other(format!("Invalid password hash: {}", e)))?;

        Ok(Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok())
    }

    /// Check if any users exist
    pub fn has_users(&self) -> bool {
        !self.users.is_empty()
    }

    /// Remove a user
    pub fn remove_user(&mut self, username: &str) -> Result<()> {
        self.users
            .remove(username)
            .ok_or_else(|| ArcaneError::Other(format!("User '{}' not found", username)))?;
        self.save()?;
        Ok(())
    }

    /// List all usernames
    pub fn list_users(&self) -> Vec<String> {
        self.users.keys().cloned().collect()
    }
}

/// Parse connection string: arcane://username;password
pub fn parse_connection_string(conn_str: &str) -> Result<(String, String)> {
    if !conn_str.starts_with("arcane://") {
        return Err(ArcaneError::Other(
            "Invalid connection string format. Expected: arcane://username;password".to_string(),
        ));
    }

    let credentials = &conn_str[9..];
    let parts: Vec<&str> = credentials.split(';').collect();

    if parts.len() != 2 {
        return Err(ArcaneError::Other(
            "Invalid connection string format. Expected: arcane://username;password".to_string(),
        ));
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_parse_connection_string() {
        let (user, pass) = parse_connection_string("arcane://admin;secret123").unwrap();
        assert_eq!(user, "admin");
        assert_eq!(pass, "secret123");
    }

    #[test]
    fn test_auth_manager() {
        let dir = TempDir::new().unwrap();
        let mut auth = AuthManager::load(dir.path()).unwrap();

        auth.add_user("alice".to_string(), "password123").unwrap();
        assert!(auth.verify("alice", "password123").unwrap());
        assert!(!auth.verify("alice", "wrongpass").unwrap());
    }
}
