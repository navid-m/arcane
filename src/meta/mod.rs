//! Meta version information module.
//!
//! Provides functionality to retrieve and display the version of ArcaneDB.

/// Show the version of Arcane.
pub fn show_version() {
    println!("ArcaneDB v{} - Copyright (C) Navid M 2026", get_version());
}

/// Get the version string of Arcane.
pub fn get_version() -> String {
    option_env!("CARGO_PKG_VERSION")
        .unwrap_or("-Unknown")
        .to_owned()
}
