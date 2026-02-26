/// Show the version of Arcane.
pub fn show_version() {
    println!(
        "ArcaneDB v{} - Copyright (C) Navid M 2026",
        option_env!("CARGO_PKG_VERSION").unwrap_or("Unknown")
    );
}
