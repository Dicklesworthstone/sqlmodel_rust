//! SQLite driver for SQLModel Rust.
//!
//! `sqlmodel-sqlite` is the **SQLite driver** for the SQLModel ecosystem. It implements
//! the `Connection` trait from `sqlmodel-core`, providing a lightweight backend that is
//! ideal for local development, embedded use, and testing.
//!
//! # Role In The Architecture
//!
//! - Implements `sqlmodel-core::Connection` for SQLite
//! - Supplies FFI-backed execution and type conversion
//! - Enables `sqlmodel-query` and `sqlmodel-session` to run against SQLite
//!
// FFI bindings require unsafe code - this is expected for database drivers
#![allow(unsafe_code)]
//!
//! This crate provides a SQLite database driver using FFI bindings to libsqlite3.
//! It implements the `Connection` trait from sqlmodel-core for seamless integration
//! with the rest of the SQLModel ecosystem.
//!
//! # Features
//!
//! - Full Connection trait implementation
//! - Transaction support with savepoints
//! - Type-safe parameter binding
//! - In-memory and file-based databases
//! - Configurable open flags and busy timeout
//!
//! # Example
//!
//! ```rust,ignore
//! use sqlmodel_sqlite::{SqliteConnection, SqliteConfig};
//! use sqlmodel_core::{Connection, Value, Cx, Outcome};
//!
//! // Open an in-memory database
//! let conn = SqliteConnection::open_memory().unwrap();
//!
//! // Create a table
//! conn.execute_raw("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
//!
//! // Insert data using the Connection trait
//! let cx = Cx::for_testing();
//! match conn.insert(&cx, "INSERT INTO users (name) VALUES (?)", &[Value::Text("Alice".into())]).await {
//!     Outcome::Ok(id) => println!("Inserted user with id: {}", id),
//!     Outcome::Err(e) => eprintln!("Error: {}", e),
//!     _ => {}
//! }
//! ```
//!
//! # Type Mapping
//!
//! | Rust Type | SQLite Type |
//! |-----------|-------------|
//! | `bool` | INTEGER (0/1) |
//! | `i8`, `i16`, `i32` | INTEGER |
//! | `i64` | INTEGER |
//! | `f32`, `f64` | REAL |
//! | `String` | TEXT |
//! | `Vec<u8>` | BLOB |
//! | `Option<T>` | NULL or T |
//! | `Date`, `Time`, `Timestamp` | TEXT (ISO-8601) |
//! | `Uuid` | BLOB (16 bytes) |
//! | `Json` | TEXT |
//!
//! # Thread Safety
//!
//! `SqliteConnection` is both `Send` and `Sync`, using internal mutex
//! synchronization to protect the underlying SQLite handle. This allows
//! connections to be shared across async tasks safely.

pub mod connection;
pub mod ffi;
pub mod types;

pub use connection::{OpenFlags, SqliteConfig, SqliteConnection, SqliteTransaction};

// Console integration (feature-gated)
#[cfg(feature = "console")]
pub use sqlmodel_console::ConsoleAware;

/// Re-export the SQLite library version.
pub fn sqlite_version() -> &'static str {
    ffi::version()
}

/// Re-export the SQLite library version number.
pub fn sqlite_version_number() -> i32 {
    ffi::version_number()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_version() {
        let version = sqlite_version();
        assert!(
            version.starts_with('3'),
            "Expected SQLite 3.x, got {}",
            version
        );
    }

    #[test]
    fn test_sqlite_version_number() {
        let num = sqlite_version_number();
        assert!(
            num >= 3_000_000,
            "Expected SQLite 3.x.x (>= 3000000), got {}",
            num
        );
    }
}
