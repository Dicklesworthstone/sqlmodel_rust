//! PostgreSQL driver for SQLModel Rust.
//!
//! This crate implements the PostgreSQL wire protocol from scratch using
//! asupersync's TCP primitives. It provides:
//!
//! - Message framing and parsing
//! - Authentication (cleartext, MD5, SCRAM-SHA-256)
//! - Simple and extended query protocols
//! - Connection management with state machine
//! - Type conversion between Rust and PostgreSQL types
//!
//! # Type System
//!
//! The `types` module provides comprehensive type mapping between PostgreSQL
//! and Rust types, including:
//!
//! - OID constants for all built-in types
//! - Text and binary encoding/decoding
//! - Type registry for runtime type lookup
//!
//! # Example
//!
//! ```rust,ignore
//! use sqlmodel_postgres::{PgConfig, PgConnection};
//!
//! let config = PgConfig::new()
//!     .host("localhost")
//!     .port(5432)
//!     .user("postgres")
//!     .database("mydb");
//!
//! let conn = PgConnection::connect(config)?;
//! ```

pub mod auth;
pub mod config;
pub mod connection;
pub mod protocol;
pub mod types;

pub use config::{PgConfig, SslMode};
pub use connection::{ConnectionState, PgConnection, TransactionStatusState};
pub use types::{Format, TypeCategory, TypeInfo, TypeRegistry};

// Console integration (feature-gated)
#[cfg(feature = "console")]
pub use connection::ConnectionStage;
#[cfg(feature = "console")]
pub use sqlmodel_console::ConsoleAware;
