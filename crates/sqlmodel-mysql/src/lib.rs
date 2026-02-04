//! MySQL driver for SQLModel Rust.
//!
//! `sqlmodel-mysql` is the **MySQL driver** for the SQLModel ecosystem. It
//! implements the MySQL wire protocol from scratch using asupersync's TCP
//! primitives and exposes a `Connection` implementation for query execution.
//!
//! # Role In The Architecture
//!
//! - Implements `sqlmodel-core::Connection` for MySQL
//! - Provides authentication, protocol framing, and type conversions
//! - Powers `sqlmodel-query` execution and `sqlmodel-session` persistence
//!
//! This crate implements the MySQL wire protocol from scratch using
//! asupersync's TCP primitives. It provides:
//!
//! - Packet framing with sequence numbers
//! - Authentication (mysql_native_password, caching_sha2_password)
//! - Text and binary query protocols
//! - Prepared statement support
//! - Connection management with state machine
//! - Type conversion between Rust and MySQL types
//!
//! # MySQL Protocol Overview
//!
//! MySQL uses a packet-based protocol with:
//! - 3-byte payload length + 1-byte sequence number header
//! - Packets over 16MB are split
//! - Request/response pairing via sequence numbers
//!
//! # Example
//!
//! ```rust,ignore
//! use sqlmodel_mysql::{MySqlConfig, MySqlConnection};
//!
//! let config = MySqlConfig::new()
//!     .host("localhost")
//!     .port(3306)
//!     .user("root")
//!     .database("mydb");
//!
//! let conn = MySqlConnection::connect(config)?;
//! ```

pub mod async_connection;
pub mod auth;
pub mod config;
pub mod connection;
pub mod protocol;
pub mod tls;
pub mod types;

pub use async_connection::{MySqlAsyncConnection, SharedMySqlConnection};
pub use config::{MySqlConfig, SslMode, TlsConfig};
pub use connection::{ConnectionState, MySqlConnection};

// Console integration (feature-gated)
#[cfg(feature = "console")]
pub use sqlmodel_console::ConsoleAware;
