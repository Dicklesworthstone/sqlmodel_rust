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

pub mod auth;
pub mod protocol;
