//! FrankenSQLite driver for SQLModel Rust.
//!
//! `sqlmodel-frankensqlite` is a **pure-Rust SQLite driver** for the SQLModel ecosystem.
//! It implements the `Connection` trait from `sqlmodel-core`, backed by
//! [FrankenSQLite](https://github.com/Dicklesworthstone/frankensqlite) — a pure-Rust
//! SQLite reimplementation with page-level MVCC and RaptorQ self-healing.
//!
//! # Role In The Architecture
//!
//! - Implements `sqlmodel-core::Connection` for FrankenSQLite
//! - No FFI or `unsafe` code (beyond the Send/Sync wrappers)
//! - Enables `sqlmodel-query` and `sqlmodel-session` to run against FrankenSQLite
//! - Supports `BEGIN CONCURRENT` for parallel write throughput
//!
//! # Thread Safety
//!
//! `FrankenConnection` is both `Send` and `Sync`, using internal mutex
//! synchronization to protect the underlying FrankenSQLite connection.
//! This allows connections to be shared across async tasks safely.

#![allow(unsafe_code)] // Only for Send/Sync impls on the mutex-guarded inner type

pub mod connection;
pub mod value;

pub use connection::{FrankenConnection, FrankenExclusiveTransaction, FrankenTransaction};
