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
//! - No FFI and no `unsafe` code
//! - Enables `sqlmodel-query` and `sqlmodel-session` to run against FrankenSQLite
//! - Supports `BEGIN CONCURRENT` for parallel write throughput
//!
//! # Thread Safety
//!
//! `FrankenConnection` is both `Send` and `Sync`. The fsqlite engine handle is
//! owned by a dedicated worker thread and reached through a channel handle
//! (`fsqlite::AsyncConnection`), so no `Rc`/`RefCell` engine state ever crosses
//! a thread boundary; the mutex only guards the adapter's own bookkeeping
//! (`in_transaction`, `last_insert_rowid`). Connections can be shared across
//! async tasks safely without any `unsafe impl Send/Sync` wrappers.

pub mod connection;
pub mod value;

pub use connection::{
    FrankenConnection, FrankenExclusiveTransaction, FrankenExclusiveTransactionError,
    FrankenTransaction,
};
