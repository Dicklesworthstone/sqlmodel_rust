//! # SQLModel Rust ORM User Guide
//!
//! Comprehensive guide to building applications with SQLModel Rust.

#![doc = include_str!("../../../docs/guide/README.md")]

#[doc = include_str!("../../../docs/guide/models.md")]
pub mod models {}

#[doc = include_str!("../../../docs/guide/queries.md")]
pub mod queries {}

#[doc = include_str!("../../../docs/guide/sessions.md")]
pub mod sessions {}

#[doc = include_str!("../../../docs/guide/relationships.md")]
pub mod relationships {}

#[doc = include_str!("../../../docs/guide/inheritance.md")]
pub mod inheritance {}

#[doc = include_str!("../../../docs/guide/migrations.md")]
pub mod migrations {}

#[doc = include_str!("../../../docs/guide/drivers.md")]
pub mod drivers {}

#[doc = include_str!("../../../docs/guide/pooling.md")]
pub mod pooling {}

#[doc = include_str!("../../../docs/guide/errors.md")]
pub mod errors {}

#[doc = include_str!("../../../docs/guide/cancellation.md")]
pub mod cancellation {}

#[doc = include_str!("../../../docs/guide/testing.md")]
pub mod testing {}
