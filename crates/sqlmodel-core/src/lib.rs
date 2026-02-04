//! Core types and traits for SQLModel Rust.
//!
//! `sqlmodel-core` is the **foundation layer** for the entire ecosystem. It defines the
//! traits and core data types that all other crates build on.
//!
//! # Role In The Architecture
//!
//! - **Contract layer**: `Model` and `Connection` are the primary traits implemented by
//!   user models and database drivers.
//! - **Data model**: `Row`, `Value`, and `SqlType` represent query inputs/outputs and
//!   are shared across query, schema, and driver crates.
//! - **Structured concurrency**: re-exports `Cx` and `Outcome` from asupersync so every
//!   async database operation is cancel-correct and budget-aware.
//!
//! # Who Uses This Crate
//!
//! - `sqlmodel-macros` generates `Model` implementations defined here.
//! - `sqlmodel-query` consumes `Model` metadata and `Value` to build SQL.
//! - `sqlmodel-schema` inspects `Model` metadata to generate DDL.
//! - `sqlmodel-session` depends on `Connection`, `Row`, and `Value` for unit-of-work flows.
//! - Driver crates (`sqlmodel-postgres`, `sqlmodel-mysql`, `sqlmodel-sqlite`) implement
//!   `Connection` and operate on `Row`/`Value`.
//!
//! Most applications should use the `sqlmodel` facade; reach for `sqlmodel-core` directly
//! when writing drivers or advanced integrations.

// Re-export asupersync primitives for structured concurrency
pub use asupersync::{Budget, Cx, Outcome, RegionId, TaskId};

pub mod connection;
pub mod dynamic;
pub mod error;
pub mod field;
pub mod hybrid;
pub mod identifiers;
pub mod model;
pub mod relationship;
pub mod row;
pub mod types;
pub mod validate;
pub mod value;

pub use connection::{
    Connection, IsolationLevel, PreparedStatement, Transaction, TransactionInternal, TransactionOps,
};
pub use error::{Error, FieldValidationError, Result, ValidationError, ValidationErrorKind};
pub use field::{
    Column, Field, FieldInfo, InheritanceInfo, InheritanceStrategy, ReferentialAction,
};
pub use hybrid::Hybrid;
pub use identifiers::{quote_ident, quote_ident_mysql, sanitize_identifier};
pub use model::{
    AttributeChange, AutoIncrement, ExtraFieldsBehavior, Model, ModelConfig, ModelEvents,
    SoftDelete, Timestamps,
};
pub use relationship::{
    Lazy, LazyLoader, LinkTableInfo, PassiveDeletes, Related, RelatedMany, RelationshipInfo,
    RelationshipKind, find_back_relationship, find_relationship, validate_back_populates,
};
pub use row::Row;
pub use types::{SqlEnum, SqlType, TypeInfo};
pub use validate::{
    DumpMode, DumpOptions, DumpResult, ModelDump, ModelValidate, SqlModelDump, SqlModelValidate,
    ValidateInput, ValidateOptions, ValidateResult, apply_serialization_aliases,
    apply_validation_aliases,
};
pub use value::Value;
