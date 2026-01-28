//! Core types and traits for SQLModel Rust.
//!
//! This crate provides the foundational abstractions for type-safe SQL operations:
//!
//! - `Model` trait for ORM-style struct mapping
//! - `Field` types for column definitions
//! - `Connection` trait for database connections
//! - `Outcome` re-export from asupersync for cancel-correct operations
//! - `Cx` context for structured concurrency

// Re-export asupersync primitives for structured concurrency
pub use asupersync::{Budget, Cx, Outcome, RegionId, TaskId};

pub mod connection;
pub mod error;
pub mod field;
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
pub use field::{Column, Field, FieldInfo, ReferentialAction};
pub use model::{
    AutoIncrement, ExtraFieldsBehavior, Model, ModelConfig, ModelEvents, SoftDelete, Timestamps,
};
pub use relationship::{
    Lazy, LazyLoader, LinkTableInfo, Related, RelatedMany, RelationshipInfo, RelationshipKind,
    find_back_relationship, find_relationship, validate_back_populates,
};
pub use row::Row;
pub use types::{SqlType, TypeInfo};
pub use validate::{
    DumpMode, DumpOptions, DumpResult, ModelDump, ModelValidate, SqlModelDump, SqlModelValidate,
    ValidateInput, ValidateOptions, ValidateResult, apply_serialization_aliases,
    apply_validation_aliases,
};
pub use value::Value;
