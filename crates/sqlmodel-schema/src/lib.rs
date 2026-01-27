//! Schema definition and migration support for SQLModel Rust.
//!
//! This crate provides:
//! - Schema definition from Model types
//! - Expected schema extraction from Model definitions
//! - Table creation/alteration SQL generation
//! - Migration tracking and execution
//! - Database introspection

pub mod create;
pub mod expected;
pub mod introspect;
pub mod migrate;

pub use create::{CreateTable, SchemaBuilder};
pub use expected::{
    ModelSchema, ModelTuple, expected_schema, normalize_sql_type, table_schema_from_fields,
    table_schema_from_model,
};
pub use introspect::{
    CheckConstraintInfo, ColumnInfo, DatabaseSchema, Dialect, ForeignKeyInfo, IndexInfo,
    Introspector, ParsedSqlType, TableInfo, UniqueConstraintInfo,
};
pub use migrate::{Migration, MigrationRunner, MigrationStatus};

use asupersync::{Cx, Outcome};
use sqlmodel_core::{Connection, Model};

/// Create a table for a model type.
///
/// # Example
///
/// ```ignore
/// use sqlmodel::{Model, create_table};
///
/// #[derive(Model)]
/// struct Hero {
///     id: Option<i64>,
///     name: String,
/// }
///
/// // Generate CREATE TABLE SQL
/// let sql = create_table::<Hero>().if_not_exists().build();
/// ```
pub fn create_table<M: Model>() -> CreateTable<M> {
    CreateTable::new()
}

/// Create all tables for the given models.
///
/// This is a convenience function for creating multiple tables
/// in the correct order based on foreign key dependencies.
pub async fn create_all<C: Connection>(
    cx: &Cx,
    conn: &C,
    schemas: &[&str],
) -> Outcome<(), sqlmodel_core::Error> {
    for sql in schemas {
        match conn.execute(cx, sql, &[]).await {
            Outcome::Ok(_) => continue,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        }
    }
    Outcome::Ok(())
}

/// Drop a table.
pub async fn drop_table<C: Connection>(
    cx: &Cx,
    conn: &C,
    table_name: &str,
    if_exists: bool,
) -> Outcome<(), sqlmodel_core::Error> {
    let sql = if if_exists {
        format!("DROP TABLE IF EXISTS {}", table_name)
    } else {
        format!("DROP TABLE {}", table_name)
    };

    conn.execute(cx, &sql, &[]).await.map(|_| ())
}
