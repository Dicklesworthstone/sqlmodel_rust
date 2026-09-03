//! Schema definition and migration support for SQLModel Rust.
//!
//! `sqlmodel-schema` is the **DDL and migrations layer**. It inspects `Model` metadata
//! to generate CREATE/ALTER SQL and provides tooling for schema diffs and migrations.
//!
//! # Role In The Architecture
//!
//! - **Schema extraction**: derive expected tables/columns from `Model` definitions.
//! - **Diff engine**: compare desired vs. actual schema for migration planning.
//! - **DDL generation**: emit dialect-specific SQL for SQLite, MySQL, and Postgres.
//! - **Migration runner**: track, apply, and validate migrations.
//!
//! Applications typically use this via `sqlmodel::SchemaBuilder`, but it can also be
//! embedded in custom tooling or CI migration checks.

pub mod create;
pub mod ddl;
pub mod diff;
pub mod expected;
pub mod introspect;
pub mod migrate;

pub use create::{CreateTable, SchemaBuilder};
pub use ddl::{
    DdlGenerator, MysqlDdlGenerator, PostgresDdlGenerator, SqliteDdlGenerator,
    generator_for_dialect,
};
pub use expected::{
    ModelSchema, ModelTuple, expected_schema, normalize_sql_type, table_schema_from_fields,
    table_schema_from_model,
};
pub use introspect::{
    CheckConstraintInfo, ColumnInfo, DatabaseSchema, Dialect, ForeignKeyInfo, IndexInfo,
    Introspector, ParsedSqlType, TableInfo, UniqueConstraintInfo,
};
pub use migrate::{
    DEFAULT_LOCK_TIMEOUT, Migration, MigrationFormat, MigrationRunner, MigrationStatus,
    MigrationWriter, split_statements,
};

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
    let sql = drop_table_sql(conn.dialect(), table_name, if_exists);
    conn.execute(cx, &sql, &[]).await.map(|_| ())
}

/// Generate DROP TABLE SQL for `dialect` (for testing/inspection).
///
/// This is the same SQL that `drop_table` executes.
pub fn drop_table_sql(
    dialect: sqlmodel_core::Dialect,
    table_name: &str,
    if_exists: bool,
) -> String {
    let table = dialect.quote_identifier(table_name);
    if if_exists {
        format!("DROP TABLE IF EXISTS {table}")
    } else {
        format!("DROP TABLE {table}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ================================================================================
    // DROP TABLE Identifier Quoting Tests
    // ================================================================================

    #[test]
    fn test_drop_table_sql_simple() {
        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "users", true);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"users\"");

        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "heroes", false);
        assert_eq!(sql, "DROP TABLE \"heroes\"");
    }

    #[test]
    fn test_drop_table_sql_with_keyword_name() {
        // SQL keywords must be quoted
        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "order", true);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"order\"");

        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "select", true);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"select\"");

        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "user", true);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"user\"");
    }

    #[test]
    fn test_drop_table_sql_with_embedded_quotes() {
        // Embedded quotes must be doubled
        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "my\"table", true);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"my\"\"table\"");

        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "test\"\"name", false);
        assert_eq!(sql, "DROP TABLE \"test\"\"\"\"name\"");

        // Just a quote
        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "\"", true);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"\"\"\"");
    }

    #[test]
    fn test_drop_table_sql_with_spaces() {
        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "my table", true);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"my table\"");
    }

    #[test]
    fn test_drop_table_sql_with_unicode() {
        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "用户", true);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"用户\"");

        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "tâble_émoji_🦀", false);
        assert_eq!(sql, "DROP TABLE \"tâble_émoji_🦀\"");
    }

    #[test]
    fn test_drop_table_sql_edge_cases() {
        // Empty table name (unusual but should be quoted)
        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "", true);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"\"");

        // Single character
        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "x", true);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"x\"");

        // Numbers at start (not valid unquoted identifier in most DBs)
        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "123table", true);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"123table\"");

        // Special characters
        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "table-with-dashes", true);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"table-with-dashes\"");

        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, "table.with.dots", true);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"table.with.dots\"");
    }

    #[test]
    fn test_drop_table_sql_sql_injection_attempt_neutralized() {
        // SQL injection attempt - the quote_ident should neutralize it
        let malicious = "users\"; DROP TABLE secrets; --";
        let sql = drop_table_sql(sqlmodel_core::Dialect::Sqlite, malicious, true);
        // The embedded quote should be doubled, neutralizing the injection
        assert_eq!(
            sql,
            "DROP TABLE IF EXISTS \"users\"\"; DROP TABLE secrets; --\""
        );
        // Verify the whole thing is treated as a single identifier
        assert!(sql.starts_with("DROP TABLE IF EXISTS \""));
        assert!(sql.ends_with('"'));
        // Count quotes: 1 opening + 2 for doubled embedded quote + 1 closing = 4
        let quote_count = sql.matches('"').count();
        assert_eq!(quote_count, 4);
    }
}
