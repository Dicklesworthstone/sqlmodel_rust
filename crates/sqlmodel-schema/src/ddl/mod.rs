//! DDL (Data Definition Language) generation from schema operations.
//!
//! This module converts `SchemaOperation`s from the diff engine into executable
//! SQL statements for each supported database dialect (SQLite, MySQL, PostgreSQL).

mod mysql;
mod postgres;
mod sqlite;

pub use mysql::MysqlDdlGenerator;
pub use postgres::PostgresDdlGenerator;
pub use sqlite::SqliteDdlGenerator;

use crate::diff::SchemaOperation;
use crate::introspect::{
    ColumnInfo, Dialect, ForeignKeyInfo, IndexInfo, TableInfo, UniqueConstraintInfo,
};

/// Generates DDL SQL statements from schema operations.
pub trait DdlGenerator {
    /// The database dialect name.
    fn dialect(&self) -> &'static str;

    /// Generate DDL statement(s) for a single schema operation.
    ///
    /// Some operations (like SQLite DROP COLUMN) may produce multiple statements.
    fn generate(&self, op: &SchemaOperation) -> Vec<String>;

    /// Generate DDL statements for multiple operations.
    fn generate_all(&self, ops: &[SchemaOperation]) -> Vec<String> {
        ops.iter().flat_map(|op| self.generate(op)).collect()
    }

    /// Generate rollback DDL statements (inverse operations).
    ///
    /// Returns statements in reverse order, suitable for undoing the original operations.
    /// Operations without an inverse (like DROP TABLE) are skipped.
    fn generate_rollback(&self, ops: &[SchemaOperation]) -> Vec<String> {
        ops.iter()
            .rev()
            .filter_map(|op| op.inverse())
            .flat_map(|op| self.generate(&op))
            .collect()
    }
}

/// Create a DDL generator for the given dialect.
pub fn generator_for_dialect(dialect: Dialect) -> Box<dyn DdlGenerator> {
    match dialect {
        Dialect::Sqlite => Box::new(SqliteDdlGenerator),
        Dialect::Mysql => Box::new(MysqlDdlGenerator),
        Dialect::Postgres => Box::new(PostgresDdlGenerator),
    }
}

// ============================================================================
// Shared Helpers
// ============================================================================

/// Quote an identifier (table/column name) for SQL.
///
/// Different dialects use different quote characters:
/// - SQLite/PostgreSQL: double quotes
/// - MySQL: backticks
fn quote_identifier(name: &str, dialect: Dialect) -> String {
    match dialect {
        Dialect::Mysql => format!("`{}`", name.replace('`', "``")),
        Dialect::Sqlite | Dialect::Postgres => format!("\"{}\"", name.replace('"', "\"\"")),
    }
}

/// Format a column definition for CREATE TABLE or ADD COLUMN.
fn format_column_def(col: &ColumnInfo, dialect: Dialect) -> String {
    let mut parts = vec![quote_identifier(&col.name, dialect), col.sql_type.clone()];

    if !col.nullable {
        parts.push("NOT NULL".to_string());
    }

    if let Some(ref default) = col.default {
        parts.push(format!("DEFAULT {}", default));
    }

    // Auto-increment handling varies by dialect
    match dialect {
        Dialect::Sqlite => {
            // SQLite: INTEGER PRIMARY KEY implies AUTOINCREMENT
            // Explicit AUTOINCREMENT keyword is rarely needed
        }
        Dialect::Mysql => {
            if col.auto_increment {
                parts.push("AUTO_INCREMENT".to_string());
            }
        }
        Dialect::Postgres => {
            // PostgreSQL uses SERIAL types or GENERATED AS IDENTITY
            // The sql_type should already contain SERIAL/BIGSERIAL if auto-increment
        }
    }

    parts.join(" ")
}

/// Format the ON DELETE/UPDATE action for foreign keys.
fn format_referential_action(action: Option<&String>) -> &str {
    match action.map(|s| s.to_uppercase()).as_deref() {
        Some("CASCADE") => "CASCADE",
        Some("SET NULL") => "SET NULL",
        Some("SET DEFAULT") => "SET DEFAULT",
        Some("RESTRICT") => "RESTRICT",
        _ => "NO ACTION",
    }
}

/// Format a foreign key constraint clause.
fn format_fk_constraint(fk: &ForeignKeyInfo, dialect: Dialect) -> String {
    let mut sql = format!(
        "FOREIGN KEY ({}) REFERENCES {}({})",
        quote_identifier(&fk.column, dialect),
        quote_identifier(&fk.foreign_table, dialect),
        quote_identifier(&fk.foreign_column, dialect),
    );

    let on_delete = format_referential_action(fk.on_delete.as_ref());
    let on_update = format_referential_action(fk.on_update.as_ref());

    if on_delete != "NO ACTION" {
        sql.push_str(&format!(" ON DELETE {}", on_delete));
    }
    if on_update != "NO ACTION" {
        sql.push_str(&format!(" ON UPDATE {}", on_update));
    }

    sql
}

/// Format a unique constraint clause.
fn format_unique_constraint(unique: &UniqueConstraintInfo, dialect: Dialect) -> String {
    let cols: Vec<String> = unique
        .columns
        .iter()
        .map(|c| quote_identifier(c, dialect))
        .collect();

    if let Some(ref name) = unique.name {
        format!(
            "CONSTRAINT {} UNIQUE ({})",
            quote_identifier(name, dialect),
            cols.join(", ")
        )
    } else {
        format!("UNIQUE ({})", cols.join(", "))
    }
}

/// Generate CREATE TABLE SQL with configurable `IF NOT EXISTS`.
///
/// Kept private to `ddl` and its submodules (SQLite drop-column needs a
/// strict create without IF NOT EXISTS for table recreation).
fn generate_create_table_with_if_not_exists(
    table: &TableInfo,
    dialect: Dialect,
    if_not_exists: bool,
) -> String {
    tracing::debug!(
        dialect = %match dialect {
            Dialect::Sqlite => "sqlite",
            Dialect::Mysql => "mysql",
            Dialect::Postgres => "postgres",
        },
        table = %table.name,
        columns = table.columns.len(),
        "Generating CREATE TABLE DDL"
    );

    let mut parts = Vec::new();

    // Column definitions
    for col in &table.columns {
        parts.push(format!("  {}", format_column_def(col, dialect)));
    }

    // Primary key constraint (if not embedded in column definition)
    if !table.primary_key.is_empty() {
        let pk_cols: Vec<String> = table
            .primary_key
            .iter()
            .map(|c| quote_identifier(c, dialect))
            .collect();
        parts.push(format!("  PRIMARY KEY ({})", pk_cols.join(", ")));
    }

    // Unique constraints
    for unique in &table.unique_constraints {
        parts.push(format!("  {}", format_unique_constraint(unique, dialect)));
    }

    // Foreign key constraints
    for fk in &table.foreign_keys {
        parts.push(format!("  {}", format_fk_constraint(fk, dialect)));
    }

    let table_name = quote_identifier(&table.name, dialect);
    let ine = if if_not_exists { " IF NOT EXISTS" } else { "" };
    let sql = format!(
        "CREATE TABLE{} {} (\n{}\n)",
        ine,
        table_name,
        parts.join(",\n")
    );

    tracing::trace!(sql = %sql, "Generated CREATE TABLE statement");
    sql
}

/// Generate CREATE TABLE SQL (defaulting to `IF NOT EXISTS`).
fn generate_create_table(table: &TableInfo, dialect: Dialect) -> String {
    generate_create_table_with_if_not_exists(table, dialect, true)
}

/// Generate DROP TABLE SQL.
fn generate_drop_table(table_name: &str, dialect: Dialect) -> String {
    tracing::debug!(table = %table_name, "Generating DROP TABLE DDL");
    format!(
        "DROP TABLE IF EXISTS {}",
        quote_identifier(table_name, dialect)
    )
}

/// Generate RENAME TABLE SQL.
fn generate_rename_table(from: &str, to: &str, dialect: Dialect) -> String {
    tracing::debug!(from = %from, to = %to, "Generating RENAME TABLE DDL");
    match dialect {
        Dialect::Sqlite => format!(
            "ALTER TABLE {} RENAME TO {}",
            quote_identifier(from, dialect),
            quote_identifier(to, dialect)
        ),
        Dialect::Mysql => format!(
            "RENAME TABLE {} TO {}",
            quote_identifier(from, dialect),
            quote_identifier(to, dialect)
        ),
        Dialect::Postgres => format!(
            "ALTER TABLE {} RENAME TO {}",
            quote_identifier(from, dialect),
            quote_identifier(to, dialect)
        ),
    }
}

/// Generate ADD COLUMN SQL.
fn generate_add_column(table: &str, column: &ColumnInfo, dialect: Dialect) -> String {
    tracing::debug!(table = %table, column = %column.name, "Generating ADD COLUMN DDL");
    format!(
        "ALTER TABLE {} ADD COLUMN {}",
        quote_identifier(table, dialect),
        format_column_def(column, dialect)
    )
}

/// Generate RENAME COLUMN SQL.
fn generate_rename_column(table: &str, from: &str, to: &str, dialect: Dialect) -> String {
    tracing::debug!(table = %table, from = %from, to = %to, "Generating RENAME COLUMN DDL");
    match dialect {
        Dialect::Sqlite => {
            // SQLite 3.25.0+ supports RENAME COLUMN
            format!(
                "ALTER TABLE {} RENAME COLUMN {} TO {}",
                quote_identifier(table, dialect),
                quote_identifier(from, dialect),
                quote_identifier(to, dialect)
            )
        }
        Dialect::Mysql => format!(
            "ALTER TABLE {} RENAME COLUMN {} TO {}",
            quote_identifier(table, dialect),
            quote_identifier(from, dialect),
            quote_identifier(to, dialect)
        ),
        Dialect::Postgres => format!(
            "ALTER TABLE {} RENAME COLUMN {} TO {}",
            quote_identifier(table, dialect),
            quote_identifier(from, dialect),
            quote_identifier(to, dialect)
        ),
    }
}

/// Generate CREATE INDEX SQL.
fn generate_create_index(table: &str, index: &IndexInfo, dialect: Dialect) -> String {
    tracing::debug!(
        table = %table,
        index = %index.name,
        columns = ?index.columns,
        unique = index.unique,
        "Generating CREATE INDEX DDL"
    );

    let unique = if index.unique { "UNIQUE " } else { "" };
    let cols: Vec<String> = index
        .columns
        .iter()
        .map(|c| quote_identifier(c, dialect))
        .collect();

    // Include index type for databases that support it
    let using = match dialect {
        Dialect::Postgres => {
            if let Some(ref idx_type) = index.index_type {
                format!(" USING {}", idx_type)
            } else {
                String::new()
            }
        }
        Dialect::Mysql => {
            if let Some(ref idx_type) = index.index_type {
                if idx_type.eq_ignore_ascii_case("BTREE") {
                    String::new()
                } else {
                    format!(" USING {}", idx_type)
                }
            } else {
                String::new()
            }
        }
        Dialect::Sqlite => String::new(),
    };

    format!(
        "CREATE {}INDEX {} ON {}{}({})",
        unique,
        quote_identifier(&index.name, dialect),
        quote_identifier(table, dialect),
        using,
        cols.join(", ")
    )
}

/// Generate DROP INDEX SQL.
fn generate_drop_index(table: &str, index_name: &str, dialect: Dialect) -> String {
    tracing::debug!(table = %table, index = %index_name, "Generating DROP INDEX DDL");
    match dialect {
        Dialect::Sqlite => format!(
            "DROP INDEX IF EXISTS {}",
            quote_identifier(index_name, dialect)
        ),
        Dialect::Mysql => format!(
            "DROP INDEX {} ON {}",
            quote_identifier(index_name, dialect),
            quote_identifier(table, dialect)
        ),
        Dialect::Postgres => format!(
            "DROP INDEX IF EXISTS {}",
            quote_identifier(index_name, dialect)
        ),
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::introspect::ParsedSqlType;

    fn make_column(name: &str, sql_type: &str, nullable: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            sql_type: sql_type.to_string(),
            parsed_type: ParsedSqlType::parse(sql_type),
            nullable,
            default: None,
            primary_key: false,
            auto_increment: false,
            comment: None,
        }
    }

    fn make_table(name: &str, columns: Vec<ColumnInfo>, pk: Vec<&str>) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            columns,
            primary_key: pk.into_iter().map(String::from).collect(),
            foreign_keys: Vec::new(),
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            indexes: Vec::new(),
            comment: None,
        }
    }

    #[test]
    fn test_quote_identifier_sqlite() {
        assert_eq!(quote_identifier("name", Dialect::Sqlite), "\"name\"");
        assert_eq!(quote_identifier("table", Dialect::Sqlite), "\"table\"");
        assert_eq!(
            quote_identifier("col\"name", Dialect::Sqlite),
            "\"col\"\"name\""
        );
    }

    #[test]
    fn test_quote_identifier_mysql() {
        assert_eq!(quote_identifier("name", Dialect::Mysql), "`name`");
        assert_eq!(quote_identifier("table", Dialect::Mysql), "`table`");
        assert_eq!(quote_identifier("col`name", Dialect::Mysql), "`col``name`");
    }

    #[test]
    fn test_format_column_def_basic() {
        let col = make_column("name", "TEXT", false);
        let def = format_column_def(&col, Dialect::Sqlite);
        assert!(def.contains("\"name\""));
        assert!(def.contains("TEXT"));
        assert!(def.contains("NOT NULL"));
    }

    #[test]
    fn test_format_column_def_nullable() {
        let col = make_column("name", "TEXT", true);
        let def = format_column_def(&col, Dialect::Sqlite);
        assert!(!def.contains("NOT NULL"));
    }

    #[test]
    fn test_format_column_def_with_default() {
        let mut col = make_column("status", "TEXT", false);
        col.default = Some("'active'".to_string());
        let def = format_column_def(&col, Dialect::Sqlite);
        assert!(def.contains("DEFAULT 'active'"));
    }

    #[test]
    fn test_format_column_def_auto_increment_mysql() {
        let mut col = make_column("id", "INT", false);
        col.auto_increment = true;
        let def = format_column_def(&col, Dialect::Mysql);
        assert!(def.contains("AUTO_INCREMENT"));
    }

    #[test]
    fn test_generate_create_table_basic() {
        let table = make_table(
            "heroes",
            vec![
                make_column("id", "INTEGER", false),
                make_column("name", "TEXT", false),
            ],
            vec!["id"],
        );
        let sql = generate_create_table(&table, Dialect::Sqlite);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS"));
        assert!(sql.contains("\"heroes\""));
        assert!(sql.contains("\"id\""));
        assert!(sql.contains("\"name\""));
        assert!(sql.contains("PRIMARY KEY"));
    }

    #[test]
    fn test_generate_create_table_with_fk() {
        let mut table = make_table(
            "heroes",
            vec![
                make_column("id", "INTEGER", false),
                make_column("team_id", "INTEGER", true),
            ],
            vec!["id"],
        );
        table.foreign_keys.push(ForeignKeyInfo {
            name: Some("fk_heroes_team".to_string()),
            column: "team_id".to_string(),
            foreign_table: "teams".to_string(),
            foreign_column: "id".to_string(),
            on_delete: Some("CASCADE".to_string()),
            on_update: None,
        });

        let sql = generate_create_table(&table, Dialect::Sqlite);
        assert!(sql.contains("FOREIGN KEY"));
        assert!(sql.contains("REFERENCES"));
        assert!(sql.contains("ON DELETE CASCADE"));
    }

    #[test]
    fn test_generate_drop_table() {
        let sql = generate_drop_table("heroes", Dialect::Sqlite);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"heroes\"");
    }

    #[test]
    fn test_generate_rename_table_sqlite() {
        let sql = generate_rename_table("old_name", "new_name", Dialect::Sqlite);
        assert!(sql.contains("ALTER TABLE"));
        assert!(sql.contains("RENAME TO"));
    }

    #[test]
    fn test_generate_rename_table_mysql() {
        let sql = generate_rename_table("old_name", "new_name", Dialect::Mysql);
        assert!(sql.contains("RENAME TABLE"));
    }

    #[test]
    fn test_generate_add_column() {
        let col = make_column("age", "INTEGER", true);
        let sql = generate_add_column("heroes", &col, Dialect::Sqlite);
        assert!(sql.contains("ALTER TABLE"));
        assert!(sql.contains("ADD COLUMN"));
        assert!(sql.contains("\"age\""));
    }

    #[test]
    fn test_generate_rename_column() {
        let sql = generate_rename_column("heroes", "old_name", "new_name", Dialect::Postgres);
        assert!(sql.contains("ALTER TABLE"));
        assert!(sql.contains("RENAME COLUMN"));
    }

    #[test]
    fn test_generate_create_index() {
        let index = IndexInfo {
            name: "idx_heroes_name".to_string(),
            columns: vec!["name".to_string()],
            unique: false,
            index_type: None,
            primary: false,
        };
        let sql = generate_create_index("heroes", &index, Dialect::Sqlite);
        assert!(sql.contains("CREATE INDEX"));
        assert!(sql.contains("\"idx_heroes_name\""));
        assert!(sql.contains("ON \"heroes\""));
    }

    #[test]
    fn test_generate_create_unique_index() {
        let index = IndexInfo {
            name: "idx_heroes_name_unique".to_string(),
            columns: vec!["name".to_string()],
            unique: true,
            index_type: None,
            primary: false,
        };
        let sql = generate_create_index("heroes", &index, Dialect::Sqlite);
        assert!(sql.contains("CREATE UNIQUE INDEX"));
    }

    #[test]
    fn test_generate_drop_index_sqlite() {
        let sql = generate_drop_index("heroes", "idx_heroes_name", Dialect::Sqlite);
        assert_eq!(sql, "DROP INDEX IF EXISTS \"idx_heroes_name\"");
    }

    #[test]
    fn test_generate_drop_index_mysql() {
        let sql = generate_drop_index("heroes", "idx_heroes_name", Dialect::Mysql);
        assert!(sql.contains("DROP INDEX"));
        assert!(sql.contains("ON `heroes`"));
    }

    #[test]
    fn test_generator_for_dialect() {
        let sqlite = generator_for_dialect(Dialect::Sqlite);
        assert_eq!(sqlite.dialect(), "sqlite");

        let mysql = generator_for_dialect(Dialect::Mysql);
        assert_eq!(mysql.dialect(), "mysql");

        let postgres = generator_for_dialect(Dialect::Postgres);
        assert_eq!(postgres.dialect(), "postgres");
    }

    #[test]
    fn test_referential_action_formatting() {
        assert_eq!(
            format_referential_action(Some(&"CASCADE".to_string())),
            "CASCADE"
        );
        assert_eq!(
            format_referential_action(Some(&"cascade".to_string())),
            "CASCADE"
        );
        assert_eq!(
            format_referential_action(Some(&"SET NULL".to_string())),
            "SET NULL"
        );
        assert_eq!(format_referential_action(None), "NO ACTION");
    }
}
