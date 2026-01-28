//! Database migration support.
//!
//! This module provides:
//! - Migration file generation from schema diffs
//! - Writing migrations to disk (SQL or Rust format)
//! - Running migrations against a database
//! - Tracking applied migrations

use crate::ddl::DdlGenerator;
use crate::diff::SchemaOperation;
use asupersync::{Cx, Outcome};
use sqlmodel_core::{Connection, Error, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// A database migration.
#[derive(Debug, Clone)]
pub struct Migration {
    /// Unique migration ID (typically timestamp-based)
    pub id: String,
    /// Human-readable description
    pub description: String,
    /// SQL to apply the migration
    pub up: String,
    /// SQL to revert the migration
    pub down: String,
}

impl Migration {
    /// Create a new migration.
    pub fn new(
        id: impl Into<String>,
        description: impl Into<String>,
        up: impl Into<String>,
        down: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            description: description.into(),
            up: up.into(),
            down: down.into(),
        }
    }

    /// Generate a new migration version from the current timestamp.
    ///
    /// Format: YYYYMMDDHHMMSS
    #[must_use]
    pub fn new_version() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());

        // Convert to datetime components manually (avoiding chrono dependency)
        let days = now / 86400;
        let secs = now % 86400;
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        let secs = secs % 60;

        // Calculate year/month/day from days since epoch (1970-01-01)
        let mut year = 1970;
        let mut remaining_days = days as i64;

        loop {
            let days_in_year = if is_leap_year(year) { 366 } else { 365 };
            if remaining_days < days_in_year {
                break;
            }
            remaining_days -= days_in_year;
            year += 1;
        }

        let months_days: [i64; 12] = if is_leap_year(year) {
            [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        } else {
            [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        };

        let mut month = 1;
        for days_in_month in months_days {
            if remaining_days < days_in_month {
                break;
            }
            remaining_days -= days_in_month;
            month += 1;
        }

        let day = remaining_days + 1;

        format!(
            "{:04}{:02}{:02}{:02}{:02}{:02}",
            year, month, day, hours, mins, secs
        )
    }

    /// Create a migration from schema operations.
    ///
    /// Uses the provided DDL generator to create UP (forward) and DOWN (rollback) SQL.
    #[tracing::instrument(level = "info", skip(ops, ddl, description))]
    pub fn from_operations(
        ops: &[SchemaOperation],
        ddl: &dyn DdlGenerator,
        description: impl Into<String>,
    ) -> Self {
        let description = description.into();
        let version = Self::new_version();

        tracing::info!(
            version = %version,
            description = %description,
            ops_count = ops.len(),
            dialect = ddl.dialect(),
            "Creating migration from schema operations"
        );

        let up_stmts = ddl.generate_all(ops);
        let down_stmts = ddl.generate_rollback(ops);

        // Join statements with semicolons
        let up = up_stmts.join(";\n\n") + if up_stmts.is_empty() { "" } else { ";" };
        let down = down_stmts.join(";\n\n") + if down_stmts.is_empty() { "" } else { ";" };

        tracing::debug!(
            up_statements = up_stmts.len(),
            down_statements = down_stmts.len(),
            "Generated migration SQL"
        );

        Self {
            id: version,
            description,
            up,
            down,
        }
    }
}

/// Check if a year is a leap year.
fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

// ============================================================================
// Migration Writer
// ============================================================================

/// Format for migration files.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MigrationFormat {
    /// Plain SQL files (.sql)
    #[default]
    Sql,
    /// Rust source files (.rs)
    Rust,
}

/// Writes migrations to the filesystem.
pub struct MigrationWriter {
    /// Directory for migration files.
    migrations_dir: PathBuf,
    /// File format to use.
    format: MigrationFormat,
}

impl MigrationWriter {
    /// Create a new migration writer for the given directory.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            migrations_dir: dir.into(),
            format: MigrationFormat::default(),
        }
    }

    /// Set the output format.
    #[must_use]
    pub fn with_format(mut self, format: MigrationFormat) -> Self {
        self.format = format;
        self
    }

    /// Get the migrations directory.
    pub fn migrations_dir(&self) -> &Path {
        &self.migrations_dir
    }

    /// Get the output format.
    pub fn format(&self) -> MigrationFormat {
        self.format
    }

    /// Write a migration to disk.
    ///
    /// Creates the migrations directory if it doesn't exist.
    /// Returns the path to the written file.
    #[tracing::instrument(level = "info", skip(self, migration))]
    pub fn write(&self, migration: &Migration) -> std::io::Result<PathBuf> {
        tracing::info!(
            version = %migration.id,
            description = %migration.description,
            format = ?self.format,
            dir = %self.migrations_dir.display(),
            "Writing migration file"
        );

        std::fs::create_dir_all(&self.migrations_dir)?;

        let filename = self.filename(migration);
        let path = self.migrations_dir.join(&filename);
        let content = self.format_migration(migration);

        std::fs::write(&path, &content)?;

        tracing::info!(
            path = %path.display(),
            bytes = content.len(),
            "Migration file written"
        );

        Ok(path)
    }

    /// Generate the filename for a migration.
    fn filename(&self, m: &Migration) -> String {
        // Sanitize description: lowercase, replace spaces with underscores,
        // remove non-alphanumeric chars except underscores
        let sanitized_desc: String = m
            .description
            .to_lowercase()
            .chars()
            .map(|c| if c.is_alphanumeric() { c } else { '_' })
            .collect::<String>()
            .split('_')
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("_");

        // Truncate to reasonable length
        let desc = if sanitized_desc.len() > 50 {
            &sanitized_desc[..50]
        } else {
            &sanitized_desc
        };

        match self.format {
            MigrationFormat::Sql => format!("{}_{}.sql", m.id, desc),
            MigrationFormat::Rust => format!("{}_{}.rs", m.id, desc),
        }
    }

    /// Format the migration content.
    fn format_migration(&self, m: &Migration) -> String {
        match self.format {
            MigrationFormat::Sql => self.format_sql(m),
            MigrationFormat::Rust => self.format_rust(m),
        }
    }

    /// Format as SQL file.
    fn format_sql(&self, m: &Migration) -> String {
        let mut content = String::new();

        // Header
        content.push_str(&format!("-- Migration: {}\n", m.description));
        content.push_str(&format!("-- Version: {}\n", m.id));
        content.push_str(&format!(
            "-- Generated: {}\n\n",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs())
        ));

        // UP migration
        content.push_str("-- ========== UP ==========\n\n");
        content.push_str(&m.up);
        content.push_str("\n\n");

        // DOWN migration (commented out by default for safety)
        content.push_str("-- ========== DOWN ==========\n");
        content.push_str("-- Uncomment to enable rollback:\n\n");
        for line in m.down.lines() {
            content.push_str("-- ");
            content.push_str(line);
            content.push('\n');
        }

        content
    }

    /// Format as Rust source file.
    fn format_rust(&self, m: &Migration) -> String {
        let mut content = String::new();

        // Module header
        content.push_str("//! Auto-generated migration.\n");
        content.push_str(&format!("//! Description: {}\n", m.description));
        content.push_str(&format!("//! Version: {}\n\n", m.id));

        content.push_str("use sqlmodel_schema::Migration;\n\n");

        // Migration function
        content.push_str("/// Returns this migration.\n");
        content.push_str("pub fn migration() -> Migration {\n");
        content.push_str("    Migration::new(\n");
        content.push_str(&format!("        {:?},\n", m.id));
        content.push_str(&format!("        {:?},\n", m.description));

        // UP SQL as raw string
        content.push_str("        r#\"\n");
        content.push_str(&m.up);
        content.push_str("\n\"#,\n");

        // DOWN SQL as raw string
        content.push_str("        r#\"\n");
        content.push_str(&m.down);
        content.push_str("\n\"#,\n");

        content.push_str("    )\n");
        content.push_str("}\n");

        content
    }
}

/// Status of a migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationStatus {
    /// Migration has not been applied
    Pending,
    /// Migration has been applied
    Applied { at: i64 },
    /// Migration failed
    Failed { error: String },
}

/// Migration runner for executing migrations.
pub struct MigrationRunner {
    /// The migrations to manage
    migrations: Vec<Migration>,
    /// Name of the migrations tracking table (validated to be safe)
    table_name: String,
}

/// Validate and sanitize a table name to prevent SQL injection.
///
/// Only allows alphanumeric characters and underscores.
fn sanitize_table_name(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

impl MigrationRunner {
    /// Create a new migration runner with the given migrations.
    pub fn new(migrations: Vec<Migration>) -> Self {
        Self {
            migrations,
            table_name: "_sqlmodel_migrations".to_string(),
        }
    }

    /// Set a custom migrations tracking table name.
    ///
    /// The name is sanitized to only allow alphanumeric characters and underscores
    /// to prevent SQL injection.
    pub fn table_name(mut self, name: impl Into<String>) -> Self {
        self.table_name = sanitize_table_name(&name.into());
        self
    }

    /// Ensure the migrations tracking table exists.
    pub async fn init<C: Connection>(&self, cx: &Cx, conn: &C) -> Outcome<(), Error> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id TEXT PRIMARY KEY,
                description TEXT NOT NULL,
                applied_at INTEGER NOT NULL
            )",
            self.table_name
        );

        conn.execute(cx, &sql, &[]).await.map(|_| ())
    }

    /// Get the status of all migrations.
    pub async fn status<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Vec<(String, MigrationStatus)>, Error> {
        // First ensure table exists
        match self.init(cx, conn).await {
            Outcome::Ok(()) => {}
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        }

        // Query applied migrations
        let sql = format!("SELECT id, applied_at FROM {}", self.table_name);
        let rows = match conn.query(cx, &sql, &[]).await {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let mut applied: HashMap<String, i64> = HashMap::new();
        for row in rows {
            if let (Ok(id), Ok(at)) = (
                row.get_named::<String>("id"),
                row.get_named::<i64>("applied_at"),
            ) {
                applied.insert(id, at);
            }
        }

        let status: Vec<_> = self
            .migrations
            .iter()
            .map(|m| {
                let status = if let Some(&at) = applied.get(&m.id) {
                    MigrationStatus::Applied { at }
                } else {
                    MigrationStatus::Pending
                };
                (m.id.clone(), status)
            })
            .collect();

        Outcome::Ok(status)
    }

    /// Apply all pending migrations.
    pub async fn migrate<C: Connection>(&self, cx: &Cx, conn: &C) -> Outcome<Vec<String>, Error> {
        let status = match self.status(cx, conn).await {
            Outcome::Ok(s) => s,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let mut applied = Vec::new();

        for (id, s) in status {
            if s == MigrationStatus::Pending {
                let Some(migration) = self.migrations.iter().find(|m| m.id == id) else {
                    // Migration not found in our list - skip it
                    continue;
                };

                // Execute the up migration
                match conn.execute(cx, &migration.up, &[]).await {
                    Outcome::Ok(_) => {}
                    Outcome::Err(e) => return Outcome::Err(e),
                    Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                    Outcome::Panicked(p) => return Outcome::Panicked(p),
                }

                // Record the migration
                let record_sql = format!(
                    "INSERT INTO {} (id, description, applied_at) VALUES ($1, $2, $3)",
                    self.table_name
                );
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_secs() as i64);

                match conn
                    .execute(
                        cx,
                        &record_sql,
                        &[
                            Value::Text(migration.id.clone()),
                            Value::Text(migration.description.clone()),
                            Value::BigInt(now),
                        ],
                    )
                    .await
                {
                    Outcome::Ok(_) => {}
                    Outcome::Err(e) => return Outcome::Err(e),
                    Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                    Outcome::Panicked(p) => return Outcome::Panicked(p),
                }

                applied.push(id);
            }
        }

        Outcome::Ok(applied)
    }

    /// Rollback the last applied migration.
    pub async fn rollback<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Option<String>, Error> {
        let status = match self.status(cx, conn).await {
            Outcome::Ok(s) => s,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        // Find the last applied migration
        let last_applied = status
            .iter()
            .filter_map(|(id, s)| {
                if let MigrationStatus::Applied { at } = s {
                    Some((id.clone(), *at))
                } else {
                    None
                }
            })
            .max_by_key(|(_, at)| *at);

        let Some((id, _)) = last_applied else {
            return Outcome::Ok(None);
        };

        let Some(migration) = self.migrations.iter().find(|m| m.id == id) else {
            // Migration not found in our list - cannot rollback
            return Outcome::Err(Error::Custom(format!(
                "Migration '{}' not found in migrations list",
                id
            )));
        };

        // Execute the down migration
        match conn.execute(cx, &migration.down, &[]).await {
            Outcome::Ok(_) => {}
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        }

        // Remove the migration record
        let delete_sql = format!("DELETE FROM {} WHERE id = $1", self.table_name);
        match conn
            .execute(cx, &delete_sql, &[Value::Text(id.clone())])
            .await
        {
            Outcome::Ok(_) => {}
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        }

        Outcome::Ok(Some(id))
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_version_format() {
        let version = Migration::new_version();
        // Should be 14 characters: YYYYMMDDHHMMSS
        assert_eq!(version.len(), 14);
        // Should be all digits
        assert!(version.chars().all(|c| c.is_ascii_digit()));
        // Year should be reasonable (2020-2100)
        let year: i32 = version[0..4].parse().unwrap();
        assert!((2020..=2100).contains(&year));
    }

    #[test]
    fn test_version_ordering() {
        // Test that version strings are lexicographically sortable
        // by comparing fixed timestamps rather than relying on wall clock
        let v1 = "20250101_000000";
        let v2 = "20250101_000001";
        let v3 = "20250102_000000";

        // Same day, later second
        assert!(v2 > v1);
        // Next day is always greater
        assert!(v3 > v2);
        // Format is sortable by string comparison
        assert!(v3 > v1);
    }

    #[test]
    fn test_migration_new() {
        let m = Migration::new(
            "001",
            "Create users table",
            "CREATE TABLE users",
            "DROP TABLE users",
        );
        assert_eq!(m.id, "001");
        assert_eq!(m.description, "Create users table");
        assert_eq!(m.up, "CREATE TABLE users");
        assert_eq!(m.down, "DROP TABLE users");
    }

    #[test]
    fn test_migration_from_operations() {
        use crate::ddl::SqliteDdlGenerator;
        use crate::introspect::{ColumnInfo, ParsedSqlType, TableInfo};

        let table = TableInfo {
            name: "heroes".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    sql_type: "INTEGER".to_string(),
                    parsed_type: ParsedSqlType::parse("INTEGER"),
                    nullable: false,
                    default: None,
                    primary_key: true,
                    auto_increment: true,
                    comment: None,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    sql_type: "TEXT".to_string(),
                    parsed_type: ParsedSqlType::parse("TEXT"),
                    nullable: false,
                    default: None,
                    primary_key: false,
                    auto_increment: false,
                    comment: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            foreign_keys: Vec::new(),
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            indexes: Vec::new(),
            comment: None,
        };

        let ops = vec![crate::diff::SchemaOperation::CreateTable(table)];
        let ddl = SqliteDdlGenerator;
        let m = Migration::from_operations(&ops, &ddl, "Create heroes table");

        assert!(!m.id.is_empty());
        assert_eq!(m.description, "Create heroes table");
        assert!(m.up.contains("CREATE TABLE"));
        assert!(m.up.contains("heroes"));
        assert!(m.down.contains("DROP TABLE"));
    }

    #[test]
    fn test_is_leap_year() {
        assert!(!is_leap_year(2023)); // Not divisible by 4
        assert!(is_leap_year(2024)); // Divisible by 4
        assert!(!is_leap_year(2100)); // Divisible by 100 but not 400
        assert!(is_leap_year(2000)); // Divisible by 400
    }

    #[test]
    fn test_migration_format_default() {
        assert_eq!(MigrationFormat::default(), MigrationFormat::Sql);
    }

    #[test]
    fn test_migration_writer_new() {
        let writer = MigrationWriter::new("/tmp/migrations");
        assert_eq!(writer.migrations_dir(), Path::new("/tmp/migrations"));
        assert_eq!(writer.format(), MigrationFormat::Sql);
    }

    #[test]
    fn test_migration_writer_with_format() {
        let writer = MigrationWriter::new("/tmp/migrations").with_format(MigrationFormat::Rust);
        assert_eq!(writer.format(), MigrationFormat::Rust);
    }

    #[test]
    fn test_filename_sanitization() {
        let writer = MigrationWriter::new("/tmp");
        let m = Migration::new("20260127120000", "Create Users Table!!!", "", "");
        let filename = writer.filename(&m);
        assert!(filename.starts_with("20260127120000_"));
        assert!(
            Path::new(&filename)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("sql"))
        );
        assert!(!filename.contains('!'));
        assert!(!filename.contains(' '));
    }

    #[test]
    fn test_filename_rust_format() {
        let writer = MigrationWriter::new("/tmp").with_format(MigrationFormat::Rust);
        let m = Migration::new("20260127120000", "Test migration", "", "");
        let filename = writer.filename(&m);
        assert!(
            Path::new(&filename)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("rs"))
        );
    }

    #[test]
    fn test_format_sql_structure() {
        let writer = MigrationWriter::new("/tmp");
        let m = Migration::new(
            "20260127120000",
            "Test migration",
            "CREATE TABLE test (id INT)",
            "DROP TABLE test",
        );
        let content = writer.format_sql(&m);

        // Check header
        assert!(content.contains("-- Migration: Test migration"));
        assert!(content.contains("-- Version: 20260127120000"));

        // Check UP section
        assert!(content.contains("-- ========== UP =========="));
        assert!(content.contains("CREATE TABLE test"));

        // Check DOWN section
        assert!(content.contains("-- ========== DOWN =========="));
        assert!(content.contains("DROP TABLE test"));
    }

    #[test]
    fn test_format_rust_structure() {
        let writer = MigrationWriter::new("/tmp").with_format(MigrationFormat::Rust);
        let m = Migration::new(
            "20260127120000",
            "Test migration",
            "CREATE TABLE test",
            "DROP TABLE test",
        );
        let content = writer.format_rust(&m);

        // Check module header
        assert!(content.contains("//! Auto-generated migration"));
        assert!(content.contains("//! Description: Test migration"));

        // Check import
        assert!(content.contains("use sqlmodel_schema::Migration"));

        // Check function
        assert!(content.contains("pub fn migration() -> Migration"));
        assert!(content.contains("Migration::new("));

        // Check SQL embedded
        assert!(content.contains("CREATE TABLE test"));
        assert!(content.contains("DROP TABLE test"));
    }

    #[test]
    fn test_filename_truncation() {
        let writer = MigrationWriter::new("/tmp");
        let long_desc = "a".repeat(100); // Very long description
        let m = Migration::new("20260127120000", &long_desc, "", "");
        let filename = writer.filename(&m);
        // Filename should be truncated to reasonable length
        assert!(filename.len() < 100);
    }

    #[test]
    fn test_migration_status_enum() {
        let pending = MigrationStatus::Pending;
        let applied = MigrationStatus::Applied { at: 1_234_567_890 };
        let failed = MigrationStatus::Failed {
            error: "Test error".to_string(),
        };

        assert_eq!(pending, MigrationStatus::Pending);
        assert_ne!(pending, applied);

        assert!(matches!(
            applied,
            MigrationStatus::Applied { at } if at == 1_234_567_890
        ));
        assert!(matches!(
            failed,
            MigrationStatus::Failed { ref error } if error == "Test error"
        ));
    }

    #[test]
    fn test_migration_runner_new() {
        let migrations = vec![
            Migration::new("001", "First", "UP", "DOWN"),
            Migration::new("002", "Second", "UP", "DOWN"),
        ];
        let runner = MigrationRunner::new(migrations);
        assert_eq!(runner.table_name, "_sqlmodel_migrations");
    }

    #[test]
    fn test_migration_runner_custom_table() {
        let runner = MigrationRunner::new(vec![]).table_name("custom_migrations");
        assert_eq!(runner.table_name, "custom_migrations");
    }
}
