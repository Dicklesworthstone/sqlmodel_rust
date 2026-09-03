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
use sqlmodel_core::{
    Connection, Error, TransactionMode, TransactionOps, TransactionOptions, Value,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

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

    /// Fingerprint of the `up` SQL (64-bit FNV-1a, 16 hex digits).
    ///
    /// Recorded in the tracking table when the migration is applied and
    /// compared on every later run, so a migration edited after it ran is
    /// reported as [`MigrationStatus::Drifted`] instead of silently trusted.
    #[must_use]
    pub fn checksum(&self) -> String {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in self.up.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        format!("{hash:016x}")
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

/// Split a migration script into its statements on top-level semicolons.
///
/// Single- and double-quoted strings, backtick identifiers, `--` line
/// comments, `/* */` block comments, and PostgreSQL dollar quoting (`$$`,
/// `$tag$`) are respected, so a semicolon inside any of them does not split.
/// Statements that contain nothing but whitespace or comments are dropped
/// (MySQL rejects an empty query).
#[must_use]
pub fn split_statements(sql: &str) -> Vec<String> {
    let chars: Vec<char> = sql.chars().collect();
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut has_content = false;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '-' && chars.get(i + 1) == Some(&'-') {
            while i < chars.len() && chars[i] != '\n' {
                current.push(chars[i]);
                i += 1;
            }
            continue;
        }
        if c == '/' && chars.get(i + 1) == Some(&'*') {
            let start = i;
            i += 2;
            while i < chars.len() && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                i += 1;
            }
            i = (i + 2).min(chars.len());
            current.extend(&chars[start..i]);
            continue;
        }
        if c == '\'' || c == '"' || c == '`' {
            has_content = true;
            current.push(c);
            i += 1;
            while i < chars.len() {
                current.push(chars[i]);
                if chars[i] == c {
                    if chars.get(i + 1) == Some(&c) {
                        current.push(c);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if c == '$' {
            let mut j = i + 1;
            while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            let is_tag =
                chars.get(j) == Some(&'$') && (j == i + 1 || !chars[i + 1].is_ascii_digit());
            if is_tag {
                has_content = true;
                let tag: Vec<char> = chars[i..=j].to_vec();
                current.extend(&tag);
                i = j + 1;
                while i < chars.len() {
                    if chars[i] == '$' && chars[i..].starts_with(&tag) {
                        current.extend(&tag);
                        i += tag.len();
                        break;
                    }
                    current.push(chars[i]);
                    i += 1;
                }
                continue;
            }
        }
        if c == ';' {
            if has_content {
                statements.push(current.trim().to_string());
            }
            current.clear();
            has_content = false;
            i += 1;
            continue;
        }
        if !c.is_whitespace() {
            has_content = true;
        }
        current.push(c);
        i += 1;
    }
    if has_content {
        statements.push(current.trim().to_string());
    }
    statements
}

/// Whether a top-level statement starts, commits, or rolls back a
/// transaction itself (`BEGIN`, `COMMIT`, `END`, `ROLLBACK`). A `BEGIN`
/// inside a dollar-quoted function body is not a top-level statement and
/// does not count.
fn statement_controls_transaction(statement: &str) -> bool {
    let first = statement
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == ';')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    matches!(first.as_str(), "BEGIN" | "COMMIT" | "END" | "ROLLBACK")
}

/// Which script of a migration to run.
#[derive(Debug, Clone, Copy)]
enum ScriptDirection {
    Up,
    Down,
}

impl ScriptDirection {
    const fn name(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
        }
    }
}

/// The error for a statement that failed inside a migration: names the
/// migration, the direction, and the statement, and keeps the driver error
/// as the source.
fn migration_error(
    id: &str,
    direction: &str,
    index: usize,
    statement: &str,
    source: Error,
) -> Error {
    let preview: String = statement.chars().take(80).collect();
    let ellipsis = if statement.chars().count() > 80 {
        "…"
    } else {
        ""
    };
    Error::Schema(sqlmodel_core::error::SchemaError {
        kind: sqlmodel_core::error::SchemaErrorKind::Migration,
        message: format!(
            "migration `{id}` {direction} failed at statement {} (`{preview}{ellipsis}`): {source}",
            index + 1
        ),
        source: Some(Box::new(source)),
    })
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
    /// Applied, but its `up` SQL has changed since: the recorded checksum
    /// differs from [`Migration::checksum`] of the current definition.
    /// `migrate` refuses to run while any migration is in this state.
    Drifted {
        at: i64,
        recorded: String,
        current: String,
    },
}

/// Migration runner for executing migrations.
pub struct MigrationRunner {
    /// The migrations to manage
    migrations: Vec<Migration>,
    /// Name of the migrations tracking table (validated to be safe)
    table_name: String,
    /// How long `migrate`/`rollback` wait for the server-side migration lock.
    lock_timeout: Duration,
}

/// Default wait for the migration lock held by another runner.
pub const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(120);

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
            lock_timeout: DEFAULT_LOCK_TIMEOUT,
        }
    }

    /// How long to wait for the migration lock another runner holds before
    /// failing (default [`DEFAULT_LOCK_TIMEOUT`], two minutes). On PostgreSQL
    /// the runner polls `pg_try_advisory_lock` until the deadline; on MySQL
    /// the wait is `GET_LOCK`'s own timeout. SQLite holds no server lock.
    /// `Duration::ZERO` fails immediately when the lock is taken.
    #[must_use]
    pub fn lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = timeout;
        self
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
    ///
    /// The `id` column is `VARCHAR(255)` rather than `TEXT`: MySQL refuses a
    /// `TEXT` primary key without a key length, while SQLite and PostgreSQL treat
    /// `VARCHAR(255)` as text. Migration ids are short (timestamps or slugs).
    pub async fn init<C: Connection>(&self, cx: &Cx, conn: &C) -> Outcome<(), Error> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id VARCHAR(255) PRIMARY KEY,
                description TEXT NOT NULL,
                applied_at BIGINT NOT NULL,
                checksum VARCHAR(64) NOT NULL DEFAULT ''
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
        let sql = format!("SELECT id, applied_at, checksum FROM {}", self.table_name);
        let rows = match conn.query(cx, &sql, &[]).await {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let mut applied: HashMap<String, (i64, String)> = HashMap::new();
        for row in rows {
            if let (Ok(id), Ok(at)) = (
                row.get_named::<String>("id"),
                row.get_named::<i64>("applied_at"),
            ) {
                let recorded = row.get_named::<String>("checksum").unwrap_or_default();
                applied.insert(id, (at, recorded));
            }
        }

        let status: Vec<_> = self
            .migrations
            .iter()
            .map(|m| {
                let status = match applied.get(&m.id) {
                    None => MigrationStatus::Pending,
                    Some((at, recorded)) => {
                        let current = m.checksum();
                        // An empty recorded checksum is a row written before
                        // checksums existed; it cannot be verified.
                        if recorded.is_empty() || *recorded == current {
                            MigrationStatus::Applied { at: *at }
                        } else {
                            MigrationStatus::Drifted {
                                at: *at,
                                recorded: recorded.clone(),
                                current,
                            }
                        }
                    }
                };
                (m.id.clone(), status)
            })
            .collect();

        Outcome::Ok(status)
    }

    /// Run one migration script (its statements one at a time) and then the
    /// tracking-table statement, in one transaction where the dialect has
    /// transactional DDL.
    async fn run_script<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        migration: &Migration,
        direction: ScriptDirection,
        record_sql: &str,
        record_params: &[Value],
    ) -> Outcome<bool, Error> {
        let id = migration.id.as_str();
        let script = match direction {
            ScriptDirection::Up => migration.up.as_str(),
            ScriptDirection::Down => migration.down.as_str(),
        };
        let direction_kind = direction;
        let direction = direction.name();
        let statements = split_statements(script);
        // A script that manages its own transaction (SQLite table recreation
        // emits `PRAGMA foreign_keys=OFF; BEGIN; ...; COMMIT; PRAGMA
        // foreign_keys=ON`) runs as written: nesting it in another transaction
        // fails at its BEGIN, and the PRAGMA has no effect inside one. Its
        // tracking row is then written after the script, outside it.
        let owns_transaction = statements.iter().any(|s| statement_controls_transaction(s));
        if conn.dialect().supports_transactional_ddl() && !owns_transaction {
            // On SQLite take the write lock at BEGIN (IMMEDIATE) so two
            // runners serialize here instead of conflicting at COMMIT.
            let options = if conn.dialect() == sqlmodel_core::Dialect::Sqlite {
                TransactionOptions::default().with_mode(TransactionMode::Immediate)
            } else {
                TransactionOptions::default()
            };
            let tx = match conn.begin_with_options(cx, options).await {
                Outcome::Ok(tx) => tx,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };
            // Re-check under the lock: another runner may have applied (or
            // rolled back) this migration between our status read and BEGIN.
            let check_sql = format!(
                "SELECT 1 FROM {} WHERE id = {}",
                self.table_name,
                conn.dialect().placeholder(1)
            );
            let recorded = match tx
                .query(cx, &check_sql, &[Value::Text(id.to_string())])
                .await
            {
                Outcome::Ok(rows) => !rows.is_empty(),
                Outcome::Err(e) => {
                    let _ = tx.rollback(cx).await;
                    return Outcome::Err(e);
                }
                Outcome::Cancelled(r) => {
                    let _ = tx.rollback(cx).await;
                    return Outcome::Cancelled(r);
                }
                Outcome::Panicked(p) => {
                    let _ = tx.rollback(cx).await;
                    return Outcome::Panicked(p);
                }
            };
            let already_done = match direction_kind {
                ScriptDirection::Up => recorded,
                ScriptDirection::Down => !recorded,
            };
            if already_done {
                let _ = tx.rollback(cx).await;
                return Outcome::Ok(false);
            }
            for (index, statement) in statements.iter().enumerate() {
                match tx.execute(cx, statement, &[]).await {
                    Outcome::Ok(_) => {}
                    Outcome::Err(e) => {
                        let _ = tx.rollback(cx).await;
                        return Outcome::Err(migration_error(id, direction, index, statement, e));
                    }
                    Outcome::Cancelled(r) => {
                        let _ = tx.rollback(cx).await;
                        return Outcome::Cancelled(r);
                    }
                    Outcome::Panicked(p) => {
                        let _ = tx.rollback(cx).await;
                        return Outcome::Panicked(p);
                    }
                }
            }
            match tx.execute(cx, record_sql, record_params).await {
                Outcome::Ok(_) => {}
                Outcome::Err(e) => {
                    let _ = tx.rollback(cx).await;
                    return Outcome::Err(e);
                }
                Outcome::Cancelled(r) => {
                    let _ = tx.rollback(cx).await;
                    return Outcome::Cancelled(r);
                }
                Outcome::Panicked(p) => {
                    let _ = tx.rollback(cx).await;
                    return Outcome::Panicked(p);
                }
            }
            return tx.commit(cx).await.map(|()| true);
        }

        // No transactional DDL (MySQL): statements before a failure stay
        // applied and no tracking row is written; see `migrate`.
        for (index, statement) in statements.iter().enumerate() {
            match conn.execute(cx, statement, &[]).await {
                Outcome::Ok(_) => {}
                Outcome::Err(e) => {
                    return Outcome::Err(migration_error(id, direction, index, statement, e));
                }
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            }
        }
        conn.execute(cx, record_sql, record_params)
            .await
            .map(|_| true)
    }

    /// Key for the server-side lock that serializes runners on one database:
    /// a hash of the tracking table name, so runners sharing a history table
    /// exclude each other and unrelated ones do not.
    fn lock_key(&self) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in self.table_name.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        hash
    }

    /// Take the migration lock: `pg_advisory_lock` on PostgreSQL and
    /// `GET_LOCK` on MySQL, both session-level and held until
    /// [`Self::release_lock`]. SQLite has no server to hold a lock; there the
    /// database file lock and the tracking table's primary key keep a second
    /// runner from applying a migration twice, but it fails instead of
    /// waiting (see [`Self::migrate`]).
    /// The statement that tries to take the migration lock on `dialect`, with
    /// its parameters: `pg_try_advisory_lock(key)` (non-blocking; polled by
    /// [`Self::acquire_lock`]) or `GET_LOCK(name, seconds)` with the configured
    /// timeout. `None` on SQLite, which holds no server lock.
    fn lock_statement(&self, dialect: sqlmodel_core::Dialect) -> Option<(String, Vec<Value>)> {
        match dialect {
            sqlmodel_core::Dialect::Postgres => {
                #[allow(clippy::cast_possible_wrap)] // any bigint is a valid key
                let key = self.lock_key() as i64;
                Some((
                    "SELECT pg_try_advisory_lock($1)".to_string(),
                    vec![Value::BigInt(key)],
                ))
            }
            sqlmodel_core::Dialect::Mysql => {
                let name = format!("sqlmodel_migrate_{:016x}", self.lock_key());
                let seconds = i64::try_from(self.lock_timeout.as_secs()).unwrap_or(i64::MAX);
                Some((
                    "SELECT GET_LOCK(?, ?)".to_string(),
                    vec![Value::Text(name), Value::BigInt(seconds)],
                ))
            }
            sqlmodel_core::Dialect::Sqlite => None,
        }
    }

    async fn acquire_lock<C: Connection>(&self, cx: &Cx, conn: &C) -> Outcome<(), Error> {
        let dialect = conn.dialect();
        let Some((sql, params)) = self.lock_statement(dialect) else {
            return Outcome::Ok(());
        };
        let deadline = Instant::now() + self.lock_timeout;
        loop {
            let rows = match conn.query(cx, &sql, &params).await {
                Outcome::Ok(rows) => rows,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };
            // `pg_try_advisory_lock` returns a boolean, `GET_LOCK` 1/0/NULL.
            let granted = rows
                .first()
                .and_then(|r| r.get(0))
                .is_some_and(|v| matches!(v, Value::Bool(true)) || v.as_i64() == Some(1));
            if granted {
                return Outcome::Ok(());
            }
            // MySQL already waited inside GET_LOCK; PostgreSQL's try-lock
            // returns at once, so poll until the deadline.
            if dialect != sqlmodel_core::Dialect::Postgres || Instant::now() >= deadline {
                return Outcome::Err(Error::Custom(format!(
                    "could not acquire the migration lock for `{}` within {:?}; \
                     another runner is still migrating this database",
                    self.table_name, self.lock_timeout
                )));
            }
            if let Some(reason) = sqlmodel_core::cancel_requested(cx) {
                return Outcome::Cancelled(reason);
            }
            asupersync::time::sleep(cx.now(), Duration::from_millis(100)).await;
        }
    }

    /// Release the lock taken by [`Self::acquire_lock`]. Best effort: a
    /// session-level lock also goes away with the connection.
    async fn release_lock<C: Connection>(&self, cx: &Cx, conn: &C) {
        match conn.dialect() {
            sqlmodel_core::Dialect::Postgres => {
                #[allow(clippy::cast_possible_wrap)]
                let key = self.lock_key() as i64;
                let _ = conn
                    .execute(cx, "SELECT pg_advisory_unlock($1)", &[Value::BigInt(key)])
                    .await;
            }
            sqlmodel_core::Dialect::Mysql => {
                let name = format!("sqlmodel_migrate_{:016x}", self.lock_key());
                let _ = conn
                    .query(cx, "SELECT RELEASE_LOCK(?)", &[Value::Text(name)])
                    .await;
            }
            sqlmodel_core::Dialect::Sqlite => {}
        }
    }

    /// Apply all pending migrations, in order, and return the ids applied.
    ///
    /// Only one runner at a time migrates a given database: on PostgreSQL and
    /// MySQL the call holds a server-side lock keyed by the tracking table
    /// (`pg_try_advisory_lock` polled / `GET_LOCK`, waiting up to
    /// [`Self::lock_timeout`], two minutes by default),
    /// so a second runner waits and then finds nothing pending. SQLite has no
    /// server to hold such a lock; a second runner there fails on the database
    /// file lock or the tracking table's primary key instead of waiting, and
    /// can simply be run again. Either way no migration is applied twice.
    ///
    /// Each migration's statements are split on top-level semicolons (see
    /// [`split_statements`]) and run one at a time: the PostgreSQL extended
    /// protocol and MySQL's text protocol do not accept several statements in
    /// one `execute`, so a multi-statement migration such as the output of
    /// [`Migration::from_operations`] would otherwise never run there.
    ///
    /// On dialects with transactional DDL (PostgreSQL, SQLite) a migration and
    /// its tracking row are applied in one transaction: a failing statement
    /// rolls the whole migration back and the database is unchanged. On MySQL
    /// every DDL statement commits implicitly, so a failing statement leaves the
    /// statements before it applied and writes no tracking row; the error names
    /// the migration and the statement, and the next `migrate` runs the
    /// migration again from its first statement. Write MySQL migrations so each
    /// statement can be repeated (`CREATE TABLE IF NOT EXISTS`, `DROP ... IF
    /// EXISTS`) or keep one DDL statement per migration.
    ///
    /// Refuses to run while any applied migration is
    /// [`MigrationStatus::Drifted`].
    pub async fn migrate<C: Connection>(&self, cx: &Cx, conn: &C) -> Outcome<Vec<String>, Error> {
        match self.acquire_lock(cx, conn).await {
            Outcome::Ok(()) => {}
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        }
        let result = self.migrate_locked(cx, conn).await;
        self.release_lock(cx, conn).await;
        result
    }

    /// [`Self::migrate`] with the lock already held.
    async fn migrate_locked<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Vec<String>, Error> {
        let status = match self.status(cx, conn).await {
            Outcome::Ok(s) => s,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        // Refuse to run anything while an applied migration no longer matches
        // what was applied: the history is no longer trustworthy.
        if let Some((
            id,
            MigrationStatus::Drifted {
                recorded, current, ..
            },
        )) = status
            .iter()
            .find(|(_, s)| matches!(s, MigrationStatus::Drifted { .. }))
        {
            return Outcome::Err(Error::Custom(format!(
                "migration `{id}` was modified after it was applied (recorded checksum \
                 {recorded}, current {current}); refusing to run migrations"
            )));
        }

        let mut applied = Vec::new();

        for (id, s) in status {
            if s == MigrationStatus::Pending {
                let Some(migration) = self.migrations.iter().find(|m| m.id == id) else {
                    // Migration not found in our list - skip it
                    continue;
                };

                // The tracking row. Placeholders must follow the connection's
                // dialect: MySQL rejects `$1`, and SQLite only accepted it by treating
                // `$1` as a named parameter.
                let dialect = conn.dialect();
                let record_sql = format!(
                    "INSERT INTO {} (id, description, applied_at, checksum) VALUES ({}, {}, {}, {})",
                    self.table_name,
                    dialect.placeholder(1),
                    dialect.placeholder(2),
                    dialect.placeholder(3),
                    dialect.placeholder(4)
                );
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_secs() as i64);
                let record_params = [
                    Value::Text(migration.id.clone()),
                    Value::Text(migration.description.clone()),
                    Value::BigInt(now),
                    Value::Text(migration.checksum()),
                ];

                match self
                    .run_script(
                        cx,
                        conn,
                        migration,
                        ScriptDirection::Up,
                        &record_sql,
                        &record_params,
                    )
                    .await
                {
                    Outcome::Ok(true) => applied.push(id),
                    // Another runner got there first; nothing to do.
                    Outcome::Ok(false) => {}
                    Outcome::Err(e) => return Outcome::Err(e),
                    Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                    Outcome::Panicked(p) => return Outcome::Panicked(p),
                }
            }
        }

        Outcome::Ok(applied)
    }

    /// Roll back the last applied migration and return its id, or `None` when
    /// nothing is applied. Holds the same lock as [`Self::migrate`].
    pub async fn rollback<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Option<String>, Error> {
        match self.acquire_lock(cx, conn).await {
            Outcome::Ok(()) => {}
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        }
        let result = self.rollback_locked(cx, conn).await;
        self.release_lock(cx, conn).await;
        result
    }

    /// [`Self::rollback`] with the lock already held.
    async fn rollback_locked<C: Connection>(
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
                // A drifted migration can still be rolled back; that is how
                // drift gets resolved.
                if let MigrationStatus::Applied { at } | MigrationStatus::Drifted { at, .. } = s {
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

        // The down script and the removal of the tracking row, with the same
        // statement splitting and transaction rules as `migrate`.
        let delete_sql = format!(
            "DELETE FROM {} WHERE id = {}",
            self.table_name,
            conn.dialect().placeholder(1)
        );
        match self
            .run_script(
                cx,
                conn,
                migration,
                ScriptDirection::Down,
                &delete_sql,
                &[Value::Text(id.clone())],
            )
            .await
        {
            Outcome::Ok(true) => {}
            // Another runner rolled it back first.
            Outcome::Ok(false) => return Outcome::Ok(None),
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
    fn split_statements_respects_quotes_comments_and_dollar_quoting() {
        assert_eq!(
            split_statements("CREATE TABLE a (id INT);\n\nINSERT INTO a VALUES (1);"),
            vec!["CREATE TABLE a (id INT)", "INSERT INTO a VALUES (1)"]
        );
        assert_eq!(split_statements("SELECT 1"), vec!["SELECT 1"]);
        assert_eq!(split_statements("   ;  ; \n"), Vec::<String>::new());
        assert_eq!(
            split_statements("INSERT INTO t VALUES ('a;b', \"c;d\", `e;f`); SELECT 'it''s;'"),
            vec![
                "INSERT INTO t VALUES ('a;b', \"c;d\", `e;f`)",
                "SELECT 'it''s;'"
            ]
        );
        // Comments never split and are kept with the statement they precede;
        // a trailing comment-only fragment is dropped.
        assert_eq!(
            split_statements(
                "SELECT 1; -- trailing; comment\n/* block; comment */ SELECT 2; -- only a comment"
            ),
            vec![
                "SELECT 1",
                "-- trailing; comment\n/* block; comment */ SELECT 2"
            ]
        );
        assert_eq!(
            split_statements(
                "CREATE FUNCTION f() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql; SELECT f()"
            ),
            vec![
                "CREATE FUNCTION f() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql",
                "SELECT f()"
            ]
        );
        assert_eq!(
            split_statements("DO $body$ BEGIN PERFORM 1; END $body$; SELECT $1; SELECT 2"),
            vec![
                "DO $body$ BEGIN PERFORM 1; END $body$",
                "SELECT $1",
                "SELECT 2"
            ]
        );
        // `from_operations` output: statements joined with ";\n\n" plus a trailing ";".
        let m = Migration::new("1", "d", "A;\n\nB;", "");
        assert_eq!(split_statements(&m.up), vec!["A", "B"]);
    }

    #[test]
    fn scripts_that_manage_their_own_transaction_are_detected() {
        for s in [
            "BEGIN",
            "begin transaction",
            "COMMIT",
            "END",
            "ROLLBACK",
            "  Begin;",
        ] {
            assert!(statement_controls_transaction(s), "{s}");
        }
        for s in [
            "CREATE TABLE t (id INT)",
            "PRAGMA foreign_keys=OFF",
            "DO $$ BEGIN PERFORM 1; END $$",
            "SELECT 'BEGIN'",
        ] {
            assert!(!statement_controls_transaction(s), "{s}");
        }
    }

    #[test]
    fn migration_error_names_migration_direction_and_statement() {
        let e = migration_error(
            "0004_partial",
            "up",
            2,
            "INSERT INTO missing (id) VALUES (1)",
            Error::Custom("no such table".into()),
        );
        let text = e.to_string();
        assert!(text.contains("0004_partial"), "{text}");
        assert!(text.contains("up failed at statement 3"), "{text}");
        assert!(text.contains("no such table"), "{text}");
        assert!(matches!(
            e,
            Error::Schema(sqlmodel_core::error::SchemaError {
                kind: sqlmodel_core::error::SchemaErrorKind::Migration,
                ..
            })
        ));
    }

    #[test]
    fn checksum_is_stable_and_tracks_the_up_sql_only() {
        let a = Migration::new(
            "0001",
            "create",
            "CREATE TABLE t (id INTEGER)",
            "DROP TABLE t",
        );
        let same = Migration::new(
            "0001",
            "other description",
            "CREATE TABLE t (id INTEGER)",
            "",
        );
        let edited = Migration::new(
            "0001",
            "create",
            "CREATE TABLE t (id BIGINT)",
            "DROP TABLE t",
        );
        assert_eq!(a.checksum().len(), 16);
        assert_eq!(
            a.checksum(),
            same.checksum(),
            "id/description/down do not count"
        );
        assert_ne!(a.checksum(), edited.checksum(), "the up SQL does");
        assert_eq!(a.checksum(), a.checksum(), "deterministic");
    }

    #[test]
    fn test_migration_status_enum() {
        let pending = MigrationStatus::Pending;
        let applied = MigrationStatus::Applied { at: 1_234_567_890 };

        assert_eq!(pending, MigrationStatus::Pending);
        assert_ne!(pending, applied);

        assert!(matches!(
            applied,
            MigrationStatus::Applied { at } if at == 1_234_567_890
        ));
    }

    #[test]
    fn lock_statement_carries_the_configured_timeout() {
        let runner = MigrationRunner::new(vec![]);
        assert_eq!(runner.lock_timeout, DEFAULT_LOCK_TIMEOUT);
        let (sql, params) = runner
            .lock_statement(sqlmodel_core::Dialect::Mysql)
            .expect("mysql locks");
        assert_eq!(sql, "SELECT GET_LOCK(?, ?)");
        assert_eq!(params[1], Value::BigInt(120), "default wait is two minutes");

        let quick = MigrationRunner::new(vec![]).lock_timeout(Duration::from_secs(5));
        let (_, params) = quick
            .lock_statement(sqlmodel_core::Dialect::Mysql)
            .expect("mysql locks");
        assert_eq!(params[1], Value::BigInt(5));
        let (sql, params) = quick
            .lock_statement(sqlmodel_core::Dialect::Postgres)
            .expect("postgres locks");
        assert_eq!(sql, "SELECT pg_try_advisory_lock($1)");
        assert!(matches!(params[0], Value::BigInt(_)));
        assert!(
            quick
                .lock_statement(sqlmodel_core::Dialect::Sqlite)
                .is_none(),
            "SQLite holds no server lock"
        );

        // Two runners on the same tracking table contend for the same key;
        // different tables do not.
        let same = MigrationRunner::new(vec![]).table_name("_sqlmodel_migrations");
        assert_eq!(runner.lock_key(), same.lock_key());
        let other = MigrationRunner::new(vec![]).table_name("other_history");
        assert_ne!(runner.lock_key(), other.lock_key());
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
