//! Database connection traits.
//!
//! This module defines the core abstractions for database connections:
//!
//! - [`Connection`] - Main trait for executing queries and managing transactions
//! - [`Transaction`] - Trait for transactional operations with savepoint support
//! - [`IsolationLevel`] - SQL transaction isolation levels
//! - [`PreparedStatement`] - Pre-compiled statement for efficient repeated execution
//!
//! All operations integrate with asupersync's structured concurrency via `Cx` context
//! for proper cancellation and timeout handling.

use crate::error::Result;
use crate::row::Row;
use crate::value::Value;
use asupersync::{Cx, Outcome};

/// Transaction isolation level.
///
/// Defines the degree to which one transaction must be isolated from
/// resource or data modifications made by other concurrent transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Read uncommitted: Transactions can see uncommitted changes from others.
    /// This is the lowest isolation level, providing minimal guarantees.
    /// Use with caution - dirty reads, non-repeatable reads, and phantoms possible.
    ReadUncommitted,

    /// Read committed: Transactions only see committed changes from others.
    /// This is the default for PostgreSQL. Prevents dirty reads but allows
    /// non-repeatable reads and phantoms.
    #[default]
    ReadCommitted,

    /// Repeatable read: Transactions see a consistent snapshot of the database.
    /// Prevents dirty reads and non-repeatable reads, but phantoms are possible
    /// in some databases (though not in PostgreSQL).
    RepeatableRead,

    /// Serializable: Transactions appear to execute sequentially.
    /// The highest isolation level, providing complete isolation but potentially
    /// requiring retries due to serialization failures.
    Serializable,
}

impl IsolationLevel {
    /// Get the SQL syntax for this isolation level.
    #[must_use]
    pub const fn as_sql(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }
}

/// A prepared statement for repeated execution.
///
/// Prepared statements are pre-compiled by the database, allowing efficient
/// repeated execution with different parameter values. They also help prevent
/// SQL injection since parameters are handled separately from the SQL text.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// Unique identifier for this prepared statement (driver-specific)
    id: u64,
    /// The original SQL text
    sql: String,
    /// Number of expected parameters
    param_count: usize,
    /// Column information for result rows (if available)
    columns: Option<Vec<String>>,
}

impl PreparedStatement {
    /// Create a new prepared statement.
    ///
    /// This is typically called by the driver, not by users directly.
    #[must_use]
    pub fn new(id: u64, sql: String, param_count: usize) -> Self {
        Self {
            id,
            sql,
            param_count,
            columns: None,
        }
    }

    /// Create a prepared statement with column information.
    #[must_use]
    pub fn with_columns(id: u64, sql: String, param_count: usize, columns: Vec<String>) -> Self {
        Self {
            id,
            sql,
            param_count,
            columns: Some(columns),
        }
    }

    /// Get the statement ID.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Get the original SQL text.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Get the expected number of parameters.
    #[must_use]
    pub const fn param_count(&self) -> usize {
        self.param_count
    }

    /// Get the column information, if available.
    #[must_use]
    pub fn columns(&self) -> Option<&[String]> {
        self.columns.as_deref()
    }

    /// Check if the provided parameters match the expected count.
    #[must_use]
    pub fn validate_params(&self, params: &[Value]) -> bool {
        params.len() == self.param_count
    }
}

/// A database connection capable of executing queries.
///
/// All operations are async and take a `Cx` context for cancellation/timeout support.
/// Implementations must be `Send + Sync` for use across async boundaries.
///
/// # Transaction Support
///
/// Use [`begin`](Connection::begin) or [`begin_with`](Connection::begin_with) to
/// start transactions. Transactions must be explicitly committed or rolled back.
///
/// # Example
///
/// ```rust,ignore
/// // Execute a simple query
/// let rows = conn.query(&cx, "SELECT * FROM users WHERE id = $1", &[Value::Int(1)]).await?;
///
/// // Use a transaction
/// let mut tx = conn.begin(&cx).await?;
/// tx.execute(&cx, "INSERT INTO logs (msg) VALUES ($1)", &[Value::Text("action".into())]).await?;
/// tx.commit(&cx).await?;
/// ```
/// SQL dialect enumeration for cross-database compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Dialect {
    /// PostgreSQL dialect (uses $1, $2 placeholders)
    #[default]
    Postgres,
    /// SQLite dialect (uses ?1, ?2 placeholders)
    Sqlite,
    /// MySQL dialect (uses ? placeholders)
    Mysql,
}

impl Dialect {
    /// Generate a placeholder for the given parameter index (1-based).
    pub fn placeholder(self, index: usize) -> String {
        match self {
            Dialect::Postgres => format!("${index}"),
            Dialect::Sqlite => format!("?{index}"),
            Dialect::Mysql => "?".to_string(),
        }
    }

    /// Get the string concatenation operator for this dialect.
    pub const fn concat_op(self) -> &'static str {
        match self {
            Dialect::Postgres | Dialect::Sqlite => "||",
            Dialect::Mysql => "", // MySQL uses CONCAT() function
        }
    }

    /// Check if this dialect supports ILIKE.
    pub const fn supports_ilike(self) -> bool {
        matches!(self, Dialect::Postgres)
    }

    /// Quote an identifier for this dialect.
    ///
    /// Properly escapes embedded quote characters by doubling them:
    /// - For Postgres/SQLite: `"` becomes `""`
    /// - For MySQL: `` ` `` becomes ``` `` ```
    pub fn quote_identifier(self, name: &str) -> String {
        match self {
            Dialect::Postgres | Dialect::Sqlite => {
                let escaped = name.replace('"', "\"\"");
                format!("\"{escaped}\"")
            }
            Dialect::Mysql => {
                let escaped = name.replace('`', "``");
                format!("`{escaped}`")
            }
        }
    }
}

pub trait Connection: Send + Sync {
    /// The transaction type returned by this connection.
    type Tx<'conn>: TransactionOps
    where
        Self: 'conn;

    /// Get the SQL dialect for this connection.
    ///
    /// This is used by query builders to generate dialect-specific SQL.
    /// Defaults to Postgres for backwards compatibility.
    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    /// Execute a query and return all rows.
    fn query(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, crate::Error>> + Send;

    /// Execute a query and return the first row, if any.
    fn query_one(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Option<Row>, crate::Error>> + Send;

    /// Execute a statement (INSERT, UPDATE, DELETE) and return rows affected.
    fn execute(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<u64, crate::Error>> + Send;

    /// Execute an INSERT and return the last inserted ID.
    ///
    /// For PostgreSQL, this typically uses RETURNING to get the inserted ID.
    /// The exact behavior depends on the driver implementation.
    fn insert(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<i64, crate::Error>> + Send;

    /// Execute multiple statements in a batch.
    ///
    /// Returns the number of rows affected by each statement.
    /// The statements are executed sequentially but may be optimized
    /// by the driver for better performance.
    fn batch(
        &self,
        cx: &Cx,
        statements: &[(String, Vec<Value>)],
    ) -> impl Future<Output = Outcome<Vec<u64>, crate::Error>> + Send;

    /// Begin a transaction with default isolation level (ReadCommitted).
    fn begin(&self, cx: &Cx) -> impl Future<Output = Outcome<Self::Tx<'_>, crate::Error>> + Send;

    /// Begin a transaction with a specific isolation level.
    fn begin_with(
        &self,
        cx: &Cx,
        isolation: IsolationLevel,
    ) -> impl Future<Output = Outcome<Self::Tx<'_>, crate::Error>> + Send;

    /// Prepare a statement for repeated execution.
    ///
    /// Prepared statements are cached by the driver and can be executed
    /// multiple times with different parameters efficiently.
    fn prepare(
        &self,
        cx: &Cx,
        sql: &str,
    ) -> impl Future<Output = Outcome<PreparedStatement, crate::Error>> + Send;

    /// Execute a prepared statement and return all rows.
    fn query_prepared(
        &self,
        cx: &Cx,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, crate::Error>> + Send;

    /// Execute a prepared statement (INSERT, UPDATE, DELETE) and return rows affected.
    fn execute_prepared(
        &self,
        cx: &Cx,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> impl Future<Output = Outcome<u64, crate::Error>> + Send;

    /// Check if the connection is still valid by sending a ping.
    fn ping(&self, cx: &Cx) -> impl Future<Output = Outcome<(), crate::Error>> + Send;

    /// Check if the connection is still valid (alias for ping that returns bool).
    fn is_valid(&self, cx: &Cx) -> impl Future<Output = bool> + Send {
        async {
            match self.ping(cx).await {
                Outcome::Ok(()) => true,
                Outcome::Err(_) | Outcome::Cancelled(_) | Outcome::Panicked(_) => false,
            }
        }
    }

    /// Close the connection gracefully.
    fn close(self, cx: &Cx) -> impl Future<Output = Result<()>> + Send;
}

/// Trait for transaction operations.
///
/// This trait defines the interface for database transactions with
/// support for savepoints. Transactions must be explicitly committed
/// or rolled back; dropping without commit triggers automatic rollback.
///
/// # Savepoints
///
/// Savepoints allow partial rollback within a transaction:
///
/// ```rust,ignore
/// let mut tx = conn.begin(&cx).await?;
/// tx.execute(&cx, "INSERT INTO t1 (a) VALUES (1)", &[]).await?;
/// tx.savepoint(&cx, "sp1").await?;
/// tx.execute(&cx, "INSERT INTO t1 (a) VALUES (2)", &[]).await?;
/// tx.rollback_to(&cx, "sp1").await?;  // Rollback only the second insert
/// tx.commit(&cx).await?;  // Only first insert is committed
/// ```
pub trait TransactionOps: Send {
    /// Execute a query within this transaction.
    fn query(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, crate::Error>> + Send;

    /// Execute a query and return the first row, if any.
    fn query_one(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Option<Row>, crate::Error>> + Send;

    /// Execute a statement within this transaction.
    fn execute(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<u64, crate::Error>> + Send;

    /// Create a savepoint within this transaction.
    ///
    /// Savepoints allow partial rollback without aborting the entire transaction.
    fn savepoint(
        &self,
        cx: &Cx,
        name: &str,
    ) -> impl Future<Output = Outcome<(), crate::Error>> + Send;

    /// Rollback to a previously created savepoint.
    ///
    /// All changes made after the savepoint are discarded, but the transaction
    /// remains active and changes before the savepoint are preserved.
    fn rollback_to(
        &self,
        cx: &Cx,
        name: &str,
    ) -> impl Future<Output = Outcome<(), crate::Error>> + Send;

    /// Release a savepoint, making the changes permanent within the transaction.
    ///
    /// This frees resources associated with the savepoint but does not commit
    /// changes to the database (that happens when the transaction commits).
    fn release(
        &self,
        cx: &Cx,
        name: &str,
    ) -> impl Future<Output = Outcome<(), crate::Error>> + Send;

    /// Commit the transaction, making all changes permanent.
    fn commit(self, cx: &Cx) -> impl Future<Output = Outcome<(), crate::Error>> + Send;

    /// Rollback the transaction, discarding all changes.
    fn rollback(self, cx: &Cx) -> impl Future<Output = Outcome<(), crate::Error>> + Send;
}

/// A database transaction (concrete implementation).
///
/// Transactions provide ACID guarantees and can be committed or rolled back.
/// If dropped without committing, the transaction is automatically rolled back.
///
/// This is a concrete type used by the default transaction implementation.
/// Driver-specific implementations may use their own types that implement
/// [`TransactionOps`].
pub struct Transaction<'conn> {
    /// The underlying connection
    conn: &'conn dyn TransactionInternal,
    /// Whether this transaction has been finalized (committed or rolled back)
    finalized: bool,
}

/// Internal trait for transaction operations (object-safe subset).
///
/// This trait provides a boxed-future version of TransactionOps for
/// use with trait objects.
pub trait TransactionInternal: Send + Sync {
    /// Execute a query.
    fn query_internal(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> std::pin::Pin<Box<dyn Future<Output = Outcome<Vec<Row>, crate::Error>> + Send + '_>>;

    /// Execute a query and return first row.
    fn query_one_internal(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> std::pin::Pin<Box<dyn Future<Output = Outcome<Option<Row>, crate::Error>> + Send + '_>>;

    /// Execute a statement.
    fn execute_internal(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> std::pin::Pin<Box<dyn Future<Output = Outcome<u64, crate::Error>> + Send + '_>>;

    /// Create a savepoint.
    fn savepoint_internal(
        &self,
        cx: &Cx,
        name: &str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Outcome<(), crate::Error>> + Send + '_>>;

    /// Rollback to a savepoint.
    fn rollback_to_internal(
        &self,
        cx: &Cx,
        name: &str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Outcome<(), crate::Error>> + Send + '_>>;

    /// Release a savepoint.
    fn release_internal(
        &self,
        cx: &Cx,
        name: &str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Outcome<(), crate::Error>> + Send + '_>>;

    /// Commit the transaction.
    fn commit_internal(
        &self,
        cx: &Cx,
    ) -> std::pin::Pin<Box<dyn Future<Output = Outcome<(), crate::Error>> + Send + '_>>;

    /// Rollback the transaction.
    fn rollback_internal(
        &self,
        cx: &Cx,
    ) -> std::pin::Pin<Box<dyn Future<Output = Outcome<(), crate::Error>> + Send + '_>>;
}

impl<'conn> Transaction<'conn> {
    /// Create a new transaction wrapper.
    ///
    /// This is typically called by the driver, not by users directly.
    pub fn new(conn: &'conn dyn TransactionInternal) -> Self {
        Self {
            conn,
            finalized: false,
        }
    }

    /// Check if this transaction has been finalized.
    #[must_use]
    pub const fn is_finalized(&self) -> bool {
        self.finalized
    }
}

impl TransactionOps for Transaction<'_> {
    fn query(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, crate::Error>> + Send {
        self.conn.query_internal(cx, sql, params)
    }

    fn query_one(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Option<Row>, crate::Error>> + Send {
        self.conn.query_one_internal(cx, sql, params)
    }

    fn execute(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<u64, crate::Error>> + Send {
        self.conn.execute_internal(cx, sql, params)
    }

    fn savepoint(
        &self,
        cx: &Cx,
        name: &str,
    ) -> impl Future<Output = Outcome<(), crate::Error>> + Send {
        self.conn.savepoint_internal(cx, name)
    }

    fn rollback_to(
        &self,
        cx: &Cx,
        name: &str,
    ) -> impl Future<Output = Outcome<(), crate::Error>> + Send {
        self.conn.rollback_to_internal(cx, name)
    }

    fn release(
        &self,
        cx: &Cx,
        name: &str,
    ) -> impl Future<Output = Outcome<(), crate::Error>> + Send {
        self.conn.release_internal(cx, name)
    }

    async fn commit(mut self, cx: &Cx) -> Outcome<(), crate::Error> {
        self.finalized = true;
        self.conn.commit_internal(cx).await
    }

    async fn rollback(mut self, cx: &Cx) -> Outcome<(), crate::Error> {
        self.finalized = true;
        self.conn.rollback_internal(cx).await
    }
}

use std::future::Future;

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.finalized {
            // Transaction was not committed/rolled back explicitly.
            // The actual rollback happens at the protocol level when the
            // connection detects an unfinalized transaction scope.
            // We can't do async in drop, so we just mark it here.
        }
    }
}

/// Configuration for database connections.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Connection string or URL
    pub url: String,
    /// Connection timeout in milliseconds
    pub connect_timeout_ms: u64,
    /// Query timeout in milliseconds
    pub query_timeout_ms: u64,
    /// SSL mode
    pub ssl_mode: SslMode,
    /// Application name for connection identification
    pub application_name: Option<String>,
}

/// SSL connection mode.
#[derive(Debug, Clone, Copy, Default)]
pub enum SslMode {
    /// Never use SSL
    Disable,
    /// Prefer SSL but allow non-SSL
    #[default]
    Prefer,
    /// Require SSL
    Require,
    /// Verify server certificate
    VerifyCa,
    /// Verify server certificate and hostname
    VerifyFull,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            connect_timeout_ms: 30_000,
            query_timeout_ms: 30_000,
            ssl_mode: SslMode::default(),
            application_name: None,
        }
    }
}

impl ConnectionConfig {
    /// Create a new connection config with the given URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    /// Set the connection timeout.
    pub fn connect_timeout(mut self, ms: u64) -> Self {
        self.connect_timeout_ms = ms;
        self
    }

    /// Set the query timeout.
    pub fn query_timeout(mut self, ms: u64) -> Self {
        self.query_timeout_ms = ms;
        self
    }

    /// Set the SSL mode.
    pub fn ssl_mode(mut self, mode: SslMode) -> Self {
        self.ssl_mode = mode;
        self
    }

    /// Set the application name.
    pub fn application_name(mut self, name: impl Into<String>) -> Self {
        self.application_name = Some(name.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolation_level_default() {
        let level = IsolationLevel::default();
        assert_eq!(level, IsolationLevel::ReadCommitted);
    }

    #[test]
    fn test_isolation_level_as_sql() {
        assert_eq!(IsolationLevel::ReadUncommitted.as_sql(), "READ UNCOMMITTED");
        assert_eq!(IsolationLevel::ReadCommitted.as_sql(), "READ COMMITTED");
        assert_eq!(IsolationLevel::RepeatableRead.as_sql(), "REPEATABLE READ");
        assert_eq!(IsolationLevel::Serializable.as_sql(), "SERIALIZABLE");
    }

    #[test]
    fn test_prepared_statement_new() {
        let stmt = PreparedStatement::new(1, "SELECT * FROM users WHERE id = $1".to_string(), 1);
        assert_eq!(stmt.id(), 1);
        assert_eq!(stmt.sql(), "SELECT * FROM users WHERE id = $1");
        assert_eq!(stmt.param_count(), 1);
        assert!(stmt.columns().is_none());
    }

    #[test]
    fn test_prepared_statement_with_columns() {
        let stmt = PreparedStatement::with_columns(
            2,
            "SELECT id, name FROM users".to_string(),
            0,
            vec!["id".to_string(), "name".to_string()],
        );
        assert_eq!(stmt.id(), 2);
        assert_eq!(stmt.param_count(), 0);
        assert_eq!(
            stmt.columns(),
            Some(&["id".to_string(), "name".to_string()][..])
        );
    }

    #[test]
    fn test_prepared_statement_validate_params() {
        let stmt = PreparedStatement::new(1, "SELECT $1, $2".to_string(), 2);

        assert!(!stmt.validate_params(&[]));
        assert!(!stmt.validate_params(&[Value::Int(1)]));
        assert!(stmt.validate_params(&[Value::Int(1), Value::Int(2)]));
        assert!(!stmt.validate_params(&[Value::Int(1), Value::Int(2), Value::Int(3)]));
    }

    #[test]
    fn test_ssl_mode_default() {
        let mode = SslMode::default();
        assert!(matches!(mode, SslMode::Prefer));
    }

    #[test]
    fn test_connection_config_builder() {
        let config = ConnectionConfig::new("postgres://localhost/test")
            .connect_timeout(5000)
            .query_timeout(10000)
            .ssl_mode(SslMode::Require)
            .application_name("test_app");

        assert_eq!(config.url, "postgres://localhost/test");
        assert_eq!(config.connect_timeout_ms, 5000);
        assert_eq!(config.query_timeout_ms, 10000);
        assert!(matches!(config.ssl_mode, SslMode::Require));
        assert_eq!(config.application_name, Some("test_app".to_string()));
    }

    #[test]
    fn test_connection_config_default() {
        let config = ConnectionConfig::default();
        assert_eq!(config.url, "");
        assert_eq!(config.connect_timeout_ms, 30_000);
        assert_eq!(config.query_timeout_ms, 30_000);
        assert!(matches!(config.ssl_mode, SslMode::Prefer));
        assert!(config.application_name.is_none());
    }
}
