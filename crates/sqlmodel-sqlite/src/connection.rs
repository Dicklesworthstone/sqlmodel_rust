//! SQLite connection implementation.
//!
//! This module provides safe wrappers around SQLite's C API and implements
//! the Connection trait from sqlmodel-core.
//!
//! # Console Integration
//!
//! When the `console` feature is enabled, the connection can report status
//! during operations. Use the `ConsoleAware` trait to attach a console.
//!
//! ```rust,ignore
//! use sqlmodel_sqlite::SqliteConnection;
//! use sqlmodel_console::{SqlModelConsole, ConsoleAware};
//! use std::sync::Arc;
//!
//! let console = Arc::new(SqlModelConsole::new());
//! let mut conn = SqliteConnection::open_memory().unwrap();
//! conn.set_console(Some(console));
//! ```

// Allow casts in FFI code where we need to match C types exactly
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::result_large_err)] // Error type is defined in sqlmodel-core
#![allow(clippy::borrow_as_ptr)] // FFI requires raw pointers
#![allow(clippy::if_not_else)] // Clearer for error handling
#![allow(clippy::implicit_clone)] // Minor optimization
#![allow(clippy::map_unwrap_or)] // Clearer for optional formatting
#![allow(clippy::redundant_closure)] // format_value requires context

use crate::ffi;
use crate::types;
use sqlmodel_core::{
    Connection, Cx, Error, IsolationLevel, Outcome, PreparedStatement, Row, TransactionOps, Value,
    error::{ConnectionError, ConnectionErrorKind, QueryError, QueryErrorKind},
    row::ColumnInfo,
};
use std::ffi::{CStr, CString, c_int};
use std::future::Future;
use std::ptr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(feature = "console")]
use sqlmodel_console::{ConsoleAware, SqlModelConsole};

/// Configuration for opening SQLite connections.
#[derive(Debug, Clone)]
pub struct SqliteConfig {
    /// Path to the database file, or ":memory:" for in-memory database.
    pub path: String,
    /// Open flags (read-only, read-write, create, etc.)
    pub flags: OpenFlags,
    /// Busy timeout in milliseconds.
    pub busy_timeout_ms: u32,
}

/// Flags controlling how the database is opened.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenFlags {
    /// Open for reading only.
    pub read_only: bool,
    /// Open for reading and writing.
    pub read_write: bool,
    /// Create the database if it doesn't exist.
    pub create: bool,
    /// Enable URI filename interpretation.
    pub uri: bool,
    /// Open in multi-thread mode (connections not shared between threads).
    pub no_mutex: bool,
    /// Open in serialized mode (connections can be shared).
    pub full_mutex: bool,
    /// Enable shared cache mode.
    pub shared_cache: bool,
    /// Disable shared cache mode.
    pub private_cache: bool,
}

impl OpenFlags {
    /// Create flags for read-only access.
    pub fn read_only() -> Self {
        Self {
            read_only: true,
            ..Default::default()
        }
    }

    /// Create flags for read-write access (database must exist).
    pub fn read_write() -> Self {
        Self {
            read_write: true,
            ..Default::default()
        }
    }

    /// Create flags for read-write access with creation if needed.
    pub fn create_read_write() -> Self {
        Self {
            read_write: true,
            create: true,
            ..Default::default()
        }
    }

    fn to_sqlite_flags(self) -> c_int {
        let mut flags = 0;

        if self.read_only {
            flags |= ffi::SQLITE_OPEN_READONLY;
        }
        if self.read_write {
            flags |= ffi::SQLITE_OPEN_READWRITE;
        }
        if self.create {
            flags |= ffi::SQLITE_OPEN_CREATE;
        }
        if self.uri {
            flags |= ffi::SQLITE_OPEN_URI;
        }
        if self.no_mutex {
            flags |= ffi::SQLITE_OPEN_NOMUTEX;
        }
        if self.full_mutex {
            flags |= ffi::SQLITE_OPEN_FULLMUTEX;
        }
        if self.shared_cache {
            flags |= ffi::SQLITE_OPEN_SHAREDCACHE;
        }
        if self.private_cache {
            flags |= ffi::SQLITE_OPEN_PRIVATECACHE;
        }

        // Default to read-write if no mode specified
        if flags & (ffi::SQLITE_OPEN_READONLY | ffi::SQLITE_OPEN_READWRITE) == 0 {
            flags |= ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE;
        }

        flags
    }
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: ":memory:".to_string(),
            flags: OpenFlags::create_read_write(),
            busy_timeout_ms: 5000,
        }
    }
}

impl SqliteConfig {
    /// Create a new config for a file-based database.
    pub fn file(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            flags: OpenFlags::create_read_write(),
            busy_timeout_ms: 5000,
        }
    }

    /// Create a new config for an in-memory database.
    pub fn memory() -> Self {
        Self::default()
    }

    /// Set open flags.
    pub fn flags(mut self, flags: OpenFlags) -> Self {
        self.flags = flags;
        self
    }

    /// Set busy timeout.
    pub fn busy_timeout(mut self, ms: u32) -> Self {
        self.busy_timeout_ms = ms;
        self
    }
}

/// Inner state of the SQLite connection, protected by a mutex for thread safety.
struct SqliteInner {
    db: *mut ffi::sqlite3,
    in_transaction: bool,
}

// SAFETY: SQLite handles can be safely sent between threads when using
// SQLITE_OPEN_FULLMUTEX (serialized mode) or when properly synchronized.
// We use a Mutex to ensure synchronization.
unsafe impl Send for SqliteInner {}

/// A connection to a SQLite database.
///
/// This is a thread-safe wrapper around a SQLite database handle.
pub struct SqliteConnection {
    inner: Mutex<SqliteInner>,
    path: String,
    /// Optional console for rich output
    #[cfg(feature = "console")]
    console: Option<Arc<SqlModelConsole>>,
}

// SqliteConnection is Send + Sync because all access goes through the Mutex
unsafe impl Send for SqliteConnection {}
unsafe impl Sync for SqliteConnection {}

impl SqliteConnection {
    /// Open a new SQLite connection with the given configuration.
    pub fn open(config: &SqliteConfig) -> Result<Self, Error> {
        let c_path = CString::new(config.path.as_str()).map_err(|_| {
            Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Connect,
                message: "Invalid path: contains null byte".to_string(),
                source: None,
            })
        })?;

        let mut db: *mut ffi::sqlite3 = ptr::null_mut();
        let flags = config.flags.to_sqlite_flags();

        // SAFETY: We pass valid pointers and check the return value
        let rc = unsafe { ffi::sqlite3_open_v2(c_path.as_ptr(), &mut db, flags, ptr::null()) };

        if rc != ffi::SQLITE_OK {
            let msg = if !db.is_null() {
                // SAFETY: db is valid, errmsg returns a valid C string
                unsafe {
                    let err_ptr = ffi::sqlite3_errmsg(db);
                    let msg = CStr::from_ptr(err_ptr).to_string_lossy().into_owned();
                    ffi::sqlite3_close(db);
                    msg
                }
            } else {
                ffi::error_string(rc).to_string()
            };

            return Err(Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Connect,
                message: format!("Failed to open database: {}", msg),
                source: None,
            }));
        }

        // Set busy timeout
        if config.busy_timeout_ms > 0 {
            // SAFETY: db is valid
            unsafe {
                ffi::sqlite3_busy_timeout(db, config.busy_timeout_ms as c_int);
            }
        }

        Ok(Self {
            inner: Mutex::new(SqliteInner {
                db,
                in_transaction: false,
            }),
            path: config.path.clone(),
            #[cfg(feature = "console")]
            console: None,
        })
    }

    /// Open an in-memory database.
    pub fn open_memory() -> Result<Self, Error> {
        Self::open(&SqliteConfig::memory())
    }

    /// Open a file-based database.
    pub fn open_file(path: impl Into<String>) -> Result<Self, Error> {
        Self::open(&SqliteConfig::file(path))
    }

    /// Get the database path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Execute SQL directly without preparing (for DDL, etc.)
    pub fn execute_raw(&self, sql: &str) -> Result<(), Error> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let c_sql = CString::new(sql).map_err(|_| {
            Error::Query(QueryError {
                kind: QueryErrorKind::Syntax,
                sql: Some(sql.to_string()),
                sqlstate: None,
                message: "SQL contains null byte".to_string(),
                detail: None,
                hint: None,
                position: None,
                source: None,
            })
        })?;

        let mut errmsg: *mut std::ffi::c_char = ptr::null_mut();

        // SAFETY: All pointers are valid
        let rc = unsafe {
            ffi::sqlite3_exec(inner.db, c_sql.as_ptr(), None, ptr::null_mut(), &mut errmsg)
        };

        if rc != ffi::SQLITE_OK {
            let msg = if !errmsg.is_null() {
                // SAFETY: errmsg is valid
                let msg = unsafe { CStr::from_ptr(errmsg).to_string_lossy().into_owned() };
                unsafe { ffi::sqlite3_free(errmsg.cast()) };
                msg
            } else {
                ffi::error_string(rc).to_string()
            };

            return Err(Error::Query(QueryError {
                kind: error_code_to_kind(rc),
                sql: Some(sql.to_string()),
                sqlstate: None,
                message: msg,
                detail: None,
                hint: None,
                position: None,
                source: None,
            }));
        }

        Ok(())
    }

    /// Backup the current database to a destination path using the SQLite backup API.
    ///
    /// This opens (or creates) the destination database and performs an online backup
    /// from this connection's `main` database into the destination's `main` database.
    pub fn backup_to_path(&self, dest_path: impl AsRef<str>) -> Result<(), Error> {
        let dest = SqliteConnection::open(
            &SqliteConfig::file(dest_path.as_ref()).flags(OpenFlags::create_read_write()),
        )?;
        self.backup_to_connection(&dest)
    }

    /// Backup the current database to another open SQLite connection.
    pub fn backup_to_connection(&self, dest: &SqliteConnection) -> Result<(), Error> {
        let self_first = (self as *const _ as usize) <= (dest as *const _ as usize);
        let (source_guard, dest_guard) = if self_first {
            let source_guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let dest_guard = dest.inner.lock().unwrap_or_else(|e| e.into_inner());
            (source_guard, dest_guard)
        } else {
            let dest_guard = dest.inner.lock().unwrap_or_else(|e| e.into_inner());
            let source_guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            (source_guard, dest_guard)
        };

        let source_db = source_guard.db;
        let dest_db = dest_guard.db;

        let main = CString::new("main").expect("static sqlite db name");

        // SAFETY: We hold locks on both connections; db pointers are valid.
        let backup =
            unsafe { ffi::sqlite3_backup_init(dest_db, main.as_ptr(), source_db, main.as_ptr()) };
        if backup.is_null() {
            let msg = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(dest_db)) }
                .to_string_lossy()
                .into_owned();
            return Err(Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Connect,
                message: format!("SQLite backup init failed: {msg}"),
                source: None,
            }));
        }

        let mut rc = unsafe { ffi::sqlite3_backup_step(backup, 100) };
        loop {
            if rc == ffi::SQLITE_DONE {
                break;
            }
            if rc == ffi::SQLITE_OK {
                rc = unsafe { ffi::sqlite3_backup_step(backup, 100) };
                continue;
            }
            if rc == ffi::SQLITE_BUSY || rc == ffi::SQLITE_LOCKED {
                std::thread::sleep(Duration::from_millis(50));
                rc = unsafe { ffi::sqlite3_backup_step(backup, 100) };
                continue;
            }
            break;
        }

        let finish_rc = unsafe { ffi::sqlite3_backup_finish(backup) };

        if rc != ffi::SQLITE_DONE && rc != ffi::SQLITE_OK {
            let msg = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(dest_db)) }
                .to_string_lossy()
                .into_owned();
            return Err(Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Connect,
                message: format!("SQLite backup failed: {} ({})", msg, ffi::error_string(rc)),
                source: None,
            }));
        }

        if finish_rc != ffi::SQLITE_OK {
            let msg = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(dest_db)) }
                .to_string_lossy()
                .into_owned();
            return Err(Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Connect,
                message: format!(
                    "SQLite backup finish failed: {} ({})",
                    msg,
                    ffi::error_string(finish_rc)
                ),
                source: None,
            }));
        }

        Ok(())
    }

    /// Get the last insert rowid.
    pub fn last_insert_rowid(&self) -> i64 {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: db is valid
        unsafe { ffi::sqlite3_last_insert_rowid(inner.db) }
    }

    /// Get the number of rows changed by the last statement.
    pub fn changes(&self) -> i32 {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: db is valid
        unsafe { ffi::sqlite3_changes(inner.db) }
    }

    /// Prepare and execute a query synchronously, returning all rows.
    ///
    /// This is a blocking operation suitable for simple use cases.
    /// For async usage, use the `Connection` trait methods instead.
    pub fn query_sync(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, Error> {
        #[cfg(feature = "console")]
        let start = std::time::Instant::now();

        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let stmt = prepare_stmt(inner.db, sql)?;

        // Bind parameters
        for (i, param) in params.iter().enumerate() {
            // SAFETY: stmt is valid, index is 1-based
            let rc = unsafe { types::bind_value(stmt, (i + 1) as c_int, param) };
            if rc != ffi::SQLITE_OK {
                // SAFETY: stmt is valid
                unsafe { ffi::sqlite3_finalize(stmt) };
                return Err(bind_error(inner.db, sql, i + 1));
            }
        }

        // Fetch column names
        // SAFETY: stmt is valid
        let col_count = unsafe { ffi::sqlite3_column_count(stmt) };
        let mut col_names = Vec::with_capacity(col_count as usize);
        for i in 0..col_count {
            let name =
                unsafe { types::column_name(stmt, i) }.unwrap_or_else(|| format!("col{}", i));
            col_names.push(name);
        }
        let columns = Arc::new(ColumnInfo::new(col_names.clone()));

        // Fetch rows
        let mut rows = Vec::new();
        loop {
            // SAFETY: stmt is valid
            let rc = unsafe { ffi::sqlite3_step(stmt) };
            match rc {
                ffi::SQLITE_ROW => {
                    let mut values = Vec::with_capacity(col_count as usize);
                    for i in 0..col_count {
                        // SAFETY: stmt is valid, we just got SQLITE_ROW
                        let value = unsafe { types::read_column(stmt, i) };
                        values.push(value);
                    }
                    rows.push(Row::with_columns(Arc::clone(&columns), values));
                }
                ffi::SQLITE_DONE => break,
                _ => {
                    // SAFETY: stmt is valid
                    unsafe { ffi::sqlite3_finalize(stmt) };
                    return Err(step_error(inner.db, sql));
                }
            }
        }

        // SAFETY: stmt is valid
        unsafe { ffi::sqlite3_finalize(stmt) };

        // Emit console output for PRAGMA queries and timing
        #[cfg(feature = "console")]
        {
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            self.emit_query_result(sql, &col_names, &rows, elapsed_ms);
        }

        Ok(rows)
    }

    /// Prepare and execute a statement synchronously, returning rows affected.
    ///
    /// This is a blocking operation suitable for simple use cases.
    /// For async usage, use the `Connection` trait methods instead.
    pub fn execute_sync(&self, sql: &str, params: &[Value]) -> Result<u64, Error> {
        #[cfg(feature = "console")]
        let start = std::time::Instant::now();

        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let stmt = prepare_stmt(inner.db, sql)?;

        // Bind parameters
        for (i, param) in params.iter().enumerate() {
            // SAFETY: stmt is valid
            let rc = unsafe { types::bind_value(stmt, (i + 1) as c_int, param) };
            if rc != ffi::SQLITE_OK {
                // SAFETY: stmt is valid
                unsafe { ffi::sqlite3_finalize(stmt) };
                return Err(bind_error(inner.db, sql, i + 1));
            }
        }

        // Execute
        // SAFETY: stmt is valid
        let rc = unsafe { ffi::sqlite3_step(stmt) };

        // SAFETY: stmt is valid
        unsafe { ffi::sqlite3_finalize(stmt) };

        match rc {
            ffi::SQLITE_DONE | ffi::SQLITE_ROW => {
                // SAFETY: db is valid
                let changes = unsafe { ffi::sqlite3_changes(inner.db) };

                #[cfg(feature = "console")]
                {
                    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
                    self.emit_execute_timing(sql, changes as u64, elapsed_ms);
                }

                Ok(changes as u64)
            }
            _ => Err(step_error(inner.db, sql)),
        }
    }

    /// Execute an INSERT and return the last inserted rowid.
    fn insert_sync(&self, sql: &str, params: &[Value]) -> Result<i64, Error> {
        self.execute_sync(sql, params)?;
        Ok(self.last_insert_rowid())
    }

    /// Begin a transaction.
    fn begin_sync(&self, isolation: IsolationLevel) -> Result<(), Error> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.in_transaction {
            return Err(Error::Query(QueryError {
                kind: QueryErrorKind::Database,
                sql: None,
                sqlstate: None,
                message: "Already in a transaction".to_string(),
                detail: None,
                hint: None,
                position: None,
                source: None,
            }));
        }

        // SQLite doesn't support isolation levels in the same way as PostgreSQL,
        // but we can approximate with different transaction types
        let begin_sql = match isolation {
            IsolationLevel::Serializable => "BEGIN EXCLUSIVE",
            IsolationLevel::RepeatableRead | IsolationLevel::ReadCommitted => "BEGIN IMMEDIATE",
            IsolationLevel::ReadUncommitted => "BEGIN DEFERRED",
        };

        drop(inner); // Release lock before calling execute_raw
        self.execute_raw(begin_sql)?;

        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.in_transaction = true;
        self.emit_transaction_state("BEGIN");
        Ok(())
    }

    /// Commit the current transaction.
    fn commit_sync(&self) -> Result<(), Error> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if !inner.in_transaction {
            return Err(Error::Query(QueryError {
                kind: QueryErrorKind::Database,
                sql: None,
                sqlstate: None,
                message: "Not in a transaction".to_string(),
                detail: None,
                hint: None,
                position: None,
                source: None,
            }));
        }

        drop(inner);
        self.execute_raw("COMMIT")?;

        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.in_transaction = false;
        self.emit_transaction_state("COMMIT");
        Ok(())
    }

    /// Rollback the current transaction.
    fn rollback_sync(&self) -> Result<(), Error> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if !inner.in_transaction {
            return Err(Error::Query(QueryError {
                kind: QueryErrorKind::Database,
                sql: None,
                sqlstate: None,
                message: "Not in a transaction".to_string(),
                detail: None,
                hint: None,
                position: None,
                source: None,
            }));
        }

        drop(inner);
        self.execute_raw("ROLLBACK")?;

        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.in_transaction = false;
        self.emit_transaction_state("ROLLBACK");
        Ok(())
    }
}

impl Drop for SqliteConnection {
    fn drop(&mut self) {
        if let Ok(inner) = self.inner.lock() {
            if !inner.db.is_null() {
                // SAFETY: db is valid
                unsafe {
                    ffi::sqlite3_close_v2(inner.db);
                }
            }
        }
    }
}

/// A SQLite transaction.
pub struct SqliteTransaction<'conn> {
    conn: &'conn SqliteConnection,
    committed: bool,
}

impl<'conn> SqliteTransaction<'conn> {
    fn new(conn: &'conn SqliteConnection) -> Self {
        Self {
            conn,
            committed: false,
        }
    }
}

impl Drop for SqliteTransaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // Auto-rollback on drop if not committed
            let _ = self.conn.rollback_sync();
        }
    }
}

// Implement Connection trait for SqliteConnection
impl Connection for SqliteConnection {
    type Tx<'conn>
        = SqliteTransaction<'conn>
    where
        Self: 'conn;

    fn dialect(&self) -> sqlmodel_core::Dialect {
        sqlmodel_core::Dialect::Sqlite
    }

    fn query(
        &self,
        _cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
        let result = self.query_sync(sql, params);
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn query_one(
        &self,
        _cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Option<Row>, Error>> + Send {
        let result = self.query_sync(sql, params).map(|mut rows| rows.pop());
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn execute(
        &self,
        _cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<u64, Error>> + Send {
        let result = self.execute_sync(sql, params);
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn insert(
        &self,
        _cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<i64, Error>> + Send {
        let result = self.insert_sync(sql, params);
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn batch(
        &self,
        _cx: &Cx,
        statements: &[(String, Vec<Value>)],
    ) -> impl Future<Output = Outcome<Vec<u64>, Error>> + Send {
        let mut results = Vec::with_capacity(statements.len());
        let mut error = None;

        for (sql, params) in statements {
            match self.execute_sync(sql, params) {
                Ok(n) => results.push(n),
                Err(e) => {
                    error = Some(e);
                    break;
                }
            }
        }

        async move {
            match error {
                Some(e) => Outcome::Err(e),
                None => Outcome::Ok(results),
            }
        }
    }

    fn begin(&self, cx: &Cx) -> impl Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
        self.begin_with(cx, IsolationLevel::default())
    }

    fn begin_with(
        &self,
        _cx: &Cx,
        isolation: IsolationLevel,
    ) -> impl Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
        let result = self
            .begin_sync(isolation)
            .map(|()| SqliteTransaction::new(self));
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn prepare(
        &self,
        _cx: &Cx,
        sql: &str,
    ) -> impl Future<Output = Outcome<PreparedStatement, Error>> + Send {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let result = prepare_stmt(inner.db, sql).map(|stmt| {
            // SAFETY: stmt is valid
            let param_count = unsafe { ffi::sqlite3_bind_parameter_count(stmt) } as usize;
            let col_count = unsafe { ffi::sqlite3_column_count(stmt) } as c_int;

            let mut columns = Vec::with_capacity(col_count as usize);
            for i in 0..col_count {
                if let Some(name) = unsafe { types::column_name(stmt, i) } {
                    columns.push(name);
                }
            }

            // SAFETY: stmt is valid
            unsafe { ffi::sqlite3_finalize(stmt) };

            // Use address as pseudo-ID since we don't cache statements yet
            let id = sql.as_ptr() as u64;
            PreparedStatement::with_columns(id, sql.to_string(), param_count, columns)
        });

        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn query_prepared(
        &self,
        cx: &Cx,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
        // For now, just re-execute the SQL
        // Future optimization: cache prepared statements
        self.query(cx, stmt.sql(), params)
    }

    fn execute_prepared(
        &self,
        cx: &Cx,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> impl Future<Output = Outcome<u64, Error>> + Send {
        self.execute(cx, stmt.sql(), params)
    }

    fn ping(&self, _cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
        // Simple ping: execute a trivial query
        let result = self.query_sync("SELECT 1", &[]).map(|_| ());
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    async fn close(self, _cx: &Cx) -> sqlmodel_core::Result<()> {
        // Connection is closed on drop
        Ok(())
    }
}

// Implement TransactionOps for SqliteTransaction
impl TransactionOps for SqliteTransaction<'_> {
    fn query(
        &self,
        _cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
        let result = self.conn.query_sync(sql, params);
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn query_one(
        &self,
        _cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Option<Row>, Error>> + Send {
        let result = self.conn.query_sync(sql, params).map(|mut rows| rows.pop());
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn execute(
        &self,
        _cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<u64, Error>> + Send {
        let result = self.conn.execute_sync(sql, params);
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn savepoint(&self, _cx: &Cx, name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        // Quote identifier to prevent SQL injection
        let quoted_name = format!("\"{}\"", name.replace('"', "\"\""));
        let sql = format!("SAVEPOINT {}", quoted_name);
        let result = self.conn.execute_raw(&sql);
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn rollback_to(&self, _cx: &Cx, name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        // Quote identifier to prevent SQL injection
        let quoted_name = format!("\"{}\"", name.replace('"', "\"\""));
        let sql = format!("ROLLBACK TO {}", quoted_name);
        let result = self.conn.execute_raw(&sql);
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn release(&self, _cx: &Cx, name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        // Quote identifier to prevent SQL injection
        let quoted_name = format!("\"{}\"", name.replace('"', "\"\""));
        let sql = format!("RELEASE {}", quoted_name);
        let result = self.conn.execute_raw(&sql);
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    async fn commit(mut self, _cx: &Cx) -> Outcome<(), Error> {
        self.committed = true;
        self.conn
            .commit_sync()
            .map_or_else(Outcome::Err, Outcome::Ok)
    }

    async fn rollback(mut self, _cx: &Cx) -> Outcome<(), Error> {
        self.committed = true; // Prevent double rollback in drop
        self.conn
            .rollback_sync()
            .map_or_else(Outcome::Err, Outcome::Ok)
    }
}

// Helper functions

fn prepare_stmt(db: *mut ffi::sqlite3, sql: &str) -> Result<*mut ffi::sqlite3_stmt, Error> {
    let c_sql = CString::new(sql).map_err(|_| {
        Error::Query(QueryError {
            kind: QueryErrorKind::Syntax,
            sql: Some(sql.to_string()),
            sqlstate: None,
            message: "SQL contains null byte".to_string(),
            detail: None,
            hint: None,
            position: None,
            source: None,
        })
    })?;

    let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();

    // SAFETY: All pointers are valid
    let rc = unsafe {
        ffi::sqlite3_prepare_v2(
            db,
            c_sql.as_ptr(),
            c_sql.as_bytes().len() as c_int,
            &mut stmt,
            ptr::null_mut(),
        )
    };

    if rc != ffi::SQLITE_OK {
        return Err(prepare_error(db, sql));
    }

    Ok(stmt)
}

fn prepare_error(db: *mut ffi::sqlite3, sql: &str) -> Error {
    // SAFETY: db is valid
    let msg = unsafe {
        let ptr = ffi::sqlite3_errmsg(db);
        CStr::from_ptr(ptr).to_string_lossy().into_owned()
    };
    let code = unsafe { ffi::sqlite3_errcode(db) };

    Error::Query(QueryError {
        kind: error_code_to_kind(code),
        sql: Some(sql.to_string()),
        sqlstate: None,
        message: msg,
        detail: None,
        hint: None,
        position: None,
        source: None,
    })
}

fn bind_error(db: *mut ffi::sqlite3, sql: &str, param_index: usize) -> Error {
    // SAFETY: db is valid
    let msg = unsafe {
        let ptr = ffi::sqlite3_errmsg(db);
        CStr::from_ptr(ptr).to_string_lossy().into_owned()
    };

    Error::Query(QueryError {
        kind: QueryErrorKind::Database,
        sql: Some(sql.to_string()),
        sqlstate: None,
        message: format!("Failed to bind parameter {}: {}", param_index, msg),
        detail: None,
        hint: None,
        position: None,
        source: None,
    })
}

fn step_error(db: *mut ffi::sqlite3, sql: &str) -> Error {
    // SAFETY: db is valid
    let msg = unsafe {
        let ptr = ffi::sqlite3_errmsg(db);
        CStr::from_ptr(ptr).to_string_lossy().into_owned()
    };
    let code = unsafe { ffi::sqlite3_errcode(db) };

    Error::Query(QueryError {
        kind: error_code_to_kind(code),
        sql: Some(sql.to_string()),
        sqlstate: None,
        message: msg,
        detail: None,
        hint: None,
        position: None,
        source: None,
    })
}

fn error_code_to_kind(code: c_int) -> QueryErrorKind {
    match code {
        ffi::SQLITE_CONSTRAINT => QueryErrorKind::Constraint,
        ffi::SQLITE_BUSY | ffi::SQLITE_LOCKED => QueryErrorKind::Deadlock,
        ffi::SQLITE_PERM | ffi::SQLITE_AUTH => QueryErrorKind::Permission,
        ffi::SQLITE_NOTFOUND => QueryErrorKind::NotFound,
        ffi::SQLITE_TOOBIG => QueryErrorKind::DataTruncation,
        ffi::SQLITE_INTERRUPT => QueryErrorKind::Cancelled,
        _ => QueryErrorKind::Database,
    }
}

/// Format a Value for display in console output.
#[allow(dead_code)]
fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::TinyInt(n) => n.to_string(),
        Value::SmallInt(n) => n.to_string(),
        Value::Int(n) => n.to_string(),
        Value::BigInt(n) => n.to_string(),
        Value::Float(n) => format!("{:.6}", n),
        Value::Double(n) => format!("{:.6}", n),
        Value::Text(s) => s.clone(),
        Value::Bytes(b) => format!("[BLOB: {} bytes]", b.len()),
        Value::Date(d) => d.to_string(),
        Value::Time(t) => t.to_string(),
        Value::Timestamp(ts) => ts.to_string(),
        Value::TimestampTz(ts) => ts.to_string(),
        Value::Json(j) => j.to_string(),
        Value::Uuid(u) => {
            // Format UUID as hex string: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
            format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                u[0],
                u[1],
                u[2],
                u[3],
                u[4],
                u[5],
                u[6],
                u[7],
                u[8],
                u[9],
                u[10],
                u[11],
                u[12],
                u[13],
                u[14],
                u[15]
            )
        }
        Value::Decimal(d) => d.to_string(),
        Value::Array(arr) => format!("[{} items]", arr.len()),
        Value::Default => "DEFAULT".to_string(),
    }
}

// ==================== Console Support ====================

#[cfg(feature = "console")]
impl ConsoleAware for SqliteConnection {
    fn set_console(&mut self, console: Option<Arc<SqlModelConsole>>) {
        self.console = console;
        // Emit database status when console is attached
        self.emit_open_status();
    }

    fn console(&self) -> Option<&Arc<SqlModelConsole>> {
        self.console.as_ref()
    }

    fn has_console(&self) -> bool {
        self.console.is_some()
    }
}

impl SqliteConnection {
    /// Emit database open status to console if available.
    #[cfg(feature = "console")]
    fn emit_open_status(&self) {
        if let Some(console) = &self.console {
            // Get database info
            let mode = if self.path == ":memory:" {
                "in-memory"
            } else {
                "file"
            };

            // Query journal mode if we can
            let journal_mode = self
                .query_sync("PRAGMA journal_mode", &[])
                .ok()
                .and_then(|rows| rows.first().and_then(|r| r.get_as::<String>(0).ok()));

            let page_size = self
                .query_sync("PRAGMA page_size", &[])
                .ok()
                .and_then(|rows| rows.first().and_then(|r| r.get_as::<i64>(0).ok()));

            if console.mode().is_plain() {
                // Plain text output for agents
                let journal = journal_mode.as_deref().unwrap_or("unknown");
                console.status(&format!(
                    "Opened SQLite database: {} ({} mode, journal: {})",
                    self.path, mode, journal
                ));
            } else {
                // Rich output
                console.status(&format!("SQLite database: {}", self.path));
                console.status(&format!("  Mode: {}", mode));
                if let Some(journal) = journal_mode {
                    console.status(&format!("  Journal: {}", journal.to_uppercase()));
                }
                if let Some(size) = page_size {
                    console.status(&format!("  Page size: {} bytes", size));
                }
            }
        }
    }

    /// Emit transaction state to console if available.
    #[cfg(feature = "console")]
    fn emit_transaction_state(&self, state: &str) {
        if let Some(console) = &self.console {
            if console.mode().is_plain() {
                console.status(&format!("Transaction: {}", state));
            } else {
                console.status(&format!("[{}] Transaction {}", state, state.to_lowercase()));
            }
        }
    }

    /// Emit query timing to console if available.
    #[cfg(feature = "console")]
    fn emit_query_timing(&self, elapsed_ms: f64, rows: usize) {
        if let Some(console) = &self.console {
            console.status(&format!("Query: {:.1}ms, {} rows", elapsed_ms, rows));
        }
    }

    /// Emit query results with PRAGMA-aware formatting.
    #[cfg(feature = "console")]
    fn emit_query_result(&self, sql: &str, col_names: &[String], rows: &[Row], elapsed_ms: f64) {
        if let Some(console) = &self.console {
            // Check if this is a PRAGMA query for special formatting
            let sql_upper = sql.trim().to_uppercase();
            let is_pragma = sql_upper.starts_with("PRAGMA");

            if is_pragma && !rows.is_empty() {
                // Format PRAGMA results as a table
                if console.mode().is_plain() {
                    // Plain text format for agents
                    console.status(&format!("{}:", sql.trim()));
                    // Header
                    console.status(&format!("  {}", col_names.join("|")));
                    // Rows
                    for row in rows.iter().take(20) {
                        let values: Vec<String> = (0..col_names.len())
                            .map(|i| {
                                row.get(i)
                                    .map(|v| format_value(v))
                                    .unwrap_or_else(|| "NULL".to_string())
                            })
                            .collect();
                        console.status(&format!("  {}", values.join("|")));
                    }
                    if rows.len() > 20 {
                        console.status(&format!("  ... and {} more rows", rows.len() - 20));
                    }
                    console.status(&format!("  ({:.1}ms)", elapsed_ms));
                } else {
                    // Rich format with table rendering
                    let mut table_output = String::new();
                    table_output.push_str(&format!("PRAGMA Query Results ({:.1}ms)\n", elapsed_ms));

                    // Calculate column widths
                    let mut widths: Vec<usize> = col_names.iter().map(|c| c.len()).collect();
                    for row in rows.iter().take(20) {
                        for (i, w) in widths.iter_mut().enumerate() {
                            let val_len = row.get(i).map(|v| format_value(v).len()).unwrap_or(4); // "NULL".len()
                            if val_len > *w {
                                *w = val_len;
                            }
                        }
                    }

                    // Build header separator
                    let sep: String = widths
                        .iter()
                        .map(|w| "-".repeat(*w + 2))
                        .collect::<Vec<_>>()
                        .join("+");
                    table_output.push_str(&format!("+{}+\n", sep));

                    // Header row
                    let header: String = col_names
                        .iter()
                        .enumerate()
                        .map(|(i, name)| format!(" {:width$} ", name, width = widths[i]))
                        .collect::<Vec<_>>()
                        .join("|");
                    table_output.push_str(&format!("|{}|\n", header));
                    table_output.push_str(&format!("+{}+\n", sep));

                    // Data rows
                    for row in rows.iter().take(20) {
                        let data: String = (0..col_names.len())
                            .map(|i| {
                                let val = row
                                    .get(i)
                                    .map(|v| format_value(v))
                                    .unwrap_or_else(|| "NULL".to_string());
                                format!(" {:width$} ", val, width = widths[i])
                            })
                            .collect::<Vec<_>>()
                            .join("|");
                        table_output.push_str(&format!("|{}|\n", data));
                    }
                    table_output.push_str(&format!("+{}+", sep));

                    if rows.len() > 20 {
                        table_output.push_str(&format!("\n... and {} more rows", rows.len() - 20));
                    }

                    console.status(&table_output);
                }
            } else {
                // Regular query timing
                self.emit_query_timing(elapsed_ms, rows.len());
            }
        }
    }

    /// Emit execute operation timing to console.
    #[cfg(feature = "console")]
    fn emit_execute_timing(&self, sql: &str, rows_affected: u64, elapsed_ms: f64) {
        if let Some(console) = &self.console {
            let sql_upper = sql.trim().to_uppercase();

            // Provide contextual message based on operation type
            let op_type = if sql_upper.starts_with("INSERT") {
                "Insert"
            } else if sql_upper.starts_with("UPDATE") {
                "Update"
            } else if sql_upper.starts_with("DELETE") {
                "Delete"
            } else if sql_upper.starts_with("CREATE") {
                "Create"
            } else if sql_upper.starts_with("DROP") {
                "Drop"
            } else if sql_upper.starts_with("ALTER") {
                "Alter"
            } else {
                "Execute"
            };

            if console.mode().is_plain() {
                console.status(&format!(
                    "{}: {} rows affected ({:.1}ms)",
                    op_type, rows_affected, elapsed_ms
                ));
            } else {
                console.status(&format!(
                    "[{}] {} rows affected ({:.1}ms)",
                    op_type.to_uppercase(),
                    rows_affected,
                    elapsed_ms
                ));
            }
        }
    }

    /// Emit busy waiting status to console.
    #[cfg(feature = "console")]
    pub fn emit_busy_waiting(&self, elapsed_secs: f64) {
        if let Some(console) = &self.console {
            if console.mode().is_plain() {
                console.status(&format!(
                    "Waiting for database lock... ({:.1}s)",
                    elapsed_secs
                ));
            } else {
                console.status(&format!(
                    "[..] Waiting for database lock... ({:.1}s)",
                    elapsed_secs
                ));
            }
        }
    }

    /// Emit WAL checkpoint progress to console.
    #[cfg(feature = "console")]
    pub fn emit_checkpoint_progress(&self, pages_done: u32, pages_total: u32) {
        if let Some(console) = &self.console {
            let pct = if pages_total > 0 {
                (pages_done as f64 / pages_total as f64) * 100.0
            } else {
                100.0
            };

            if console.mode().is_plain() {
                console.status(&format!(
                    "WAL checkpoint: {:.0}% ({}/{} pages)",
                    pct, pages_done, pages_total
                ));
            } else {
                // ASCII progress bar for rich mode
                let bar_width: usize = 20;
                let filled = ((pct / 100.0) * bar_width as f64).round() as usize;
                let empty = bar_width.saturating_sub(filled);
                let bar = format!("[{}{}]", "=".repeat(filled), " ".repeat(empty));
                console.status(&format!(
                    "WAL checkpoint: {} {:.0}% ({}/{} pages)",
                    bar, pct, pages_done, pages_total
                ));
            }
        }
    }

    /// No-op when console feature is disabled.
    #[cfg(not(feature = "console"))]
    #[allow(dead_code)]
    fn emit_open_status(&self) {}

    /// No-op when console feature is disabled.
    #[cfg(not(feature = "console"))]
    fn emit_transaction_state(&self, _state: &str) {}

    /// No-op when console feature is disabled.
    #[cfg(not(feature = "console"))]
    #[allow(dead_code)]
    fn emit_query_timing(&self, _elapsed_ms: f64, _rows: usize) {}

    /// No-op when console feature is disabled.
    #[cfg(not(feature = "console"))]
    #[allow(dead_code)]
    fn emit_query_result(
        &self,
        _sql: &str,
        _col_names: &[String],
        _rows: &[Row],
        _elapsed_ms: f64,
    ) {
    }

    /// No-op when console feature is disabled.
    #[cfg(not(feature = "console"))]
    #[allow(dead_code)]
    fn emit_execute_timing(&self, _sql: &str, _rows_affected: u64, _elapsed_ms: f64) {}

    /// No-op when console feature is disabled.
    #[cfg(not(feature = "console"))]
    pub fn emit_busy_waiting(&self, _elapsed_secs: f64) {}

    /// No-op when console feature is disabled.
    #[cfg(not(feature = "console"))]
    pub fn emit_checkpoint_progress(&self, _pages_done: u32, _pages_total: u32) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_memory() {
        let conn = SqliteConnection::open_memory().unwrap();
        assert_eq!(conn.path(), ":memory:");
    }

    #[test]
    fn test_execute_raw() {
        let conn = SqliteConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute_raw("INSERT INTO test (name) VALUES ('Alice')")
            .unwrap();
        assert_eq!(conn.changes(), 1);
        assert_eq!(conn.last_insert_rowid(), 1);
    }

    #[test]
    fn test_query_sync() {
        let conn = SqliteConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute_raw("INSERT INTO test (name) VALUES ('Alice'), ('Bob')")
            .unwrap();

        let rows = conn
            .query_sync("SELECT * FROM test ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 2);

        assert_eq!(rows[0].get_named::<i32>("id").unwrap(), 1);
        assert_eq!(rows[0].get_named::<String>("name").unwrap(), "Alice");
        assert_eq!(rows[1].get_named::<i32>("id").unwrap(), 2);
        assert_eq!(rows[1].get_named::<String>("name").unwrap(), "Bob");
    }

    #[test]
    fn test_parameterized_query() {
        let conn = SqliteConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
            .unwrap();

        conn.execute_sync(
            "INSERT INTO test (name, age) VALUES (?, ?)",
            &[Value::Text("Alice".to_string()), Value::Int(30)],
        )
        .unwrap();

        let rows = conn
            .query_sync(
                "SELECT * FROM test WHERE name = ?",
                &[Value::Text("Alice".to_string())],
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_named::<String>("name").unwrap(), "Alice");
        assert_eq!(rows[0].get_named::<i32>("age").unwrap(), 30);
    }

    #[test]
    fn test_null_handling() {
        let conn = SqliteConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();

        conn.execute_sync("INSERT INTO test (name) VALUES (?)", &[Value::Null])
            .unwrap();

        let rows = conn.query_sync("SELECT * FROM test", &[]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_named::<Option<String>>("name").unwrap(), None);
    }

    #[test]
    fn test_transaction() {
        let conn = SqliteConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();

        // Start transaction, insert, rollback
        conn.begin_sync(IsolationLevel::default()).unwrap();
        conn.execute_sync(
            "INSERT INTO test (name) VALUES (?)",
            &[Value::Text("Alice".to_string())],
        )
        .unwrap();
        conn.rollback_sync().unwrap();

        // Verify rollback worked
        let rows = conn.query_sync("SELECT * FROM test", &[]).unwrap();
        assert_eq!(rows.len(), 0);

        // Start transaction, insert, commit
        conn.begin_sync(IsolationLevel::default()).unwrap();
        conn.execute_sync(
            "INSERT INTO test (name) VALUES (?)",
            &[Value::Text("Bob".to_string())],
        )
        .unwrap();
        conn.commit_sync().unwrap();

        // Verify commit worked
        let rows = conn.query_sync("SELECT * FROM test", &[]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_named::<String>("name").unwrap(), "Bob");
    }

    #[test]
    fn test_insert_rowid() {
        let conn = SqliteConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();

        let rowid = conn
            .insert_sync(
                "INSERT INTO test (name) VALUES (?)",
                &[Value::Text("Alice".to_string())],
            )
            .unwrap();
        assert_eq!(rowid, 1);

        let rowid = conn
            .insert_sync(
                "INSERT INTO test (name) VALUES (?)",
                &[Value::Text("Bob".to_string())],
            )
            .unwrap();
        assert_eq!(rowid, 2);
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_type_conversions() {
        let conn = SqliteConnection::open_memory().unwrap();
        conn.execute_raw(
            "CREATE TABLE types (
                b BOOLEAN,
                i INTEGER,
                f REAL,
                t TEXT,
                bl BLOB
            )",
        )
        .unwrap();

        conn.execute_sync(
            "INSERT INTO types VALUES (?, ?, ?, ?, ?)",
            &[
                Value::Bool(true),
                Value::BigInt(42),
                Value::Double(3.14),
                Value::Text("hello".to_string()),
                Value::Bytes(vec![1, 2, 3]),
            ],
        )
        .unwrap();

        let rows = conn.query_sync("SELECT * FROM types", &[]).unwrap();
        assert_eq!(rows.len(), 1);

        // SQLite stores booleans as integers
        let b: i32 = rows[0].get_named("b").unwrap();
        assert_eq!(b, 1);

        let i: i32 = rows[0].get_named("i").unwrap();
        assert_eq!(i, 42);

        let f: f64 = rows[0].get_named("f").unwrap();
        assert!((f - 3.14).abs() < 0.001);

        let t: String = rows[0].get_named("t").unwrap();
        assert_eq!(t, "hello");

        let bl: Vec<u8> = rows[0].get_named("bl").unwrap();
        assert_eq!(bl, vec![1, 2, 3]);
    }

    #[test]
    fn test_open_flags() {
        // Test creating a database with create flag
        let tmp = std::env::temp_dir().join("sqlmodel_test.db");
        let _ = std::fs::remove_file(&tmp); // Ensure it doesn't exist

        let config = SqliteConfig::file(tmp.to_string_lossy().to_string())
            .flags(OpenFlags::create_read_write());
        let conn = SqliteConnection::open(&config).unwrap();
        conn.execute_raw("CREATE TABLE test (id INTEGER)").unwrap();
        drop(conn);

        // Open as read-only
        let config =
            SqliteConfig::file(tmp.to_string_lossy().to_string()).flags(OpenFlags::read_only());
        let conn = SqliteConnection::open(&config).unwrap();

        // Reading should work
        let rows = conn.query_sync("SELECT * FROM test", &[]).unwrap();
        assert_eq!(rows.len(), 0);

        // Writing should fail
        let result = conn.execute_raw("INSERT INTO test VALUES (1)");
        assert!(result.is_err());

        drop(conn);
        let _ = std::fs::remove_file(&tmp);
    }

    // ==================== Console Integration Tests ====================

    #[cfg(feature = "console")]
    mod console_tests {
        use super::*;

        /// Test that ConsoleAware trait is properly implemented.
        #[test]
        fn test_console_aware_trait_impl() {
            let mut conn = SqliteConnection::open_memory().unwrap();

            // Initially no console
            assert!(!conn.has_console());
            assert!(conn.console().is_none());

            // Attach console
            let console = Arc::new(SqlModelConsole::with_mode(
                sqlmodel_console::OutputMode::Plain,
            ));
            conn.set_console(Some(console.clone()));

            // Verify console is attached
            assert!(conn.has_console());
            assert!(conn.console().is_some());

            // Detach console
            conn.set_console(None);
            assert!(!conn.has_console());
        }

        /// Test database open feedback is emitted when console is attached.
        #[test]
        fn test_database_open_feedback() {
            let mut conn = SqliteConnection::open_memory().unwrap();

            // Attaching console should emit open status
            // (output goes to stderr, we just verify no panic)
            let console = Arc::new(SqlModelConsole::with_mode(
                sqlmodel_console::OutputMode::Plain,
            ));
            conn.set_console(Some(console));

            // No panic means success
        }

        /// Test PRAGMA query formatting.
        #[test]
        fn test_pragma_formatting() {
            let mut conn = SqliteConnection::open_memory().unwrap();

            // Create a table to have something in pragma_table_info
            conn.execute_raw("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
                .unwrap();

            // Attach console for formatted output
            let console = Arc::new(SqlModelConsole::with_mode(
                sqlmodel_console::OutputMode::Plain,
            ));
            conn.set_console(Some(console));

            // Execute PRAGMA query - should format as table
            let rows = conn.query_sync("PRAGMA table_info(test)", &[]).unwrap();

            // Verify we got the expected columns
            assert!(!rows.is_empty());
        }

        /// Test transaction state display.
        #[test]
        fn test_transaction_state() {
            let mut conn = SqliteConnection::open_memory().unwrap();
            conn.execute_raw("CREATE TABLE test (id INTEGER PRIMARY KEY)")
                .unwrap();

            // Attach console
            let console = Arc::new(SqlModelConsole::with_mode(
                sqlmodel_console::OutputMode::Plain,
            ));
            conn.set_console(Some(console));

            // Transaction operations should emit state
            conn.begin_sync(IsolationLevel::default()).unwrap();
            conn.execute_sync("INSERT INTO test (id) VALUES (?)", &[Value::Int(1)])
                .unwrap();
            conn.commit_sync().unwrap();

            // Verify the transaction worked
            let rows = conn.query_sync("SELECT * FROM test", &[]).unwrap();
            assert_eq!(rows.len(), 1);
        }

        /// Test WAL checkpoint progress output.
        #[test]
        fn test_wal_checkpoint_progress() {
            let conn = SqliteConnection::open_memory().unwrap();

            // emit_checkpoint_progress should not panic
            conn.emit_checkpoint_progress(50, 100);
            conn.emit_checkpoint_progress(100, 100);
            conn.emit_checkpoint_progress(0, 0);
        }

        /// Test busy timeout feedback output.
        #[test]
        fn test_busy_timeout_feedback() {
            let conn = SqliteConnection::open_memory().unwrap();

            // emit_busy_waiting should not panic
            conn.emit_busy_waiting(0.5);
            conn.emit_busy_waiting(2.1);
        }

        /// Test that console disabled produces no output (no panic).
        #[test]
        fn test_console_disabled_no_output() {
            let conn = SqliteConnection::open_memory().unwrap();

            // Without console, all emit methods should be no-ops
            conn.emit_busy_waiting(1.0);
            conn.emit_checkpoint_progress(10, 100);

            // Query should work without console
            conn.execute_raw("CREATE TABLE test (id INTEGER PRIMARY KEY)")
                .unwrap();
            let rows = conn.query_sync("SELECT * FROM test", &[]).unwrap();
            assert_eq!(rows.len(), 0);
        }

        /// Test plain mode output format (parseable by agents).
        #[test]
        fn test_plain_mode_output() {
            let mut conn = SqliteConnection::open_memory().unwrap();

            // Attach plain mode console
            let console = Arc::new(SqlModelConsole::with_mode(
                sqlmodel_console::OutputMode::Plain,
            ));
            conn.set_console(Some(console.clone()));

            // Verify plain mode is active
            assert!(conn.console().unwrap().is_plain());

            // Execute operations (output should be plain text)
            conn.execute_raw("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
                .unwrap();
            conn.execute_sync(
                "INSERT INTO test (name) VALUES (?)",
                &[Value::Text("Alice".to_string())],
            )
            .unwrap();

            let rows = conn.query_sync("PRAGMA table_info(test)", &[]).unwrap();
            assert!(!rows.is_empty());
        }

        /// Test rich mode output format.
        #[test]
        fn test_rich_mode_output() {
            let mut conn = SqliteConnection::open_memory().unwrap();

            // Attach rich mode console
            let console = Arc::new(SqlModelConsole::with_mode(
                sqlmodel_console::OutputMode::Rich,
            ));
            conn.set_console(Some(console.clone()));

            // Verify rich mode is active
            assert!(conn.console().unwrap().is_rich());

            // Execute operations (output should have formatting)
            conn.execute_raw("CREATE TABLE test (id INTEGER PRIMARY KEY)")
                .unwrap();
            conn.emit_checkpoint_progress(50, 100);
        }
    }
}
