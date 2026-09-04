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
    Connection, Cx, Dialect, Error, IsolationLevel, Outcome, PreparedStatement, Row,
    TransactionMode, TransactionOps, TransactionOptions, Value,
    error::{ConfigError, ConnectionError, ConnectionErrorKind, QueryError, QueryErrorKind},
    row::ColumnInfo,
};
use std::ffi::{CStr, CString, c_int};
use std::future::Future;
use std::ptr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Exact SQLite result codes retained on errors produced by the native driver.
///
/// SQLite extended result codes preserve the primary result in their low byte.
/// Callers that need a fail-closed contract can compare [`Self::primary`] with
/// constants from [`crate::ffi`] while retaining [`Self::extended`] for more
/// precise diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqliteErrorCode {
    primary: c_int,
    extended: c_int,
}

impl SqliteErrorCode {
    const fn from_result_codes(result: c_int, extended: c_int) -> Self {
        Self {
            primary: result & 0xff,
            extended,
        }
    }

    /// SQLite primary result code, such as `SQLITE_READONLY`.
    #[must_use]
    pub const fn primary(self) -> c_int {
        self.primary
    }

    /// SQLite extended result code, such as `SQLITE_READONLY_CANTLOCK`.
    #[must_use]
    pub const fn extended(self) -> c_int {
        self.extended
    }
}

impl std::fmt::Display for SqliteErrorCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "SQLite result code {} (extended {})",
            self.primary, self.extended
        )
    }
}

impl std::error::Error for SqliteErrorCode {}

/// Return exact native SQLite result codes retained on a driver error.
///
/// Errors produced before SQLite is called (for example, SQL containing a NUL
/// byte) and SQLModel lifecycle errors have no native result code and return
/// `None`.
#[must_use]
pub fn sqlite_error_code(error: &Error) -> Option<SqliteErrorCode> {
    std::error::Error::source(error)?
        .downcast_ref::<SqliteErrorCode>()
        .copied()
}

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
    /// Enable shared cache mode (except for a plain `:memory:` database, which
    /// SQLite always keeps private).
    pub shared_cache: bool,
    /// Explicitly disable shared cache mode. Private cache is also the default
    /// when `shared_cache` is false, so process-global SQLite configuration
    /// cannot silently change a connection's cache mode.
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
        } else {
            // A private cache is the fail-closed default. Besides matching
            // SQLite's normal default, an explicit flag prevents a process-wide
            // sqlite3_enable_shared_cache() call elsewhere from silently
            // changing this connection's backup-safety contract. A URI
            // `cache=` parameter can still override this flag and is inspected
            // from SQLite's parsed filename after open.
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
    uses_shared_cache: bool,
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
        if config.flags.shared_cache && config.flags.private_cache {
            return Err(Error::Config(ConfigError {
                message: "SQLite shared_cache and private_cache flags are mutually exclusive"
                    .to_string(),
                source: None,
            }));
        }
        let busy_timeout_ms = c_int::try_from(config.busy_timeout_ms).map_err(|_| {
            Error::Config(ConfigError {
                message: format!(
                    "SQLite busy timeout {}ms exceeds the native {}ms limit",
                    config.busy_timeout_ms,
                    c_int::MAX
                ),
                source: None,
            })
        })?;
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
            let error_code = sqlite_error_code_from_db(db, rc);
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
                source: Some(Box::new(error_code)),
            }));
        }

        // Set busy timeout
        if busy_timeout_ms > 0 {
            // SAFETY: db is valid
            let busy_rc = unsafe { ffi::sqlite3_busy_timeout(db, busy_timeout_ms) };
            if busy_rc != ffi::SQLITE_OK {
                let error_code = sqlite_error_code_from_db(db, busy_rc);
                let msg = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(db)) }
                    .to_string_lossy()
                    .into_owned();
                // SAFETY: db was opened successfully above and is not shared yet.
                unsafe { ffi::sqlite3_close(db) };
                return Err(Error::Connection(ConnectionError {
                    kind: ConnectionErrorKind::Connect,
                    message: format!("Failed to configure SQLite busy timeout: {msg}"),
                    source: Some(Box::new(error_code)),
                }));
            }
        }

        Ok(Self {
            inner: Mutex::new(SqliteInner {
                db,
                in_transaction: false,
            }),
            path: config.path.clone(),
            uses_shared_cache: connection_uses_shared_cache(config),
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
            let error_code = sqlite_error_code_from_db(inner.db, rc);
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
                source: Some(Box::new(error_code)),
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
    ///
    /// The destination must not use SQLite shared-cache mode because this
    /// wrapper cannot coordinate other connections attached to that cache.
    pub fn backup_to_connection(&self, dest: &SqliteConnection) -> Result<(), Error> {
        if std::ptr::eq(self, dest) {
            return Err(Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Connect,
                message: "SQLite backup source and destination must be different connections"
                    .to_string(),
                source: None,
            }));
        }
        // SQLite requires exclusive in-process access to a shared-cache
        // destination for the entire backup operation. This wrapper can lock
        // the two participating connections, but it cannot discover or lock a
        // third connection attached to the same shared cache. Reject that
        // configuration instead of exposing SQLite's documented mutex
        // deadlock/malfunction surface.
        if dest.uses_shared_cache {
            return Err(Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Connect,
                message: "SQLite backup destinations cannot use shared-cache mode".to_string(),
                source: None,
            }));
        }
        let self_first = (std::ptr::from_ref(self) as usize) <= (std::ptr::from_ref(dest) as usize);
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
        let source_busy_timeout_ms = sqlite_busy_timeout_ms(source_db)?;
        let dest_busy_timeout_ms = sqlite_busy_timeout_ms(dest_db)?;

        let main = CString::new("main").expect("static sqlite db name");

        // SAFETY: We hold locks on both connections; db pointers are valid.
        let backup =
            unsafe { ffi::sqlite3_backup_init(dest_db, main.as_ptr(), source_db, main.as_ptr()) };
        if backup.is_null() {
            let result_code = unsafe { ffi::sqlite3_errcode(dest_db) };
            let error_code = sqlite_error_code_from_db(dest_db, result_code);
            let msg = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(dest_db)) }
                .to_string_lossy()
                .into_owned();
            return Err(Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Connect,
                message: format!("SQLite backup init failed: {msg}"),
                source: Some(Box::new(error_code)),
            }));
        }

        let busy_deadline = Instant::now()
            + Duration::from_millis(
                u64::try_from(source_busy_timeout_ms.max(dest_busy_timeout_ms))
                    .expect("SQLite busy timeout is non-negative"),
            );
        // sqlite3_backup_step() may invoke either connection's configured
        // busy handler before returning SQLITE_BUSY. Temporarily disable those
        // native waits and apply the deadline in this loop instead; otherwise
        // a retry started just before the deadline could block for another
        // complete busy-timeout interval. The guard restores both connection
        // settings before their mutex guards are released.
        let _busy_timeout_guard = BackupBusyTimeoutGuard::disable(
            source_db,
            dest_db,
            source_busy_timeout_ms,
            dest_busy_timeout_ms,
        );
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
                let now = Instant::now();
                if now >= busy_deadline {
                    break;
                }
                std::thread::sleep(
                    Duration::from_millis(50).min(busy_deadline.saturating_duration_since(now)),
                );
                if Instant::now() >= busy_deadline {
                    break;
                }
                rc = unsafe { ffi::sqlite3_backup_step(backup, 100) };
                continue;
            }
            break;
        }

        let backup_error = if rc != ffi::SQLITE_DONE && rc != ffi::SQLITE_OK {
            // sqlite3_backup_step returns the authoritative result directly
            // and does not promise to replace the destination connection's
            // error state. Preserve that direct (possibly extended) code;
            // consulting sqlite3_extended_errcode/sqlite3_errmsg here could
            // substitute an unrelated stale error from the same family.
            Some(backup_step_error(dest_db, rc))
        } else {
            None
        };

        let finish_rc = unsafe { ffi::sqlite3_backup_finish(backup) };

        if let Some(error) = backup_error {
            return Err(error);
        }

        if finish_rc != ffi::SQLITE_OK {
            let error_code = sqlite_error_code_from_db(dest_db, finish_rc);
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
                source: Some(Box::new(error_code)),
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
                let error = bind_error(inner.db, sql, i + 1, rc);
                // SAFETY: stmt is valid
                unsafe { ffi::sqlite3_finalize(stmt) };
                return Err(error);
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
                    let error = step_error(inner.db, sql, rc);
                    // SAFETY: stmt is valid
                    unsafe { ffi::sqlite3_finalize(stmt) };
                    return Err(error);
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
                let error = bind_error(inner.db, sql, i + 1, rc);
                // SAFETY: stmt is valid
                unsafe { ffi::sqlite3_finalize(stmt) };
                return Err(error);
            }
        }

        // Execute through SQLITE_DONE. DML with RETURNING can yield one or
        // more SQLITE_ROW results before a later commit-time failure, so the
        // first row is not proof that the statement completed successfully.
        let execution_error = loop {
            // SAFETY: stmt is valid until it reaches DONE or an error below.
            let rc = unsafe { ffi::sqlite3_step(stmt) };
            match rc {
                ffi::SQLITE_ROW => continue,
                ffi::SQLITE_DONE => break None,
                _ => break Some(step_error(inner.db, sql, rc)),
            }
        };

        // SAFETY: stmt is valid
        unsafe { ffi::sqlite3_finalize(stmt) };

        if let Some(error) = execution_error {
            return Err(error);
        }

        // SAFETY: db is valid
        let changes = unsafe { ffi::sqlite3_changes(inner.db) };

        #[cfg(feature = "console")]
        {
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            self.emit_execute_timing(sql, changes as u64, elapsed_ms);
        }

        Ok(changes as u64)
    }

    /// Execute an INSERT and return the last inserted rowid.
    fn insert_sync(&self, sql: &str, params: &[Value]) -> Result<i64, Error> {
        self.execute_sync(sql, params)?;
        Ok(self.last_insert_rowid())
    }

    /// Begin a transaction, mapping the isolation level onto SQLite's locking forms.
    fn begin_sync(&self, isolation: IsolationLevel) -> Result<(), Error> {
        // SQLite doesn't support isolation levels in the same way as PostgreSQL,
        // but we can approximate with different transaction types
        let begin_sql = match isolation {
            IsolationLevel::Serializable => "BEGIN EXCLUSIVE",
            IsolationLevel::RepeatableRead | IsolationLevel::ReadCommitted => "BEGIN IMMEDIATE",
            IsolationLevel::ReadUncommitted => "BEGIN DEFERRED",
        };
        self.begin_statement_sync(begin_sql)
    }

    /// Begin a transaction with explicit [`TransactionOptions`].
    ///
    /// `Default` uses the isolation-level mapping; `Immediate`/`Exclusive`/
    /// `Deferred` select SQLite's locking forms directly; `Concurrent` is
    /// refused because C SQLite has no concurrent-writer mode (use
    /// `sqlmodel-frankensqlite`).
    fn begin_options_sync(&self, options: TransactionOptions) -> Result<(), Error> {
        match options.mode {
            TransactionMode::Default => self.begin_sync(options.isolation),
            TransactionMode::Immediate => self.begin_statement_sync("BEGIN IMMEDIATE"),
            TransactionMode::Exclusive => self.begin_statement_sync("BEGIN EXCLUSIVE"),
            TransactionMode::Deferred => self.begin_statement_sync("BEGIN DEFERRED"),
            TransactionMode::Concurrent => Err(Error::unsupported_transaction_mode(
                options.mode,
                Dialect::Sqlite,
            )),
        }
    }

    /// Issue the given `BEGIN ...` statement and mark the connection as in a transaction.
    fn begin_statement_sync(&self, begin_sql: &str) -> Result<(), Error> {
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
        if let Ok(inner) = self.inner.lock()
            && !inner.db.is_null()
        {
            // SAFETY: db is valid
            unsafe {
                ffi::sqlite3_close_v2(inner.db);
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

    // The C SQLite FFI is synchronous, so every operation runs to completion
    // before its future is returned. Cancellation is therefore honoured at one
    // point: a `Cx` that is already cancelled never reaches SQLite (the
    // statement is not executed and `Outcome::Cancelled` is returned).

    fn query(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
        let outcome = match sqlmodel_core::cancel_requested(cx) {
            Some(reason) => Outcome::Cancelled(reason),
            None => self
                .query_sync(sql, params)
                .map_or_else(Outcome::Err, Outcome::Ok),
        };
        async move { outcome }
    }

    fn query_one(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Option<Row>, Error>> + Send {
        let outcome = match sqlmodel_core::cancel_requested(cx) {
            Some(reason) => Outcome::Cancelled(reason),
            None => self
                .query_sync(sql, params)
                .map(|mut rows| rows.pop())
                .map_or_else(Outcome::Err, Outcome::Ok),
        };
        async move { outcome }
    }

    fn execute(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<u64, Error>> + Send {
        let outcome = match sqlmodel_core::cancel_requested(cx) {
            Some(reason) => Outcome::Cancelled(reason),
            None => self
                .execute_sync(sql, params)
                .map_or_else(Outcome::Err, Outcome::Ok),
        };
        async move { outcome }
    }

    fn insert(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<i64, Error>> + Send {
        let outcome = match sqlmodel_core::cancel_requested(cx) {
            Some(reason) => Outcome::Cancelled(reason),
            None => self
                .insert_sync(sql, params)
                .map_or_else(Outcome::Err, Outcome::Ok),
        };
        async move { outcome }
    }

    fn batch(
        &self,
        cx: &Cx,
        statements: &[(String, Vec<Value>)],
    ) -> impl Future<Output = Outcome<Vec<u64>, Error>> + Send {
        let outcome = if let Some(reason) = sqlmodel_core::cancel_requested(cx) {
            Outcome::Cancelled(reason)
        } else {
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
            match error {
                Some(e) => Outcome::Err(e),
                None => Outcome::Ok(results),
            }
        };
        async move { outcome }
    }

    fn begin(&self, cx: &Cx) -> impl Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
        self.begin_with(cx, IsolationLevel::default())
    }

    fn begin_with(
        &self,
        cx: &Cx,
        isolation: IsolationLevel,
    ) -> impl Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
        let outcome = match sqlmodel_core::cancel_requested(cx) {
            Some(reason) => Outcome::Cancelled(reason),
            None => self
                .begin_sync(isolation)
                .map(|()| SqliteTransaction::new(self))
                .map_or_else(Outcome::Err, Outcome::Ok),
        };
        async move { outcome }
    }

    /// C SQLite offers the three locking forms but no concurrent-writer mode.
    fn supports_transaction_mode(&self, mode: TransactionMode) -> bool {
        !matches!(mode, TransactionMode::Concurrent)
    }

    fn begin_with_options(
        &self,
        cx: &Cx,
        options: TransactionOptions,
    ) -> impl Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
        let outcome = match sqlmodel_core::cancel_requested(cx) {
            Some(reason) => Outcome::Cancelled(reason),
            None => self
                .begin_options_sync(options)
                .map(|()| SqliteTransaction::new(self))
                .map_or_else(Outcome::Err, Outcome::Ok),
        };
        async move { outcome }
    }

    fn prepare(
        &self,
        cx: &Cx,
        sql: &str,
    ) -> impl Future<Output = Outcome<PreparedStatement, Error>> + Send {
        if let Some(reason) = sqlmodel_core::cancel_requested(cx) {
            return std::future::ready(Outcome::Cancelled(reason));
        }
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

        std::future::ready(result.map_or_else(Outcome::Err, Outcome::Ok))
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

    fn ping(&self, cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
        // Simple ping: execute a trivial query
        let outcome = match sqlmodel_core::cancel_requested(cx) {
            Some(reason) => Outcome::Cancelled(reason),
            None => self
                .query_sync("SELECT 1", &[])
                .map(|_| ())
                .map_or_else(Outcome::Err, Outcome::Ok),
        };
        async move { outcome }
    }

    fn close(self, _cx: &Cx) -> impl Future<Output = sqlmodel_core::Result<()>> + Send {
        // Connection is closed on drop
        std::future::ready(Ok(()))
    }
}

// Implement TransactionOps for SqliteTransaction
impl TransactionOps for SqliteTransaction<'_> {
    fn query(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
        // Every operation, including transaction-scoped ones, returns
        // Cancelled for an already-cancelled context before touching the
        // database (mirrors the postgres driver and `Connection` methods).
        let result = if let Some(reason) = sqlmodel_core::cancel_requested(cx) {
            Outcome::Cancelled(reason)
        } else {
            self.conn
                .query_sync(sql, params)
                .map_or_else(Outcome::Err, Outcome::Ok)
        };
        async move { result }
    }

    fn query_one(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Option<Row>, Error>> + Send {
        let result = if let Some(reason) = sqlmodel_core::cancel_requested(cx) {
            Outcome::Cancelled(reason)
        } else {
            self.conn
                .query_sync(sql, params)
                .map(|mut rows| rows.pop())
                .map_or_else(Outcome::Err, Outcome::Ok)
        };
        async move { result }
    }

    fn execute(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<u64, Error>> + Send {
        let result = if let Some(reason) = sqlmodel_core::cancel_requested(cx) {
            Outcome::Cancelled(reason)
        } else {
            self.conn
                .execute_sync(sql, params)
                .map_or_else(Outcome::Err, Outcome::Ok)
        };
        async move { result }
    }

    fn savepoint(&self, cx: &Cx, name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        // Quote identifier to prevent SQL injection
        let quoted_name = format!("\"{}\"", name.replace('"', "\"\""));
        let sql = format!("SAVEPOINT {quoted_name}");
        let result = if let Some(reason) = sqlmodel_core::cancel_requested(cx) {
            Outcome::Cancelled(reason)
        } else {
            self.conn
                .execute_raw(&sql)
                .map_or_else(Outcome::Err, Outcome::Ok)
        };
        async move { result }
    }

    fn rollback_to(&self, cx: &Cx, name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        // Quote identifier to prevent SQL injection
        let quoted_name = format!("\"{}\"", name.replace('"', "\"\""));
        let sql = format!("ROLLBACK TO {quoted_name}");
        let result = if let Some(reason) = sqlmodel_core::cancel_requested(cx) {
            Outcome::Cancelled(reason)
        } else {
            self.conn
                .execute_raw(&sql)
                .map_or_else(Outcome::Err, Outcome::Ok)
        };
        async move { result }
    }

    fn release(&self, cx: &Cx, name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        // Quote identifier to prevent SQL injection
        let quoted_name = format!("\"{}\"", name.replace('"', "\"\""));
        let sql = format!("RELEASE {quoted_name}");
        let result = if let Some(reason) = sqlmodel_core::cancel_requested(cx) {
            Outcome::Cancelled(reason)
        } else {
            self.conn
                .execute_raw(&sql)
                .map_or_else(Outcome::Err, Outcome::Ok)
        };
        async move { result }
    }

    fn commit(mut self, cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
        // A cancelled commit leaves the transaction uncommitted, so the drop
        // path still rolls it back: cancellation can never turn into a
        // partial commit.
        let result = if let Some(reason) = sqlmodel_core::cancel_requested(cx) {
            Outcome::Cancelled(reason)
        } else {
            self.committed = true;
            self.conn
                .commit_sync()
                .map_or_else(Outcome::Err, Outcome::Ok)
        };
        std::future::ready(result)
    }

    fn rollback(mut self, cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
        let result = if let Some(reason) = sqlmodel_core::cancel_requested(cx) {
            Outcome::Cancelled(reason)
        } else {
            self.committed = true; // Prevent double rollback in drop
            self.conn
                .rollback_sync()
                .map_or_else(Outcome::Err, Outcome::Ok)
        };
        std::future::ready(result)
    }
}

// Helper functions

fn connection_uses_shared_cache(config: &SqliteConfig) -> bool {
    // A plain :memory: database is always private even if SHAREDCACHE was
    // requested. Named in-memory databases can share only through URI mode and
    // are handled by the URI cache-mode parser below.
    if config.path == ":memory:" || config.path.is_empty() {
        return false;
    }

    let uri_mode = sqlite_uri_cache_mode(&config.path);
    if config.flags.uri {
        // SQLITE_OPEN_URI guarantees that SQLite interpreted this exact
        // file: URI, so its final cache parameter is authoritative.
        uri_mode.unwrap_or(config.flags.shared_cache)
    } else {
        // URI parsing can also be enabled process-wide by third-party code.
        // Without visibility into that global setting, reject a connection as
        // shared if either interpretation could be shared. This may reject a
        // safe backup but can never admit an unsafe one.
        config.flags.shared_cache || uri_mode == Some(true)
    }
}

struct BackupBusyTimeoutGuard {
    source_db: *mut ffi::sqlite3,
    dest_db: *mut ffi::sqlite3,
    source_timeout_ms: c_int,
    dest_timeout_ms: c_int,
}

impl BackupBusyTimeoutGuard {
    fn disable(
        source_db: *mut ffi::sqlite3,
        dest_db: *mut ffi::sqlite3,
        source_timeout_ms: c_int,
        dest_timeout_ms: c_int,
    ) -> Self {
        // SAFETY: backup_to_connection holds both connection mutexes and both
        // database handles remain valid for this guard's lifetime.
        let source_rc = unsafe { ffi::sqlite3_busy_timeout(source_db, 0) };
        let dest_rc = unsafe { ffi::sqlite3_busy_timeout(dest_db, 0) };
        debug_assert_eq!(source_rc, ffi::SQLITE_OK);
        debug_assert_eq!(dest_rc, ffi::SQLITE_OK);

        Self {
            source_db,
            dest_db,
            source_timeout_ms,
            dest_timeout_ms,
        }
    }
}

fn sqlite_busy_timeout_ms(db: *mut ffi::sqlite3) -> Result<c_int, Error> {
    const SQL: &str = "PRAGMA busy_timeout";
    let stmt = prepare_stmt(db, SQL)?;

    // SAFETY: stmt is valid and PRAGMA busy_timeout returns exactly one row.
    let row_rc = unsafe { ffi::sqlite3_step(stmt) };
    if row_rc != ffi::SQLITE_ROW {
        let error = step_error(db, SQL, row_rc);
        unsafe { ffi::sqlite3_finalize(stmt) };
        return Err(error);
    }
    let timeout_ms = unsafe { ffi::sqlite3_column_int(stmt, 0) };

    let done_rc = unsafe { ffi::sqlite3_step(stmt) };
    if done_rc != ffi::SQLITE_DONE {
        let error = step_error(db, SQL, done_rc);
        unsafe { ffi::sqlite3_finalize(stmt) };
        return Err(error);
    }
    unsafe { ffi::sqlite3_finalize(stmt) };

    Ok(timeout_ms.max(0))
}

impl Drop for BackupBusyTimeoutGuard {
    fn drop(&mut self) {
        // sqlite3_busy_timeout returns SQLITE_OK for valid handles. Both
        // handles are still protected by their connection mutexes here.
        let source_rc =
            unsafe { ffi::sqlite3_busy_timeout(self.source_db, self.source_timeout_ms) };
        let dest_rc = unsafe { ffi::sqlite3_busy_timeout(self.dest_db, self.dest_timeout_ms) };
        debug_assert_eq!(source_rc, ffi::SQLITE_OK);
        debug_assert_eq!(dest_rc, ffi::SQLITE_OK);
    }
}

fn sqlite_uri_cache_mode(path: &str) -> Option<bool> {
    let query = path.strip_prefix("file:")?.split_once('?')?.1;
    let query = query.split_once('#').map_or(query, |(query, _)| query);
    let mut cache_mode = None;

    for parameter in query.split('&') {
        let (name, value) = parameter.split_once('=').unwrap_or((parameter, ""));
        let name = percent_decode_uri_component(name);
        if name != b"cache" {
            continue;
        }

        match percent_decode_uri_component(value).as_slice() {
            b"shared" => cache_mode = Some(true),
            b"private" => cache_mode = Some(false),
            _ => {
                // With URI parsing enabled SQLite rejects unknown cache modes,
                // so this branch cannot describe a successfully opened URI.
                // If URI parsing was disabled, the text is only a filename and
                // the explicit open flag remains authoritative.
            }
        }
    }

    cache_mode
}

fn percent_decode_uri_component(component: &str) -> Vec<u8> {
    let bytes = component.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && let (Some(high), Some(low)) = (bytes.get(index + 1), bytes.get(index + 2))
            && let (Some(high), Some(low)) = (hex_nibble(*high), hex_nibble(*low))
        {
            let byte = (high << 4) | low;
            if byte == 0 {
                // SQLite truncates the current URI component at an
                // encoded NUL and resumes at its next raw separator.
                break;
            }
            decoded.push(byte);
            index += 3;
            continue;
        }

        decoded.push(bytes[index]);
        index += 1;
    }
    decoded
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn sqlite_error_code_from_db(db: *mut ffi::sqlite3, result: c_int) -> SqliteErrorCode {
    let primary = result & 0xff;
    let observed_extended = if db.is_null() {
        result
    } else {
        // SAFETY: every non-null pointer passed here is an open SQLite handle
        // held by the caller for the duration of this observation.
        unsafe { ffi::sqlite3_extended_errcode(db) }
    };
    // Some APIs return an error directly without replacing the connection's
    // previous error state. Never expose a contradictory primary/extended pair;
    // the direct result remains the authoritative fallback in that case.
    let extended = if observed_extended & 0xff == primary {
        observed_extended
    } else {
        result
    };
    SqliteErrorCode::from_result_codes(result, extended)
}

fn direct_sqlite_error_code(result: c_int) -> SqliteErrorCode {
    SqliteErrorCode::from_result_codes(result, result)
}

fn backup_step_error(db: *mut ffi::sqlite3, result: c_int) -> Error {
    let detail = if db.is_null() {
        ffi::error_string(result).to_string()
    } else {
        // SQLite documents backup routine failures on the destination
        // connection. Capture its detailed message while retaining `result`
        // itself as the authoritative exact code.
        unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(db)) }
            .to_string_lossy()
            .into_owned()
    };
    Error::Connection(ConnectionError {
        kind: ConnectionErrorKind::Connect,
        message: format!(
            "SQLite backup failed: {detail} ({})",
            ffi::error_string(result)
        ),
        source: Some(Box::new(direct_sqlite_error_code(result))),
    })
}

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
        return Err(prepare_error(db, sql, rc));
    }

    if stmt.is_null() {
        return Err(Error::Query(QueryError {
            kind: QueryErrorKind::Syntax,
            sql: Some(sql.to_string()),
            sqlstate: None,
            message: "SQL contains no executable statement".to_string(),
            detail: None,
            hint: None,
            position: None,
            source: None,
        }));
    }

    Ok(stmt)
}

fn prepare_error(db: *mut ffi::sqlite3, sql: &str, code: c_int) -> Error {
    // SAFETY: db is valid
    let msg = unsafe {
        let ptr = ffi::sqlite3_errmsg(db);
        CStr::from_ptr(ptr).to_string_lossy().into_owned()
    };
    let error_code = sqlite_error_code_from_db(db, code);

    Error::Query(QueryError {
        kind: error_code_to_kind(code),
        sql: Some(sql.to_string()),
        sqlstate: None,
        message: msg,
        detail: None,
        hint: None,
        position: None,
        source: Some(Box::new(error_code)),
    })
}

fn bind_error(db: *mut ffi::sqlite3, sql: &str, param_index: usize, code: c_int) -> Error {
    // SAFETY: db is valid
    let msg = unsafe {
        let ptr = ffi::sqlite3_errmsg(db);
        CStr::from_ptr(ptr).to_string_lossy().into_owned()
    };
    let error_code = sqlite_error_code_from_db(db, code);

    Error::Query(QueryError {
        kind: error_code_to_kind(code),
        sql: Some(sql.to_string()),
        sqlstate: None,
        message: format!("Failed to bind parameter {}: {}", param_index, msg),
        detail: None,
        hint: None,
        position: None,
        source: Some(Box::new(error_code)),
    })
}

fn step_error(db: *mut ffi::sqlite3, sql: &str, code: c_int) -> Error {
    // SAFETY: db is valid
    let msg = unsafe {
        let ptr = ffi::sqlite3_errmsg(db);
        CStr::from_ptr(ptr).to_string_lossy().into_owned()
    };
    let error_code = sqlite_error_code_from_db(db, code);

    Error::Query(QueryError {
        kind: error_code_to_kind(code),
        sql: Some(sql.to_string()),
        sqlstate: None,
        message: msg,
        detail: None,
        hint: None,
        position: None,
        source: Some(Box::new(error_code)),
    })
}

fn error_code_to_kind(code: c_int) -> QueryErrorKind {
    match code & 0xff {
        ffi::SQLITE_CONSTRAINT => QueryErrorKind::Constraint,
        ffi::SQLITE_BUSY | ffi::SQLITE_LOCKED => QueryErrorKind::Deadlock,
        ffi::SQLITE_PERM | ffi::SQLITE_READONLY | ffi::SQLITE_AUTH => QueryErrorKind::Permission,
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

    static NEXT_TEMP_DB: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn unique_temp_db_path(label: &str) -> std::path::PathBuf {
        let nonce = NEXT_TEMP_DB.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "sqlmodel_{label}_{}_{}.db",
            std::process::id(),
            nonce
        ))
    }

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

        let pre_sqlite_error = conn
            .execute_raw("SELECT \0")
            .expect_err("NUL-bearing SQL must fail before SQLite");
        assert_eq!(
            sqlite_error_code(&pre_sqlite_error),
            None,
            "errors produced before the native call must not invent a SQLite result code"
        );
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
    fn test_prepared_errors_retain_exact_native_codes() {
        let conn = SqliteConnection::open_memory().unwrap();

        let prepare_error = conn
            .query_sync("SELEC 1", &[])
            .expect_err("invalid SQL must fail during prepare");
        let prepare_code = sqlite_error_code(&prepare_error)
            .expect("prepare failures must retain their native SQLite result code");
        assert_eq!(prepare_code.primary(), ffi::SQLITE_ERROR);
        assert_eq!(prepare_code.extended(), ffi::SQLITE_ERROR);

        let query_bind_error = conn
            .query_sync("SELECT ?1", &[Value::Int(1), Value::Int(2)])
            .expect_err("binding beyond the statement parameter count must fail");
        let execute_bind_error = conn
            .execute_sync("SELECT ?1", &[Value::Int(1), Value::Int(2)])
            .expect_err("execute_sync must retain the same bind failure");
        for error in [&query_bind_error, &execute_bind_error] {
            let code = sqlite_error_code(error)
                .expect("bind failures must survive statement finalization");
            assert_eq!(code.primary(), ffi::SQLITE_RANGE);
            assert_eq!(code.extended(), ffi::SQLITE_RANGE);
        }

        conn.execute_raw("CREATE TABLE exact_codes (value INTEGER UNIQUE)")
            .unwrap();
        conn.execute_sync("INSERT INTO exact_codes VALUES (1)", &[])
            .unwrap();
        let execute_step_error = conn
            .execute_sync("INSERT INTO exact_codes VALUES (1)", &[])
            .expect_err("duplicate prepared insert must fail during step");
        let query_step_error = conn
            .query_sync("INSERT INTO exact_codes VALUES (1) RETURNING value", &[])
            .expect_err("query_sync must retain a step failure before finalization");
        for error in [&execute_step_error, &query_step_error] {
            let code = sqlite_error_code(error)
                .expect("step failures must retain their extended SQLite result code");
            assert_eq!(code.primary(), ffi::SQLITE_CONSTRAINT);
            assert_eq!(code.extended(), ffi::SQLITE_CONSTRAINT_UNIQUE);
            assert!(
                matches!(error, Error::Query(query) if query.kind == QueryErrorKind::Constraint),
                "unique violations should map to the constraint error family: {error}"
            );
        }
    }

    #[test]
    fn test_empty_prepared_sql_is_rejected_before_statement_ffi() {
        let conn = SqliteConnection::open_memory().unwrap();

        for sql in ["", " \n\t", "-- comment only\n", "/* comment only */"] {
            for error in [
                conn.query_sync(sql, &[])
                    .expect_err("empty query SQL must not produce a null statement"),
                conn.execute_sync(sql, &[])
                    .expect_err("empty execute SQL must not produce a null statement"),
            ] {
                assert!(
                    matches!(error, Error::Query(ref query) if query.kind == QueryErrorKind::Syntax),
                    "empty prepared SQL should be a typed syntax error: {error}"
                );
                assert!(error.to_string().contains("no executable statement"));
                assert_eq!(sqlite_error_code(&error), None);
            }
        }
    }

    #[test]
    fn test_execute_returning_steps_until_done_and_retains_late_busy() {
        let path = unique_temp_db_path("returning_busy");
        let _ = std::fs::remove_file(&path);
        let config = SqliteConfig::file(path.to_string_lossy().into_owned()).busy_timeout(0);
        let writer = SqliteConnection::open(&config).unwrap();
        writer.execute_raw("PRAGMA journal_mode=DELETE").unwrap();
        writer
            .execute_raw("CREATE TABLE returning_rows (value INTEGER)")
            .unwrap();
        let reader = SqliteConnection::open(&config).unwrap();
        reader.execute_raw("BEGIN DEFERRED").unwrap();
        reader
            .query_sync("SELECT COUNT(*) FROM returning_rows", &[])
            .unwrap();

        let error = writer
            .execute_sync("INSERT INTO returning_rows VALUES (7) RETURNING value", &[])
            .expect_err("the read lock must surface after RETURNING rows but before DONE");
        let code = sqlite_error_code(&error)
            .expect("late RETURNING completion failure must retain its native code");
        assert_eq!(code.primary(), ffi::SQLITE_BUSY);
        assert!(
            matches!(error, Error::Query(ref query) if query.kind == QueryErrorKind::Deadlock),
            "late SQLITE_BUSY should map to the deadlock family: {error}"
        );

        reader.execute_raw("ROLLBACK").unwrap();
        let rows = writer
            .query_sync("SELECT COUNT(*) AS row_count FROM returning_rows", &[])
            .unwrap();
        assert_eq!(rows[0].get_named::<i64>("row_count").unwrap(), 0);
        drop(reader);
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_non_native_connection_preflights_have_no_sqlite_code() {
        let invalid_timeout = SqliteConfig::memory().busy_timeout(c_int::MAX as u32 + 1);
        let Err(timeout_error) = SqliteConnection::open(&invalid_timeout) else {
            panic!("out-of-range native busy timeout must fail closed");
        };
        assert!(matches!(timeout_error, Error::Config(_)));
        assert_eq!(sqlite_error_code(&timeout_error), None);

        let conn = SqliteConnection::open_memory().unwrap();
        let backup_error = conn
            .backup_to_connection(&conn)
            .expect_err("backing a connection up onto itself must fail before locking");
        assert!(
            backup_error
                .to_string()
                .contains("source and destination must be different")
        );
        assert_eq!(sqlite_error_code(&backup_error), None);

        let contradictory_cache_flags = SqliteConfig::memory().flags(OpenFlags {
            shared_cache: true,
            private_cache: true,
            ..OpenFlags::create_read_write()
        });
        let Err(cache_error) = SqliteConnection::open(&contradictory_cache_flags) else {
            panic!("contradictory SQLite cache flags must fail before native open");
        };
        assert!(matches!(cache_error, Error::Config(_)));
        assert_eq!(sqlite_error_code(&cache_error), None);
        assert_ne!(
            OpenFlags::create_read_write().to_sqlite_flags() & ffi::SQLITE_OPEN_PRIVATECACHE,
            0,
            "ordinary opens must override process-global shared-cache mode"
        );
    }

    #[test]
    fn test_backup_copies_data_and_opposite_directions_do_not_deadlock() {
        let left = Arc::new(SqliteConnection::open_memory().unwrap());
        let right = Arc::new(SqliteConnection::open_memory().unwrap());
        left.execute_raw("CREATE TABLE backup_rows (value INTEGER)")
            .unwrap();
        left.execute_raw("INSERT INTO backup_rows VALUES (7)")
            .unwrap();

        left.backup_to_connection(&right)
            .expect("ordinary backup should copy the source database");
        let copied = right
            .query_sync("SELECT value FROM backup_rows", &[])
            .expect("copied table should be readable");
        assert_eq!(copied[0].get_named::<i64>("value").unwrap(), 7);

        let barrier = Arc::new(std::sync::Barrier::new(3));
        let (completed_tx, completed_rx) = std::sync::mpsc::channel();
        let mut workers = Vec::new();
        for (source, destination) in [
            (Arc::clone(&left), Arc::clone(&right)),
            (Arc::clone(&right), Arc::clone(&left)),
        ] {
            let worker_barrier = Arc::clone(&barrier);
            let worker_tx = completed_tx.clone();
            workers.push(std::thread::spawn(move || {
                worker_barrier.wait();
                let result = source.backup_to_connection(&destination);
                worker_tx.send(result).expect("test receiver remains live");
            }));
        }
        drop(completed_tx);
        barrier.wait();
        for _ in 0..2 {
            completed_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("opposing backups must not deadlock")
                .expect("serialized opposing backup should succeed");
        }
        for worker in workers {
            worker.join().expect("backup worker should not panic");
        }
    }

    #[test]
    fn test_backup_lock_retries_respect_deadline_and_restore_busy_timeouts() {
        let source = SqliteConnection::open(&SqliteConfig::memory().busy_timeout(500)).unwrap();
        source
            .execute_raw("CREATE TABLE backup_rows (value INTEGER)")
            .unwrap();
        source
            .execute_raw("INSERT INTO backup_rows VALUES (7)")
            .unwrap();

        let path = unique_temp_db_path("backup_deadline");
        let _ = std::fs::remove_file(&path);
        let destination_config =
            SqliteConfig::file(path.to_string_lossy().into_owned()).busy_timeout(450);
        let destination = SqliteConnection::open(&destination_config).unwrap();
        destination
            .execute_raw("CREATE TABLE old_rows (value INTEGER)")
            .unwrap();
        source.execute_raw("PRAGMA busy_timeout=520").unwrap();
        destination.execute_raw("PRAGMA busy_timeout=470").unwrap();
        let blocker = SqliteConnection::open(&destination_config).unwrap();
        blocker.execute_raw("BEGIN EXCLUSIVE").unwrap();

        let started = Instant::now();
        let error = source
            .backup_to_connection(&destination)
            .expect_err("an exclusive destination lock must block the backup");
        let elapsed = started.elapsed();
        let code = sqlite_error_code(&error)
            .expect("a lock-blocked backup must retain its native SQLite result code");
        assert!(
            matches!(code.primary(), ffi::SQLITE_BUSY | ffi::SQLITE_LOCKED),
            "unexpected lock failure code: {code}"
        );
        assert!(
            elapsed < Duration::from_millis(800),
            "backup retry deadline overran by a native busy-timeout interval: {elapsed:?}"
        );

        blocker.execute_raw("ROLLBACK").unwrap();
        for (connection, expected_timeout) in [(&source, 520), (&destination, 470)] {
            let rows = connection.query_sync("PRAGMA busy_timeout", &[]).unwrap();
            assert_eq!(
                rows[0].get_named::<i32>("timeout").unwrap(),
                expected_timeout
            );
        }

        drop(blocker);
        drop(destination);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_backup_rejects_shared_cache_destination_before_locking() {
        let source = SqliteConnection::open_memory().unwrap();
        let shared_uri = format!(
            "file:sqlmodel_backup_shared_{}?mode=memory&cache=shared",
            std::process::id()
        );
        let destination =
            SqliteConnection::open(&SqliteConfig::file(shared_uri).flags(OpenFlags {
                uri: true,
                ..OpenFlags::create_read_write()
            }))
            .expect("shared-cache connection should open for the preflight test");
        assert!(destination.uses_shared_cache);

        let error = source
            .backup_to_connection(&destination)
            .expect_err("shared-cache backup destination must fail closed");
        assert!(error.to_string().contains("shared-cache mode"));
        assert_eq!(sqlite_error_code(&error), None);

        let plain_memory = SqliteConnection::open(&SqliteConfig::memory().flags(OpenFlags {
            shared_cache: true,
            ..OpenFlags::create_read_write()
        }))
        .expect("plain :memory: remains private even with SHAREDCACHE requested");
        assert!(!plain_memory.uses_shared_cache);
        source
            .backup_to_connection(&plain_memory)
            .expect("a truly private in-memory destination is backup-safe");

        let private_uri = format!(
            "file:sqlmodel_backup_private_{}?mode=memory&cache=private",
            std::process::id()
        );
        let uri_overrides_flag =
            SqliteConnection::open(&SqliteConfig::file(private_uri).flags(OpenFlags {
                uri: true,
                shared_cache: true,
                ..OpenFlags::create_read_write()
            }))
            .expect("URI cache=private should override the shared-cache open flag");
        assert!(!uri_overrides_flag.uses_shared_cache);
        source
            .backup_to_connection(&uri_overrides_flag)
            .expect("an effectively private URI destination is backup-safe");
    }

    #[test]
    fn test_sqlite_uri_cache_mode_matches_sqlite_uri_rules() {
        assert_eq!(
            sqlite_uri_cache_mode("file:memory?mode=memory&cache=shared"),
            Some(true)
        );
        assert_eq!(
            sqlite_uri_cache_mode("file:memory?cache=private&cache=shared"),
            Some(true),
            "SQLite applies duplicate cache parameters in order, so the last one wins"
        );
        assert_eq!(
            sqlite_uri_cache_mode("file:memory?%63ache=%70rivate"),
            Some(false),
            "SQLite percent-decodes URI parameter names and values"
        );
        assert_eq!(
            sqlite_uri_cache_mode("file:memory?cache%00ignored=shared%00ignored"),
            Some(true),
            "SQLite truncates URI components at encoded NUL bytes"
        );
        assert_eq!(
            sqlite_uri_cache_mode("file:memory?CACHE=shared"),
            None,
            "SQLite URI parameter names are case-sensitive"
        );
        assert_eq!(
            sqlite_uri_cache_mode("file:memory?cache=shared#cache=private"),
            Some(true),
            "SQLite ignores URI fragments"
        );
    }

    #[test]
    fn test_backup_failure_retains_native_destination_error() {
        let source = SqliteConnection::open_memory().unwrap();
        source
            .execute_raw("CREATE TABLE backup_source (value INTEGER)")
            .unwrap();

        let path = unique_temp_db_path("readonly_backup");
        let writable = SqliteConnection::open_file(path.to_string_lossy().into_owned()).unwrap();
        writable
            .execute_raw("CREATE TABLE backup_destination (value INTEGER)")
            .unwrap();
        drop(writable);
        let destination = SqliteConnection::open(
            &SqliteConfig::file(path.to_string_lossy().into_owned()).flags(OpenFlags::read_only()),
        )
        .unwrap();

        let error = source
            .backup_to_connection(&destination)
            .expect_err("read-only destination must reject backup writes");
        let code = sqlite_error_code(&error)
            .expect("native backup failure must retain its SQLite result code");
        assert_eq!(code.primary(), ffi::SQLITE_READONLY);
        assert!(error.to_string().to_ascii_lowercase().contains("readonly"));

        drop(destination);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_backup_step_error_preserves_direct_extended_result() {
        let direct_extended = ffi::SQLITE_IOERR | (42 << 8);
        let error = backup_step_error(std::ptr::null_mut(), direct_extended);
        let code = sqlite_error_code(&error)
            .expect("a backup-step failure must retain its direct native result");
        assert_eq!(code.primary(), ffi::SQLITE_IOERR);
        assert_eq!(code.extended(), direct_extended);
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
        let tmp = unique_temp_db_path("open_flags");
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
        let error = conn
            .execute_raw("INSERT INTO test VALUES (1)")
            .expect_err("read-only connection must reject writes");
        let error_code = sqlite_error_code(&error)
            .expect("native write rejection must retain its exact SQLite result code");
        assert_eq!(error_code.primary(), ffi::SQLITE_READONLY);
        assert_eq!(error_code.extended() & 0xff, ffi::SQLITE_READONLY);
        assert!(
            matches!(error, Error::Query(ref query) if query.kind == QueryErrorKind::Permission),
            "SQLITE_READONLY should map to the permission error family: {error}"
        );

        let prepared_error = conn
            .execute_sync("INSERT INTO test VALUES (1)", &[])
            .expect_err("prepared writes must also retain SQLITE_READONLY");
        let prepared_code = sqlite_error_code(&prepared_error)
            .expect("prepared write rejection must retain its native result code");
        assert_eq!(prepared_code.primary(), ffi::SQLITE_READONLY);
        assert!(
            matches!(prepared_error, Error::Query(ref query) if query.kind == QueryErrorKind::Permission),
            "prepared SQLITE_READONLY should map to permission: {prepared_error}"
        );

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
