//! SQLite connection implementation.
//!
//! This module provides safe wrappers around SQLite's C API and implements
//! the Connection trait from sqlmodel-core.

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
        let inner = self.inner.lock().unwrap();
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

    /// Get the last insert rowid.
    pub fn last_insert_rowid(&self) -> i64 {
        let inner = self.inner.lock().unwrap();
        // SAFETY: db is valid
        unsafe { ffi::sqlite3_last_insert_rowid(inner.db) }
    }

    /// Get the number of rows changed by the last statement.
    pub fn changes(&self) -> i32 {
        let inner = self.inner.lock().unwrap();
        // SAFETY: db is valid
        unsafe { ffi::sqlite3_changes(inner.db) }
    }

    /// Prepare and execute a query, returning all rows.
    fn query_sync(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, Error> {
        let inner = self.inner.lock().unwrap();
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
        let columns = Arc::new(ColumnInfo::new(col_names));

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
        Ok(rows)
    }

    /// Prepare and execute a statement, returning rows affected.
    fn execute_sync(&self, sql: &str, params: &[Value]) -> Result<u64, Error> {
        let inner = self.inner.lock().unwrap();
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
        let inner = self.inner.lock().unwrap();
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

        let mut inner = self.inner.lock().unwrap();
        inner.in_transaction = true;
        Ok(())
    }

    /// Commit the current transaction.
    fn commit_sync(&self) -> Result<(), Error> {
        let inner = self.inner.lock().unwrap();
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

        let mut inner = self.inner.lock().unwrap();
        inner.in_transaction = false;
        Ok(())
    }

    /// Rollback the current transaction.
    fn rollback_sync(&self) -> Result<(), Error> {
        let inner = self.inner.lock().unwrap();
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

        let mut inner = self.inner.lock().unwrap();
        inner.in_transaction = false;
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
        let inner = self.inner.lock().unwrap();
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

    fn close(self, _cx: &Cx) -> impl Future<Output = sqlmodel_core::Result<()>> + Send {
        // Connection is closed on drop
        async { Ok(()) }
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
        let sql = format!("SAVEPOINT {}", name);
        let result = self.conn.execute_raw(&sql);
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn rollback_to(&self, _cx: &Cx, name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        let sql = format!("ROLLBACK TO {}", name);
        let result = self.conn.execute_raw(&sql);
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn release(&self, _cx: &Cx, name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        let sql = format!("RELEASE {}", name);
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
}
