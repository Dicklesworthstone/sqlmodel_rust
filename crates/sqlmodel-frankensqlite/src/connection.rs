//! FrankenSQLite connection implementing `sqlmodel_core::Connection`.
//!
//! Wraps [`fsqlite::AsyncConnection`] in `Arc<Mutex<>>` so SQLModel can retain
//! its synchronous adapter surface and transaction bookkeeping while the raw
//! FrankenSQLite connection remains owned by a dedicated worker thread.
//!
//! # fsqlite 0.2 async bridge
//!
//! fsqlite 0.2 made every engine entry point `async fn` with `!Send` futures
//! (the engine lives in `Rc<RefCell<>>`). [`fsqlite::AsyncConnection`] owns
//! that engine and drives its futures on a dedicated large-stack worker. The
//! adapter's synchronous methods exchange commands and results over channels,
//! so engine futures never consume the caller's stack or cross thread
//! boundaries.

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::result_large_err)]

use crate::value::{sqlite_to_value, value_to_sqlite};
use fsqlite::compat::OpenFlags;
use fsqlite_types::value::SqliteValue;
use sqlmodel_core::{
    Connection, Cx, IsolationLevel, Outcome, PreparedStatement, Row, TransactionMode,
    TransactionOps, TransactionOptions, Value,
    error::{ConnectionError, ConnectionErrorKind, Error, QueryError, QueryErrorKind},
    row::ColumnInfo,
};
use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionProfile {
    Generic,
    StrictDurableControlPlane,
}

/// Inner state guarded by a mutex.
struct FrankenInner {
    /// Channel handle for the worker-owned FrankenSQLite connection.
    conn: fsqlite::AsyncConnection,
    /// Whether we are currently inside a transaction.
    in_transaction: bool,
    /// The last inserted rowid (tracked manually since frankensqlite stubs it).
    last_insert_rowid: i64,
}

/// A SQLite connection backed by FrankenSQLite (pure Rust).
///
/// Implements `sqlmodel_core::Connection` and provides sync helper methods
/// (`execute_raw`, `query_sync`, `execute_sync`, etc.) matching the
/// `SqliteConnection` API for drop-in replacement.
pub struct FrankenConnection {
    inner: Arc<Mutex<FrankenInner>>,
    path: String,
    profile: ConnectionProfile,
}

/// How the underlying frankensqlite handle is shut down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloseMode {
    /// Ordinary close: roll back any active transaction, then run the final
    /// passive WAL checkpoint before releasing the handle.
    Checkpoint,
    /// Close without the final WAL checkpoint. Committed frames stay in the
    /// WAL sidecar, where they remain durable and are recovered and
    /// published by the next open.
    SkipCheckpoint,
}

fn required_file_identity(
    file: &std::fs::File,
    path: &str,
) -> Result<fsqlite::FileIdentity, Error> {
    fsqlite::FileIdentity::from_file(file)
        .map_err(Error::Io)?
        .ok_or_else(|| {
            Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Connect,
                message: format!(
                    "strict durable control-plane database `{path}` has no stable filesystem identity"
                ),
                source: None,
            })
        })
}

fn raw_pragma_value(conn: &fsqlite::AsyncConnection, sql: &str) -> Result<SqliteValue, Error> {
    conn.query_sync(sql)
        .map_err(|error| franken_to_query_error(&error, sql))?
        .into_iter()
        .next()
        .and_then(|row| row.values().first().cloned())
        .ok_or_else(|| {
            Error::Custom(format!(
                "profile verification query returned no value: {sql}"
            ))
        })
}

fn install_strict_durable_control_plane_profile(
    conn: &fsqlite::AsyncConnection,
) -> Result<(), Error> {
    for sql in [
        "PRAGMA journal_mode = WAL;",
        "PRAGMA synchronous = FULL;",
        "PRAGMA fsqlite.stmt_microbatch = OFF;",
    ] {
        conn.execute_sync(sql)
            .map_err(|error| franken_to_query_error(&error, sql))?;
    }

    verify_strict_durable_control_plane_profile(conn)
}

fn verify_strict_durable_control_plane_profile(
    conn: &fsqlite::AsyncConnection,
) -> Result<(), Error> {
    let journal_mode = raw_pragma_value(conn, "PRAGMA journal_mode;")?;
    let synchronous = raw_pragma_value(conn, "PRAGMA synchronous;")?;
    let microbatch = raw_pragma_value(conn, "PRAGMA fsqlite.stmt_microbatch;")?;
    let journal_is_wal =
        matches!(journal_mode, SqliteValue::Text(ref mode) if mode.eq_ignore_ascii_case("wal"));
    let synchronous_is_full =
        matches!(synchronous, SqliteValue::Text(ref mode) if mode.eq_ignore_ascii_case("full"));
    if !journal_is_wal || !synchronous_is_full || microbatch != SqliteValue::Integer(0) {
        return Err(Error::Custom(format!(
            "strict durable control-plane profile did not hold: journal_mode={journal_mode:?}, synchronous={synchronous:?}, stmt_microbatch={microbatch:?}"
        )));
    }
    Ok(())
}

fn enforce_profile_sql(profile: ConnectionProfile, sql: &str) -> Result<(), Error> {
    if profile != ConnectionProfile::StrictDurableControlPlane {
        return Ok(());
    }
    let upper = sql.to_ascii_uppercase();
    if upper.contains("PRAGMA")
        && !matches!(
            upper.trim(),
            "PRAGMA TABLE_INFO(FLEET_TRUST_STATE);"
                | "PRAGMA TABLE_INFO(FLEET_TRUST_STATE)"
                | "PRAGMA JOURNAL_MODE;"
                | "PRAGMA JOURNAL_MODE"
                | "PRAGMA SYNCHRONOUS;"
                | "PRAGMA SYNCHRONOUS"
                | "PRAGMA FSQLITE.STMT_MICROBATCH;"
                | "PRAGMA FSQLITE.STMT_MICROBATCH"
        )
    {
        return Err(Error::Custom(
            "strict durable control-plane connection rejects non-allowlisted PRAGMA statements"
                .to_string(),
        ));
    }
    Ok(())
}

fn enforce_scoped_transaction_sql(sql: &str) -> Result<(), Error> {
    const CONTROL_KEYWORDS: [&str; 6] =
        ["BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE"];

    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut first_token_seen = false;
    let mut statement_ended = false;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' | b'`' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == quote {
                        index += 1;
                        if index < bytes.len() && bytes[index] == quote {
                            index += 1;
                            continue;
                        }
                        break;
                    }
                    index += 1;
                }
            }
            b'[' => {
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b']' {
                        index += 1;
                        if index < bytes.len() && bytes[index] == b']' {
                            index += 1;
                            continue;
                        }
                        break;
                    }
                    index += 1;
                }
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while index < bytes.len() && !matches!(bytes[index], b'\n' | b'\r') {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                let token = &sql[start..index];
                if statement_ended {
                    return Err(Error::Custom(
                        "exclusive transaction scope accepts exactly one SQL statement".to_string(),
                    ));
                }
                if !first_token_seen
                    && CONTROL_KEYWORDS
                        .iter()
                        .any(|keyword| token.eq_ignore_ascii_case(keyword))
                {
                    return Err(Error::Custom(format!(
                        "exclusive transaction scope rejects transaction-control keyword `{token}`"
                    )));
                }
                first_token_seen = true;
            }
            b';' if first_token_seen => {
                statement_ended = true;
                index += 1;
            }
            _ => index += 1,
        }
    }
    Ok(())
}

impl FrankenConnection {
    fn from_raw_connection(
        path: String,
        conn: fsqlite::AsyncConnection,
        profile: ConnectionProfile,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FrankenInner {
                conn,
                in_transaction: false,
                last_insert_rowid: 0,
            })),
            path,
            profile,
        }
    }

    /// Open a connection with the given path.
    ///
    /// Use `":memory:"` for an in-memory database, or a file path for
    /// persistent storage.
    pub fn open(path: impl Into<String>) -> Result<Self, Error> {
        let path = path.into();
        let conn =
            fsqlite::AsyncConnection::open_sync(&path).map_err(|e| franken_to_conn_error(&e))?;
        Ok(Self::from_raw_connection(
            path,
            conn,
            ConnectionProfile::Generic,
        ))
    }

    /// Open a connection while requesting a specific page size for newly created databases.
    pub fn open_with_page_size(
        path: impl Into<String>,
        page_size_bytes: u32,
    ) -> Result<Self, Error> {
        let path = path.into();
        let conn = fsqlite::AsyncConnection::open_with_page_size_sync(&path, page_size_bytes)
            .map_err(|e| franken_to_conn_error(&e))?;
        Ok(Self::from_raw_connection(
            path,
            conn,
            ConnectionProfile::Generic,
        ))
    }

    /// Open an existing database without creating or rewriting anything.
    ///
    /// `open` joins the namespace with `NamespaceOpenIntent::Shared`, which
    /// establishes a generation when none exists — and that writes the
    /// `-fsqlite-ns-gate` and `-fsqlite-ns-use` sidecars next to the database.
    /// A caller that promises to be strictly query-only (a read-only pool, a
    /// probe, an inspection tool) must not touch the filesystem at all when the
    /// target is absent or unreadable.
    ///
    /// This routes to fsqlite's `open_existing`, which is
    /// `NamespaceOpenIntent::ReadOnlyExisting`: it joins an existing generation
    /// without creating or rewriting namespace records, and fails closed when
    /// they are missing or malformed. Use this instead of `open` wherever
    /// "query only" is part of the contract.
    pub fn open_existing(path: impl Into<String>) -> Result<Self, Error> {
        let path = path.into();
        let conn = fsqlite::AsyncConnection::open_existing_sync(&path)
            .map_err(|e| franken_to_conn_error(&e))?;
        Ok(Self::from_raw_connection(
            path,
            conn,
            ConnectionProfile::Generic,
        ))
    }

    /// Open an in-memory database.
    pub fn open_memory() -> Result<Self, Error> {
        Self::open(":memory:")
    }

    /// Open a file-based database.
    pub fn open_file(path: impl Into<String>) -> Result<Self, Error> {
        Self::open(path)
    }

    /// Open a file through the sealed control-plane durability profile.
    ///
    /// The pathname is atomically reserved when absent, or pinned by a live
    /// descriptor when present. FrankenSQLite verifies that exact identity
    /// before recovery or schema loading and enables strict multi-process
    /// refusal. Before this method returns, the connection is configured for
    /// WAL per-commit stable-media synchronization and statement
    /// micro-batching is disabled. Policy-changing PRAGMAs are rejected for
    /// the lifetime of the returned connection.
    pub fn open_strict_durable_control_plane_file(path: impl Into<String>) -> Result<Self, Error> {
        let path = path.into();
        let path_ref = Path::new(&path);
        let mut env = fsqlite::ConnectionEnv::default();
        env.set_strict_multi_process(true);

        let conn = match fsqlite::fsqlite_vfs::host_fs::reserve_new_file(path_ref) {
            Ok(reservation) => {
                let identity = required_file_identity(&reservation, &path)?;
                fsqlite::AsyncConnection::open_reserved_with_expected_identity_and_env_sync(
                    &path, identity, env,
                )
                .map_err(|error| franken_to_conn_error(&error))?
            }
            Err(fsqlite_error::FrankenError::Io(error))
                if error.kind() == std::io::ErrorKind::AlreadyExists =>
            {
                let identity_guard =
                    fsqlite::fsqlite_vfs::host_fs::open_existing_regular_file_no_follow(path_ref)
                        .map_err(|error| franken_to_conn_error(&error))?;
                let identity = required_file_identity(&identity_guard, &path)?;
                fsqlite::AsyncConnection::open_existing_with_expected_identity_and_env_sync(
                    &path, identity, env,
                )
                .map_err(|error| franken_to_conn_error(&error))?
            }
            Err(error) => return Err(franken_to_conn_error(&error)),
        };

        install_strict_durable_control_plane_profile(&conn)?;
        Ok(Self::from_raw_connection(
            path,
            conn,
            ConnectionProfile::StrictDurableControlPlane,
        ))
    }

    /// Open an existing file-based database with SQLite read-only flags.
    pub fn open_file_read_only(path: impl Into<String>) -> Result<Self, Error> {
        let path = path.into();
        let conn =
            fsqlite::AsyncConnection::open_with_flags_sync(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)
                .map_err(|e| franken_to_conn_error(&e))?;
        Ok(Self::from_raw_connection(
            path,
            conn,
            ConnectionProfile::Generic,
        ))
    }

    /// Open a file-based database in schema-only read mode.
    ///
    /// This is appropriate for read-only inspection paths that must avoid
    /// introducing writer semantics such as close-time checkpoints.
    pub fn open_schema_only(path: impl Into<String>) -> Result<Self, Error> {
        let path = path.into();
        let conn = fsqlite::AsyncConnection::open_schema_only_sync(&path)
            .map_err(|e| franken_to_conn_error(&e))?;
        Ok(Self::from_raw_connection(
            path,
            conn,
            ConnectionProfile::Generic,
        ))
    }

    /// Open a file-based database with a requested page size for new files.
    pub fn open_file_with_page_size(
        path: impl Into<String>,
        page_size_bytes: u32,
    ) -> Result<Self, Error> {
        Self::open_with_page_size(path, page_size_bytes)
    }

    /// Get the database path.
    pub fn path(&self) -> &str {
        &self.path
    }

    fn close_inner(inner: Arc<Mutex<FrankenInner>>, mode: CloseMode) -> Result<(), Error> {
        match Arc::try_unwrap(inner) {
            Ok(mutex) => {
                let FrankenInner { mut conn, .. } = mutex
                    .into_inner()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let closed = match mode {
                    CloseMode::Checkpoint => conn.close_sync(),
                    CloseMode::SkipCheckpoint => conn.close_without_checkpoint_sync(),
                };
                closed.map_err(|e| franken_to_conn_error(e.as_ref()))
            }
            Err(inner) => Err(Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Disconnected,
                message: format!(
                    "cannot close FrankenConnection cleanly while {} strong references remain",
                    Arc::strong_count(&inner)
                ),
                source: None,
            })),
        }
    }

    /// Close the underlying frankensqlite connection synchronously.
    pub fn close_sync(self) -> Result<(), Error> {
        let Self {
            inner,
            path: _,
            profile: _,
        } = self;
        Self::close_inner(inner, CloseMode::Checkpoint)
    }

    /// Close the underlying connection synchronously, skipping the final WAL
    /// checkpoint.
    ///
    /// FrankenSQLite's ordinary close runs a passive checkpoint (WAL -> DB)
    /// before releasing the handle. This variant skips that step: committed
    /// frames stay in the WAL sidecar, where they remain durable and are
    /// recovered and published by the next open. Use it when teardown latency
    /// or checkpoint contention matters more than compacting the WAL — the
    /// pool retirement path ([`Connection::close_for_pool`]) uses it so bulk
    /// connection churn never serializes on close-time checkpoints.
    pub fn close_without_checkpoint_sync(self) -> Result<(), Error> {
        let Self {
            inner,
            path: _,
            profile: _,
        } = self;
        Self::close_inner(inner, CloseMode::SkipCheckpoint)
    }

    /// Execute SQL directly without parameter binding (for DDL, PRAGMAs, etc.)
    pub fn execute_raw(&self, sql: &str) -> Result<(), Error> {
        enforce_profile_sql(self.profile, sql)?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner
            .conn
            .execute_sync(sql)
            .map_err(|e| franken_to_query_error(&e, sql))?;
        Ok(())
    }

    /// Prepare and execute a query synchronously, returning all rows.
    pub fn query_sync(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, Error> {
        enforce_profile_sql(self.profile, sql)?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let sqlite_params: Vec<SqliteValue> = params.iter().map(value_to_sqlite).collect();
        let schema_columns = self.get_returning_star_columns(sql, &inner);

        let franken_rows = if sqlite_params.is_empty() {
            inner.conn.query_sync(sql)
        } else {
            inner.conn.query_with_params_sync(sql, &sqlite_params)
        }
        .map_err(|e| franken_to_query_error(&e, sql))?;

        Ok(convert_rows_with_schema(
            &franken_rows,
            sql,
            schema_columns.as_deref(),
        ))
    }

    /// Get column names for RETURNING * from the table schema.
    fn get_returning_star_columns(&self, sql: &str, inner: &FrankenInner) -> Option<Vec<String>> {
        let upper = sql.to_uppercase();

        // Check if this is a RETURNING * query
        if !upper.contains(" RETURNING *") && !upper.ends_with("RETURNING *") {
            return None;
        }

        // Extract table name
        let table_name = extract_table_name_for_returning(sql)?;

        // Query PRAGMA table_info to get column names
        let pragma_sql = format!("PRAGMA table_info({})", table_name);
        let Ok(pragma_rows) = inner.conn.query_sync(&pragma_sql) else {
            return None;
        };

        // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
        // Column index 1 is the name
        let columns: Vec<String> = pragma_rows
            .iter()
            .filter_map(|row| {
                row.values().get(1).and_then(|v| match v {
                    SqliteValue::Text(s) => Some(s.to_string()),
                    _ => None,
                })
            })
            .collect();

        if columns.is_empty() {
            None
        } else {
            Some(columns)
        }
    }

    /// Prepare and execute a statement synchronously, returning rows affected.
    pub fn execute_sync(&self, sql: &str, params: &[Value]) -> Result<u64, Error> {
        enforce_profile_sql(self.profile, sql)?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let sqlite_params: Vec<SqliteValue> = params.iter().map(value_to_sqlite).collect();

        let count = if sqlite_params.is_empty() {
            inner.conn.execute_sync(sql)
        } else {
            inner.conn.execute_with_params_sync(sql, &sqlite_params)
        }
        .map_err(|e| franken_to_query_error(&e, sql))?;

        // Track last_insert_rowid for INSERT statements
        if is_insert_sql(sql) {
            // After an INSERT, query last_insert_rowid()
            if let Ok(id) = inner.conn.last_insert_rowid_sync() {
                inner.last_insert_rowid = id;
            }
        }

        Ok(count as u64)
    }

    /// Get the last inserted rowid.
    pub fn last_insert_rowid(&self) -> i64 {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.last_insert_rowid
    }

    /// Get the number of rows changed by the last statement.
    pub fn changes(&self) -> i64 {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Ok(rows) = inner.conn.query_sync("SELECT changes()")
            && let Some(row) = rows.first()
            && let Some(SqliteValue::Integer(n)) = row.get(0)
        {
            return *n;
        }
        0
    }

    /// Execute an INSERT and return the last inserted rowid.
    fn insert_sync(&self, sql: &str, params: &[Value]) -> Result<i64, Error> {
        self.execute_sync(sql, params)?;
        Ok(self.last_insert_rowid())
    }

    /// Begin a transaction, mapping the isolation level onto SQLite's locking forms.
    fn begin_sync(&self, isolation: IsolationLevel) -> Result<(), Error> {
        let begin_sql = match isolation {
            IsolationLevel::Serializable => "BEGIN EXCLUSIVE",
            IsolationLevel::RepeatableRead | IsolationLevel::ReadCommitted => "BEGIN IMMEDIATE",
            IsolationLevel::ReadUncommitted => "BEGIN DEFERRED",
        };
        self.begin_statement_sync(begin_sql)
    }

    /// Begin a transaction with explicit [`TransactionOptions`].
    ///
    /// `Concurrent` issues `BEGIN CONCURRENT`, FrankenSQLite's page-level MVCC
    /// mode in which several connections may write to the same database at
    /// once; conflicting writers fail at `COMMIT` with a retryable error
    /// (`Error::is_retryable()` is true). `Immediate`/`Exclusive`/`Deferred`
    /// select SQLite's locking forms directly; `Default` uses the
    /// isolation-level mapping of [`Connection::begin_with`].
    fn begin_options_sync(&self, options: TransactionOptions) -> Result<(), Error> {
        match options.mode {
            TransactionMode::Default => self.begin_sync(options.isolation),
            TransactionMode::Concurrent => self.begin_statement_sync("BEGIN CONCURRENT"),
            TransactionMode::Immediate => self.begin_statement_sync("BEGIN IMMEDIATE"),
            TransactionMode::Exclusive => self.begin_statement_sync("BEGIN EXCLUSIVE"),
            TransactionMode::Deferred => self.begin_statement_sync("BEGIN DEFERRED"),
        }
    }

    /// Begin a `BEGIN CONCURRENT` transaction synchronously.
    ///
    /// Synchronous counterpart of `Connection::begin_with_options(cx,
    /// TransactionOptions::concurrent())` for callers using the sync helper
    /// family (`execute_sync`, `query_sync`, `commit_sync`, `rollback_sync`).
    pub fn begin_concurrent_sync(&self) -> Result<(), Error> {
        self.begin_statement_sync("BEGIN CONCURRENT")
    }

    /// Issue the given `BEGIN ...` statement and mark the connection as in a transaction.
    fn begin_statement_sync(&self, begin_sql: &str) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
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

        inner
            .conn
            .execute_sync(begin_sql)
            .map_err(|e| franken_to_query_error(&e, begin_sql))?;

        inner.in_transaction = true;
        Ok(())
    }

    /// Commit the current transaction.
    fn commit_sync(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
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

        inner
            .conn
            .execute_sync("COMMIT")
            .map_err(|e| franken_to_query_error(&e, "COMMIT"))?;

        inner.in_transaction = false;
        Ok(())
    }

    /// Rollback the current transaction.
    fn rollback_sync(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
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

        inner
            .conn
            .execute_sync("ROLLBACK")
            .map_err(|e| franken_to_query_error(&e, "ROLLBACK"))?;

        inner.in_transaction = false;
        Ok(())
    }

    /// Execute a closure while holding one exclusive database transaction and
    /// the connection mutex for its full lifetime.
    ///
    /// This is the synchronous initialization/read primitive for security
    /// state that must not expose a probe-to-materialization or
    /// preflight-to-DDL interleaving window. The transaction rolls back on a
    /// closure error or unwind.
    pub fn with_exclusive_transaction<T>(
        &self,
        operation: impl FnOnce(&mut FrankenExclusiveTransaction<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        match self.with_exclusive_transaction_result(operation) {
            Ok(value) => Ok(value),
            Err(FrankenExclusiveTransactionError::Database(error)) => Err(*error),
            Err(FrankenExclusiveTransactionError::Operation(error)) => Err(error),
            Err(FrankenExclusiveTransactionError::OperationRollback {
                operation,
                rollback,
            }) => Err(Error::Custom(format!(
                "exclusive transaction operation failed ({operation}); rollback also failed and the connection remains transaction-bound: {rollback}"
            ))),
        }
    }

    /// Execute a closure with its native operation error while retaining
    /// database-boundary failures as a separate, boxed error variant.
    pub fn with_exclusive_transaction_result<T, E>(
        &self,
        operation: impl FnOnce(&mut FrankenExclusiveTransaction<'_>) -> Result<T, E>,
    ) -> Result<T, FrankenExclusiveTransactionError<E>> {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if inner.in_transaction {
            return Err(FrankenExclusiveTransactionError::Database(Box::new(
                Error::Custom(
                    "cannot start nested exclusive FrankenSQLite transaction".to_string(),
                ),
            )));
        }
        inner
            .conn
            .execute_sync("BEGIN EXCLUSIVE")
            .map_err(|error| {
                FrankenExclusiveTransactionError::Database(Box::new(franken_to_query_error(
                    &error,
                    "BEGIN EXCLUSIVE",
                )))
            })?;
        inner.in_transaction = true;
        if self.profile == ConnectionProfile::StrictDurableControlPlane
            && let Err(error) = verify_strict_durable_control_plane_profile(&inner.conn)
        {
            return match inner.conn.execute_sync("ROLLBACK") {
                Ok(_) => {
                    inner.in_transaction = false;
                    Err(FrankenExclusiveTransactionError::Database(Box::new(error)))
                }
                Err(rollback_error) => Err(FrankenExclusiveTransactionError::Database(Box::new(
                    Error::Custom(format!(
                        "strict profile verification failed ({error}); rollback also failed and the connection remains transaction-bound: {rollback_error}"
                    )),
                ))),
            };
        }
        let mut transaction = FrankenExclusiveTransaction {
            inner: &mut inner,
            profile: self.profile,
            finished: false,
        };
        match operation(&mut transaction) {
            Ok(value) => {
                transaction
                    .commit()
                    .map_err(|error| FrankenExclusiveTransactionError::Database(Box::new(error)))?;
                Ok(value)
            }
            Err(error) => {
                if let Err(rollback_error) = transaction.rollback() {
                    return Err(FrankenExclusiveTransactionError::OperationRollback {
                        operation: error,
                        rollback: Box::new(rollback_error),
                    });
                }
                Err(FrankenExclusiveTransactionError::Operation(error))
            }
        }
    }
}

/// Failure from an exclusive transaction, preserving whether the database
/// boundary or the caller's operation rejected the transaction.
#[derive(Debug)]
pub enum FrankenExclusiveTransactionError<E> {
    /// The database failed to begin, verify, commit, or roll back the transaction.
    Database(Box<Error>),
    /// The caller rejected the transaction and rollback succeeded.
    Operation(E),
    /// The caller rejected the transaction and rollback also failed.
    OperationRollback {
        /// The original caller error.
        operation: E,
        /// The database rollback failure.
        rollback: Box<Error>,
    },
}

impl<E: std::fmt::Display> std::fmt::Display for FrankenExclusiveTransactionError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(error) => {
                write!(formatter, "exclusive transaction database failure: {error}")
            }
            Self::Operation(error) => {
                write!(formatter, "exclusive transaction operation failed: {error}")
            }
            Self::OperationRollback {
                operation,
                rollback,
            } => write!(
                formatter,
                "exclusive transaction operation failed ({operation}); rollback also failed and the connection remains transaction-bound: {rollback}"
            ),
        }
    }
}

impl<E> std::error::Error for FrankenExclusiveTransactionError<E> where
    E: std::error::Error + 'static
{
}

/// Synchronous operations scoped to one mutex-held exclusive transaction.
pub struct FrankenExclusiveTransaction<'a> {
    inner: &'a mut FrankenInner,
    profile: ConnectionProfile,
    finished: bool,
}

impl FrankenExclusiveTransaction<'_> {
    /// Execute DDL or a parameter-free statement inside the transaction.
    pub fn execute_raw(&mut self, sql: &str) -> Result<(), Error> {
        enforce_scoped_transaction_sql(sql)?;
        enforce_profile_sql(self.profile, sql)?;
        self.inner
            .conn
            .execute_sync(sql)
            .map_err(|error| franken_to_query_error(&error, sql))?;
        Ok(())
    }

    /// Execute a query inside the transaction and return converted SQLModel rows.
    pub fn query_sync(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Row>, Error> {
        enforce_scoped_transaction_sql(sql)?;
        enforce_profile_sql(self.profile, sql)?;
        let sqlite_params: Vec<SqliteValue> = params.iter().map(value_to_sqlite).collect();
        let inner = &*self.inner;
        let rows = if sqlite_params.is_empty() {
            inner.conn.query_sync(sql)
        } else {
            inner.conn.query_with_params_sync(sql, &sqlite_params)
        }
        .map_err(|error| franken_to_query_error(&error, sql))?;
        Ok(convert_rows_with_schema(&rows, sql, None))
    }

    /// Execute a parameterized statement inside the transaction.
    pub fn execute_sync(&mut self, sql: &str, params: &[Value]) -> Result<u64, Error> {
        enforce_scoped_transaction_sql(sql)?;
        enforce_profile_sql(self.profile, sql)?;
        let sqlite_params: Vec<SqliteValue> = params.iter().map(value_to_sqlite).collect();
        let inner = &*self.inner;
        let count = if sqlite_params.is_empty() {
            inner.conn.execute_sync(sql)
        } else {
            inner.conn.execute_with_params_sync(sql, &sqlite_params)
        }
        .map_err(|error| franken_to_query_error(&error, sql))?;
        Ok(count as u64)
    }

    fn commit(&mut self) -> Result<(), Error> {
        match self.inner.conn.execute_sync("COMMIT") {
            Ok(_) => {
                self.inner.in_transaction = false;
                self.finished = true;
                Ok(())
            }
            Err(commit_error) => match self.inner.conn.execute_sync("ROLLBACK") {
                Ok(_) => {
                    self.inner.in_transaction = false;
                    self.finished = true;
                    Err(franken_to_query_error(&commit_error, "COMMIT"))
                }
                Err(rollback_error) => Err(Error::Custom(format!(
                    "exclusive transaction commit failed ({commit_error}); rollback also failed and the connection remains transaction-bound: {rollback_error}"
                ))),
            },
        }
    }

    fn rollback(&mut self) -> Result<(), Error> {
        self.inner
            .conn
            .execute_sync("ROLLBACK")
            .map_err(|error| franken_to_query_error(&error, "ROLLBACK"))?;
        self.inner.in_transaction = false;
        self.finished = true;
        Ok(())
    }
}

impl Drop for FrankenExclusiveTransaction<'_> {
    fn drop(&mut self) {
        if !self.finished && self.inner.conn.execute_sync("ROLLBACK").is_ok() {
            self.inner.in_transaction = false;
        }
    }
}

// ── Connection trait impl ─────────────────────────────────────────────────

impl Connection for FrankenConnection {
    type Tx<'conn>
        = FrankenTransaction<'conn>
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
            .map(|()| FrankenTransaction::new(self));
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    /// FrankenSQLite supports every mode: the three SQLite locking forms and
    /// `BEGIN CONCURRENT` (page-level MVCC).
    fn supports_transaction_mode(&self, _mode: TransactionMode) -> bool {
        true
    }

    fn begin_with_options(
        &self,
        _cx: &Cx,
        options: TransactionOptions,
    ) -> impl Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
        let result = self
            .begin_options_sync(options)
            .map(|()| FrankenTransaction::new(self));
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn prepare(
        &self,
        _cx: &Cx,
        sql: &str,
    ) -> impl Future<Output = Outcome<PreparedStatement, Error>> + Send {
        // Count parameters (simple heuristic: count ?N placeholders)
        let param_count = count_params(sql);
        let id = sql.as_ptr() as u64;

        // Try to infer column names from the SQL
        let columns = infer_column_names(sql);

        let stmt = if columns.is_empty() {
            PreparedStatement::new(id, sql.to_string(), param_count)
        } else {
            PreparedStatement::with_columns(id, sql.to_string(), param_count, columns)
        };

        async move { Outcome::Ok(stmt) }
    }

    fn query_prepared(
        &self,
        cx: &Cx,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
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
        let result = self.query_sync("SELECT 1", &[]).map(|_| ());
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn close(self, _cx: &Cx) -> impl Future<Output = sqlmodel_core::Result<()>> + Send {
        std::future::ready(self.close_sync())
    }

    fn close_for_pool(self, _cx: &Cx) -> impl Future<Output = sqlmodel_core::Result<()>> + Send {
        std::future::ready(self.close_without_checkpoint_sync())
    }
}

// ── Transaction ───────────────────────────────────────────────────────────

/// A FrankenSQLite transaction.
pub struct FrankenTransaction<'conn> {
    conn: &'conn FrankenConnection,
    committed: bool,
}

impl<'conn> FrankenTransaction<'conn> {
    fn new(conn: &'conn FrankenConnection) -> Self {
        Self {
            conn,
            committed: false,
        }
    }
}

impl Drop for FrankenTransaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.conn.rollback_sync();
        }
    }
}

impl TransactionOps for FrankenTransaction<'_> {
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
        let quoted = format!("\"{}\"", name.replace('"', "\"\""));
        let sql = format!("SAVEPOINT {quoted}");
        let result = self.conn.execute_raw(&sql);
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn rollback_to(&self, _cx: &Cx, name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        let quoted = format!("\"{}\"", name.replace('"', "\"\""));
        let sql = format!("ROLLBACK TO {quoted}");
        let result = self.conn.execute_raw(&sql);
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn release(&self, _cx: &Cx, name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        let quoted = format!("\"{}\"", name.replace('"', "\"\""));
        let sql = format!("RELEASE {quoted}");
        let result = self.conn.execute_raw(&sql);
        async move { result.map_or_else(Outcome::Err, Outcome::Ok) }
    }

    fn commit(mut self, _cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
        self.committed = true;
        std::future::ready(
            self.conn
                .commit_sync()
                .map_or_else(Outcome::Err, Outcome::Ok),
        )
    }

    fn rollback(mut self, _cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
        self.committed = true; // Prevent double rollback in drop
        std::future::ready(
            self.conn
                .rollback_sync()
                .map_or_else(Outcome::Err, Outcome::Ok),
        )
    }
}

// ── Helper functions ──────────────────────────────────────────────────────

/// Convert frankensqlite rows to sqlmodel-core rows.
///
/// frankensqlite `Row` has no column names, so we infer them from the SQL
/// or fall back to positional names (`_c0`, `_c1`, ...).
#[allow(dead_code)]
fn convert_rows(franken_rows: &[fsqlite_core::connection::Row], sql: &str) -> Vec<Row> {
    convert_rows_with_schema(franken_rows, sql, None)
}

/// Convert frankensqlite rows to sqlmodel-core rows with optional schema-provided column names.
///
/// If `schema_columns` is provided (e.g., from PRAGMA table_info for RETURNING *),
/// those names are used instead of inferring from SQL.
fn convert_rows_with_schema(
    franken_rows: &[fsqlite_core::connection::Row],
    sql: &str,
    schema_columns: Option<&[String]>,
) -> Vec<Row> {
    if franken_rows.is_empty() {
        return Vec::new();
    }

    // Determine column count from first row
    let col_count = franken_rows[0].values().len();

    // Use schema columns if provided, otherwise infer from SQL
    let mut col_names = if let Some(schema_cols) = schema_columns {
        schema_cols.to_vec()
    } else {
        infer_column_names(sql)
    };

    // Pad or trim to match actual column count
    while col_names.len() < col_count {
        col_names.push(format!("_c{}", col_names.len()));
    }
    col_names.truncate(col_count);

    let columns = Arc::new(ColumnInfo::new(col_names));

    franken_rows
        .iter()
        .map(|fr| {
            let values: Vec<Value> = fr.values().iter().map(sqlite_to_value).collect();
            Row::with_columns(Arc::clone(&columns), values)
        })
        .collect()
}

/// Infer column names from SQL text.
///
/// Handles common patterns:
/// - `SELECT col1, col2 AS alias, ...`
/// - `PRAGMA table_info(...)` and other PRAGMA results
/// - Expression-only SELECT with aliases
///
/// Falls back to empty vec if parsing fails.
fn infer_column_names(sql: &str) -> Vec<String> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    // PRAGMA column name lookup
    if upper.starts_with("PRAGMA") {
        return infer_pragma_columns(&upper);
    }

    // For SELECT, try to extract column names from the result columns
    if upper.starts_with("SELECT") || upper.starts_with("WITH") {
        return infer_select_columns(trimmed);
    }

    // For INSERT/UPDATE/DELETE with RETURNING clause
    if upper.contains(" RETURNING ") || upper.ends_with(" RETURNING *") {
        return infer_returning_columns(trimmed);
    }

    Vec::new()
}

/// Infer column names for PRAGMA results.
fn infer_pragma_columns(upper_sql: &str) -> Vec<String> {
    // Extract PRAGMA name (e.g., "PRAGMA table_info(x)" -> "table_info")
    let after_pragma = upper_sql.trim_start_matches("PRAGMA").trim();
    let pragma_name = after_pragma
        .split(|c: char| c == '(' || c == ';' || c == '=' || c.is_whitespace())
        .next()
        .unwrap_or("")
        .trim();

    match pragma_name {
        "TABLE_INFO" | "TABLE_XINFO" => {
            vec![
                "cid".into(),
                "name".into(),
                "type".into(),
                "notnull".into(),
                "dflt_value".into(),
                "pk".into(),
            ]
        }
        "INDEX_LIST" => vec![
            "seq".into(),
            "name".into(),
            "unique".into(),
            "origin".into(),
            "partial".into(),
        ],
        "INDEX_INFO" | "INDEX_XINFO" => {
            vec!["seqno".into(), "cid".into(), "name".into()]
        }
        "FOREIGN_KEY_LIST" => vec![
            "id".into(),
            "seq".into(),
            "table".into(),
            "from".into(),
            "to".into(),
            "on_update".into(),
            "on_delete".into(),
            "match".into(),
        ],
        "DATABASE_LIST" => vec!["seq".into(), "name".into(), "file".into()],
        "COMPILE_OPTIONS" => vec!["compile_option".into()],
        "COLLATION_LIST" => vec!["seq".into(), "name".into()],
        "INTEGRITY_CHECK" => vec!["integrity_check".into()],
        "QUICK_CHECK" => vec!["quick_check".into()],
        "WAL_CHECKPOINT" => vec!["busy".into(), "log".into(), "checkpointed".into()],
        "FREELIST_COUNT" => vec!["freelist_count".into()],
        "PAGE_COUNT" => vec!["page_count".into()],
        _ => {
            // For simple PRAGMA (e.g., PRAGMA journal_mode), return the pragma name
            if !after_pragma.contains('(') && !after_pragma.contains('=') {
                vec![pragma_name.to_lowercase()]
            } else {
                Vec::new()
            }
        }
    }
}

/// Infer column names from a SELECT statement.
///
/// Extracts aliases and bare column references from the result column list.
fn infer_select_columns(sql: &str) -> Vec<String> {
    // Find the columns between SELECT and FROM (or end of statement)
    let upper = sql.to_uppercase();

    // Skip past WITH clause if present
    let select_start = if upper.starts_with("WITH") {
        // Find the actual SELECT after the CTE
        if let Some(pos) = find_main_select(&upper) {
            pos
        } else {
            return Vec::new();
        }
    } else {
        0
    };

    let after_select = &sql[select_start..];
    let upper_after = &upper[select_start..];

    // Skip SELECT [DISTINCT] keyword
    let col_start = if upper_after.starts_with("SELECT DISTINCT") {
        15
    } else if upper_after.starts_with("SELECT ALL") {
        10
    } else if upper_after.starts_with("SELECT") {
        6
    } else {
        return Vec::new();
    };

    let cols_str = &after_select[col_start..];

    // Find the FROM clause (respecting parentheses depth)
    let from_pos = find_keyword_at_depth_zero(cols_str, "FROM");
    let cols_region = if let Some(pos) = from_pos {
        &cols_str[..pos]
    } else {
        // No FROM: everything after SELECT is result columns (minus ORDER BY, LIMIT, etc.)
        let end_pos = find_keyword_at_depth_zero(cols_str, "ORDER")
            .or_else(|| find_keyword_at_depth_zero(cols_str, "LIMIT"))
            .or_else(|| find_keyword_at_depth_zero(cols_str, "GROUP"))
            .or_else(|| find_keyword_at_depth_zero(cols_str, "HAVING"))
            .or_else(|| cols_str.find(';'));
        if let Some(pos) = end_pos {
            &cols_str[..pos]
        } else {
            cols_str
        }
    };

    // Split by commas (respecting parentheses depth)
    let columns = split_at_depth_zero(cols_region, ',');

    columns
        .iter()
        .map(|col| extract_column_name(col.trim()))
        .collect()
}

/// Infer column names from a RETURNING clause in INSERT/UPDATE/DELETE.
///
/// For `RETURNING *`, we return `["*"]` and let the caller handle expansion.
/// For explicit columns, we parse them like SELECT columns.
fn infer_returning_columns(sql: &str) -> Vec<String> {
    let upper = sql.to_uppercase();

    // Find RETURNING keyword
    let Some(returning_pos) = find_keyword_at_depth_zero(&upper, "RETURNING") else {
        return Vec::new();
    };

    // Extract the part after RETURNING
    let after_returning = &sql[returning_pos + 9..].trim_start();

    // Handle "RETURNING *"
    if after_returning.trim() == "*"
        || after_returning.starts_with("* ")
        || after_returning.starts_with("*;")
    {
        // For RETURNING *, we need to get column names from the table.
        // Extract table name from INSERT INTO or UPDATE or DELETE FROM.
        if let Some(table_name) = extract_table_name_for_returning(sql) {
            // Return a marker that indicates we need schema lookup
            return vec![format!("__returning_star_table:{table_name}")];
        }
        return vec!["*".to_string()];
    }

    // Parse explicit column list (same logic as SELECT columns)
    // Find end markers (semicolon or end of string)
    let end_pos = after_returning.find(';').unwrap_or(after_returning.len());
    let cols_region = &after_returning[..end_pos];

    // Split by commas at depth 0
    let columns = split_at_depth_zero(cols_region, ',');

    columns
        .iter()
        .map(|col| extract_column_name(col.trim()))
        .collect()
}

/// Extract the table name from INSERT INTO, UPDATE, or DELETE FROM for RETURNING.
fn extract_table_name_for_returning(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();

    // INSERT INTO table_name (...)
    if upper.starts_with("INSERT")
        && let Some(into_pos) = upper.find(" INTO ")
    {
        let after_into = &sql[into_pos + 6..].trim_start();
        // Table name is the next word (may be quoted)
        let table = extract_identifier(after_into);
        if !table.is_empty() {
            return Some(table);
        }
    }

    // UPDATE table_name SET ...
    if upper.starts_with("UPDATE") {
        let after_update = &sql[6..].trim_start();
        let table = extract_identifier(after_update);
        if !table.is_empty() {
            return Some(table);
        }
    }

    // DELETE FROM table_name ...
    if upper.starts_with("DELETE")
        && let Some(from_pos) = upper.find(" FROM ")
    {
        let after_from = &sql[from_pos + 6..].trim_start();
        let table = extract_identifier(after_from);
        if !table.is_empty() {
            return Some(table);
        }
    }

    None
}

/// Extract an identifier (table/column name) from the start of a string.
/// Handles quoted identifiers with double quotes.
fn extract_identifier(s: &str) -> String {
    let trimmed = s.trim_start();
    if trimmed.is_empty() {
        return String::new();
    }

    // Quoted identifier
    if let Some(stripped) = trimmed.strip_prefix('"') {
        if let Some(end) = stripped.find('"') {
            return stripped[..end].to_string();
        }
        return String::new();
    }

    // Unquoted identifier
    let end = trimmed
        .find(|c: char| !c.is_alphanumeric() && c != '_')
        .unwrap_or(trimmed.len());
    trimmed[..end].to_string()
}

/// Extract a column name or alias from a result column expression.
fn extract_column_name(col_expr: &str) -> String {
    let trimmed = col_expr.trim();

    // Check for AS alias (case-insensitive) — search backwards to handle
    // expressions containing "AS" in sub-expressions.
    // We need to find " AS " at depth 0.
    if let Some(as_pos) = find_last_as_at_depth_zero(trimmed) {
        let alias = trimmed[as_pos + 4..].trim().trim_matches('"');
        return alias.to_string();
    }

    // Star expansion — return *
    if trimmed == "*" {
        return "*".to_string();
    }

    // Table.column — return just column
    if let Some(dot_pos) = trimmed.rfind('.') {
        return trimmed[dot_pos + 1..].trim_matches('"').to_string();
    }

    // Bare identifier
    trimmed.trim_matches('"').to_string()
}

/// Find the last occurrence of " AS " at parentheses depth 0 (case-insensitive).
fn find_last_as_at_depth_zero(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len < 4 {
        return None;
    }
    let mut depth = 0i32;
    let mut last_match = None;

    // Track depth forward, record all " AS " positions at depth 0
    for i in 0..len {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        // Check for " AS " pattern: space, A/a, S/s, space
        if depth == 0
            && i + 3 < len
            && (bytes[i] == b' ')
            && (bytes[i + 1] == b'A' || bytes[i + 1] == b'a')
            && (bytes[i + 2] == b'S' || bytes[i + 2] == b's')
            && (bytes[i + 3] == b' ')
        {
            last_match = Some(i);
        }
    }
    last_match
}

/// Find a keyword at parentheses depth 0.
fn find_keyword_at_depth_zero(s: &str, keyword: &str) -> Option<usize> {
    let upper = s.to_uppercase();
    let kw_upper = keyword.to_uppercase();
    let kw_len = kw_upper.len();
    let mut depth = 0i32;

    for (i, c) in upper.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        if depth == 0 && upper[i..].starts_with(&kw_upper) {
            // Ensure it's a word boundary (alphanumeric OR underscore counts as word char)
            let is_word_char = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
            let before_ok = i == 0 || !is_word_char(upper.as_bytes()[i - 1]);
            let after_ok = i + kw_len >= upper.len() || !is_word_char(upper.as_bytes()[i + kw_len]);
            if before_ok && after_ok {
                return Some(i);
            }
        }
    }
    None
}

/// Split a string by a delimiter at parentheses depth 0.
fn split_at_depth_zero(s: &str, delim: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;

    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ if c == delim && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Find the position of the main SELECT in a WITH ... SELECT statement.
fn find_main_select(upper: &str) -> Option<usize> {
    // Walk past CTE definitions (respecting parentheses)
    let mut depth = 0i32;
    let bytes = upper.as_bytes();
    let mut i = 4; // Skip "WITH"

    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'S' if depth == 0 && upper[i..].starts_with("SELECT") => {
                return Some(i);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Check if SQL is an INSERT statement (case-insensitive).
fn is_insert_sql(sql: &str) -> bool {
    let trimmed = sql.trim().to_uppercase();
    trimmed.starts_with("INSERT")
        || trimmed.starts_with("REPLACE")
        || trimmed.starts_with("INSERT OR")
}

/// Count parameter placeholders in SQL (?1, ?2, etc. or bare ?).
fn count_params(sql: &str) -> usize {
    let mut max_param = 0usize;
    let mut bare_count = 0usize;
    let bytes = sql.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'?' {
            i += 1;
            let mut num = 0u64;
            let mut has_digits = false;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                num = num * 10 + u64::from(bytes[i] - b'0');
                has_digits = true;
                i += 1;
            }
            if has_digits {
                max_param = max_param.max(num as usize);
            } else {
                bare_count += 1;
            }
        } else {
            i += 1;
        }
    }

    if max_param > 0 { max_param } else { bare_count }
}

// ── Error conversion ──────────────────────────────────────────────────────

fn franken_to_conn_error(e: &fsqlite_error::FrankenError) -> Error {
    Error::Connection(ConnectionError {
        kind: ConnectionErrorKind::Connect,
        message: e.to_string(),
        source: None,
    })
}

fn franken_to_query_error(e: &fsqlite_error::FrankenError, sql: &str) -> Error {
    use fsqlite_error::FrankenError;

    let kind = match e {
        FrankenError::UniqueViolation { .. } | FrankenError::NotNullViolation { .. } => {
            QueryErrorKind::Constraint
        }
        FrankenError::ForeignKeyViolation | FrankenError::CheckViolation { .. } => {
            QueryErrorKind::Constraint
        }
        FrankenError::WriteConflict { .. } | FrankenError::SerializationFailure { .. } => {
            QueryErrorKind::Deadlock
        }
        FrankenError::SyntaxError { .. } => QueryErrorKind::Syntax,
        FrankenError::QueryReturnedNoRows => QueryErrorKind::NotFound,
        _ => QueryErrorKind::Database,
    };

    Error::Query(QueryError {
        kind,
        sql: Some(sql.to_string()),
        sqlstate: None,
        message: e.to_string(),
        detail: None,
        hint: None,
        position: None,
        source: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Remove a database file and every engine sidecar the fsqlite 0.3.x
    /// family can leave behind (`-wal`/`-shm`/`-journal`, the ns gate/use
    /// pair, the WAL certificate files, the migration-state marker, and the
    /// time-travel history files). Orphaned sidecars without their main
    /// database make the engine refuse a fresh open, so every file-based test
    /// must clean the full family before and after running.
    fn remove_db_family(path: &str) {
        for suffix in [
            "",
            "-wal",
            "-shm",
            "-journal",
            "-fsqlite-ns-gate",
            "-fsqlite-ns-use",
            "-wal-cert",
            "-wal-cert-head",
            ".fsqlite-migration-state",
            ".fsqlite-history",
            ".fsqlite-history-idx",
        ] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }

    #[test]
    fn open_memory_succeeds() {
        let conn = FrankenConnection::open_memory().expect("should open in-memory db");
        assert_eq!(conn.path(), ":memory:");
    }

    #[test]
    fn close_sync_succeeds() {
        let conn = FrankenConnection::open_memory().expect("should open in-memory db");
        conn.close_sync()
            .expect("close_sync should close the underlying frankensqlite connection");
    }

    #[test]
    fn small_stack_open_schema_and_insert_are_worker_backed() {
        let dir = std::env::temp_dir().join("sqlmodel_frankensqlite_small_stack_test");
        std::fs::create_dir_all(&dir).expect("small-stack test directory should exist");
        let path = dir
            .join(format!("fresh-schema-{}.db", std::process::id()))
            .to_string_lossy()
            .into_owned();
        remove_db_family(&path);

        let worker_path = path.clone();
        std::thread::Builder::new()
            .name("sqlmodel-frankensqlite-small-stack".to_string())
            .stack_size(256 * 1024)
            .spawn(move || {
                let conn = FrankenConnection::open_file(&worker_path)
                    .expect("small-stack consumer should open through the worker");
                conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
                    .expect("small-stack consumer should create a fresh schema through the worker");
                conn.execute_sync(
                    "INSERT INTO t (id, name) VALUES (?1, ?2)",
                    &[Value::BigInt(1), Value::Text("stack-safe".into())],
                )
                .expect("small-stack consumer should insert through the worker");
                let rows = conn
                    .query_sync("SELECT name FROM t WHERE id = 1", &[])
                    .expect("small-stack consumer should query through the worker");
                assert_eq!(
                    rows.first().and_then(|row| row.get(0)),
                    Some(&Value::Text("stack-safe".into()))
                );
                conn.close_sync()
                    .expect("small-stack consumer should join the worker cleanly");
            })
            .expect("small-stack consumer thread should spawn")
            .join()
            .expect("worker-backed operations must not overflow the consumer stack");

        remove_db_family(&path);
    }

    #[test]
    fn execute_raw_create_table() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
    }

    #[test]
    fn query_sync_basic() {
        let conn = FrankenConnection::open_memory().unwrap();
        let rows = conn.query_sync("SELECT 1 + 2, 'hello'", &[]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(3)));
        assert_eq!(rows[0].get(1), Some(&Value::Text("hello".into())));
    }

    #[test]
    fn execute_sync_insert() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        let count = conn
            .execute_sync(
                "INSERT INTO t (val) VALUES (?1)",
                &[Value::Text("test".into())],
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn query_with_params() {
        let conn = FrankenConnection::open_memory().unwrap();
        let rows = conn
            .query_sync("SELECT ?1 + ?2", &[Value::BigInt(10), Value::BigInt(20)])
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(30)));
    }

    #[test]
    fn returning_star_uses_schema_columns_without_reentrant_query() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();

        let rows = conn
            .query_sync("INSERT INTO t (val) VALUES ('alpha') RETURNING *", &[])
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_by_name("id"), Some(&Value::BigInt(1)));
        assert_eq!(
            rows[0].get_by_name("val"),
            Some(&Value::Text("alpha".into()))
        );
    }

    #[test]
    fn transaction_commit() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();

        conn.begin_sync(IsolationLevel::ReadCommitted).unwrap();
        conn.execute_sync(
            "INSERT INTO t (val) VALUES (?1)",
            &[Value::Text("a".into())],
        )
        .unwrap();
        conn.commit_sync().unwrap();

        let rows = conn.query_sync("SELECT val FROM t", &[]).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn transaction_rollback() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();

        conn.begin_sync(IsolationLevel::ReadCommitted).unwrap();
        conn.execute_sync(
            "INSERT INTO t (val) VALUES (?1)",
            &[Value::Text("a".into())],
        )
        .unwrap();
        conn.rollback_sync().unwrap();

        let rows = conn.query_sync("SELECT val FROM t", &[]).unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn exclusive_transaction_commits_or_rolls_back_as_one_unit() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();

        conn.with_exclusive_transaction(|transaction| {
            transaction.execute_sync(
                "INSERT INTO t VALUES (?1, ?2)",
                &[Value::BigInt(1), Value::Text("committed".into())],
            )?;
            Ok(())
        })
        .unwrap();

        let rejected: Result<(), FrankenExclusiveTransactionError<String>> = conn
            .with_exclusive_transaction_result(|transaction| {
                transaction
                    .execute_sync(
                        "INSERT INTO t VALUES (?1, ?2)",
                        &[Value::BigInt(2), Value::Text("rolled-back".into())],
                    )
                    .map_err(|error| error.to_string())?;
                Err("reject transaction".to_string())
            });
        assert!(matches!(
            rejected,
            Err(FrankenExclusiveTransactionError::Operation(error))
                if error == "reject transaction"
        ));

        let rows = conn
            .query_sync("SELECT id, val FROM t ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(1)));
        assert_eq!(rows[0].get(1), Some(&Value::Text("committed".into())));
    }

    #[test]
    fn exclusive_transaction_rejects_embedded_transaction_control() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();

        conn.with_exclusive_transaction(|transaction| {
            for sql in [
                "COMMIT;",
                "/* hidden */ ROLLBACK;",
                "SELECT 1; COMMIT;",
                "-- hidden\nSAVEPOINT inner;",
                "RELEASE inner;",
                "END TRANSACTION;",
                "BEGIN IMMEDIATE;",
            ] {
                let error = transaction
                    .execute_raw(sql)
                    .expect_err("scoped transaction control must be sealed");
                let message = error.to_string();
                assert!(
                    message.contains("transaction-control keyword")
                        || message.contains("exactly one SQL statement")
                );
            }
            enforce_scoped_transaction_sql("SELECT 'COMMIT', \"BEGIN\", `ROLLBACK`, [SAVEPOINT];")
                .expect("quoted strings and identifiers are not transaction control");
            transaction.execute_sync("INSERT INTO t VALUES (1)", &[])?;
            Ok(())
        })
        .expect("rejected control SQL must leave the wrapper transaction usable");

        let rows = conn.query_sync("SELECT id FROM t", &[]).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    #[ignore = "requires SQLMODEL_FSQLITE_RETAINED_TEST_DB and intentionally retains the database"]
    fn strict_durable_control_plane_profile_opens_and_reopens_retained_file() {
        let path = std::env::var("SQLMODEL_FSQLITE_RETAINED_TEST_DB")
            .expect("set SQLMODEL_FSQLITE_RETAINED_TEST_DB to an isolated retained path");
        let conn = FrankenConnection::open_strict_durable_control_plane_file(path.clone())
            .expect("open strict durable file");

        assert_eq!(
            conn.query_sync("PRAGMA journal_mode;", &[]).unwrap()[0].get(0),
            Some(&Value::Text("wal".into()))
        );
        assert_eq!(
            conn.query_sync("PRAGMA synchronous;", &[]).unwrap()[0].get(0),
            Some(&Value::Text("FULL".into()))
        );
        assert_eq!(
            conn.query_sync("PRAGMA fsqlite.stmt_microbatch;", &[])
                .unwrap()[0]
                .get(0),
            Some(&Value::BigInt(0))
        );

        conn.with_exclusive_transaction(|transaction| {
            transaction.execute_raw(
                "CREATE TABLE IF NOT EXISTS strict_profile_probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL);",
            )?;
            transaction.execute_sync(
                "INSERT OR REPLACE INTO strict_profile_probe VALUES (?1, ?2);",
                &[Value::BigInt(1), Value::Text("durable".into())],
            )?;
            Ok(())
        })
        .expect("commit retained probe");
        conn.close_sync().expect("close first connection");

        let reopened = FrankenConnection::open_strict_durable_control_plane_file(path)
            .expect("reopen strict durable file");
        let rows = reopened
            .query_sync(
                "SELECT value FROM strict_profile_probe WHERE id = ?1;",
                &[Value::BigInt(1)],
            )
            .expect("read retained probe");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::Text("durable".into())));
    }

    #[test]
    fn strict_durable_profile_rejects_policy_downgrade_sql() {
        for sql in [
            "PRAGMA synchronous = NORMAL;",
            "PRAGMA journal_mode=DELETE;",
            "PRAGMA fsqlite.stmt_microbatch = ON;",
            "PRAGMA synchronous(NORMAL);",
            "/* policy bypass */ PRAGMA synchronous = NORMAL;",
        ] {
            let error = enforce_profile_sql(ConnectionProfile::StrictDurableControlPlane, sql)
                .expect_err("sealed profile must reject a downgrade");
            assert!(error.to_string().contains("non-allowlisted PRAGMA"));
        }
        enforce_profile_sql(
            ConnectionProfile::StrictDurableControlPlane,
            "PRAGMA synchronous;",
        )
        .expect("read-only profile inspection remains available");
    }

    #[test]
    fn dialect_is_sqlite() {
        let conn = FrankenConnection::open_memory().unwrap();
        assert_eq!(conn.dialect(), sqlmodel_core::Dialect::Sqlite);
    }

    #[test]
    fn count_params_numbered() {
        assert_eq!(count_params("SELECT ?1, ?2, ?3"), 3);
        assert_eq!(count_params("INSERT INTO t VALUES (?1, ?2)"), 2);
    }

    #[test]
    fn count_params_bare() {
        assert_eq!(count_params("SELECT ?, ?"), 2);
    }

    #[test]
    fn count_params_none() {
        assert_eq!(count_params("SELECT 1"), 0);
    }

    #[test]
    fn infer_select_column_names() {
        let names = infer_column_names("SELECT id, name AS username, count(*) AS total FROM t");
        assert_eq!(names, vec!["id", "username", "total"]);
    }

    #[test]
    fn infer_pragma_table_info() {
        let names = infer_column_names("PRAGMA table_info(users)");
        assert!(names.contains(&"name".to_string()));
        assert!(names.contains(&"type".to_string()));
    }

    #[test]
    fn infer_expression_select() {
        let names = infer_column_names("SELECT 1 + 2 AS result");
        assert_eq!(names, vec!["result"]);
    }

    #[test]
    fn ping_succeeds() {
        let conn = FrankenConnection::open_memory().unwrap();
        let result = conn.query_sync("SELECT 1", &[]);
        assert!(result.is_ok());
    }

    #[test]
    fn multiple_statements_in_execute_raw() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw(
            "CREATE TABLE a (id INTEGER PRIMARY KEY); CREATE TABLE b (id INTEGER PRIMARY KEY)",
        )
        .unwrap();
        // Verify both tables exist by inserting into them
        conn.execute_sync("INSERT INTO a (id) VALUES (1)", &[])
            .unwrap();
        conn.execute_sync("INSERT INTO b (id) VALUES (1)", &[])
            .unwrap();
    }

    #[test]
    fn insert_returns_rowid() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        // Insert and verify via query
        conn.execute_sync(
            "INSERT INTO t (val) VALUES (?1)",
            &[Value::Text("a".into())],
        )
        .unwrap();
        let rows = conn.query_sync("SELECT id FROM t", &[]).unwrap();
        assert_eq!(rows.len(), 1);
        // Verify we got a row back (auto-increment may not produce the
        // same values as C SQLite, but row should exist)
        assert!(rows[0].get(0).is_some());
    }

    // ── BEGIN CONCURRENT tests ────────────────────────────────────────────

    #[test]
    fn begin_concurrent_basic() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute_raw("BEGIN CONCURRENT").unwrap();
        conn.execute_raw("INSERT INTO t VALUES (1, 'hello')")
            .unwrap();
        conn.execute_raw("COMMIT").unwrap();

        let rows = conn
            .query_sync("SELECT val FROM t WHERE id = 1", &[])
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::Text("hello".into())));
    }

    #[test]
    fn begin_concurrent_rollback() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute_raw("BEGIN CONCURRENT").unwrap();
        conn.execute_raw("INSERT INTO t VALUES (1, 'gone')")
            .unwrap();
        conn.execute_raw("ROLLBACK").unwrap();

        let rows = conn.query_sync("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(0)));
    }

    #[test]
    fn begin_concurrent_with_params() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute_raw("BEGIN CONCURRENT").unwrap();
        conn.execute_sync(
            "INSERT INTO t VALUES (?1, ?2)",
            &[Value::BigInt(1), Value::Text("parameterized".into())],
        )
        .unwrap();
        conn.execute_raw("COMMIT").unwrap();

        let rows = conn
            .query_sync("SELECT val FROM t WHERE id = ?1", &[Value::BigInt(1)])
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::Text("parameterized".into())));
    }

    #[test]
    fn begin_concurrent_multiple_inserts() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute_raw("BEGIN CONCURRENT").unwrap();
        for i in 1..=100 {
            conn.execute_sync(
                "INSERT INTO t VALUES (?1, ?2)",
                &[Value::BigInt(i), Value::Text(format!("row_{i}"))],
            )
            .unwrap();
        }
        conn.execute_raw("COMMIT").unwrap();

        let rows = conn.query_sync("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(100)));
    }

    // ── Isolation level tests ─────────────────────────────────────────────

    #[test]
    fn begin_serializable_uses_exclusive() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.begin_sync(IsolationLevel::Serializable).unwrap();
        conn.execute_sync("INSERT INTO t VALUES (1)", &[]).unwrap();
        conn.commit_sync().unwrap();
        let rows = conn.query_sync("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(1)));
    }

    #[test]
    fn begin_read_uncommitted_uses_deferred() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.begin_sync(IsolationLevel::ReadUncommitted).unwrap();
        conn.execute_sync("INSERT INTO t VALUES (1)", &[]).unwrap();
        conn.commit_sync().unwrap();
        let rows = conn.query_sync("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(1)));
    }

    #[test]
    fn double_begin_returns_error() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.begin_sync(IsolationLevel::ReadCommitted).unwrap();
        let err = conn.begin_sync(IsolationLevel::ReadCommitted).unwrap_err();
        assert!(err.to_string().contains("Already in a transaction"));
    }

    #[test]
    fn commit_without_begin_returns_error() {
        let conn = FrankenConnection::open_memory().unwrap();
        let err = conn.commit_sync().unwrap_err();
        assert!(err.to_string().contains("Not in a transaction"));
    }

    #[test]
    fn rollback_without_begin_returns_error() {
        let conn = FrankenConnection::open_memory().unwrap();
        let err = conn.rollback_sync().unwrap_err();
        assert!(err.to_string().contains("Not in a transaction"));
    }

    // ── Savepoint tests ──────────────────────────────────────────────────

    #[test]
    fn savepoint_and_release() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute_raw("BEGIN CONCURRENT").unwrap();
        conn.execute_raw("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute_raw("SAVEPOINT sp1").unwrap();
        conn.execute_raw("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.execute_raw("RELEASE sp1").unwrap();
        conn.execute_raw("COMMIT").unwrap();

        let rows = conn.query_sync("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(2)));
    }

    #[test]
    fn savepoint_rollback_to() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute_raw("BEGIN CONCURRENT").unwrap();
        conn.execute_raw("INSERT INTO t VALUES (1, 'keep')")
            .unwrap();
        conn.execute_raw("SAVEPOINT sp1").unwrap();
        conn.execute_raw("INSERT INTO t VALUES (2, 'discard')")
            .unwrap();
        conn.execute_raw("ROLLBACK TO sp1").unwrap();
        conn.execute_raw("COMMIT").unwrap();

        let rows = conn.query_sync("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(1)));
        let rows = conn
            .query_sync("SELECT val FROM t WHERE id = 1", &[])
            .unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::Text("keep".into())));
    }

    // ── File-based connection test ────────────────────────────────────────

    #[test]
    fn file_based_connection() {
        let dir = std::env::temp_dir().join("sqlmodel_franken_test");
        let _ = std::fs::create_dir_all(&dir);
        let db_path = dir.join("test_file.db");
        let path_str = db_path.display().to_string();

        // Clean up the main database and every WAL sidecar from previous runs.
        remove_db_family(&path_str);

        {
            let conn = FrankenConnection::open_file(&path_str).unwrap();
            conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .unwrap();
            conn.execute_raw("BEGIN CONCURRENT").unwrap();
            conn.execute_sync("INSERT INTO t VALUES (1, 'persistent')", &[])
                .unwrap();
            conn.execute_raw("COMMIT").unwrap();
        }

        // Reopen and verify data persisted
        {
            let conn = FrankenConnection::open_file(&path_str).unwrap();
            let rows = conn
                .query_sync("SELECT val FROM t WHERE id = 1", &[])
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0), Some(&Value::Text("persistent".into())));
        }

        remove_db_family(&path_str);
    }

    #[test]
    fn close_without_checkpoint_keeps_committed_rows_durable() {
        let dir = std::env::temp_dir().join("sqlmodel_franken_no_checkpoint_close_test");
        let _ = std::fs::create_dir_all(&dir);
        let db_path = dir.join("test_file.db");
        let path_str = db_path.display().to_string();

        // Clean up from previous runs (main db plus WAL sidecars).
        remove_db_family(&path_str);

        {
            let conn = FrankenConnection::open_file(&path_str).unwrap();
            conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .unwrap();
            conn.execute_sync("INSERT INTO t VALUES (1, 'durable')", &[])
                .unwrap();
            conn.close_without_checkpoint_sync()
                .expect("close without final WAL checkpoint");
        }

        // The skipped checkpoint must not cost durability: the committed row
        // is recovered from the WAL sidecar on the next open.
        {
            let conn = FrankenConnection::open_file(&path_str).unwrap();
            let rows = conn
                .query_sync("SELECT val FROM t WHERE id = 1", &[])
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0), Some(&Value::Text("durable".into())));
            conn.close_sync().expect("close reopened connection");
        }

        remove_db_family(&path_str);
    }

    #[test]
    fn schema_only_file_connection_reads_existing_rows() {
        let dir = std::env::temp_dir().join("sqlmodel_franken_schema_only_test");
        let _ = std::fs::create_dir_all(&dir);
        let db_path = dir.join("test_file.db");
        let path_str = db_path.display().to_string();

        remove_db_family(&path_str);

        {
            let conn = FrankenConnection::open_file(&path_str).unwrap();
            conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .unwrap();
            conn.execute_raw("BEGIN CONCURRENT").unwrap();
            conn.execute_sync("INSERT INTO t VALUES (1, 'readable')", &[])
                .unwrap();
            conn.execute_raw("COMMIT").unwrap();
        }

        {
            let conn = FrankenConnection::open_schema_only(&path_str).unwrap();
            conn.execute_raw("PRAGMA busy_timeout = 250").unwrap();
            let rows = conn
                .query_sync("SELECT val FROM t WHERE id = 1", &[])
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0), Some(&Value::Text("readable".into())));
        }

        remove_db_family(&path_str);
    }

    // ── Error mapping tests ──────────────────────────────────────────────

    #[test]
    fn invalid_sql_returns_query_error() {
        let conn = FrankenConnection::open_memory().unwrap();
        let err = conn.execute_raw("SELECTT 1").unwrap_err();
        // frankensqlite returns a Database-level error for unrecognized statements
        match &err {
            Error::Query(qe) => {
                assert!(
                    qe.kind == QueryErrorKind::Syntax || qe.kind == QueryErrorKind::Database,
                    "expected Syntax or Database, got: {:?}",
                    qe.kind
                );
            }
            other => panic!("expected Query error, got: {other}"),
        }
    }

    #[test]
    fn error_type_mapping_write_conflict() {
        // Verify that WriteConflict maps to Deadlock kind
        use fsqlite_error::FrankenError;
        let err = FrankenError::WriteConflict {
            page: 42,
            holder: 99,
        };
        let mapped = franken_to_query_error(&err, "COMMIT");
        match mapped {
            Error::Query(qe) => assert_eq!(qe.kind, QueryErrorKind::Deadlock),
            other => panic!("expected Deadlock error, got: {other}"),
        }
    }

    #[test]
    fn error_type_mapping_serialization_failure() {
        use fsqlite_error::FrankenError;
        let err = FrankenError::SerializationFailure { page: 7 };
        let mapped = franken_to_query_error(&err, "COMMIT");
        match mapped {
            Error::Query(qe) => assert_eq!(qe.kind, QueryErrorKind::Deadlock),
            other => panic!("expected Deadlock error, got: {other}"),
        }
    }

    // ── Column inference edge cases ──────────────────────────────────────

    #[test]
    fn infer_columns_star_select() {
        let names = infer_column_names("SELECT * FROM t");
        assert_eq!(names, vec!["*"]);
    }

    #[test]
    fn infer_columns_table_qualified() {
        let names = infer_column_names("SELECT t.id, t.name FROM t");
        assert_eq!(names, vec!["id", "name"]);
    }

    #[test]
    fn infer_columns_table_qualified_with_alias() {
        // This is the pattern used in mcp-agent-mail-db queries
        let names = infer_column_names(
            "SELECT m.id, m.subject, a.name as from_name, m.body_md FROM messages m JOIN agents a ON a.id = m.sender_id",
        );
        assert_eq!(names, vec!["id", "subject", "from_name", "body_md"]);
    }

    #[test]
    fn infer_columns_lowercase_as() {
        let names = infer_column_names("SELECT a.name as alias_name FROM t");
        assert_eq!(names, vec!["alias_name"]);
    }

    #[test]
    fn infer_columns_with_cte() {
        let names = infer_column_names("WITH cte AS (SELECT 1 AS x) SELECT x, x + 1 AS y FROM cte");
        assert_eq!(names, vec!["x", "y"]);
    }

    #[test]
    fn infer_columns_subquery_alias() {
        let names = infer_column_names("SELECT (SELECT 1) AS sub, 2 AS plain");
        assert_eq!(names, vec!["sub", "plain"]);
    }

    #[test]
    fn infer_columns_no_from() {
        let names = infer_column_names("SELECT 1 AS a, 2 AS b, 3 AS c");
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn infer_pragma_database_list() {
        let names = infer_column_names("PRAGMA database_list");
        assert_eq!(names, vec!["seq", "name", "file"]);
    }

    #[test]
    fn infer_pragma_integrity_check() {
        let names = infer_column_names("PRAGMA integrity_check");
        assert_eq!(names, vec!["integrity_check"]);
    }

    #[test]
    fn infer_pragma_quick_check() {
        let names = infer_column_names("PRAGMA quick_check");
        assert_eq!(names, vec!["quick_check"]);
    }

    #[test]
    fn infer_pragma_simple_value() {
        let names = infer_column_names("PRAGMA journal_mode");
        assert_eq!(names, vec!["journal_mode"]);
    }

    // ── changes() test ───────────────────────────────────────────────────

    #[test]
    fn changes_returns_value() {
        // frankensqlite's changes() may return 0 for non-INSERT statements;
        // verify it at least doesn't panic and returns a non-negative value
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute_sync("INSERT INTO t VALUES (1, 'a')", &[])
            .unwrap();
        let c = conn.changes();
        assert!(c >= 0, "changes() should be non-negative, got {c}");
    }

    // ── last_insert_rowid tracking ───────────────────────────────────────

    #[test]
    fn last_insert_rowid_accessible() {
        // frankensqlite may not update last_insert_rowid() the same way as C SQLite;
        // verify the method is callable and returns a consistent value
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute_sync("INSERT INTO t (val) VALUES ('a')", &[])
            .unwrap();
        let rowid = conn.last_insert_rowid();
        // At minimum, should not panic; value may be 0 if frankensqlite
        // doesn't support last_insert_rowid() via SELECT
        assert!(rowid >= 0, "last_insert_rowid should be >= 0, got {rowid}");
    }

    // ── Transaction + Connection trait async bridge ──────────────────────

    #[test]
    fn connection_trait_query_async_bridge() {
        use sqlmodel_core::Cx;
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute_sync("INSERT INTO t VALUES (1, 'async')", &[])
            .unwrap();

        let cx = Cx::for_testing();
        // Test that the async Connection::query method works correctly
        let result = asupersync::runtime::RuntimeBuilder::current_thread()
            .build()
            .unwrap()
            .block_on(async { Connection::query(&conn, &cx, "SELECT val FROM t", &[]).await });
        match result {
            Outcome::Ok(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get(0), Some(&Value::Text("async".into())));
            }
            other => panic!("expected Ok, got: {other:?}"),
        }
    }

    #[test]
    fn connection_trait_begin_and_commit() {
        use sqlmodel_core::Cx;
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();

        let rt = asupersync::runtime::RuntimeBuilder::current_thread()
            .build()
            .unwrap();
        let cx = Cx::for_testing();

        rt.block_on(async {
            let tx = conn.begin(&cx).await.into_result().unwrap();
            TransactionOps::execute(&tx, &cx, "INSERT INTO t VALUES (1)", &[])
                .await
                .into_result()
                .unwrap();
            tx.commit(&cx).await.into_result().unwrap();
        });

        let rows = conn.query_sync("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(1)));
    }

    #[test]
    fn transaction_drop_auto_rollback() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();

        let rt = asupersync::runtime::RuntimeBuilder::current_thread()
            .build()
            .unwrap();
        let cx = Cx::for_testing();

        rt.block_on(async {
            let tx = conn.begin(&cx).await.into_result().unwrap();
            TransactionOps::execute(&tx, &cx, "INSERT INTO t VALUES (1)", &[])
                .await
                .into_result()
                .unwrap();
            // Drop tx without commit — should auto-rollback
            drop(tx);
        });

        let rows = conn.query_sync("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(0)));
    }

    // ── Runtime-nesting probes (fsqlite 0.2 worker bridge safety) ───────
    //
    // The adapter blocks on worker-channel responses while a consumer may
    // itself be inside `block_on` or on an asupersync worker thread. These
    // probes prove that bridge neither deadlocks nor panics in either case.

    #[test]
    fn nested_block_on_inside_outer_block_on() {
        use sqlmodel_core::Cx;
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();

        let outer = asupersync::runtime::RuntimeBuilder::current_thread()
            .build()
            .unwrap();
        let cx = Cx::for_testing();

        // Every adapter call below waits on the dedicated worker while the
        // outer runtime's block_on is active on this thread.
        outer.block_on(async {
            conn.execute_sync("INSERT INTO t VALUES (1, 'nested')", &[])
                .unwrap();
            let rows = conn.query_sync("SELECT val FROM t", &[]).unwrap();
            assert_eq!(rows[0].get(0), Some(&Value::Text("nested".into())));

            let tx = conn.begin(&cx).await.into_result().unwrap();
            TransactionOps::execute(&tx, &cx, "INSERT INTO t VALUES (2, 'tx')", &[])
                .await
                .into_result()
                .unwrap();
            tx.commit(&cx).await.into_result().unwrap();
        });

        let rows = conn.query_sync("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(2)));
    }

    #[test]
    fn nested_block_on_on_worker_thread() {
        use sqlmodel_core::Cx;
        let conn = Arc::new(FrankenConnection::open_memory().unwrap());
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();

        let outer = asupersync::runtime::RuntimeBuilder::multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();

        // The spawned task's Send future runs on an asupersync worker thread;
        // the adapter's sync helpers then wait on the dedicated connection
        // worker from inside that runtime worker.
        let task_conn = Arc::clone(&conn);
        let handle = outer.handle().spawn(async move {
            let cx = Cx::for_testing();
            task_conn
                .execute_sync("INSERT INTO t VALUES (1, 'worker')", &[])
                .unwrap();
            let rows = Connection::query(&*task_conn, &cx, "SELECT val FROM t", &[])
                .await
                .into_result()
                .unwrap();
            assert_eq!(rows[0].get(0), Some(&Value::Text("worker".into())));
            rows.len()
        });

        let row_count = outer.block_on(handle);
        assert_eq!(row_count, 1);

        let rows = conn.query_sync("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(1)));
    }

    // ── Batch execution ──────────────────────────────────────────────────

    #[test]
    fn batch_multiple_statements() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();

        let rt = asupersync::runtime::RuntimeBuilder::current_thread()
            .build()
            .unwrap();
        let cx = Cx::for_testing();

        let results = rt.block_on(async {
            Connection::batch(
                &conn,
                &cx,
                &[
                    ("INSERT INTO t VALUES (1, 'a')".to_string(), vec![]),
                    ("INSERT INTO t VALUES (2, 'b')".to_string(), vec![]),
                    ("INSERT INTO t VALUES (3, 'c')".to_string(), vec![]),
                ],
            )
            .await
            .into_result()
            .unwrap()
        });

        assert_eq!(results.len(), 3);
        let rows = conn.query_sync("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(3)));
    }

    // ── NULL handling ────────────────────────────────────────────────────

    #[test]
    fn null_values_round_trip() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute_sync(
            "INSERT INTO t VALUES (?1, ?2)",
            &[Value::BigInt(1), Value::Null],
        )
        .unwrap();
        let rows = conn
            .query_sync("SELECT val FROM t WHERE id = 1", &[])
            .unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::Null));
    }

    // ── Blob handling ────────────────────────────────────────────────────

    #[test]
    fn blob_values_round_trip() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)")
            .unwrap();
        let blob = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF];
        conn.execute_sync(
            "INSERT INTO t VALUES (1, ?1)",
            &[Value::Bytes(blob.clone())],
        )
        .unwrap();
        let rows = conn
            .query_sync("SELECT data FROM t WHERE id = 1", &[])
            .unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::Bytes(blob)));
    }

    // br-22iss: Test UPDATE with numbered placeholders matching E2E failure scenario
    #[test]
    fn update_with_numbered_placeholders_in_where() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw(
            "CREATE TABLE agents (
                id INTEGER PRIMARY KEY,
                project_id INTEGER,
                name TEXT,
                contact_policy TEXT
            )",
        )
        .unwrap();

        // Insert two agents
        conn.execute_sync(
            "INSERT INTO agents (project_id, name, contact_policy) VALUES (?1, ?2, ?3)",
            &[
                Value::BigInt(1),
                Value::Text("BlueLake".into()),
                Value::Text("auto".into()),
            ],
        )
        .unwrap();
        conn.execute_sync(
            "INSERT INTO agents (project_id, name, contact_policy) VALUES (?1, ?2, ?3)",
            &[
                Value::BigInt(1),
                Value::Text("RedFox".into()),
                Value::Text("auto".into()),
            ],
        )
        .unwrap();

        // Verify both agents exist
        let rows = conn
            .query_sync(
                "SELECT * FROM agents WHERE project_id = ?1",
                &[Value::BigInt(1)],
            )
            .unwrap();
        assert_eq!(rows.len(), 2, "should have 2 agents");

        // Update RedFox's contact_policy - this is the failing pattern from E2E
        let affected = conn
            .execute_sync(
                "UPDATE agents SET contact_policy = ?1 WHERE project_id = ?2 AND name = ?3",
                &[
                    Value::Text("open".into()),
                    Value::BigInt(1),
                    Value::Text("RedFox".into()),
                ],
            )
            .unwrap();
        assert_eq!(affected, 1, "should affect 1 row");

        // Verify the update worked
        let rows = conn
            .query_sync(
                "SELECT contact_policy FROM agents WHERE name = ?1",
                &[Value::Text("RedFox".into())],
            )
            .unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::Text("open".into())));
    }

    // br-22iss: Test UPDATE with 4 numbered placeholders matching exact E2E scenario
    #[test]
    fn update_with_four_numbered_placeholders_in_where() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw(
            "CREATE TABLE agents (
                id INTEGER PRIMARY KEY,
                project_id INTEGER,
                name TEXT,
                contact_policy TEXT,
                last_active_ts INTEGER
            )",
        )
        .unwrap();

        // Insert two agents
        conn.execute_sync(
            "INSERT INTO agents (project_id, name, contact_policy, last_active_ts) VALUES (?1, ?2, ?3, ?4)",
            &[Value::BigInt(1), Value::Text("BlueLake".into()), Value::Text("auto".into()), Value::BigInt(1000)],
        )
        .unwrap();
        conn.execute_sync(
            "INSERT INTO agents (project_id, name, contact_policy, last_active_ts) VALUES (?1, ?2, ?3, ?4)",
            &[Value::BigInt(1), Value::Text("RedFox".into()), Value::Text("auto".into()), Value::BigInt(1000)],
        )
        .unwrap();

        // Verify both agents exist
        let rows = conn
            .query_sync(
                "SELECT * FROM agents WHERE project_id = ?1",
                &[Value::BigInt(1)],
            )
            .unwrap();
        assert_eq!(rows.len(), 2, "should have 2 agents");

        // Exact E2E scenario: UPDATE agents SET contact_policy = ?1, last_active_ts = ?2 WHERE project_id = ?3 AND name = ?4
        let affected = conn
            .execute_sync(
                "UPDATE agents SET contact_policy = ?1, last_active_ts = ?2 WHERE project_id = ?3 AND name = ?4",
                &[Value::Text("open".into()), Value::BigInt(2000), Value::BigInt(1), Value::Text("RedFox".into())],
            )
            .unwrap();
        assert_eq!(affected, 1, "should affect 1 row");

        // Verify the update worked
        let rows = conn
            .query_sync(
                "SELECT contact_policy, last_active_ts FROM agents WHERE name = ?1",
                &[Value::Text("RedFox".into())],
            )
            .unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::Text("open".into())));
        assert_eq!(rows[0].get(1), Some(&Value::BigInt(2000)));
    }
}
