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
        let schema_columns = star_columns_from_schema(sql, &inner);

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

        let committed = inner
            .conn
            .execute_sync("COMMIT")
            .map_err(|e| franken_to_query_error(&e, "COMMIT"));
        if let Err(e) = committed {
            // A failed COMMIT (snapshot conflict, busy) leaves the transaction
            // open on its old snapshot. Until 2026-09 the driver kept its
            // `in_transaction` flag too, so the connection went on reading
            // stale data and a retry re-created tables that already existed
            // (found by the e2e migration-runner race). Roll back so the
            // connection is back in autocommit with a fresh snapshot.
            let _ = inner.conn.execute_sync("ROLLBACK");
            inner.in_transaction = false;
            return Err(e);
        }

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
        let schema_columns = star_columns_from_schema(sql, inner);
        let rows = if sqlite_params.is_empty() {
            inner.conn.query_sync(sql)
        } else {
            inner.conn.query_with_params_sync(sql, &sqlite_params)
        }
        .map_err(|error| franken_to_query_error(&error, sql))?;
        Ok(convert_rows_with_schema(
            &rows,
            sql,
            schema_columns.as_deref(),
        ))
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

    // FrankenSQLite is driven synchronously (block_on bridge), so every
    // operation runs to completion before its future is returned. Cancellation
    // is honoured at one point: an already-cancelled `Cx` never reaches the
    // engine and the operation returns `Outcome::Cancelled` instead.

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
                .map(|()| FrankenTransaction::new(self))
                .map_or_else(Outcome::Err, Outcome::Ok),
        };
        async move { outcome }
    }

    /// FrankenSQLite supports every mode: the three SQLite locking forms and
    /// `BEGIN CONCURRENT` (page-level MVCC).
    fn supports_transaction_mode(&self, _mode: TransactionMode) -> bool {
        true
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
                .map(|()| FrankenTransaction::new(self))
                .map_or_else(Outcome::Err, Outcome::Ok),
        };
        async move { outcome }
    }

    fn prepare(
        &self,
        cx: &Cx,
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

        let outcome = match sqlmodel_core::cancel_requested(cx) {
            Some(reason) => Outcome::Cancelled(reason),
            None => Outcome::Ok(stmt),
        };
        async move { outcome }
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

    fn ping(&self, cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
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
        let quoted = format!("\"{}\"", name.replace('"', "\"\""));
        let sql = format!("SAVEPOINT {quoted}");
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
        let quoted = format!("\"{}\"", name.replace('"', "\"\""));
        let sql = format!("ROLLBACK TO {quoted}");
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
        let quoted = format!("\"{}\"", name.replace('"', "\"\""));
        let sql = format!("RELEASE {quoted}");
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
/// Result column names for statements whose projection is a bare `*`, taken from
/// the table schema (`PRAGMA table_info`) instead of guessed from the SQL text.
///
/// fsqlite's async facade returns rows without column labels, and the text-based
/// inference in [`infer_column_names`] cannot expand `*`. Without this, every
/// `select!(Model)` (which emits `SELECT * FROM table ...`) hydrated rows with
/// placeholder names and `from_row` failed with "column not found" — the first
/// ORM-level run through this driver found exactly that.
///
/// Handles `INSERT/UPDATE/DELETE ... RETURNING *` and `SELECT [DISTINCT|ALL] *`
/// or `SELECT t.*` over a single table (no JOIN); anything else returns `None`
/// and falls back to text inference.
fn star_columns_from_schema(sql: &str, inner: &FrankenInner) -> Option<Vec<String>> {
    let (table_name, extra_columns) = extract_star_projection(sql)?;
    let pragma_sql = format!("PRAGMA table_info({})", quote_pragma_table(&table_name));
    let pragma_rows = inner.conn.query_sync(&pragma_sql).ok()?;
    // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    let mut columns: Vec<String> = pragma_rows
        .iter()
        .filter_map(|row| {
            row.values().get(1).and_then(|v| match v {
                SqliteValue::Text(s) => Some(s.to_string()),
                _ => None,
            })
        })
        .collect();
    if columns.is_empty() {
        return None;
    }
    // `SELECT *, fk AS __parent_pk FROM t` (the session's one-to-many loader):
    // the extra items follow the table's columns in the result.
    columns.extend(extra_columns);
    Some(columns)
}

/// Always quoted: a bare reserved word such as `order` or `select` is a syntax
/// error inside `PRAGMA table_info(...)`, and that lookup is what gives
/// `SELECT *` its column names.
fn quote_pragma_table(table: &str) -> String {
    format!("\"{}\"", table.replace('"', "\"\""))
}

/// The single source table of a statement whose result projection is `*`
/// (test helper over [`extract_star_projection`]).
#[cfg(test)]
fn extract_table_name_for_star_projection(sql: &str) -> Option<String> {
    extract_star_projection(sql).map(|(table, _)| table)
}

/// The single source table of a statement whose projection starts with `*`
/// (or `<table>.*`), plus the result names of any select items that follow the
/// star (`SELECT *, fk AS __parent_pk FROM t`).
fn extract_star_projection(sql: &str) -> Option<(String, Vec<String>)> {
    let upper = sql.to_uppercase();
    if upper.contains(" RETURNING *") || upper.ends_with("RETURNING *") {
        return extract_table_name_for_returning(sql).map(|table| (table, Vec::new()));
    }
    extract_select_star_projection(sql)
}

/// `SELECT [DISTINCT|ALL] *[, item...] FROM <table> ...` or
/// `SELECT <table>.*[, item...] FROM <table> ...` over exactly one table (no
/// JOIN, no comma-separated FROM list). Extra items are named by their alias
/// (`expr AS name`) or by the last identifier of the expression.
fn extract_select_star_projection(sql: &str) -> Option<(String, Vec<String>)> {
    let trimmed = sql.trim().trim_start_matches('(');
    let upper = trimmed.to_uppercase();
    let rest = upper.strip_prefix("SELECT")?;
    let rest = rest.trim_start();
    let rest = if let Some(r) = rest.strip_prefix("DISTINCT ") {
        r
    } else if let Some(r) = rest.strip_prefix("ALL ") {
        r
    } else {
        rest
    };
    let projection_start = trimmed.len() - rest.len();
    let from_pos = rest.find(" FROM ")?;
    let projection = trimmed[projection_start..projection_start + from_pos].trim();
    let items = split_top_level_commas(projection);
    let (star, extras) = items.split_first()?;
    let star = star.trim();
    let star_qualifier = if star == "*" {
        None
    } else {
        match star.rsplit_once('.') {
            Some((qualifier, "*")) => Some(qualifier.trim_matches('"').to_string()),
            _ => return None,
        }
    };
    let after_from = &trimmed[projection_start + from_pos + " FROM ".len()..];
    let table = extract_identifier(after_from);
    if table.is_empty() {
        return None;
    }
    // Anything that widens the row shape disqualifies the schema lookup for a
    // bare `*`. A qualified `"t".*` is exactly t's columns whatever is joined,
    // which is what `select!(Model)` emits as soon as a JOIN is added.
    let tail_upper = after_from.to_uppercase();
    let widened = tail_upper.contains(" JOIN ")
        || tail_upper[table.len().min(tail_upper.len())..]
            .trim_start()
            .starts_with(',');
    if widened && star_qualifier.is_none() {
        return None;
    }
    if let Some(qualifier) = star_qualifier
        && !qualifier.eq_ignore_ascii_case(&table)
    {
        return None;
    }
    let extra_names = extras
        .iter()
        .map(|item| select_item_result_name(item.trim()))
        .collect::<Option<Vec<String>>>()?;
    Some((table, extra_names))
}

/// Split a select list on commas that are not inside parentheses or quotes.
fn split_top_level_commas(projection: &str) -> Vec<&str> {
    let mut items = Vec::new();
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut start = 0;
    for (i, c) in projection.char_indices() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), _) => {}
            (None, '\'' | '"' | '`') => quote = Some(c),
            (None, '(') => depth += 1,
            (None, ')') => depth = depth.saturating_sub(1),
            (None, ',') if depth == 0 => {
                items.push(&projection[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    items.push(&projection[start..]);
    items
}

/// The result column name of one select item: its `AS` alias, or the last
/// identifier of a plain column reference. Expressions without an alias are
/// not named (the caller falls back to text inference).
fn select_item_result_name(item: &str) -> Option<String> {
    let upper = item.to_uppercase();
    if let Some(pos) = upper.rfind(" AS ") {
        let alias = item[pos + " AS ".len()..].trim();
        return Some(alias.trim_matches(|c| c == '"' || c == '`').to_string());
    }
    let last = item.rsplit('.').next()?.trim();
    let bare = last.trim_matches(|c| c == '"' || c == '`');
    if !bare.is_empty() && bare.chars().all(|c| c.is_alphanumeric() || c == '_') {
        Some(bare.to_string())
    } else {
        None
    }
}

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
        // A `BEGIN CONCURRENT` transaction whose write set overlaps a commit that
        // landed after its snapshot (SQLITE_BUSY_SNAPSHOT), or whose snapshot was
        // garbage-collected: the transaction must be retried from the start.
        FrankenError::BusySnapshot { .. } | FrankenError::SnapshotTooOld { .. } => {
            QueryErrorKind::Serialization
        }
        // Classic SQLITE_BUSY: another connection holds the lock right now; also
        // transient. Classified as a lock-wait timeout so `Error::is_retryable()`
        // is true.
        FrankenError::Busy | FrankenError::BusyRecovery => QueryErrorKind::Timeout,
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

    /// `SELECT *` gets its names from `PRAGMA table_info`, which must quote a
    /// reserved-word table (`order`) or the lookup fails and every column is
    /// unnamed (found by the e2e reserved-word scenario, 2026-09).
    #[test]
    fn select_star_names_columns_of_a_reserved_word_table() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw(
            "CREATE TABLE \"order\" (id INTEGER PRIMARY KEY, \"user\" TEXT, \"select\" INTEGER)",
        )
        .unwrap();
        conn.execute_sync(
            "INSERT INTO \"order\" (id, \"user\", \"select\") VALUES (1, 'ann', 10)",
            &[],
        )
        .unwrap();
        let rows = conn
            .query_sync(
                "SELECT * FROM \"order\" WHERE \"select\" > ?1",
                &[Value::BigInt(5)],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_named::<String>("user").unwrap(), "ann");
        assert_eq!(rows[0].get_named::<i64>("select").unwrap(), 10);
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
    fn select_star_table_extraction_handles_orm_shapes() {
        for (sql, expected) in [
            ("SELECT * FROM gadgets", Some("gadgets")),
            (
                "SELECT * FROM gadgets WHERE weight > ?1 ORDER BY id LIMIT 5",
                Some("gadgets"),
            ),
            (
                "SELECT DISTINCT * FROM \"e2e_smoke_gadgets\" WHERE x IS NULL",
                Some("e2e_smoke_gadgets"),
            ),
            ("select * from t", Some("t")),
            ("SELECT t.* FROM t WHERE 1", Some("t")),
            ("SELECT \"t\".* FROM \"t\"", Some("t")),
            ("SELECT id, name FROM t", None),
            ("SELECT * FROM a JOIN b ON a.id = b.a_id", None),
            // A qualified star is that table's columns whatever is joined
            // (what `select!(Model).join(..)` emits).
            (
                "SELECT \"a\".* FROM \"a\" INNER JOIN \"b\" ON \"a\".\"b_id\" = \"b\".\"id\"",
                Some("a"),
            ),
            ("SELECT * FROM a, b", None),
            ("SELECT count(*) FROM t", None),
            ("SELECT u.* FROM t", None),
            ("PRAGMA table_info(t)", None),
            (
                "SELECT *, author_id AS __parent_pk FROM books WHERE author_id IN (?1)",
                Some("books"),
            ),
        ] {
            assert_eq!(
                extract_table_name_for_star_projection(sql).as_deref(),
                expected,
                "{sql}"
            );
        }

        // Extra select items after the star are named after the table's columns
        // (the session's one-to-many loader depends on `__parent_pk`).
        let (table, extras) = extract_star_projection(
            "SELECT *, \"author_id\" AS __parent_pk FROM \"books\" WHERE \"author_id\" IN (?1)",
        )
        .expect("star projection with an aliased extra item");
        assert_eq!(table, "books");
        assert_eq!(extras, vec!["__parent_pk".to_string()]);
        let (_, extras) =
            extract_star_projection("SELECT t.*, t.x, upper(name) AS up, y FROM t").unwrap();
        assert_eq!(extras, vec!["x".to_string(), "up".into(), "y".into()]);
        // An unaliased expression has no derivable name: fall back to inference.
        assert!(extract_star_projection("SELECT *, count(*) FROM t").is_none());
    }

    #[test]
    fn select_star_rows_carry_real_column_names() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw(
            "CREATE TABLE gadgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL, weight INTEGER)",
        )
        .unwrap();
        conn.execute_raw("INSERT INTO gadgets VALUES (1, 'gear', 120), (2, 'spring', NULL)")
            .unwrap();

        let rows = conn
            .query_sync(
                "SELECT * FROM gadgets WHERE weight > ?1",
                &[Value::BigInt(100)],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_named::<i64>("id").unwrap(), 1);
        assert_eq!(rows[0].get_named::<String>("name").unwrap(), "gear");
        assert_eq!(rows[0].get_named::<i64>("weight").unwrap(), 120);

        let rows = conn
            .query_sync("SELECT DISTINCT * FROM gadgets WHERE weight IS NULL", &[])
            .unwrap();
        assert_eq!(rows[0].get_named::<String>("name").unwrap(), "spring");
        assert!(rows[0].get_named::<i64>("weight").is_err(), "NULL weight");
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

    // ── TransactionMode through the Connection trait ─────────────────────
    //
    // These are the first tests that reach `BEGIN CONCURRENT` through the
    // `sqlmodel_core::Connection` API rather than raw SQL: this is the path
    // `Session`, `Pool` users, and the query builders take.

    fn concurrent_test_db(name: &str) -> String {
        let dir = std::env::temp_dir().join("sqlmodel_franken_concurrent_mode_tests");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join(format!("{name}_{}.db", std::process::id()));
        let path_str = path.to_string_lossy().into_owned();
        remove_db_family(&path_str);
        path_str
    }

    #[test]
    fn supports_every_transaction_mode() {
        let conn = FrankenConnection::open_memory().unwrap();
        for mode in [
            TransactionMode::Default,
            TransactionMode::Concurrent,
            TransactionMode::Immediate,
            TransactionMode::Exclusive,
            TransactionMode::Deferred,
        ] {
            assert!(conn.supports_transaction_mode(mode), "{mode:?}");
        }
    }

    #[test]
    fn begin_concurrent_sync_helper_round_trips() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.begin_concurrent_sync().unwrap();
        assert!(
            conn.begin_concurrent_sync().is_err(),
            "nested begin is refused like the other begin helpers"
        );
        conn.execute_sync(
            "INSERT INTO t VALUES (?1, ?2)",
            &[Value::BigInt(7), Value::Text("via helper".into())],
        )
        .unwrap();
        conn.commit_sync().unwrap();
        let rows = conn
            .query_sync("SELECT val FROM t WHERE id = 7", &[])
            .unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::Text("via helper".into())));
    }

    #[test]
    fn two_concurrent_writers_on_disjoint_pages_both_commit() {
        use sqlmodel_core::Cx;
        // MVCC conflict detection is page-level: two rows of one tiny table share
        // a page and would conflict, so give each writer its own table (its own
        // B-tree root page). Same-page conflicts are covered by the next test.
        let path = concurrent_test_db("disjoint");
        let a = FrankenConnection::open_file(&path).unwrap();
        a.execute_raw("CREATE TABLE ta (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        a.execute_raw("CREATE TABLE tb (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        let b = FrankenConnection::open_file(&path).unwrap();

        let rt = asupersync::runtime::RuntimeBuilder::current_thread()
            .build()
            .unwrap();
        let cx = Cx::for_testing();
        rt.block_on(async {
            let tx_a = Connection::begin_with_options(&a, &cx, TransactionOptions::concurrent())
                .await
                .into_result()
                .expect("begin concurrent on a");
            let tx_b = Connection::begin_with_options(&b, &cx, TransactionOptions::concurrent())
                .await
                .into_result()
                .expect("begin concurrent on b while a is open");

            TransactionOps::execute(&tx_a, &cx, "INSERT INTO ta VALUES (1, 'from a')", &[])
                .await
                .into_result()
                .unwrap();
            TransactionOps::execute(&tx_b, &cx, "INSERT INTO tb VALUES (2, 'from b')", &[])
                .await
                .into_result()
                .unwrap();

            tx_a.commit(&cx).await.into_result().expect("commit a");
            tx_b.commit(&cx)
                .await
                .into_result()
                .expect("commit b: disjoint pages, no conflict");
        });

        let rows_a = a.query_sync("SELECT val FROM ta", &[]).unwrap();
        let rows_b = a.query_sync("SELECT val FROM tb", &[]).unwrap();
        assert_eq!(rows_a[0].get(0), Some(&Value::Text("from a".into())));
        assert_eq!(rows_b[0].get(0), Some(&Value::Text("from b".into())));

        drop(a);
        drop(b);
        remove_db_family(&path);
    }

    /// After a COMMIT that fails on a snapshot conflict the connection must be
    /// back in autocommit on a fresh snapshot: it sees what the other
    /// connection committed and can begin again. Found by the e2e
    /// migration-runner race (2026-09): the loser kept reading its old
    /// snapshot and re-created tables that already existed.
    #[test]
    fn failed_commit_returns_the_connection_to_a_fresh_snapshot() {
        use sqlmodel_core::Cx;
        let path = concurrent_test_db("failed_commit");
        let a = FrankenConnection::open_file(&path).unwrap();
        a.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        a.execute_raw("INSERT INTO t VALUES (1, 0)").unwrap();
        let b = FrankenConnection::open_file(&path).unwrap();
        let rt = asupersync::runtime::RuntimeBuilder::current_thread()
            .build()
            .unwrap();
        let cx = Cx::for_testing();
        rt.block_on(async {
            let tx_a = Connection::begin_with_options(&a, &cx, TransactionOptions::concurrent())
                .await
                .into_result()
                .unwrap();
            let tx_b = Connection::begin_with_options(&b, &cx, TransactionOptions::concurrent())
                .await
                .into_result()
                .unwrap();
            TransactionOps::execute(&tx_a, &cx, "UPDATE t SET val = 10 WHERE id = 1", &[])
                .await
                .into_result()
                .unwrap();
            TransactionOps::execute(&tx_a, &cx, "CREATE TABLE winner (id INTEGER)", &[])
                .await
                .into_result()
                .unwrap();
            let stmt_b =
                TransactionOps::execute(&tx_b, &cx, "UPDATE t SET val = 20 WHERE id = 1", &[])
                    .await;
            tx_a.commit(&cx).await.into_result().expect("a commits");
            let failed = match stmt_b {
                Outcome::Err(e) => {
                    let _ = tx_b.rollback(&cx).await;
                    e
                }
                Outcome::Ok(_) => match tx_b.commit(&cx).await {
                    Outcome::Err(e) => e,
                    other => panic!("b must lose the conflict, got {other:?}"),
                },
                other => panic!("unexpected: {other:?}"),
            };
            assert!(failed.is_retryable(), "{failed}");
        });
        // b is in autocommit again and sees a's commit ...
        let rows = b.query_sync("SELECT val FROM t WHERE id = 1", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(10)), "b sees a's value");
        let rows = b
            .query_sync("SELECT name FROM sqlite_master WHERE name = 'winner'", &[])
            .unwrap();
        assert_eq!(rows.len(), 1, "b sees the table a created");
        // ... and can run a new transaction.
        b.begin_sync(IsolationLevel::ReadCommitted).unwrap();
        b.execute_sync("UPDATE t SET val = 30 WHERE id = 1", &[])
            .unwrap();
        b.commit_sync().unwrap();
        let rows = a.query_sync("SELECT val FROM t WHERE id = 1", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(30)));
        drop(a);
        drop(b);
        remove_db_family(&path);
    }

    #[test]
    fn conflicting_concurrent_writers_surface_a_retryable_error() {
        use sqlmodel_core::Cx;
        let path = concurrent_test_db("conflict");
        let a = FrankenConnection::open_file(&path).unwrap();
        a.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        a.execute_raw("INSERT INTO t VALUES (1, 0)").unwrap();
        let b = FrankenConnection::open_file(&path).unwrap();

        let rt = asupersync::runtime::RuntimeBuilder::current_thread()
            .build()
            .unwrap();
        let cx = Cx::for_testing();
        let conflict = rt.block_on(async {
            let tx_a = Connection::begin_with_options(&a, &cx, TransactionOptions::concurrent())
                .await
                .into_result()
                .unwrap();
            let tx_b = Connection::begin_with_options(&b, &cx, TransactionOptions::concurrent())
                .await
                .into_result()
                .unwrap();

            TransactionOps::execute(&tx_a, &cx, "UPDATE t SET val = val + 1 WHERE id = 1", &[])
                .await
                .into_result()
                .unwrap();
            // The conflicting write may be detected at the statement or at commit,
            // depending on the engine's conflict-detection point; either is a
            // retryable failure for the second writer.
            let stmt_b =
                TransactionOps::execute(&tx_b, &cx, "UPDATE t SET val = val + 1 WHERE id = 1", &[])
                    .await;

            tx_a.commit(&cx)
                .await
                .into_result()
                .expect("first writer commits");
            match stmt_b {
                Outcome::Err(e) => {
                    let _ = tx_b.rollback(&cx).await;
                    Some(e)
                }
                Outcome::Ok(_) => match tx_b.commit(&cx).await {
                    Outcome::Err(e) => Some(e),
                    Outcome::Ok(()) => None,
                    Outcome::Cancelled(r) => panic!("unexpected cancellation: {r:?}"),
                    Outcome::Panicked(p) => panic!("unexpected panic: {p:?}"),
                },
                Outcome::Cancelled(r) => panic!("unexpected cancellation: {r:?}"),
                Outcome::Panicked(p) => panic!("unexpected panic: {p:?}"),
            }
        });

        let err = conflict.expect("second writer must fail on the same row");
        assert!(
            err.is_retryable(),
            "write conflict must be classified retryable so retry_transaction can handle it: {err}"
        );

        let rows = a.query_sync("SELECT val FROM t WHERE id = 1", &[]).unwrap();
        assert_eq!(
            rows[0].get(0),
            Some(&Value::BigInt(1)),
            "exactly one increment survived; no lost update"
        );

        drop(a);
        drop(b);
        remove_db_family(&path);
    }

    #[test]
    fn explicit_locking_modes_map_to_their_begin_forms() {
        use sqlmodel_core::Cx;
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        let rt = asupersync::runtime::RuntimeBuilder::current_thread()
            .build()
            .unwrap();
        let cx = Cx::for_testing();
        for mode in [
            TransactionMode::Immediate,
            TransactionMode::Exclusive,
            TransactionMode::Deferred,
            TransactionMode::Default,
        ] {
            rt.block_on(async {
                let tx = Connection::begin_with_options(
                    &conn,
                    &cx,
                    TransactionOptions::new().with_mode(mode),
                )
                .await
                .into_result()
                .unwrap_or_else(|e| panic!("{mode:?}: {e}"));
                TransactionOps::execute(&tx, &cx, "INSERT INTO t DEFAULT VALUES", &[])
                    .await
                    .into_result()
                    .unwrap();
                tx.commit(&cx).await.into_result().unwrap();
            });
        }
        let rows = conn.query_sync("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(4)));
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
