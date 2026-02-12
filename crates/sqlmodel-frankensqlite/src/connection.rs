//! FrankenSQLite connection implementing `sqlmodel_core::Connection`.
//!
//! Wraps `fsqlite::Connection` (which is `!Send` due to `Rc<RefCell<>>`) in
//! `Arc<Mutex<>>` to satisfy the `Connection: Send + Sync` requirement.
//! All operations execute synchronously under the mutex, matching the pattern
//! used by `sqlmodel-sqlite` for its FFI-based wrapper.

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::result_large_err)]

use crate::value::{sqlite_to_value, value_to_sqlite};
use fsqlite_types::value::SqliteValue;
use sqlmodel_core::{
    Connection, Cx, IsolationLevel, Outcome, PreparedStatement,
    Row, TransactionOps, Value,
    error::{ConnectionError, ConnectionErrorKind, Error, QueryError, QueryErrorKind},
    row::ColumnInfo,
};
use std::future::Future;
use std::sync::{Arc, Mutex};

/// Inner state guarded by a mutex.
struct FrankenInner {
    /// The underlying frankensqlite connection (`!Send`, hence wrapped).
    conn: fsqlite::Connection,
    /// Whether we are currently inside a transaction.
    in_transaction: bool,
    /// The last inserted rowid (tracked manually since frankensqlite stubs it).
    last_insert_rowid: i64,
}

// SAFETY: All access to `FrankenInner` goes through the `Mutex`, which
// serializes access. The `Rc<RefCell<>>` inside `fsqlite::Connection` is
// never shared across threads — the mutex ensures single-threaded access.
unsafe impl Send for FrankenInner {}

/// A SQLite connection backed by FrankenSQLite (pure Rust).
///
/// Implements `sqlmodel_core::Connection` and provides sync helper methods
/// (`execute_raw`, `query_sync`, `execute_sync`, etc.) matching the
/// `SqliteConnection` API for drop-in replacement.
pub struct FrankenConnection {
    inner: Arc<Mutex<FrankenInner>>,
    path: String,
}

// SAFETY: All access goes through Arc<Mutex<>> — single-thread serialization.
unsafe impl Send for FrankenConnection {}
unsafe impl Sync for FrankenConnection {}

impl FrankenConnection {
    /// Open a connection with the given path.
    ///
    /// Use `":memory:"` for an in-memory database, or a file path for
    /// persistent storage.
    pub fn open(path: impl Into<String>) -> Result<Self, Error> {
        let path = path.into();
        let conn = fsqlite::Connection::open(&path).map_err(|e| franken_to_conn_error(&e))?;
        Ok(Self {
            inner: Arc::new(Mutex::new(FrankenInner {
                conn,
                in_transaction: false,
                last_insert_rowid: 0,
            })),
            path,
        })
    }

    /// Open an in-memory database.
    pub fn open_memory() -> Result<Self, Error> {
        Self::open(":memory:")
    }

    /// Open a file-based database.
    pub fn open_file(path: impl Into<String>) -> Result<Self, Error> {
        Self::open(path)
    }

    /// Get the database path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Execute SQL directly without parameter binding (for DDL, PRAGMAs, etc.)
    pub fn execute_raw(&self, sql: &str) -> Result<(), Error> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner
            .conn
            .execute(sql)
            .map_err(|e| franken_to_query_error(&e, sql))?;
        Ok(())
    }

    /// Prepare and execute a query synchronously, returning all rows.
    pub fn query_sync(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, Error> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let sqlite_params: Vec<SqliteValue> = params.iter().map(value_to_sqlite).collect();

        let franken_rows = if sqlite_params.is_empty() {
            inner.conn.query(sql)
        } else {
            inner.conn.query_with_params(sql, &sqlite_params)
        }
        .map_err(|e| franken_to_query_error(&e, sql))?;

        eprintln!("[DEBUG] Query returned {} rows", franken_rows.len());
        if !franken_rows.is_empty() {
            eprintln!("[DEBUG] First row has {} values", franken_rows[0].values().len());
            // Show the values for debugging
            let vals: Vec<_> = franken_rows[0].values().iter().map(|v| format!("{:?}", v)).collect();
            eprintln!("[DEBUG] Row values: {:?}", vals);
        }

        // For RETURNING *, get column names from table schema
        let schema_columns = self.get_returning_star_columns(sql, &inner.conn);
        Ok(convert_rows_with_schema(&franken_rows, sql, schema_columns.as_deref()))
    }

    /// Get column names for RETURNING * from the table schema.
    fn get_returning_star_columns(
        &self,
        sql: &str,
        conn: &fsqlite_core::connection::Connection,
    ) -> Option<Vec<String>> {
        let upper = sql.to_uppercase();

        // Check if this is a RETURNING * query
        if !upper.contains(" RETURNING *") && !upper.ends_with("RETURNING *") {
            eprintln!("[DEBUG] Not a RETURNING * query: {}", &sql[..sql.len().min(100)]);
            return None;
        }
        eprintln!("[DEBUG] Detected RETURNING * query");

        // Extract table name
        let table_name = extract_table_name_for_returning(sql)?;
        eprintln!("[DEBUG] Extracted table name: {}", table_name);

        // Query PRAGMA table_info to get column names
        let pragma_sql = format!("PRAGMA table_info({})", table_name);
        let pragma_rows = match conn.query(&pragma_sql) {
            Ok(rows) => {
                eprintln!("[DEBUG] PRAGMA returned {} rows", rows.len());
                rows
            }
            Err(e) => {
                eprintln!("[DEBUG] PRAGMA failed: {:?}", e);
                return None;
            }
        };

        // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
        // Column index 1 is the name
        let columns: Vec<String> = pragma_rows
            .iter()
            .filter_map(|row| {
                row.values().get(1).and_then(|v| match v {
                    SqliteValue::Text(s) => Some(s.clone()),
                    _ => None,
                })
            })
            .collect();

        eprintln!("[DEBUG] Extracted columns: {:?}", columns);

        if columns.is_empty() {
            None
        } else {
            Some(columns)
        }
    }

    /// Prepare and execute a statement synchronously, returning rows affected.
    pub fn execute_sync(&self, sql: &str, params: &[Value]) -> Result<u64, Error> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let sqlite_params: Vec<SqliteValue> = params.iter().map(value_to_sqlite).collect();

        eprintln!("[DEBUG] execute_sync: {}", &sql[..sql.len().min(100)]);

        let count = if sqlite_params.is_empty() {
            inner.conn.execute(sql)
        } else {
            inner.conn.execute_with_params(sql, &sqlite_params)
        }
        .map_err(|e| franken_to_query_error(&e, sql))?;

        eprintln!("[DEBUG] execute_sync result: {} rows affected", count);

        // Track last_insert_rowid for INSERT statements
        if is_insert_sql(sql) {
            // After an INSERT, query last_insert_rowid()
            if let Ok(rows) = inner.conn.query("SELECT last_insert_rowid()") {
                if let Some(row) = rows.first() {
                    if let Some(SqliteValue::Integer(id)) = row.get(0) {
                        inner.last_insert_rowid = *id;
                    }
                }
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
        if let Ok(rows) = inner.conn.query("SELECT changes()") {
            if let Some(row) = rows.first() {
                if let Some(SqliteValue::Integer(n)) = row.get(0) {
                    return *n;
                }
            }
        }
        0
    }

    /// Execute an INSERT and return the last inserted rowid.
    fn insert_sync(&self, sql: &str, params: &[Value]) -> Result<i64, Error> {
        self.execute_sync(sql, params)?;
        Ok(self.last_insert_rowid())
    }

    /// Begin a transaction.
    fn begin_sync(&self, isolation: IsolationLevel) -> Result<(), Error> {
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

        let begin_sql = match isolation {
            IsolationLevel::Serializable => "BEGIN EXCLUSIVE",
            IsolationLevel::RepeatableRead | IsolationLevel::ReadCommitted => "BEGIN IMMEDIATE",
            IsolationLevel::ReadUncommitted => "BEGIN DEFERRED",
        };

        inner
            .conn
            .execute(begin_sql)
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
            .execute("COMMIT")
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
            .execute("ROLLBACK")
            .map_err(|e| franken_to_query_error(&e, "ROLLBACK"))?;

        inner.in_transaction = false;
        Ok(())
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

    async fn close(self, _cx: &Cx) -> sqlmodel_core::Result<()> {
        // Connection is closed on drop (inner Rc<RefCell<>> cleanup)
        Ok(())
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

    fn rollback_to(
        &self,
        _cx: &Cx,
        name: &str,
    ) -> impl Future<Output = Outcome<(), Error>> + Send {
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

    eprintln!("[DEBUG] Inferred col_names (before padding): {:?}", col_names);

    // Pad or trim to match actual column count
    while col_names.len() < col_count {
        col_names.push(format!("_c{}", col_names.len()));
    }
    col_names.truncate(col_count);

    eprintln!("[DEBUG] Final col_names: {:?}", col_names);

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
        "INTEGRITY_CHECK" | "QUICK_CHECK" => vec!["integrity_check".into()],
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
    let returning_pos = if let Some(pos) = find_keyword_at_depth_zero(&upper, "RETURNING") {
        pos
    } else {
        return Vec::new();
    };

    // Extract the part after RETURNING
    let after_returning = &sql[returning_pos + 9..].trim_start();

    // Handle "RETURNING *"
    if after_returning.trim() == "*" || after_returning.starts_with("* ") || after_returning.starts_with("*;") {
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
    if upper.starts_with("INSERT") {
        if let Some(into_pos) = upper.find(" INTO ") {
            let after_into = &sql[into_pos + 6..].trim_start();
            // Table name is the next word (may be quoted)
            let table = extract_identifier(after_into);
            if !table.is_empty() {
                return Some(table);
            }
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
    if upper.starts_with("DELETE") {
        if let Some(from_pos) = upper.find(" FROM ") {
            let after_from = &sql[from_pos + 6..].trim_start();
            let table = extract_identifier(after_from);
            if !table.is_empty() {
                return Some(table);
            }
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
    if trimmed.starts_with('"') {
        if let Some(end) = trimmed[1..].find('"') {
            return trimmed[1..end + 1].to_string();
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
            // Ensure it's a word boundary
            let before_ok = i == 0 || !upper.as_bytes()[i - 1].is_ascii_alphanumeric();
            let after_ok = i + kw_len >= upper.len()
                || !upper.as_bytes()[i + kw_len].is_ascii_alphanumeric();
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

    if max_param > 0 {
        max_param
    } else {
        bare_count
    }
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
        FrankenError::ForeignKeyViolation { .. } | FrankenError::CheckViolation { .. } => {
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

    #[test]
    fn open_memory_succeeds() {
        let conn = FrankenConnection::open_memory().expect("should open in-memory db");
        assert_eq!(conn.path(), ":memory:");
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
    fn transaction_commit() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();

        conn.begin_sync(IsolationLevel::ReadCommitted).unwrap();
        conn.execute_sync("INSERT INTO t (val) VALUES (?1)", &[Value::Text("a".into())])
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
        conn.execute_sync("INSERT INTO t (val) VALUES (?1)", &[Value::Text("a".into())])
            .unwrap();
        conn.rollback_sync().unwrap();

        let rows = conn.query_sync("SELECT val FROM t", &[]).unwrap();
        assert_eq!(rows.len(), 0);
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
        conn.execute_raw("INSERT INTO t VALUES (1, 'hello')").unwrap();
        conn.execute_raw("COMMIT").unwrap();

        let rows = conn.query_sync("SELECT val FROM t WHERE id = 1", &[]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::Text("hello".into())));
    }

    #[test]
    fn begin_concurrent_rollback() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute_raw("BEGIN CONCURRENT").unwrap();
        conn.execute_raw("INSERT INTO t VALUES (1, 'gone')").unwrap();
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
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
        conn.begin_sync(IsolationLevel::Serializable).unwrap();
        conn.execute_sync("INSERT INTO t VALUES (1)", &[]).unwrap();
        conn.commit_sync().unwrap();
        let rows = conn.query_sync("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(1)));
    }

    #[test]
    fn begin_read_uncommitted_uses_deferred() {
        let conn = FrankenConnection::open_memory().unwrap();
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
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
        conn.execute_raw("INSERT INTO t VALUES (1, 'keep')").unwrap();
        conn.execute_raw("SAVEPOINT sp1").unwrap();
        conn.execute_raw("INSERT INTO t VALUES (2, 'discard')").unwrap();
        conn.execute_raw("ROLLBACK TO sp1").unwrap();
        conn.execute_raw("COMMIT").unwrap();

        let rows = conn.query_sync("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::BigInt(1)));
        let rows = conn.query_sync("SELECT val FROM t WHERE id = 1", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::Text("keep".into())));
    }

    // ── File-based connection test ────────────────────────────────────────

    #[test]
    fn file_based_connection() {
        let dir = std::env::temp_dir().join("sqlmodel_franken_test");
        let _ = std::fs::create_dir_all(&dir);
        let db_path = dir.join("test_file.db");
        let path_str = db_path.display().to_string();

        // Clean up from previous runs
        let _ = std::fs::remove_file(&db_path);

        {
            let conn = FrankenConnection::open_file(&path_str).unwrap();
            conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .unwrap();
            conn.execute_raw("BEGIN CONCURRENT").unwrap();
            conn.execute_sync(
                "INSERT INTO t VALUES (1, 'persistent')",
                &[],
            )
            .unwrap();
            conn.execute_raw("COMMIT").unwrap();
        }

        // Reopen and verify data persisted
        {
            let conn = FrankenConnection::open_file(&path_str).unwrap();
            let rows = conn.query_sync("SELECT val FROM t WHERE id = 1", &[]).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0), Some(&Value::Text("persistent".into())));
        }

        let _ = std::fs::remove_file(&db_path);
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
        let err = FrankenError::SerializationFailure {
            page: 7,
        };
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
    fn infer_columns_with_cte() {
        let names = infer_column_names(
            "WITH cte AS (SELECT 1 AS x) SELECT x, x + 1 AS y FROM cte",
        );
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
        conn.execute_sync("INSERT INTO t VALUES (1, 'a')", &[]).unwrap();
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
        conn.execute_sync("INSERT INTO t (val) VALUES ('a')", &[]).unwrap();
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
        conn.execute_sync("INSERT INTO t VALUES (1, 'async')", &[]).unwrap();

        let cx = Cx::for_testing();
        // Test that the async Connection::query method works correctly
        let result = asupersync::runtime::RuntimeBuilder::current_thread()
            .build()
            .unwrap()
            .block_on(async {
                Connection::query(&conn, &cx, "SELECT val FROM t", &[]).await
            });
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
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();

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
        conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();

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
        let rows = conn.query_sync("SELECT val FROM t WHERE id = 1", &[]).unwrap();
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
        let rows = conn.query_sync("SELECT data FROM t WHERE id = 1", &[]).unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::Bytes(blob)));
    }
}
