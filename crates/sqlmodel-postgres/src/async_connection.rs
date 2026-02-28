//! Async PostgreSQL connection implementation.
//!
//! This module implements an async PostgreSQL connection using asupersync's TCP
//! primitives. It provides a shared wrapper that implements `sqlmodel-core`'s
//! [`Connection`] trait.
//!
//! The implementation currently focuses on:
//! - Async connect + authentication (cleartext, MD5, SCRAM-SHA-256)
//! - Extended query protocol for parameterized queries
//! - Row decoding via the postgres type registry (OID + text/binary format)
//! - Basic transaction support (BEGIN/COMMIT/ROLLBACK + savepoints)

// Allow `impl Future` return types in trait methods - intentional for async trait compat
#![allow(clippy::manual_async_fn)]
// The Error type is intentionally large to carry full context
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::future::Future;
#[cfg(feature = "tls")]
use std::io::{Read, Write};
use std::sync::Arc;

use asupersync::io::{AsyncRead, AsyncWrite, ReadBuf};
use asupersync::net::TcpStream;
use asupersync::sync::Mutex;
use asupersync::{Cx, Outcome};

use sqlmodel_core::connection::{Connection, IsolationLevel, PreparedStatement, TransactionOps};
use sqlmodel_core::error::{
    ConnectionError, ConnectionErrorKind, ProtocolError, QueryError, QueryErrorKind,
};
use sqlmodel_core::row::ColumnInfo;
use sqlmodel_core::{Error, Row, Value};

use crate::auth::ScramClient;
use crate::config::{PgConfig, SslMode};
use crate::connection::{ConnectionState, TransactionStatusState};
use crate::protocol::{
    BackendMessage, DescribeKind, ErrorFields, FrontendMessage, MessageReader, MessageWriter,
    PROTOCOL_VERSION,
};
use crate::types::{Format, decode_value, encode_value};

#[cfg(feature = "tls")]
use crate::tls;

enum PgAsyncStream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(AsyncTlsStream),
    #[cfg(feature = "tls")]
    Closed,
}

impl PgAsyncStream {
    #[cfg(feature = "tls")]
    async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            PgAsyncStream::Plain(s) => read_exact_plain_async(s, buf).await,
            #[cfg(feature = "tls")]
            PgAsyncStream::Tls(s) => s.read_exact(buf).await,
            #[cfg(feature = "tls")]
            PgAsyncStream::Closed => Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "connection closed",
            )),
        }
    }

    async fn read_some(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            PgAsyncStream::Plain(s) => read_some_plain_async(s, buf).await,
            #[cfg(feature = "tls")]
            PgAsyncStream::Tls(s) => s.read_plain(buf).await,
            #[cfg(feature = "tls")]
            PgAsyncStream::Closed => Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "connection closed",
            )),
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            PgAsyncStream::Plain(s) => write_all_plain_async(s, buf).await,
            #[cfg(feature = "tls")]
            PgAsyncStream::Tls(s) => s.write_all(buf).await,
            #[cfg(feature = "tls")]
            PgAsyncStream::Closed => Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "connection closed",
            )),
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            PgAsyncStream::Plain(s) => flush_plain_async(s).await,
            #[cfg(feature = "tls")]
            PgAsyncStream::Tls(s) => s.flush().await,
            #[cfg(feature = "tls")]
            PgAsyncStream::Closed => Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "connection closed",
            )),
        }
    }
}

#[cfg(feature = "tls")]
struct AsyncTlsStream {
    tcp: TcpStream,
    tls: rustls::ClientConnection,
}

#[cfg(feature = "tls")]
impl AsyncTlsStream {
    async fn handshake(mut tcp: TcpStream, ssl_mode: SslMode, host: &str) -> Result<Self, Error> {
        let config = tls::build_client_config(ssl_mode)?;
        let server_name = tls::server_name(host)?;
        let mut tls = rustls::ClientConnection::new(std::sync::Arc::new(config), server_name)
            .map_err(|e| connection_error(format!("Failed to create TLS connection: {e}")))?;

        while tls.is_handshaking() {
            while tls.wants_write() {
                let mut out = Vec::new();
                tls.write_tls(&mut out)
                    .map_err(|e| connection_error(format!("TLS handshake write_tls error: {e}")))?;
                if !out.is_empty() {
                    write_all_plain_async(&mut tcp, &out).await.map_err(|e| {
                        Error::Connection(ConnectionError {
                            kind: ConnectionErrorKind::Disconnected,
                            message: format!("TLS handshake write error: {e}"),
                            source: Some(Box::new(e)),
                        })
                    })?;
                }
            }

            if tls.wants_read() {
                let mut buf = [0u8; 8192];
                let n = read_some_plain_async(&mut tcp, &mut buf)
                    .await
                    .map_err(|e| {
                        Error::Connection(ConnectionError {
                            kind: ConnectionErrorKind::Disconnected,
                            message: format!("TLS handshake read error: {e}"),
                            source: Some(Box::new(e)),
                        })
                    })?;
                if n == 0 {
                    return Err(connection_error("Connection closed during TLS handshake"));
                }

                let mut cursor = std::io::Cursor::new(&buf[..n]);
                tls.read_tls(&mut cursor)
                    .map_err(|e| connection_error(format!("TLS handshake read_tls error: {e}")))?;
                tls.process_new_packets()
                    .map_err(|e| connection_error(format!("TLS handshake error: {e}")))?;
            }
        }

        Ok(Self { tcp, tls })
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        let mut read = 0;
        while read < buf.len() {
            let n = self.read_plain(&mut buf[read..]).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                ));
            }
            read += n;
        }
        Ok(())
    }

    async fn read_plain(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.tls.reader().read(out) {
                Ok(n) if n > 0 => return Ok(n),
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }

            if !self.tls.wants_read() {
                return Ok(0);
            }

            let mut enc = [0u8; 8192];
            let n = read_some_plain_async(&mut self.tcp, &mut enc).await?;
            if n == 0 {
                return Ok(0);
            }

            let mut cursor = std::io::Cursor::new(&enc[..n]);
            self.tls.read_tls(&mut cursor)?;
            self.tls
                .process_new_packets()
                .map_err(|e| std::io::Error::other(format!("TLS error: {e}")))?;
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let mut written = 0;
        while written < buf.len() {
            let n = self.tls.writer().write(&buf[written..])?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "TLS write zero",
                ));
            }
            written += n;
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        self.tls.writer().flush()?;
        while self.tls.wants_write() {
            let mut out = Vec::new();
            self.tls.write_tls(&mut out)?;
            if !out.is_empty() {
                write_all_plain_async(&mut self.tcp, &out).await?;
            }
        }
        flush_plain_async(&mut self.tcp).await
    }
}

#[cfg(feature = "tls")]
async fn read_exact_plain_async(stream: &mut TcpStream, buf: &mut [u8]) -> std::io::Result<()> {
    let mut read = 0;
    while read < buf.len() {
        let n = read_some_plain_async(stream, &mut buf[read..]).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }
        read += n;
    }
    Ok(())
}

async fn read_some_plain_async(stream: &mut TcpStream, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut read_buf = ReadBuf::new(buf);
    std::future::poll_fn(|cx| std::pin::Pin::new(&mut *stream).poll_read(cx, &mut read_buf))
        .await?;
    Ok(read_buf.filled().len())
}

async fn write_all_plain_async(stream: &mut TcpStream, buf: &[u8]) -> std::io::Result<()> {
    let mut written = 0;
    while written < buf.len() {
        let n = std::future::poll_fn(|cx| {
            std::pin::Pin::new(&mut *stream).poll_write(cx, &buf[written..])
        })
        .await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "connection closed",
            ));
        }
        written += n;
    }
    Ok(())
}

async fn flush_plain_async(stream: &mut TcpStream) -> std::io::Result<()> {
    std::future::poll_fn(|cx| std::pin::Pin::new(&mut *stream).poll_flush(cx)).await
}

/// Async PostgreSQL connection.
///
/// This connection uses asupersync's TCP stream for non-blocking I/O and
/// supports the extended query protocol for parameter binding.
pub struct PgAsyncConnection {
    stream: PgAsyncStream,
    state: ConnectionState,
    process_id: i32,
    secret_key: i32,
    parameters: HashMap<String, String>,
    next_prepared_id: u64,
    prepared: HashMap<u64, PgPreparedMeta>,
    config: PgConfig,
    reader: MessageReader,
    writer: MessageWriter,
    read_buf: Vec<u8>,
}

#[derive(Debug, Clone)]
struct PgPreparedMeta {
    name: String,
    param_type_oids: Vec<u32>,
}

impl std::fmt::Debug for PgAsyncConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgAsyncConnection")
            .field("state", &self.state)
            .field("process_id", &self.process_id)
            .field("host", &self.config.host)
            .field("port", &self.config.port)
            .field("database", &self.config.database)
            .finish_non_exhaustive()
    }
}

impl PgAsyncConnection {
    /// Establish a new async connection to the PostgreSQL server.
    pub async fn connect(_cx: &Cx, config: PgConfig) -> Outcome<Self, Error> {
        let addr = config.socket_addr();
        let socket_addr = match addr.parse() {
            Ok(a) => a,
            Err(e) => {
                return Outcome::Err(Error::Connection(ConnectionError {
                    kind: ConnectionErrorKind::Connect,
                    message: format!("Invalid socket address: {}", e),
                    source: None,
                }));
            }
        };

        let stream = match TcpStream::connect_timeout(socket_addr, config.connect_timeout).await {
            Ok(s) => s,
            Err(e) => {
                let kind = if e.kind() == std::io::ErrorKind::ConnectionRefused {
                    ConnectionErrorKind::Refused
                } else {
                    ConnectionErrorKind::Connect
                };
                return Outcome::Err(Error::Connection(ConnectionError {
                    kind,
                    message: format!("Failed to connect to {}: {}", addr, e),
                    source: Some(Box::new(e)),
                }));
            }
        };

        stream.set_nodelay(true).ok();

        let mut conn = Self {
            stream: PgAsyncStream::Plain(stream),
            state: ConnectionState::Connecting,
            process_id: 0,
            secret_key: 0,
            parameters: HashMap::new(),
            next_prepared_id: 1,
            prepared: HashMap::new(),
            config,
            reader: MessageReader::new(),
            writer: MessageWriter::new(),
            read_buf: vec![0u8; 8192],
        };

        // SSL negotiation (feature-gated TLS)
        if conn.config.ssl_mode.should_try_ssl() {
            #[cfg(feature = "tls")]
            match conn.negotiate_ssl().await {
                Outcome::Ok(()) => {}
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            }

            #[cfg(not(feature = "tls"))]
            if conn.config.ssl_mode != SslMode::Prefer {
                return Outcome::Err(connection_error(
                    "TLS requested but 'sqlmodel-postgres' was built without feature 'tls'",
                ));
            }
        }

        // Startup + authentication
        if let Outcome::Err(e) = conn.send_startup().await {
            return Outcome::Err(e);
        }
        conn.state = ConnectionState::Authenticating;

        match conn.handle_auth().await {
            Outcome::Ok(()) => {}
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        }

        match conn.read_startup_messages().await {
            Outcome::Ok(()) => Outcome::Ok(conn),
            Outcome::Err(e) => Outcome::Err(e),
            Outcome::Cancelled(r) => Outcome::Cancelled(r),
            Outcome::Panicked(p) => Outcome::Panicked(p),
        }
    }

    /// Run a parameterized query and return all rows.
    pub async fn query_async(
        &mut self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> Outcome<Vec<Row>, Error> {
        match self.run_extended(cx, sql, params).await {
            Outcome::Ok(result) => Outcome::Ok(result.rows),
            Outcome::Err(e) => Outcome::Err(e),
            Outcome::Cancelled(r) => Outcome::Cancelled(r),
            Outcome::Panicked(p) => Outcome::Panicked(p),
        }
    }

    /// Execute a statement and return rows affected.
    pub async fn execute_async(
        &mut self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> Outcome<u64, Error> {
        match self.run_extended(cx, sql, params).await {
            Outcome::Ok(result) => {
                Outcome::Ok(parse_rows_affected(result.command_tag.as_deref()).unwrap_or(0))
            }
            Outcome::Err(e) => Outcome::Err(e),
            Outcome::Cancelled(r) => Outcome::Cancelled(r),
            Outcome::Panicked(p) => Outcome::Panicked(p),
        }
    }

    /// Execute an INSERT and return the inserted id.
    ///
    /// PostgreSQL requires `RETURNING` to retrieve generated IDs. This method
    /// expects the SQL to return a single-row, single-column result set
    /// containing an integer id.
    pub async fn insert_async(
        &mut self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> Outcome<i64, Error> {
        let result = match self.run_extended(cx, sql, params).await {
            Outcome::Ok(r) => r,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let Some(row) = result.rows.first() else {
            return Outcome::Err(query_error_msg(
                "INSERT did not return an id; add `RETURNING id`",
                QueryErrorKind::Database,
            ));
        };
        let Some(id_value) = row.get(0) else {
            return Outcome::Err(query_error_msg(
                "INSERT result row missing id column",
                QueryErrorKind::Database,
            ));
        };
        match id_value.as_i64() {
            Some(v) => Outcome::Ok(v),
            None => Outcome::Err(query_error_msg(
                "INSERT returned non-integer id",
                QueryErrorKind::Database,
            )),
        }
    }

    /// Ping the server.
    pub async fn ping_async(&mut self, cx: &Cx) -> Outcome<(), Error> {
        self.execute_async(cx, "SELECT 1", &[]).await.map(|_| ())
    }

    /// Close the connection.
    pub async fn close_async(&mut self, cx: &Cx) -> Outcome<(), Error> {
        // Best-effort terminate. If this fails, the drop will close the socket.
        //
        // Note: server-side prepared statements are released when the connection terminates;
        // explicit Close/DEALLOCATE is not required for correctness here.
        let _ = self.send_message(cx, &FrontendMessage::Terminate).await;
        self.state = ConnectionState::Closed;
        Outcome::Ok(())
    }

    // ==================== Prepared statements ====================

    /// Prepare a server-side statement and return a reusable handle.
    pub async fn prepare_async(&mut self, cx: &Cx, sql: &str) -> Outcome<PreparedStatement, Error> {
        let stmt_id = self.next_prepared_id;
        self.next_prepared_id = self.next_prepared_id.saturating_add(1);
        let stmt_name = format!("sqlmodel_stmt_{stmt_id}");

        if let Outcome::Err(e) = self
            .send_message(
                cx,
                &FrontendMessage::Parse {
                    name: stmt_name.clone(),
                    query: sql.to_string(),
                    // Let PostgreSQL infer types where possible; ambiguous queries will error
                    // and should add explicit casts.
                    param_types: Vec::new(),
                },
            )
            .await
        {
            return Outcome::Err(e);
        }

        if let Outcome::Err(e) = self
            .send_message(
                cx,
                &FrontendMessage::Describe {
                    kind: DescribeKind::Statement,
                    name: stmt_name.clone(),
                },
            )
            .await
        {
            return Outcome::Err(e);
        }

        if let Outcome::Err(e) = self.send_message(cx, &FrontendMessage::Sync).await {
            return Outcome::Err(e);
        }

        let mut param_type_oids: Option<Vec<u32>> = None;
        let mut columns: Option<Vec<String>> = None;

        loop {
            let msg = match self.receive_message(cx).await {
                Outcome::Ok(m) => m,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };

            match msg {
                BackendMessage::ParseComplete
                | BackendMessage::BindComplete
                | BackendMessage::CloseComplete
                | BackendMessage::NoData
                | BackendMessage::EmptyQueryResponse => {}
                BackendMessage::ParameterDescription(oids) => {
                    param_type_oids = Some(oids);
                }
                BackendMessage::RowDescription(desc) => {
                    columns = Some(desc.iter().map(|f| f.name.clone()).collect());
                }
                BackendMessage::ReadyForQuery(status) => {
                    self.state = ConnectionState::Ready(TransactionStatusState::from(status));
                    break;
                }
                BackendMessage::ErrorResponse(e) => {
                    self.state = ConnectionState::Error;
                    return Outcome::Err(error_from_fields(&e));
                }
                BackendMessage::NoticeResponse(_notice) => {}
                other => {
                    return Outcome::Err(protocol_error(format!(
                        "Unexpected message during prepare: {other:?}"
                    )));
                }
            }
        }

        let param_type_oids = param_type_oids.unwrap_or_default();
        self.prepared.insert(
            stmt_id,
            PgPreparedMeta {
                name: stmt_name,
                param_type_oids: param_type_oids.clone(),
            },
        );

        match columns {
            Some(cols) => Outcome::Ok(PreparedStatement::with_columns(
                stmt_id,
                sql.to_string(),
                param_type_oids.len(),
                cols,
            )),
            None => Outcome::Ok(PreparedStatement::new(
                stmt_id,
                sql.to_string(),
                param_type_oids.len(),
            )),
        }
    }

    pub async fn query_prepared_async(
        &mut self,
        cx: &Cx,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> Outcome<Vec<Row>, Error> {
        let meta = match self.prepared.get(&stmt.id()) {
            Some(m) => m.clone(),
            None => {
                return Outcome::Err(query_error_msg(
                    format!("Unknown prepared statement id {}", stmt.id()),
                    QueryErrorKind::Database,
                ));
            }
        };

        if meta.param_type_oids.len() != params.len() {
            return Outcome::Err(query_error_msg(
                format!(
                    "Prepared statement expects {} params, got {}",
                    meta.param_type_oids.len(),
                    params.len()
                ),
                QueryErrorKind::Database,
            ));
        }

        match self.run_prepared(cx, &meta, params).await {
            Outcome::Ok(result) => Outcome::Ok(result.rows),
            Outcome::Err(e) => Outcome::Err(e),
            Outcome::Cancelled(r) => Outcome::Cancelled(r),
            Outcome::Panicked(p) => Outcome::Panicked(p),
        }
    }

    pub async fn execute_prepared_async(
        &mut self,
        cx: &Cx,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> Outcome<u64, Error> {
        let meta = match self.prepared.get(&stmt.id()) {
            Some(m) => m.clone(),
            None => {
                return Outcome::Err(query_error_msg(
                    format!("Unknown prepared statement id {}", stmt.id()),
                    QueryErrorKind::Database,
                ));
            }
        };

        if meta.param_type_oids.len() != params.len() {
            return Outcome::Err(query_error_msg(
                format!(
                    "Prepared statement expects {} params, got {}",
                    meta.param_type_oids.len(),
                    params.len()
                ),
                QueryErrorKind::Database,
            ));
        }

        match self.run_prepared(cx, &meta, params).await {
            Outcome::Ok(result) => {
                Outcome::Ok(parse_rows_affected(result.command_tag.as_deref()).unwrap_or(0))
            }
            Outcome::Err(e) => Outcome::Err(e),
            Outcome::Cancelled(r) => Outcome::Cancelled(r),
            Outcome::Panicked(p) => Outcome::Panicked(p),
        }
    }

    // ==================== Protocol: extended query ====================

    async fn read_extended_result(&mut self, cx: &Cx) -> Outcome<PgQueryResult, Error> {
        // Read responses until ReadyForQuery
        let mut field_descs: Option<Vec<crate::protocol::FieldDescription>> = None;
        let mut columns: Option<Arc<ColumnInfo>> = None;
        let mut rows: Vec<Row> = Vec::new();
        let mut command_tag: Option<String> = None;

        loop {
            let msg = match self.receive_message(cx).await {
                Outcome::Ok(m) => m,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };

            match msg {
                BackendMessage::ParseComplete
                | BackendMessage::BindComplete
                | BackendMessage::CloseComplete
                | BackendMessage::ParameterDescription(_)
                | BackendMessage::NoData
                | BackendMessage::PortalSuspended
                | BackendMessage::EmptyQueryResponse => {}
                BackendMessage::RowDescription(desc) => {
                    let names: Vec<String> = desc.iter().map(|f| f.name.clone()).collect();
                    columns = Some(Arc::new(ColumnInfo::new(names)));
                    field_descs = Some(desc);
                }
                BackendMessage::DataRow(raw_values) => {
                    let Some(ref desc) = field_descs else {
                        return Outcome::Err(protocol_error(
                            "DataRow received before RowDescription",
                        ));
                    };
                    let Some(ref cols) = columns else {
                        return Outcome::Err(protocol_error("Row column metadata missing"));
                    };
                    if raw_values.len() != desc.len() {
                        return Outcome::Err(protocol_error("DataRow field count mismatch"));
                    }

                    let mut values = Vec::with_capacity(raw_values.len());
                    for (i, raw) in raw_values.into_iter().enumerate() {
                        match raw {
                            None => values.push(Value::Null),
                            Some(bytes) => {
                                let field = &desc[i];
                                let format = Format::from_code(field.format);
                                let decoded = match decode_value(
                                    field.type_oid,
                                    Some(bytes.as_slice()),
                                    format,
                                ) {
                                    Ok(v) => v,
                                    Err(e) => return Outcome::Err(e),
                                };
                                values.push(decoded);
                            }
                        }
                    }
                    rows.push(Row::with_columns(Arc::clone(cols), values));
                }
                BackendMessage::CommandComplete(tag) => {
                    command_tag = Some(tag);
                }
                BackendMessage::ReadyForQuery(status) => {
                    self.state = ConnectionState::Ready(TransactionStatusState::from(status));
                    break;
                }
                BackendMessage::ErrorResponse(e) => {
                    self.state = ConnectionState::Error;
                    return Outcome::Err(error_from_fields(&e));
                }
                BackendMessage::NoticeResponse(_notice) => {}
                _ => {}
            }
        }

        Outcome::Ok(PgQueryResult { rows, command_tag })
    }

    async fn run_extended(
        &mut self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> Outcome<PgQueryResult, Error> {
        // Encode parameters
        let mut param_types = Vec::with_capacity(params.len());
        let mut param_values = Vec::with_capacity(params.len());

        for v in params {
            if matches!(v, Value::Null) {
                param_types.push(0);
                param_values.push(None);
                continue;
            }
            match encode_value(v, Format::Text) {
                Ok((bytes, oid)) => {
                    param_types.push(oid);
                    param_values.push(Some(bytes));
                }
                Err(e) => return Outcome::Err(e),
            }
        }

        // Parse + bind unnamed statement/portal
        if let Outcome::Err(e) = self
            .send_message(
                cx,
                &FrontendMessage::Parse {
                    name: String::new(),
                    query: sql.to_string(),
                    param_types,
                },
            )
            .await
        {
            return Outcome::Err(e);
        }

        let param_formats = if params.is_empty() {
            Vec::new()
        } else {
            vec![Format::Text.code()]
        };
        if let Outcome::Err(e) = self
            .send_message(
                cx,
                &FrontendMessage::Bind {
                    portal: String::new(),
                    statement: String::new(),
                    param_formats,
                    params: param_values,
                    // Default result formats (text) when empty.
                    result_formats: Vec::new(),
                },
            )
            .await
        {
            return Outcome::Err(e);
        }

        if let Outcome::Err(e) = self
            .send_message(
                cx,
                &FrontendMessage::Describe {
                    kind: DescribeKind::Portal,
                    name: String::new(),
                },
            )
            .await
        {
            return Outcome::Err(e);
        }

        if let Outcome::Err(e) = self
            .send_message(
                cx,
                &FrontendMessage::Execute {
                    portal: String::new(),
                    max_rows: 0,
                },
            )
            .await
        {
            return Outcome::Err(e);
        }

        if let Outcome::Err(e) = self.send_message(cx, &FrontendMessage::Sync).await {
            return Outcome::Err(e);
        }
        self.read_extended_result(cx).await
    }

    async fn run_prepared(
        &mut self,
        cx: &Cx,
        meta: &PgPreparedMeta,
        params: &[Value],
    ) -> Outcome<PgQueryResult, Error> {
        let mut param_values = Vec::with_capacity(params.len());

        for (i, v) in params.iter().enumerate() {
            if matches!(v, Value::Null) {
                param_values.push(None);
                continue;
            }
            match encode_value(v, Format::Text) {
                Ok((bytes, oid)) => {
                    let expected = meta.param_type_oids.get(i).copied().unwrap_or(0);
                    if expected != 0 && expected != oid {
                        return Outcome::Err(query_error_msg(
                            format!(
                                "Prepared statement param {} expects type OID {}, got {}",
                                i + 1,
                                expected,
                                oid
                            ),
                            QueryErrorKind::Database,
                        ));
                    }
                    param_values.push(Some(bytes));
                }
                Err(e) => return Outcome::Err(e),
            }
        }

        let param_formats = if params.is_empty() {
            Vec::new()
        } else {
            vec![Format::Text.code()]
        };

        if let Outcome::Err(e) = self
            .send_message(
                cx,
                &FrontendMessage::Bind {
                    portal: String::new(),
                    statement: meta.name.clone(),
                    param_formats,
                    params: param_values,
                    result_formats: Vec::new(),
                },
            )
            .await
        {
            return Outcome::Err(e);
        }

        if let Outcome::Err(e) = self
            .send_message(
                cx,
                &FrontendMessage::Describe {
                    kind: DescribeKind::Portal,
                    name: String::new(),
                },
            )
            .await
        {
            return Outcome::Err(e);
        }

        if let Outcome::Err(e) = self
            .send_message(
                cx,
                &FrontendMessage::Execute {
                    portal: String::new(),
                    max_rows: 0,
                },
            )
            .await
        {
            return Outcome::Err(e);
        }

        if let Outcome::Err(e) = self.send_message(cx, &FrontendMessage::Sync).await {
            return Outcome::Err(e);
        }

        self.read_extended_result(cx).await
    }

    // ==================== Startup + auth ====================

    #[cfg(feature = "tls")]
    async fn negotiate_ssl(&mut self) -> Outcome<(), Error> {
        // Send SSL request
        if let Outcome::Err(e) = self.send_message_no_cx(&FrontendMessage::SSLRequest).await {
            return Outcome::Err(e);
        }

        // Read single-byte response
        let mut buf = [0u8; 1];
        if let Err(e) = self.stream.read_exact(&mut buf).await {
            return Outcome::Err(Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Ssl,
                message: format!("Failed to read SSL response: {}", e),
                source: Some(Box::new(e)),
            }));
        }

        match buf[0] {
            b'S' => {
                #[cfg(feature = "tls")]
                {
                    let plain = match std::mem::replace(&mut self.stream, PgAsyncStream::Closed) {
                        PgAsyncStream::Plain(s) => s,
                        other => {
                            self.stream = other;
                            return Outcome::Err(connection_error(
                                "TLS upgrade requires a plain TCP stream",
                            ));
                        }
                    };

                    let tls_stream = match AsyncTlsStream::handshake(
                        plain,
                        self.config.ssl_mode,
                        &self.config.host,
                    )
                    .await
                    {
                        Ok(s) => s,
                        Err(e) => return Outcome::Err(e),
                    };

                    self.stream = PgAsyncStream::Tls(tls_stream);
                    Outcome::Ok(())
                }

                #[cfg(not(feature = "tls"))]
                {
                    Outcome::Err(connection_error(
                        "TLS requested but 'sqlmodel-postgres' was built without feature 'tls'",
                    ))
                }
            }
            b'N' => {
                if self.config.ssl_mode.is_required() {
                    Outcome::Err(Error::Connection(ConnectionError {
                        kind: ConnectionErrorKind::Ssl,
                        message: "Server does not support SSL".to_string(),
                        source: None,
                    }))
                } else {
                    Outcome::Ok(())
                }
            }
            other => Outcome::Err(Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Ssl,
                message: format!("Unexpected SSL response: 0x{other:02x}"),
                source: None,
            })),
        }
    }

    async fn send_startup(&mut self) -> Outcome<(), Error> {
        let params = self.config.startup_params();
        self.send_message_no_cx(&FrontendMessage::Startup {
            version: PROTOCOL_VERSION,
            params,
        })
        .await
    }

    fn require_auth_value(&self, message: &'static str) -> Outcome<&str, Error> {
        // NOTE: Auth values are sourced from runtime config, not hardcoded.
        match self.config.password.as_deref() {
            Some(password) => Outcome::Ok(password),
            None => Outcome::Err(auth_error(message)),
        }
    }

    async fn handle_auth(&mut self) -> Outcome<(), Error> {
        loop {
            let msg = match self.receive_message_no_cx().await {
                Outcome::Ok(m) => m,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };

            match msg {
                BackendMessage::AuthenticationOk => return Outcome::Ok(()),
                BackendMessage::AuthenticationCleartextPassword => {
                    let auth_value = match self
                        .require_auth_value("Authentication value required but not provided")
                    {
                        Outcome::Ok(password) => password,
                        Outcome::Err(e) => return Outcome::Err(e),
                        Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                        Outcome::Panicked(p) => return Outcome::Panicked(p),
                    };
                    if let Outcome::Err(e) = self
                        .send_message_no_cx(&FrontendMessage::PasswordMessage(
                            auth_value.to_string(),
                        ))
                        .await
                    {
                        return Outcome::Err(e);
                    }
                }
                BackendMessage::AuthenticationMD5Password(salt) => {
                    let auth_value = match self
                        .require_auth_value("Authentication value required but not provided")
                    {
                        Outcome::Ok(password) => password,
                        Outcome::Err(e) => return Outcome::Err(e),
                        Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                        Outcome::Panicked(p) => return Outcome::Panicked(p),
                    };
                    let hash = md5_password(&self.config.user, auth_value, salt);
                    if let Outcome::Err(e) = self
                        .send_message_no_cx(&FrontendMessage::PasswordMessage(hash))
                        .await
                    {
                        return Outcome::Err(e);
                    }
                }
                BackendMessage::AuthenticationSASL(mechanisms) => {
                    if mechanisms.contains(&"SCRAM-SHA-256".to_string()) {
                        match self.scram_auth().await {
                            Outcome::Ok(()) => {}
                            Outcome::Err(e) => return Outcome::Err(e),
                            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                            Outcome::Panicked(p) => return Outcome::Panicked(p),
                        }
                    } else {
                        return Outcome::Err(auth_error(format!(
                            "Unsupported SASL mechanisms: {:?}",
                            mechanisms
                        )));
                    }
                }
                BackendMessage::ErrorResponse(e) => {
                    self.state = ConnectionState::Error;
                    return Outcome::Err(error_from_fields(&e));
                }
                other => {
                    return Outcome::Err(protocol_error(format!(
                        "Unexpected message during auth: {other:?}"
                    )));
                }
            }
        }
    }

    async fn scram_auth(&mut self) -> Outcome<(), Error> {
        let auth_value =
            match self.require_auth_value("Authentication value required for SCRAM-SHA-256") {
                Outcome::Ok(password) => password,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };

        let mut client = ScramClient::new(&self.config.user, auth_value);

        // Client-first
        let client_first = client.client_first();
        if let Outcome::Err(e) = self
            .send_message_no_cx(&FrontendMessage::SASLInitialResponse {
                mechanism: "SCRAM-SHA-256".to_string(),
                data: client_first,
            })
            .await
        {
            return Outcome::Err(e);
        }

        // Server-first
        let msg = match self.receive_message_no_cx().await {
            Outcome::Ok(m) => m,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };
        let server_first_data = match msg {
            BackendMessage::AuthenticationSASLContinue(data) => data,
            BackendMessage::ErrorResponse(e) => {
                self.state = ConnectionState::Error;
                return Outcome::Err(error_from_fields(&e));
            }
            other => {
                return Outcome::Err(protocol_error(format!(
                    "Expected SASL continue, got: {other:?}"
                )));
            }
        };

        // Client-final
        let client_final = match client.process_server_first(&server_first_data) {
            Ok(v) => v,
            Err(e) => return Outcome::Err(e),
        };
        if let Outcome::Err(e) = self
            .send_message_no_cx(&FrontendMessage::SASLResponse(client_final))
            .await
        {
            return Outcome::Err(e);
        }

        // Server-final
        let msg = match self.receive_message_no_cx().await {
            Outcome::Ok(m) => m,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };
        let server_final_data = match msg {
            BackendMessage::AuthenticationSASLFinal(data) => data,
            BackendMessage::ErrorResponse(e) => {
                self.state = ConnectionState::Error;
                return Outcome::Err(error_from_fields(&e));
            }
            other => {
                return Outcome::Err(protocol_error(format!(
                    "Expected SASL final, got: {other:?}"
                )));
            }
        };

        if let Err(e) = client.verify_server_final(&server_final_data) {
            return Outcome::Err(e);
        }

        // Wait for AuthenticationOk
        let msg = match self.receive_message_no_cx().await {
            Outcome::Ok(m) => m,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };
        match msg {
            BackendMessage::AuthenticationOk => Outcome::Ok(()),
            BackendMessage::ErrorResponse(e) => {
                self.state = ConnectionState::Error;
                Outcome::Err(error_from_fields(&e))
            }
            other => Outcome::Err(protocol_error(format!(
                "Expected AuthenticationOk, got: {other:?}"
            ))),
        }
    }

    async fn read_startup_messages(&mut self) -> Outcome<(), Error> {
        loop {
            let msg = match self.receive_message_no_cx().await {
                Outcome::Ok(m) => m,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };

            match msg {
                BackendMessage::BackendKeyData {
                    process_id,
                    secret_key,
                } => {
                    self.process_id = process_id;
                    self.secret_key = secret_key;
                }
                BackendMessage::ParameterStatus { name, value } => {
                    self.parameters.insert(name, value);
                }
                BackendMessage::ReadyForQuery(status) => {
                    self.state = ConnectionState::Ready(TransactionStatusState::from(status));
                    return Outcome::Ok(());
                }
                BackendMessage::ErrorResponse(e) => {
                    self.state = ConnectionState::Error;
                    return Outcome::Err(error_from_fields(&e));
                }
                BackendMessage::NoticeResponse(_notice) => {}
                other => {
                    return Outcome::Err(protocol_error(format!(
                        "Unexpected startup message: {other:?}"
                    )));
                }
            }
        }
    }

    // ==================== I/O ====================

    async fn send_message(&mut self, cx: &Cx, msg: &FrontendMessage) -> Outcome<(), Error> {
        // If cancelled, propagate early.
        if let Some(reason) = cx.cancel_reason() {
            return Outcome::Cancelled(reason);
        }
        self.send_message_no_cx(msg).await
    }

    async fn receive_message(&mut self, cx: &Cx) -> Outcome<BackendMessage, Error> {
        if let Some(reason) = cx.cancel_reason() {
            return Outcome::Cancelled(reason);
        }
        self.receive_message_no_cx().await
    }

    async fn send_message_no_cx(&mut self, msg: &FrontendMessage) -> Outcome<(), Error> {
        let data = self.writer.write(msg).to_vec();

        if let Err(e) = self.stream.write_all(&data).await {
            self.state = ConnectionState::Error;
            return Outcome::Err(Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Disconnected,
                message: format!("Failed to write to server: {}", e),
                source: Some(Box::new(e)),
            }));
        }

        if let Err(e) = self.stream.flush().await {
            self.state = ConnectionState::Error;
            return Outcome::Err(Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Disconnected,
                message: format!("Failed to flush stream: {}", e),
                source: Some(Box::new(e)),
            }));
        }

        Outcome::Ok(())
    }

    async fn receive_message_no_cx(&mut self) -> Outcome<BackendMessage, Error> {
        loop {
            match self.reader.next_message() {
                Ok(Some(msg)) => return Outcome::Ok(msg),
                Ok(None) => {}
                Err(e) => {
                    self.state = ConnectionState::Error;
                    return Outcome::Err(protocol_error(format!("Protocol error: {}", e)));
                }
            }

            let n = match self.stream.read_some(&mut self.read_buf).await {
                Ok(n) => n,
                Err(e) => {
                    self.state = ConnectionState::Error;
                    return Outcome::Err(match e.kind() {
                        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => {
                            Error::Timeout
                        }
                        _ => Error::Connection(ConnectionError {
                            kind: ConnectionErrorKind::Disconnected,
                            message: format!("Failed to read from server: {}", e),
                            source: Some(Box::new(e)),
                        }),
                    });
                }
            };

            if n == 0 {
                self.state = ConnectionState::Disconnected;
                return Outcome::Err(Error::Connection(ConnectionError {
                    kind: ConnectionErrorKind::Disconnected,
                    message: "Connection closed by server".to_string(),
                    source: None,
                }));
            }

            // Only append raw bytes; let next_message() at the top of the
            // loop handle parsing.  The old code called feed() here, which
            // parsed *and consumed* all complete messages from the buffer and
            // returned them in a Vec — but the caller only checked for Err,
            // silently discarding the Ok(messages).  On the next iteration
            // next_message() would see an empty buffer and block forever on
            // the socket read.  (See issue #9.)
            self.reader.push(&self.read_buf[..n]);
        }
    }
}

/// Shared, cloneable PostgreSQL connection with interior mutability.
pub struct SharedPgConnection {
    inner: Arc<Mutex<PgAsyncConnection>>,
}

impl SharedPgConnection {
    pub fn new(conn: PgAsyncConnection) -> Self {
        Self {
            inner: Arc::new(Mutex::new(conn)),
        }
    }

    pub async fn connect(cx: &Cx, config: PgConfig) -> Outcome<Self, Error> {
        match PgAsyncConnection::connect(cx, config).await {
            Outcome::Ok(conn) => Outcome::Ok(Self::new(conn)),
            Outcome::Err(e) => Outcome::Err(e),
            Outcome::Cancelled(r) => Outcome::Cancelled(r),
            Outcome::Panicked(p) => Outcome::Panicked(p),
        }
    }

    pub fn inner(&self) -> &Arc<Mutex<PgAsyncConnection>> {
        &self.inner
    }

    async fn begin_transaction_impl(
        &self,
        cx: &Cx,
        isolation: Option<IsolationLevel>,
    ) -> Outcome<SharedPgTransaction<'_>, Error> {
        let inner = Arc::clone(&self.inner);
        let Ok(mut guard) = inner.lock(cx).await else {
            return Outcome::Err(connection_error("Failed to acquire connection lock"));
        };

        if let Some(level) = isolation {
            let sql = format!("SET TRANSACTION ISOLATION LEVEL {}", level.as_sql());
            match guard.execute_async(cx, &sql, &[]).await {
                Outcome::Ok(_) => {}
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            }
        }

        match guard.execute_async(cx, "BEGIN", &[]).await {
            Outcome::Ok(_) => {}
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        }

        drop(guard);
        Outcome::Ok(SharedPgTransaction {
            inner,
            committed: false,
            _marker: std::marker::PhantomData,
        })
    }
}

impl Clone for SharedPgConnection {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl std::fmt::Debug for SharedPgConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedPgConnection")
            .field("inner", &"Arc<Mutex<PgAsyncConnection>>")
            .finish()
    }
}

pub struct SharedPgTransaction<'conn> {
    inner: Arc<Mutex<PgAsyncConnection>>,
    committed: bool,
    _marker: std::marker::PhantomData<&'conn ()>,
}

impl<'conn> Drop for SharedPgTransaction<'conn> {
    fn drop(&mut self) {
        if !self.committed {
            // WARNING: Transaction was dropped without commit() or rollback()!
            // We cannot do async work in Drop, so the PostgreSQL transaction will
            // remain open until the connection is closed or a new transaction
            // is started.
            #[cfg(debug_assertions)]
            eprintln!(
                "WARNING: SharedPgTransaction dropped without commit/rollback. \
                 The PostgreSQL transaction may still be open."
            );
        }
    }
}

impl Connection for SharedPgConnection {
    type Tx<'conn>
        = SharedPgTransaction<'conn>
    where
        Self: 'conn;

    fn dialect(&self) -> sqlmodel_core::Dialect {
        sqlmodel_core::Dialect::Postgres
    }

    fn query(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let sql = sql.to_string();
        let params = params.to_vec();
        async move {
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            guard.query_async(cx, &sql, &params).await
        }
    }

    fn query_one(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Option<Row>, Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let sql = sql.to_string();
        let params = params.to_vec();
        async move {
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            let rows = match guard.query_async(cx, &sql, &params).await {
                Outcome::Ok(r) => r,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };
            Outcome::Ok(rows.into_iter().next())
        }
    }

    fn execute(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<u64, Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let sql = sql.to_string();
        let params = params.to_vec();
        async move {
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            guard.execute_async(cx, &sql, &params).await
        }
    }

    fn insert(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<i64, Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let sql = sql.to_string();
        let params = params.to_vec();
        async move {
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            guard.insert_async(cx, &sql, &params).await
        }
    }

    fn batch(
        &self,
        cx: &Cx,
        statements: &[(String, Vec<Value>)],
    ) -> impl Future<Output = Outcome<Vec<u64>, Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let statements = statements.to_vec();
        async move {
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            let mut results = Vec::with_capacity(statements.len());
            for (sql, params) in &statements {
                match guard.execute_async(cx, sql, params).await {
                    Outcome::Ok(n) => results.push(n),
                    Outcome::Err(e) => return Outcome::Err(e),
                    Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                    Outcome::Panicked(p) => return Outcome::Panicked(p),
                }
            }
            Outcome::Ok(results)
        }
    }

    fn begin(&self, cx: &Cx) -> impl Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
        self.begin_with(cx, IsolationLevel::default())
    }

    fn begin_with(
        &self,
        cx: &Cx,
        isolation: IsolationLevel,
    ) -> impl Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
        self.begin_transaction_impl(cx, Some(isolation))
    }

    fn prepare(
        &self,
        cx: &Cx,
        sql: &str,
    ) -> impl Future<Output = Outcome<PreparedStatement, Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let sql = sql.to_string();
        async move {
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            guard.prepare_async(cx, &sql).await
        }
    }

    fn query_prepared(
        &self,
        cx: &Cx,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let stmt = stmt.clone();
        let params = params.to_vec();
        async move {
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            guard.query_prepared_async(cx, &stmt, &params).await
        }
    }

    fn execute_prepared(
        &self,
        cx: &Cx,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> impl Future<Output = Outcome<u64, Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let stmt = stmt.clone();
        let params = params.to_vec();
        async move {
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            guard.execute_prepared_async(cx, &stmt, &params).await
        }
    }

    fn ping(&self, cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            guard.ping_async(cx).await
        }
    }

    async fn close(self, cx: &Cx) -> sqlmodel_core::Result<()> {
        let Ok(mut guard) = self.inner.lock(cx).await else {
            return Err(connection_error("Failed to acquire connection lock"));
        };
        match guard.close_async(cx).await {
            Outcome::Ok(()) => Ok(()),
            Outcome::Err(e) => Err(e),
            Outcome::Cancelled(r) => Err(Error::Query(QueryError {
                kind: QueryErrorKind::Cancelled,
                message: format!("Cancelled: {r:?}"),
                sqlstate: None,
                sql: None,
                detail: None,
                hint: None,
                position: None,
                source: None,
            })),
            Outcome::Panicked(p) => Err(Error::Protocol(ProtocolError {
                message: format!("Panicked: {p:?}"),
                raw_data: None,
                source: None,
            })),
        }
    }
}

impl<'conn> TransactionOps for SharedPgTransaction<'conn> {
    fn query(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let sql = sql.to_string();
        let params = params.to_vec();
        async move {
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            guard.query_async(cx, &sql, &params).await
        }
    }

    fn query_one(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Option<Row>, Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let sql = sql.to_string();
        let params = params.to_vec();
        async move {
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            let rows = match guard.query_async(cx, &sql, &params).await {
                Outcome::Ok(r) => r,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };
            Outcome::Ok(rows.into_iter().next())
        }
    }

    fn execute(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<u64, Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let sql = sql.to_string();
        let params = params.to_vec();
        async move {
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            guard.execute_async(cx, &sql, &params).await
        }
    }

    fn savepoint(&self, cx: &Cx, name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        async move {
            if let Err(e) = validate_savepoint_name(&name) {
                return Outcome::Err(e);
            }
            let sql = format!("SAVEPOINT {}", name);
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            guard.execute_async(cx, &sql, &[]).await.map(|_| ())
        }
    }

    fn rollback_to(&self, cx: &Cx, name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        async move {
            if let Err(e) = validate_savepoint_name(&name) {
                return Outcome::Err(e);
            }
            let sql = format!("ROLLBACK TO SAVEPOINT {}", name);
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            guard.execute_async(cx, &sql, &[]).await.map(|_| ())
        }
    }

    fn release(&self, cx: &Cx, name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        async move {
            if let Err(e) = validate_savepoint_name(&name) {
                return Outcome::Err(e);
            }
            let sql = format!("RELEASE SAVEPOINT {}", name);
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            guard.execute_async(cx, &sql, &[]).await.map(|_| ())
        }
    }

    // Note: clippy sometimes flags `self.committed = true` as unused, but Drop reads it.
    #[allow(unused_assignments)]
    fn commit(mut self, cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            let result = guard.execute_async(cx, "COMMIT", &[]).await;
            if matches!(result, Outcome::Ok(_)) {
                self.committed = true;
            }
            result.map(|_| ())
        }
    }

    #[allow(unused_assignments)]
    fn rollback(mut self, cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            let result = guard.execute_async(cx, "ROLLBACK", &[]).await;
            if matches!(result, Outcome::Ok(_)) {
                self.committed = true;
            }
            result.map(|_| ())
        }
    }
}

// ==================== Helpers ====================

struct PgQueryResult {
    rows: Vec<Row>,
    command_tag: Option<String>,
}

fn connection_error(msg: impl Into<String>) -> Error {
    Error::Connection(ConnectionError {
        kind: ConnectionErrorKind::Connect,
        message: msg.into(),
        source: None,
    })
}

fn auth_error(msg: impl Into<String>) -> Error {
    Error::Connection(ConnectionError {
        kind: ConnectionErrorKind::Authentication,
        message: msg.into(),
        source: None,
    })
}

fn protocol_error(msg: impl Into<String>) -> Error {
    Error::Protocol(ProtocolError {
        message: msg.into(),
        raw_data: None,
        source: None,
    })
}

fn query_error_msg(msg: impl Into<String>, kind: QueryErrorKind) -> Error {
    Error::Query(QueryError {
        kind,
        message: msg.into(),
        sqlstate: None,
        sql: None,
        detail: None,
        hint: None,
        position: None,
        source: None,
    })
}

fn error_from_fields(fields: &ErrorFields) -> Error {
    let kind = match fields.code.get(..2) {
        Some("08") => {
            return Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Connect,
                message: fields.message.clone(),
                source: None,
            });
        }
        Some("28") => {
            return Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Authentication,
                message: fields.message.clone(),
                source: None,
            });
        }
        Some("42") => QueryErrorKind::Syntax,
        Some("23") => QueryErrorKind::Constraint,
        Some("40") => {
            if fields.code == "40001" {
                QueryErrorKind::Serialization
            } else {
                QueryErrorKind::Deadlock
            }
        }
        Some("57") => {
            if fields.code == "57014" {
                QueryErrorKind::Cancelled
            } else {
                QueryErrorKind::Timeout
            }
        }
        _ => QueryErrorKind::Database,
    };

    Error::Query(QueryError {
        kind,
        sql: None,
        sqlstate: Some(fields.code.clone()),
        message: fields.message.clone(),
        detail: fields.detail.clone(),
        hint: fields.hint.clone(),
        position: fields.position.map(|p| p as usize),
        source: None,
    })
}

fn parse_rows_affected(tag: Option<&str>) -> Option<u64> {
    let tag = tag?;
    let mut parts = tag.split_whitespace().collect::<Vec<_>>();
    parts.pop().and_then(|last| last.parse::<u64>().ok())
}

/// Validate a savepoint name to reduce SQL injection risk.
fn validate_savepoint_name(name: &str) -> sqlmodel_core::Result<()> {
    if name.is_empty() {
        return Err(query_error_msg(
            "Savepoint name cannot be empty",
            QueryErrorKind::Syntax,
        ));
    }
    if name.len() > 63 {
        return Err(query_error_msg(
            "Savepoint name exceeds maximum length of 63 characters",
            QueryErrorKind::Syntax,
        ));
    }
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(query_error_msg(
            "Savepoint name cannot be empty",
            QueryErrorKind::Syntax,
        ));
    };
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(query_error_msg(
            "Savepoint name must start with a letter or underscore",
            QueryErrorKind::Syntax,
        ));
    }
    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return Err(query_error_msg(
                format!("Savepoint name contains invalid character: '{c}'"),
                QueryErrorKind::Syntax,
            ));
        }
    }
    Ok(())
}

fn md5_password(user: &str, password: &str, salt: [u8; 4]) -> String {
    use std::fmt::Write;

    let inner = format!("{password}{user}");
    let inner_hash = md5::compute(inner.as_bytes());

    let mut outer_input = format!("{inner_hash:x}").into_bytes();
    outer_input.extend_from_slice(&salt);
    let outer_hash = md5::compute(&outer_input);

    let mut result = String::with_capacity(35);
    result.push_str("md5");
    write!(&mut result, "{outer_hash:x}").unwrap();
    result
}

// Note: read/write helpers are implemented above on PgAsyncStream.
