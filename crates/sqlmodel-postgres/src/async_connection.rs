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
use crate::config::PgConfig;
use crate::connection::{ConnectionState, TransactionStatusState};
use crate::protocol::{
    BackendMessage, DescribeKind, ErrorFields, FrontendMessage, MessageReader, MessageWriter,
    PROTOCOL_VERSION,
};
use crate::types::{Format, decode_value, encode_value};

/// Async PostgreSQL connection.
///
/// This connection uses asupersync's TCP stream for non-blocking I/O and
/// supports the extended query protocol for parameter binding.
pub struct PgAsyncConnection {
    stream: TcpStream,
    state: ConnectionState,
    process_id: i32,
    secret_key: i32,
    parameters: HashMap<String, String>,
    config: PgConfig,
    reader: MessageReader,
    writer: MessageWriter,
    read_buf: Vec<u8>,
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
            stream,
            state: ConnectionState::Connecting,
            process_id: 0,
            secret_key: 0,
            parameters: HashMap::new(),
            config,
            reader: MessageReader::new(),
            writer: MessageWriter::new(),
            read_buf: vec![0u8; 8192],
        };

        // SSL negotiation (TLS not implemented; matches sync driver behavior)
        if conn.config.ssl_mode.should_try_ssl() {
            match conn.negotiate_ssl().await {
                Outcome::Ok(()) => {}
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
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
        let _ = self.send_message(cx, &FrontendMessage::Terminate).await;
        self.state = ConnectionState::Closed;
        Outcome::Ok(())
    }

    // ==================== Protocol: extended query ====================

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

    // ==================== Startup + auth ====================

    async fn negotiate_ssl(&mut self) -> Outcome<(), Error> {
        // Send SSL request
        if let Outcome::Err(e) = self.send_message_no_cx(&FrontendMessage::SSLRequest).await {
            return Outcome::Err(e);
        }

        // Read single-byte response
        let mut buf = [0u8; 1];
        match read_exact_async(&mut self.stream, &mut buf).await {
            Ok(()) => {}
            Err(e) => {
                return Outcome::Err(Error::Connection(ConnectionError {
                    kind: ConnectionErrorKind::Ssl,
                    message: format!("Failed to read SSL response: {}", e),
                    source: Some(Box::new(e)),
                }));
            }
        }

        match buf[0] {
            b'S' => {
                // Server supports SSL but TLS handshake is not implemented.
                if self.config.ssl_mode.is_required() {
                    Outcome::Err(Error::Connection(ConnectionError {
                        kind: ConnectionErrorKind::Ssl,
                        message: "SSL/TLS not yet implemented".to_string(),
                        source: None,
                    }))
                } else {
                    Outcome::Err(Error::Connection(ConnectionError {
                        kind: ConnectionErrorKind::Ssl,
                        message: "SSL/TLS not yet implemented, reconnect with ssl_mode=disable"
                            .to_string(),
                        source: None,
                    }))
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

        let mut written = 0;
        while written < data.len() {
            match std::future::poll_fn(|cx| {
                std::pin::Pin::new(&mut self.stream).poll_write(cx, &data[written..])
            })
            .await
            {
                Ok(n) => {
                    if n == 0 {
                        self.state = ConnectionState::Error;
                        return Outcome::Err(Error::Connection(ConnectionError {
                            kind: ConnectionErrorKind::Disconnected,
                            message: "Connection closed while writing".to_string(),
                            source: None,
                        }));
                    }
                    written += n;
                }
                Err(e) => {
                    self.state = ConnectionState::Error;
                    return Outcome::Err(Error::Connection(ConnectionError {
                        kind: ConnectionErrorKind::Disconnected,
                        message: format!("Failed to write to server: {}", e),
                        source: Some(Box::new(e)),
                    }));
                }
            }
        }

        match std::future::poll_fn(|cx| std::pin::Pin::new(&mut self.stream).poll_flush(cx)).await {
            Ok(()) => Outcome::Ok(()),
            Err(e) => {
                self.state = ConnectionState::Error;
                Outcome::Err(Error::Connection(ConnectionError {
                    kind: ConnectionErrorKind::Disconnected,
                    message: format!("Failed to flush stream: {}", e),
                    source: Some(Box::new(e)),
                }))
            }
        }
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

            let mut read_buf = ReadBuf::new(&mut self.read_buf);
            match std::future::poll_fn(|cx| {
                std::pin::Pin::new(&mut self.stream).poll_read(cx, &mut read_buf)
            })
            .await
            {
                Ok(()) => {
                    let n = read_buf.filled().len();
                    if n == 0 {
                        self.state = ConnectionState::Disconnected;
                        return Outcome::Err(Error::Connection(ConnectionError {
                            kind: ConnectionErrorKind::Disconnected,
                            message: "Connection closed by server".to_string(),
                            source: None,
                        }));
                    }
                    if let Err(e) = self.reader.feed(read_buf.filled()) {
                        self.state = ConnectionState::Error;
                        return Outcome::Err(protocol_error(format!("Protocol error: {}", e)));
                    }
                }
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
            }
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
        _cx: &Cx,
        sql: &str,
    ) -> impl Future<Output = Outcome<PreparedStatement, Error>> + Send {
        let sql = sql.to_string();
        async move {
            // Note: Client-side prepared statement stub. Server-side prepared statements
            // (PostgreSQL PREPARE/EXECUTE) can be added later for performance optimization.
            // Current implementation passes through to regular query execution.
            Outcome::Ok(PreparedStatement::new(0, sql, 0))
        }
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

async fn read_exact_async(stream: &mut TcpStream, buf: &mut [u8]) -> std::io::Result<()> {
    let mut read = 0;
    while read < buf.len() {
        let mut read_buf = ReadBuf::new(&mut buf[read..]);
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut *stream).poll_read(cx, &mut read_buf))
            .await?;
        let n = read_buf.filled().len();
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
