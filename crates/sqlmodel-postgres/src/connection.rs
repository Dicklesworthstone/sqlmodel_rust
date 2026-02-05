//! PostgreSQL connection implementation.
//!
//! This module implements the PostgreSQL wire protocol connection,
//! including connection establishment, authentication, and state management.
//!
//! # Console Integration
//!
//! When the `console` feature is enabled, the connection can report progress
//! during connection establishment. Use the `ConsoleAware` trait to attach
//! a console for rich output.
//!
//! ```rust,ignore
//! use sqlmodel_postgres::{PgConfig, PgConnection};
//! use sqlmodel_console::{SqlModelConsole, ConsoleAware};
//! use std::sync::Arc;
//!
//! let console = Arc::new(SqlModelConsole::new());
//! let mut conn = PgConnection::connect(config)?;
//! conn.set_console(Some(console));
//! ```

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
#[cfg(feature = "console")]
use std::sync::Arc;

use sqlmodel_core::Error;
use sqlmodel_core::error::{
    ConnectionError, ConnectionErrorKind, ProtocolError, QueryError, QueryErrorKind,
};

#[cfg(feature = "console")]
use sqlmodel_console::{ConsoleAware, SqlModelConsole};

use crate::auth::ScramClient;
use crate::config::PgConfig;
use crate::protocol::{
    BackendMessage, ErrorFields, FrontendMessage, MessageReader, MessageWriter, PROTOCOL_VERSION,
    TransactionStatus,
};

/// Connection state in the PostgreSQL protocol state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected
    Disconnected,
    /// TCP connection established, sending startup
    Connecting,
    /// Performing authentication handshake
    Authenticating,
    /// Ready for queries
    Ready(TransactionStatusState),
    /// Currently executing a query
    InQuery,
    /// In a transaction block
    InTransaction(TransactionStatusState),
    /// Connection is in an error state
    Error,
    /// Connection has been closed
    Closed,
}

/// Transaction status from the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionStatusState {
    /// Not in a transaction block ('I')
    #[default]
    Idle,
    /// In a transaction block ('T')
    InTransaction,
    /// In a failed transaction block ('E')
    InFailed,
}

impl From<TransactionStatus> for TransactionStatusState {
    fn from(status: TransactionStatus) -> Self {
        match status {
            TransactionStatus::Idle => TransactionStatusState::Idle,
            TransactionStatus::Transaction => TransactionStatusState::InTransaction,
            TransactionStatus::Error => TransactionStatusState::InFailed,
        }
    }
}

/// PostgreSQL connection.
///
/// Manages a TCP connection to a PostgreSQL server, handling the wire protocol,
/// authentication, and state tracking.
///
/// # Console Support
///
/// When the `console` feature is enabled, the connection can report progress
/// via an attached `SqlModelConsole`. This provides rich feedback during
/// connection establishment and query execution.
pub struct PgConnection {
    /// TCP stream to the server
    stream: TcpStream,
    /// Current connection state
    state: ConnectionState,
    /// Backend process ID (for query cancellation)
    process_id: i32,
    /// Secret key (for query cancellation)
    secret_key: i32,
    /// Server parameters received during startup
    parameters: HashMap<String, String>,
    /// Connection configuration
    config: PgConfig,
    /// Message reader for parsing backend messages
    reader: MessageReader,
    /// Message writer for encoding frontend messages
    writer: MessageWriter,
    /// Read buffer
    read_buf: Vec<u8>,
    /// Optional console for rich output
    #[cfg(feature = "console")]
    console: Option<Arc<SqlModelConsole>>,
}

impl std::fmt::Debug for PgConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgConnection")
            .field("state", &self.state)
            .field("process_id", &self.process_id)
            .field("host", &self.config.host)
            .field("port", &self.config.port)
            .field("database", &self.config.database)
            .finish_non_exhaustive()
    }
}

impl PgConnection {
    /// Establish a new connection to the PostgreSQL server.
    ///
    /// This performs the complete connection handshake:
    /// 1. TCP connection
    /// 2. SSL negotiation (if configured)
    /// 3. Startup message
    /// 4. Authentication
    /// 5. Receive server parameters and ReadyForQuery
    #[allow(clippy::result_large_err)]
    pub fn connect(config: PgConfig) -> Result<Self, Error> {
        // 1. TCP connection with timeout
        let stream = TcpStream::connect_timeout(
            &config.socket_addr().parse().map_err(|e| {
                Error::Connection(ConnectionError {
                    kind: ConnectionErrorKind::Connect,
                    message: format!("Invalid socket address: {}", e),
                    source: None,
                })
            })?,
            config.connect_timeout,
        )
        .map_err(|e| {
            let kind = if e.kind() == std::io::ErrorKind::ConnectionRefused {
                ConnectionErrorKind::Refused
            } else {
                ConnectionErrorKind::Connect
            };
            Error::Connection(ConnectionError {
                kind,
                message: format!("Failed to connect to {}: {}", config.socket_addr(), e),
                source: Some(Box::new(e)),
            })
        })?;

        // Set TCP options
        stream.set_nodelay(true).ok();
        stream.set_read_timeout(Some(config.connect_timeout)).ok();
        stream.set_write_timeout(Some(config.connect_timeout)).ok();

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
            #[cfg(feature = "console")]
            console: None,
        };

        // 2. SSL negotiation (if configured)
        if conn.config.ssl_mode.should_try_ssl() {
            conn.negotiate_ssl()?;
        }

        // 3. Send startup message
        conn.send_startup()?;
        conn.state = ConnectionState::Authenticating;

        // 4. Handle authentication
        conn.handle_auth()?;

        // 5. Read remaining startup messages until ReadyForQuery
        conn.read_startup_messages()?;

        Ok(conn)
    }

    /// Get the current connection state.
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    /// Check if the connection is ready for queries.
    pub fn is_ready(&self) -> bool {
        matches!(self.state, ConnectionState::Ready(_))
    }

    /// Get the backend process ID (for query cancellation).
    pub fn process_id(&self) -> i32 {
        self.process_id
    }

    /// Get the secret key (for query cancellation).
    pub fn secret_key(&self) -> i32 {
        self.secret_key
    }

    /// Get a server parameter value.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(|s| s.as_str())
    }

    /// Get all server parameters.
    pub fn parameters(&self) -> &HashMap<String, String> {
        &self.parameters
    }

    /// Close the connection gracefully.
    #[allow(clippy::result_large_err)]
    pub fn close(&mut self) -> Result<(), Error> {
        if matches!(
            self.state,
            ConnectionState::Closed | ConnectionState::Disconnected
        ) {
            return Ok(());
        }

        // Send Terminate message
        self.send_message(&FrontendMessage::Terminate)?;
        self.state = ConnectionState::Closed;
        Ok(())
    }

    // ==================== SSL Negotiation ====================

    #[allow(clippy::result_large_err)]
    fn negotiate_ssl(&mut self) -> Result<(), Error> {
        // Send SSL request
        self.send_message(&FrontendMessage::SSLRequest)?;

        // Read single-byte response
        let mut buf = [0u8; 1];
        self.stream.read_exact(&mut buf).map_err(|e| {
            Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Ssl,
                message: format!("Failed to read SSL response: {}", e),
                source: Some(Box::new(e)),
            })
        })?;

        match buf[0] {
            b'S' => {
                // Server supports SSL - would need TLS handshake here
                // For now, we don't implement TLS, so error if required
                if self.config.ssl_mode.is_required() {
                    return Err(Error::Connection(ConnectionError {
                        kind: ConnectionErrorKind::Ssl,
                        message: "SSL/TLS not yet implemented".to_string(),
                        source: None,
                    }));
                }
                // If prefer mode, we'd continue without SSL
                // But we need to reconnect without SSL since we already sent SSLRequest
                Err(Error::Connection(ConnectionError {
                    kind: ConnectionErrorKind::Ssl,
                    message: "SSL/TLS not yet implemented, reconnect with ssl_mode=disable"
                        .to_string(),
                    source: None,
                }))
            }
            b'N' => {
                // Server doesn't support SSL
                if self.config.ssl_mode.is_required() {
                    return Err(Error::Connection(ConnectionError {
                        kind: ConnectionErrorKind::Ssl,
                        message: "Server does not support SSL".to_string(),
                        source: None,
                    }));
                }
                // Continue without SSL (prefer mode)
                Ok(())
            }
            _ => Err(Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Ssl,
                message: format!("Unexpected SSL response: 0x{:02x}", buf[0]),
                source: None,
            })),
        }
    }

    // ==================== Startup ====================

    #[allow(clippy::result_large_err)]
    fn send_startup(&mut self) -> Result<(), Error> {
        let params = self.config.startup_params();
        let msg = FrontendMessage::Startup {
            version: PROTOCOL_VERSION,
            params,
        };
        self.send_message(&msg)
    }

    // ==================== Authentication ====================

    #[allow(clippy::result_large_err)]
    fn require_auth_value(&self, message: &'static str) -> Result<&str, Error> {
        // NOTE: Auth values are sourced from runtime config, not hardcoded.
        self.config
            .password
            .as_deref()
            .ok_or_else(|| auth_error(message))
    }

    #[allow(clippy::result_large_err)]
    fn handle_auth(&mut self) -> Result<(), Error> {
        loop {
            let msg = self.receive_message()?;

            match msg {
                BackendMessage::AuthenticationOk => {
                    return Ok(());
                }
                BackendMessage::AuthenticationCleartextPassword => {
                    let auth_value =
                        self.require_auth_value("Authentication value required but not provided")?;
                    self.send_message(&FrontendMessage::PasswordMessage(auth_value.to_string()))?;
                }
                BackendMessage::AuthenticationMD5Password(salt) => {
                    let auth_value =
                        self.require_auth_value("Authentication value required but not provided")?;
                    let hash = md5_password(&self.config.user, auth_value, salt);
                    self.send_message(&FrontendMessage::PasswordMessage(hash))?;
                }
                BackendMessage::AuthenticationSASL(mechanisms) => {
                    if mechanisms.contains(&"SCRAM-SHA-256".to_string()) {
                        self.scram_auth()?;
                    } else {
                        return Err(auth_error(format!(
                            "Unsupported SASL mechanisms: {:?}",
                            mechanisms
                        )));
                    }
                }
                BackendMessage::ErrorResponse(e) => {
                    self.state = ConnectionState::Error;
                    return Err(error_from_fields(&e));
                }
                _ => {
                    return Err(Error::Protocol(ProtocolError {
                        message: format!("Unexpected message during auth: {:?}", msg),
                        raw_data: None,
                        source: None,
                    }));
                }
            }
        }
    }

    #[allow(clippy::result_large_err)]
    fn scram_auth(&mut self) -> Result<(), Error> {
        let auth_value =
            self.require_auth_value("Authentication value required for SCRAM-SHA-256")?;

        let mut client = ScramClient::new(&self.config.user, auth_value);

        // Send client-first message
        let client_first = client.client_first();
        self.send_message(&FrontendMessage::SASLInitialResponse {
            mechanism: "SCRAM-SHA-256".to_string(),
            data: client_first,
        })?;

        // Receive server-first
        let msg = self.receive_message()?;
        let server_first_data = match msg {
            BackendMessage::AuthenticationSASLContinue(data) => data,
            BackendMessage::ErrorResponse(e) => {
                self.state = ConnectionState::Error;
                return Err(error_from_fields(&e));
            }
            _ => {
                return Err(Error::Protocol(ProtocolError {
                    message: format!("Expected SASL continue, got: {:?}", msg),
                    raw_data: None,
                    source: None,
                }));
            }
        };

        // Generate and send client-final
        let client_final = client.process_server_first(&server_first_data)?;
        self.send_message(&FrontendMessage::SASLResponse(client_final))?;

        // Receive server-final
        let msg = self.receive_message()?;
        let server_final_data = match msg {
            BackendMessage::AuthenticationSASLFinal(data) => data,
            BackendMessage::ErrorResponse(e) => {
                self.state = ConnectionState::Error;
                return Err(error_from_fields(&e));
            }
            _ => {
                return Err(Error::Protocol(ProtocolError {
                    message: format!("Expected SASL final, got: {:?}", msg),
                    raw_data: None,
                    source: None,
                }));
            }
        };

        // Verify server signature
        client.verify_server_final(&server_final_data)?;

        // Wait for AuthenticationOk
        let msg = self.receive_message()?;
        match msg {
            BackendMessage::AuthenticationOk => Ok(()),
            BackendMessage::ErrorResponse(e) => {
                self.state = ConnectionState::Error;
                Err(error_from_fields(&e))
            }
            _ => Err(Error::Protocol(ProtocolError {
                message: format!("Expected AuthenticationOk, got: {:?}", msg),
                raw_data: None,
                source: None,
            })),
        }
    }

    // ==================== Startup Messages ====================

    #[allow(clippy::result_large_err)]
    fn read_startup_messages(&mut self) -> Result<(), Error> {
        loop {
            let msg = self.receive_message()?;

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
                    self.state = ConnectionState::Ready(status.into());
                    return Ok(());
                }
                BackendMessage::ErrorResponse(e) => {
                    self.state = ConnectionState::Error;
                    return Err(error_from_fields(&e));
                }
                BackendMessage::NoticeResponse(_notice) => {
                    // Log but continue - notices are informational
                }
                _ => {
                    return Err(Error::Protocol(ProtocolError {
                        message: format!("Unexpected startup message: {:?}", msg),
                        raw_data: None,
                        source: None,
                    }));
                }
            }
        }
    }

    // ==================== Low-Level I/O ====================

    #[allow(clippy::result_large_err)]
    fn send_message(&mut self, msg: &FrontendMessage) -> Result<(), Error> {
        let data = self.writer.write(msg);
        self.stream.write_all(data).map_err(|e| {
            self.state = ConnectionState::Error;
            Error::Io(e)
        })?;
        self.stream.flush().map_err(|e| {
            self.state = ConnectionState::Error;
            Error::Io(e)
        })?;
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn receive_message(&mut self) -> Result<BackendMessage, Error> {
        // Try to parse any complete messages from buffer first
        loop {
            match self.reader.next_message() {
                Ok(Some(msg)) => return Ok(msg),
                Ok(None) => {
                    // Need more data
                    let n = self.stream.read(&mut self.read_buf).map_err(|e| {
                        if e.kind() == std::io::ErrorKind::TimedOut
                            || e.kind() == std::io::ErrorKind::WouldBlock
                        {
                            Error::Timeout
                        } else {
                            self.state = ConnectionState::Error;
                            Error::Connection(ConnectionError {
                                kind: ConnectionErrorKind::Disconnected,
                                message: format!("Failed to read from server: {}", e),
                                source: Some(Box::new(e)),
                            })
                        }
                    })?;

                    if n == 0 {
                        self.state = ConnectionState::Disconnected;
                        return Err(Error::Connection(ConnectionError {
                            kind: ConnectionErrorKind::Disconnected,
                            message: "Connection closed by server".to_string(),
                            source: None,
                        }));
                    }

                    // Feed data to reader
                    self.reader.feed(&self.read_buf[..n]).map_err(|e| {
                        Error::Protocol(ProtocolError {
                            message: format!("Protocol error: {}", e),
                            raw_data: None,
                            source: None,
                        })
                    })?;
                }
                Err(e) => {
                    self.state = ConnectionState::Error;
                    return Err(Error::Protocol(ProtocolError {
                        message: format!("Protocol error: {}", e),
                        raw_data: None,
                        source: None,
                    }));
                }
            }
        }
    }
}

impl Drop for PgConnection {
    fn drop(&mut self) {
        // Try to close gracefully, ignore errors
        let _ = self.close();
    }
}

// ==================== Console Support ====================

#[cfg(feature = "console")]
impl ConsoleAware for PgConnection {
    fn set_console(&mut self, console: Option<Arc<SqlModelConsole>>) {
        self.console = console;
    }

    fn console(&self) -> Option<&Arc<SqlModelConsole>> {
        self.console.as_ref()
    }

    fn has_console(&self) -> bool {
        self.console.is_some()
    }
}

/// Connection progress stage for console output.
#[cfg(feature = "console")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStage {
    /// Resolving DNS
    DnsResolve,
    /// Establishing TCP connection
    TcpConnect,
    /// Negotiating SSL/TLS
    SslNegotiate,
    /// SSL/TLS established
    SslEstablished,
    /// Sending startup message
    Startup,
    /// Authenticating
    Authenticating,
    /// Authentication complete
    Authenticated,
    /// Ready for queries
    Ready,
}

#[cfg(feature = "console")]
impl ConnectionStage {
    /// Get a human-readable description of the stage.
    #[must_use]
    pub fn description(&self) -> &'static str {
        match self {
            Self::DnsResolve => "Resolving DNS",
            Self::TcpConnect => "Connecting (TCP)",
            Self::SslNegotiate => "Negotiating SSL",
            Self::SslEstablished => "SSL established",
            Self::Startup => "Sending startup",
            Self::Authenticating => "Authenticating",
            Self::Authenticated => "Authenticated",
            Self::Ready => "Ready",
        }
    }
}

#[cfg(feature = "console")]
impl PgConnection {
    /// Emit a connection progress message to the console.
    ///
    /// This is a no-op if no console is attached.
    pub fn emit_progress(&self, stage: ConnectionStage, success: bool) {
        if let Some(console) = &self.console {
            let status = if success { "[OK]" } else { "[..] " };
            let message = format!("{} {}", status, stage.description());
            console.info(&message);
        }
    }

    /// Emit a connection success message with server info.
    pub fn emit_connected(&self) {
        if let Some(console) = &self.console {
            let server_version = self
                .parameters
                .get("server_version")
                .map_or("unknown", |s| s.as_str());
            let message = format!(
                "Connected to PostgreSQL {} at {}:{}",
                server_version, self.config.host, self.config.port
            );
            console.success(&message);
        }
    }

    /// Emit a plain-text connection summary (for agent mode).
    pub fn emit_connected_plain(&self) -> String {
        let server_version = self
            .parameters
            .get("server_version")
            .map_or("unknown", |s| s.as_str());
        format!(
            "Connected to PostgreSQL {} at {}:{}",
            server_version, self.config.host, self.config.port
        )
    }
}

// ==================== Helper Functions ====================

/// Compute MD5 password hash as per PostgreSQL protocol.
fn md5_password(user: &str, password: &str, salt: [u8; 4]) -> String {
    use std::fmt::Write;

    // md5(md5(password + user) + salt)
    let inner = format!("{}{}", password, user);
    let inner_hash = md5::compute(inner.as_bytes());

    let mut outer_input = format!("{:x}", inner_hash).into_bytes();
    outer_input.extend_from_slice(&salt);
    let outer_hash = md5::compute(&outer_input);

    let mut result = String::with_capacity(35);
    result.push_str("md5");
    write!(&mut result, "{:x}", outer_hash).unwrap();
    result
}

fn auth_error(msg: impl Into<String>) -> Error {
    Error::Connection(ConnectionError {
        kind: ConnectionErrorKind::Authentication,
        message: msg.into(),
        source: None,
    })
}

fn error_from_fields(fields: &ErrorFields) -> Error {
    // Determine error kind from SQLSTATE
    let kind = match fields.code.get(..2) {
        Some("08") => {
            // Connection exception
            return Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Connect,
                message: fields.message.clone(),
                source: None,
            });
        }
        Some("28") => {
            // Invalid authorization specification
            return Error::Connection(ConnectionError {
                kind: ConnectionErrorKind::Authentication,
                message: fields.message.clone(),
                source: None,
            });
        }
        Some("42") => QueryErrorKind::Syntax, // Syntax error or access rule violation
        Some("23") => QueryErrorKind::Constraint, // Integrity constraint violation
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_md5_password() {
        // Example from PostgreSQL documentation
        let hash = md5_password("postgres", "mysecretpassword", *b"abcd");
        assert!(hash.starts_with("md5"));
        assert_eq!(hash.len(), 35); // "md5" + 32 hex chars
    }

    #[test]
    fn test_transaction_status_conversion() {
        assert_eq!(
            TransactionStatusState::from(TransactionStatus::Idle),
            TransactionStatusState::Idle
        );
        assert_eq!(
            TransactionStatusState::from(TransactionStatus::Transaction),
            TransactionStatusState::InTransaction
        );
        assert_eq!(
            TransactionStatusState::from(TransactionStatus::Error),
            TransactionStatusState::InFailed
        );
    }

    #[test]
    fn test_error_classification() {
        let fields = ErrorFields {
            severity: "ERROR".to_string(),
            code: "23505".to_string(),
            message: "unique violation".to_string(),
            ..Default::default()
        };
        let err = error_from_fields(&fields);
        assert!(matches!(err, Error::Query(q) if q.kind == QueryErrorKind::Constraint));

        let fields = ErrorFields {
            severity: "FATAL".to_string(),
            code: "28P01".to_string(),
            message: "password authentication failed".to_string(),
            ..Default::default()
        };
        let err = error_from_fields(&fields);
        assert!(matches!(
            err,
            Error::Connection(c) if c.kind == ConnectionErrorKind::Authentication
        ));
    }
}
