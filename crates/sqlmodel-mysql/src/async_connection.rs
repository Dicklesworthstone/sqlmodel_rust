//! Async MySQL connection implementation.
//!
//! This module implements the async MySQL connection using asupersync's TCP primitives.
//! It provides the `Connection` trait implementation for integration with sqlmodel-core.

use std::future::Future;
use std::io::{self, Read as StdRead, Write as StdWrite};
use std::net::TcpStream as StdTcpStream;
#[cfg(feature = "console")]
use std::sync::Arc;

use asupersync::io::{AsyncRead, AsyncWrite, ReadBuf};
use asupersync::net::TcpStream;
use asupersync::{Cx, Outcome};

use sqlmodel_core::connection::{Connection, IsolationLevel, PreparedStatement, TransactionOps};
use sqlmodel_core::error::{
    ConnectionError, ConnectionErrorKind, ProtocolError, QueryError, QueryErrorKind,
};
use sqlmodel_core::{Error, Row, Value};

#[cfg(feature = "console")]
use sqlmodel_console::{ConsoleAware, SqlModelConsole};

use crate::auth;
use crate::config::MySqlConfig;
use crate::connection::{ConnectionState, ServerCapabilities};
use crate::protocol::{
    Command, ErrPacket, PacketHeader, PacketReader, PacketType, PacketWriter, capabilities,
    charset, MAX_PACKET_SIZE,
};
use crate::types::{ColumnDef, FieldType, decode_text_value, interpolate_params};

/// Async MySQL connection.
///
/// This connection uses asupersync's TCP stream for non-blocking I/O
/// and implements the `Connection` trait from sqlmodel-core.
pub struct MySqlAsyncConnection {
    /// TCP stream (either sync for compatibility or async wrapper)
    stream: ConnectionStream,
    /// Current connection state
    state: ConnectionState,
    /// Server capabilities from handshake
    server_caps: Option<ServerCapabilities>,
    /// Connection ID
    connection_id: u32,
    /// Server status flags
    status_flags: u16,
    /// Affected rows from last statement
    affected_rows: u64,
    /// Last insert ID
    last_insert_id: u64,
    /// Number of warnings
    warnings: u16,
    /// Connection configuration
    config: MySqlConfig,
    /// Current sequence ID for packet framing
    sequence_id: u8,
    /// Optional console for rich output
    #[cfg(feature = "console")]
    console: Option<Arc<SqlModelConsole>>,
}

/// Connection stream wrapper for sync/async compatibility.
enum ConnectionStream {
    /// Standard sync TCP stream (for initial connection)
    Sync(StdTcpStream),
    /// Async TCP stream (for async operations)
    Async(TcpStream),
}

impl std::fmt::Debug for MySqlAsyncConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySqlAsyncConnection")
            .field("state", &self.state)
            .field("connection_id", &self.connection_id)
            .field("host", &self.config.host)
            .field("port", &self.config.port)
            .field("database", &self.config.database)
            .finish_non_exhaustive()
    }
}

impl MySqlAsyncConnection {
    /// Establish a new async connection to the MySQL server.
    ///
    /// This performs the complete connection handshake asynchronously:
    /// 1. TCP connection
    /// 2. Receive server handshake
    /// 3. Send handshake response with authentication
    /// 4. Handle auth result (possibly auth switch)
    pub async fn connect(_cx: &Cx, config: MySqlConfig) -> Outcome<Self, Error> {
        // Use async TCP connect
        let addr = config.socket_addr();
        let stream = match TcpStream::connect_timeout(
            addr.parse().map_err(|e| {
                Error::Connection(ConnectionError {
                    kind: ConnectionErrorKind::Connect,
                    message: format!("Invalid socket address: {}", e),
                    source: None,
                })
            })?,
            config.connect_timeout,
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                let kind = if e.kind() == io::ErrorKind::ConnectionRefused {
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

        // Set TCP options
        stream.set_nodelay(true).ok();

        let mut conn = Self {
            stream: ConnectionStream::Async(stream),
            state: ConnectionState::Connecting,
            server_caps: None,
            connection_id: 0,
            status_flags: 0,
            affected_rows: 0,
            last_insert_id: 0,
            warnings: 0,
            config,
            sequence_id: 0,
            #[cfg(feature = "console")]
            console: None,
        };

        // 2. Receive server handshake
        match conn.read_handshake_async().await {
            Outcome::Ok(server_caps) => {
                conn.connection_id = server_caps.connection_id;
                conn.server_caps = Some(server_caps);
                conn.state = ConnectionState::Authenticating;
            }
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        }

        // 3. Send handshake response
        if let Outcome::Err(e) = conn.send_handshake_response_async().await {
            return Outcome::Err(e);
        }

        // 4. Handle authentication result
        if let Outcome::Err(e) = conn.handle_auth_result_async().await {
            return Outcome::Err(e);
        }

        conn.state = ConnectionState::Ready;
        Outcome::Ok(conn)
    }

    /// Get the current connection state.
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    /// Check if the connection is ready for queries.
    pub fn is_ready(&self) -> bool {
        matches!(self.state, ConnectionState::Ready)
    }

    /// Get the connection ID.
    pub fn connection_id(&self) -> u32 {
        self.connection_id
    }

    /// Get the server version.
    pub fn server_version(&self) -> Option<&str> {
        self.server_caps
            .as_ref()
            .map(|caps| caps.server_version.as_str())
    }

    /// Get the number of affected rows from the last statement.
    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    /// Get the last insert ID.
    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }

    // === Async I/O methods ===

    /// Read a complete packet from the stream asynchronously.
    async fn read_packet_async(&mut self) -> Outcome<(Vec<u8>, u8), Error> {
        // Read header (4 bytes)
        let mut header_buf = [0u8; 4];

        match &mut self.stream {
            ConnectionStream::Async(stream) => {
                let mut read_buf = ReadBuf::new(&mut header_buf);
                // Use poll-based reading with async
                match std::future::poll_fn(|cx| {
                    std::pin::Pin::new(&mut *stream).poll_read(cx, &mut read_buf)
                })
                .await
                {
                    Ok(()) if read_buf.filled().len() == 4 => {}
                    Ok(()) => {
                        return Outcome::Err(Error::Connection(ConnectionError {
                            kind: ConnectionErrorKind::Disconnected,
                            message: "Connection closed while reading header".to_string(),
                            source: None,
                        }));
                    }
                    Err(e) => {
                        return Outcome::Err(Error::Connection(ConnectionError {
                            kind: ConnectionErrorKind::Disconnected,
                            message: format!("Failed to read packet header: {}", e),
                            source: Some(Box::new(e)),
                        }));
                    }
                }
            }
            ConnectionStream::Sync(stream) => {
                if let Err(e) = stream.read_exact(&mut header_buf) {
                    return Outcome::Err(Error::Connection(ConnectionError {
                        kind: ConnectionErrorKind::Disconnected,
                        message: format!("Failed to read packet header: {}", e),
                        source: Some(Box::new(e)),
                    }));
                }
            }
        }

        let header = PacketHeader::from_bytes(&header_buf);
        let payload_len = header.payload_length as usize;
        self.sequence_id = header.sequence_id.wrapping_add(1);

        // Read payload
        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            match &mut self.stream {
                ConnectionStream::Async(stream) => {
                    let mut total_read = 0;
                    while total_read < payload_len {
                        let mut read_buf = ReadBuf::new(&mut payload[total_read..]);
                        match std::future::poll_fn(|cx| {
                            std::pin::Pin::new(&mut *stream).poll_read(cx, &mut read_buf)
                        })
                        .await
                        {
                            Ok(()) => {
                                let n = read_buf.filled().len();
                                if n == 0 {
                                    return Outcome::Err(Error::Connection(ConnectionError {
                                        kind: ConnectionErrorKind::Disconnected,
                                        message: "Connection closed while reading payload"
                                            .to_string(),
                                        source: None,
                                    }));
                                }
                                total_read += n;
                            }
                            Err(e) => {
                                return Outcome::Err(Error::Connection(ConnectionError {
                                    kind: ConnectionErrorKind::Disconnected,
                                    message: format!("Failed to read packet payload: {}", e),
                                    source: Some(Box::new(e)),
                                }));
                            }
                        }
                    }
                }
                ConnectionStream::Sync(stream) => {
                    if let Err(e) = stream.read_exact(&mut payload) {
                        return Outcome::Err(Error::Connection(ConnectionError {
                            kind: ConnectionErrorKind::Disconnected,
                            message: format!("Failed to read packet payload: {}", e),
                            source: Some(Box::new(e)),
                        }));
                    }
                }
            }
        }

        // Handle multi-packet payloads
        if payload_len == MAX_PACKET_SIZE {
            loop {
                let mut header_buf = [0u8; 4];
                match &mut self.stream {
                    ConnectionStream::Async(stream) => {
                        let mut read_buf = ReadBuf::new(&mut header_buf);
                        if let Err(e) = std::future::poll_fn(|cx| {
                            std::pin::Pin::new(&mut *stream).poll_read(cx, &mut read_buf)
                        })
                        .await
                        {
                            return Outcome::Err(Error::Connection(ConnectionError {
                                kind: ConnectionErrorKind::Disconnected,
                                message: format!("Failed to read continuation header: {}", e),
                                source: Some(Box::new(e)),
                            }));
                        }
                    }
                    ConnectionStream::Sync(stream) => {
                        if let Err(e) = stream.read_exact(&mut header_buf) {
                            return Outcome::Err(Error::Connection(ConnectionError {
                                kind: ConnectionErrorKind::Disconnected,
                                message: format!("Failed to read continuation header: {}", e),
                                source: Some(Box::new(e)),
                            }));
                        }
                    }
                }

                let cont_header = PacketHeader::from_bytes(&header_buf);
                let cont_len = cont_header.payload_length as usize;
                self.sequence_id = cont_header.sequence_id.wrapping_add(1);

                if cont_len > 0 {
                    let mut cont_payload = vec![0u8; cont_len];
                    match &mut self.stream {
                        ConnectionStream::Async(stream) => {
                            let mut total_read = 0;
                            while total_read < cont_len {
                                let mut read_buf = ReadBuf::new(&mut cont_payload[total_read..]);
                                match std::future::poll_fn(|cx| {
                                    std::pin::Pin::new(&mut *stream).poll_read(cx, &mut read_buf)
                                })
                                .await
                                {
                                    Ok(()) => {
                                        let n = read_buf.filled().len();
                                        if n == 0 {
                                            break;
                                        }
                                        total_read += n;
                                    }
                                    Err(e) => {
                                        return Outcome::Err(Error::Connection(ConnectionError {
                                            kind: ConnectionErrorKind::Disconnected,
                                            message: format!(
                                                "Failed to read continuation payload: {}",
                                                e
                                            ),
                                            source: Some(Box::new(e)),
                                        }));
                                    }
                                }
                            }
                        }
                        ConnectionStream::Sync(stream) => {
                            if let Err(e) = stream.read_exact(&mut cont_payload) {
                                return Outcome::Err(Error::Connection(ConnectionError {
                                    kind: ConnectionErrorKind::Disconnected,
                                    message: format!("Failed to read continuation payload: {}", e),
                                    source: Some(Box::new(e)),
                                }));
                            }
                        }
                    }
                    payload.extend_from_slice(&cont_payload);
                }

                if cont_len < MAX_PACKET_SIZE {
                    break;
                }
            }
        }

        Outcome::Ok((payload, header.sequence_id))
    }

    /// Write a packet to the stream asynchronously.
    async fn write_packet_async(&mut self, payload: &[u8]) -> Outcome<(), Error> {
        let writer = PacketWriter::new();
        let packet = writer.build_packet_from_payload(payload, self.sequence_id);
        self.sequence_id = self.sequence_id.wrapping_add(1);

        match &mut self.stream {
            ConnectionStream::Async(stream) => {
                match std::future::poll_fn(|cx| {
                    std::pin::Pin::new(&mut *stream).poll_write(cx, &packet)
                })
                .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        return Outcome::Err(Error::Connection(ConnectionError {
                            kind: ConnectionErrorKind::Disconnected,
                            message: format!("Failed to write packet: {}", e),
                            source: Some(Box::new(e)),
                        }));
                    }
                }

                match std::future::poll_fn(|cx| std::pin::Pin::new(&mut *stream).poll_flush(cx))
                    .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        return Outcome::Err(Error::Connection(ConnectionError {
                            kind: ConnectionErrorKind::Disconnected,
                            message: format!("Failed to flush stream: {}", e),
                            source: Some(Box::new(e)),
                        }));
                    }
                }
            }
            ConnectionStream::Sync(stream) => {
                if let Err(e) = stream.write_all(&packet) {
                    return Outcome::Err(Error::Connection(ConnectionError {
                        kind: ConnectionErrorKind::Disconnected,
                        message: format!("Failed to write packet: {}", e),
                        source: Some(Box::new(e)),
                    }));
                }
                if let Err(e) = stream.flush() {
                    return Outcome::Err(Error::Connection(ConnectionError {
                        kind: ConnectionErrorKind::Disconnected,
                        message: format!("Failed to flush stream: {}", e),
                        source: Some(Box::new(e)),
                    }));
                }
            }
        }

        Outcome::Ok(())
    }

    // === Handshake methods ===

    /// Read the server handshake packet asynchronously.
    async fn read_handshake_async(&mut self) -> Outcome<ServerCapabilities, Error> {
        let (payload, _) = match self.read_packet_async().await {
            Outcome::Ok(p) => p,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let mut reader = PacketReader::new(&payload);

        // Protocol version
        let protocol_version = match reader.read_u8() {
            Some(v) => v,
            None => return Outcome::Err(protocol_error("Missing protocol version")),
        };

        if protocol_version != 10 {
            return Outcome::Err(protocol_error(format!(
                "Unsupported protocol version: {}",
                protocol_version
            )));
        }

        // Server version (null-terminated string)
        let server_version = match reader.read_null_string() {
            Some(v) => v,
            None => return Outcome::Err(protocol_error("Missing server version")),
        };

        // Connection ID
        let connection_id = match reader.read_u32_le() {
            Some(v) => v,
            None => return Outcome::Err(protocol_error("Missing connection ID")),
        };

        // Auth plugin data part 1 (8 bytes)
        let auth_data_1 = match reader.read_bytes(8) {
            Some(v) => v,
            None => return Outcome::Err(protocol_error("Missing auth data")),
        };

        // Filler (1 byte)
        reader.skip(1);

        // Capability flags (lower 2 bytes)
        let caps_lower = match reader.read_u16_le() {
            Some(v) => v,
            None => return Outcome::Err(protocol_error("Missing capability flags")),
        };

        // Character set
        let charset_val = reader.read_u8().unwrap_or(charset::UTF8MB4_0900_AI_CI);

        // Status flags
        let status_flags = reader.read_u16_le().unwrap_or(0);

        // Capability flags (upper 2 bytes)
        let caps_upper = reader.read_u16_le().unwrap_or(0);
        let capabilities_val = u32::from(caps_lower) | (u32::from(caps_upper) << 16);

        // Length of auth-plugin-data (if CLIENT_PLUGIN_AUTH)
        let auth_data_len = if capabilities_val & capabilities::CLIENT_PLUGIN_AUTH != 0 {
            reader.read_u8().unwrap_or(0) as usize
        } else {
            0
        };

        // Reserved (10 bytes)
        reader.skip(10);

        // Auth plugin data part 2 (if CLIENT_SECURE_CONNECTION)
        let mut auth_data = auth_data_1.to_vec();
        if capabilities_val & capabilities::CLIENT_SECURE_CONNECTION != 0 {
            let len2 = if auth_data_len > 8 {
                auth_data_len - 8
            } else {
                13 // Default length
            };
            if let Some(data2) = reader.read_bytes(len2) {
                // Remove trailing NUL if present
                let data2_clean = if data2.last() == Some(&0) {
                    &data2[..data2.len() - 1]
                } else {
                    data2
                };
                auth_data.extend_from_slice(data2_clean);
            }
        }

        // Auth plugin name (if CLIENT_PLUGIN_AUTH)
        let auth_plugin = if capabilities_val & capabilities::CLIENT_PLUGIN_AUTH != 0 {
            reader.read_null_string().unwrap_or_default()
        } else {
            auth::plugins::MYSQL_NATIVE_PASSWORD.to_string()
        };

        Outcome::Ok(ServerCapabilities {
            capabilities: capabilities_val,
            protocol_version,
            server_version,
            connection_id,
            auth_plugin,
            auth_data,
            charset: charset_val,
            status_flags,
        })
    }

    /// Send the handshake response packet asynchronously.
    async fn send_handshake_response_async(&mut self) -> Outcome<(), Error> {
        let server_caps = match self.server_caps.as_ref() {
            Some(c) => c,
            None => return Outcome::Err(protocol_error("No server handshake received")),
        };

        // Determine client capabilities
        let client_caps = self.config.capability_flags() & server_caps.capabilities;

        // Build authentication response
        let auth_response =
            self.compute_auth_response(&server_caps.auth_plugin, &server_caps.auth_data);

        let mut writer = PacketWriter::new();

        // Client capability flags (4 bytes)
        writer.write_u32_le(client_caps);

        // Max packet size (4 bytes)
        writer.write_u32_le(self.config.max_packet_size);

        // Character set (1 byte)
        writer.write_u8(self.config.charset);

        // Reserved (23 bytes of zeros)
        writer.write_zeros(23);

        // Username (null-terminated)
        writer.write_null_string(&self.config.user);

        // Auth response
        if client_caps & capabilities::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
            writer.write_lenenc_bytes(&auth_response);
        } else if client_caps & capabilities::CLIENT_SECURE_CONNECTION != 0 {
            #[allow(clippy::cast_possible_truncation)]
            writer.write_u8(auth_response.len() as u8);
            writer.write_bytes(&auth_response);
        } else {
            writer.write_bytes(&auth_response);
            writer.write_u8(0); // Null terminator
        }

        // Database (if CLIENT_CONNECT_WITH_DB)
        if client_caps & capabilities::CLIENT_CONNECT_WITH_DB != 0 {
            if let Some(ref db) = self.config.database {
                writer.write_null_string(db);
            } else {
                writer.write_u8(0); // Empty string
            }
        }

        // Auth plugin name (if CLIENT_PLUGIN_AUTH)
        if client_caps & capabilities::CLIENT_PLUGIN_AUTH != 0 {
            writer.write_null_string(&server_caps.auth_plugin);
        }

        // Connection attributes (if CLIENT_CONNECT_ATTRS)
        if client_caps & capabilities::CLIENT_CONNECT_ATTRS != 0
            && !self.config.attributes.is_empty()
        {
            let mut attrs_writer = PacketWriter::new();
            for (key, value) in &self.config.attributes {
                attrs_writer.write_lenenc_string(key);
                attrs_writer.write_lenenc_string(value);
            }
            let attrs_data = attrs_writer.into_bytes();
            writer.write_lenenc_bytes(&attrs_data);
        }

        self.write_packet_async(writer.as_bytes()).await
    }

    /// Compute authentication response based on the plugin.
    fn compute_auth_response(&self, plugin: &str, auth_data: &[u8]) -> Vec<u8> {
        let password = self.config.password.as_deref().unwrap_or("");

        match plugin {
            auth::plugins::MYSQL_NATIVE_PASSWORD => {
                auth::mysql_native_password(password, auth_data)
            }
            auth::plugins::CACHING_SHA2_PASSWORD => {
                auth::caching_sha2_password(password, auth_data)
            }
            auth::plugins::MYSQL_CLEAR_PASSWORD => {
                let mut result = password.as_bytes().to_vec();
                result.push(0);
                result
            }
            _ => auth::mysql_native_password(password, auth_data),
        }
    }

    /// Handle authentication result asynchronously.
    async fn handle_auth_result_async(&mut self) -> Outcome<(), Error> {
        let (payload, _) = match self.read_packet_async().await {
            Outcome::Ok(p) => p,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        if payload.is_empty() {
            return Outcome::Err(protocol_error("Empty authentication response"));
        }

        match PacketType::from_first_byte(payload[0], payload.len() as u32) {
            PacketType::Ok => {
                let mut reader = PacketReader::new(&payload);
                if let Some(ok) = reader.parse_ok_packet() {
                    self.status_flags = ok.status_flags;
                    self.affected_rows = ok.affected_rows;
                }
                Outcome::Ok(())
            }
            PacketType::Error => {
                let mut reader = PacketReader::new(&payload);
                let err = match reader.parse_err_packet() {
                    Some(e) => e,
                    None => return Outcome::Err(protocol_error("Invalid error packet")),
                };
                Outcome::Err(auth_error(format!(
                    "Authentication failed: {} ({})",
                    err.error_message, err.error_code
                )))
            }
            PacketType::Eof => {
                // Auth switch request
                self.handle_auth_switch_async(&payload[1..]).await
            }
            _ => self.handle_additional_auth_async(&payload).await,
        }
    }

    /// Handle auth switch request asynchronously.
    async fn handle_auth_switch_async(&mut self, data: &[u8]) -> Outcome<(), Error> {
        let mut reader = PacketReader::new(data);

        let plugin = match reader.read_null_string() {
            Some(p) => p,
            None => return Outcome::Err(protocol_error("Missing plugin name in auth switch")),
        };

        let auth_data = reader.read_rest();
        let response = self.compute_auth_response(&plugin, auth_data);

        if let Outcome::Err(e) = self.write_packet_async(&response).await {
            return Outcome::Err(e);
        }

        self.handle_auth_result_async().await
    }

    /// Handle additional auth data asynchronously.
    async fn handle_additional_auth_async(&mut self, data: &[u8]) -> Outcome<(), Error> {
        if data.is_empty() {
            return Outcome::Err(protocol_error("Empty additional auth data"));
        }

        match data[0] {
            auth::caching_sha2::FAST_AUTH_SUCCESS => {
                let (payload, _) = match self.read_packet_async().await {
                    Outcome::Ok(p) => p,
                    Outcome::Err(e) => return Outcome::Err(e),
                    Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                    Outcome::Panicked(p) => return Outcome::Panicked(p),
                };
                let mut reader = PacketReader::new(&payload);
                if let Some(ok) = reader.parse_ok_packet() {
                    self.status_flags = ok.status_flags;
                }
                Outcome::Ok(())
            }
            auth::caching_sha2::PERFORM_FULL_AUTH => Outcome::Err(auth_error(
                "Full authentication required - please use TLS connection",
            )),
            _ => {
                let mut reader = PacketReader::new(data);
                if let Some(ok) = reader.parse_ok_packet() {
                    self.status_flags = ok.status_flags;
                    Outcome::Ok(())
                } else {
                    Outcome::Err(protocol_error(format!(
                        "Unknown auth response: {:02X}",
                        data[0]
                    )))
                }
            }
        }
    }

    /// Execute a text protocol query asynchronously.
    pub async fn query_async(
        &mut self,
        _cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> Outcome<Vec<Row>, Error> {
        let sql = interpolate_params(sql, params);
        if !self.is_ready() && self.state != ConnectionState::InTransaction {
            return Outcome::Err(connection_error("Connection not ready for queries"));
        }

        self.state = ConnectionState::InQuery;
        self.sequence_id = 0;

        // Send COM_QUERY
        let mut writer = PacketWriter::new();
        writer.write_u8(Command::Query as u8);
        writer.write_bytes(sql.as_bytes());

        if let Outcome::Err(e) = self.write_packet_async(writer.as_bytes()).await {
            return Outcome::Err(e);
        }

        // Read response
        let (payload, _) = match self.read_packet_async().await {
            Outcome::Ok(p) => p,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        if payload.is_empty() {
            self.state = ConnectionState::Ready;
            return Outcome::Err(protocol_error("Empty query response"));
        }

        match PacketType::from_first_byte(payload[0], payload.len() as u32) {
            PacketType::Ok => {
                let mut reader = PacketReader::new(&payload);
                if let Some(ok) = reader.parse_ok_packet() {
                    self.affected_rows = ok.affected_rows;
                    self.last_insert_id = ok.last_insert_id;
                    self.status_flags = ok.status_flags;
                    self.warnings = ok.warnings;
                }
                self.state = if self.status_flags
                    & crate::protocol::server_status::SERVER_STATUS_IN_TRANS
                    != 0
                {
                    ConnectionState::InTransaction
                } else {
                    ConnectionState::Ready
                };
                Outcome::Ok(vec![])
            }
            PacketType::Error => {
                self.state = ConnectionState::Ready;
                let mut reader = PacketReader::new(&payload);
                let err = match reader.parse_err_packet() {
                    Some(e) => e,
                    None => return Outcome::Err(protocol_error("Invalid error packet")),
                };
                Outcome::Err(query_error(&err))
            }
            PacketType::LocalInfile => {
                self.state = ConnectionState::Ready;
                Outcome::Err(query_error_msg("LOCAL INFILE not supported"))
            }
            _ => self.read_result_set_async(&payload).await,
        }
    }

    /// Read a result set asynchronously.
    async fn read_result_set_async(&mut self, first_packet: &[u8]) -> Outcome<Vec<Row>, Error> {
        let mut reader = PacketReader::new(first_packet);
        let column_count = match reader.read_lenenc_int() {
            Some(c) => c as usize,
            None => return Outcome::Err(protocol_error("Invalid column count")),
        };

        // Read column definitions
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let (payload, _) = match self.read_packet_async().await {
                Outcome::Ok(p) => p,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };
            match self.parse_column_def(&payload) {
                Ok(col) => columns.push(col),
                Err(e) => return Outcome::Err(e),
            }
        }

        // Check for EOF packet
        let server_caps = self.server_caps.as_ref().map_or(0, |c| c.capabilities);
        if server_caps & capabilities::CLIENT_DEPRECATE_EOF == 0 {
            let (payload, _) = match self.read_packet_async().await {
                Outcome::Ok(p) => p,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };
            if payload.first() == Some(&0xFE) {
                // EOF packet - continue to rows
            }
        }

        // Read rows until EOF or OK
        let mut rows = Vec::new();
        loop {
            let (payload, _) = match self.read_packet_async().await {
                Outcome::Ok(p) => p,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };

            if payload.is_empty() {
                break;
            }

            match PacketType::from_first_byte(payload[0], payload.len() as u32) {
                PacketType::Eof | PacketType::Ok => {
                    let mut reader = PacketReader::new(&payload);
                    if payload[0] == 0x00 {
                        if let Some(ok) = reader.parse_ok_packet() {
                            self.status_flags = ok.status_flags;
                            self.warnings = ok.warnings;
                        }
                    } else if payload[0] == 0xFE {
                        if let Some(eof) = reader.parse_eof_packet() {
                            self.status_flags = eof.status_flags;
                            self.warnings = eof.warnings;
                        }
                    }
                    break;
                }
                PacketType::Error => {
                    let mut reader = PacketReader::new(&payload);
                    let err = match reader.parse_err_packet() {
                        Some(e) => e,
                        None => return Outcome::Err(protocol_error("Invalid error packet")),
                    };
                    self.state = ConnectionState::Ready;
                    return Outcome::Err(query_error(&err));
                }
                _ => {
                    let row = self.parse_text_row(&payload, &columns);
                    rows.push(row);
                }
            }
        }

        self.state =
            if self.status_flags & crate::protocol::server_status::SERVER_STATUS_IN_TRANS != 0 {
                ConnectionState::InTransaction
            } else {
                ConnectionState::Ready
            };

        Outcome::Ok(rows)
    }

    /// Parse a column definition packet.
    fn parse_column_def(&self, data: &[u8]) -> Result<ColumnDef, Error> {
        let mut reader = PacketReader::new(data);

        let catalog = reader
            .read_lenenc_string()
            .ok_or_else(|| protocol_error("Missing catalog"))?;
        let schema = reader
            .read_lenenc_string()
            .ok_or_else(|| protocol_error("Missing schema"))?;
        let table = reader
            .read_lenenc_string()
            .ok_or_else(|| protocol_error("Missing table"))?;
        let org_table = reader
            .read_lenenc_string()
            .ok_or_else(|| protocol_error("Missing org_table"))?;
        let name = reader
            .read_lenenc_string()
            .ok_or_else(|| protocol_error("Missing name"))?;
        let org_name = reader
            .read_lenenc_string()
            .ok_or_else(|| protocol_error("Missing org_name"))?;

        let _fixed_len = reader.read_lenenc_int();

        let charset_val = reader
            .read_u16_le()
            .ok_or_else(|| protocol_error("Missing charset"))?;
        let column_length = reader
            .read_u32_le()
            .ok_or_else(|| protocol_error("Missing column_length"))?;
        let column_type = FieldType::from_u8(
            reader
                .read_u8()
                .ok_or_else(|| protocol_error("Missing column_type"))?,
        );
        let flags = reader
            .read_u16_le()
            .ok_or_else(|| protocol_error("Missing flags"))?;
        let decimals = reader
            .read_u8()
            .ok_or_else(|| protocol_error("Missing decimals"))?;

        Ok(ColumnDef {
            catalog,
            schema,
            table,
            org_table,
            name,
            org_name,
            charset: charset_val,
            column_length,
            column_type,
            flags,
            decimals,
        })
    }

    /// Parse a text protocol row.
    fn parse_text_row(&self, data: &[u8], columns: &[ColumnDef]) -> Row {
        let mut reader = PacketReader::new(data);
        let mut values = Vec::with_capacity(columns.len());

        for col in columns {
            if reader.peek() == Some(0xFB) {
                reader.skip(1);
                values.push(Value::Null);
            } else if let Some(data) = reader.read_lenenc_bytes() {
                let is_unsigned = col.is_unsigned();
                let value = decode_text_value(col.column_type, &data, is_unsigned);
                values.push(value);
            } else {
                values.push(Value::Null);
            }
        }

        let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        Row::new(column_names, values)
    }

    /// Ping the server asynchronously.
    pub async fn ping_async(&mut self, _cx: &Cx) -> Outcome<(), Error> {
        self.sequence_id = 0;

        let mut writer = PacketWriter::new();
        writer.write_u8(Command::Ping as u8);

        if let Outcome::Err(e) = self.write_packet_async(writer.as_bytes()).await {
            return Outcome::Err(e);
        }

        let (payload, _) = match self.read_packet_async().await {
            Outcome::Ok(p) => p,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        if payload.first() == Some(&0x00) {
            Outcome::Ok(())
        } else {
            Outcome::Err(connection_error("Ping failed"))
        }
    }

    /// Close the connection asynchronously.
    pub async fn close_async(mut self, _cx: &Cx) -> Result<(), Error> {
        if self.state == ConnectionState::Closed {
            return Ok(());
        }

        self.sequence_id = 0;

        let mut writer = PacketWriter::new();
        writer.write_u8(Command::Quit as u8);

        // Best effort - ignore errors on close
        let _ = self.write_packet_async(writer.as_bytes()).await;

        self.state = ConnectionState::Closed;
        Ok(())
    }
}

// === Connection trait implementation ===

impl Connection for MySqlAsyncConnection {
    type Tx<'conn> = MySqlTransaction<'conn>;

    fn query(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
        // Note: This requires &mut self, but trait uses &self
        // We need interior mutability or a different approach
        // For now, use a workaround
        async move {
            // This is a limitation - we need mutable access
            // In a real implementation, we'd use interior mutability
            Outcome::Err(connection_error(
                "Query requires mutable access - use query_async directly",
            ))
        }
    }

    fn query_one(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Option<Row>, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Query requires mutable access - use query_async directly",
            ))
        }
    }

    fn execute(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<u64, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Execute requires mutable access - use query_async directly",
            ))
        }
    }

    fn insert(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Outcome<i64, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Insert requires mutable access - use query_async directly",
            ))
        }
    }

    fn batch(
        &self,
        cx: &Cx,
        statements: &[(String, Vec<Value>)],
    ) -> impl Future<Output = Outcome<Vec<u64>, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Batch requires mutable access - use query_async directly",
            ))
        }
    }

    fn begin(&self, cx: &Cx) -> impl Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Begin requires mutable access - use transaction methods directly",
            ))
        }
    }

    fn begin_with(
        &self,
        cx: &Cx,
        isolation: IsolationLevel,
    ) -> impl Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Begin requires mutable access - use transaction methods directly",
            ))
        }
    }

    fn prepare(
        &self,
        cx: &Cx,
        sql: &str,
    ) -> impl Future<Output = Outcome<PreparedStatement, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Prepare not yet implemented for MySQL async",
            ))
        }
    }

    fn query_prepared(
        &self,
        cx: &Cx,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Prepared query not yet implemented for MySQL async",
            ))
        }
    }

    fn execute_prepared(
        &self,
        cx: &Cx,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> impl Future<Output = Outcome<u64, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Prepared execute not yet implemented for MySQL async",
            ))
        }
    }

    fn ping(&self, cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Ping requires mutable access - use ping_async directly",
            ))
        }
    }

    fn close(self, cx: &Cx) -> impl Future<Output = Result<(), Error>> + Send {
        async move { self.close_async(cx).await }
    }
}

/// MySQL transaction (placeholder).
pub struct MySqlTransaction<'conn> {
    conn: &'conn mut MySqlAsyncConnection,
}

impl<'conn> TransactionOps for MySqlTransaction<'conn> {
    fn query(
        &self,
        _cx: &Cx,
        _sql: &str,
        _params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
        async move { Outcome::Err(connection_error("Transaction query not yet implemented")) }
    }

    fn query_one(
        &self,
        _cx: &Cx,
        _sql: &str,
        _params: &[Value],
    ) -> impl Future<Output = Outcome<Option<Row>, Error>> + Send {
        async move { Outcome::Err(connection_error("Transaction query_one not yet implemented")) }
    }

    fn execute(
        &self,
        _cx: &Cx,
        _sql: &str,
        _params: &[Value],
    ) -> impl Future<Output = Outcome<u64, Error>> + Send {
        async move { Outcome::Err(connection_error("Transaction execute not yet implemented")) }
    }

    fn savepoint(
        &self,
        _cx: &Cx,
        _name: &str,
    ) -> impl Future<Output = Outcome<(), Error>> + Send {
        async move { Outcome::Err(connection_error("Transaction savepoint not yet implemented")) }
    }

    fn rollback_to(
        &self,
        _cx: &Cx,
        _name: &str,
    ) -> impl Future<Output = Outcome<(), Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Transaction rollback_to not yet implemented",
            ))
        }
    }

    fn release(
        &self,
        _cx: &Cx,
        _name: &str,
    ) -> impl Future<Output = Outcome<(), Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Transaction release not yet implemented",
            ))
        }
    }

    fn commit(self, _cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
        async move { Outcome::Err(connection_error("Transaction commit not yet implemented")) }
    }

    fn rollback(self, _cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
        async move { Outcome::Err(connection_error("Transaction rollback not yet implemented")) }
    }
}

// === Console integration ===

#[cfg(feature = "console")]
impl ConsoleAware for MySqlAsyncConnection {
    fn set_console(&mut self, console: Option<Arc<SqlModelConsole>>) {
        self.console = console;
    }

    fn console(&self) -> Option<&Arc<SqlModelConsole>> {
        self.console.as_ref()
    }
}

// === Helper functions ===

fn protocol_error(msg: impl Into<String>) -> Error {
    Error::Protocol(ProtocolError {
        message: msg.into(),
        raw_data: None,
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

fn connection_error(msg: impl Into<String>) -> Error {
    Error::Connection(ConnectionError {
        kind: ConnectionErrorKind::Connect,
        message: msg.into(),
        source: None,
    })
}

fn query_error(err: &ErrPacket) -> Error {
    let kind = if err.is_duplicate_key() || err.is_foreign_key_violation() {
        QueryErrorKind::Constraint
    } else {
        QueryErrorKind::Syntax
    };

    Error::Query(QueryError {
        kind,
        message: err.error_message.clone(),
        sqlstate: Some(err.sql_state.clone()),
        sql: None,
        detail: None,
        hint: None,
        position: None,
        source: None,
    })
}

fn query_error_msg(msg: impl Into<String>) -> Error {
    Error::Query(QueryError {
        kind: QueryErrorKind::Syntax,
        message: msg.into(),
        sqlstate: None,
        sql: None,
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
    fn test_connection_state() {
        assert_eq!(ConnectionState::Disconnected, ConnectionState::Disconnected);
    }

    #[test]
    fn test_error_helpers() {
        let err = protocol_error("test");
        assert!(matches!(err, Error::Protocol(_)));

        let err = auth_error("auth failed");
        assert!(matches!(err, Error::Connection(_)));

        let err = connection_error("conn failed");
        assert!(matches!(err, Error::Connection(_)));
    }
}
