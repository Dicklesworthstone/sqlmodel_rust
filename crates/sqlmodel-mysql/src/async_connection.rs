//! Async MySQL connection implementation.
//!
//! This module implements the async MySQL connection using asupersync's TCP primitives.
//! It provides the `Connection` trait implementation for integration with sqlmodel-core.

// Allow `impl Future` return types in trait methods - intentional design for async trait compat
#![allow(clippy::manual_async_fn)]
// The Error type is intentionally large to carry full context
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::future::Future;
use std::io::{self, Read as StdRead, Write as StdWrite};
use std::net::TcpStream as StdTcpStream;
use std::sync::Arc;

use asupersync::io::{AsyncRead, AsyncWrite, ReadBuf};
use asupersync::net::TcpStream;
use asupersync::sync::Mutex;
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
    Command, ErrPacket, MAX_PACKET_SIZE, PacketHeader, PacketReader, PacketType, PacketWriter,
    capabilities, charset, prepared,
};
use crate::types::{
    ColumnDef, FieldType, decode_binary_value_with_len, decode_text_value, interpolate_params,
};

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
    /// Prepared statement metadata (keyed by statement ID)
    prepared_stmts: HashMap<u32, PreparedStmtMeta>,
    /// Optional console for rich output
    #[cfg(feature = "console")]
    console: Option<Arc<SqlModelConsole>>,
}

/// Metadata for a prepared statement.
///
/// Stores the MySQL-specific information needed to execute
/// and decode results from a prepared statement.
#[derive(Debug, Clone)]
struct PreparedStmtMeta {
    /// Server-assigned statement ID (stored for potential future use in close/reset)
    #[allow(dead_code)]
    statement_id: u32,
    /// Parameter column definitions (for type encoding)
    params: Vec<ColumnDef>,
    /// Result column definitions (for binary decoding)
    columns: Vec<ColumnDef>,
}

/// Connection stream wrapper for sync/async compatibility.
#[allow(dead_code)]
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
            prepared_stmts: HashMap::new(),
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
        // Read header (4 bytes) - must loop since TCP can fragment reads
        let mut header_buf = [0u8; 4];

        match &mut self.stream {
            ConnectionStream::Async(stream) => {
                let mut header_read = 0;
                while header_read < 4 {
                    let mut read_buf = ReadBuf::new(&mut header_buf[header_read..]);
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
                                    message: "Connection closed while reading header".to_string(),
                                    source: None,
                                }));
                            }
                            header_read += n;
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
                // Read continuation header with loop (TCP can fragment)
                let mut header_buf = [0u8; 4];
                match &mut self.stream {
                    ConnectionStream::Async(stream) => {
                        let mut header_read = 0;
                        while header_read < 4 {
                            let mut read_buf = ReadBuf::new(&mut header_buf[header_read..]);
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
                                            message: "Connection closed while reading continuation header".to_string(),
                                            source: None,
                                        }));
                                    }
                                    header_read += n;
                                }
                                Err(e) => {
                                    return Outcome::Err(Error::Connection(ConnectionError {
                                        kind: ConnectionErrorKind::Disconnected,
                                        message: format!(
                                            "Failed to read continuation header: {}",
                                            e
                                        ),
                                        source: Some(Box::new(e)),
                                    }));
                                }
                            }
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
                                            return Outcome::Err(Error::Connection(ConnectionError {
                                                kind: ConnectionErrorKind::Disconnected,
                                                message: "Connection closed while reading continuation payload".to_string(),
                                                source: None,
                                            }));
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
                // Loop to handle partial writes (poll_write may return fewer bytes)
                let mut written = 0;
                while written < packet.len() {
                    match std::future::poll_fn(|cx| {
                        std::pin::Pin::new(&mut *stream).poll_write(cx, &packet[written..])
                    })
                    .await
                    {
                        Ok(n) => {
                            if n == 0 {
                                return Outcome::Err(Error::Connection(ConnectionError {
                                    kind: ConnectionErrorKind::Disconnected,
                                    message: "Connection closed while writing packet".to_string(),
                                    source: None,
                                }));
                            }
                            written += n;
                        }
                        Err(e) => {
                            return Outcome::Err(Error::Connection(ConnectionError {
                                kind: ConnectionErrorKind::Disconnected,
                                message: format!("Failed to write packet: {}", e),
                                source: Some(Box::new(e)),
                            }));
                        }
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
        let Some(protocol_version) = reader.read_u8() else {
            return Outcome::Err(protocol_error("Missing protocol version"));
        };

        if protocol_version != 10 {
            return Outcome::Err(protocol_error(format!(
                "Unsupported protocol version: {}",
                protocol_version
            )));
        }

        // Server version (null-terminated string)
        let Some(server_version) = reader.read_null_string() else {
            return Outcome::Err(protocol_error("Missing server version"));
        };

        // Connection ID
        let Some(connection_id) = reader.read_u32_le() else {
            return Outcome::Err(protocol_error("Missing connection ID"));
        };

        // Auth plugin data part 1 (8 bytes)
        let Some(auth_data_1) = reader.read_bytes(8) else {
            return Outcome::Err(protocol_error("Missing auth data"));
        };

        // Filler (1 byte)
        reader.skip(1);

        // Capability flags (lower 2 bytes)
        let Some(caps_lower) = reader.read_u16_le() else {
            return Outcome::Err(protocol_error("Missing capability flags"));
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
        let Some(server_caps) = self.server_caps.as_ref() else {
            return Outcome::Err(protocol_error("No server handshake received"));
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
    /// Uses a loop to handle auth switches without recursion.
    async fn handle_auth_result_async(&mut self) -> Outcome<(), Error> {
        // Loop to handle potential auth switches without recursion
        loop {
            let (payload, _) = match self.read_packet_async().await {
                Outcome::Ok(p) => p,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };

            if payload.is_empty() {
                return Outcome::Err(protocol_error("Empty authentication response"));
            }

            #[allow(clippy::cast_possible_truncation)] // MySQL packets are max 16MB
            match PacketType::from_first_byte(payload[0], payload.len() as u32) {
                PacketType::Ok => {
                    let mut reader = PacketReader::new(&payload);
                    if let Some(ok) = reader.parse_ok_packet() {
                        self.status_flags = ok.status_flags;
                        self.affected_rows = ok.affected_rows;
                    }
                    return Outcome::Ok(());
                }
                PacketType::Error => {
                    let mut reader = PacketReader::new(&payload);
                    let Some(err) = reader.parse_err_packet() else {
                        return Outcome::Err(protocol_error("Invalid error packet"));
                    };
                    return Outcome::Err(auth_error(format!(
                        "Authentication failed: {} ({})",
                        err.error_message, err.error_code
                    )));
                }
                PacketType::Eof => {
                    // Auth switch request - handle inline to avoid recursion
                    let data = &payload[1..];
                    let mut reader = PacketReader::new(data);

                    let Some(plugin) = reader.read_null_string() else {
                        return Outcome::Err(protocol_error("Missing plugin name in auth switch"));
                    };

                    let auth_data = reader.read_rest();
                    let response = self.compute_auth_response(&plugin, auth_data);

                    if let Outcome::Err(e) = self.write_packet_async(&response).await {
                        return Outcome::Err(e);
                    }
                    // Continue loop to read next auth result
                }
                _ => {
                    // Handle additional auth data
                    return self.handle_additional_auth_async(&payload).await;
                }
            }
        }
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

        #[allow(clippy::cast_possible_truncation)] // MySQL packets are max 16MB
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
                let Some(err) = reader.parse_err_packet() else {
                    return Outcome::Err(protocol_error("Invalid error packet"));
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
        #[allow(clippy::cast_possible_truncation)] // Column count fits in usize
        let Some(column_count) = reader.read_lenenc_int().map(|c| c as usize) else {
            return Outcome::Err(protocol_error("Invalid column count"));
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

            #[allow(clippy::cast_possible_truncation)] // MySQL packets are max 16MB
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
                    let Some(err) = reader.parse_err_packet() else {
                        return Outcome::Err(protocol_error("Invalid error packet"));
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

    /// Execute a statement asynchronously and return affected rows.
    ///
    /// This is similar to `query_async` but returns the number of affected rows
    /// instead of the result set. Useful for INSERT, UPDATE, DELETE statements.
    pub async fn execute_async(
        &mut self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> Outcome<u64, Error> {
        // Execute the query
        match self.query_async(cx, sql, params).await {
            Outcome::Ok(_) => Outcome::Ok(self.affected_rows),
            Outcome::Err(e) => Outcome::Err(e),
            Outcome::Cancelled(c) => Outcome::Cancelled(c),
            Outcome::Panicked(p) => Outcome::Panicked(p),
        }
    }

    /// Prepare a statement for later execution using the binary protocol.
    ///
    /// This sends COM_STMT_PREPARE to the server and stores the metadata
    /// needed for later execution via `query_prepared_async` or `execute_prepared_async`.
    pub async fn prepare_async(
        &mut self,
        _cx: &Cx,
        sql: &str,
    ) -> Outcome<PreparedStatement, Error> {
        if !self.is_ready() && self.state != ConnectionState::InTransaction {
            return Outcome::Err(connection_error("Connection not ready for prepare"));
        }

        self.sequence_id = 0;

        // Send COM_STMT_PREPARE
        let packet = prepared::build_stmt_prepare_packet(sql, self.sequence_id);
        if let Outcome::Err(e) = self.write_packet_raw_async(&packet).await {
            return Outcome::Err(e);
        }

        // Read response
        let (payload, _) = match self.read_packet_async().await {
            Outcome::Ok(p) => p,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        // Check for error
        if payload.first() == Some(&0xFF) {
            let mut reader = PacketReader::new(&payload);
            let Some(err) = reader.parse_err_packet() else {
                return Outcome::Err(protocol_error("Invalid error packet"));
            };
            return Outcome::Err(query_error(&err));
        }

        // Parse COM_STMT_PREPARE_OK
        let Some(prep_ok) = prepared::parse_stmt_prepare_ok(&payload) else {
            return Outcome::Err(protocol_error("Invalid prepare OK response"));
        };

        // Read parameter column definitions
        let mut param_defs = Vec::with_capacity(prep_ok.num_params as usize);
        for _ in 0..prep_ok.num_params {
            let (payload, _) = match self.read_packet_async().await {
                Outcome::Ok(p) => p,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };
            match self.parse_column_def(&payload) {
                Ok(col) => param_defs.push(col),
                Err(e) => return Outcome::Err(e),
            }
        }

        // Read EOF after params (if not CLIENT_DEPRECATE_EOF)
        let server_caps = self.server_caps.as_ref().map_or(0, |c| c.capabilities);
        if prep_ok.num_params > 0 && server_caps & capabilities::CLIENT_DEPRECATE_EOF == 0 {
            let (payload, _) = match self.read_packet_async().await {
                Outcome::Ok(p) => p,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };
            if payload.first() != Some(&0xFE) {
                return Outcome::Err(protocol_error("Expected EOF after param definitions"));
            }
        }

        // Read column definitions
        let mut column_defs = Vec::with_capacity(prep_ok.num_columns as usize);
        for _ in 0..prep_ok.num_columns {
            let (payload, _) = match self.read_packet_async().await {
                Outcome::Ok(p) => p,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };
            match self.parse_column_def(&payload) {
                Ok(col) => column_defs.push(col),
                Err(e) => return Outcome::Err(e),
            }
        }

        // Read EOF after columns (if not CLIENT_DEPRECATE_EOF)
        if prep_ok.num_columns > 0 && server_caps & capabilities::CLIENT_DEPRECATE_EOF == 0 {
            let (payload, _) = match self.read_packet_async().await {
                Outcome::Ok(p) => p,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };
            if payload.first() != Some(&0xFE) {
                return Outcome::Err(protocol_error("Expected EOF after column definitions"));
            }
        }

        // Store metadata
        let meta = PreparedStmtMeta {
            statement_id: prep_ok.statement_id,
            params: param_defs,
            columns: column_defs.clone(),
        };
        self.prepared_stmts.insert(prep_ok.statement_id, meta);

        // Return core PreparedStatement
        let column_names: Vec<String> = column_defs.iter().map(|c| c.name.clone()).collect();
        Outcome::Ok(PreparedStatement::with_columns(
            u64::from(prep_ok.statement_id),
            sql.to_string(),
            prep_ok.num_params as usize,
            column_names,
        ))
    }

    /// Execute a prepared statement and return result rows (binary protocol).
    pub async fn query_prepared_async(
        &mut self,
        _cx: &Cx,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> Outcome<Vec<Row>, Error> {
        #[allow(clippy::cast_possible_truncation)] // Statement IDs are u32 in MySQL
        let stmt_id = stmt.id() as u32;

        // Look up metadata
        let Some(meta) = self.prepared_stmts.get(&stmt_id).cloned() else {
            return Outcome::Err(connection_error("Unknown prepared statement"));
        };

        // Verify param count
        if params.len() != meta.params.len() {
            return Outcome::Err(connection_error(format!(
                "Expected {} parameters, got {}",
                meta.params.len(),
                params.len()
            )));
        }

        if !self.is_ready() && self.state != ConnectionState::InTransaction {
            return Outcome::Err(connection_error("Connection not ready for query"));
        }

        self.state = ConnectionState::InQuery;
        self.sequence_id = 0;

        // Build and send COM_STMT_EXECUTE
        let param_types: Vec<FieldType> = meta.params.iter().map(|c| c.column_type).collect();
        let packet = prepared::build_stmt_execute_packet(
            stmt_id,
            params,
            Some(&param_types),
            self.sequence_id,
        );
        if let Outcome::Err(e) = self.write_packet_raw_async(&packet).await {
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
            return Outcome::Err(protocol_error("Empty execute response"));
        }

        #[allow(clippy::cast_possible_truncation)] // MySQL packets are max 16MB
        match PacketType::from_first_byte(payload[0], payload.len() as u32) {
            PacketType::Ok => {
                // Non-SELECT statement - parse OK packet
                let mut reader = PacketReader::new(&payload);
                if let Some(ok) = reader.parse_ok_packet() {
                    self.affected_rows = ok.affected_rows;
                    self.last_insert_id = ok.last_insert_id;
                    self.status_flags = ok.status_flags;
                    self.warnings = ok.warnings;
                }
                self.state = ConnectionState::Ready;
                Outcome::Ok(vec![])
            }
            PacketType::Error => {
                self.state = ConnectionState::Ready;
                let mut reader = PacketReader::new(&payload);
                let Some(err) = reader.parse_err_packet() else {
                    return Outcome::Err(protocol_error("Invalid error packet"));
                };
                Outcome::Err(query_error(&err))
            }
            _ => {
                // Result set - read binary protocol rows
                self.read_binary_result_set_async(&payload, &meta.columns)
                    .await
            }
        }
    }

    /// Execute a prepared statement and return affected row count.
    pub async fn execute_prepared_async(
        &mut self,
        cx: &Cx,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> Outcome<u64, Error> {
        match self.query_prepared_async(cx, stmt, params).await {
            Outcome::Ok(_) => Outcome::Ok(self.affected_rows),
            Outcome::Err(e) => Outcome::Err(e),
            Outcome::Cancelled(c) => Outcome::Cancelled(c),
            Outcome::Panicked(p) => Outcome::Panicked(p),
        }
    }

    /// Close a prepared statement.
    pub async fn close_prepared_async(&mut self, stmt: &PreparedStatement) {
        #[allow(clippy::cast_possible_truncation)] // Statement IDs are u32 in MySQL
        let stmt_id = stmt.id() as u32;
        self.prepared_stmts.remove(&stmt_id);

        self.sequence_id = 0;
        let packet = prepared::build_stmt_close_packet(stmt_id, self.sequence_id);
        // Best effort - no response expected
        let _ = self.write_packet_raw_async(&packet).await;
    }

    /// Read a binary protocol result set.
    async fn read_binary_result_set_async(
        &mut self,
        first_packet: &[u8],
        columns: &[ColumnDef],
    ) -> Outcome<Vec<Row>, Error> {
        // First packet contains column count
        let mut reader = PacketReader::new(first_packet);
        #[allow(clippy::cast_possible_truncation)] // Column count fits in usize
        let Some(column_count) = reader.read_lenenc_int().map(|c| c as usize) else {
            return Outcome::Err(protocol_error("Invalid column count"));
        };

        // The column definitions were already provided from prepare
        // But server sends them again in binary result set - we need to read them
        let mut result_columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let (payload, _) = match self.read_packet_async().await {
                Outcome::Ok(p) => p,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };
            match self.parse_column_def(&payload) {
                Ok(col) => result_columns.push(col),
                Err(e) => return Outcome::Err(e),
            }
        }

        // Use the columns from the result set if available, otherwise use prepared metadata
        let cols = if result_columns.len() == columns.len() {
            &result_columns
        } else {
            columns
        };

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

        // Read binary rows until EOF or OK
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

            #[allow(clippy::cast_possible_truncation)] // MySQL packets are max 16MB
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
                    let Some(err) = reader.parse_err_packet() else {
                        return Outcome::Err(protocol_error("Invalid error packet"));
                    };
                    self.state = ConnectionState::Ready;
                    return Outcome::Err(query_error(&err));
                }
                _ => {
                    let row = self.parse_binary_row(&payload, cols);
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

    /// Parse a binary protocol row.
    fn parse_binary_row(&self, data: &[u8], columns: &[ColumnDef]) -> Row {
        // Binary row format:
        // - 0x00 header (1 byte)
        // - NULL bitmap ((column_count + 7 + 2) / 8 bytes)
        // - Column values (only non-NULL)

        let mut values = Vec::with_capacity(columns.len());
        let mut column_names = Vec::with_capacity(columns.len());

        if data.is_empty() {
            return Row::new(column_names, values);
        }

        // Skip header byte (0x00)
        let mut pos = 1;

        // NULL bitmap: (column_count + 7 + 2) / 8 bytes
        // The +2 offset is for the reserved bits at the beginning
        let null_bitmap_len = (columns.len() + 7 + 2) / 8;
        if pos + null_bitmap_len > data.len() {
            return Row::new(column_names, values);
        }
        let null_bitmap = &data[pos..pos + null_bitmap_len];
        pos += null_bitmap_len;

        // Parse column values
        for (i, col) in columns.iter().enumerate() {
            column_names.push(col.name.clone());

            // Check NULL bitmap (bit position is i + 2 due to offset)
            let bit_pos = i + 2;
            let is_null = (null_bitmap[bit_pos / 8] & (1 << (bit_pos % 8))) != 0;

            if is_null {
                values.push(Value::Null);
            } else {
                let is_unsigned = col.flags & 0x20 != 0; // UNSIGNED_FLAG
                let (value, consumed) =
                    decode_binary_value_with_len(&data[pos..], col.column_type, is_unsigned);
                values.push(value);
                pos += consumed;
            }
        }

        Row::new(column_names, values)
    }

    /// Write a pre-built packet (with header already included).
    async fn write_packet_raw_async(&mut self, packet: &[u8]) -> Outcome<(), Error> {
        match &mut self.stream {
            ConnectionStream::Async(stream) => {
                let mut written = 0;
                while written < packet.len() {
                    match std::future::poll_fn(|cx| {
                        std::pin::Pin::new(&mut *stream).poll_write(cx, &packet[written..])
                    })
                    .await
                    {
                        Ok(n) => written += n,
                        Err(e) => {
                            return Outcome::Err(Error::Connection(ConnectionError {
                                kind: ConnectionErrorKind::Disconnected,
                                message: format!("Failed to write packet: {}", e),
                                source: Some(Box::new(e)),
                            }));
                        }
                    }
                }
                // Flush
                if let Err(e) =
                    std::future::poll_fn(|cx| std::pin::Pin::new(&mut *stream).poll_flush(cx)).await
                {
                    return Outcome::Err(Error::Connection(ConnectionError {
                        kind: ConnectionErrorKind::Disconnected,
                        message: format!("Failed to flush: {}", e),
                        source: Some(Box::new(e)),
                    }));
                }
                Outcome::Ok(())
            }
            ConnectionStream::Sync(stream) => {
                if let Err(e) = stream.write_all(packet) {
                    return Outcome::Err(Error::Connection(ConnectionError {
                        kind: ConnectionErrorKind::Disconnected,
                        message: format!("Failed to write packet: {}", e),
                        source: Some(Box::new(e)),
                    }));
                }
                if let Err(e) = stream.flush() {
                    return Outcome::Err(Error::Connection(ConnectionError {
                        kind: ConnectionErrorKind::Disconnected,
                        message: format!("Failed to flush: {}", e),
                        source: Some(Box::new(e)),
                    }));
                }
                Outcome::Ok(())
            }
        }
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

    fn dialect(&self) -> sqlmodel_core::Dialect {
        sqlmodel_core::Dialect::Mysql
    }

    fn query(
        &self,
        _cx: &Cx,
        _sql: &str,
        _params: &[Value],
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
        _cx: &Cx,
        _sql: &str,
        _params: &[Value],
    ) -> impl Future<Output = Outcome<Option<Row>, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Query requires mutable access - use query_async directly",
            ))
        }
    }

    fn execute(
        &self,
        _cx: &Cx,
        _sql: &str,
        _params: &[Value],
    ) -> impl Future<Output = Outcome<u64, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Execute requires mutable access - use query_async directly",
            ))
        }
    }

    fn insert(
        &self,
        _cx: &Cx,
        _sql: &str,
        _params: &[Value],
    ) -> impl Future<Output = Outcome<i64, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Insert requires mutable access - use query_async directly",
            ))
        }
    }

    fn batch(
        &self,
        _cx: &Cx,
        _statements: &[(String, Vec<Value>)],
    ) -> impl Future<Output = Outcome<Vec<u64>, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Batch requires mutable access - use query_async directly",
            ))
        }
    }

    fn begin(&self, _cx: &Cx) -> impl Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Begin requires mutable access - use transaction methods directly",
            ))
        }
    }

    fn begin_with(
        &self,
        _cx: &Cx,
        _isolation: IsolationLevel,
    ) -> impl Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Begin requires mutable access - use transaction methods directly",
            ))
        }
    }

    fn prepare(
        &self,
        _cx: &Cx,
        _sql: &str,
    ) -> impl Future<Output = Outcome<PreparedStatement, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Prepare not yet implemented for MySQL async",
            ))
        }
    }

    fn query_prepared(
        &self,
        _cx: &Cx,
        _stmt: &PreparedStatement,
        _params: &[Value],
    ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Prepared query not yet implemented for MySQL async",
            ))
        }
    }

    fn execute_prepared(
        &self,
        _cx: &Cx,
        _stmt: &PreparedStatement,
        _params: &[Value],
    ) -> impl Future<Output = Outcome<u64, Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Prepared execute not yet implemented for MySQL async",
            ))
        }
    }

    fn ping(&self, _cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
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
    #[allow(dead_code)]
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
        async move {
            Outcome::Err(connection_error(
                "Transaction query_one not yet implemented",
            ))
        }
    }

    fn execute(
        &self,
        _cx: &Cx,
        _sql: &str,
        _params: &[Value],
    ) -> impl Future<Output = Outcome<u64, Error>> + Send {
        async move { Outcome::Err(connection_error("Transaction execute not yet implemented")) }
    }

    fn savepoint(&self, _cx: &Cx, _name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        async move {
            Outcome::Err(connection_error(
                "Transaction savepoint not yet implemented",
            ))
        }
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

    fn release(&self, _cx: &Cx, _name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        async move { Outcome::Err(connection_error("Transaction release not yet implemented")) }
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

/// Validate a savepoint name to prevent SQL injection.
///
/// MySQL identifiers must:
/// - Not be empty
/// - Start with a letter or underscore
/// - Contain only letters, digits, underscores, or dollar signs
/// - Be at most 64 characters
fn validate_savepoint_name(name: &str) -> Result<(), Error> {
    if name.is_empty() {
        return Err(query_error_msg("Savepoint name cannot be empty"));
    }
    if name.len() > 64 {
        return Err(query_error_msg(
            "Savepoint name exceeds maximum length of 64 characters",
        ));
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(query_error_msg(
            "Savepoint name must start with a letter or underscore",
        ));
    }
    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '_' && c != '$' {
            return Err(query_error_msg(format!(
                "Savepoint name contains invalid character: '{}'",
                c
            )));
        }
    }
    Ok(())
}

// === Shared connection wrapper ===

/// A thread-safe, shared MySQL connection with interior mutability.
///
/// This wrapper allows the `Connection` trait to be implemented properly
/// by wrapping the raw `MySqlAsyncConnection` in an async mutex.
///
/// # Example
///
/// ```ignore
/// let conn = MySqlAsyncConnection::connect(&cx, config).await?;
/// let shared = SharedMySqlConnection::new(conn);
///
/// // Now you can use &shared with the Connection trait
/// let rows = shared.query(&cx, "SELECT * FROM users", &[]).await?;
/// ```
pub struct SharedMySqlConnection {
    inner: Arc<Mutex<MySqlAsyncConnection>>,
}

impl SharedMySqlConnection {
    /// Create a new shared connection from a raw connection.
    pub fn new(conn: MySqlAsyncConnection) -> Self {
        Self {
            inner: Arc::new(Mutex::new(conn)),
        }
    }

    /// Create a new shared connection by connecting to the server.
    pub async fn connect(cx: &Cx, config: MySqlConfig) -> Outcome<Self, Error> {
        match MySqlAsyncConnection::connect(cx, config).await {
            Outcome::Ok(conn) => Outcome::Ok(Self::new(conn)),
            Outcome::Err(e) => Outcome::Err(e),
            Outcome::Cancelled(c) => Outcome::Cancelled(c),
            Outcome::Panicked(p) => Outcome::Panicked(p),
        }
    }

    /// Get the inner Arc for cloning.
    pub fn inner(&self) -> &Arc<Mutex<MySqlAsyncConnection>> {
        &self.inner
    }
}

impl Clone for SharedMySqlConnection {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl std::fmt::Debug for SharedMySqlConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedMySqlConnection")
            .field("inner", &"Arc<Mutex<MySqlAsyncConnection>>")
            .finish()
    }
}

/// Transaction type for SharedMySqlConnection.
///
/// This transaction holds a clone of the Arc to the connection and executes
/// transaction operations by acquiring the mutex lock for each operation.
/// The transaction must be committed or rolled back explicitly.
///
/// # Warning: Uncommitted Transactions
///
/// If a transaction is dropped without calling `commit()` or `rollback()`,
/// the underlying MySQL transaction will remain open until the connection
/// is closed or a new transaction is started. This is because Rust's `Drop`
/// trait cannot perform async operations.
///
/// **Always explicitly call `commit()` or `rollback()` before dropping.**
///
/// Note: The lifetime parameter is required by the Connection trait but the
/// actual implementation holds an owned Arc, so the transaction can outlive
/// the reference to SharedMySqlConnection if needed.
pub struct SharedMySqlTransaction<'conn> {
    inner: Arc<Mutex<MySqlAsyncConnection>>,
    committed: bool,
    _marker: std::marker::PhantomData<&'conn ()>,
}

impl SharedMySqlConnection {
    /// Internal implementation for beginning a transaction.
    async fn begin_transaction_impl(
        &self,
        cx: &Cx,
        isolation: Option<IsolationLevel>,
    ) -> Outcome<SharedMySqlTransaction<'_>, Error> {
        let inner = Arc::clone(&self.inner);

        // Acquire lock
        let Ok(mut guard) = inner.lock(cx).await else {
            return Outcome::Err(connection_error("Failed to acquire connection lock"));
        };

        // Set isolation level if specified
        if let Some(level) = isolation {
            let isolation_sql = format!("SET TRANSACTION ISOLATION LEVEL {}", level.as_sql());
            match guard.execute_async(cx, &isolation_sql, &[]).await {
                Outcome::Ok(_) => {}
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(c) => return Outcome::Cancelled(c),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            }
        }

        // Start transaction
        match guard.execute_async(cx, "BEGIN", &[]).await {
            Outcome::Ok(_) => {}
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(c) => return Outcome::Cancelled(c),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        }

        drop(guard);

        Outcome::Ok(SharedMySqlTransaction {
            inner,
            committed: false,
            _marker: std::marker::PhantomData,
        })
    }
}

impl Connection for SharedMySqlConnection {
    type Tx<'conn>
        = SharedMySqlTransaction<'conn>
    where
        Self: 'conn;

    fn dialect(&self) -> sqlmodel_core::Dialect {
        sqlmodel_core::Dialect::Mysql
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
                Outcome::Cancelled(c) => return Outcome::Cancelled(c),
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
            match guard.execute_async(cx, &sql, &params).await {
                Outcome::Ok(_) => Outcome::Ok(guard.last_insert_id() as i64),
                Outcome::Err(e) => Outcome::Err(e),
                Outcome::Cancelled(c) => Outcome::Cancelled(c),
                Outcome::Panicked(p) => Outcome::Panicked(p),
            }
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
                    Outcome::Cancelled(c) => return Outcome::Cancelled(c),
                    Outcome::Panicked(p) => return Outcome::Panicked(p),
                }
            }
            Outcome::Ok(results)
        }
    }

    fn begin(&self, cx: &Cx) -> impl Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
        self.begin_transaction_impl(cx, None)
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

    fn close(self, cx: &Cx) -> impl Future<Output = Result<(), Error>> + Send {
        async move {
            // Try to get exclusive access - if we have the only Arc, we can close
            match Arc::try_unwrap(self.inner) {
                Ok(mutex) => {
                    let conn = mutex.into_inner();
                    conn.close_async(cx).await
                }
                Err(_) => {
                    // Other references exist, can't close
                    Err(connection_error(
                        "Cannot close: other references to connection exist",
                    ))
                }
            }
        }
    }
}

impl<'conn> TransactionOps for SharedMySqlTransaction<'conn> {
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
                Outcome::Cancelled(c) => return Outcome::Cancelled(c),
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
        // Validate name before building SQL to prevent injection
        let validation_result = validate_savepoint_name(name);
        let sql = format!("SAVEPOINT {}", name);
        async move {
            // Return validation error if name was invalid
            if let Err(e) = validation_result {
                return Outcome::Err(e);
            }
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            match guard.execute_async(cx, &sql, &[]).await {
                Outcome::Ok(_) => Outcome::Ok(()),
                Outcome::Err(e) => Outcome::Err(e),
                Outcome::Cancelled(c) => Outcome::Cancelled(c),
                Outcome::Panicked(p) => Outcome::Panicked(p),
            }
        }
    }

    fn rollback_to(&self, cx: &Cx, name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        let inner = Arc::clone(&self.inner);
        // Validate name before building SQL to prevent injection
        let validation_result = validate_savepoint_name(name);
        let sql = format!("ROLLBACK TO SAVEPOINT {}", name);
        async move {
            // Return validation error if name was invalid
            if let Err(e) = validation_result {
                return Outcome::Err(e);
            }
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            match guard.execute_async(cx, &sql, &[]).await {
                Outcome::Ok(_) => Outcome::Ok(()),
                Outcome::Err(e) => Outcome::Err(e),
                Outcome::Cancelled(c) => Outcome::Cancelled(c),
                Outcome::Panicked(p) => Outcome::Panicked(p),
            }
        }
    }

    fn release(&self, cx: &Cx, name: &str) -> impl Future<Output = Outcome<(), Error>> + Send {
        let inner = Arc::clone(&self.inner);
        // Validate name before building SQL to prevent injection
        let validation_result = validate_savepoint_name(name);
        let sql = format!("RELEASE SAVEPOINT {}", name);
        async move {
            // Return validation error if name was invalid
            if let Err(e) = validation_result {
                return Outcome::Err(e);
            }
            let Ok(mut guard) = inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            match guard.execute_async(cx, &sql, &[]).await {
                Outcome::Ok(_) => Outcome::Ok(()),
                Outcome::Err(e) => Outcome::Err(e),
                Outcome::Cancelled(c) => Outcome::Cancelled(c),
                Outcome::Panicked(p) => Outcome::Panicked(p),
            }
        }
    }

    // Note: clippy incorrectly flags `self.committed = true` as unused, but
    // the Drop impl reads this field to determine if rollback logging is needed.
    #[allow(unused_assignments)]
    fn commit(mut self, cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
        async move {
            let Ok(mut guard) = self.inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            match guard.execute_async(cx, "COMMIT", &[]).await {
                Outcome::Ok(_) => {
                    self.committed = true;
                    Outcome::Ok(())
                }
                Outcome::Err(e) => Outcome::Err(e),
                Outcome::Cancelled(c) => Outcome::Cancelled(c),
                Outcome::Panicked(p) => Outcome::Panicked(p),
            }
        }
    }

    fn rollback(self, cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
        async move {
            let Ok(mut guard) = self.inner.lock(cx).await else {
                return Outcome::Err(connection_error("Failed to acquire connection lock"));
            };
            match guard.execute_async(cx, "ROLLBACK", &[]).await {
                Outcome::Ok(_) => Outcome::Ok(()),
                Outcome::Err(e) => Outcome::Err(e),
                Outcome::Cancelled(c) => Outcome::Cancelled(c),
                Outcome::Panicked(p) => Outcome::Panicked(p),
            }
        }
    }
}

impl<'conn> Drop for SharedMySqlTransaction<'conn> {
    fn drop(&mut self) {
        if !self.committed {
            // WARNING: Transaction was dropped without commit() or rollback()!
            // We cannot do async work in Drop, so the MySQL transaction will
            // remain open until the connection is closed or a new transaction
            // is started. This may cause unexpected behavior.
            //
            // To fix: Always call tx.commit(cx).await or tx.rollback(cx).await
            // before the transaction goes out of scope.
            #[cfg(debug_assertions)]
            eprintln!(
                "WARNING: SharedMySqlTransaction dropped without commit/rollback. \
                 The MySQL transaction may still be open."
            );
        }
    }
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

    #[test]
    fn test_validate_savepoint_name_valid() {
        // Valid names
        assert!(validate_savepoint_name("sp1").is_ok());
        assert!(validate_savepoint_name("_savepoint").is_ok());
        assert!(validate_savepoint_name("SavePoint_123").is_ok());
        assert!(validate_savepoint_name("sp$test").is_ok());
        assert!(validate_savepoint_name("a").is_ok());
        assert!(validate_savepoint_name("_").is_ok());
    }

    #[test]
    fn test_validate_savepoint_name_invalid() {
        // Empty name
        assert!(validate_savepoint_name("").is_err());

        // Starts with digit
        assert!(validate_savepoint_name("1savepoint").is_err());

        // Contains invalid characters
        assert!(validate_savepoint_name("save-point").is_err());
        assert!(validate_savepoint_name("save point").is_err());
        assert!(validate_savepoint_name("save;drop table").is_err());
        assert!(validate_savepoint_name("sp'--").is_err());

        // Too long (over 64 chars)
        let long_name = "a".repeat(65);
        assert!(validate_savepoint_name(&long_name).is_err());
    }
}
