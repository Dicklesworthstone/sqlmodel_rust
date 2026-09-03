//! MySQL wire protocol implementation.
//!
//! MySQL packets have a 4-byte header:
//! - 3 bytes: payload length (little-endian)
//! - 1 byte: sequence number
//!
//! Maximum packet payload is 2^24 - 1 (16MB - 1). Larger payloads
//! are split into multiple packets.

pub mod prepared;
pub mod reader;
pub mod writer;

pub use prepared::{
    PreparedStatement, StmtPrepareOk, build_stmt_close_packet, build_stmt_execute_packet,
    build_stmt_prepare_packet, build_stmt_reset_packet, parse_stmt_prepare_ok,
};
pub use reader::PacketReader;
pub use writer::PacketWriter;

/// Maximum payload size for a single MySQL packet (2^24 - 1 bytes).
pub const MAX_PACKET_SIZE: usize = 0xFF_FF_FF;

/// MySQL capability flags (client and server).
#[allow(dead_code)]
pub mod capabilities {
    pub const CLIENT_LONG_PASSWORD: u32 = 1;
    pub const CLIENT_FOUND_ROWS: u32 = 1 << 1;
    pub const CLIENT_LONG_FLAG: u32 = 1 << 2;
    pub const CLIENT_CONNECT_WITH_DB: u32 = 1 << 3;
    pub const CLIENT_NO_SCHEMA: u32 = 1 << 4;
    pub const CLIENT_COMPRESS: u32 = 1 << 5;
    pub const CLIENT_ODBC: u32 = 1 << 6;
    pub const CLIENT_LOCAL_FILES: u32 = 1 << 7;
    pub const CLIENT_IGNORE_SPACE: u32 = 1 << 8;
    pub const CLIENT_PROTOCOL_41: u32 = 1 << 9;
    pub const CLIENT_INTERACTIVE: u32 = 1 << 10;
    pub const CLIENT_SSL: u32 = 1 << 11;
    pub const CLIENT_IGNORE_SIGPIPE: u32 = 1 << 12;
    pub const CLIENT_TRANSACTIONS: u32 = 1 << 13;
    pub const CLIENT_RESERVED: u32 = 1 << 14;
    pub const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
    pub const CLIENT_MULTI_STATEMENTS: u32 = 1 << 16;
    pub const CLIENT_MULTI_RESULTS: u32 = 1 << 17;
    pub const CLIENT_PS_MULTI_RESULTS: u32 = 1 << 18;
    pub const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
    pub const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
    pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 1 << 21;
    pub const CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS: u32 = 1 << 22;
    pub const CLIENT_SESSION_TRACK: u32 = 1 << 23;
    pub const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;
    pub const CLIENT_OPTIONAL_RESULTSET_METADATA: u32 = 1 << 25;
    pub const CLIENT_ZSTD_COMPRESSION_ALGORITHM: u32 = 1 << 26;
    pub const CLIENT_QUERY_ATTRIBUTES: u32 = 1 << 27;

    /// Default client capabilities for modern MySQL connections.
    pub const DEFAULT_CLIENT_FLAGS: u32 = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_LONG_PASSWORD
        | CLIENT_TRANSACTIONS
        | CLIENT_MULTI_STATEMENTS
        | CLIENT_MULTI_RESULTS
        | CLIENT_PS_MULTI_RESULTS
        | CLIENT_PLUGIN_AUTH
        | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
        | CLIENT_CONNECT_WITH_DB
        | CLIENT_DEPRECATE_EOF;
}

/// MySQL command codes (COM_xxx).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Command {
    /// Sleep (internal use)
    Sleep = 0x00,
    /// Quit connection
    Quit = 0x01,
    /// Switch database
    InitDb = 0x02,
    /// Text protocol query
    Query = 0x03,
    /// List fields in table (deprecated)
    FieldList = 0x04,
    /// Create database
    CreateDb = 0x05,
    /// Drop database
    DropDb = 0x06,
    /// Refresh (flush tables, etc.)
    Refresh = 0x07,
    /// Shutdown server
    Shutdown = 0x08,
    /// Statistics
    Statistics = 0x09,
    /// Process info
    ProcessInfo = 0x0a,
    /// Connect (internal use)
    Connect = 0x0b,
    /// Kill process
    ProcessKill = 0x0c,
    /// Debug
    Debug = 0x0d,
    /// Ping server
    Ping = 0x0e,
    /// Time (internal use)
    Time = 0x0f,
    /// Delayed insert (deprecated)
    DelayedInsert = 0x10,
    /// Change user
    ChangeUser = 0x11,
    /// Binlog dump
    BinlogDump = 0x12,
    /// Table dump
    TableDump = 0x13,
    /// Connect out
    ConnectOut = 0x14,
    /// Register slave
    RegisterSlave = 0x15,
    /// Prepare statement
    StmtPrepare = 0x16,
    /// Execute prepared statement
    StmtExecute = 0x17,
    /// Send long data for prepared statement
    StmtSendLongData = 0x18,
    /// Close prepared statement
    StmtClose = 0x19,
    /// Reset prepared statement
    StmtReset = 0x1a,
    /// Set option
    SetOption = 0x1b,
    /// Fetch cursor rows
    StmtFetch = 0x1c,
    /// Daemon (internal use)
    Daemon = 0x1d,
    /// Binlog dump GTID
    BinlogDumpGtid = 0x1e,
    /// Reset connection
    ResetConnection = 0x1f,
}

/// MySQL server status flags.
#[allow(dead_code)]
pub mod server_status {
    pub const SERVER_STATUS_IN_TRANS: u16 = 0x0001;
    pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
    pub const SERVER_MORE_RESULTS_EXISTS: u16 = 0x0008;
    pub const SERVER_STATUS_NO_GOOD_INDEX_USED: u16 = 0x0010;
    pub const SERVER_STATUS_NO_INDEX_USED: u16 = 0x0020;
    pub const SERVER_STATUS_CURSOR_EXISTS: u16 = 0x0040;
    pub const SERVER_STATUS_LAST_ROW_SENT: u16 = 0x0080;
    pub const SERVER_STATUS_DB_DROPPED: u16 = 0x0100;
    pub const SERVER_STATUS_NO_BACKSLASH_ESCAPES: u16 = 0x0200;
    pub const SERVER_STATUS_METADATA_CHANGED: u16 = 0x0400;
    pub const SERVER_QUERY_WAS_SLOW: u16 = 0x0800;
    pub const SERVER_PS_OUT_PARAMS: u16 = 0x1000;
    pub const SERVER_STATUS_IN_TRANS_READONLY: u16 = 0x2000;
    pub const SERVER_SESSION_STATE_CHANGED: u16 = 0x4000;
}

/// MySQL character set codes.
#[allow(dead_code)]
pub mod charset {
    pub const LATIN1_SWEDISH_CI: u8 = 8;
    pub const UTF8_GENERAL_CI: u8 = 33;
    pub const BINARY: u8 = 63;
    pub const UTF8MB4_GENERAL_CI: u8 = 45;
    pub const UTF8MB4_UNICODE_CI: u8 = 224;
    pub const UTF8MB4_0900_AI_CI: u8 = 255;

    /// Default charset for new connections (utf8mb4).
    pub const DEFAULT_CHARSET: u8 = UTF8MB4_0900_AI_CI;
}

/// A MySQL packet header.
#[derive(Debug, Clone, Copy)]
pub struct PacketHeader {
    /// Payload length (3 bytes, max 16MB - 1)
    pub payload_length: u32,
    /// Sequence number (wraps at 255)
    pub sequence_id: u8,
}

impl PacketHeader {
    /// Total header size in bytes.
    pub const SIZE: usize = 4;

    /// Parse a packet header from 4 bytes.
    pub fn from_bytes(bytes: &[u8; 4]) -> Self {
        let payload_length =
            u32::from(bytes[0]) | (u32::from(bytes[1]) << 8) | (u32::from(bytes[2]) << 16);
        let sequence_id = bytes[3];
        Self {
            payload_length,
            sequence_id,
        }
    }

    /// Encode the header to 4 bytes.
    pub fn to_bytes(&self) -> [u8; 4] {
        [
            (self.payload_length & 0xFF) as u8,
            ((self.payload_length >> 8) & 0xFF) as u8,
            ((self.payload_length >> 16) & 0xFF) as u8,
            self.sequence_id,
        ]
    }
}

/// Server response packet types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    /// OK packet (0x00)
    Ok,
    /// Error packet (0xFF)
    Error,
    /// EOF packet (0xFE) - deprecated in CLIENT_DEPRECATE_EOF
    Eof,
    /// Local infile request (0xFB)
    LocalInfile,
    /// Data packet (result set row, etc.)
    Data,
}

impl PacketType {
    /// Detect packet type from the first byte of payload.
    ///
    /// This is the classification for the *first* packet of a response. Inside
    /// a result set's row phase use [`RowPacket::classify`]: there a leading
    /// `0x00` is a row, not an OK packet.
    pub fn from_first_byte(byte: u8, payload_len: u32) -> Self {
        match byte {
            0x00 => PacketType::Ok,
            0xFF => PacketType::Error,
            // EOF is 0xFE with payload < 9 bytes
            0xFE if payload_len < 9 => PacketType::Eof,
            0xFB => PacketType::LocalInfile,
            _ => PacketType::Data,
        }
    }
}

/// What a packet is once a result set's column definitions have been read.
///
/// A text-protocol row whose first value is the empty string starts with
/// `0x00` (a zero-length length-encoded string), and every binary-protocol row
/// starts with `0x00`; neither is an OK packet. Reading them as one dropped the
/// rows and left the real terminator in the stream, so the next statement
/// failed with "Invalid column count" (found 2026-09 through the introspector,
/// whose `TABLE_COMMENT` is the empty string for a table without a comment).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowPacket {
    /// A data row, text or binary.
    Row,
    /// `0xFE` with a payload shorter than 16 MiB: the EOF packet (fewer than
    /// 9 bytes) or, with `CLIENT_DEPRECATE_EOF`, the OK packet that ends the
    /// rows. A row can only start with `0xFE` when its first value is at least
    /// 16 MiB long, which cannot fit in one packet.
    Terminator,
    /// `0xFF`: the server aborted the result set.
    Error,
}

impl RowPacket {
    /// Classify a packet read in a result set's row phase.
    #[must_use]
    pub fn classify(payload: &[u8]) -> Self {
        match payload.first() {
            Some(0xFF) => RowPacket::Error,
            Some(0xFE) if payload.len() < 0xFF_FFFF => RowPacket::Terminator,
            _ => RowPacket::Row,
        }
    }
}

/// Parse the packet that ends a result set's rows and return its
/// `(status_flags, warnings)`: an EOF packet when shorter than 9 bytes,
/// otherwise an OK packet carrying the `0xFE` header.
#[must_use]
pub fn parse_result_terminator(payload: &[u8]) -> Option<(u16, u16)> {
    if payload.len() < 9 {
        let mut reader = PacketReader::new(payload);
        reader
            .parse_eof_packet()
            .map(|eof| (eof.status_flags, eof.warnings))
    } else {
        // OK layout after the 0xFE header: affected rows, last insert id,
        // status, warnings. Parsed here rather than through `parse_ok_packet`,
        // whose optional-marker skip would swallow a zero affected-rows byte.
        let mut reader = PacketReader::new(payload.get(1..)?);
        let _affected_rows = reader.read_lenenc_int()?;
        let _last_insert_id = reader.read_lenenc_int()?;
        let status_flags = reader.read_u16_le()?;
        let warnings = reader.read_u16_le()?;
        Some((status_flags, warnings))
    }
}

/// Parsed OK packet.
#[derive(Debug, Clone)]
pub struct OkPacket {
    /// Number of affected rows
    pub affected_rows: u64,
    /// Last insert ID
    pub last_insert_id: u64,
    /// Server status flags
    pub status_flags: u16,
    /// Number of warnings
    pub warnings: u16,
    /// Info string (if any)
    pub info: String,
}

/// Parsed Error packet.
#[derive(Debug, Clone)]
pub struct ErrPacket {
    /// Error code
    pub error_code: u16,
    /// SQL state (5 characters)
    pub sql_state: String,
    /// Error message
    pub error_message: String,
}

impl ErrPacket {
    /// Check if this is a unique constraint violation.
    pub fn is_duplicate_key(&self) -> bool {
        // MySQL error code 1062 = ER_DUP_ENTRY
        self.error_code == 1062
    }

    /// Check if this is a foreign key constraint violation.
    pub fn is_foreign_key_violation(&self) -> bool {
        // MySQL error codes 1451, 1452 = foreign key violations
        self.error_code == 1451 || self.error_code == 1452
    }
}

/// Parsed EOF packet (deprecated in newer MySQL versions).
#[derive(Debug, Clone, Copy)]
pub struct EofPacket {
    /// Number of warnings
    pub warnings: u16,
    /// Server status flags
    pub status_flags: u16,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packet_header_roundtrip() {
        let header = PacketHeader {
            payload_length: 0x0012_3456,
            sequence_id: 7,
        };
        let bytes = header.to_bytes();
        let parsed = PacketHeader::from_bytes(&bytes);
        assert_eq!(header.payload_length, parsed.payload_length);
        assert_eq!(header.sequence_id, parsed.sequence_id);
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn test_packet_header_max_size() {
        let header = PacketHeader {
            payload_length: MAX_PACKET_SIZE as u32,
            sequence_id: 255,
        };
        let bytes = header.to_bytes();
        assert_eq!(bytes, [0xFF, 0xFF, 0xFF, 255]);
    }

    #[test]
    fn row_phase_never_takes_a_leading_zero_for_an_ok_packet() {
        // Text row whose first value is the empty string, then "x".
        assert_eq!(RowPacket::classify(&[0x00, 0x01, b'x']), RowPacket::Row);
        // Binary rows always start with 0x00.
        assert_eq!(RowPacket::classify(&[0x00, 0x00, 0x2A]), RowPacket::Row);
        // Ordinary text row: first byte is the first value's length.
        assert_eq!(RowPacket::classify(&[0x2C, b'a']), RowPacket::Row);
        // EOF and the DEPRECATE_EOF OK terminator, short and long.
        assert_eq!(
            RowPacket::classify(&[0xFE, 0x00, 0x00, 0x02, 0x00]),
            RowPacket::Terminator
        );
        assert_eq!(RowPacket::classify(&[0xFE; 32]), RowPacket::Terminator);
        assert_eq!(RowPacket::classify(&[0xFF, 0x28, 0x04]), RowPacket::Error);
        assert_eq!(RowPacket::classify(&[]), RowPacket::Row);

        // Terminator parsing: EOF layout (warnings, status), then OK layout.
        assert_eq!(
            parse_result_terminator(&[0xFE, 0x03, 0x00, 0x22, 0x00]),
            Some((0x0022, 3))
        );
        assert_eq!(
            parse_result_terminator(&[0xFE, 0x00, 0x00, 0x22, 0x00, 0x03, 0x00, 0x00, 0x00]),
            Some((0x0022, 3))
        );
    }

    #[test]
    fn test_packet_type_detection() {
        assert_eq!(PacketType::from_first_byte(0x00, 10), PacketType::Ok);
        assert_eq!(PacketType::from_first_byte(0xFF, 10), PacketType::Error);
        assert_eq!(PacketType::from_first_byte(0xFE, 5), PacketType::Eof);
        assert_eq!(PacketType::from_first_byte(0xFE, 100), PacketType::Data);
        assert_eq!(
            PacketType::from_first_byte(0xFB, 10),
            PacketType::LocalInfile
        );
        assert_eq!(PacketType::from_first_byte(0x42, 10), PacketType::Data);
    }

    #[test]
    fn test_err_packet_error_types() {
        let dup = ErrPacket {
            error_code: 1062,
            sql_state: "23000".to_string(),
            error_message: "Duplicate entry".to_string(),
        };
        assert!(dup.is_duplicate_key());
        assert!(!dup.is_foreign_key_violation());

        let fk = ErrPacket {
            error_code: 1451,
            sql_state: "23000".to_string(),
            error_message: "Cannot delete".to_string(),
        };
        assert!(!fk.is_duplicate_key());
        assert!(fk.is_foreign_key_violation());
    }
}
