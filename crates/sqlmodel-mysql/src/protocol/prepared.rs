//! MySQL prepared statement (binary protocol) implementation.
//!
//! This module implements COM_STMT_PREPARE, COM_STMT_EXECUTE, and COM_STMT_CLOSE
//! for the MySQL binary protocol. Prepared statements provide:
//!
//! - Type-safe parameter binding (no SQL injection risk)
//! - Better performance for repeated queries (parse once, execute many)
//! - Binary data transfer (more efficient than text protocol)
//!
//! # Protocol Flow
//!
//! 1. **Prepare**: Client sends COM_STMT_PREPARE with SQL
//!    - Server returns statement ID, param count, column count
//!    - Server sends param column definitions (if any)
//!    - Server sends result column definitions (if any)
//!
//! 2. **Execute**: Client sends COM_STMT_EXECUTE with statement ID + binary params
//!    - Server returns result set (binary protocol) or OK packet
//!
//! 3. **Close**: Client sends COM_STMT_CLOSE with statement ID
//!    - No server response
//!
//! # References
//!
//! - [COM_STMT_PREPARE](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html)
//! - [COM_STMT_EXECUTE](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html)
//! - [Binary Protocol Result Set](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html)

#![allow(clippy::cast_possible_truncation)]

use super::{Command, PacketWriter};
use crate::types::{ColumnDef, FieldType};
use sqlmodel_core::Value;

/// Response from COM_STMT_PREPARE.
///
/// This is sent by the server after successfully preparing a statement.
#[derive(Debug, Clone)]
pub struct StmtPrepareOk {
    /// Unique statement identifier (used in execute/close)
    pub statement_id: u32,
    /// Number of columns in result set (0 for non-SELECT)
    pub num_columns: u16,
    /// Number of parameters (placeholders) in the SQL
    pub num_params: u16,
    /// Number of warnings generated during prepare
    pub warnings: u16,
}

/// A prepared statement with its metadata.
///
/// Holds the server-assigned statement ID and column definitions
/// for both parameters and result columns.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// Server-assigned statement ID
    pub statement_id: u32,
    /// SQL query (for debugging/logging)
    pub sql: String,
    /// Parameter column definitions
    pub params: Vec<ColumnDef>,
    /// Result column definitions
    pub columns: Vec<ColumnDef>,
}

impl PreparedStatement {
    /// Create a new prepared statement from prepare response.
    pub fn new(
        statement_id: u32,
        sql: String,
        params: Vec<ColumnDef>,
        columns: Vec<ColumnDef>,
    ) -> Self {
        Self {
            statement_id,
            sql,
            params,
            columns,
        }
    }

    /// Get the number of parameters expected.
    #[must_use]
    pub fn param_count(&self) -> usize {
        self.params.len()
    }

    /// Get the number of result columns.
    #[must_use]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// Build a COM_STMT_PREPARE packet.
///
/// # Arguments
///
/// * `sql` - The SQL query with `?` placeholders for parameters
/// * `sequence_id` - The packet sequence number (typically 0)
///
/// # Returns
///
/// The complete packet bytes ready to send.
pub fn build_stmt_prepare_packet(sql: &str, sequence_id: u8) -> Vec<u8> {
    let mut writer = PacketWriter::with_capacity(1 + sql.len());
    writer.write_u8(Command::StmtPrepare as u8);
    writer.write_bytes(sql.as_bytes());
    writer.build_packet(sequence_id)
}

/// Build a COM_STMT_EXECUTE packet.
///
/// # Arguments
///
/// * `statement_id` - The statement ID from COM_STMT_PREPARE_OK
/// * `params` - Parameter values (must match the number of placeholders)
/// * `param_types` - Optional parameter type hints from previous prepare
/// * `sequence_id` - The packet sequence number (typically 0)
///
/// # Binary Protocol Parameter Encoding
///
/// The execute packet format is:
/// - Command byte (0x17)
/// - Statement ID (4 bytes, little-endian)
/// - Flags (1 byte): 0x00 = no cursor, 0x01 = cursor read-only
/// - Iteration count (4 bytes, always 1)
/// - NULL bitmap (if num_params > 0)
/// - New params bound flag (1 byte)
/// - Parameter types and values (if new_params_bound = 1)
pub fn build_stmt_execute_packet(
    statement_id: u32,
    params: &[Value],
    param_types: Option<&[FieldType]>,
    sequence_id: u8,
) -> Vec<u8> {
    let mut writer = PacketWriter::with_capacity(64 + params.len() * 16);

    // Command
    writer.write_u8(Command::StmtExecute as u8);

    // Statement ID (4 bytes LE)
    writer.write_u32_le(statement_id);

    // Flags: 0x00 = CURSOR_TYPE_NO_CURSOR
    writer.write_u8(0x00);

    // Iteration count: always 1
    writer.write_u32_le(1);

    if !params.is_empty() {
        // NULL bitmap: (num_params + 7) / 8 bytes
        let null_bitmap_len = params.len().div_ceil(8);
        let mut null_bitmap = vec![0u8; null_bitmap_len];

        for (i, param) in params.iter().enumerate() {
            if matches!(param, Value::Null) {
                null_bitmap[i / 8] |= 1 << (i % 8);
            }
        }
        writer.write_bytes(&null_bitmap);

        // New params bound flag: 1 = we're sending types
        writer.write_u8(1);

        // Parameter types (2 bytes each: type + flags)
        for (i, param) in params.iter().enumerate() {
            let field_type = if let Some(types) = param_types {
                if i < types.len() {
                    types[i]
                } else {
                    value_to_field_type(param)
                }
            } else {
                value_to_field_type(param)
            };

            // Type byte
            writer.write_u8(field_type as u8);
            // Flags byte (0x00 for signed, 0x80 for unsigned)
            let flags = if is_unsigned_value(param) { 0x80 } else { 0x00 };
            writer.write_u8(flags);
        }

        // Parameter values (only non-NULL)
        for param in params {
            if !matches!(param, Value::Null) {
                encode_binary_param(&mut writer, param);
            }
        }
    }

    writer.build_packet(sequence_id)
}

/// Build a COM_STMT_CLOSE packet.
///
/// # Arguments
///
/// * `statement_id` - The statement ID to close
/// * `sequence_id` - The packet sequence number (typically 0)
///
/// # Note
///
/// The server does not send a response to COM_STMT_CLOSE.
pub fn build_stmt_close_packet(statement_id: u32, sequence_id: u8) -> Vec<u8> {
    let mut writer = PacketWriter::with_capacity(5);
    writer.write_u8(Command::StmtClose as u8);
    writer.write_u32_le(statement_id);
    writer.build_packet(sequence_id)
}

/// Build a COM_STMT_RESET packet.
///
/// Resets the data of a prepared statement which was accumulated with
/// COM_STMT_SEND_LONG_DATA.
///
/// # Arguments
///
/// * `statement_id` - The statement ID to reset
/// * `sequence_id` - The packet sequence number
pub fn build_stmt_reset_packet(statement_id: u32, sequence_id: u8) -> Vec<u8> {
    let mut writer = PacketWriter::with_capacity(5);
    writer.write_u8(Command::StmtReset as u8);
    writer.write_u32_le(statement_id);
    writer.build_packet(sequence_id)
}

/// Parse a COM_STMT_PREPARE_OK response.
///
/// # Format
///
/// - Status: 0x00 (1 byte)
/// - Statement ID (4 bytes)
/// - Number of columns (2 bytes)
/// - Number of parameters (2 bytes)
/// - Reserved: 0x00 (1 byte)
/// - Warning count (2 bytes, if CLIENT_PROTOCOL_41)
///
/// # Returns
///
/// `Some(StmtPrepareOk)` if parsing succeeds, `None` if data is malformed.
pub fn parse_stmt_prepare_ok(data: &[u8]) -> Option<StmtPrepareOk> {
    if data.len() < 12 {
        return None;
    }

    // First byte should be 0x00 (OK status)
    if data[0] != 0x00 {
        return None;
    }

    let statement_id = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
    let num_columns = u16::from_le_bytes([data[5], data[6]]);
    let num_params = u16::from_le_bytes([data[7], data[8]]);
    // data[9] is reserved (0x00)
    let warnings = if data.len() >= 12 {
        u16::from_le_bytes([data[10], data[11]])
    } else {
        0
    };

    Some(StmtPrepareOk {
        statement_id,
        num_columns,
        num_params,
        warnings,
    })
}

/// Determine the MySQL field type for a Value.
fn value_to_field_type(value: &Value) -> FieldType {
    match value {
        Value::Null => FieldType::Null,
        Value::Bool(_) => FieldType::Tiny,
        Value::TinyInt(_) => FieldType::Tiny,
        Value::SmallInt(_) => FieldType::Short,
        Value::Int(_) => FieldType::Long,
        Value::BigInt(_) => FieldType::LongLong,
        Value::Float(_) => FieldType::Float,
        Value::Double(_) => FieldType::Double,
        Value::Decimal(_) => FieldType::NewDecimal,
        Value::Text(_) => FieldType::VarString,
        Value::Bytes(_) => FieldType::Blob,
        Value::Json(_) => FieldType::Json,
        Value::Date(_) => FieldType::Date,
        Value::Time(_) => FieldType::Time,
        Value::Timestamp(_) | Value::TimestampTz(_) => FieldType::DateTime,
        Value::Uuid(_) => FieldType::Blob,
        Value::Array(_) => FieldType::Json,
    }
}

/// Check if a value should use unsigned encoding.
fn is_unsigned_value(value: &Value) -> bool {
    // In Rust, we use signed types, so we typically send as signed.
    // Only mark as unsigned if the value is explicitly positive and large.
    matches!(value, Value::BigInt(i) if *i > i64::MAX / 2)
}

/// Encode a parameter value for binary protocol.
fn encode_binary_param(writer: &mut PacketWriter, value: &Value) {
    match value {
        Value::Null => {
            // NULL values are indicated in the NULL bitmap, no data here
        }
        Value::Bool(b) => {
            writer.write_u8(if *b { 1 } else { 0 });
        }
        Value::TinyInt(i) => {
            writer.write_u8(*i as u8);
        }
        Value::SmallInt(i) => {
            writer.write_u16_le(*i as u16);
        }
        Value::Int(i) => {
            writer.write_u32_le(*i as u32);
        }
        Value::BigInt(i) => {
            writer.write_u64_le(*i as u64);
        }
        Value::Float(f) => {
            writer.write_bytes(&f.to_le_bytes());
        }
        Value::Double(f) => {
            writer.write_bytes(&f.to_le_bytes());
        }
        Value::Decimal(s) => {
            write_length_encoded_string(writer, s);
        }
        Value::Text(s) => {
            write_length_encoded_string(writer, s);
        }
        Value::Bytes(b) => {
            write_length_encoded_bytes(writer, b);
        }
        Value::Json(j) => {
            let s = j.to_string();
            write_length_encoded_string(writer, &s);
        }
        Value::Date(days) => {
            // MySQL DATE binary format: length + year(2) + month(1) + day(1)
            // We have days since epoch, need to convert
            encode_binary_date(writer, *days);
        }
        Value::Time(micros) => {
            // MySQL TIME binary format
            encode_binary_time(writer, *micros);
        }
        Value::Timestamp(micros) | Value::TimestampTz(micros) => {
            // MySQL DATETIME binary format
            encode_binary_datetime(writer, *micros);
        }
        Value::Uuid(bytes) => {
            write_length_encoded_bytes(writer, bytes);
        }
        Value::Array(arr) => {
            // Encode arrays as JSON
            let s = serde_json::to_string(arr).unwrap_or_default();
            write_length_encoded_string(writer, &s);
        }
    }
}

/// Write a length-encoded string.
fn write_length_encoded_string(writer: &mut PacketWriter, s: &str) {
    write_length_encoded_bytes(writer, s.as_bytes());
}

/// Write length-encoded bytes.
fn write_length_encoded_bytes(writer: &mut PacketWriter, data: &[u8]) {
    let len = data.len();
    if len < 251 {
        writer.write_u8(len as u8);
    } else if len < 0x10000 {
        writer.write_u8(0xFC);
        writer.write_u16_le(len as u16);
    } else if len < 0x0100_0000 {
        writer.write_u8(0xFD);
        writer.write_u8((len & 0xFF) as u8);
        writer.write_u8(((len >> 8) & 0xFF) as u8);
        writer.write_u8(((len >> 16) & 0xFF) as u8);
    } else {
        writer.write_u8(0xFE);
        writer.write_u64_le(len as u64);
    }
    writer.write_bytes(data);
}

/// Encode a date value (days since epoch) to MySQL binary format.
fn encode_binary_date(writer: &mut PacketWriter, days: i32) {
    // Convert days since Unix epoch (1970-01-01) to year/month/day
    // Using a simplified algorithm
    let (year, month, day) = days_to_ymd(days);

    if year == 0 && month == 0 && day == 0 {
        // Zero date - send length 0
        writer.write_u8(0);
    } else {
        writer.write_u8(4); // length
        writer.write_u16_le(year as u16);
        writer.write_u8(month as u8);
        writer.write_u8(day as u8);
    }
}

/// Encode a time value (microseconds since midnight) to MySQL binary format.
fn encode_binary_time(writer: &mut PacketWriter, micros: i64) {
    let is_negative = micros < 0;
    let micros = micros.unsigned_abs();

    let total_seconds = micros / 1_000_000;
    let microseconds = (micros % 1_000_000) as u32;

    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    // For times > 24 hours, we need to include days
    let days = hours / 24;
    let hours = hours % 24;

    if days == 0 && hours == 0 && minutes == 0 && seconds == 0 && microseconds == 0 {
        writer.write_u8(0); // length 0 for zero time
    } else if microseconds == 0 {
        writer.write_u8(8); // length without microseconds
        writer.write_u8(if is_negative { 1 } else { 0 });
        writer.write_u32_le(days as u32);
        writer.write_u8(hours as u8);
        writer.write_u8(minutes as u8);
        writer.write_u8(seconds as u8);
    } else {
        writer.write_u8(12); // length with microseconds
        writer.write_u8(if is_negative { 1 } else { 0 });
        writer.write_u32_le(days as u32);
        writer.write_u8(hours as u8);
        writer.write_u8(minutes as u8);
        writer.write_u8(seconds as u8);
        writer.write_u32_le(microseconds);
    }
}

/// Encode a datetime value (microseconds since epoch) to MySQL binary format.
fn encode_binary_datetime(writer: &mut PacketWriter, micros: i64) {
    // Convert microseconds since Unix epoch to date/time components
    let total_seconds = micros / 1_000_000;
    let microseconds = (micros % 1_000_000).unsigned_abs() as u32;

    // Days since epoch
    let days = (total_seconds / 86400) as i32;
    let time_of_day = (total_seconds % 86400).unsigned_abs();

    let (year, month, day) = days_to_ymd(days);
    let hour = (time_of_day / 3600) as u8;
    let minute = ((time_of_day % 3600) / 60) as u8;
    let second = (time_of_day % 60) as u8;

    if year == 0
        && month == 0
        && day == 0
        && hour == 0
        && minute == 0
        && second == 0
        && microseconds == 0
    {
        writer.write_u8(0); // Zero datetime
    } else if hour == 0 && minute == 0 && second == 0 && microseconds == 0 {
        writer.write_u8(4); // Date only
        writer.write_u16_le(year as u16);
        writer.write_u8(month as u8);
        writer.write_u8(day as u8);
    } else if microseconds == 0 {
        writer.write_u8(7); // Date + time without microseconds
        writer.write_u16_le(year as u16);
        writer.write_u8(month as u8);
        writer.write_u8(day as u8);
        writer.write_u8(hour);
        writer.write_u8(minute);
        writer.write_u8(second);
    } else {
        writer.write_u8(11); // Full datetime with microseconds
        writer.write_u16_le(year as u16);
        writer.write_u8(month as u8);
        writer.write_u8(day as u8);
        writer.write_u8(hour);
        writer.write_u8(minute);
        writer.write_u8(second);
        writer.write_u32_le(microseconds);
    }
}

/// Convert days since Unix epoch to (year, month, day).
///
/// Uses the civil calendar algorithm from Howard Hinnant.
/// Unix epoch is 1970-01-01 (day 0).
fn days_to_ymd(days: i32) -> (i32, i32, i32) {
    // Shift epoch from 1970-01-01 to 0000-03-01 (simplifies leap year handling)
    // 719468 is the number of days from 0000-03-01 to 1970-01-01
    let z = days + 719_468;

    // Compute era (400-year period)
    let era = if z >= 0 {
        z / 146_097
    } else {
        (z - 146_096) / 146_097
    };
    let doe = (z - era * 146_097) as u32; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // year of era [0, 399]
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // month in [0, 11] starting from March
    let d = doy - (153 * mp + 2) / 5 + 1; // day [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // month [1, 12]

    // Adjust year if month is Jan or Feb (we shifted to March-based year)
    let year = if m <= 2 { y + 1 } else { y };

    (year, m as i32, d as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_stmt_prepare_packet() {
        let packet = build_stmt_prepare_packet("SELECT * FROM users WHERE id = ?", 0);

        // Check header
        assert_eq!(packet[3], 0); // sequence_id

        // Check payload starts with StmtPrepare command
        assert_eq!(packet[4], Command::StmtPrepare as u8);

        // Check SQL follows
        assert_eq!(&packet[5..], b"SELECT * FROM users WHERE id = ?");
    }

    #[test]
    fn test_build_stmt_close_packet() {
        let packet = build_stmt_close_packet(42, 0);

        // Header (4 bytes) + command (1) + stmt_id (4) = 9 bytes total
        assert_eq!(packet.len(), 9);
        assert_eq!(packet[4], Command::StmtClose as u8);

        // Statement ID
        let stmt_id = u32::from_le_bytes([packet[5], packet[6], packet[7], packet[8]]);
        assert_eq!(stmt_id, 42);
    }

    #[test]
    fn test_parse_stmt_prepare_ok() {
        // Construct a valid COM_STMT_PREPARE_OK response
        let data = [
            0x00, // status
            0x01, 0x00, 0x00, 0x00, // statement_id = 1
            0x03, 0x00, // num_columns = 3
            0x02, 0x00, // num_params = 2
            0x00, // reserved
            0x00, 0x00, // warnings = 0
        ];

        let result = parse_stmt_prepare_ok(&data).unwrap();
        assert_eq!(result.statement_id, 1);
        assert_eq!(result.num_columns, 3);
        assert_eq!(result.num_params, 2);
        assert_eq!(result.warnings, 0);
    }

    #[test]
    fn test_parse_stmt_prepare_ok_invalid() {
        // Too short
        assert!(parse_stmt_prepare_ok(&[0x00, 0x01]).is_none());

        // Wrong status byte
        let data = [
            0xFF, // error status
            0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
        ];
        assert!(parse_stmt_prepare_ok(&data).is_none());
    }

    #[test]
    fn test_build_stmt_execute_no_params() {
        let packet = build_stmt_execute_packet(1, &[], None, 0);

        // Check command
        assert_eq!(packet[4], Command::StmtExecute as u8);

        // Statement ID
        let stmt_id = u32::from_le_bytes([packet[5], packet[6], packet[7], packet[8]]);
        assert_eq!(stmt_id, 1);

        // Flags
        assert_eq!(packet[9], 0x00);

        // Iteration count
        let iter_count = u32::from_le_bytes([packet[10], packet[11], packet[12], packet[13]]);
        assert_eq!(iter_count, 1);
    }

    #[test]
    fn test_build_stmt_execute_with_params() {
        let params = vec![Value::Int(42), Value::Text("hello".to_string())];
        let packet = build_stmt_execute_packet(1, &params, None, 0);

        // Check command
        assert_eq!(packet[4], Command::StmtExecute as u8);

        // Statement ID = 1
        let stmt_id = u32::from_le_bytes([packet[5], packet[6], packet[7], packet[8]]);
        assert_eq!(stmt_id, 1);

        // Flags = 0
        assert_eq!(packet[9], 0x00);

        // Iteration count = 1
        let iter_count = u32::from_le_bytes([packet[10], packet[11], packet[12], packet[13]]);
        assert_eq!(iter_count, 1);

        // NULL bitmap (1 byte for 2 params)
        assert_eq!(packet[14], 0x00); // no NULLs

        // New params bound = 1
        assert_eq!(packet[15], 0x01);

        // Types: LONG (0x03) for Int, VAR_STRING (0xFD) for Text
        assert_eq!(packet[16], FieldType::Long as u8);
        assert_eq!(packet[17], 0x00); // flags
        assert_eq!(packet[18], FieldType::VarString as u8);
        assert_eq!(packet[19], 0x00); // flags
    }

    #[test]
    fn test_build_stmt_execute_with_null() {
        let params = vec![Value::Null, Value::Int(42)];
        let packet = build_stmt_execute_packet(1, &params, None, 0);

        // NULL bitmap should have bit 0 set
        assert_eq!(packet[14], 0x01);
    }

    #[test]
    fn test_value_to_field_type() {
        assert_eq!(value_to_field_type(&Value::Null), FieldType::Null);
        assert_eq!(value_to_field_type(&Value::Bool(true)), FieldType::Tiny);
        assert_eq!(value_to_field_type(&Value::TinyInt(1)), FieldType::Tiny);
        assert_eq!(value_to_field_type(&Value::SmallInt(1)), FieldType::Short);
        assert_eq!(value_to_field_type(&Value::Int(1)), FieldType::Long);
        assert_eq!(value_to_field_type(&Value::BigInt(1)), FieldType::LongLong);
        assert_eq!(value_to_field_type(&Value::Float(1.0)), FieldType::Float);
        assert_eq!(value_to_field_type(&Value::Double(1.0)), FieldType::Double);
        assert_eq!(
            value_to_field_type(&Value::Text(String::new())),
            FieldType::VarString
        );
        assert_eq!(value_to_field_type(&Value::Bytes(vec![])), FieldType::Blob);
    }

    #[test]
    fn test_days_to_ymd() {
        // Unix epoch
        assert_eq!(days_to_ymd(0), (1970, 1, 1));

        // 2000-01-01 is day 10957
        assert_eq!(days_to_ymd(10957), (2000, 1, 1));

        // 2024-02-29 (leap year) is day 19782
        assert_eq!(days_to_ymd(19782), (2024, 2, 29));
    }
}
