//! MySQL type system and type conversion.
//!
//! This module provides:
//! - MySQL field type constants
//! - Encoding/decoding between Rust types and MySQL wire format
//! - Type information for column definitions
//!
//! # MySQL Type System
//!
//! MySQL uses field type codes in result sets and binary protocol.
//! The encoding differs between text protocol (all strings) and
//! binary protocol (type-specific binary encoding).

#![allow(clippy::cast_possible_truncation)]

use sqlmodel_core::Value;

/// MySQL field type codes.
///
/// These are the `MYSQL_TYPE_*` constants from the MySQL C API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FieldType {
    /// DECIMAL (MYSQL_TYPE_DECIMAL)
    Decimal = 0x00,
    /// TINYINT (MYSQL_TYPE_TINY)
    Tiny = 0x01,
    /// SMALLINT (MYSQL_TYPE_SHORT)
    Short = 0x02,
    /// INT (MYSQL_TYPE_LONG)
    Long = 0x03,
    /// FLOAT (MYSQL_TYPE_FLOAT)
    Float = 0x04,
    /// DOUBLE (MYSQL_TYPE_DOUBLE)
    Double = 0x05,
    /// NULL (MYSQL_TYPE_NULL)
    Null = 0x06,
    /// TIMESTAMP (MYSQL_TYPE_TIMESTAMP)
    Timestamp = 0x07,
    /// BIGINT (MYSQL_TYPE_LONGLONG)
    LongLong = 0x08,
    /// MEDIUMINT (MYSQL_TYPE_INT24)
    Int24 = 0x09,
    /// DATE (MYSQL_TYPE_DATE)
    Date = 0x0A,
    /// TIME (MYSQL_TYPE_TIME)
    Time = 0x0B,
    /// DATETIME (MYSQL_TYPE_DATETIME)
    DateTime = 0x0C,
    /// YEAR (MYSQL_TYPE_YEAR)
    Year = 0x0D,
    /// NEWDATE (MYSQL_TYPE_NEWDATE) - internal use
    NewDate = 0x0E,
    /// VARCHAR (MYSQL_TYPE_VARCHAR)
    VarChar = 0x0F,
    /// BIT (MYSQL_TYPE_BIT)
    Bit = 0x10,
    /// TIMESTAMP2 (MYSQL_TYPE_TIMESTAMP2) - MySQL 5.6+
    Timestamp2 = 0x11,
    /// DATETIME2 (MYSQL_TYPE_DATETIME2) - MySQL 5.6+
    DateTime2 = 0x12,
    /// TIME2 (MYSQL_TYPE_TIME2) - MySQL 5.6+
    Time2 = 0x13,
    /// JSON (MYSQL_TYPE_JSON) - MySQL 5.7.8+
    Json = 0xF5,
    /// NEWDECIMAL (MYSQL_TYPE_NEWDECIMAL)
    NewDecimal = 0xF6,
    /// ENUM (MYSQL_TYPE_ENUM)
    Enum = 0xF7,
    /// SET (MYSQL_TYPE_SET)
    Set = 0xF8,
    /// TINYBLOB (MYSQL_TYPE_TINY_BLOB)
    TinyBlob = 0xF9,
    /// MEDIUMBLOB (MYSQL_TYPE_MEDIUM_BLOB)
    MediumBlob = 0xFA,
    /// LONGBLOB (MYSQL_TYPE_LONG_BLOB)
    LongBlob = 0xFB,
    /// BLOB (MYSQL_TYPE_BLOB)
    Blob = 0xFC,
    /// VARCHAR (MYSQL_TYPE_VAR_STRING)
    VarString = 0xFD,
    /// CHAR (MYSQL_TYPE_STRING)
    String = 0xFE,
    /// GEOMETRY (MYSQL_TYPE_GEOMETRY)
    Geometry = 0xFF,
}

impl FieldType {
    /// Parse a field type from a byte.
    #[must_use]
    pub fn from_u8(value: u8) -> Self {
        match value {
            0x00 => FieldType::Decimal,
            0x01 => FieldType::Tiny,
            0x02 => FieldType::Short,
            0x03 => FieldType::Long,
            0x04 => FieldType::Float,
            0x05 => FieldType::Double,
            0x06 => FieldType::Null,
            0x07 => FieldType::Timestamp,
            0x08 => FieldType::LongLong,
            0x09 => FieldType::Int24,
            0x0A => FieldType::Date,
            0x0B => FieldType::Time,
            0x0C => FieldType::DateTime,
            0x0D => FieldType::Year,
            0x0E => FieldType::NewDate,
            0x0F => FieldType::VarChar,
            0x10 => FieldType::Bit,
            0x11 => FieldType::Timestamp2,
            0x12 => FieldType::DateTime2,
            0x13 => FieldType::Time2,
            0xF5 => FieldType::Json,
            0xF6 => FieldType::NewDecimal,
            0xF7 => FieldType::Enum,
            0xF8 => FieldType::Set,
            0xF9 => FieldType::TinyBlob,
            0xFA => FieldType::MediumBlob,
            0xFB => FieldType::LongBlob,
            0xFC => FieldType::Blob,
            0xFD => FieldType::VarString,
            0xFE => FieldType::String,
            0xFF => FieldType::Geometry,
            _ => FieldType::String, // Unknown types treated as string
        }
    }

    /// Check if this is an integer type.
    #[must_use]
    pub const fn is_integer(self) -> bool {
        matches!(
            self,
            FieldType::Tiny
                | FieldType::Short
                | FieldType::Long
                | FieldType::LongLong
                | FieldType::Int24
                | FieldType::Year
        )
    }

    /// Check if this is a floating-point type.
    #[must_use]
    pub const fn is_float(self) -> bool {
        matches!(self, FieldType::Float | FieldType::Double)
    }

    /// Check if this is a decimal type.
    #[must_use]
    pub const fn is_decimal(self) -> bool {
        matches!(self, FieldType::Decimal | FieldType::NewDecimal)
    }

    /// Check if this is a string type.
    #[must_use]
    pub const fn is_string(self) -> bool {
        matches!(
            self,
            FieldType::VarChar
                | FieldType::VarString
                | FieldType::String
                | FieldType::Enum
                | FieldType::Set
        )
    }

    /// Check if this is a binary/blob type.
    #[must_use]
    pub const fn is_blob(self) -> bool {
        matches!(
            self,
            FieldType::TinyBlob
                | FieldType::MediumBlob
                | FieldType::LongBlob
                | FieldType::Blob
                | FieldType::Geometry
        )
    }

    /// Check if this is a date/time type.
    #[must_use]
    pub const fn is_temporal(self) -> bool {
        matches!(
            self,
            FieldType::Date
                | FieldType::Time
                | FieldType::DateTime
                | FieldType::Timestamp
                | FieldType::NewDate
                | FieldType::Timestamp2
                | FieldType::DateTime2
                | FieldType::Time2
        )
    }

    /// Get the type name as a string.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            FieldType::Decimal => "DECIMAL",
            FieldType::Tiny => "TINYINT",
            FieldType::Short => "SMALLINT",
            FieldType::Long => "INT",
            FieldType::Float => "FLOAT",
            FieldType::Double => "DOUBLE",
            FieldType::Null => "NULL",
            FieldType::Timestamp => "TIMESTAMP",
            FieldType::LongLong => "BIGINT",
            FieldType::Int24 => "MEDIUMINT",
            FieldType::Date => "DATE",
            FieldType::Time => "TIME",
            FieldType::DateTime => "DATETIME",
            FieldType::Year => "YEAR",
            FieldType::NewDate => "DATE",
            FieldType::VarChar => "VARCHAR",
            FieldType::Bit => "BIT",
            FieldType::Timestamp2 => "TIMESTAMP",
            FieldType::DateTime2 => "DATETIME",
            FieldType::Time2 => "TIME",
            FieldType::Json => "JSON",
            FieldType::NewDecimal => "DECIMAL",
            FieldType::Enum => "ENUM",
            FieldType::Set => "SET",
            FieldType::TinyBlob => "TINYBLOB",
            FieldType::MediumBlob => "MEDIUMBLOB",
            FieldType::LongBlob => "LONGBLOB",
            FieldType::Blob => "BLOB",
            FieldType::VarString => "VARCHAR",
            FieldType::String => "CHAR",
            FieldType::Geometry => "GEOMETRY",
        }
    }
}

/// Column flags in result set metadata.
#[allow(dead_code)]
pub mod column_flags {
    pub const NOT_NULL: u16 = 1;
    pub const PRIMARY_KEY: u16 = 2;
    pub const UNIQUE_KEY: u16 = 4;
    pub const MULTIPLE_KEY: u16 = 8;
    pub const BLOB: u16 = 16;
    pub const UNSIGNED: u16 = 32;
    pub const ZEROFILL: u16 = 64;
    pub const BINARY: u16 = 128;
    pub const ENUM: u16 = 256;
    pub const AUTO_INCREMENT: u16 = 512;
    pub const TIMESTAMP: u16 = 1024;
    pub const SET: u16 = 2048;
    pub const NO_DEFAULT_VALUE: u16 = 4096;
    pub const ON_UPDATE_NOW: u16 = 8192;
    pub const NUM: u16 = 32768;
}

/// Column definition from a result set.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// Catalog name (always "def")
    pub catalog: String,
    /// Schema (database) name
    pub schema: String,
    /// Table name (or alias)
    pub table: String,
    /// Original table name
    pub org_table: String,
    /// Column name (or alias)
    pub name: String,
    /// Original column name
    pub org_name: String,
    /// Character set number
    pub charset: u16,
    /// Column length
    pub column_length: u32,
    /// Column type
    pub column_type: FieldType,
    /// Column flags
    pub flags: u16,
    /// Number of decimals
    pub decimals: u8,
}

impl ColumnDef {
    /// Check if the column is NOT NULL.
    #[must_use]
    pub const fn is_not_null(&self) -> bool {
        self.flags & column_flags::NOT_NULL != 0
    }

    /// Check if the column is a primary key.
    #[must_use]
    pub const fn is_primary_key(&self) -> bool {
        self.flags & column_flags::PRIMARY_KEY != 0
    }

    /// Check if the column is unsigned.
    #[must_use]
    pub const fn is_unsigned(&self) -> bool {
        self.flags & column_flags::UNSIGNED != 0
    }

    /// Check if the column is auto-increment.
    #[must_use]
    pub const fn is_auto_increment(&self) -> bool {
        self.flags & column_flags::AUTO_INCREMENT != 0
    }

    /// Check if the column is binary.
    #[must_use]
    pub const fn is_binary(&self) -> bool {
        self.flags & column_flags::BINARY != 0
    }

    /// Check if the column is a BLOB type.
    #[must_use]
    pub const fn is_blob(&self) -> bool {
        self.flags & column_flags::BLOB != 0
    }
}

/// Decode a text protocol value to a sqlmodel Value.
///
/// In text protocol, all values are transmitted as strings.
/// This function parses the string based on the column type.
pub fn decode_text_value(field_type: FieldType, data: &[u8], is_unsigned: bool) -> Value {
    let text = String::from_utf8_lossy(data);

    match field_type {
        // TINYINT (8-bit)
        FieldType::Tiny => {
            if is_unsigned {
                text.parse::<u8>().map_or_else(
                    |_| Value::Text(text.into_owned()),
                    |v| Value::TinyInt(v as i8),
                )
            } else {
                text.parse::<i8>()
                    .map_or_else(|_| Value::Text(text.into_owned()), Value::TinyInt)
            }
        }
        // SMALLINT (16-bit)
        FieldType::Short | FieldType::Year => {
            if is_unsigned {
                text.parse::<u16>().map_or_else(
                    |_| Value::Text(text.into_owned()),
                    |v| Value::SmallInt(v as i16),
                )
            } else {
                text.parse::<i16>()
                    .map_or_else(|_| Value::Text(text.into_owned()), Value::SmallInt)
            }
        }
        // INT/MEDIUMINT (32-bit)
        FieldType::Long | FieldType::Int24 => {
            if is_unsigned {
                text.parse::<u32>()
                    .map_or_else(|_| Value::Text(text.into_owned()), |v| Value::Int(v as i32))
            } else {
                text.parse::<i32>()
                    .map_or_else(|_| Value::Text(text.into_owned()), Value::Int)
            }
        }
        // BIGINT (64-bit)
        FieldType::LongLong => {
            if is_unsigned {
                text.parse::<u64>().map_or_else(
                    |_| Value::Text(text.into_owned()),
                    |v| Value::BigInt(v as i64),
                )
            } else {
                text.parse::<i64>()
                    .map_or_else(|_| Value::Text(text.into_owned()), Value::BigInt)
            }
        }

        // FLOAT (32-bit)
        FieldType::Float => text
            .parse::<f32>()
            .map_or_else(|_| Value::Text(text.into_owned()), Value::Float),

        // DOUBLE (64-bit)
        FieldType::Double => text
            .parse::<f64>()
            .map_or_else(|_| Value::Text(text.into_owned()), Value::Double),

        // Decimal (keep as text to preserve precision)
        FieldType::Decimal | FieldType::NewDecimal => Value::Text(text.into_owned()),

        // Binary/blob types
        FieldType::TinyBlob
        | FieldType::MediumBlob
        | FieldType::LongBlob
        | FieldType::Blob
        | FieldType::Geometry
        | FieldType::Bit => Value::Bytes(data.to_vec()),

        // JSON
        FieldType::Json => {
            // Try to parse as JSON, fall back to text
            serde_json::from_str(&text).map_or_else(|_| Value::Text(text.into_owned()), Value::Json)
        }

        // NULL type
        FieldType::Null => Value::Null,

        // All other types (strings, dates, times) as text
        _ => Value::Text(text.into_owned()),
    }
}

/// Decode a binary protocol value to a sqlmodel Value.
///
/// In binary protocol, values are encoded in type-specific binary formats.
pub fn decode_binary_value(field_type: FieldType, data: &[u8], is_unsigned: bool) -> Value {
    match field_type {
        // TINY (1 byte)
        FieldType::Tiny => {
            if data.is_empty() {
                return Value::Null;
            }
            // Both signed and unsigned map to i8 (interpretation differs at application level)
            let _ = is_unsigned;
            Value::TinyInt(data[0] as i8)
        }

        // SHORT (2 bytes, little-endian)
        FieldType::Short | FieldType::Year => {
            if data.len() < 2 {
                return Value::Null;
            }
            let val = u16::from_le_bytes([data[0], data[1]]);
            // Both signed and unsigned map to i16 (interpretation differs at application level)
            let _ = is_unsigned;
            Value::SmallInt(val as i16)
        }

        // LONG/INT24 (4 bytes, little-endian)
        FieldType::Long | FieldType::Int24 => {
            if data.len() < 4 {
                return Value::Null;
            }
            let val = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            // Both signed and unsigned map to i32 (interpretation differs at application level)
            let _ = is_unsigned;
            Value::Int(val as i32)
        }

        // LONGLONG (8 bytes, little-endian)
        FieldType::LongLong => {
            if data.len() < 8 {
                return Value::Null;
            }
            let val = u64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            // Both signed and unsigned map to i64 (interpretation differs at application level)
            let _ = is_unsigned;
            Value::BigInt(val as i64)
        }

        // FLOAT (4 bytes)
        FieldType::Float => {
            if data.len() < 4 {
                return Value::Null;
            }
            let val = f32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            Value::Float(val)
        }

        // DOUBLE (8 bytes)
        FieldType::Double => {
            if data.len() < 8 {
                return Value::Null;
            }
            let val = f64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            Value::Double(val)
        }

        // Binary types
        FieldType::TinyBlob
        | FieldType::MediumBlob
        | FieldType::LongBlob
        | FieldType::Blob
        | FieldType::Geometry
        | FieldType::Bit => Value::Bytes(data.to_vec()),

        // JSON
        FieldType::Json => {
            let text = String::from_utf8_lossy(data);
            serde_json::from_str(&text).map_or_else(|_| Value::Bytes(data.to_vec()), Value::Json)
        }

        // Date/Time types - binary format encodes components
        FieldType::Date
        | FieldType::NewDate
        | FieldType::Time
        | FieldType::DateTime
        | FieldType::Timestamp
        | FieldType::Time2
        | FieldType::DateTime2
        | FieldType::Timestamp2 => {
            // For now, keep as text (we'd need more complex parsing for binary date/time)
            // The text representation is more portable
            Value::Text(decode_binary_datetime(field_type, data))
        }

        // Decimal types - keep as text for precision
        FieldType::Decimal | FieldType::NewDecimal => {
            Value::Text(String::from_utf8_lossy(data).into_owned())
        }

        // String types
        _ => Value::Text(String::from_utf8_lossy(data).into_owned()),
    }
}

/// Decode binary date/time values to ISO format strings.
fn decode_binary_datetime(field_type: FieldType, data: &[u8]) -> String {
    match field_type {
        FieldType::Date | FieldType::NewDate => {
            if data.len() >= 4 {
                let year = u16::from_le_bytes([data[0], data[1]]);
                let month = data[2];
                let day = data[3];
                format!("{year:04}-{month:02}-{day:02}")
            } else {
                // Empty or insufficient data returns zero date
                "0000-00-00".to_string()
            }
        }

        FieldType::Time | FieldType::Time2 => {
            if data.len() >= 8 {
                let is_negative = data[0] != 0;
                let days = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
                let hours = data[5];
                let minutes = data[6];
                let seconds = data[7];
                let total_hours = days * 24 + u32::from(hours);
                let sign = if is_negative { "-" } else { "" };
                format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}")
            } else {
                // Empty or insufficient data returns zero time
                "00:00:00".to_string()
            }
        }

        FieldType::DateTime
        | FieldType::Timestamp
        | FieldType::DateTime2
        | FieldType::Timestamp2 => {
            if data.len() >= 7 {
                let year = u16::from_le_bytes([data[0], data[1]]);
                let month = data[2];
                let day = data[3];
                let hour = data[4];
                let minute = data[5];
                let second = data[6];

                if data.len() >= 11 {
                    let microseconds = u32::from_le_bytes([data[7], data[8], data[9], data[10]]);
                    format!(
                        "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{microseconds:06}"
                    )
                } else {
                    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
                }
            } else if data.len() >= 4 {
                let year = u16::from_le_bytes([data[0], data[1]]);
                let month = data[2];
                let day = data[3];
                format!("{year:04}-{month:02}-{day:02} 00:00:00")
            } else {
                "0000-00-00 00:00:00".to_string()
            }
        }

        _ => String::from_utf8_lossy(data).into_owned(),
    }
}

/// Encode a sqlmodel Value for binary protocol.
///
/// Returns the encoded bytes for the value.
pub fn encode_binary_value(value: &Value, field_type: FieldType) -> Vec<u8> {
    match value {
        Value::Null => vec![],

        Value::Bool(b) => vec![u8::from(*b)],

        Value::TinyInt(i) => vec![*i as u8],

        Value::SmallInt(i) => i.to_le_bytes().to_vec(),

        Value::Int(i) => i.to_le_bytes().to_vec(),

        Value::BigInt(i) => match field_type {
            FieldType::Tiny => vec![*i as u8],
            FieldType::Short | FieldType::Year => (*i as i16).to_le_bytes().to_vec(),
            FieldType::Long | FieldType::Int24 => (*i as i32).to_le_bytes().to_vec(),
            _ => i.to_le_bytes().to_vec(),
        },

        Value::Float(f) => f.to_le_bytes().to_vec(),

        Value::Double(f) => f.to_le_bytes().to_vec(),

        Value::Decimal(s) => encode_length_prefixed_bytes(s.as_bytes()),

        Value::Text(s) => {
            let bytes = s.as_bytes();
            encode_length_prefixed_bytes(bytes)
        }

        Value::Bytes(b) => encode_length_prefixed_bytes(b),

        Value::Json(j) => {
            let s = j.to_string();
            encode_length_prefixed_bytes(s.as_bytes())
        }

        // Date is days since epoch (i32)
        Value::Date(d) => d.to_le_bytes().to_vec(),

        // Time is microseconds since midnight (i64)
        Value::Time(t) => t.to_le_bytes().to_vec(),

        // Timestamp is microseconds since epoch (i64)
        Value::Timestamp(t) | Value::TimestampTz(t) => t.to_le_bytes().to_vec(),

        // UUID is 16 bytes
        Value::Uuid(u) => encode_length_prefixed_bytes(u),

        // Array - encode as JSON for MySQL
        Value::Array(arr) => {
            let json = serde_json::to_string(arr).unwrap_or_default();
            encode_length_prefixed_bytes(json.as_bytes())
        }

        // Default should never reach encode - query builder puts "DEFAULT"
        // directly in SQL text. Return empty bytes as defensive fallback.
        Value::Default => vec![],
    }
}

/// Decode a binary protocol value and return bytes consumed.
///
/// This is used when parsing binary result set rows where we need to know
/// how many bytes each value occupies.
///
/// # Returns
///
/// Tuple of (decoded value, bytes consumed)
pub fn decode_binary_value_with_len(
    data: &[u8],
    field_type: FieldType,
    _is_unsigned: bool,
) -> (Value, usize) {
    match field_type {
        // Fixed-size integer types
        FieldType::Tiny => {
            if data.is_empty() {
                return (Value::Null, 0);
            }
            (Value::TinyInt(data[0] as i8), 1)
        }

        FieldType::Short | FieldType::Year => {
            if data.len() < 2 {
                return (Value::Null, 0);
            }
            let val = u16::from_le_bytes([data[0], data[1]]);
            (Value::SmallInt(val as i16), 2)
        }

        FieldType::Long | FieldType::Int24 => {
            if data.len() < 4 {
                return (Value::Null, 0);
            }
            let val = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            (Value::Int(val as i32), 4)
        }

        FieldType::LongLong => {
            if data.len() < 8 {
                return (Value::Null, 0);
            }
            let val = u64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            (Value::BigInt(val as i64), 8)
        }

        FieldType::Float => {
            if data.len() < 4 {
                return (Value::Null, 0);
            }
            let val = f32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            (Value::Float(val), 4)
        }

        FieldType::Double => {
            if data.len() < 8 {
                return (Value::Null, 0);
            }
            let val = f64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            (Value::Double(val), 8)
        }

        // Date types - variable length with length prefix byte
        FieldType::Date | FieldType::NewDate => {
            if data.is_empty() {
                return (Value::Null, 0);
            }
            let len = data[0] as usize;
            if len == 0 {
                return (Value::Text("0000-00-00".to_string()), 1);
            }
            if data.len() < 1 + len || len < 4 {
                return (Value::Null, 1);
            }
            let year = u16::from_le_bytes([data[1], data[2]]);
            let month = data[3];
            let day = data[4];
            (
                Value::Text(format!("{year:04}-{month:02}-{day:02}")),
                1 + len,
            )
        }

        FieldType::Time | FieldType::Time2 => {
            if data.is_empty() {
                return (Value::Null, 0);
            }
            let len = data[0] as usize;
            if len == 0 {
                return (Value::Text("00:00:00".to_string()), 1);
            }
            if data.len() < 1 + len || len < 8 {
                return (Value::Text("00:00:00".to_string()), 1);
            }
            let is_negative = data[1] != 0;
            let days = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
            let hours = data[6];
            let minutes = data[7];
            let seconds = data[8];
            let total_hours = days * 24 + u32::from(hours);
            let sign = if is_negative { "-" } else { "" };
            (
                Value::Text(format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}")),
                1 + len,
            )
        }

        FieldType::DateTime
        | FieldType::Timestamp
        | FieldType::DateTime2
        | FieldType::Timestamp2 => {
            if data.is_empty() {
                return (Value::Null, 0);
            }
            let len = data[0] as usize;
            if len == 0 {
                return (Value::Text("0000-00-00 00:00:00".to_string()), 1);
            }
            if data.len() < 1 + len {
                return (Value::Null, 1);
            }
            if len >= 4 {
                let year = u16::from_le_bytes([data[1], data[2]]);
                let month = data[3];
                let day = data[4];
                if len >= 7 {
                    let hour = data[5];
                    let minute = data[6];
                    let second = data[7];
                    if len >= 11 {
                        let microseconds =
                            u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
                        return (
                            Value::Text(format!(
                                "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{microseconds:06}"
                            )),
                            1 + len,
                        );
                    }
                    return (
                        Value::Text(format!(
                            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}"
                        )),
                        1 + len,
                    );
                }
                return (
                    Value::Text(format!("{year:04}-{month:02}-{day:02} 00:00:00")),
                    1 + len,
                );
            }
            (Value::Text("0000-00-00 00:00:00".to_string()), 1 + len)
        }

        // Variable-length types with length-encoded prefix
        FieldType::Decimal
        | FieldType::NewDecimal
        | FieldType::VarChar
        | FieldType::VarString
        | FieldType::String
        | FieldType::Enum
        | FieldType::Set
        | FieldType::TinyBlob
        | FieldType::MediumBlob
        | FieldType::LongBlob
        | FieldType::Blob
        | FieldType::Json
        | FieldType::Geometry
        | FieldType::Bit => {
            let (str_len, prefix_len) = read_lenenc_int(data);
            if str_len == 0 && prefix_len == 0 {
                return (Value::Null, 0);
            }
            let total_len = prefix_len + str_len;
            if data.len() < total_len {
                return (Value::Null, prefix_len);
            }
            let str_data = &data[prefix_len..total_len];
            let value = match field_type {
                FieldType::TinyBlob
                | FieldType::MediumBlob
                | FieldType::LongBlob
                | FieldType::Blob
                | FieldType::Geometry
                | FieldType::Bit => Value::Bytes(str_data.to_vec()),
                FieldType::Json => {
                    let text = String::from_utf8_lossy(str_data);
                    serde_json::from_str(&text)
                        .map_or_else(|_| Value::Bytes(str_data.to_vec()), Value::Json)
                }
                _ => Value::Text(String::from_utf8_lossy(str_data).into_owned()),
            };
            (value, total_len)
        }

        // Null type
        FieldType::Null => (Value::Null, 0),
    }
}

/// Read a length-encoded integer from data.
///
/// Returns (value, bytes consumed).
fn read_lenenc_int(data: &[u8]) -> (usize, usize) {
    if data.is_empty() {
        return (0, 0);
    }
    match data[0] {
        0..=250 => (data[0] as usize, 1),
        0xFC => {
            if data.len() < 3 {
                return (0, 1);
            }
            let val = u16::from_le_bytes([data[1], data[2]]) as usize;
            (val, 3)
        }
        0xFD => {
            if data.len() < 4 {
                return (0, 1);
            }
            let val = u32::from_le_bytes([data[1], data[2], data[3], 0]) as usize;
            (val, 4)
        }
        0xFE => {
            if data.len() < 9 {
                return (0, 1);
            }
            let val = u64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ]) as usize;
            (val, 9)
        }
        // 0xFB is NULL indicator, 0xFF is error - both handled by the exhaustive match above
        251..=255 => (0, 1),
    }
}

/// Encode bytes with a length prefix.
fn encode_length_prefixed_bytes(data: &[u8]) -> Vec<u8> {
    let len = data.len();
    let mut result = Vec::with_capacity(len + 9);

    if len < 251 {
        result.push(len as u8);
    } else if len < 0x10000 {
        result.push(0xFC);
        result.extend_from_slice(&(len as u16).to_le_bytes());
    } else if len < 0x0100_0000 {
        result.push(0xFD);
        result.push((len & 0xFF) as u8);
        result.push(((len >> 8) & 0xFF) as u8);
        result.push(((len >> 16) & 0xFF) as u8);
    } else {
        result.push(0xFE);
        result.extend_from_slice(&(len as u64).to_le_bytes());
    }

    result.extend_from_slice(data);
    result
}

/// Escape a string for use in MySQL text protocol.
///
/// This escapes special characters to prevent SQL injection.
fn escape_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 2);
    result.push('\'');
    for ch in s.chars() {
        match ch {
            '\'' => result.push_str("''"),
            '\\' => result.push_str("\\\\"),
            '\0' => result.push_str("\\0"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\x1a' => result.push_str("\\Z"), // Ctrl+Z
            _ => result.push(ch),
        }
    }
    result.push('\'');
    result
}

/// Escape bytes for use in MySQL text protocol.
fn escape_bytes(data: &[u8]) -> String {
    let mut result = String::with_capacity(data.len() * 2 + 3);
    result.push_str("X'");
    for byte in data {
        result.push_str(&format!("{byte:02X}"));
    }
    result.push('\'');
    result
}

/// Format a sqlmodel Value for use in MySQL text protocol SQL.
///
/// This converts a Value to a properly escaped SQL literal string.
pub fn format_value_for_sql(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Value::TinyInt(i) => i.to_string(),
        Value::SmallInt(i) => i.to_string(),
        Value::Int(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Float(f) => {
            if f.is_nan() {
                "NULL".to_string()
            } else if f.is_infinite() {
                if f.is_sign_positive() {
                    "1e308".to_string() // Close to infinity
                } else {
                    "-1e308".to_string()
                }
            } else {
                f.to_string()
            }
        }
        Value::Double(f) => {
            if f.is_nan() {
                "NULL".to_string()
            } else if f.is_infinite() {
                if f.is_sign_positive() {
                    "1e308".to_string()
                } else {
                    "-1e308".to_string()
                }
            } else {
                f.to_string()
            }
        }
        Value::Decimal(s) => s.clone(),
        Value::Text(s) => escape_string(s),
        Value::Bytes(b) => escape_bytes(b),
        Value::Json(j) => escape_string(&j.to_string()),
        Value::Date(d) => format!("'{}'", d), // ISO date format
        Value::Time(t) => format!("'{}'", t), // microseconds as-is for now
        Value::Timestamp(t) | Value::TimestampTz(t) => format!("'{}'", t),
        Value::Uuid(u) => escape_bytes(u),
        Value::Array(arr) => {
            // MySQL doesn't have native arrays, encode as JSON
            let json = serde_json::to_string(arr).unwrap_or_default();
            escape_string(&json)
        }
        Value::Default => "DEFAULT".to_string(),
    }
}

/// Interpolate parameters into a SQL query string.
///
/// Replaces `$1`, `$2`, etc. placeholders with properly escaped values.
/// Also supports `?` placeholders (MySQL style) - replaced in order.
pub fn interpolate_params(sql: &str, params: &[Value]) -> String {
    if params.is_empty() {
        return sql.to_string();
    }

    let mut result = String::with_capacity(sql.len() + params.len() * 20);
    let mut chars = sql.chars().peekable();
    let mut param_index = 0;

    while let Some(ch) = chars.next() {
        match ch {
            // MySQL-style ? placeholder
            '?' => {
                if param_index < params.len() {
                    result.push_str(&format_value_for_sql(&params[param_index]));
                    param_index += 1;
                } else {
                    result.push('?');
                }
            }
            // PostgreSQL-style $N placeholder
            '$' => {
                let mut num_str = String::new();
                while let Some(&next_ch) = chars.peek() {
                    if next_ch.is_ascii_digit() {
                        num_str.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                if num_str.is_empty() {
                    result.push('$');
                } else if let Ok(n) = num_str.parse::<usize>() {
                    if n > 0 && n <= params.len() {
                        result.push_str(&format_value_for_sql(&params[n - 1]));
                    } else {
                        result.push('$');
                        result.push_str(&num_str);
                    }
                } else {
                    result.push('$');
                    result.push_str(&num_str);
                }
            }
            // Handle string literals (don't replace placeholders inside)
            '\'' => {
                result.push(ch);
                while let Some(next_ch) = chars.next() {
                    result.push(next_ch);
                    if next_ch == '\'' {
                        // Check for escaped quote
                        if chars.peek() == Some(&'\'') {
                            result.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                }
            }
            // Handle double-quoted identifiers
            '"' => {
                result.push(ch);
                while let Some(next_ch) = chars.next() {
                    result.push(next_ch);
                    if next_ch == '"' {
                        if chars.peek() == Some(&'"') {
                            result.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                }
            }
            // Handle backtick identifiers (MySQL-specific)
            '`' => {
                result.push(ch);
                while let Some(next_ch) = chars.next() {
                    result.push(next_ch);
                    if next_ch == '`' {
                        if chars.peek() == Some(&'`') {
                            result.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                }
            }
            _ => result.push(ch),
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_string() {
        assert_eq!(escape_string("hello"), "'hello'");
        assert_eq!(escape_string("it's"), "'it''s'");
        assert_eq!(escape_string("a\\b"), "'a\\\\b'");
        assert_eq!(escape_string("line\nbreak"), "'line\\nbreak'");
    }

    #[test]
    fn test_format_value() {
        assert_eq!(format_value_for_sql(&Value::Null), "NULL");
        assert_eq!(format_value_for_sql(&Value::Int(42)), "42");
        assert_eq!(
            format_value_for_sql(&Value::Text("hello".to_string())),
            "'hello'"
        );
        assert_eq!(format_value_for_sql(&Value::Bool(true)), "TRUE");
    }

    #[test]
    fn test_interpolate_params_question_mark() {
        let sql = "SELECT * FROM users WHERE id = ? AND name = ?";
        let params = vec![Value::Int(1), Value::Text("Alice".to_string())];
        let result = interpolate_params(sql, &params);
        assert_eq!(
            result,
            "SELECT * FROM users WHERE id = 1 AND name = 'Alice'"
        );
    }

    #[test]
    fn test_interpolate_params_dollar() {
        let sql = "SELECT * FROM users WHERE id = $1 AND name = $2";
        let params = vec![Value::Int(1), Value::Text("Alice".to_string())];
        let result = interpolate_params(sql, &params);
        assert_eq!(
            result,
            "SELECT * FROM users WHERE id = 1 AND name = 'Alice'"
        );
    }

    #[test]
    fn test_interpolate_no_replace_in_string() {
        let sql = "SELECT * FROM users WHERE name = '$1' AND id = ?";
        let params = vec![Value::Int(42)];
        let result = interpolate_params(sql, &params);
        assert_eq!(result, "SELECT * FROM users WHERE name = '$1' AND id = 42");
    }

    #[test]
    fn test_field_type_from_u8() {
        assert_eq!(FieldType::from_u8(0x01), FieldType::Tiny);
        assert_eq!(FieldType::from_u8(0x03), FieldType::Long);
        assert_eq!(FieldType::from_u8(0x08), FieldType::LongLong);
        assert_eq!(FieldType::from_u8(0xFC), FieldType::Blob);
        assert_eq!(FieldType::from_u8(0xF5), FieldType::Json);
    }

    #[test]
    fn test_field_type_categories() {
        assert!(FieldType::Tiny.is_integer());
        assert!(FieldType::Long.is_integer());
        assert!(FieldType::LongLong.is_integer());

        assert!(FieldType::Float.is_float());
        assert!(FieldType::Double.is_float());

        assert!(FieldType::Decimal.is_decimal());
        assert!(FieldType::NewDecimal.is_decimal());

        assert!(FieldType::VarChar.is_string());
        assert!(FieldType::String.is_string());

        assert!(FieldType::Blob.is_blob());
        assert!(FieldType::TinyBlob.is_blob());

        assert!(FieldType::Date.is_temporal());
        assert!(FieldType::DateTime.is_temporal());
        assert!(FieldType::Timestamp.is_temporal());
    }

    #[test]
    fn test_decode_text_integer() {
        let val = decode_text_value(FieldType::Long, b"42", false);
        assert!(matches!(val, Value::Int(42)));

        let val = decode_text_value(FieldType::LongLong, b"-100", false);
        assert!(matches!(val, Value::BigInt(-100)));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_decode_text_float() {
        let val = decode_text_value(FieldType::Double, b"3.14", false);
        if let Value::Double(f) = val {
            assert!((f - 3.14).abs() < 0.001);
        } else {
            panic!("Expected double");
        }
    }

    #[test]
    fn test_decode_text_string() {
        let val = decode_text_value(FieldType::VarChar, b"hello", false);
        assert!(matches!(val, Value::Text(s) if s == "hello"));
    }

    #[test]
    fn test_decode_binary_tiny() {
        let val = decode_binary_value(FieldType::Tiny, &[42], false);
        assert!(matches!(val, Value::TinyInt(42)));

        let val = decode_binary_value(FieldType::Tiny, &[255u8], true);
        assert!(matches!(val, Value::TinyInt(-1))); // 255u8 as i8 = -1

        let val = decode_binary_value(FieldType::Tiny, &[255], false);
        assert!(matches!(val, Value::TinyInt(-1)));
    }

    #[test]
    fn test_decode_binary_long() {
        let val = decode_binary_value(FieldType::Long, &[0x2A, 0x00, 0x00, 0x00], false);
        assert!(matches!(val, Value::Int(42)));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_decode_binary_double() {
        let pi_bytes = 3.14159_f64.to_le_bytes();
        let val = decode_binary_value(FieldType::Double, &pi_bytes, false);
        if let Value::Double(f) = val {
            assert!((f - 3.14159).abs() < 0.00001);
        } else {
            panic!("Expected double");
        }
    }

    #[test]
    fn test_column_flags() {
        let col = ColumnDef {
            catalog: "def".to_string(),
            schema: "test".to_string(),
            table: "users".to_string(),
            org_table: "users".to_string(),
            name: "id".to_string(),
            org_name: "id".to_string(),
            charset: 33,
            column_length: 11,
            column_type: FieldType::Long,
            flags: column_flags::NOT_NULL
                | column_flags::PRIMARY_KEY
                | column_flags::AUTO_INCREMENT
                | column_flags::UNSIGNED,
            decimals: 0,
        };

        assert!(col.is_not_null());
        assert!(col.is_primary_key());
        assert!(col.is_auto_increment());
        assert!(col.is_unsigned());
        assert!(!col.is_binary());
    }

    #[test]
    fn test_encode_length_prefixed() {
        // Short string
        let result = encode_length_prefixed_bytes(b"hello");
        assert_eq!(result[0], 5);
        assert_eq!(&result[1..], b"hello");

        // Empty
        let result = encode_length_prefixed_bytes(b"");
        assert_eq!(result, vec![0]);
    }
}
