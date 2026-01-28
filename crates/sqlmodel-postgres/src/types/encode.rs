//! PostgreSQL type encoding (Rust → PostgreSQL).
//!
//! This module provides traits and implementations for encoding Rust values
//! to PostgreSQL wire format in both text and binary representations.

// The Error type is intentionally large to provide rich error context.
// This is a design decision made at the workspace level.
#![allow(clippy::result_large_err)]
// Truncation is expected when converting between timestamp types
#![allow(clippy::cast_possible_truncation)]

use sqlmodel_core::Error;
use sqlmodel_core::error::TypeError;

use super::oid;

/// Wire format for PostgreSQL values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Format {
    /// Text format (human-readable strings)
    #[default]
    Text,
    /// Binary format (PostgreSQL native binary representation)
    Binary,
}

impl Format {
    /// Get the format code for the wire protocol (0 = text, 1 = binary).
    #[must_use]
    pub const fn code(self) -> i16 {
        match self {
            Format::Text => 0,
            Format::Binary => 1,
        }
    }

    /// Create format from wire protocol code.
    #[must_use]
    pub const fn from_code(code: i16) -> Self {
        match code {
            1 => Format::Binary,
            _ => Format::Text,
        }
    }
}

/// Encode a value to PostgreSQL text format.
pub trait TextEncode {
    /// Encode self to a text string for PostgreSQL.
    fn encode_text(&self) -> String;
}

/// Encode a value to PostgreSQL binary format.
pub trait BinaryEncode {
    /// Encode self to binary bytes for PostgreSQL.
    fn encode_binary(&self, buf: &mut Vec<u8>);
}

/// Combined encoding trait that supports both formats.
pub trait Encode: TextEncode + BinaryEncode {
    /// Get the PostgreSQL OID for this type.
    fn oid() -> u32;

    /// Encode to the specified format.
    fn encode(&self, format: Format, buf: &mut Vec<u8>) {
        match format {
            Format::Text => buf.extend(self.encode_text().as_bytes()),
            Format::Binary => self.encode_binary(buf),
        }
    }
}

// ==================== Boolean ====================

impl TextEncode for bool {
    fn encode_text(&self) -> String {
        if *self { "t" } else { "f" }.to_string()
    }
}

impl BinaryEncode for bool {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.push(u8::from(*self));
    }
}

impl Encode for bool {
    fn oid() -> u32 {
        oid::BOOL
    }
}

// ==================== Integers ====================

impl TextEncode for i8 {
    fn encode_text(&self) -> String {
        self.to_string()
    }
}

impl BinaryEncode for i8 {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.push(*self as u8);
    }
}

impl TextEncode for i16 {
    fn encode_text(&self) -> String {
        self.to_string()
    }
}

impl BinaryEncode for i16 {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }
}

impl Encode for i16 {
    fn oid() -> u32 {
        oid::INT2
    }
}

impl TextEncode for i32 {
    fn encode_text(&self) -> String {
        self.to_string()
    }
}

impl BinaryEncode for i32 {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }
}

impl Encode for i32 {
    fn oid() -> u32 {
        oid::INT4
    }
}

impl TextEncode for i64 {
    fn encode_text(&self) -> String {
        self.to_string()
    }
}

impl BinaryEncode for i64 {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }
}

impl Encode for i64 {
    fn oid() -> u32 {
        oid::INT8
    }
}

// ==================== Unsigned Integers ====================
// PostgreSQL doesn't have unsigned types, so we encode as the next larger signed type

impl TextEncode for u32 {
    fn encode_text(&self) -> String {
        // Encode as i64 to avoid overflow
        i64::from(*self).to_string()
    }
}

impl BinaryEncode for u32 {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        // Encode as i64
        i64::from(*self).encode_binary(buf);
    }
}

// ==================== Floating Point ====================

impl TextEncode for f32 {
    fn encode_text(&self) -> String {
        if self.is_nan() {
            "NaN".to_string()
        } else if self.is_infinite() {
            if self.is_sign_positive() {
                "Infinity".to_string()
            } else {
                "-Infinity".to_string()
            }
        } else {
            self.to_string()
        }
    }
}

impl BinaryEncode for f32 {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }
}

impl Encode for f32 {
    fn oid() -> u32 {
        oid::FLOAT4
    }
}

impl TextEncode for f64 {
    fn encode_text(&self) -> String {
        if self.is_nan() {
            "NaN".to_string()
        } else if self.is_infinite() {
            if self.is_sign_positive() {
                "Infinity".to_string()
            } else {
                "-Infinity".to_string()
            }
        } else {
            self.to_string()
        }
    }
}

impl BinaryEncode for f64 {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }
}

impl Encode for f64 {
    fn oid() -> u32 {
        oid::FLOAT8
    }
}

// ==================== Strings ====================

impl TextEncode for str {
    fn encode_text(&self) -> String {
        self.to_string()
    }
}

impl BinaryEncode for str {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }
}

impl TextEncode for String {
    fn encode_text(&self) -> String {
        self.clone()
    }
}

impl BinaryEncode for String {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }
}

impl Encode for String {
    fn oid() -> u32 {
        oid::TEXT
    }
}

impl TextEncode for &str {
    fn encode_text(&self) -> String {
        (*self).to_string()
    }
}

impl BinaryEncode for &str {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }
}

// ==================== Bytes ====================

impl TextEncode for [u8] {
    fn encode_text(&self) -> String {
        // Encode as hex with \x prefix
        let mut s = String::with_capacity(2 + self.len() * 2);
        s.push_str("\\x");
        for byte in self {
            s.push_str(&format!("{byte:02x}"));
        }
        s
    }
}

impl BinaryEncode for [u8] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }
}

impl TextEncode for Vec<u8> {
    fn encode_text(&self) -> String {
        self.as_slice().encode_text()
    }
}

impl BinaryEncode for Vec<u8> {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }
}

impl Encode for Vec<u8> {
    fn oid() -> u32 {
        oid::BYTEA
    }
}

// ==================== UUID ====================

/// Encode a UUID (16-byte array) to PostgreSQL format.
impl TextEncode for [u8; 16] {
    fn encode_text(&self) -> String {
        format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            self[0],
            self[1],
            self[2],
            self[3],
            self[4],
            self[5],
            self[6],
            self[7],
            self[8],
            self[9],
            self[10],
            self[11],
            self[12],
            self[13],
            self[14],
            self[15]
        )
    }
}

impl BinaryEncode for [u8; 16] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }
}

// ==================== Date/Time ====================

/// Days since Unix epoch (1970-01-01).
/// PostgreSQL uses 2000-01-01 as its epoch, so we need to convert.
const PG_EPOCH_OFFSET_DAYS: i32 = 10_957; // Days from 1970-01-01 to 2000-01-01

/// Microseconds since Unix epoch for timestamp.
/// PostgreSQL uses 2000-01-01 as its epoch.
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000; // Micros from 1970 to 2000

/// Encode a date as days since Unix epoch.
///
/// Input is days since 1970-01-01, output is days since 2000-01-01.
pub fn encode_date_days(days_since_unix: i32) -> i32 {
    days_since_unix - PG_EPOCH_OFFSET_DAYS
}

/// Encode a timestamp as microseconds since Unix epoch.
///
/// Input is microseconds since 1970-01-01 00:00:00 UTC.
/// Output is microseconds since 2000-01-01 00:00:00 UTC (PostgreSQL epoch).
pub fn encode_timestamp_micros(micros_since_unix: i64) -> i64 {
    micros_since_unix - PG_EPOCH_OFFSET_MICROS
}

/// Encode a time as microseconds since midnight.
pub fn encode_time_micros(micros_since_midnight: i64) -> i64 {
    micros_since_midnight
}

// ==================== Optional Values ====================

impl<T: TextEncode> TextEncode for Option<T> {
    fn encode_text(&self) -> String {
        match self {
            Some(v) => v.encode_text(),
            None => String::new(),
        }
    }
}

impl<T: BinaryEncode> BinaryEncode for Option<T> {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        if let Some(v) = self {
            v.encode_binary(buf);
        }
    }
}

// ==================== Value Encoding ====================

use sqlmodel_core::value::Value;

/// Encode a dynamic Value to the specified format.
///
/// Returns the encoded bytes and the appropriate OID.
pub fn encode_value(value: &Value, format: Format) -> Result<(Vec<u8>, u32), Error> {
    let mut buf = Vec::new();
    let type_oid = match value {
        Value::Null => return Ok((vec![], oid::UNKNOWN)),
        Value::Bool(v) => {
            match format {
                Format::Text => buf.extend(v.encode_text().as_bytes()),
                Format::Binary => v.encode_binary(&mut buf),
            }
            oid::BOOL
        }
        Value::TinyInt(v) => {
            match format {
                Format::Text => buf.extend(v.encode_text().as_bytes()),
                Format::Binary => {
                    // PostgreSQL doesn't have int1, encode as int2
                    i16::from(*v).encode_binary(&mut buf);
                }
            }
            oid::INT2
        }
        Value::SmallInt(v) => {
            match format {
                Format::Text => buf.extend(v.encode_text().as_bytes()),
                Format::Binary => v.encode_binary(&mut buf),
            }
            oid::INT2
        }
        Value::Int(v) => {
            match format {
                Format::Text => buf.extend(v.encode_text().as_bytes()),
                Format::Binary => v.encode_binary(&mut buf),
            }
            oid::INT4
        }
        Value::BigInt(v) => {
            match format {
                Format::Text => buf.extend(v.encode_text().as_bytes()),
                Format::Binary => v.encode_binary(&mut buf),
            }
            oid::INT8
        }
        Value::Float(v) => {
            match format {
                Format::Text => buf.extend(v.encode_text().as_bytes()),
                Format::Binary => v.encode_binary(&mut buf),
            }
            oid::FLOAT4
        }
        Value::Double(v) => {
            match format {
                Format::Text => buf.extend(v.encode_text().as_bytes()),
                Format::Binary => v.encode_binary(&mut buf),
            }
            oid::FLOAT8
        }
        Value::Decimal(v) => {
            buf.extend(v.as_bytes());
            oid::NUMERIC
        }
        Value::Text(v) => {
            buf.extend(v.as_bytes());
            oid::TEXT
        }
        Value::Bytes(v) => {
            match format {
                Format::Text => buf.extend(v.encode_text().as_bytes()),
                Format::Binary => v.encode_binary(&mut buf),
            }
            oid::BYTEA
        }
        Value::Date(days) => {
            match format {
                Format::Text => {
                    // Convert days since Unix epoch to YYYY-MM-DD
                    let date = days_to_date_string(*days);
                    buf.extend(date.as_bytes());
                }
                Format::Binary => {
                    encode_date_days(*days).encode_binary(&mut buf);
                }
            }
            oid::DATE
        }
        Value::Time(micros) => {
            match format {
                Format::Text => {
                    let time = micros_to_time_string(*micros);
                    buf.extend(time.as_bytes());
                }
                Format::Binary => {
                    micros.encode_binary(&mut buf);
                }
            }
            oid::TIME
        }
        Value::Timestamp(micros) => {
            match format {
                Format::Text => {
                    let ts = micros_to_timestamp_string(*micros);
                    buf.extend(ts.as_bytes());
                }
                Format::Binary => {
                    encode_timestamp_micros(*micros).encode_binary(&mut buf);
                }
            }
            oid::TIMESTAMP
        }
        Value::TimestampTz(micros) => {
            match format {
                Format::Text => {
                    let ts = micros_to_timestamp_string(*micros);
                    buf.extend(ts.as_bytes());
                    buf.extend(b"+00");
                }
                Format::Binary => {
                    encode_timestamp_micros(*micros).encode_binary(&mut buf);
                }
            }
            oid::TIMESTAMPTZ
        }
        Value::Uuid(bytes) => {
            match format {
                Format::Text => buf.extend(bytes.encode_text().as_bytes()),
                Format::Binary => bytes.encode_binary(&mut buf),
            }
            oid::UUID
        }
        Value::Json(json) => {
            buf.extend(json.to_string().as_bytes());
            oid::JSON
        }
        Value::Array(values) => {
            return Err(Error::Type(TypeError {
                expected: "scalar value",
                actual: format!("array with {} elements", values.len()),
                column: None,
                rust_type: None,
            }));
        }
        Value::Default => return Ok((vec![], oid::UNKNOWN)),
    };

    Ok((buf, type_oid))
}

// ==================== Helper Functions ====================

/// Convert days since Unix epoch to YYYY-MM-DD string.
#[allow(clippy::many_single_char_names)]
fn days_to_date_string(days: i32) -> String {
    // Julian day conversion algorithm - variable names follow the standard algorithm
    // Reference: https://howardhinnant.github.io/date_algorithms.html
    let unix_epoch_jd = 2_440_588; // Julian day of 1970-01-01
    let jd = unix_epoch_jd + i64::from(days);

    // Julian day to Gregorian date conversion
    let l = jd + 68_569;
    let n = 4 * l / 146_097;
    let l = l - (146_097 * n + 3) / 4;
    let i = 4000 * (l + 1) / 1_461_001;
    let l = l - 1461 * i / 4 + 31;
    let j = 80 * l / 2447;
    let d = l - 2447 * j / 80;
    let l = j / 11;
    let m = j + 2 - 12 * l;
    let y = 100 * (n - 49) + i + l;

    format!("{y:04}-{m:02}-{d:02}")
}

/// Convert microseconds since midnight to HH:MM:SS.ffffff string.
fn micros_to_time_string(micros: i64) -> String {
    let total_secs = micros / 1_000_000;
    let frac_micros = micros % 1_000_000;
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;

    if frac_micros == 0 {
        format!("{hours:02}:{mins:02}:{secs:02}")
    } else {
        format!("{hours:02}:{mins:02}:{secs:02}.{frac_micros:06}")
    }
}

/// Convert microseconds since Unix epoch to timestamp string.
fn micros_to_timestamp_string(micros: i64) -> String {
    let days = micros / (86_400 * 1_000_000);
    let day_micros = micros % (86_400 * 1_000_000);

    let date = days_to_date_string(days as i32);
    let time = micros_to_time_string(day_micros);

    format!("{date} {time}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool_encoding() {
        assert_eq!(true.encode_text(), "t");
        assert_eq!(false.encode_text(), "f");

        let mut buf = Vec::new();
        true.encode_binary(&mut buf);
        assert_eq!(buf, vec![1]);

        buf.clear();
        false.encode_binary(&mut buf);
        assert_eq!(buf, vec![0]);
    }

    #[test]
    fn test_integer_encoding() {
        assert_eq!(42i32.encode_text(), "42");
        assert_eq!((-100i64).encode_text(), "-100");

        let mut buf = Vec::new();
        42i32.encode_binary(&mut buf);
        assert_eq!(buf, vec![0, 0, 0, 42]);

        buf.clear();
        256i32.encode_binary(&mut buf);
        assert_eq!(buf, vec![0, 0, 1, 0]);
    }

    #[test]
    fn test_float_encoding() {
        assert_eq!(f64::NAN.encode_text(), "NaN");
        assert_eq!(f64::INFINITY.encode_text(), "Infinity");
        assert_eq!(f64::NEG_INFINITY.encode_text(), "-Infinity");
    }

    #[test]
    fn test_bytea_encoding() {
        let bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
        assert_eq!(bytes.encode_text(), "\\xdeadbeef");
    }

    #[test]
    fn test_uuid_encoding() {
        let uuid: [u8; 16] = [
            0x55, 0x06, 0x9c, 0x47, 0x86, 0x8b, 0x4a, 0x08, 0xa4, 0x7f, 0x36, 0x53, 0x26, 0x2b,
            0xce, 0x35,
        ];
        assert_eq!(uuid.encode_text(), "55069c47-868b-4a08-a47f-3653262bce35");
    }

    #[test]
    fn test_format_code() {
        assert_eq!(Format::Text.code(), 0);
        assert_eq!(Format::Binary.code(), 1);
        assert_eq!(Format::from_code(0), Format::Text);
        assert_eq!(Format::from_code(1), Format::Binary);
    }
}
