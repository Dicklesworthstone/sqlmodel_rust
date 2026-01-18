//! PostgreSQL type decoding (PostgreSQL → Rust).
//!
//! This module provides traits and implementations for decoding PostgreSQL
//! wire format values to Rust types in both text and binary representations.

// The Error type is intentionally large to provide rich error context.
// This is a design decision made at the workspace level.
#![allow(clippy::result_large_err)]
// Truncation is expected when converting between timestamp types
#![allow(clippy::cast_possible_truncation)]

use sqlmodel_core::Error;
use sqlmodel_core::error::TypeError;
use sqlmodel_core::value::Value;

use super::encode::Format;
use super::oid;

/// Decode a value from PostgreSQL text format.
pub trait TextDecode: Sized {
    /// Decode from a PostgreSQL text representation.
    fn decode_text(s: &str) -> Result<Self, Error>;
}

/// Decode a value from PostgreSQL binary format.
pub trait BinaryDecode: Sized {
    /// Decode from PostgreSQL binary representation.
    fn decode_binary(data: &[u8]) -> Result<Self, Error>;
}

/// Combined decoding trait that supports both formats.
pub trait Decode: TextDecode + BinaryDecode {
    /// Decode from the specified format.
    fn decode(data: &[u8], format: Format) -> Result<Self, Error> {
        match format {
            Format::Text => {
                let s = std::str::from_utf8(data).map_err(|_| {
                    Error::Type(TypeError {
                        expected: "valid UTF-8",
                        actual: format!("invalid bytes: {:?}", &data[..data.len().min(20)]),
                        column: None,
                        rust_type: None,
                    })
                })?;
                Self::decode_text(s)
            }
            Format::Binary => Self::decode_binary(data),
        }
    }
}

// ==================== Boolean ====================

impl TextDecode for bool {
    fn decode_text(s: &str) -> Result<Self, Error> {
        match s {
            "t" | "true" | "TRUE" | "1" | "y" | "yes" | "on" => Ok(true),
            "f" | "false" | "FALSE" | "0" | "n" | "no" | "off" => Ok(false),
            _ => Err(type_error("bool", s)),
        }
    }
}

impl BinaryDecode for bool {
    fn decode_binary(data: &[u8]) -> Result<Self, Error> {
        if data.len() != 1 {
            return Err(binary_length_error("bool", 1, data.len()));
        }
        Ok(data[0] != 0)
    }
}

impl Decode for bool {}

// ==================== Integers ====================

impl TextDecode for i8 {
    fn decode_text(s: &str) -> Result<Self, Error> {
        s.parse().map_err(|_| type_error("i8", s))
    }
}

impl BinaryDecode for i8 {
    fn decode_binary(data: &[u8]) -> Result<Self, Error> {
        if data.len() != 1 {
            return Err(binary_length_error("i8", 1, data.len()));
        }
        Ok(data[0] as i8)
    }
}

impl Decode for i8 {}

impl TextDecode for i16 {
    fn decode_text(s: &str) -> Result<Self, Error> {
        s.parse().map_err(|_| type_error("int2", s))
    }
}

impl BinaryDecode for i16 {
    fn decode_binary(data: &[u8]) -> Result<Self, Error> {
        if data.len() != 2 {
            return Err(binary_length_error("int2", 2, data.len()));
        }
        Ok(i16::from_be_bytes([data[0], data[1]]))
    }
}

impl Decode for i16 {}

impl TextDecode for i32 {
    fn decode_text(s: &str) -> Result<Self, Error> {
        s.parse().map_err(|_| type_error("int4", s))
    }
}

impl BinaryDecode for i32 {
    fn decode_binary(data: &[u8]) -> Result<Self, Error> {
        if data.len() != 4 {
            return Err(binary_length_error("int4", 4, data.len()));
        }
        Ok(i32::from_be_bytes([data[0], data[1], data[2], data[3]]))
    }
}

impl Decode for i32 {}

impl TextDecode for i64 {
    fn decode_text(s: &str) -> Result<Self, Error> {
        s.parse().map_err(|_| type_error("int8", s))
    }
}

impl BinaryDecode for i64 {
    fn decode_binary(data: &[u8]) -> Result<Self, Error> {
        if data.len() != 8 {
            return Err(binary_length_error("int8", 8, data.len()));
        }
        Ok(i64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]))
    }
}

impl Decode for i64 {}

// ==================== Unsigned Integers ====================

impl TextDecode for u32 {
    fn decode_text(s: &str) -> Result<Self, Error> {
        // PostgreSQL sends OID as unsigned 32-bit
        s.parse().map_err(|_| type_error("oid", s))
    }
}

impl BinaryDecode for u32 {
    fn decode_binary(data: &[u8]) -> Result<Self, Error> {
        if data.len() != 4 {
            return Err(binary_length_error("oid", 4, data.len()));
        }
        Ok(u32::from_be_bytes([data[0], data[1], data[2], data[3]]))
    }
}

impl Decode for u32 {}

// ==================== Floating Point ====================

impl TextDecode for f32 {
    fn decode_text(s: &str) -> Result<Self, Error> {
        match s {
            "NaN" => Ok(f32::NAN),
            "Infinity" => Ok(f32::INFINITY),
            "-Infinity" => Ok(f32::NEG_INFINITY),
            _ => s.parse().map_err(|_| type_error("float4", s)),
        }
    }
}

impl BinaryDecode for f32 {
    fn decode_binary(data: &[u8]) -> Result<Self, Error> {
        if data.len() != 4 {
            return Err(binary_length_error("float4", 4, data.len()));
        }
        Ok(f32::from_be_bytes([data[0], data[1], data[2], data[3]]))
    }
}

impl Decode for f32 {}

impl TextDecode for f64 {
    fn decode_text(s: &str) -> Result<Self, Error> {
        match s {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            _ => s.parse().map_err(|_| type_error("float8", s)),
        }
    }
}

impl BinaryDecode for f64 {
    fn decode_binary(data: &[u8]) -> Result<Self, Error> {
        if data.len() != 8 {
            return Err(binary_length_error("float8", 8, data.len()));
        }
        Ok(f64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]))
    }
}

impl Decode for f64 {}

// ==================== Strings ====================

impl TextDecode for String {
    fn decode_text(s: &str) -> Result<Self, Error> {
        Ok(s.to_string())
    }
}

impl BinaryDecode for String {
    fn decode_binary(data: &[u8]) -> Result<Self, Error> {
        String::from_utf8(data.to_vec()).map_err(|_| {
            Error::Type(TypeError {
                expected: "valid UTF-8",
                actual: format!("invalid bytes: {:?}", &data[..data.len().min(20)]),
                column: None,
                rust_type: None,
            })
        })
    }
}

impl Decode for String {}

// ==================== Bytes (bytea) ====================

impl TextDecode for Vec<u8> {
    fn decode_text(s: &str) -> Result<Self, Error> {
        // PostgreSQL bytea can be in hex format (\x...) or escape format
        if let Some(hex) = s.strip_prefix("\\x") {
            decode_hex(hex)
        } else {
            // Escape format: \\ for backslash, \NNN for octal
            decode_bytea_escape(s)
        }
    }
}

impl BinaryDecode for Vec<u8> {
    fn decode_binary(data: &[u8]) -> Result<Self, Error> {
        Ok(data.to_vec())
    }
}

impl Decode for Vec<u8> {}

// ==================== UUID ====================

impl TextDecode for [u8; 16] {
    fn decode_text(s: &str) -> Result<Self, Error> {
        // Parse UUID string: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        let s = s.replace('-', "");
        if s.len() != 32 {
            return Err(type_error("uuid", s));
        }

        let mut bytes = [0u8; 16];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte =
                u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).map_err(|_| type_error("uuid", &s))?;
        }
        Ok(bytes)
    }
}

impl BinaryDecode for [u8; 16] {
    fn decode_binary(data: &[u8]) -> Result<Self, Error> {
        if data.len() != 16 {
            return Err(binary_length_error("uuid", 16, data.len()));
        }
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(data);
        Ok(bytes)
    }
}

impl Decode for [u8; 16] {}

// ==================== Date/Time ====================

/// PostgreSQL epoch offset: days from 1970-01-01 to 2000-01-01.
const PG_EPOCH_OFFSET_DAYS: i32 = 10_957;

/// PostgreSQL epoch offset: microseconds from 1970-01-01 to 2000-01-01.
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// Decode a date from PostgreSQL format.
///
/// Returns days since Unix epoch (1970-01-01).
pub fn decode_date_days(pg_days: i32) -> i32 {
    pg_days + PG_EPOCH_OFFSET_DAYS
}

/// Decode a timestamp from PostgreSQL format.
///
/// Returns microseconds since Unix epoch (1970-01-01 00:00:00 UTC).
pub fn decode_timestamp_micros(pg_micros: i64) -> i64 {
    pg_micros + PG_EPOCH_OFFSET_MICROS
}

/// Parse a date string in YYYY-MM-DD format.
///
/// Returns days since Unix epoch.
pub fn parse_date_string(s: &str) -> Result<i32, Error> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return Err(type_error("date", s));
    }

    let year: i32 = parts[0].parse().map_err(|_| type_error("date", s))?;
    let month: u32 = parts[1].parse().map_err(|_| type_error("date", s))?;
    let day: u32 = parts[2].parse().map_err(|_| type_error("date", s))?;

    // Convert to days since Unix epoch using a simple algorithm
    Ok(date_to_days(year, month, day))
}

/// Parse a time string in HH:MM:SS[.ffffff] format.
///
/// Returns microseconds since midnight.
pub fn parse_time_string(s: &str) -> Result<i64, Error> {
    let (time_part, micros_part) = if let Some(pos) = s.find('.') {
        (&s[..pos], Some(&s[pos + 1..]))
    } else {
        (s, None)
    };

    let parts: Vec<&str> = time_part.split(':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        return Err(type_error("time", s));
    }

    let hours: i64 = parts[0].parse().map_err(|_| type_error("time", s))?;
    let mins: i64 = parts[1].parse().map_err(|_| type_error("time", s))?;
    let secs: i64 = if parts.len() == 3 {
        parts[2].parse().map_err(|_| type_error("time", s))?
    } else {
        0
    };

    let mut micros = (hours * 3600 + mins * 60 + secs) * 1_000_000;

    if let Some(frac) = micros_part {
        // Pad or truncate to 6 digits
        let frac_str = if frac.len() > 6 { &frac[..6] } else { frac };
        let frac_micros: i64 = frac_str.parse().map_err(|_| type_error("time", s))?;
        let multiplier = 10_i64.pow(6 - frac_str.len() as u32);
        micros += frac_micros * multiplier;
    }

    Ok(micros)
}

/// Parse a timestamp string.
///
/// Returns microseconds since Unix epoch.
pub fn parse_timestamp_string(s: &str) -> Result<i64, Error> {
    // Handle formats: "YYYY-MM-DD HH:MM:SS[.ffffff]" or "YYYY-MM-DDTHH:MM:SS[.ffffff]"
    let s = s.replace('T', " ");

    // Remove timezone suffix if present
    let s = if let Some(pos) = s.find('+') {
        &s[..pos]
    } else if let Some(pos) = s.rfind('-') {
        // Check if this is date separator or timezone
        if pos > 10 { &s[..pos] } else { &s }
    } else {
        &s
    };

    let parts: Vec<&str> = s.split(' ').collect();
    if parts.len() != 2 {
        // Try just date
        if parts.len() == 1 {
            let days = parse_date_string(parts[0])?;
            return Ok(i64::from(days) * 86_400 * 1_000_000);
        }
        return Err(type_error("timestamp", s));
    }

    let days = parse_date_string(parts[0])?;
    let time_micros = parse_time_string(parts[1])?;

    Ok(i64::from(days) * 86_400 * 1_000_000 + time_micros)
}

// ==================== Value Decoding ====================

/// Decode a PostgreSQL value to a dynamic Value.
///
/// # Arguments
/// * `type_oid` - The PostgreSQL OID of the type
/// * `data` - The raw data bytes (None for NULL)
/// * `format` - Wire format (text or binary)
pub fn decode_value(type_oid: u32, data: Option<&[u8]>, format: Format) -> Result<Value, Error> {
    let Some(data) = data else {
        return Ok(Value::Null);
    };

    match (type_oid, format) {
        // Boolean
        (oid::BOOL, Format::Binary) => Ok(Value::Bool(bool::decode_binary(data)?)),
        (oid::BOOL, Format::Text) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            Ok(Value::Bool(bool::decode_text(s)?))
        }

        // Integers
        (oid::INT2, Format::Binary) => Ok(Value::SmallInt(i16::decode_binary(data)?)),
        (oid::INT2, Format::Text) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            Ok(Value::SmallInt(i16::decode_text(s)?))
        }

        (oid::INT4, Format::Binary) => Ok(Value::Int(i32::decode_binary(data)?)),
        (oid::INT4, Format::Text) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            Ok(Value::Int(i32::decode_text(s)?))
        }

        (oid::INT8, Format::Binary) => Ok(Value::BigInt(i64::decode_binary(data)?)),
        (oid::INT8, Format::Text) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            Ok(Value::BigInt(i64::decode_text(s)?))
        }

        // Floats
        (oid::FLOAT4, Format::Binary) => Ok(Value::Float(f32::decode_binary(data)?)),
        (oid::FLOAT4, Format::Text) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            Ok(Value::Float(f32::decode_text(s)?))
        }

        (oid::FLOAT8, Format::Binary) => Ok(Value::Double(f64::decode_binary(data)?)),
        (oid::FLOAT8, Format::Text) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            Ok(Value::Double(f64::decode_text(s)?))
        }

        // Numeric (decimal)
        (oid::NUMERIC, _) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            Ok(Value::Decimal(s.to_string()))
        }

        // Text types
        (oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::CHAR, _) => {
            Ok(Value::Text(String::decode_binary(data)?))
        }

        // Bytea
        (oid::BYTEA, Format::Binary) => Ok(Value::Bytes(data.to_vec())),
        (oid::BYTEA, Format::Text) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            Ok(Value::Bytes(Vec::<u8>::decode_text(s)?))
        }

        // Date
        (oid::DATE, Format::Binary) => {
            let pg_days = i32::decode_binary(data)?;
            Ok(Value::Date(decode_date_days(pg_days)))
        }
        (oid::DATE, Format::Text) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            Ok(Value::Date(parse_date_string(s)?))
        }

        // Time
        (oid::TIME | oid::TIMETZ, Format::Binary) => {
            let micros = i64::decode_binary(data)?;
            Ok(Value::Time(micros))
        }
        (oid::TIME | oid::TIMETZ, Format::Text) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            Ok(Value::Time(parse_time_string(s)?))
        }

        // Timestamp
        (oid::TIMESTAMP, Format::Binary) => {
            let pg_micros = i64::decode_binary(data)?;
            Ok(Value::Timestamp(decode_timestamp_micros(pg_micros)))
        }
        (oid::TIMESTAMP, Format::Text) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            Ok(Value::Timestamp(parse_timestamp_string(s)?))
        }

        // Timestamp with time zone
        (oid::TIMESTAMPTZ, Format::Binary) => {
            let pg_micros = i64::decode_binary(data)?;
            Ok(Value::TimestampTz(decode_timestamp_micros(pg_micros)))
        }
        (oid::TIMESTAMPTZ, Format::Text) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            Ok(Value::TimestampTz(parse_timestamp_string(s)?))
        }

        // UUID
        (oid::UUID, Format::Binary) => {
            let bytes = <[u8; 16]>::decode_binary(data)?;
            Ok(Value::Uuid(bytes))
        }
        (oid::UUID, Format::Text) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            let bytes = <[u8; 16]>::decode_text(s)?;
            Ok(Value::Uuid(bytes))
        }

        // JSON
        (oid::JSON, _) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            let json: serde_json::Value =
                serde_json::from_str(s).map_err(|e| type_error_with_source("json", s, e))?;
            Ok(Value::Json(json))
        }

        // JSONB (binary has a version byte prefix)
        (oid::JSONB, Format::Binary) => {
            if data.is_empty() {
                return Err(type_error("jsonb", "empty data"));
            }
            // Skip version byte (always 1 for now)
            let json_data = &data[1..];
            let s = std::str::from_utf8(json_data).map_err(utf8_error)?;
            let json: serde_json::Value =
                serde_json::from_str(s).map_err(|e| type_error_with_source("jsonb", s, e))?;
            Ok(Value::Json(json))
        }
        (oid::JSONB, Format::Text) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            let json: serde_json::Value =
                serde_json::from_str(s).map_err(|e| type_error_with_source("jsonb", s, e))?;
            Ok(Value::Json(json))
        }

        // OID type (treated as unsigned int)
        (oid::OID | oid::XID | oid::CID, Format::Binary) => {
            let v = u32::decode_binary(data)?;
            Ok(Value::Int(v as i32))
        }
        (oid::OID | oid::XID | oid::CID, Format::Text) => {
            let s = std::str::from_utf8(data).map_err(utf8_error)?;
            let v = u32::decode_text(s)?;
            Ok(Value::Int(v as i32))
        }

        // Unknown type - return as text
        (_, _) => Ok(Value::Text(String::decode_binary(data)?)),
    }
}

// ==================== Helper Functions ====================

fn type_error(expected: &'static str, value: impl std::fmt::Display) -> Error {
    Error::Type(TypeError {
        expected,
        actual: format!("invalid value: {}", value),
        column: None,
        rust_type: None,
    })
}

fn type_error_with_source<E: std::error::Error>(
    expected: &'static str,
    value: impl std::fmt::Display,
    source: E,
) -> Error {
    Error::Type(TypeError {
        expected,
        actual: format!("invalid value: {} ({})", value, source),
        column: None,
        rust_type: None,
    })
}

fn binary_length_error(type_name: &'static str, expected: usize, actual: usize) -> Error {
    Error::Type(TypeError {
        expected: type_name,
        actual: format!("expected {} bytes, got {}", expected, actual),
        column: None,
        rust_type: None,
    })
}

fn utf8_error(_e: std::str::Utf8Error) -> Error {
    Error::Type(TypeError {
        expected: "valid UTF-8",
        actual: "invalid UTF-8 bytes".to_string(),
        column: None,
        rust_type: None,
    })
}

/// Decode hex string to bytes.
fn decode_hex(s: &str) -> Result<Vec<u8>, Error> {
    let s = s.trim();
    if s.len() % 2 != 0 {
        return Err(type_error("bytea hex", s));
    }

    let mut bytes = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        let byte = u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| type_error("bytea hex", s))?;
        bytes.push(byte);
    }
    Ok(bytes)
}

/// Decode PostgreSQL bytea escape format.
fn decode_bytea_escape(s: &str) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.peek() {
                Some('\\') => {
                    chars.next();
                    bytes.push(b'\\');
                }
                Some(c) if c.is_ascii_digit() => {
                    // Octal escape: \NNN
                    let mut octal = String::with_capacity(3);
                    for _ in 0..3 {
                        if let Some(&c) = chars.peek() {
                            if c.is_ascii_digit() {
                                octal.push(c);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                    }
                    let byte =
                        u8::from_str_radix(&octal, 8).map_err(|_| type_error("bytea escape", s))?;
                    bytes.push(byte);
                }
                _ => {
                    // Just a backslash
                    bytes.push(b'\\');
                }
            }
        } else {
            bytes.push(c as u8);
        }
    }

    Ok(bytes)
}

/// Convert year/month/day to days since Unix epoch.
fn date_to_days(year: i32, month: u32, day: u32) -> i32 {
    // Algorithm from https://howardhinnant.github.io/date_algorithms.html
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i32 - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool_decoding() {
        assert!(bool::decode_text("t").unwrap());
        assert!(bool::decode_text("true").unwrap());
        assert!(!bool::decode_text("f").unwrap());
        assert!(!bool::decode_text("false").unwrap());

        assert!(bool::decode_binary(&[1]).unwrap());
        assert!(!bool::decode_binary(&[0]).unwrap());
    }

    #[test]
    fn test_integer_decoding() {
        assert_eq!(i32::decode_text("42").unwrap(), 42);
        assert_eq!(i32::decode_text("-100").unwrap(), -100);

        assert_eq!(i32::decode_binary(&[0, 0, 0, 42]).unwrap(), 42);
        assert_eq!(i32::decode_binary(&[0, 0, 1, 0]).unwrap(), 256);
    }

    #[test]
    fn test_float_decoding() {
        assert!(f64::decode_text("NaN").unwrap().is_nan());
        assert!(f64::decode_text("Infinity").unwrap().is_infinite());
        assert!(f64::decode_text("-Infinity").unwrap().is_infinite());
        // Use a value not close to any math constant to avoid clippy::approx_constant
        // and use epsilon comparison to avoid clippy::float_cmp
        let decoded = f64::decode_text("1.5").unwrap();
        assert!((decoded - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_bytea_hex_decoding() {
        let bytes = Vec::<u8>::decode_text("\\xdeadbeef").unwrap();
        assert_eq!(bytes, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_uuid_decoding() {
        let uuid = <[u8; 16]>::decode_text("55069c47-868b-4a08-a47f-3653262bce35").unwrap();
        assert_eq!(
            uuid,
            [
                0x55, 0x06, 0x9c, 0x47, 0x86, 0x8b, 0x4a, 0x08, 0xa4, 0x7f, 0x36, 0x53, 0x26, 0x2b,
                0xce, 0x35
            ]
        );
    }

    #[test]
    fn test_date_parsing() {
        // 2000-01-01 is day 10957 since Unix epoch
        assert_eq!(parse_date_string("2000-01-01").unwrap(), 10_957);
        // 1970-01-01 is day 0
        assert_eq!(parse_date_string("1970-01-01").unwrap(), 0);
    }

    #[test]
    fn test_time_parsing() {
        assert_eq!(parse_time_string("00:00:00").unwrap(), 0);
        assert_eq!(parse_time_string("01:00:00").unwrap(), 3_600_000_000);
        assert_eq!(
            parse_time_string("12:30:45.123456").unwrap(),
            45_045_123_456
        );
    }

    #[test]
    fn test_decode_value_null() {
        let value = decode_value(oid::INT4, None, Format::Binary).unwrap();
        assert!(matches!(value, Value::Null));
    }

    #[test]
    fn test_decode_value_int() {
        let value = decode_value(oid::INT4, Some(&[0, 0, 0, 42]), Format::Binary).unwrap();
        assert!(matches!(value, Value::Int(42)));
    }
}
