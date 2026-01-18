//! Type encoding and decoding between Rust and SQLite.
//!
//! SQLite has a simple type system with 5 storage classes:
//! - INTEGER: Signed integer (1, 2, 3, 4, 6, or 8 bytes)
//! - REAL: 8-byte IEEE floating point
//! - TEXT: UTF-8 or UTF-16 string
//! - BLOB: Binary data
//! - NULL: The NULL value
//!
//! We map these to/from sqlmodel-core's Value type.

use crate::ffi;
use sqlmodel_core::Value;
use std::ffi::{CStr, c_int, c_void};
use std::ptr;

/// Bind a Value to a prepared statement parameter.
///
/// # Safety
/// - `stmt` must be a valid, non-null prepared statement handle
/// - `index` must be a valid 1-based parameter index
pub unsafe fn bind_value(stmt: *mut ffi::sqlite3_stmt, index: c_int, value: &Value) -> c_int {
    // SAFETY: All FFI calls require unsafe in Rust 2024
    unsafe {
        match value {
            Value::Null => ffi::sqlite3_bind_null(stmt, index),

            Value::Bool(b) => ffi::sqlite3_bind_int(stmt, index, if *b { 1 } else { 0 }),

            Value::TinyInt(v) => ffi::sqlite3_bind_int(stmt, index, i32::from(*v)),

            Value::SmallInt(v) => ffi::sqlite3_bind_int(stmt, index, i32::from(*v)),

            Value::Int(v) => ffi::sqlite3_bind_int(stmt, index, *v),

            Value::BigInt(v) => ffi::sqlite3_bind_int64(stmt, index, *v),

            Value::Float(v) => ffi::sqlite3_bind_double(stmt, index, f64::from(*v)),

            Value::Double(v) => ffi::sqlite3_bind_double(stmt, index, *v),

            Value::Decimal(s) => {
                let bytes = s.as_bytes();
                ffi::sqlite3_bind_text(
                    stmt,
                    index,
                    bytes.as_ptr().cast(),
                    bytes.len() as c_int,
                    ffi::sqlite_transient(),
                )
            }

            Value::Text(s) => {
                let bytes = s.as_bytes();
                ffi::sqlite3_bind_text(
                    stmt,
                    index,
                    bytes.as_ptr().cast(),
                    bytes.len() as c_int,
                    ffi::sqlite_transient(),
                )
            }

            Value::Bytes(b) => ffi::sqlite3_bind_blob(
                stmt,
                index,
                b.as_ptr().cast(),
                b.len() as c_int,
                ffi::sqlite_transient(),
            ),

            // Date stored as ISO-8601 text (YYYY-MM-DD)
            Value::Date(days) => {
                let date = days_to_date(*days);
                let bytes = date.as_bytes();
                ffi::sqlite3_bind_text(
                    stmt,
                    index,
                    bytes.as_ptr().cast(),
                    bytes.len() as c_int,
                    ffi::sqlite_transient(),
                )
            }

            // Time stored as ISO-8601 text (HH:MM:SS.sss)
            Value::Time(micros) => {
                let time = micros_to_time(*micros);
                let bytes = time.as_bytes();
                ffi::sqlite3_bind_text(
                    stmt,
                    index,
                    bytes.as_ptr().cast(),
                    bytes.len() as c_int,
                    ffi::sqlite_transient(),
                )
            }

            // Timestamp stored as ISO-8601 text
            Value::Timestamp(micros) | Value::TimestampTz(micros) => {
                let ts = micros_to_timestamp(*micros);
                let bytes = ts.as_bytes();
                ffi::sqlite3_bind_text(
                    stmt,
                    index,
                    bytes.as_ptr().cast(),
                    bytes.len() as c_int,
                    ffi::sqlite_transient(),
                )
            }

            // UUID stored as 16-byte blob
            Value::Uuid(bytes) => ffi::sqlite3_bind_blob(
                stmt,
                index,
                bytes.as_ptr().cast(),
                16,
                ffi::sqlite_transient(),
            ),

            // JSON stored as text
            Value::Json(json) => {
                let s = json.to_string();
                let bytes = s.as_bytes();
                ffi::sqlite3_bind_text(
                    stmt,
                    index,
                    bytes.as_ptr().cast(),
                    bytes.len() as c_int,
                    ffi::sqlite_transient(),
                )
            }

            // Arrays stored as JSON text
            Value::Array(arr) => {
                let json = serde_json::Value::Array(arr.iter().map(value_to_json).collect());
                let s = json.to_string();
                let bytes = s.as_bytes();
                ffi::sqlite3_bind_text(
                    stmt,
                    index,
                    bytes.as_ptr().cast(),
                    bytes.len() as c_int,
                    ffi::sqlite_transient(),
                )
            }
        }
    }
}

/// Read a column value from a result row.
///
/// # Safety
/// - `stmt` must be a valid prepared statement that has just returned SQLITE_ROW
/// - `index` must be a valid 0-based column index
pub unsafe fn read_column(stmt: *mut ffi::sqlite3_stmt, index: c_int) -> Value {
    // SAFETY: All FFI calls require unsafe in Rust 2024
    unsafe {
        let col_type = ffi::sqlite3_column_type(stmt, index);

        match col_type {
            ffi::SQLITE_NULL => Value::Null,

            ffi::SQLITE_INTEGER => {
                let v = ffi::sqlite3_column_int64(stmt, index);
                // Choose the smallest representation
                if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
                    Value::Int(v as i32)
                } else {
                    Value::BigInt(v)
                }
            }

            ffi::SQLITE_FLOAT => {
                let v = ffi::sqlite3_column_double(stmt, index);
                Value::Double(v)
            }

            ffi::SQLITE_TEXT => {
                let ptr = ffi::sqlite3_column_text(stmt, index);
                let len = ffi::sqlite3_column_bytes(stmt, index);
                if ptr.is_null() {
                    Value::Null
                } else {
                    let slice = std::slice::from_raw_parts(ptr.cast::<u8>(), len as usize);
                    let s = String::from_utf8_lossy(slice).into_owned();
                    Value::Text(s)
                }
            }

            ffi::SQLITE_BLOB => {
                let ptr = ffi::sqlite3_column_blob(stmt, index);
                let len = ffi::sqlite3_column_bytes(stmt, index);
                if ptr.is_null() || len == 0 {
                    Value::Bytes(Vec::new())
                } else {
                    let slice = std::slice::from_raw_parts(ptr.cast::<u8>(), len as usize);
                    Value::Bytes(slice.to_vec())
                }
            }

            _ => Value::Null,
        }
    }
}

/// Get the column name from a result.
///
/// # Safety
/// - `stmt` must be a valid prepared statement
/// - `index` must be a valid 0-based column index
pub unsafe fn column_name(stmt: *mut ffi::sqlite3_stmt, index: c_int) -> Option<String> {
    // SAFETY: All FFI calls require unsafe in Rust 2024
    unsafe {
        let ptr = ffi::sqlite3_column_name(stmt, index);
        if ptr.is_null() {
            None
        } else {
            CStr::from_ptr(ptr).to_str().ok().map(String::from)
        }
    }
}

/// Convert days since Unix epoch to ISO-8601 date string.
fn days_to_date(days: i32) -> String {
    // Simple calculation - for a proper implementation, use a date library
    // This is a naive implementation for basic testing
    let epoch = 719_528; // Days from year 0 to 1970-01-01
    let total_days = days + epoch;

    // Calculate year, month, day from total days
    let (year, month, day) = days_to_ymd(total_days);
    format!("{:04}-{:02}-{:02}", year, month, day)
}

/// Convert total days since year 0 to year/month/day.
fn days_to_ymd(days: i32) -> (i32, u32, u32) {
    // Simplified algorithm - good enough for testing
    let mut remaining = days;
    let mut year = 0i32;

    // Find year
    while remaining >= days_in_year(year) {
        remaining -= days_in_year(year);
        year += 1;
    }
    while remaining < 0 {
        year -= 1;
        remaining += days_in_year(year);
    }

    // Find month
    let mut month = 1u32;
    while remaining >= days_in_month(year, month) as i32 {
        remaining -= days_in_month(year, month) as i32;
        month += 1;
    }

    let day = (remaining + 1) as u32;
    (year, month, day)
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn days_in_year(year: i32) -> i32 {
    if is_leap_year(year) { 366 } else { 365 }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

/// Convert microseconds since midnight to ISO-8601 time string.
fn micros_to_time(micros: i64) -> String {
    let total_secs = micros / 1_000_000;
    let hours = (total_secs / 3600) % 24;
    let minutes = (total_secs / 60) % 60;
    let seconds = total_secs % 60;
    let millis = (micros % 1_000_000) / 1000;

    if millis > 0 {
        format!("{:02}:{:02}:{:02}.{:03}", hours, minutes, seconds, millis)
    } else {
        format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
    }
}

/// Convert microseconds since Unix epoch to ISO-8601 timestamp.
fn micros_to_timestamp(micros: i64) -> String {
    let secs = micros / 1_000_000;
    let days = (secs / 86400) as i32;
    let time_of_day = (micros % 86_400_000_000).unsigned_abs() as i64;

    let date = days_to_date(days);
    let time = micros_to_time(time_of_day);

    format!("{}T{}", date, time)
}

/// Convert a Value to a serde_json::Value for array serialization.
fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::TinyInt(v) => serde_json::Value::Number((*v).into()),
        Value::SmallInt(v) => serde_json::Value::Number((*v).into()),
        Value::Int(v) => serde_json::Value::Number((*v).into()),
        Value::BigInt(v) => serde_json::Value::Number((*v).into()),
        Value::Float(v) => serde_json::Number::from_f64(f64::from(*v))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Double(v) => serde_json::Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Decimal(s) | Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::Value::String(base64_encode(b)),
        Value::Date(d) => serde_json::Value::String(days_to_date(*d)),
        Value::Time(t) => serde_json::Value::String(micros_to_time(*t)),
        Value::Timestamp(ts) | Value::TimestampTz(ts) => {
            serde_json::Value::String(micros_to_timestamp(*ts))
        }
        Value::Uuid(bytes) => serde_json::Value::String(uuid_to_string(bytes)),
        Value::Json(j) => j.clone(),
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
    }
}

/// Simple base64 encoding for bytes in JSON.
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);

        result.push(ALPHABET[(b0 >> 2) as usize] as char);
        result.push(ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[(b2 & 0x3f) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

/// Convert UUID bytes to hyphenated string format.
fn uuid_to_string(bytes: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_days_to_date() {
        // 1970-01-01 is day 0
        assert_eq!(days_to_date(0), "1970-01-01");
        // 1970-01-02 is day 1
        assert_eq!(days_to_date(1), "1970-01-02");
    }

    #[test]
    fn test_micros_to_time() {
        assert_eq!(micros_to_time(0), "00:00:00");
        assert_eq!(micros_to_time(3_600_000_000), "01:00:00");
        assert_eq!(micros_to_time(3_661_123_000), "01:01:01.123");
    }

    #[test]
    fn test_base64_encode() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn test_uuid_to_string() {
        let uuid = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ];
        assert_eq!(
            uuid_to_string(&uuid),
            "01020304-0506-0708-090a-0b0c0d0e0f10"
        );
    }
}
