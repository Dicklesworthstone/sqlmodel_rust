//! Dynamic SQL values.

use serde::{Deserialize, Serialize};

/// A dynamically-typed SQL value.
///
/// This enum represents all possible SQL values and is used
/// for parameter binding and result fetching.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// NULL value
    Null,

    /// Boolean value
    Bool(bool),

    /// 8-bit signed integer
    TinyInt(i8),

    /// 16-bit signed integer
    SmallInt(i16),

    /// 32-bit signed integer
    Int(i32),

    /// 64-bit signed integer
    BigInt(i64),

    /// 32-bit floating point
    Float(f32),

    /// 64-bit floating point
    Double(f64),

    /// Arbitrary precision decimal (stored as string)
    Decimal(String),

    /// Text string
    Text(String),

    /// Binary data
    Bytes(Vec<u8>),

    /// Date (days since epoch)
    Date(i32),

    /// Time (microseconds since midnight)
    Time(i64),

    /// Timestamp (microseconds since epoch)
    Timestamp(i64),

    /// Timestamp with timezone (microseconds since epoch, UTC)
    TimestampTz(i64),

    /// UUID (as 16 bytes)
    Uuid([u8; 16]),

    /// JSON value
    Json(serde_json::Value),

    /// Array of values
    Array(Vec<Value>),

    /// SQL DEFAULT keyword
    Default,
}

impl Value {
    /// Check if this value is NULL.
    pub const fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Get the type name of this value.
    pub const fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "NULL",
            Value::Bool(_) => "BOOLEAN",
            Value::TinyInt(_) => "TINYINT",
            Value::SmallInt(_) => "SMALLINT",
            Value::Int(_) => "INTEGER",
            Value::BigInt(_) => "BIGINT",
            Value::Float(_) => "REAL",
            Value::Double(_) => "DOUBLE",
            Value::Decimal(_) => "DECIMAL",
            Value::Text(_) => "TEXT",
            Value::Bytes(_) => "BLOB",
            Value::Date(_) => "DATE",
            Value::Time(_) => "TIME",
            Value::Timestamp(_) => "TIMESTAMP",
            Value::TimestampTz(_) => "TIMESTAMPTZ",
            Value::Uuid(_) => "UUID",
            Value::Json(_) => "JSON",
            Value::Array(_) => "ARRAY",
            Value::Default => "DEFAULT",
        }
    }

    /// Try to convert this value to a bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            Value::TinyInt(v) => Some(*v != 0),
            Value::SmallInt(v) => Some(*v != 0),
            Value::Int(v) => Some(*v != 0),
            Value::BigInt(v) => Some(*v != 0),
            _ => None,
        }
    }

    /// Try to convert this value to an i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::TinyInt(v) => Some(i64::from(*v)),
            Value::SmallInt(v) => Some(i64::from(*v)),
            Value::Int(v) => Some(i64::from(*v)),
            Value::BigInt(v) => Some(*v),
            Value::Bool(v) => Some(if *v { 1 } else { 0 }),
            _ => None,
        }
    }

    /// Try to convert this value to an f64.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(f64::from(*v)),
            Value::Double(v) => Some(*v),
            Value::TinyInt(v) => Some(f64::from(*v)),
            Value::SmallInt(v) => Some(f64::from(*v)),
            Value::Int(v) => Some(f64::from(*v)),
            Value::BigInt(v) => Some(*v as f64),
            Value::Decimal(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Try to get this value as a string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            Value::Decimal(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get this value as a byte slice.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            Value::Text(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    /// Convert a `u64` to `Value`, clamping to `i64::MAX` if it overflows.
    ///
    /// This is a convenience method for cases where you want to store large `u64`
    /// values as the largest representable signed integer rather than erroring.
    /// A warning is logged when clamping occurs.
    ///
    /// For strict conversion that errors on overflow, use `Value::try_from(u64)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use sqlmodel_core::Value;
    ///
    /// // Small values convert normally
    /// assert_eq!(Value::from_u64_clamped(42), Value::BigInt(42));
    ///
    /// // Large values are clamped to i64::MAX
    /// assert_eq!(Value::from_u64_clamped(u64::MAX), Value::BigInt(i64::MAX));
    /// ```
    #[must_use]
    pub fn from_u64_clamped(v: u64) -> Self {
        if let Ok(signed) = i64::try_from(v) {
            Value::BigInt(signed)
        } else {
            tracing::warn!(
                value = v,
                clamped_to = i64::MAX,
                "u64 value exceeds i64::MAX; clamping to i64::MAX"
            );
            Value::BigInt(i64::MAX)
        }
    }

    /// Convert to f32, allowing precision loss for large values.
    ///
    /// This is more lenient than `TryFrom<Value> for f32`, which errors on precision loss.
    /// Only returns an error for values that cannot be represented at all (infinity, NaN).
    ///
    /// # Examples
    ///
    /// ```
    /// use sqlmodel_core::Value;
    ///
    /// // Normal values work
    /// assert!(Value::Double(1.5).to_f32_lossy().is_ok());
    ///
    /// // Large integers are converted with precision loss (no error)
    /// assert!(Value::BigInt(i64::MAX).to_f32_lossy().is_ok());
    /// ```
    #[allow(clippy::cast_possible_truncation, clippy::result_large_err)]
    pub fn to_f32_lossy(&self) -> crate::Result<f32> {
        match self {
            Value::Float(v) => Ok(*v),
            Value::Double(v) => {
                let converted = *v as f32;
                if converted.is_infinite() && !v.is_infinite() {
                    return Err(Error::Type(TypeError {
                        expected: "f32-representable value",
                        actual: format!("f64 value {} overflows f32", v),
                        column: None,
                        rust_type: Some("f32"),
                    }));
                }
                Ok(converted)
            }
            Value::TinyInt(v) => Ok(f32::from(*v)),
            Value::SmallInt(v) => Ok(f32::from(*v)),
            Value::Int(v) => Ok(*v as f32),
            Value::BigInt(v) => Ok(*v as f32),
            Value::Bool(v) => Ok(if *v { 1.0 } else { 0.0 }),
            other => Err(Error::Type(TypeError {
                expected: "numeric value",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: Some("f32"),
            })),
        }
    }

    /// Convert to f64, allowing precision loss for very large integers.
    ///
    /// This is more lenient than `TryFrom<Value> for f64`, which errors on precision loss.
    /// Only returns an error for non-numeric values.
    ///
    /// # Examples
    ///
    /// ```
    /// use sqlmodel_core::Value;
    ///
    /// // Normal values work
    /// assert!(Value::BigInt(42).to_f64_lossy().is_ok());
    ///
    /// // Large integers are converted with precision loss (no error)
    /// assert!(Value::BigInt(i64::MAX).to_f64_lossy().is_ok());
    /// ```
    #[allow(clippy::cast_precision_loss, clippy::result_large_err)]
    pub fn to_f64_lossy(&self) -> crate::Result<f64> {
        match self {
            Value::Float(v) => Ok(f64::from(*v)),
            Value::Double(v) => Ok(*v),
            Value::TinyInt(v) => Ok(f64::from(*v)),
            Value::SmallInt(v) => Ok(f64::from(*v)),
            Value::Int(v) => Ok(f64::from(*v)),
            Value::BigInt(v) => Ok(*v as f64), // May lose precision for |v| > 2^53
            Value::Bool(v) => Ok(if *v { 1.0 } else { 0.0 }),
            other => Err(Error::Type(TypeError {
                expected: "numeric value",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: Some("f64"),
            })),
        }
    }
}

// Conversion implementations
impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<i8> for Value {
    fn from(v: i8) -> Self {
        Value::TinyInt(v)
    }
}

impl From<i16> for Value {
    fn from(v: i16) -> Self {
        Value::SmallInt(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::BigInt(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Float(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Double(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(v)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

impl From<&[u8]> for Value {
    fn from(v: &[u8]) -> Self {
        Value::Bytes(v.to_vec())
    }
}

impl From<u8> for Value {
    fn from(v: u8) -> Self {
        Value::SmallInt(i16::from(v))
    }
}

impl From<u16> for Value {
    fn from(v: u16) -> Self {
        Value::Int(i32::from(v))
    }
}

impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Value::BigInt(i64::from(v))
    }
}

/// Convert a `u64` to `Value`, returning an error if the value exceeds `i64::MAX`.
///
/// SQL BIGINT is signed, so values larger than `i64::MAX` cannot be stored directly.
/// Use `Value::from_u64_clamped()` if you want silent clamping instead of an error.
impl TryFrom<u64> for Value {
    type Error = Error;

    fn try_from(v: u64) -> Result<Self, Self::Error> {
        i64::try_from(v).map(Value::BigInt).map_err(|_| {
            Error::Type(TypeError {
                expected: "u64 <= i64::MAX",
                actual: format!("u64 value {} exceeds i64::MAX ({})", v, i64::MAX),
                column: None,
                rust_type: Some("u64"),
            })
        })
    }
}

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        Value::Json(v)
    }
}

impl From<[u8; 16]> for Value {
    fn from(v: [u8; 16]) -> Self {
        Value::Uuid(v)
    }
}

/// Convert a `Vec<String>` into a `Value::Array`.
impl From<Vec<String>> for Value {
    fn from(v: Vec<String>) -> Self {
        Value::Array(v.into_iter().map(Value::Text).collect())
    }
}

/// Convert a `Vec<i32>` into a `Value::Array`.
impl From<Vec<i32>> for Value {
    fn from(v: Vec<i32>) -> Self {
        Value::Array(v.into_iter().map(Value::Int).collect())
    }
}

/// Convert a `Vec<i64>` into a `Value::Array`.
impl From<Vec<i64>> for Value {
    fn from(v: Vec<i64>) -> Self {
        Value::Array(v.into_iter().map(Value::BigInt).collect())
    }
}

/// Convert a `Vec<f64>` into a `Value::Array`.
impl From<Vec<f64>> for Value {
    fn from(v: Vec<f64>) -> Self {
        Value::Array(v.into_iter().map(Value::Double).collect())
    }
}

/// Convert a `Vec<bool>` into a `Value::Array`.
impl From<Vec<bool>> for Value {
    fn from(v: Vec<bool>) -> Self {
        Value::Array(v.into_iter().map(Value::Bool).collect())
    }
}

// TryFrom implementations for extracting values

use crate::error::{Error, TypeError};

impl TryFrom<Value> for bool {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bool(v) => Ok(v),
            Value::TinyInt(v) => Ok(v != 0),
            Value::SmallInt(v) => Ok(v != 0),
            Value::Int(v) => Ok(v != 0),
            Value::BigInt(v) => Ok(v != 0),
            other => Err(Error::Type(TypeError {
                expected: "bool",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

/// Narrow an integer-carrying `Value` to a smaller integer type: accepted when
/// the value fits, refused (never truncated) when it does not. Drivers differ in
/// which variant they report — C SQLite yields `Int` for values that fit `i32`,
/// FrankenSQLite yields `BigInt` for every INTEGER — so typed fields must accept
/// any of them.
#[allow(clippy::result_large_err)]
fn narrow_int_value<T: TryFrom<i64>>(value: &Value, expected: &'static str) -> Result<T, Error> {
    let v = value.as_i64().ok_or_else(|| {
        Error::Type(TypeError {
            expected,
            actual: value.type_name().to_string(),
            column: None,
            rust_type: None,
        })
    })?;
    T::try_from(v).map_err(|_| {
        Error::Type(TypeError {
            expected,
            actual: format!("value {} out of range", v),
            column: None,
            rust_type: None,
        })
    })
}

impl TryFrom<Value> for i8 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        narrow_int_value(&value, "i8")
    }
}

impl TryFrom<Value> for i16 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        narrow_int_value(&value, "i16")
    }
}

impl TryFrom<Value> for i32 {
    type Error = Error;

    /// Accepts any integer variant whose value fits `i32` (drivers differ in
    /// which variant they report for a column); refuses out-of-range values.
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::TinyInt(v) => Ok(i32::from(v)),
            Value::SmallInt(v) => Ok(i32::from(v)),
            Value::Int(v) => Ok(v),
            Value::BigInt(v) => i32::try_from(v).map_err(|_| {
                Error::Type(TypeError {
                    expected: "i32",
                    actual: format!("value {} out of range", v),
                    column: None,
                    rust_type: None,
                })
            }),
            Value::Bool(v) => Ok(if v { 1 } else { 0 }),
            other => Err(Error::Type(TypeError {
                expected: "i32",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::TinyInt(v) => Ok(i64::from(v)),
            Value::SmallInt(v) => Ok(i64::from(v)),
            Value::Int(v) => Ok(i64::from(v)),
            Value::BigInt(v) => Ok(v),
            Value::Bool(v) => Ok(if v { 1 } else { 0 }),
            other => Err(Error::Type(TypeError {
                expected: "i64",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

/// Maximum integer value exactly representable in f32: 2^24 = 16,777,216
const F32_MAX_EXACT_INT: i64 = 1 << 24;

impl TryFrom<Value> for f32 {
    type Error = Error;

    /// Convert a Value to f32, returning an error if precision would be lost.
    ///
    /// For lossy conversion (accepting precision loss), use `Value::to_f32_lossy()`.
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Float(v) => Ok(v),
            #[allow(clippy::cast_possible_truncation)]
            Value::Double(v) => {
                let converted = v as f32;
                // Check round-trip: if converting back doesn't match, we lost precision
                if (f64::from(converted) - v).abs() > f64::EPSILON * v.abs().max(1.0) {
                    return Err(Error::Type(TypeError {
                        expected: "f32-representable f64",
                        actual: format!("f64 value {} loses precision as f32", v),
                        column: None,
                        rust_type: Some("f32"),
                    }));
                }
                Ok(converted)
            }
            Value::TinyInt(v) => Ok(f32::from(v)),
            Value::SmallInt(v) => Ok(f32::from(v)),
            #[allow(clippy::cast_possible_truncation)]
            Value::Int(v) => {
                if i64::from(v).abs() > F32_MAX_EXACT_INT {
                    return Err(Error::Type(TypeError {
                        expected: "f32-representable i32",
                        actual: format!(
                            "i32 value {} exceeds f32 exact integer range (±{})",
                            v, F32_MAX_EXACT_INT
                        ),
                        column: None,
                        rust_type: Some("f32"),
                    }));
                }
                Ok(v as f32)
            }
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            Value::BigInt(v) => {
                // Use unsigned_abs to avoid overflow on i64::MIN
                if v.unsigned_abs() > F32_MAX_EXACT_INT as u64 {
                    return Err(Error::Type(TypeError {
                        expected: "f32-representable i64",
                        actual: format!(
                            "i64 value {} exceeds f32 exact integer range (±{})",
                            v, F32_MAX_EXACT_INT
                        ),
                        column: None,
                        rust_type: Some("f32"),
                    }));
                }
                Ok(v as f32)
            }
            // Bool to f32 is lossless (0.0 or 1.0)
            Value::Bool(v) => Ok(if v { 1.0 } else { 0.0 }),
            other => Err(Error::Type(TypeError {
                expected: "f32",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

/// Maximum integer value exactly representable in f64: 2^53 = 9,007,199,254,740,992
const F64_MAX_EXACT_INT: i64 = 1 << 53;

impl TryFrom<Value> for f64 {
    type Error = Error;

    /// Convert a Value to f64, returning an error if precision would be lost.
    ///
    /// For lossy conversion (accepting precision loss), use `Value::to_f64_lossy()`.
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Float(v) => Ok(f64::from(v)),
            Value::Double(v) => Ok(v),
            Value::TinyInt(v) => Ok(f64::from(v)),
            Value::SmallInt(v) => Ok(f64::from(v)),
            Value::Int(v) => Ok(f64::from(v)),
            #[allow(clippy::cast_precision_loss)]
            #[allow(clippy::cast_sign_loss)]
            Value::BigInt(v) => {
                // Use unsigned_abs to avoid overflow on i64::MIN
                if v.unsigned_abs() > F64_MAX_EXACT_INT as u64 {
                    return Err(Error::Type(TypeError {
                        expected: "f64-representable i64",
                        actual: format!(
                            "i64 value {} exceeds f64 exact integer range (±{})",
                            v, F64_MAX_EXACT_INT
                        ),
                        column: None,
                        rust_type: Some("f64"),
                    }));
                }
                Ok(v as f64)
            }
            // Bool to f64 is lossless (0.0 or 1.0)
            Value::Bool(v) => Ok(if v { 1.0 } else { 0.0 }),
            other => Err(Error::Type(TypeError {
                expected: "f64",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl TryFrom<Value> for String {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Text(v) => Ok(v),
            Value::Decimal(v) => Ok(v),
            other => Err(Error::Type(TypeError {
                expected: "String",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bytes(v) => Ok(v),
            Value::Text(v) => Ok(v.into_bytes()),
            other => Err(Error::Type(TypeError {
                expected: "Vec<u8>",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl TryFrom<Value> for serde_json::Value {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Json(v) => Ok(v),
            Value::Text(s) => serde_json::from_str(&s).map_err(|e| {
                Error::Type(TypeError {
                    expected: "valid JSON",
                    actual: format!("invalid JSON: {}", e),
                    column: None,
                    rust_type: None,
                })
            }),
            other => Err(Error::Type(TypeError {
                expected: "JSON",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl TryFrom<Value> for [u8; 16] {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Uuid(v) => Ok(v),
            Value::Bytes(v) if v.len() == 16 => {
                let mut arr = [0u8; 16];
                arr.copy_from_slice(&v);
                Ok(arr)
            }
            other => Err(Error::Type(TypeError {
                expected: "UUID",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

/// TryFrom for `Option<T>` - returns None for Null, tries to convert otherwise
impl<T> TryFrom<Value> for Option<T>
where
    T: TryFrom<Value, Error = Error>,
{
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Null => Ok(None),
            v => T::try_from(v).map(Some),
        }
    }
}

/// TryFrom for `Vec<String>` - extracts text array.
impl TryFrom<Value> for Vec<String> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Array(arr) => arr.into_iter().map(String::try_from).collect(),
            other => Err(Error::Type(TypeError {
                expected: "ARRAY",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

/// TryFrom for `Vec<i32>` - extracts integer array.
impl TryFrom<Value> for Vec<i32> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Array(arr) => arr.into_iter().map(i32::try_from).collect(),
            other => Err(Error::Type(TypeError {
                expected: "ARRAY",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

/// TryFrom for `Vec<i64>` - extracts bigint array.
impl TryFrom<Value> for Vec<i64> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Array(arr) => arr.into_iter().map(i64::try_from).collect(),
            other => Err(Error::Type(TypeError {
                expected: "ARRAY",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

/// TryFrom for `Vec<bool>` - extracts boolean array.
impl TryFrom<Value> for Vec<bool> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Array(arr) => arr.into_iter().map(bool::try_from).collect(),
            other => Err(Error::Type(TypeError {
                expected: "ARRAY",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl TryFrom<Value> for Vec<f64> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Array(arr) => arr.into_iter().map(f64::try_from).collect(),
            other => Err(Error::Type(TypeError {
                expected: "ARRAY",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

// ---------------------------------------------------------------------------
// ISO-8601 text forms of the temporal payloads
//
// Both SQLite drivers store `Value::Date` / `Value::Time` / `Value::Timestamp`
// / `Value::TimestampTz` as ISO-8601 text (SQLite has no temporal types, and
// text is what its date functions understand). The formatters live here so the
// two drivers cannot drift apart again: FrankenSQLite used to write raw
// integers while C SQLite wrote text with a three-digit fraction, so the same
// file read differently through each driver and microseconds were lost.
// ---------------------------------------------------------------------------

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Proleptic-Gregorian `(year, month, day)` for a count of days since
/// 1970-01-01 (negative for earlier dates). Howard Hinnant's `civil_from_days`.
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // ranges proven by the algorithm
pub fn civil_from_days(days: i32) -> (i32, u32, u32) {
    let z = i64::from(days) + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}

/// `YYYY-MM-DD` for a `Value::Date` payload (days since 1970-01-01).
#[must_use]
pub fn iso_date(days: i32) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// `HH:MM:SS` or `HH:MM:SS.ffffff` for a `Value::Time` payload (microseconds
/// since midnight). The fraction is written with all six digits whenever it is
/// non-zero, so no precision is lost.
#[must_use]
pub fn iso_time(micros: i64) -> String {
    let micros = micros.rem_euclid(MICROS_PER_DAY);
    let total_secs = micros / 1_000_000;
    let (h, mi, s) = (total_secs / 3600, (total_secs / 60) % 60, total_secs % 60);
    let frac = micros % 1_000_000;
    if frac == 0 {
        format!("{h:02}:{mi:02}:{s:02}")
    } else {
        format!("{h:02}:{mi:02}:{s:02}.{frac:06}")
    }
}

/// `YYYY-MM-DDTHH:MM:SS[.ffffff]` for a `Value::Timestamp` /
/// `Value::TimestampTz` payload (microseconds since the Unix epoch, UTC).
/// Correct for pre-1970 instants (floor division, not truncation).
#[must_use]
#[allow(clippy::cast_possible_truncation)] // a day count that overflows i32 is ~5.8 million years out
pub fn iso_timestamp(micros: i64) -> String {
    let days = micros.div_euclid(MICROS_PER_DAY) as i32;
    format!(
        "{}T{}",
        iso_date(days),
        iso_time(micros.rem_euclid(MICROS_PER_DAY))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_formatters_are_exact_and_handle_pre_epoch_values() {
        assert_eq!(iso_date(0), "1970-01-01");
        assert_eq!(iso_date(-25_567), "1900-01-01");
        assert_eq!(iso_date(19_782), "2024-02-29");
        assert_eq!(iso_date(2_932_896), "9999-12-31");
        assert_eq!(civil_from_days(-1), (1969, 12, 31));

        assert_eq!(iso_time(0), "00:00:00");
        assert_eq!(iso_time(3_661_123_000), "01:01:01.123000");
        assert_eq!(iso_time(45_296_500_000), "12:34:56.500000");
        assert_eq!(iso_time(86_399_999_999), "23:59:59.999999");

        assert_eq!(iso_timestamp(0), "1970-01-01T00:00:00");
        assert_eq!(
            iso_timestamp(1_710_498_030_123_456),
            "2024-03-15T10:20:30.123456"
        );
        assert_eq!(iso_timestamp(-2_208_988_800_000_000), "1900-01-01T00:00:00");
        assert_eq!(
            iso_timestamp(-2_208_988_800_000_000 + 3_600_000_000),
            "1900-01-01T01:00:00"
        );
        assert_eq!(iso_timestamp(-1), "1969-12-31T23:59:59.999999");
    }

    #[test]
    fn test_from_bool() {
        let v: Value = true.into();
        assert_eq!(v, Value::Bool(true));
    }

    #[test]
    fn test_from_integers() {
        assert_eq!(Value::from(42i8), Value::TinyInt(42));
        assert_eq!(Value::from(42i16), Value::SmallInt(42));
        assert_eq!(Value::from(42i32), Value::Int(42));
        assert_eq!(Value::from(42i64), Value::BigInt(42));
    }

    #[test]
    fn test_from_unsigned_integers() {
        assert_eq!(Value::from(42u8), Value::SmallInt(42));
        assert_eq!(Value::from(42u16), Value::Int(42));
        assert_eq!(Value::from(42u32), Value::BigInt(42));
        // u64 uses TryFrom, not From (see test_try_from_u64 and test_from_u64_clamped)
    }

    #[test]
    fn test_from_floats() {
        let pi_f32 = std::f32::consts::PI;
        let pi_f64 = std::f64::consts::PI;
        assert_eq!(Value::from(pi_f32), Value::Float(pi_f32));
        assert_eq!(Value::from(pi_f64), Value::Double(pi_f64));
    }

    #[test]
    fn test_from_strings() {
        assert_eq!(Value::from("hello"), Value::Text("hello".to_string()));
        assert_eq!(
            Value::from("hello".to_string()),
            Value::Text("hello".to_string())
        );
    }

    #[test]
    fn test_from_bytes() {
        let bytes = vec![1u8, 2, 3];
        assert_eq!(Value::from(bytes.clone()), Value::Bytes(bytes.clone()));
        assert_eq!(Value::from(bytes.as_slice()), Value::Bytes(bytes));
    }

    #[test]
    fn test_from_option() {
        let some: Value = Some(42i32).into();
        assert_eq!(some, Value::Int(42));

        let none: Value = Option::<i32>::None.into();
        assert_eq!(none, Value::Null);
    }

    #[test]
    fn test_try_from_bool() {
        assert!(bool::try_from(Value::Bool(true)).unwrap());
        assert!(bool::try_from(Value::Int(1)).unwrap());
        assert!(!bool::try_from(Value::Int(0)).unwrap());
        assert!(bool::try_from(Value::Text("true".to_string())).is_err());
    }

    #[test]
    fn test_try_from_i64() {
        assert_eq!(i64::try_from(Value::BigInt(42)).unwrap(), 42);
        assert_eq!(i64::try_from(Value::Int(42)).unwrap(), 42);
        assert_eq!(i64::try_from(Value::SmallInt(42)).unwrap(), 42);
        assert_eq!(i64::try_from(Value::TinyInt(42)).unwrap(), 42);
        assert!(i64::try_from(Value::Text("42".to_string())).is_err());
    }

    #[test]
    fn test_try_from_f64() {
        let pi = std::f64::consts::PI;
        let pi_f32 = std::f32::consts::PI;
        let double = f64::try_from(Value::Double(pi)).unwrap();
        assert!((double - pi).abs() < 1e-12);

        let from_float = f64::try_from(Value::Float(pi_f32)).unwrap();
        assert!((from_float - f64::from(pi_f32)).abs() < 1e-6);

        let from_int = f64::try_from(Value::Int(42)).unwrap();
        assert!((from_int - 42.0).abs() < 1e-12);
        assert!(f64::try_from(Value::Text("3.14".to_string())).is_err());
    }

    #[test]
    fn test_try_from_string() {
        assert_eq!(
            String::try_from(Value::Text("hello".to_string())).unwrap(),
            "hello"
        );
        assert!(String::try_from(Value::Int(42)).is_err());
    }

    #[test]
    fn test_try_from_bytes() {
        let bytes = vec![1u8, 2, 3];
        assert_eq!(
            Vec::<u8>::try_from(Value::Bytes(bytes.clone())).unwrap(),
            bytes
        );
        assert_eq!(
            Vec::<u8>::try_from(Value::Text("abc".to_string())).unwrap(),
            b"abc".to_vec()
        );
    }

    #[test]
    fn test_try_from_option() {
        let result: Option<i32> = Option::try_from(Value::Int(42)).unwrap();
        assert_eq!(result, Some(42));

        let result: Option<i32> = Option::try_from(Value::Null).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_round_trip_bool() {
        let original = true;
        let value: Value = original.into();
        let recovered: bool = value.try_into().unwrap();
        assert_eq!(original, recovered);
    }

    #[test]
    fn test_round_trip_i64() {
        let original: i64 = i64::MAX;
        let value: Value = original.into();
        let recovered: i64 = value.try_into().unwrap();
        assert_eq!(original, recovered);
    }

    #[test]
    fn test_round_trip_f64() {
        let original: f64 = std::f64::consts::PI;
        let value: Value = original.into();
        let recovered: f64 = value.try_into().unwrap();
        assert!((original - recovered).abs() < f64::EPSILON);
    }

    #[test]
    fn test_round_trip_string() {
        let original = "hello world".to_string();
        let value: Value = original.clone().into();
        let recovered: String = value.try_into().unwrap();
        assert_eq!(original, recovered);
    }

    #[test]
    fn test_round_trip_bytes() {
        let original = vec![0u8, 127, 255];
        let value: Value = original.clone().into();
        let recovered: Vec<u8> = value.try_into().unwrap();
        assert_eq!(original, recovered);
    }

    #[test]
    fn test_is_null() {
        assert!(Value::Null.is_null());
        assert!(!Value::Int(0).is_null());
        assert!(!Value::Bool(false).is_null());
    }

    #[test]
    fn test_as_i64() {
        assert_eq!(Value::BigInt(42).as_i64(), Some(42));
        assert_eq!(Value::Int(42).as_i64(), Some(42));
        assert_eq!(Value::Null.as_i64(), None);
        assert_eq!(Value::Text("42".to_string()).as_i64(), None);
    }

    #[test]
    fn test_as_str() {
        assert_eq!(Value::Text("hello".to_string()).as_str(), Some("hello"));
        assert_eq!(
            Value::Decimal("123.45".to_string()).as_str(),
            Some("123.45")
        );
        assert_eq!(Value::Int(42).as_str(), None);
    }

    #[test]
    fn test_type_name() {
        assert_eq!(Value::Null.type_name(), "NULL");
        assert_eq!(Value::Bool(true).type_name(), "BOOLEAN");
        assert_eq!(Value::Int(42).type_name(), "INTEGER");
        assert_eq!(Value::Text(String::new()).type_name(), "TEXT");
    }

    #[test]
    fn test_edge_cases() {
        // Empty string
        let value: Value = "".into();
        let recovered: String = value.try_into().unwrap();
        assert_eq!(recovered, "");

        // Empty bytes
        let value: Value = Vec::<u8>::new().into();
        let recovered: Vec<u8> = value.try_into().unwrap();
        assert!(recovered.is_empty());

        // Max values
        let value: Value = i64::MAX.into();
        let recovered: i64 = value.try_into().unwrap();
        assert_eq!(recovered, i64::MAX);

        let value: Value = i64::MIN.into();
        let recovered: i64 = value.try_into().unwrap();
        assert_eq!(recovered, i64::MIN);
    }

    #[test]
    fn test_array_string_roundtrip() {
        let v: Value = vec!["a".to_string(), "b".to_string()].into();
        assert_eq!(
            v,
            Value::Array(vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string())
            ])
        );
        let recovered: Vec<String> = v.try_into().unwrap();
        assert_eq!(recovered, vec!["a", "b"]);
    }

    #[test]
    fn test_array_i32_roundtrip() {
        let v: Value = vec![1i32, 2, 3].into();
        assert_eq!(
            v,
            Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
        );
        let recovered: Vec<i32> = v.try_into().unwrap();
        assert_eq!(recovered, vec![1, 2, 3]);
    }

    #[test]
    fn test_array_empty() {
        let v: Value = Vec::<String>::new().into();
        assert_eq!(v, Value::Array(vec![]));
        let recovered: Vec<String> = v.try_into().unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_array_type_error() {
        let v = Value::Text("not an array".to_string());
        let result: Result<Vec<String>, _> = v.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_try_from_u64_success() {
        // Values within i64 range should succeed
        let v: Value = Value::try_from(42u64).unwrap();
        assert_eq!(v, Value::BigInt(42));

        // Maximum valid value: i64::MAX
        let v: Value = Value::try_from(i64::MAX as u64).unwrap();
        assert_eq!(v, Value::BigInt(i64::MAX));

        // Zero should work
        let v: Value = Value::try_from(0u64).unwrap();
        assert_eq!(v, Value::BigInt(0));
    }

    #[test]
    fn test_try_from_u64_overflow_error() {
        // Values exceeding i64::MAX should error
        let result = Value::try_from(u64::MAX);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::Type(_)));

        // One more than i64::MAX should also error
        let result = Value::try_from((i64::MAX as u64) + 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_u64_clamped_normal() {
        // Normal values convert without clamping
        assert_eq!(Value::from_u64_clamped(0), Value::BigInt(0));
        assert_eq!(Value::from_u64_clamped(42), Value::BigInt(42));
        assert_eq!(
            Value::from_u64_clamped(i64::MAX as u64),
            Value::BigInt(i64::MAX)
        );
    }

    #[test]
    fn test_from_u64_clamped_overflow() {
        // Values exceeding i64::MAX are clamped
        assert_eq!(Value::from_u64_clamped(u64::MAX), Value::BigInt(i64::MAX));
        assert_eq!(
            Value::from_u64_clamped((i64::MAX as u64) + 1),
            Value::BigInt(i64::MAX)
        );
    }

    // ==================== Precision Loss Detection Tests ====================

    const F32_MAX_EXACT: i64 = 1 << 24; // 16,777,216
    const F64_MAX_EXACT: i64 = 1 << 53; // 9,007,199,254,740,992

    #[test]
    fn test_f32_from_double_precision_ok() {
        // Values exactly representable in f32
        let v: f32 = Value::Double(1.5).try_into().unwrap();
        assert!((v - 1.5).abs() < f32::EPSILON);

        let v: f32 = Value::Double(0.0).try_into().unwrap();
        assert!((v - 0.0).abs() < f32::EPSILON);
    }

    #[test]
    fn test_f32_from_double_precision_loss() {
        // f64 value that cannot be exactly represented in f32
        // 1e20 as f64 cannot round-trip through f32 exactly
        let high_precision = 1e20_f64;
        let result = f32::try_from(Value::Double(high_precision));
        assert!(result.is_err());

        // A more subtle case: numbers with more precision than f32 mantissa can hold
        // f64 has 52 mantissa bits, f32 has 23, so values with >23 bits of precision lose info
        let precise_value = 16_777_217.0_f64; // 2^24 + 1, needs 25 bits, loses precision as f32
        let result2 = f32::try_from(Value::Double(precise_value));
        assert!(result2.is_err());
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn test_f32_from_int_boundary() {
        // At boundary: exactly representable (F32_MAX_EXACT = 2^24 = 16,777,216 fits in i32)
        let boundary = F32_MAX_EXACT as i32;
        let v: f32 = Value::Int(boundary).try_into().unwrap();
        assert!((v - F32_MAX_EXACT as f32).abs() < 1.0);

        // Just over boundary: error
        let over_boundary = (F32_MAX_EXACT + 1) as i32;
        let result = f32::try_from(Value::Int(over_boundary));
        assert!(result.is_err());

        // Negative boundary
        let v: f32 = Value::Int(-boundary).try_into().unwrap();
        assert!((v - -(F32_MAX_EXACT as f32)).abs() < 1.0);
    }

    #[test]
    fn test_f32_from_bigint_boundary() {
        // At boundary: exactly representable
        let v: f32 = Value::BigInt(F32_MAX_EXACT).try_into().unwrap();
        assert!((v - F32_MAX_EXACT as f32).abs() < 1.0);

        // Just over boundary: error
        let result = f32::try_from(Value::BigInt(F32_MAX_EXACT + 1));
        assert!(result.is_err());
    }

    #[test]
    fn test_f64_from_bigint_boundary() {
        // At boundary: exactly representable
        let v: f64 = Value::BigInt(F64_MAX_EXACT).try_into().unwrap();
        assert!((v - F64_MAX_EXACT as f64).abs() < 1.0);

        // Just over boundary: error
        let result = f64::try_from(Value::BigInt(F64_MAX_EXACT + 1));
        assert!(result.is_err());
    }

    #[test]
    fn test_f32_lossy_accepts_large_values() {
        // Lossy conversion accepts values that strict conversion rejects
        assert!(Value::BigInt(i64::MAX).to_f32_lossy().is_ok());
        assert!(
            Value::Double(1.000_000_119_209_289_6)
                .to_f32_lossy()
                .is_ok()
        );
        assert!(Value::Int(i32::MAX).to_f32_lossy().is_ok());
    }

    #[test]
    fn test_f32_lossy_rejects_overflow() {
        // But rejects truly unrepresentable values (overflow to infinity)
        let result = Value::Double(f64::MAX).to_f32_lossy();
        assert!(result.is_err());
    }

    #[test]
    fn test_f64_lossy_accepts_large_integers() {
        // Lossy conversion accepts values that strict conversion rejects
        assert!(Value::BigInt(i64::MAX).to_f64_lossy().is_ok());
        assert!(Value::BigInt(i64::MIN).to_f64_lossy().is_ok());
    }

    #[test]
    fn test_f64_lossy_rejects_non_numeric() {
        let result = Value::Text("not a number".to_string()).to_f64_lossy();
        assert!(result.is_err());
    }

    // ==================== Error Message Quality Tests ====================

    #[test]
    fn test_u64_error_message_includes_value() {
        let big_val = u64::MAX;
        let result = Value::try_from(big_val);
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        // Error should include the actual value for debugging
        assert!(
            msg.contains("18446744073709551615") || msg.contains(&big_val.to_string()),
            "Error should include the u64 value, got: {}",
            msg
        );
    }

    #[test]
    fn test_f32_precision_error_is_descriptive() {
        let result = f32::try_from(Value::BigInt(i64::MAX));
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        // Error should explain the precision issue
        assert!(
            msg.contains("f32") && (msg.contains("exact") || msg.contains("precision")),
            "Error should describe f32 precision issue, got: {}",
            msg
        );
    }

    #[test]
    fn test_f64_precision_error_is_descriptive() {
        let result = f64::try_from(Value::BigInt(i64::MAX));
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("f64") && (msg.contains("exact") || msg.contains("precision")),
            "Error should describe f64 precision issue, got: {}",
            msg
        );
    }

    // ==================== Negative Boundary Tests ====================

    #[test]
    fn test_f64_negative_boundary() {
        // Exactly -2^53 is representable
        let neg_boundary = -(1i64 << 53);
        let v: f64 = Value::BigInt(neg_boundary).try_into().unwrap();
        assert!((v - neg_boundary as f64).abs() < 1.0);

        // -2^53 - 1 should error
        let result = f64::try_from(Value::BigInt(neg_boundary - 1));
        assert!(result.is_err());
    }

    #[test]
    fn test_f32_negative_boundary() {
        let neg_boundary = -(F32_MAX_EXACT);
        let v: f32 = Value::BigInt(neg_boundary).try_into().unwrap();
        assert!((v - neg_boundary as f32).abs() < 1.0);

        // Just past negative boundary should error
        let result = f32::try_from(Value::BigInt(neg_boundary - 1));
        assert!(result.is_err());
    }

    // ==================== Error Type Verification Tests ====================

    #[test]
    fn test_conversion_errors_are_type_errors() {
        use crate::Error;

        // u64 overflow
        let err = Value::try_from(u64::MAX).unwrap_err();
        assert!(
            matches!(err, Error::Type(_)),
            "u64 overflow should be TypeError"
        );

        // f32 precision loss
        let err = f32::try_from(Value::BigInt(i64::MAX)).unwrap_err();
        assert!(
            matches!(err, Error::Type(_)),
            "f32 precision loss should be TypeError"
        );

        // f64 precision loss
        let err = f64::try_from(Value::BigInt(i64::MAX)).unwrap_err();
        assert!(
            matches!(err, Error::Type(_)),
            "f64 precision loss should be TypeError"
        );
    }

    #[test]
    fn test_conversion_errors_include_expected_type() {
        // f32 conversion error should mention f32
        let err = f32::try_from(Value::BigInt(i64::MAX)).unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("f32"),
            "Error should mention f32, got: {}",
            err
        );

        // f64 conversion error should mention f64
        let err = f64::try_from(Value::BigInt(i64::MAX)).unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("f64"),
            "Error should mention f64, got: {}",
            err
        );
    }

    // ==================== Roundtrip Tests ====================

    #[test]
    fn test_u64_roundtrip_within_range() {
        // Values within i64 range should roundtrip through Value
        let original = 42u64;
        let value: Value = original.try_into().unwrap();
        let recovered: i64 = value.try_into().unwrap();
        assert_eq!(recovered, original as i64);

        // Max representable value
        let original = i64::MAX as u64;
        let value: Value = original.try_into().unwrap();
        let recovered: i64 = value.try_into().unwrap();
        assert_eq!(recovered, i64::MAX);
    }

    #[test]
    fn test_f32_roundtrip_preserves_value() {
        let original = std::f32::consts::PI;
        let value: Value = original.into();
        let recovered: f32 = value.try_into().unwrap();
        assert!((original - recovered).abs() < f32::EPSILON);
    }

    #[test]
    fn test_f64_i64_roundtrip() {
        // Small i64 values should roundtrip through f64 exactly
        let original = 12345i64;
        let value = Value::BigInt(original);
        let as_f64: f64 = value.try_into().unwrap();
        assert!((as_f64 - original as f64).abs() < 1e-10);
    }

    // ==================== Additional Boundary Tests ====================

    #[test]
    fn test_u64_boundary_edge_cases() {
        // Zero is always valid
        let v: Value = 0u64.try_into().unwrap();
        assert_eq!(v, Value::BigInt(0));

        // i64::MAX - 1 is valid
        let v: Value = ((i64::MAX - 1) as u64).try_into().unwrap();
        assert_eq!(v, Value::BigInt(i64::MAX - 1));

        // i64::MAX is valid
        let v: Value = (i64::MAX as u64).try_into().unwrap();
        assert_eq!(v, Value::BigInt(i64::MAX));
    }

    #[test]
    fn test_i64_min_max_to_f64() {
        // i64::MAX exceeds f64 exact range (2^63-1 > 2^53)
        let result = f64::try_from(Value::BigInt(i64::MAX));
        assert!(result.is_err());

        // i64::MIN also exceeds f64 exact range (-2^63 < -2^53)
        let result = f64::try_from(Value::BigInt(i64::MIN));
        assert!(result.is_err());
    }

    #[test]
    fn test_f32_from_float_is_lossless() {
        // Converting from Float variant should always succeed
        let original = std::f32::consts::E;
        let v: f32 = Value::Float(original).try_into().unwrap();
        assert!((v - original).abs() < f32::EPSILON);
    }
}
