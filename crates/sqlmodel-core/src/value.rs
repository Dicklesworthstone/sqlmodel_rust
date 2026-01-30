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

impl TryFrom<Value> for i8 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::TinyInt(v) => Ok(v),
            Value::Bool(v) => Ok(if v { 1 } else { 0 }),
            other => Err(Error::Type(TypeError {
                expected: "i8",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl TryFrom<Value> for i16 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::TinyInt(v) => Ok(i16::from(v)),
            Value::SmallInt(v) => Ok(v),
            Value::Bool(v) => Ok(if v { 1 } else { 0 }),
            other => Err(Error::Type(TypeError {
                expected: "i16",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl TryFrom<Value> for i32 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::TinyInt(v) => Ok(i32::from(v)),
            Value::SmallInt(v) => Ok(i32::from(v)),
            Value::Int(v) => Ok(v),
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

impl TryFrom<Value> for f32 {
    type Error = Error;

    #[allow(clippy::cast_possible_truncation)]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Float(v) => Ok(v),
            // Intentional truncation: user explicitly requested f32 from f64
            Value::Double(v) => Ok(v as f32),
            Value::TinyInt(v) => Ok(f32::from(v)),
            Value::SmallInt(v) => Ok(f32::from(v)),
            Value::Int(v) => Ok(v as f32),
            Value::BigInt(v) => Ok(v as f32),
            other => Err(Error::Type(TypeError {
                expected: "f32",
                actual: other.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl TryFrom<Value> for f64 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Float(v) => Ok(f64::from(v)),
            Value::Double(v) => Ok(v),
            Value::TinyInt(v) => Ok(f64::from(v)),
            Value::SmallInt(v) => Ok(f64::from(v)),
            Value::Int(v) => Ok(f64::from(v)),
            Value::BigInt(v) => Ok(v as f64),
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
