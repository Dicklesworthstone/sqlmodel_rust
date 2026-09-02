//! Bidirectional conversion between `sqlmodel_core::Value` and `fsqlite_types::value::SqliteValue`.

use fsqlite_types::value::SqliteValue;
use sqlmodel_core::value::{Value, iso_date, iso_time, iso_timestamp};

/// Convert a `sqlmodel_core::Value` to a `SqliteValue` for parameter binding.
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
pub fn value_to_sqlite(v: &Value) -> SqliteValue {
    match v {
        Value::Null | Value::Default => SqliteValue::Null,
        Value::Bool(b) => SqliteValue::Integer(i64::from(*b)),
        Value::TinyInt(i) => SqliteValue::Integer(i64::from(*i)),
        Value::SmallInt(i) => SqliteValue::Integer(i64::from(*i)),
        Value::Int(i) => SqliteValue::Integer(i64::from(*i)),
        Value::BigInt(i) => SqliteValue::Integer(*i),
        Value::Float(f) => SqliteValue::Float(f64::from(*f)),
        Value::Double(f) => SqliteValue::Float(*f),
        Value::Decimal(s) => {
            // Try integer first, then float, then text
            if let Ok(i) = s.parse::<i64>() {
                SqliteValue::Integer(i)
            } else if let Ok(f) = s.parse::<f64>() {
                SqliteValue::Float(f)
            } else {
                SqliteValue::Text(s.clone().into())
            }
        }
        Value::Text(s) => SqliteValue::Text(s.clone().into()),
        Value::Bytes(b) => SqliteValue::Blob(b.clone().into()),
        // Same ISO-8601 text the C SQLite driver writes, so one database file
        // reads identically through both drivers and SQLite's date functions
        // work on the columns. (Raw integers were stored here before; the
        // chrono conversions still accept them for existing files.)
        Value::Date(d) => SqliteValue::Text(iso_date(*d).into()),
        Value::Time(t) => SqliteValue::Text(iso_time(*t).into()),
        Value::Timestamp(ts) | Value::TimestampTz(ts) => {
            SqliteValue::Text(iso_timestamp(*ts).into())
        }
        Value::Uuid(bytes) => SqliteValue::Blob(bytes.to_vec().into()),
        Value::Json(v) => SqliteValue::Text(serde_json::to_string(v).unwrap_or_default().into()),
        Value::Array(_) => SqliteValue::Null, // Arrays not supported in SQLite
    }
}

/// Convert a `SqliteValue` to a `sqlmodel_core::Value` for result extraction.
pub fn sqlite_to_value(sv: &SqliteValue) -> Value {
    match sv {
        SqliteValue::Null => Value::Null,
        SqliteValue::Integer(i) => Value::BigInt(*i),
        SqliteValue::Float(f) => Value::Double(*f),
        SqliteValue::Text(s) => Value::Text(s.to_string()),
        SqliteValue::Blob(b) => Value::Bytes(b.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::f32::consts::PI as PI32;
    use std::f64::consts::{E, PI as PI64};

    #[test]
    fn null_roundtrip() {
        assert!(matches!(value_to_sqlite(&Value::Null), SqliteValue::Null));
        assert!(matches!(sqlite_to_value(&SqliteValue::Null), Value::Null));
    }

    #[test]
    fn bool_to_integer() {
        assert_eq!(value_to_sqlite(&Value::Bool(true)), SqliteValue::Integer(1));
        assert_eq!(
            value_to_sqlite(&Value::Bool(false)),
            SqliteValue::Integer(0)
        );
    }

    #[test]
    fn integer_variants() {
        assert_eq!(
            value_to_sqlite(&Value::TinyInt(42)),
            SqliteValue::Integer(42)
        );
        assert_eq!(
            value_to_sqlite(&Value::SmallInt(1000)),
            SqliteValue::Integer(1000)
        );
        assert_eq!(
            value_to_sqlite(&Value::Int(100_000)),
            SqliteValue::Integer(100_000)
        );
        assert_eq!(
            value_to_sqlite(&Value::BigInt(i64::MAX)),
            SqliteValue::Integer(i64::MAX)
        );
    }

    #[test]
    fn float_variants() {
        let sv = value_to_sqlite(&Value::Float(PI32));
        assert!(matches!(sv, SqliteValue::Float(_)));

        assert_eq!(value_to_sqlite(&Value::Double(E)), SqliteValue::Float(E));
    }

    #[test]
    fn text_roundtrip() {
        let v = Value::Text("hello".into());
        let sv = value_to_sqlite(&v);
        assert_eq!(sv, SqliteValue::Text("hello".into()));
        assert_eq!(sqlite_to_value(&sv), v);
    }

    #[test]
    fn bytes_roundtrip() {
        let v = Value::Bytes(vec![0xDE, 0xAD]);
        let sv = value_to_sqlite(&v);
        assert_eq!(sv, SqliteValue::Blob(vec![0xDE, 0xAD].into()));
        assert_eq!(sqlite_to_value(&sv), v);
    }

    #[test]
    fn json_to_text() {
        let json = serde_json::json!({"key": "value"});
        let sv = value_to_sqlite(&Value::Json(json.clone()));
        assert!(matches!(sv, SqliteValue::Text(_)));
    }

    #[test]
    fn uuid_to_blob() {
        let uuid = [1u8; 16];
        let sv = value_to_sqlite(&Value::Uuid(uuid));
        assert_eq!(sv, SqliteValue::Blob(uuid.to_vec().into()));
    }

    #[test]
    fn decimal_string_to_integer() {
        let sv = value_to_sqlite(&Value::Decimal("42".into()));
        assert_eq!(sv, SqliteValue::Integer(42));
    }

    #[test]
    fn decimal_string_to_float() {
        let sv = value_to_sqlite(&Value::Decimal("3.14".into()));
        assert!(matches!(sv, SqliteValue::Float(_)));
    }

    #[test]
    fn temporal_values_are_iso_text_like_the_c_sqlite_driver() {
        assert_eq!(
            value_to_sqlite(&Value::Timestamp(1_700_000_000_000_000)),
            SqliteValue::Text("2023-11-14T22:13:20".into())
        );
        assert_eq!(
            value_to_sqlite(&Value::TimestampTz(1_710_498_030_123_456)),
            SqliteValue::Text("2024-03-15T10:20:30.123456".into())
        );
        assert_eq!(
            value_to_sqlite(&Value::Date(19_782)),
            SqliteValue::Text("2024-02-29".into())
        );
        assert_eq!(
            value_to_sqlite(&Value::Time(86_399_999_999)),
            SqliteValue::Text("23:59:59.999999".into())
        );
    }

    #[test]
    fn default_to_null() {
        assert!(matches!(
            value_to_sqlite(&Value::Default),
            SqliteValue::Null
        ));
    }

    #[test]
    fn sqlite_integer_to_bigint() {
        let v = sqlite_to_value(&SqliteValue::Integer(42));
        assert_eq!(v, Value::BigInt(42));
    }

    #[test]
    fn sqlite_float_to_double() {
        let v = sqlite_to_value(&SqliteValue::Float(PI64));
        assert_eq!(v, Value::Double(PI64));
    }
}
