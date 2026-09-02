//! Optional conversions between [`Value`] and popular external value types.
//!
//! SQLModel's wire format for temporal values is fixed and dependency-free:
//! [`Value::Date`] is days since 1970-01-01, [`Value::Time`] is microseconds
//! since midnight, [`Value::Timestamp`] / [`Value::TimestampTz`] are microseconds
//! since the Unix epoch, [`Value::Uuid`] is the 16 raw bytes, and
//! [`Value::Decimal`] is the decimal's text form. The derive macro already maps
//! the type *names* `chrono::NaiveDate`, `chrono::NaiveDateTime`, `uuid::Uuid`,
//! and `rust_decimal::Decimal` to the right [`crate::SqlType`]; this module adds
//! the `From` / `TryFrom` / [`FromValue`] impls that make such fields actually
//! compile in `#[derive(Model)]` structs.
//!
//! Enable per type with the sqlmodel-core features `chrono`, `uuid`, `decimal`
//! (re-exported by the facade under the same names).
//!
//! Precision policy: SQL temporal types carry microseconds. Converting a chrono
//! value with nanosecond precision into a [`Value`] truncates to microseconds
//! (the same behaviour as every other ORM/driver); the reverse direction is
//! exact. Out-of-range values (a `Value::Date` beyond chrono's year range, a
//! `Value::Text` that does not parse) are errors, never clamped.

#![allow(clippy::result_large_err)]

use crate::error::{Error, TypeError};
use crate::row::FromValue;
use crate::value::Value;

fn type_error(expected: &'static str, actual: &Value, rust_type: &'static str) -> Error {
    Error::Type(TypeError {
        expected,
        actual: actual.type_name().to_string(),
        column: None,
        rust_type: Some(rust_type),
    })
}

fn parse_error(expected: &'static str, text: &str, rust_type: &'static str) -> Error {
    Error::Type(TypeError {
        expected,
        actual: format!("unparsable text {text:?}"),
        column: None,
        rust_type: Some(rust_type),
    })
}

#[cfg(feature = "chrono")]
mod chrono_impls {
    use super::*;
    use chrono::{DateTime, Days, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};

    const MICROS_PER_SECOND: i64 = 1_000_000;

    fn epoch_date() -> NaiveDate {
        NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a valid date")
    }

    // ---- into Value -------------------------------------------------------

    impl From<NaiveDate> for Value {
        /// Days since 1970-01-01 (negative before the epoch).
        fn from(d: NaiveDate) -> Self {
            let days = d.signed_duration_since(epoch_date()).num_days();
            // NaiveDate's year range (±262,143) keeps this well inside i32.
            Value::Date(days as i32)
        }
    }

    impl From<NaiveTime> for Value {
        /// Microseconds since midnight; sub-microsecond digits are truncated.
        fn from(t: NaiveTime) -> Self {
            let micros = i64::from(t.num_seconds_from_midnight()) * MICROS_PER_SECOND
                + i64::from(t.nanosecond() / 1_000);
            Value::Time(micros)
        }
    }

    impl From<NaiveDateTime> for Value {
        /// Microseconds since the Unix epoch, treating the naive value as UTC;
        /// sub-microsecond digits are truncated.
        fn from(dt: NaiveDateTime) -> Self {
            Value::Timestamp(dt.and_utc().timestamp_micros())
        }
    }

    impl From<DateTime<Utc>> for Value {
        /// Microseconds since the Unix epoch (`TIMESTAMPTZ`).
        fn from(dt: DateTime<Utc>) -> Self {
            Value::TimestampTz(dt.timestamp_micros())
        }
    }

    impl From<DateTime<FixedOffset>> for Value {
        /// Microseconds since the Unix epoch (`TIMESTAMPTZ`); the offset is
        /// applied, not stored, exactly like PostgreSQL does.
        fn from(dt: DateTime<FixedOffset>) -> Self {
            Value::TimestampTz(dt.timestamp_micros())
        }
    }

    // ---- out of Value -----------------------------------------------------

    fn date_from_days(days: i32) -> Result<NaiveDate, Error> {
        let epoch = epoch_date();
        let result = if days >= 0 {
            epoch.checked_add_days(Days::new(days as u64))
        } else {
            epoch.checked_sub_days(Days::new(days.unsigned_abs() as u64))
        };
        result.ok_or_else(|| {
            Error::Type(TypeError {
                expected: "date within chrono's range",
                actual: format!("Value::Date({days}) is out of range"),
                column: None,
                rust_type: Some("chrono::NaiveDate"),
            })
        })
    }

    fn datetime_from_micros(us: i64, rust_type: &'static str) -> Result<DateTime<Utc>, Error> {
        DateTime::<Utc>::from_timestamp_micros(us).ok_or_else(|| {
            Error::Type(TypeError {
                expected: "timestamp within chrono's range",
                actual: format!("{us} microseconds since epoch is out of range"),
                column: None,
                rust_type: Some(rust_type),
            })
        })
    }

    fn parse_naive_datetime(text: &str) -> Option<NaiveDateTime> {
        let text = text.trim();
        if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
            return Some(dt.naive_utc());
        }
        for fmt in [
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
        ] {
            if let Ok(dt) = NaiveDateTime::parse_from_str(text, fmt) {
                return Some(dt);
            }
        }
        None
    }

    impl TryFrom<Value> for NaiveDate {
        type Error = Error;

        fn try_from(value: Value) -> Result<Self, Error> {
            match &value {
                Value::Date(days) => date_from_days(*days),
                // Drivers without a date type store `Value::Date` as its integer payload
                // (days since 1970-01-01) and report it back as a plain integer.
                Value::Int(days) => date_from_days(*days),
                Value::BigInt(days) => i32::try_from(*days)
                    .map_err(|_| type_error("date", &value, "chrono::NaiveDate"))
                    .and_then(date_from_days),
                Value::Timestamp(us) | Value::TimestampTz(us) => {
                    datetime_from_micros(*us, "chrono::NaiveDate").map(|dt| dt.date_naive())
                }
                Value::Text(s) => NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
                    .map_err(|_| parse_error("date (YYYY-MM-DD)", s, "chrono::NaiveDate")),
                other => Err(type_error("date", other, "chrono::NaiveDate")),
            }
        }
    }

    impl TryFrom<Value> for NaiveTime {
        type Error = Error;

        fn try_from(value: Value) -> Result<Self, Error> {
            match &value {
                // `BigInt` is the integer wire form of `Value::Time` (microseconds since midnight).
                Value::Time(us) | Value::BigInt(us) if *us >= 0 => {
                    let secs = u32::try_from(us / MICROS_PER_SECOND).ok();
                    let nanos = u32::try_from((us % MICROS_PER_SECOND) * 1_000).ok();
                    secs.zip(nanos)
                        .and_then(|(s, n)| NaiveTime::from_num_seconds_from_midnight_opt(s, n))
                        .ok_or_else(|| {
                            Error::Type(TypeError {
                                expected: "time within one day",
                                actual: format!("Value::Time({us}) exceeds 24h"),
                                column: None,
                                rust_type: Some("chrono::NaiveTime"),
                            })
                        })
                }
                Value::Text(s) => {
                    let t = s.trim();
                    NaiveTime::parse_from_str(t, "%H:%M:%S%.f")
                        .or_else(|_| NaiveTime::parse_from_str(t, "%H:%M:%S"))
                        .or_else(|_| NaiveTime::parse_from_str(t, "%H:%M"))
                        .map_err(|_| {
                            parse_error("time (HH:MM[:SS[.ffffff]])", s, "chrono::NaiveTime")
                        })
                }
                other => Err(type_error("time", other, "chrono::NaiveTime")),
            }
        }
    }

    impl TryFrom<Value> for NaiveDateTime {
        type Error = Error;

        fn try_from(value: Value) -> Result<Self, Error> {
            match &value {
                // `BigInt` is the integer wire form of a timestamp (microseconds since the epoch).
                Value::Timestamp(us) | Value::TimestampTz(us) | Value::BigInt(us) => {
                    datetime_from_micros(*us, "chrono::NaiveDateTime").map(|dt| dt.naive_utc())
                }
                Value::Date(days) => {
                    date_from_days(*days).map(|d| d.and_hms_opt(0, 0, 0).expect("midnight"))
                }
                Value::Text(s) => parse_naive_datetime(s)
                    .ok_or_else(|| parse_error("timestamp (ISO 8601)", s, "chrono::NaiveDateTime")),
                other => Err(type_error("timestamp", other, "chrono::NaiveDateTime")),
            }
        }
    }

    impl TryFrom<Value> for DateTime<Utc> {
        type Error = Error;

        fn try_from(value: Value) -> Result<Self, Error> {
            match &value {
                Value::TimestampTz(us) | Value::Timestamp(us) | Value::BigInt(us) => {
                    datetime_from_micros(*us, "chrono::DateTime<Utc>")
                }
                Value::Text(s) => parse_naive_datetime(s)
                    .map(|dt| dt.and_utc())
                    .ok_or_else(|| parse_error("timestamp (RFC 3339)", s, "chrono::DateTime<Utc>")),
                other => Err(type_error("timestamptz", other, "chrono::DateTime<Utc>")),
            }
        }
    }

    impl TryFrom<Value> for DateTime<FixedOffset> {
        type Error = Error;

        fn try_from(value: Value) -> Result<Self, Error> {
            match &value {
                Value::Text(s) => DateTime::parse_from_rfc3339(s.trim()).map_err(|_| {
                    parse_error(
                        "timestamp (RFC 3339 with offset)",
                        s,
                        "chrono::DateTime<FixedOffset>",
                    )
                }),
                _ => DateTime::<Utc>::try_from(value).map(|dt| dt.fixed_offset()),
            }
        }
    }

    impl FromValue for NaiveDate {
        fn from_value(value: &Value) -> Result<Self, Error> {
            Self::try_from(value.clone())
        }
    }
    impl FromValue for NaiveTime {
        fn from_value(value: &Value) -> Result<Self, Error> {
            Self::try_from(value.clone())
        }
    }
    impl FromValue for NaiveDateTime {
        fn from_value(value: &Value) -> Result<Self, Error> {
            Self::try_from(value.clone())
        }
    }
    impl FromValue for DateTime<Utc> {
        fn from_value(value: &Value) -> Result<Self, Error> {
            Self::try_from(value.clone())
        }
    }
    impl FromValue for DateTime<FixedOffset> {
        fn from_value(value: &Value) -> Result<Self, Error> {
            Self::try_from(value.clone())
        }
    }
}

#[cfg(feature = "uuid")]
mod uuid_impls {
    use super::*;
    use uuid::Uuid;

    impl From<Uuid> for Value {
        fn from(u: Uuid) -> Self {
            Value::Uuid(*u.as_bytes())
        }
    }

    impl TryFrom<Value> for Uuid {
        type Error = Error;

        fn try_from(value: Value) -> Result<Self, Error> {
            match &value {
                Value::Uuid(bytes) => Ok(Uuid::from_bytes(*bytes)),
                Value::Bytes(b) if b.len() == 16 => Uuid::from_slice(b)
                    .map_err(|_| type_error("16 uuid bytes", &value, "uuid::Uuid")),
                Value::Text(s) => {
                    Uuid::parse_str(s.trim()).map_err(|_| parse_error("uuid text", s, "uuid::Uuid"))
                }
                other => Err(type_error("uuid", other, "uuid::Uuid")),
            }
        }
    }

    impl FromValue for Uuid {
        fn from_value(value: &Value) -> Result<Self, Error> {
            Self::try_from(value.clone())
        }
    }
}

#[cfg(feature = "decimal")]
mod decimal_impls {
    use super::*;
    use rust_decimal::Decimal;
    use rust_decimal::prelude::FromPrimitive;

    impl From<Decimal> for Value {
        /// Decimal text with its scale preserved (`12.50` stays `12.50`).
        fn from(d: Decimal) -> Self {
            Value::Decimal(d.to_string())
        }
    }

    impl TryFrom<Value> for Decimal {
        type Error = Error;

        fn try_from(value: Value) -> Result<Self, Error> {
            match &value {
                Value::Decimal(s) | Value::Text(s) => s
                    .trim()
                    .parse::<Decimal>()
                    .map_err(|_| parse_error("decimal text", s, "rust_decimal::Decimal")),
                Value::TinyInt(v) => Ok(Decimal::from(*v)),
                Value::SmallInt(v) => Ok(Decimal::from(*v)),
                Value::Int(v) => Ok(Decimal::from(*v)),
                Value::BigInt(v) => Ok(Decimal::from(*v)),
                Value::Float(v) => Decimal::from_f32(*v)
                    .ok_or_else(|| type_error("finite float", &value, "rust_decimal::Decimal")),
                Value::Double(v) => Decimal::from_f64(*v)
                    .ok_or_else(|| type_error("finite double", &value, "rust_decimal::Decimal")),
                other => Err(type_error("decimal", other, "rust_decimal::Decimal")),
            }
        }
    }

    impl FromValue for Decimal {
        fn from_value(value: &Value) -> Result<Self, Error> {
            Self::try_from(value.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use super::*;

    #[cfg(feature = "chrono")]
    mod chrono_tests {
        use super::*;
        use chrono::{
            DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc,
        };

        #[test]
        fn date_round_trips_including_pre_epoch_and_far_future() {
            for (y, m, d, days) in [
                (1970, 1, 1, 0),
                (1970, 1, 2, 1),
                (1969, 12, 31, -1),
                (2000, 2, 29, 11_016),
                (9999, 12, 31, 2_932_896),
            ] {
                let date = NaiveDate::from_ymd_opt(y, m, d).unwrap();
                let v = Value::from(date);
                assert_eq!(v, Value::Date(days), "{date}");
                assert_eq!(NaiveDate::try_from(v).unwrap(), date);
            }
        }

        #[test]
        fn date_accepts_text_and_timestamp_and_rejects_garbage() {
            assert_eq!(
                NaiveDate::try_from(Value::Text(" 2024-03-15 ".into())).unwrap(),
                NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()
            );
            let ts = NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(23, 59, 59)
                .unwrap();
            assert_eq!(NaiveDate::try_from(Value::from(ts)).unwrap(), ts.date());
            assert!(NaiveDate::try_from(Value::Text("15/03/2024".into())).is_err());
            assert!(NaiveDate::try_from(Value::Bool(true)).is_err());
            assert!(
                NaiveDate::try_from(Value::Date(i32::MAX)).is_err(),
                "out of range is an error, not a clamp"
            );
        }

        #[test]
        fn time_round_trips_with_microseconds_and_truncates_nanos() {
            let t = NaiveTime::from_hms_micro_opt(23, 59, 59, 999_999).unwrap();
            let v = Value::from(t);
            assert_eq!(v, Value::Time(86_399_999_999));
            assert_eq!(NaiveTime::try_from(v).unwrap(), t);

            let with_nanos = NaiveTime::from_hms_nano_opt(1, 2, 3, 123_456_789).unwrap();
            assert_eq!(
                Value::from(with_nanos),
                Value::Time(3_723_123_456),
                "nanoseconds truncated to micros"
            );

            assert_eq!(
                NaiveTime::try_from(Value::Text("07:08:09.5".into())).unwrap(),
                NaiveTime::from_hms_milli_opt(7, 8, 9, 500).unwrap()
            );
            assert_eq!(
                NaiveTime::try_from(Value::Text("07:08".into())).unwrap(),
                NaiveTime::from_hms_opt(7, 8, 0).unwrap()
            );
            assert!(
                NaiveTime::try_from(Value::Time(86_400_000_000)).is_err(),
                "24:00 is out of range"
            );
            assert!(NaiveTime::try_from(Value::Time(-1)).is_err());
        }

        #[test]
        fn naive_datetime_round_trips_and_parses_common_text_forms() {
            let dt = NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_micro_opt(10, 20, 30, 400_500)
                .unwrap();
            let v = Value::from(dt);
            assert_eq!(v, Value::Timestamp(1_710_498_030_400_500));
            assert_eq!(NaiveDateTime::try_from(v).unwrap(), dt);

            for text in [
                "2024-03-15 10:20:30.4005",
                "2024-03-15T10:20:30.400500",
                "2024-03-15T10:20:30.4005Z",
                "2024-03-15T12:20:30.4005+02:00",
            ] {
                assert_eq!(
                    NaiveDateTime::try_from(Value::Text(text.into())).unwrap(),
                    dt,
                    "{text}"
                );
            }
            assert_eq!(
                NaiveDateTime::try_from(Value::Text("2024-03-15 10:20:30".into())).unwrap(),
                dt.with_nanosecond(0).unwrap()
            );
            assert_eq!(
                NaiveDateTime::try_from(Value::Date(0)).unwrap(),
                NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
            );
            assert!(NaiveDateTime::try_from(Value::Text("yesterday".into())).is_err());
        }

        #[test]
        fn datetime_utc_and_fixed_offset_normalize_to_utc_micros() {
            let utc = Utc.with_ymd_and_hms(2024, 3, 15, 10, 20, 30).unwrap();
            let v = Value::from(utc);
            assert_eq!(v, Value::TimestampTz(1_710_498_030_000_000));
            assert_eq!(DateTime::<Utc>::try_from(v.clone()).unwrap(), utc);

            let plus_two: DateTime<FixedOffset> =
                DateTime::parse_from_rfc3339("2024-03-15T12:20:30+02:00").unwrap();
            assert_eq!(Value::from(plus_two), v, "offset applied, not stored");
            let back =
                DateTime::<FixedOffset>::try_from(Value::Text("2024-03-15T12:20:30+02:00".into()))
                    .unwrap();
            assert_eq!(back, plus_two);
            assert_eq!(
                back.offset().local_minus_utc(),
                7200,
                "text keeps its offset"
            );
            // A Timestamp (no zone) is read as UTC.
            assert_eq!(
                DateTime::<Utc>::try_from(Value::Timestamp(0)).unwrap(),
                Utc.timestamp_opt(0, 0).unwrap()
            );
        }

        #[test]
        fn option_fields_map_null_both_ways() {
            let none: Option<NaiveDate> = None;
            assert_eq!(Value::from(none), Value::Null);
            assert_eq!(
                Value::from(Some(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())),
                Value::Date(0)
            );
        }

        #[test]
        fn from_value_trait_matches_try_from() {
            let v = Value::Date(19_797);
            assert_eq!(
                <NaiveDate as FromValue>::from_value(&v).unwrap(),
                NaiveDate::try_from(v).unwrap()
            );
        }
    }

    #[cfg(feature = "uuid")]
    mod uuid_tests {
        use super::*;
        use uuid::Uuid;

        #[test]
        fn uuid_round_trips_bytes_text_and_blob_forms() {
            let u = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
            let v = Value::from(u);
            assert_eq!(v, Value::Uuid(*u.as_bytes()));
            assert_eq!(Uuid::try_from(v).unwrap(), u);
            assert_eq!(
                Uuid::try_from(Value::Text(" 6BA7B810-9DAD-11D1-80B4-00C04FD430C8 ".into()))
                    .unwrap(),
                u
            );
            assert_eq!(
                Uuid::try_from(Value::Text(u.simple().to_string())).unwrap(),
                u
            );
            assert_eq!(
                Uuid::try_from(Value::Bytes(u.as_bytes().to_vec())).unwrap(),
                u
            );
            assert_eq!(Uuid::try_from(Value::Uuid([0; 16])).unwrap(), Uuid::nil());
            assert!(Uuid::try_from(Value::Text("not-a-uuid".into())).is_err());
            assert!(Uuid::try_from(Value::Bytes(vec![1, 2, 3])).is_err());
            assert!(Uuid::try_from(Value::BigInt(1)).is_err());
            assert_eq!(
                <Uuid as FromValue>::from_value(&Value::Uuid(*u.as_bytes())).unwrap(),
                u
            );
        }
    }

    #[cfg(feature = "decimal")]
    mod decimal_tests {
        use super::*;
        use rust_decimal::Decimal;
        use std::str::FromStr;

        #[test]
        fn decimal_round_trips_preserving_scale_and_parses_numbers() {
            let d = Decimal::from_str("12.50").unwrap();
            let v = Value::from(d);
            assert_eq!(v, Value::Decimal("12.50".into()), "scale preserved");
            assert_eq!(Decimal::try_from(v).unwrap(), d);

            assert_eq!(
                Decimal::try_from(Value::Text("-0.0001".into())).unwrap(),
                Decimal::from_str("-0.0001").unwrap()
            );
            assert_eq!(
                Decimal::try_from(Value::BigInt(-42)).unwrap(),
                Decimal::from(-42)
            );
            assert_eq!(Decimal::try_from(Value::Int(7)).unwrap(), Decimal::from(7));
            assert_eq!(
                Decimal::try_from(Value::Double(2.5)).unwrap(),
                Decimal::from_str("2.5").unwrap()
            );
            assert!(Decimal::try_from(Value::Double(f64::NAN)).is_err());
            assert!(Decimal::try_from(Value::Text("12,50".into())).is_err());
            assert!(Decimal::try_from(Value::Bool(true)).is_err());

            let big = Decimal::from_str("79228162514264337593543950335").unwrap();
            assert_eq!(
                Decimal::try_from(Value::from(big)).unwrap(),
                big,
                "28-digit maximum round trips"
            );
        }
    }
}
