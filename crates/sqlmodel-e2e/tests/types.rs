//! Type round-trip matrix: every scalar the derive macro infers, including the
//! optional `chrono` / `uuid` / `rust_decimal` mappings the README promises,
//! written through `insert!` and read back through `select!` on every driver.
//!
//! The scenario also prints the raw `Value` variant each driver delivers per
//! column, which documents the wire form (C SQLite stores dates as ISO text,
//! FrankenSQLite as integers; MySQL returns DECIMAL as text; and so on).

use asupersync::Cx;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use rust_decimal::Decimal;
use sqlmodel::prelude::*;
use sqlmodel::{Dialect, SchemaBuilder};
use sqlmodel_e2e::{DriverUnderTest, Scenario, expect_outcome, run_on_every_driver};
use std::str::FromStr;
use uuid::Uuid;

#[derive(sqlmodel::Model, Debug, Clone, PartialEq)]
#[sqlmodel(table = "e2e_types")]
struct Everything {
    #[sqlmodel(primary_key)]
    id: i64,
    flag: bool,
    small: i16,
    medium: i32,
    big: i64,
    real: f32,
    double: f64,
    text: String,
    blob: Vec<u8>,
    #[sqlmodel(nullable)]
    maybe_int: Option<i32>,
    #[sqlmodel(nullable)]
    maybe_text: Option<String>,
    json: serde_json::Value,
    day: NaiveDate,
    clock: NaiveTime,
    stamp: NaiveDateTime,
    stamp_utc: DateTime<Utc>,
    uid: Uuid,
    #[sqlmodel(max_digits = 20, decimal_places = 6)]
    amount: Decimal,
}

fn rows() -> Vec<Everything> {
    vec![
        Everything {
            id: 1,
            flag: true,
            small: i16::MIN,
            medium: i32::MAX,
            big: i64::MIN,
            real: -0.5,
            double: 1e300,
            text: "héllo ✓ 日本 'quoted' \"double\"".into(),
            blob: vec![0, 1, 2, 254, 255],
            maybe_int: Some(7),
            maybe_text: None,
            json: serde_json::json!({"a": [1, 2, {"b": null}], "c": "x"}),
            day: NaiveDate::from_ymd_opt(1900, 1, 1).unwrap(),
            clock: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            stamp: NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            stamp_utc: Utc.with_ymd_and_hms(2024, 3, 15, 10, 20, 30).unwrap()
                + chrono::Duration::microseconds(123_456),
            uid: Uuid::nil(),
            amount: Decimal::from_str("-1234.567890").unwrap(),
        },
        Everything {
            id: 2,
            flag: false,
            small: i16::MAX,
            medium: i32::MIN,
            big: i64::MAX,
            real: 3.5,
            double: -2.25,
            text: String::new(),
            blob: Vec::new(),
            maybe_int: None,
            maybe_text: Some(String::new()),
            json: serde_json::json!("just a string"),
            day: NaiveDate::from_ymd_opt(9999, 12, 31).unwrap(),
            clock: NaiveTime::from_hms_micro_opt(23, 59, 59, 999_999).unwrap(),
            stamp: NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_micro_opt(10, 20, 30, 123_456)
                .unwrap(),
            // MySQL TIMESTAMP starts at 1970-01-01 00:00:01 UTC, so not the epoch itself.
            stamp_utc: Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 1).unwrap(),
            uid: Uuid::max(),
            amount: Decimal::ZERO,
        },
        Everything {
            id: 3,
            flag: true,
            small: 0,
            medium: 0,
            big: 0,
            real: 0.0,
            double: 0.0,
            text: "plain".into(),
            blob: b"binary\x00data".to_vec(),
            maybe_int: Some(i32::MIN),
            maybe_text: Some("some".into()),
            json: serde_json::json!([true, false, null, 1.5, "s"]),
            day: NaiveDate::from_ymd_opt(2024, 2, 29).unwrap(),
            clock: NaiveTime::from_hms_micro_opt(12, 34, 56, 500_000).unwrap(),
            stamp: NaiveDate::from_ymd_opt(2000, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            stamp_utc: Utc.with_ymd_and_hms(2038, 1, 19, 3, 14, 7).unwrap(),
            uid: Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef),
            // 20 significant digits: more than an f64 holds, so SQLite must keep it as text.
            amount: Decimal::from_str("12345678901234.567891").unwrap(),
        },
    ]
}

struct Types;

impl Scenario for Types {
    async fn run<C: Connection>(&self, cx: &Cx, conn: &C, driver: &DriverUnderTest) {
        let d = driver.name();
        let dialect = driver.dialect();
        let table = dialect.quote_identifier(<Everything as Model>::TABLE_NAME);
        expect_outcome(
            conn.execute(cx, &format!("DROP TABLE IF EXISTS {table}"), &[])
                .await,
            "drop stale table",
        );

        let ddl = SchemaBuilder::new()
            .dialect(dialect)
            .create_table::<Everything>()
            .build();
        eprintln!("{d}: ddl\n{}", ddl.join("\n"));
        // Pin the dialect-specific type names the builder must choose.
        let create = &ddl[0];
        match dialect {
            Dialect::Postgres => {
                assert!(create.contains("\"blob\" BYTEA"), "{create}");
                assert!(create.contains("\"uid\" UUID"), "{create}");
                assert!(create.contains("\"stamp_utc\" TIMESTAMPTZ"), "{create}");
            }
            Dialect::Mysql => {
                assert!(create.contains("`uid` BINARY(16)"), "{create}");
                assert!(create.contains("`stamp` DATETIME(6)"), "{create}");
                assert!(create.contains("`json` JSON"), "{create}");
            }
            Dialect::Sqlite => {
                assert!(create.contains("\"blob\" BLOB"), "{create}");
                // NUMERIC affinity would round the 20-digit decimal to 15 digits.
                assert!(create.contains("\"amount\" TEXT"), "{create}");
            }
        }
        for stmt in &ddl {
            expect_outcome(
                conn.execute(cx, stmt, &[]).await,
                &format!("{d}: ddl `{stmt}`"),
            );
        }

        let expected = rows();
        for row in &expected {
            expect_outcome(
                insert!(row).execute(cx, conn).await,
                &format!("{d}: insert {}", row.id),
            );
        }

        // Document the wire form every driver hands back for each column.
        let raw = expect_outcome(
            conn.query(cx, &format!("SELECT * FROM {table} WHERE id = 1"), &[])
                .await,
            &format!("{d}: raw select"),
        );
        assert_eq!(raw.len(), 1, "{d}");
        let names: Vec<String> = raw[0].column_names().map(str::to_owned).collect();
        for name in &names {
            let value = raw[0].get_named::<Value>(name).unwrap_or(Value::Null);
            eprintln!("{d}: {name:<10} -> {}", variant_name(&value));
        }

        let got: Vec<Everything> = expect_outcome(
            select!(Everything)
                .order_by(Expr::col("id").asc())
                .all(cx, conn)
                .await,
            &format!("{d}: select all"),
        );
        assert_eq!(got.len(), expected.len(), "{d}: row count");
        for (g, e) in got.iter().zip(&expected) {
            assert_eq!(g, e, "{d}: row {} does not round-trip", e.id);
        }

        // NULLs and a typed filter on a chrono column.
        let nulls: Vec<Everything> = expect_outcome(
            select!(Everything)
                .filter(Expr::col("maybe_text").is_null())
                .all(cx, conn)
                .await,
            &format!("{d}: null filter"),
        );
        assert_eq!(nulls.len(), 1, "{d}: exactly one NULL maybe_text");
        assert_eq!(nulls[0].id, 1, "{d}");

        let leap: Vec<Everything> = expect_outcome(
            select!(Everything)
                .filter(
                    Expr::col("day").eq(Value::from(NaiveDate::from_ymd_opt(2024, 2, 29).unwrap())),
                )
                .all(cx, conn)
                .await,
            &format!("{d}: date filter"),
        );
        assert_eq!(leap.len(), 1, "{d}: filter by NaiveDate");
        assert_eq!(leap[0].id, 3, "{d}");

        expect_outcome(
            conn.execute(cx, &format!("DROP TABLE {table}"), &[]).await,
            &format!("{d}: drop"),
        );
    }
}

fn variant_name(v: &Value) -> String {
    match v {
        Value::Null => "Null".into(),
        Value::Bool(_) => "Bool".into(),
        Value::TinyInt(_) => "TinyInt".into(),
        Value::SmallInt(_) => "SmallInt".into(),
        Value::Int(_) => "Int".into(),
        Value::BigInt(_) => "BigInt".into(),
        Value::Float(_) => "Float".into(),
        Value::Double(_) => "Double".into(),
        Value::Decimal(_) => "Decimal".into(),
        Value::Text(s) => format!("Text({})", s.chars().take(32).collect::<String>()),
        Value::Bytes(b) => format!("Bytes(len {})", b.len()),
        Value::Date(_) => "Date".into(),
        Value::Time(_) => "Time".into(),
        Value::Timestamp(_) => "Timestamp".into(),
        Value::TimestampTz(_) => "TimestampTz".into(),
        Value::Uuid(_) => "Uuid".into(),
        Value::Json(_) => "Json".into(),
        Value::Array(_) => "Array".into(),
        Value::Default => "Default".into(),
    }
}

#[test]
fn every_inferred_type_round_trips_on_every_available_driver() {
    let cx = Cx::for_testing();
    let ran = run_on_every_driver(&cx, &Types);
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
    assert!(ran.contains(&"c-sqlite(memory)"), "{ran:?}");
}
