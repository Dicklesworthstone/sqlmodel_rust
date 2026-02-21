#![cfg(feature = "c-sqlite-tests")]

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};

use sqlmodel::prelude::*;
use sqlmodel_schema::introspect::{Dialect, Introspector};
use sqlmodel_sqlite::SqliteConnection;

fn unwrap_outcome<T>(outcome: Outcome<T, Error>) -> std::result::Result<T, String> {
    match outcome {
        Outcome::Ok(v) => Ok(v),
        Outcome::Err(e) => Err(format!("unexpected error: {e}")),
        Outcome::Cancelled(r) => Err(format!("cancelled: {r:?}")),
        Outcome::Panicked(p) => Err(format!("panicked: {p:?}")),
    }
}

fn compact_sql_fragment(input: &str) -> String {
    input
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '"' && *c != '`')
        .collect::<String>()
        .to_ascii_lowercase()
}

#[test]
fn sqlite_introspector_extracts_check_constraints_from_live_schema() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = SqliteConnection::open_memory().expect("open sqlite memory db");
        let create_sql = r"
            CREATE TABLE heroes (
                id INTEGER PRIMARY KEY,
                age INTEGER NOT NULL,
                kind TEXT,
                CONSTRAINT age_non_negative CHECK (age >= 0),
                CHECK (age <= 150),
                CHECK (kind IN ('A,B', 'C'))
            )
        ";

        unwrap_outcome(conn.execute(&cx, create_sql, &[]).await).expect("create heroes");

        let introspector = Introspector::new(Dialect::Sqlite);
        let table_info = unwrap_outcome(introspector.table_info(&cx, &conn, "heroes").await)
            .expect("introspect heroes");

        assert_eq!(table_info.comment, None);
        assert_eq!(
            table_info.check_constraints.len(),
            3,
            "unexpected checks: {:?}",
            table_info
                .check_constraints
                .iter()
                .map(|c| (&c.name, &c.expression))
                .collect::<Vec<_>>()
        );

        let named = table_info
            .check_constraints
            .iter()
            .find(|c| c.name.as_deref() == Some("age_non_negative"));
        assert!(
            named.is_some(),
            "missing age_non_negative in {:?}",
            table_info
                .check_constraints
                .iter()
                .map(|c| (&c.name, &c.expression))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            named.expect("named check should exist").expression,
            "age >= 0"
        );

        for check in &table_info.check_constraints {
            let expr = check.expression.trim_start();
            assert!(
                !expr
                    .get(..5)
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CHECK")),
                "expression should be normalized without CHECK prefix: {}",
                check.expression
            );
        }

        let normalized: Vec<String> = table_info
            .check_constraints
            .iter()
            .map(|c| compact_sql_fragment(&c.expression))
            .collect();
        assert!(
            normalized.iter().any(|expr| expr == "age<=150"),
            "missing age<=150 check expression in {normalized:?}"
        );
        assert!(
            normalized.iter().any(|expr| expr == "kindin('a,b','c')"),
            "missing kind IN ('A,B','C') check expression in {normalized:?}"
        );
    });
}

#[test]
fn sqlite_introspector_handles_special_character_table_names() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = SqliteConnection::open_memory().expect("open sqlite memory db");
        let table_name = "heroes table-1";
        let create_sql = r#"
            CREATE TABLE "heroes table-1" (
                id INTEGER PRIMARY KEY,
                age INTEGER NOT NULL,
                CHECK (age >= 0)
            )
        "#;

        unwrap_outcome(conn.execute(&cx, create_sql, &[]).await).expect("create quoted table");

        let introspector = Introspector::new(Dialect::Sqlite);
        let names =
            unwrap_outcome(introspector.table_names(&cx, &conn).await).expect("table names");
        assert!(
            names.iter().any(|name| name == table_name),
            "expected table_names to include quoted-name table, got: {names:?}"
        );

        let table_info = unwrap_outcome(introspector.table_info(&cx, &conn, table_name).await)
            .expect("introspect quoted-name table");
        assert_eq!(table_info.name, table_name);
        assert!(
            table_info.columns.iter().any(|c| c.name == "id"),
            "expected id column in {:?}",
            table_info
                .columns
                .iter()
                .map(|c| &c.name)
                .collect::<Vec<_>>()
        );
        assert!(
            table_info
                .check_constraints
                .iter()
                .any(|c| compact_sql_fragment(&c.expression) == "age>=0"),
            "expected CHECK(age>=0), got {:?}",
            table_info
                .check_constraints
                .iter()
                .map(|c| (&c.name, &c.expression))
                .collect::<Vec<_>>()
        );
    });
}
