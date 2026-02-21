#![cfg(feature = "c-sqlite-tests")]

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};

use sqlmodel::SchemaBuilder;
use sqlmodel::prelude::*;
use sqlmodel_sqlite::SqliteConnection;

fn unwrap_outcome<T>(outcome: Outcome<T, Error>) -> std::result::Result<T, String> {
    match outcome {
        Outcome::Ok(v) => Ok(v),
        Outcome::Err(e) => Err(format!("unexpected error: {e}")),
        Outcome::Cancelled(r) => Err(format!("cancelled: {r:?}")),
        Outcome::Panicked(p) => Err(format!("panicked: {p:?}")),
    }
}

#[derive(sqlmodel::Model, Debug, Clone, PartialEq)]
#[sqlmodel(table)]
struct User {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
}

#[test]
fn sqlite_select_one_enforces_exactly_one_row() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = SqliteConnection::open_memory().expect("open sqlite memory db");

        let stmts = SchemaBuilder::new().create_table::<User>().build();
        for stmt in stmts {
            unwrap_outcome(conn.execute(&cx, &stmt, &[]).await).expect("execute ddl");
        }

        unwrap_outcome(
            conn.execute(
                &cx,
                "INSERT INTO users (id, name) VALUES (?1, ?2)",
                &[Value::BigInt(1), Value::Text("Alice".to_string())],
            )
            .await,
        )
        .expect("insert alice");
        unwrap_outcome(
            conn.execute(
                &cx,
                "INSERT INTO users (id, name) VALUES (?1, ?2)",
                &[Value::BigInt(2), Value::Text("Bob".to_string())],
            )
            .await,
        )
        .expect("insert bob");

        let alice = unwrap_outcome(
            select!(User)
                .filter(Expr::col("id").eq(1_i64))
                .one(&cx, &conn)
                .await,
        )
        .expect("one should return alice");
        assert_eq!(
            alice,
            User {
                id: 1,
                name: "Alice".to_string()
            }
        );

        let none = select!(User)
            .filter(Expr::col("id").eq(999_i64))
            .one(&cx, &conn)
            .await;
        assert!(matches!(none, Outcome::Err(Error::Custom(_))));
        if let Outcome::Err(Error::Custom(msg)) = none {
            assert!(msg.contains("found none"));
        }

        let many = select!(User).one(&cx, &conn).await;
        assert!(matches!(many, Outcome::Err(Error::Custom(_))));
        if let Outcome::Err(Error::Custom(msg)) = many {
            assert!(
                msg.contains("Expected zero or one row, found 2"),
                "unexpected message: {msg}"
            );
        }

        let one_or_none_hit = unwrap_outcome(
            select!(User)
                .filter(Expr::col("id").eq(1_i64))
                .one_or_none(&cx, &conn)
                .await,
        )
        .expect("one_or_none should return alice");
        assert_eq!(
            one_or_none_hit,
            Some(User {
                id: 1,
                name: "Alice".to_string()
            })
        );

        let one_or_none_missing = unwrap_outcome(
            select!(User)
                .filter(Expr::col("id").eq(999_i64))
                .one_or_none(&cx, &conn)
                .await,
        )
        .expect("one_or_none should return none");
        assert!(one_or_none_missing.is_none());

        let one_or_none_many = select!(User).one_or_none(&cx, &conn).await;
        assert!(matches!(one_or_none_many, Outcome::Err(Error::Custom(_))));
        if let Outcome::Err(Error::Custom(msg)) = one_or_none_many {
            assert!(
                msg.contains("Expected zero or one row, found 2"),
                "unexpected message: {msg}"
            );
        }
    });
}
