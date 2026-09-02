//! The ORM's basic path on every driver: derive a model, generate its DDL,
//! insert, select with a filter, update, delete, drop.
//!
//! This is the first time `select!`/`insert!` run through the FrankenSQLite
//! driver at all, and the first time they run against PostgreSQL/MySQL in a
//! test that CI executes. The first live PostgreSQL run showed that
//! `insert!(model).execute()` had never worked there (no `RETURNING`, which the
//! driver needs to report the id); the builder now adds it.

use asupersync::Cx;
use sqlmodel::SchemaBuilder;
use sqlmodel::prelude::*;
use sqlmodel_e2e::{DriverUnderTest, Scenario, expect_outcome, run_on_every_driver};

#[derive(sqlmodel::Model, Debug, Clone, PartialEq)]
#[sqlmodel(table = "e2e_smoke_gadgets")]
struct Gadget {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
    #[sqlmodel(nullable)]
    weight_grams: Option<i32>,
}

struct Smoke;

impl Scenario for Smoke {
    async fn run<C: Connection>(&self, cx: &Cx, conn: &C, driver: &DriverUnderTest) {
        let table = driver
            .dialect()
            .quote_identifier(<Gadget as Model>::TABLE_NAME);
        // Shared network databases may hold a table from an aborted earlier run.
        expect_outcome(
            conn.execute(cx, &format!("DROP TABLE IF EXISTS {table}"), &[])
                .await,
            "drop stale table",
        );

        for stmt in SchemaBuilder::new()
            .dialect(driver.dialect())
            .create_table::<Gadget>()
            .build()
        {
            expect_outcome(
                conn.execute(cx, &stmt, &[]).await,
                &format!("{}: ddl `{stmt}`", driver.name()),
            );
        }

        let rows = [
            Gadget {
                id: 1,
                name: "gear".into(),
                weight_grams: Some(120),
            },
            Gadget {
                id: 2,
                name: "spring".into(),
                weight_grams: None,
            },
            Gadget {
                id: 3,
                name: "bolt".into(),
                weight_grams: Some(15),
            },
        ];
        for g in &rows {
            expect_outcome(
                insert!(g).execute(cx, conn).await,
                &format!("{}: insert {}", driver.name(), g.id),
            );
        }

        let heavy: Vec<Gadget> = expect_outcome(
            select!(Gadget)
                .filter(Expr::col("weight_grams").gt(100))
                .all(cx, conn)
                .await,
            "select heavy",
        );
        assert_eq!(
            heavy,
            vec![rows[0].clone()],
            "{}: filter on nullable int",
            driver.name()
        );

        let nulls: Vec<Gadget> = expect_outcome(
            select!(Gadget)
                .filter(Expr::col("weight_grams").is_null())
                .all(cx, conn)
                .await,
            "select nulls",
        );
        assert_eq!(nulls, vec![rows[1].clone()], "{}: IS NULL", driver.name());

        let all: Vec<Gadget> = expect_outcome(
            select!(Gadget)
                .order_by(Expr::col("id").asc())
                .all(cx, conn)
                .await,
            "select all",
        );
        assert_eq!(
            all,
            rows.to_vec(),
            "{}: round trip of every row",
            driver.name()
        );

        let updated = expect_outcome(
            update!(&Gadget {
                id: 3,
                name: "bolt".into(),
                weight_grams: Some(16)
            })
            .execute(cx, conn)
            .await,
            "update",
        );
        assert_eq!(updated, 1, "{}: update affects one row", driver.name());

        let one: Gadget = expect_outcome(
            select!(Gadget)
                .filter(Expr::col("id").eq(3))
                .one(cx, conn)
                .await,
            "one",
        );
        assert_eq!(
            one.weight_grams,
            Some(16),
            "{}: update visible",
            driver.name()
        );

        let deleted = expect_outcome(
            delete!(Gadget)
                .filter(Expr::col("id").eq(2))
                .execute(cx, conn)
                .await,
            "delete",
        );
        assert_eq!(deleted, 1, "{}: delete affects one row", driver.name());

        let count = expect_outcome(
            conn.query(cx, &format!("SELECT COUNT(*) FROM {table}"), &[])
                .await,
            "count",
        );
        assert_eq!(
            count[0].get_as::<i64>(0).unwrap(),
            2,
            "{}: two rows remain",
            driver.name()
        );

        expect_outcome(
            conn.execute(cx, &format!("DROP TABLE {table}"), &[]).await,
            "drop",
        );
    }
}

/// A table and columns named after reserved words. Every builder must quote
/// identifiers for the dialect or this cannot even be created.
#[derive(sqlmodel::Model, Debug, Clone, PartialEq)]
#[sqlmodel(table = "order")]
struct Order {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(column = "user")]
    user: String,
    #[sqlmodel(column = "select")]
    select_: i32,
}

struct ReservedWords;

impl Scenario for ReservedWords {
    async fn run<C: Connection>(&self, cx: &Cx, conn: &C, driver: &DriverUnderTest) {
        let d = driver.name();
        let table = driver.dialect().quote_identifier("order");
        expect_outcome(
            conn.execute(cx, &format!("DROP TABLE IF EXISTS {table}"), &[])
                .await,
            &format!("{d}: drop stale order"),
        );
        for stmt in SchemaBuilder::new()
            .dialect(driver.dialect())
            .create_table::<Order>()
            .build()
        {
            expect_outcome(
                conn.execute(cx, &stmt, &[]).await,
                &format!("{d}: ddl `{stmt}`"),
            );
        }
        let rows = [
            Order {
                id: 1,
                user: "ann".into(),
                select_: 10,
            },
            Order {
                id: 2,
                user: "bob".into(),
                select_: 20,
            },
        ];
        for o in &rows {
            expect_outcome(
                insert!(o).execute(cx, conn).await,
                &format!("{d}: insert order {}", o.id),
            );
        }
        let big: Vec<Order> = expect_outcome(
            select!(Order)
                .filter(Expr::col("select").gt(15))
                .order_by(Expr::col("user").asc())
                .all(cx, conn)
                .await,
            &format!("{d}: select from order"),
        );
        assert_eq!(
            big,
            vec![rows[1].clone()],
            "{d}: filter on a reserved column"
        );
        let updated = expect_outcome(
            update!(&Order {
                id: 1,
                user: "ann".into(),
                select_: 11
            })
            .execute(cx, conn)
            .await,
            &format!("{d}: update order"),
        );
        assert_eq!(updated, 1, "{d}");
        let deleted = expect_outcome(
            delete!(Order)
                .filter(Expr::col("user").eq("bob"))
                .execute(cx, conn)
                .await,
            &format!("{d}: delete from order"),
        );
        assert_eq!(deleted, 1, "{d}");
        let left: Vec<Order> = expect_outcome(
            select!(Order).all(cx, conn).await,
            &format!("{d}: select remaining"),
        );
        assert_eq!(left.len(), 1, "{d}");
        assert_eq!(left[0].select_, 11, "{d}: update landed");
        expect_outcome(
            conn.execute(cx, &format!("DROP TABLE {table}"), &[]).await,
            &format!("{d}: drop order"),
        );
    }
}

#[test]
fn reserved_word_identifiers_work_on_every_available_driver() {
    let cx = Cx::for_testing();
    let ran = run_on_every_driver(&cx, &ReservedWords);
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
}

#[test]
fn model_crud_round_trips_on_every_available_driver() {
    let cx = Cx::for_testing();
    let ran = run_on_every_driver(&cx, &Smoke);
    assert!(
        ran.contains(&"frankensqlite"),
        "FrankenSQLite must be exercised: {ran:?}"
    );
    assert!(ran.contains(&"c-sqlite(memory)"));
}
