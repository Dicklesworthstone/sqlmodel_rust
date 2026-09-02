//! The ORM's basic path on every driver: derive a model, generate its DDL,
//! insert, select with a filter, update, delete, drop.
//!
//! This is the first time `select!`/`insert!` run through the FrankenSQLite
//! driver at all, and the first time they run against PostgreSQL/MySQL in a
//! test that CI executes.

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
        let table = driver.dialect().quote_identifier(<Gadget as Model>::TABLE_NAME);
        // Shared network databases may hold a table from an aborted earlier run.
        expect_outcome(
            conn.execute(cx, &format!("DROP TABLE IF EXISTS {table}"), &[]).await,
            "drop stale table",
        );

        for stmt in SchemaBuilder::new().create_table::<Gadget>().build() {
            expect_outcome(conn.execute(cx, &stmt, &[]).await, &format!("{}: ddl `{stmt}`", driver.name()));
        }

        let rows = [
            Gadget { id: 1, name: "gear".into(), weight_grams: Some(120) },
            Gadget { id: 2, name: "spring".into(), weight_grams: None },
            Gadget { id: 3, name: "bolt".into(), weight_grams: Some(15) },
        ];
        for g in &rows {
            expect_outcome(insert!(g).execute(cx, conn).await, &format!("{}: insert {}", driver.name(), g.id));
        }

        let heavy: Vec<Gadget> = expect_outcome(
            select!(Gadget)
                .filter(Expr::col("weight_grams").gt(100))
                .all(cx, conn)
                .await,
            "select heavy",
        );
        assert_eq!(heavy, vec![rows[0].clone()], "{}: filter on nullable int", driver.name());

        let nulls: Vec<Gadget> = expect_outcome(
            select!(Gadget)
                .filter(Expr::col("weight_grams").is_null())
                .all(cx, conn)
                .await,
            "select nulls",
        );
        assert_eq!(nulls, vec![rows[1].clone()], "{}: IS NULL", driver.name());

        let all: Vec<Gadget> = expect_outcome(
            select!(Gadget).order_by(Expr::col("id").asc()).all(cx, conn).await,
            "select all",
        );
        assert_eq!(all, rows.to_vec(), "{}: round trip of every row", driver.name());

        let updated = expect_outcome(
            update!(&Gadget { id: 3, name: "bolt".into(), weight_grams: Some(16) })
                .execute(cx, conn)
                .await,
            "update",
        );
        assert_eq!(updated, 1, "{}: update affects one row", driver.name());

        let one: Gadget = expect_outcome(
            select!(Gadget).filter(Expr::col("id").eq(3)).one(cx, conn).await,
            "one",
        );
        assert_eq!(one.weight_grams, Some(16), "{}: update visible", driver.name());

        let deleted = expect_outcome(
            delete!(Gadget).filter(Expr::col("id").eq(2)).execute(cx, conn).await,
            "delete",
        );
        assert_eq!(deleted, 1, "{}: delete affects one row", driver.name());

        let count = expect_outcome(
            conn.query(cx, &format!("SELECT COUNT(*) FROM {table}"), &[]).await,
            "count",
        );
        assert_eq!(count[0].get_as::<i64>(0).unwrap(), 2, "{}: two rows remain", driver.name());

        expect_outcome(conn.execute(cx, &format!("DROP TABLE {table}"), &[]).await, "drop");
    }
}

#[test]
fn model_crud_round_trips_on_every_available_driver() {
    let cx = Cx::for_testing();
    let ran = run_on_every_driver(&cx, &Smoke);
    assert!(ran.contains(&"frankensqlite"), "FrankenSQLite must be exercised: {ran:?}");
    assert!(ran.contains(&"c-sqlite(memory)"));
}
