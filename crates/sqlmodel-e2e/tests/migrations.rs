//! `MigrationRunner` against every driver.
//!
//! Writing this test found two defects that made the runner unusable on MySQL
//! (a `TEXT PRIMARY KEY` tracking column and hard-coded `$n` placeholders);
//! both are fixed in `sqlmodel-schema`. Running it against a live PostgreSQL
//! found a third, in the driver: after a failed statement the connection was
//! left desynchronized (the `ReadyForQuery` behind an `ErrorResponse` was never
//! read), so the next query returned nothing and the runner re-applied every
//! migration. The tracking rows are asserted with raw SQL so the runner cannot
//! vouch for itself.

use asupersync::Cx;
use sqlmodel::prelude::*;
use sqlmodel_e2e::{DriverUnderTest, Scenario, expect_outcome, run_on_every_driver, unique_table};
use sqlmodel_schema::MigrationStatus;

struct Migrations;

impl Scenario for Migrations {
    async fn run<C: Connection>(&self, cx: &Cx, conn: &C, driver: &DriverUnderTest) {
        let d = driver.name();
        let q = |name: &str| driver.dialect().quote_identifier(name);
        let widgets = unique_table("e2e_mig_widgets");
        let gadgets = unique_table("e2e_mig_gadgets");
        let tracking = unique_table("e2e_mig_history");

        let runner =
            MigrationRunner::new(runner_migrations(&widgets, &gadgets, &q)).table_name(&tracking);

        // init + everything pending
        expect_outcome(runner.init(cx, conn).await, &format!("{d}: init"));
        let status = expect_outcome(runner.status(cx, conn).await, &format!("{d}: status"));
        assert!(
            status.iter().all(|(_, s)| *s == MigrationStatus::Pending),
            "{d}: {status:?}"
        );

        // migrate applies in order and records each id (checked with raw SQL)
        let applied = expect_outcome(runner.migrate(cx, conn).await, &format!("{d}: migrate"));
        assert_eq!(
            applied,
            vec!["0001_widgets", "0002_seed", "0003_gadgets"],
            "{d}"
        );
        let ids = recorded_ids(cx, conn, &tracking).await;
        assert_eq!(
            ids,
            vec!["0001_widgets", "0002_seed", "0003_gadgets"],
            "{d}: tracking rows"
        );
        let seeded = expect_outcome(
            conn.query(
                cx,
                &format!("SELECT name FROM {} WHERE id = 1", q(&widgets)),
                &[],
            )
            .await,
            &format!("{d}: read seed"),
        );
        assert_eq!(seeded[0].get_as::<String>(0).unwrap(), "gear", "{d}");

        // idempotent
        let again = expect_outcome(
            runner.migrate(cx, conn).await,
            &format!("{d}: migrate again"),
        );
        assert!(again.is_empty(), "{d}: second run applied {again:?}");
        assert_eq!(recorded_ids(cx, conn, &tracking).await.len(), 3, "{d}");

        // rollback removes the last migration and its record; earlier ones stay
        let rolled = expect_outcome(runner.rollback(cx, conn).await, &format!("{d}: rollback"));
        assert_eq!(rolled.as_deref(), Some("0003_gadgets"), "{d}");
        assert_eq!(
            recorded_ids(cx, conn, &tracking).await,
            vec!["0001_widgets", "0002_seed"],
            "{d}"
        );
        assert!(
            matches!(
                conn.query(cx, &format!("SELECT 1 FROM {}", q(&gadgets)), &[])
                    .await,
                Outcome::Err(_)
            ),
            "{d}: gadgets table must be gone after rollback"
        );

        // re-apply only the rolled back one
        let reapplied = expect_outcome(runner.migrate(cx, conn).await, &format!("{d}: re-apply"));
        assert_eq!(reapplied, vec!["0003_gadgets"], "{d}");

        // a failing migration is not recorded and earlier ones stay applied
        let mut broken = vec![Migration::new(
            "0004_broken",
            "references a missing table",
            format!(
                "INSERT INTO {} (id) VALUES (1)",
                q(&unique_table("e2e_mig_missing"))
            ),
            "SELECT 1",
        )];
        let runner_broken = MigrationRunner::new({
            let mut all = runner_migrations(&widgets, &gadgets, &q);
            all.append(&mut broken);
            all
        })
        .table_name(&tracking);
        assert!(
            matches!(runner_broken.migrate(cx, conn).await, Outcome::Err(_)),
            "{d}: failing up-migration must surface"
        );
        assert_eq!(
            recorded_ids(cx, conn, &tracking).await.len(),
            3,
            "{d}: broken one not recorded"
        );

        // cleanup
        for t in [&gadgets, &widgets, &tracking] {
            expect_outcome(
                conn.execute(cx, &format!("DROP TABLE {}", q(t)), &[]).await,
                &format!("{d}: drop {t}"),
            );
        }
    }
}

fn runner_migrations(widgets: &str, gadgets: &str, q: &dyn Fn(&str) -> String) -> Vec<Migration> {
    vec![
        Migration::new(
            "0001_widgets",
            "create widgets",
            format!(
                "CREATE TABLE {} (id INTEGER PRIMARY KEY, name VARCHAR(64) NOT NULL)",
                q(widgets)
            ),
            format!("DROP TABLE {}", q(widgets)),
        ),
        Migration::new(
            "0002_seed",
            "seed a widget",
            format!("INSERT INTO {} (id, name) VALUES (1, 'gear')", q(widgets)),
            format!("DELETE FROM {} WHERE id = 1", q(widgets)),
        ),
        Migration::new(
            "0003_gadgets",
            "create gadgets",
            format!(
                "CREATE TABLE {} (id INTEGER PRIMARY KEY, widget_id INTEGER NOT NULL)",
                q(gadgets)
            ),
            format!("DROP TABLE {}", q(gadgets)),
        ),
    ]
}

async fn recorded_ids<C: Connection>(cx: &Cx, conn: &C, tracking: &str) -> Vec<String> {
    let rows = expect_outcome(
        conn.query(cx, &format!("SELECT id FROM {tracking} ORDER BY id"), &[])
            .await,
        "read tracking table",
    );
    rows.iter()
        .map(|r| r.get_as::<String>(0).unwrap())
        .collect()
}

#[test]
fn migration_runner_works_on_every_available_driver() {
    let cx = Cx::for_testing();
    let ran = run_on_every_driver(&cx, &Migrations);
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
}
