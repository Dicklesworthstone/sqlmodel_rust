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
use sqlmodel_e2e::{
    ConnectionPair, DriverUnderTest, Scenario, expect_outcome, open_connection_pair,
    run_on_every_driver, unique_table,
};
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

        // the recorded checksum is the migration's own fingerprint
        let recorded = expect_outcome(
            conn.query(
                cx,
                &format!("SELECT checksum FROM {tracking} WHERE id = '0002_seed'"),
                &[],
            )
            .await,
            &format!("{d}: read checksum"),
        );
        let seed = &runner_migrations(&widgets, &gadgets, &q)[1];
        assert_eq!(
            recorded[0].get_as::<String>(0).unwrap(),
            seed.checksum(),
            "{d}: checksum recorded on apply"
        );

        // drift: editing an applied migration's SQL is reported and blocks the runner
        let drifted_runner = MigrationRunner::new({
            let mut all = runner_migrations(&widgets, &gadgets, &q);
            all[1].up.push_str(" -- edited after it was applied");
            all
        })
        .table_name(&tracking);
        let status = expect_outcome(
            drifted_runner.status(cx, conn).await,
            &format!("{d}: status with drift"),
        );
        assert!(
            matches!(&status[1], (id, MigrationStatus::Drifted { .. }) if id == "0002_seed"),
            "{d}: {status:?}"
        );
        assert!(
            matches!(&status[0], (_, MigrationStatus::Applied { .. })),
            "{d}: unedited migrations stay Applied: {status:?}"
        );
        match drifted_runner.migrate(cx, conn).await {
            Outcome::Err(e) => assert!(
                e.to_string().contains("0002_seed"),
                "{d}: drift error must name the migration: {e}"
            ),
            other => panic!("{d}: migrate must refuse on drift, got {other:?}"),
        }
        assert_eq!(
            recorded_ids(cx, conn, &tracking).await.len(),
            3,
            "{d}: drift changed nothing"
        );

        // Partial failure: two statements succeed, the third fails. The error
        // names the migration and the statement; no tracking row is written.
        // With transactional DDL (PostgreSQL, SQLite) the database is
        // unchanged; on MySQL the DDL has already committed (documented).
        let half = unique_table("e2e_mig_half");
        let broken = Migration::new(
            "0004_partial",
            "two good statements, then a missing table",
            format!(
                "CREATE TABLE IF NOT EXISTS {half_q} (id INTEGER PRIMARY KEY);\n\
                 INSERT INTO {half_q} (id) VALUES (1);\n\
                 INSERT INTO {} (id) VALUES (1)",
                q(&unique_table("e2e_mig_missing")),
                half_q = q(&half),
            ),
            format!("DROP TABLE IF EXISTS {}", q(&half)),
        );
        let runner_broken = MigrationRunner::new({
            let mut all = runner_migrations(&widgets, &gadgets, &q);
            all.push(broken);
            all
        })
        .table_name(&tracking);
        match runner_broken.migrate(cx, conn).await {
            Outcome::Err(e) => {
                let text = e.to_string();
                assert!(
                    text.contains("0004_partial") && text.contains("statement 3"),
                    "{d}: error must name the migration and the statement: {text}"
                );
            }
            other => panic!("{d}: failing up-migration must surface, got {other:?}"),
        }
        assert_eq!(
            recorded_ids(cx, conn, &tracking).await.len(),
            3,
            "{d}: partial migration not recorded"
        );
        let half_exists = matches!(
            conn.query(cx, &format!("SELECT COUNT(*) FROM {}", q(&half)), &[])
                .await,
            Outcome::Ok(_)
        );
        if driver.dialect().supports_transactional_ddl() {
            assert!(
                !half_exists,
                "{d}: transactional DDL must roll the whole migration back"
            );
        } else {
            assert!(
                half_exists,
                "{d}: MySQL DDL commits implicitly; the documented outcome is a leftover table"
            );
            expect_outcome(
                conn.execute(cx, &format!("DROP TABLE {}", q(&half)), &[])
                    .await,
                &format!("{d}: drop leftover"),
            );
        }

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
        // Two statements, the shape `Migration::from_operations` produces: the
        // runner must split them, since neither PostgreSQL's extended protocol
        // nor MySQL accepts two statements in one execute.
        Migration::new(
            "0003_gadgets",
            "create gadgets and index it",
            format!(
                "CREATE TABLE {} (id INTEGER PRIMARY KEY, widget_id INTEGER NOT NULL);\n\n\
                 CREATE INDEX {} ON {} (widget_id);",
                q(gadgets),
                q(&format!("{gadgets}_widget_idx")),
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

/// Two runners on two connections race to apply the same migrations. Exactly
/// one applies each migration. On PostgreSQL and MySQL the loser waits on the
/// runner's server-side lock and then reports nothing to apply; on SQLite it
/// may fail on the file lock or the tracking key instead, and a re-run then
/// finds nothing to apply.
fn race_two_runners<C: Connection + 'static>(first: &C, second: &C, driver: &DriverUnderTest) {
    let driver_name = driver.name();
    let dialect = driver.dialect();
    let quote = |name: &str| dialect.quote_identifier(name);
    let widgets = unique_table("e2e_race_widgets");
    let gadgets = unique_table("e2e_race_gadgets");
    let tracking = unique_table("e2e_race_history");
    let runner =
        MigrationRunner::new(runner_migrations(&widgets, &gadgets, &quote)).table_name(&tracking);

    let run = |conn: &C, who: &str| {
        let rt = asupersync::runtime::RuntimeBuilder::current_thread()
            .build()
            .expect("runtime");
        let cx = Cx::for_testing();
        let mut applied = Vec::new();
        let mut first_error = None;
        // Documented: no server lock on SQLite, so a runner may lose the file
        // lock or the tracking key to the other one and has to run again; it
        // never applies a migration twice. Elsewhere the first call must
        // succeed because the runner holds the server-side lock.
        for attempt in 1..=50 {
            match rt.block_on(runner.migrate(&cx, conn)) {
                Outcome::Ok(ids) => {
                    applied.extend(ids);
                    break;
                }
                Outcome::Err(e) if dialect == Dialect::Sqlite => {
                    first_error.get_or_insert_with(|| e.to_string());
                    assert!(attempt < 50, "{driver_name}: {who} never got through: {e}");
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                other => panic!("{driver_name}: runner {who} failed: {other:?}"),
            }
        }
        (who.to_string(), applied, first_error)
    };
    let (result_first, result_second) = std::thread::scope(|scope| {
        let thread_first = scope.spawn(|| run(first, "a"));
        let thread_second = scope.spawn(|| run(second, "b"));
        (
            thread_first.join().expect("runner a"),
            thread_second.join().expect("runner b"),
        )
    });
    eprintln!("{driver_name}: race results: {result_first:?} {result_second:?}");
    let mut applied: Vec<String> = result_first
        .1
        .iter()
        .chain(result_second.1.iter())
        .cloned()
        .collect();
    applied.sort();
    assert_eq!(
        applied,
        vec!["0001_widgets", "0002_seed", "0003_gadgets"],
        "{driver_name}: each migration applied exactly once across both runners"
    );
    if dialect != Dialect::Sqlite {
        assert!(
            result_first.2.is_none() && result_second.2.is_none(),
            "{driver_name}: with the server lock neither runner may error"
        );
    }

    let rt = asupersync::runtime::RuntimeBuilder::current_thread()
        .build()
        .expect("runtime");
    let cx = Cx::for_testing();
    rt.block_on(async {
        assert_eq!(
            recorded_ids(&cx, first, &tracking).await.len(),
            3,
            "{driver_name}: three tracking rows"
        );
        let seeded = expect_outcome(
            first
                .query(
                    &cx,
                    &format!("SELECT COUNT(*) FROM {}", quote(&widgets)),
                    &[],
                )
                .await,
            &format!("{driver_name}: count seed"),
        );
        assert_eq!(
            seeded[0].get_as::<i64>(0).unwrap(),
            1,
            "{driver_name}: seed applied once"
        );
        for table in [&gadgets, &widgets, &tracking] {
            expect_outcome(
                first
                    .execute(&cx, &format!("DROP TABLE {}", quote(table)), &[])
                    .await,
                &format!("{driver_name}: drop {table}"),
            );
        }
    });
}

#[test]
fn two_runners_apply_each_migration_exactly_once() {
    let cx = Cx::for_testing();
    let rt = asupersync::runtime::RuntimeBuilder::current_thread()
        .build()
        .expect("runtime");
    let mut ran = Vec::new();
    for driver in DriverUnderTest::available_multi_connection() {
        let Some(pair) = open_connection_pair(&cx, &rt, &driver) else {
            continue;
        };
        match pair {
            ConnectionPair::CSqlite(a, b) => race_two_runners(&a, &b, &driver),
            ConnectionPair::Franken(a, b) => race_two_runners(&a, &b, &driver),
            ConnectionPair::Postgres(a, b) => race_two_runners(&a, &b, &driver),
            ConnectionPair::MySql(a, b) => race_two_runners(&a, &b, &driver),
        }
        ran.push(driver.name());
    }
    eprintln!("sqlmodel-e2e: runner race ran on {}", ran.join(", "));
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
    assert!(ran.contains(&"c-sqlite(file)"), "{ran:?}");
}

#[test]
fn migration_runner_works_on_every_available_driver() {
    let cx = Cx::for_testing();
    let ran = run_on_every_driver(&cx, &Migrations);
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
}
