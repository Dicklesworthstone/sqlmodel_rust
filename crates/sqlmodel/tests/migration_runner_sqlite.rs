#![cfg(feature = "c-sqlite-tests")]

//! `MigrationRunner` against a real SQLite database.
//!
//! Until this test existed the runner had only SQL-string unit tests; nothing
//! ever executed `init`/`migrate`/`rollback` against a database. Writing it
//! surfaced a real defect (the runner hard-coded PostgreSQL `$n` placeholders
//! when recording migrations, which cannot work on MySQL), so this file also
//! asserts the tracking rows through raw SQL rather than through the runner.

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};

use sqlmodel::prelude::*;
use sqlmodel_schema::MigrationStatus;
use sqlmodel_sqlite::SqliteConnection;

fn unwrap_outcome<T>(outcome: Outcome<T, Error>) -> T {
    match outcome {
        Outcome::Ok(v) => v,
        Outcome::Err(e) => panic!("unexpected error: {e}"),
        Outcome::Cancelled(r) => panic!("cancelled: {r:?}"),
        Outcome::Panicked(p) => panic!("panicked: {p:?}"),
    }
}

fn migrations() -> Vec<Migration> {
    vec![
        Migration::new(
            "0001_create_widgets",
            "create widgets",
            "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            "DROP TABLE widgets",
        ),
        Migration::new(
            "0002_seed_widgets",
            "seed one widget",
            "INSERT INTO widgets (id, name) VALUES (1, 'gear')",
            "DELETE FROM widgets WHERE id = 1",
        ),
        Migration::new(
            "0003_create_gadgets",
            "create gadgets",
            "CREATE TABLE gadgets (id INTEGER PRIMARY KEY, widget_id INTEGER NOT NULL REFERENCES widgets(id))",
            "DROP TABLE gadgets",
        ),
    ]
}

async fn table_exists(cx: &Cx, conn: &SqliteConnection, table: &str) -> bool {
    let rows = unwrap_outcome(
        conn.query(
            cx,
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            &[Value::Text(table.to_string())],
        )
        .await,
    );
    rows[0].get_as::<i64>(0).unwrap() == 1
}

async fn recorded_ids(cx: &Cx, conn: &SqliteConnection, table: &str) -> Vec<String> {
    let rows = unwrap_outcome(
        conn.query(cx, &format!("SELECT id FROM {table} ORDER BY id"), &[])
            .await,
    );
    rows.iter()
        .map(|r| r.get_as::<String>(0).unwrap())
        .collect()
}

#[test]
fn migration_runner_applies_records_is_idempotent_and_rolls_back_on_sqlite() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = SqliteConnection::open_memory().expect("open sqlite memory db");
        // Table name goes through sanitization: the dash must be stripped.
        let runner = MigrationRunner::new(migrations()).table_name("schema-history");
        let tracking = "schemahistory";

        // init creates the tracking table.
        unwrap_outcome(runner.init(&cx, &conn).await);
        assert!(table_exists(&cx, &conn, tracking).await, "tracking table created");

        // Everything pending before the first run.
        let status = unwrap_outcome(runner.status(&cx, &conn).await);
        assert_eq!(status.len(), 3);
        assert!(status.iter().all(|(_, s)| *s == MigrationStatus::Pending));

        // migrate applies all three in declaration order and records each one.
        let applied = unwrap_outcome(runner.migrate(&cx, &conn).await);
        assert_eq!(
            applied,
            vec!["0001_create_widgets", "0002_seed_widgets", "0003_create_gadgets"]
        );
        assert!(table_exists(&cx, &conn, "widgets").await);
        assert!(table_exists(&cx, &conn, "gadgets").await);
        assert_eq!(
            recorded_ids(&cx, &conn, tracking).await,
            vec!["0001_create_widgets", "0002_seed_widgets", "0003_create_gadgets"],
            "every applied migration has a tracking row (raw SQL check)"
        );
        let seeded = unwrap_outcome(
            conn.query(&cx, "SELECT name FROM widgets WHERE id = 1", &[])
                .await,
        );
        assert_eq!(seeded[0].get_as::<String>(0).unwrap(), "gear");

        // status reflects the records with a plausible applied_at timestamp.
        let status = unwrap_outcome(runner.status(&cx, &conn).await);
        for (_, s) in &status {
            match s {
                MigrationStatus::Applied { at } => assert!(*at > 1_600_000_000, "unix seconds: {at}"),
                other => panic!("expected Applied, got {other:?}"),
            }
        }

        // Idempotent: a second run applies nothing and adds no rows.
        let applied_again = unwrap_outcome(runner.migrate(&cx, &conn).await);
        assert!(applied_again.is_empty(), "nothing pending on the second run");
        assert_eq!(recorded_ids(&cx, &conn, tracking).await.len(), 3);

        // rollback reverts the most recent migration and removes its record.
        let rolled = unwrap_outcome(runner.rollback(&cx, &conn).await);
        assert_eq!(rolled.as_deref(), Some("0003_create_gadgets"));
        assert!(!table_exists(&cx, &conn, "gadgets").await, "down SQL executed");
        assert!(table_exists(&cx, &conn, "widgets").await, "earlier migrations untouched");
        assert_eq!(
            recorded_ids(&cx, &conn, tracking).await,
            vec!["0001_create_widgets", "0002_seed_widgets"]
        );

        // Re-applying only runs the rolled-back one.
        let reapplied = unwrap_outcome(runner.migrate(&cx, &conn).await);
        assert_eq!(reapplied, vec!["0003_create_gadgets"]);
        assert!(table_exists(&cx, &conn, "gadgets").await);
    });
}

#[test]
fn failing_migration_is_not_recorded_and_earlier_ones_stay_applied() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = SqliteConnection::open_memory().expect("open sqlite memory db");
        let mut list = migrations();
        list.push(Migration::new(
            "0004_broken",
            "references a missing table",
            "INSERT INTO does_not_exist (id) VALUES (1)",
            "DELETE FROM does_not_exist WHERE id = 1",
        ));
        let runner = MigrationRunner::new(list);

        let result = runner.migrate(&cx, &conn).await;
        assert!(
            matches!(result, Outcome::Err(_)),
            "a failing up-migration must surface as an error, got {result:?}"
        );

        // The three good migrations were applied and recorded; the broken one was not.
        assert_eq!(
            recorded_ids(&cx, &conn, "_sqlmodel_migrations").await,
            vec!["0001_create_widgets", "0002_seed_widgets", "0003_create_gadgets"]
        );
        let status = unwrap_outcome(runner.status(&cx, &conn).await);
        let broken = status
            .iter()
            .find(|(id, _)| id == "0004_broken")
            .expect("broken migration listed");
        assert_eq!(broken.1, MigrationStatus::Pending);

        // Rolling back after the failure removes the last *successful* migration.
        let rolled = unwrap_outcome(runner.rollback(&cx, &conn).await);
        assert_eq!(rolled.as_deref(), Some("0003_create_gadgets"));
    });
}

#[test]
fn rollback_on_a_fresh_database_is_a_no_op() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = SqliteConnection::open_memory().expect("open sqlite memory db");
        let runner = MigrationRunner::new(migrations());
        let rolled = unwrap_outcome(runner.rollback(&cx, &conn).await);
        assert_eq!(rolled, None);
    });
}
