//! Concurrent writers through the public API: `TransactionOptions::concurrent()`
//! + `retry_transaction`, two OS threads, one shared row, no lost updates.
//!
//! * FrankenSQLite: `BEGIN CONCURRENT` (page-level MVCC); conflicting writers
//!   get a retryable error and the combinator retries them.
//! * PostgreSQL / MySQL: their native MVCC (Concurrent == default).
//! * C SQLite: `Concurrent` is refused with `UnsupportedMode` and the scenario
//!   asserts exactly that instead of silently downgrading.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};
use sqlmodel::prelude::*;
use sqlmodel_core::TransactionOps;
use sqlmodel_core::error::TransactionErrorKind;
use sqlmodel_e2e::{
    ConnectionPair, DriverUnderTest, expect_outcome, open_connection_pair, unique_table,
};

const INCREMENTS_PER_WRITER: u32 = 25;

/// Run `INCREMENTS_PER_WRITER` retried increments on `conn` from one thread.
/// Returns the number of retry-loop bodies executed (>= increments; the excess
/// is the number of conflicts that were retried).
fn hammer<C: Connection + Sync>(conn: &C, table: &str, quoted: &str) -> u32 {
    let rt = RuntimeBuilder::current_thread().build().expect("runtime");
    let cx = Cx::for_testing();
    let bodies = AtomicU32::new(0);
    let policy = RetryPolicy::default()
        .max_attempts(50)
        .base_delay(Duration::from_millis(2))
        .max_delay(Duration::from_millis(40));
    let sql = format!("UPDATE {quoted} SET counter = counter + 1 WHERE id = 1");
    for i in 0..INCREMENTS_PER_WRITER {
        let out = rt.block_on(retry_transaction(
            &cx,
            conn,
            TransactionOptions::concurrent(),
            &policy,
            async |cx, tx| {
                bodies.fetch_add(1, Ordering::Relaxed);
                match tx.execute(cx, &sql, &[]).await {
                    Outcome::Ok(_) => Outcome::Ok(()),
                    Outcome::Err(e) => Outcome::Err(e),
                    Outcome::Cancelled(r) => Outcome::Cancelled(r),
                    Outcome::Panicked(p) => Outcome::Panicked(p),
                }
            },
        ));
        expect_outcome(out, &format!("{table}: increment {i}"));
    }
    bodies.load(Ordering::Relaxed)
}

fn run_pair<C: Connection + Sync>(a: &C, b: &C, table: &str, quoted: &str) -> (u32, i64) {
    let cx = Cx::for_testing();
    let rt = RuntimeBuilder::current_thread().build().expect("runtime");
    rt.block_on(async {
        expect_outcome(
            a.execute(&cx, &format!("DROP TABLE IF EXISTS {quoted}"), &[])
                .await,
            "drop stale",
        );
        expect_outcome(
            a.execute(
                &cx,
                &format!(
                    "CREATE TABLE {quoted} (id INTEGER PRIMARY KEY, counter INTEGER NOT NULL)"
                ),
                &[],
            )
            .await,
            "create",
        );
        expect_outcome(
            a.execute(
                &cx,
                &format!("INSERT INTO {quoted} (id, counter) VALUES (1, 0)"),
                &[],
            )
            .await,
            "seed",
        );
    });

    let bodies = std::thread::scope(|s| {
        let ta = s.spawn(|| hammer(a, table, quoted));
        let tb = s.spawn(|| hammer(b, table, quoted));
        ta.join().expect("writer a") + tb.join().expect("writer b")
    });

    let final_value = rt.block_on(async {
        let rows = expect_outcome(
            a.query(
                &cx,
                &format!("SELECT counter FROM {quoted} WHERE id = 1"),
                &[],
            )
            .await,
            "read final",
        );
        let v = rows[0].get_as::<i64>(0).unwrap();
        expect_outcome(
            a.execute(&cx, &format!("DROP TABLE {quoted}"), &[]).await,
            "drop",
        );
        v
    });
    (bodies, final_value)
}

#[test]
fn two_writers_never_lose_an_update_and_conflicts_are_retried() {
    let cx = Cx::for_testing();
    let rt = RuntimeBuilder::current_thread().build().expect("runtime");
    let mut exercised = Vec::new();

    for driver in DriverUnderTest::available_multi_connection() {
        let table = unique_table("e2e_counter");
        let quoted = driver.dialect().quote_identifier(&table);
        let Some(pair) = open_connection_pair(&cx, &rt, &driver) else {
            continue;
        };

        if !driver.supports_concurrent_transactions() {
            // C SQLite: the mode must be refused explicitly, not downgraded.
            let ConnectionPair::CSqlite(a, _) = &pair else {
                unreachable!()
            };
            let out = rt.block_on(Connection::begin_with_options(
                a,
                &cx,
                TransactionOptions::concurrent(),
            ));
            match out {
                Outcome::Err(Error::Transaction(t)) => {
                    assert_eq!(
                        t.kind,
                        TransactionErrorKind::UnsupportedMode,
                        "{}",
                        driver.name()
                    );
                }
                Outcome::Err(e) => {
                    panic!("{}: expected UnsupportedMode, got error {e}", driver.name())
                }
                Outcome::Ok(_) => panic!(
                    "{}: Concurrent mode was accepted but C SQLite has no such mode",
                    driver.name()
                ),
                Outcome::Cancelled(r) => panic!("{}: unexpected cancellation {r:?}", driver.name()),
                Outcome::Panicked(p) => panic!("{}: unexpected panic {p:?}", driver.name()),
            }
            exercised.push(format!("{} (refused Concurrent)", driver.name()));
            continue;
        }

        let (bodies, final_value) = match &pair {
            ConnectionPair::Franken(a, b) => run_pair(a, b, &table, &quoted),
            ConnectionPair::Postgres(a, b) => run_pair(a, b, &table, &quoted),
            ConnectionPair::MySql(a, b) => run_pair(a, b, &table, &quoted),
            ConnectionPair::CSqlite(..) => unreachable!("handled above"),
        };
        let expected = i64::from(INCREMENTS_PER_WRITER) * 2;
        assert_eq!(
            final_value,
            expected,
            "{}: every increment must land exactly once (bodies run: {bodies})",
            driver.name()
        );
        assert!(bodies >= INCREMENTS_PER_WRITER * 2, "{}", driver.name());
        eprintln!(
            "sqlmodel-e2e: {} final={final_value} bodies={bodies} retried={}",
            driver.name(),
            bodies - INCREMENTS_PER_WRITER * 2
        );
        exercised.push(driver.name().to_string());
        driver_cleanup(&driver);
    }

    assert!(
        exercised.iter().any(|e| e == "frankensqlite"),
        "FrankenSQLite concurrent writers must be exercised: {exercised:?}"
    );
    eprintln!("sqlmodel-e2e: concurrent writers exercised on {exercised:?}");
}

fn driver_cleanup(driver: &DriverUnderTest) {
    if let DriverUnderTest::Franken(p) | DriverUnderTest::CSqliteFile(p) = driver {
        sqlmodel_e2e::remove_db_family(p);
    }
}
