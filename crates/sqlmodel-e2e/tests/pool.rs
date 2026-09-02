//! `Pool` holding real driver connections (it had only ever held a mock).
//!
//! Every driver that can open more than one connection to the same database
//! gets the same script: fill the pool to `max_connections`, run a statement
//! on every lease, prove the next acquire times out, prove a release makes it
//! succeed again without creating a connection, cycle leases, then
//! `close_and_drain` and prove the pool refuses further acquires.

use std::future::Future;
use std::time::{Duration, Instant};

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};
use sqlmodel::prelude::*;
use sqlmodel::{Pool, PoolConfig};
use sqlmodel_core::error::PoolErrorKind;
use sqlmodel_e2e::{DriverUnderTest, expect_outcome};
use sqlmodel_frankensqlite::FrankenConnection;
use sqlmodel_mysql::SharedMySqlConnection;
use sqlmodel_postgres::SharedPgConnection;
use sqlmodel_sqlite::SqliteConnection;

const MAX: usize = 3;
const ACQUIRE_TIMEOUT_MS: u64 = 250;

fn pool_error_kind(e: &Error) -> Option<PoolErrorKind> {
    match e {
        Error::Pool(p) => Some(p.kind),
        _ => None,
    }
}

async fn exercise<C, F, Fut>(cx: &Cx, name: &str, factory: F)
where
    C: Connection + 'static,
    F: Fn() -> Fut,
    Fut: Future<Output = Outcome<C, Error>>,
{
    let pool: Pool<C> = Pool::new(
        PoolConfig::new(MAX)
            .min_connections(0)
            .acquire_timeout(ACQUIRE_TIMEOUT_MS)
            .test_on_checkout(true),
    );

    // Fill the pool and use every lease.
    let mut leases = Vec::new();
    for i in 0..MAX {
        let lease = expect_outcome(
            pool.acquire(cx, &factory).await,
            &format!("{name}: acquire {i}"),
        );
        let rows = expect_outcome(
            lease.query(cx, "SELECT 1", &[]).await,
            &format!("{name}: query through lease {i}"),
        );
        assert_eq!(rows.len(), 1, "{name}");
        leases.push(lease);
    }
    let stats = pool.stats();
    eprintln!("{name}: full {stats:?}");
    assert_eq!(stats.total_connections, MAX, "{name}: total at capacity");
    assert_eq!(stats.active_connections, MAX, "{name}: all active");
    assert_eq!(stats.idle_connections, 0, "{name}: none idle");
    assert!(pool.at_capacity(), "{name}");

    // One more acquire must time out, and only after the configured wait.
    let started = Instant::now();
    match pool.acquire(cx, &factory).await {
        Outcome::Err(e) => assert_eq!(
            pool_error_kind(&e),
            Some(PoolErrorKind::Timeout),
            "{name}: expected a pool timeout, got {e}"
        ),
        Outcome::Ok(_) => panic!("{name}: acquire beyond capacity must fail, but succeeded"),
        Outcome::Cancelled(r) => panic!("{name}: acquire beyond capacity was cancelled: {r:?}"),
        Outcome::Panicked(p) => panic!("{name}: acquire beyond capacity panicked: {p:?}"),
    }
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(ACQUIRE_TIMEOUT_MS - 50),
        "{name}: timed out after only {waited:?}"
    );
    assert_eq!(pool.stats().timeouts, 1, "{name}: timeout counted");

    // Releasing one lease makes the next acquire succeed by reuse, not creation.
    let created_before = pool.stats().connections_created;
    drop(leases.pop());
    let lease = expect_outcome(
        pool.acquire(cx, &factory).await,
        &format!("{name}: acquire after release"),
    );
    assert_eq!(
        pool.stats().connections_created,
        created_before,
        "{name}: a released connection is reused"
    );
    leases.push(lease);

    // Return everything; the connections stay pooled.
    leases.clear();
    let stats = pool.stats();
    eprintln!("{name}: released {stats:?}");
    assert_eq!(stats.active_connections, 0, "{name}");
    assert_eq!(
        stats.idle_connections + stats.active_connections,
        stats.total_connections,
        "{name}: accounting adds up"
    );

    // Churn: many sequential acquire/release cycles never create more than MAX.
    for i in 0..20 {
        let lease = expect_outcome(
            pool.acquire(cx, &factory).await,
            &format!("{name}: churn {i}"),
        );
        expect_outcome(
            lease.execute(cx, "SELECT 1", &[]).await,
            &format!("{name}: churn query {i}"),
        );
    }
    assert!(
        pool.stats().connections_created <= MAX as u64,
        "{name}: churn created {} connections",
        pool.stats().connections_created
    );

    // Close and drain: idle connections are closed, the pool refuses new work.
    expect_outcome(
        pool.close_and_drain(cx).await,
        &format!("{name}: close_and_drain"),
    );
    assert!(pool.is_closed(), "{name}");
    let stats = pool.stats();
    eprintln!("{name}: closed {stats:?}");
    assert_eq!(
        stats.total_connections, 0,
        "{name}: nothing left after drain"
    );
    match pool.acquire(cx, &factory).await {
        Outcome::Err(e) => assert_eq!(
            pool_error_kind(&e),
            Some(PoolErrorKind::Closed),
            "{name}: acquire after close must report Closed, got {e}"
        ),
        Outcome::Ok(_) => panic!("{name}: acquire after close must fail, but succeeded"),
        Outcome::Cancelled(r) => panic!("{name}: acquire after close was cancelled: {r:?}"),
        Outcome::Panicked(p) => panic!("{name}: acquire after close panicked: {p:?}"),
    }
}

#[test]
fn pool_holds_real_connections_on_every_multi_connection_driver() {
    let cx = Cx::for_testing();
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("asupersync runtime");
    let mut ran = Vec::new();
    for driver in DriverUnderTest::available_multi_connection() {
        let name = driver.name();
        eprintln!("sqlmodel-e2e: pool on {name}");
        match &driver {
            DriverUnderTest::CSqliteMemory => unreachable!("filtered out"),
            DriverUnderTest::CSqliteFile(path) => {
                let p = path.to_string_lossy().into_owned();
                rt.block_on(exercise(&cx, name, || {
                    let p = p.clone();
                    async move {
                        match SqliteConnection::open_file(p) {
                            Ok(c) => Outcome::Ok(c),
                            Err(e) => Outcome::Err(e),
                        }
                    }
                }));
            }
            DriverUnderTest::Franken(path) => {
                let p = path.to_string_lossy().into_owned();
                rt.block_on(exercise(&cx, name, || {
                    let p = p.clone();
                    async move {
                        match FrankenConnection::open_file(p) {
                            Ok(c) => Outcome::Ok(c),
                            Err(e) => Outcome::Err(e),
                        }
                    }
                }));
            }
            DriverUnderTest::Postgres(cfg) => {
                let cfg = cfg.clone();
                let cx2 = cx.clone();
                rt.block_on(exercise(&cx, name, || {
                    let cfg = cfg.clone();
                    let cx = cx2.clone();
                    async move { SharedPgConnection::connect(&cx, cfg).await }
                }));
            }
            DriverUnderTest::MySql(cfg) | DriverUnderTest::MariaDb(cfg) => {
                let cfg = cfg.clone();
                let cx2 = cx.clone();
                rt.block_on(exercise(&cx, name, || {
                    let cfg = cfg.clone();
                    let cx = cx2.clone();
                    async move { SharedMySqlConnection::connect(&cx, cfg).await }
                }));
            }
        }
        ran.push(name);
    }
    eprintln!("sqlmodel-e2e: pool ran on {}", ran.join(", "));
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
    assert!(ran.contains(&"c-sqlite(file)"), "{ran:?}");
}
