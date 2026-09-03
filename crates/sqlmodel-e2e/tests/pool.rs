//! `Pool` holding real driver connections (it had only ever held a mock).
//!
//! Every driver that can open more than one connection to the same database
//! gets the same script: fill the pool to `max_connections`, run a statement
//! on every lease, prove the next acquire times out, prove a release makes it
//! succeed again without creating a connection, cycle leases, then
//! `close_and_drain` and prove the pool refuses further acquires.
//!
//! A second script uses OS threads as real waiters: `max_lifetime` retirement,
//! waiters queued behind a full pool that are served in turn once leases
//! return (never by creating an extra connection), and `close_and_drain`
//! refusing queued waiters promptly while still waiting for active leases.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use asupersync::runtime::{Runtime, RuntimeBuilder};
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

/// Acquire a lease, run one statement through it, release it; returns the
/// row count so a waiter thread can report success without holding a lease.
fn acquire_and_query<C, F, Fut>(pool: &Pool<C>, factory: &F) -> (Duration, Outcome<usize, Error>)
where
    C: Connection + 'static,
    F: Fn() -> Fut + Sync,
    Fut: Future<Output = Outcome<C, Error>> + Send,
{
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("asupersync runtime");
    let cx = Cx::for_testing();
    let started = Instant::now();
    let outcome = rt.block_on(async {
        match pool.acquire(&cx, factory).await {
            Outcome::Ok(lease) => match lease.query(&cx, "SELECT 1", &[]).await {
                Outcome::Ok(rows) => Outcome::Ok(rows.len()),
                Outcome::Err(e) => Outcome::Err(e),
                Outcome::Cancelled(r) => Outcome::Cancelled(r),
                Outcome::Panicked(p) => Outcome::Panicked(p),
            },
            Outcome::Err(e) => Outcome::Err(e),
            Outcome::Cancelled(r) => Outcome::Cancelled(r),
            Outcome::Panicked(p) => Outcome::Panicked(p),
        }
    });
    (started.elapsed(), outcome)
}

/// Lifetime retirement, real waiters behind a full pool, and `close_and_drain`
/// with waiters queued and leases still active.
fn exercise_contention<C, F, Fut>(rt: &Runtime, cx: &Cx, name: &str, factory: &F)
where
    C: Connection + 'static,
    F: Fn() -> Fut + Sync,
    Fut: Future<Output = Outcome<C, Error>> + Send,
{
    // max_lifetime: a connection older than the limit is replaced on the next
    // acquire instead of being handed out again.
    let short: Pool<C> = Pool::new(
        PoolConfig::new(1)
            .min_connections(0)
            .acquire_timeout(ACQUIRE_TIMEOUT_MS)
            .max_lifetime(1),
    );
    rt.block_on(async {
        drop(expect_outcome(
            short.acquire(cx, factory).await,
            &format!("{name}: acquire (lifetime)"),
        ));
        std::thread::sleep(Duration::from_millis(5));
        let lease = expect_outcome(
            short.acquire(cx, factory).await,
            &format!("{name}: acquire after lifetime expiry"),
        );
        expect_outcome(
            lease.query(cx, "SELECT 1", &[]).await,
            &format!("{name}: query on replacement"),
        );
        drop(lease);
        let stats = short.stats();
        eprintln!("{name}: lifetime {stats:?}");
        assert_eq!(
            stats.connections_created, 2,
            "{name}: the expired connection must be replaced, not reused"
        );
        assert!(stats.connections_closed >= 1, "{name}: expired one closed");
        // With a 1 ms lifetime the replacement may itself age out during its
        // query and be retired on return; either way the pool never exceeds max.
        assert!(stats.total_connections <= 1, "{name}: never more than max");
        expect_outcome(
            short.close_and_drain(cx).await,
            &format!("{name}: close lifetime pool"),
        );
    });

    // Waiters: MAX leases held here while two threads queue; they are served
    // after the release, in turn, without a fourth connection.
    let pool: Arc<Pool<C>> = Arc::new(Pool::new(
        PoolConfig::new(MAX)
            .min_connections(0)
            .acquire_timeout(2_000),
    ));
    let mut leases = Vec::new();
    rt.block_on(async {
        for i in 0..MAX {
            leases.push(expect_outcome(
                pool.acquire(cx, factory).await,
                &format!("{name}: fill {i}"),
            ));
        }
    });
    std::thread::scope(|scope| {
        let waiters: Vec<_> = (0..2)
            .map(|_| {
                let pool = Arc::clone(&pool);
                scope.spawn(move || acquire_and_query(&pool, factory))
            })
            .collect();
        std::thread::sleep(Duration::from_millis(150));
        let stats = pool.stats();
        eprintln!("{name}: waiters queued {stats:?}");
        assert_eq!(stats.pending_requests, 2, "{name}: both waiters queued");
        assert_eq!(stats.active_connections, MAX, "{name}");
        leases.clear();
        for w in waiters {
            let (elapsed, outcome) = w.join().expect("waiter thread");
            let rows = expect_outcome(outcome, &format!("{name}: waiter acquire"));
            assert_eq!(rows, 1, "{name}");
            assert!(
                elapsed >= Duration::from_millis(100),
                "{name}: a waiter got a lease before any was released ({elapsed:?})"
            );
        }
    });
    let stats = pool.stats();
    eprintln!("{name}: after waiters {stats:?}");
    assert_eq!(stats.pending_requests, 0, "{name}");
    assert!(
        stats.connections_created <= MAX as u64,
        "{name}: waiters were served by reuse, created {}",
        stats.connections_created
    );
    assert_eq!(stats.timeouts, 0, "{name}: nobody timed out");

    // close_and_drain with waiters queued and leases active: waiters are
    // refused promptly with Closed; the drain itself waits for the leases.
    rt.block_on(async {
        for i in 0..MAX {
            leases.push(expect_outcome(
                pool.acquire(cx, factory).await,
                &format!("{name}: refill {i}"),
            ));
        }
    });
    std::thread::scope(|scope| {
        let waiters: Vec<_> = (0..2)
            .map(|_| {
                let pool = Arc::clone(&pool);
                scope.spawn(move || acquire_and_query(&pool, factory))
            })
            .collect();
        std::thread::sleep(Duration::from_millis(100));
        let holder = scope.spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            drop(leases);
        });
        let drain_started = Instant::now();
        rt.block_on(async {
            expect_outcome(
                pool.close_and_drain(cx).await,
                &format!("{name}: close_and_drain with waiters"),
            );
        });
        let drained_after = drain_started.elapsed();
        holder.join().expect("holder thread");
        assert!(
            drained_after >= Duration::from_millis(200),
            "{name}: drain returned before the active leases did ({drained_after:?})"
        );
        for w in waiters {
            let (elapsed, outcome) = w.join().expect("waiter thread");
            match outcome {
                Outcome::Err(e) => assert_eq!(
                    pool_error_kind(&e),
                    Some(PoolErrorKind::Closed),
                    "{name}: queued waiter must see Closed, got {e}"
                ),
                Outcome::Ok(_) => panic!("{name}: a queued waiter got a lease from a closing pool"),
                Outcome::Cancelled(r) => panic!("{name}: waiter cancelled: {r:?}"),
                Outcome::Panicked(p) => panic!("{name}: waiter panicked: {p:?}"),
            }
            assert!(
                elapsed < Duration::from_millis(1_500),
                "{name}: waiter was not refused promptly ({elapsed:?}); it waited for its own timeout"
            );
        }
    });
    assert!(pool.is_closed(), "{name}");
    let stats = pool.stats();
    eprintln!("{name}: drained {stats:?}");
    assert_eq!(stats.total_connections, 0, "{name}: everything retired");
}

/// A server session id as reported by `SELECT pg_backend_pid()` /
/// `SELECT CONNECTION_ID()` (MySQL reports the latter as an unsigned value).
async fn session_id<C: Connection>(cx: &Cx, conn: &C, sql: &str, label: &str) -> i64 {
    let rows = expect_outcome(conn.query(cx, sql, &[]).await, label);
    let value = rows[0]
        .get(0)
        .unwrap_or_else(|| panic!("{label}: no session id"));
    match value {
        Value::Text(s) | Value::Decimal(s) => s.parse().unwrap_or_else(|_| panic!("{label}: {s}")),
        other => other
            .as_i64()
            .unwrap_or_else(|| panic!("{label}: not an integer: {other:?}")),
    }
}

/// Server-side proof for the network drivers, observed through backend ids:
/// lifetime retirement opens a different server session; an idle connection
/// killed on the server is replaced transparently when checkout validation
/// is on; without it the dead lease fails its first statement and must be
/// detached rather than returned (the rule the pool documents).
fn exercise_server_sessions<C, F, Fut>(
    rt: &Runtime,
    cx: &Cx,
    name: &str,
    factory: &F,
    session_sql: &str,
    kill_sql: impl Fn(i64) -> String,
) where
    C: Connection + 'static,
    F: Fn() -> Fut + Sync,
    Fut: Future<Output = Outcome<C, Error>> + Send,
{
    rt.block_on(async {
        let control = expect_outcome(factory().await, &format!("{name}: control connection"));

        // 1. Retirement is a new server session, not the same one again.
        let short: Pool<C> = Pool::new(
            PoolConfig::new(1)
                .min_connections(0)
                .acquire_timeout(ACQUIRE_TIMEOUT_MS)
                .max_lifetime(1),
        );
        let first = {
            let lease = expect_outcome(
                short.acquire(cx, factory).await,
                &format!("{name}: acquire (session 1)"),
            );
            session_id(cx, &*lease, session_sql, &format!("{name}: session 1")).await
        };
        std::thread::sleep(Duration::from_millis(5));
        let second = {
            let lease = expect_outcome(
                short.acquire(cx, factory).await,
                &format!("{name}: acquire (session 2)"),
            );
            session_id(cx, &*lease, session_sql, &format!("{name}: session 2")).await
        };
        eprintln!("{name}: lifetime retirement moved session {first} -> {second}");
        assert_ne!(
            first, second,
            "{name}: retirement must open a new server session"
        );
        expect_outcome(
            short.close_and_drain(cx).await,
            &format!("{name}: close retirement pool"),
        );

        // 2. With checkout validation, a killed idle connection is replaced
        //    transparently: the next acquire succeeds on a new session.
        let pool: Pool<C> = Pool::new(
            PoolConfig::new(1)
                .min_connections(0)
                .acquire_timeout(2_000)
                .test_on_checkout(true),
        );
        let doomed = {
            let lease = expect_outcome(
                pool.acquire(cx, factory).await,
                &format!("{name}: acquire (doomed)"),
            );
            session_id(cx, &*lease, session_sql, &format!("{name}: doomed session")).await
        };
        expect_outcome(
            control.query(cx, &kill_sql(doomed), &[]).await,
            &format!("{name}: kill session {doomed}"),
        );
        std::thread::sleep(Duration::from_millis(200));
        let closed_before = pool.stats().connections_closed;
        let lease = expect_outcome(
            pool.acquire(cx, factory).await,
            &format!("{name}: acquire after the server killed the idle connection"),
        );
        let replacement = session_id(
            cx,
            &*lease,
            session_sql,
            &format!("{name}: replacement session"),
        )
        .await;
        eprintln!(
            "{name}: killed session {doomed}; checkout validation replaced it with {replacement}"
        );
        assert_ne!(replacement, doomed, "{name}: a new server session");
        expect_outcome(
            lease.query(cx, "SELECT 1", &[]).await,
            &format!("{name}: query on the replacement"),
        );
        drop(lease);
        let stats = pool.stats();
        eprintln!("{name}: after replacement {stats:?}");
        assert_eq!(
            stats.connections_closed,
            closed_before + 1,
            "{name}: the dead connection was closed"
        );
        assert_eq!(
            stats.connections_created, 2,
            "{name}: exactly one reconnect"
        );
        assert!(stats.total_connections <= 1, "{name}: never more than max");
        expect_outcome(
            pool.close_and_drain(cx).await,
            &format!("{name}: close validation pool"),
        );

        // 3. Without checkout validation the dead connection is handed out
        //    again and its first statement fails; detaching it (instead of
        //    dropping it back) makes the next acquire open a fresh session.
        let pool: Pool<C> = Pool::new(
            PoolConfig::new(1)
                .min_connections(0)
                .acquire_timeout(2_000)
                .test_on_checkout(false),
        );
        let doomed = {
            let lease = expect_outcome(
                pool.acquire(cx, factory).await,
                &format!("{name}: acquire (doomed, no validation)"),
            );
            session_id(
                cx,
                &*lease,
                session_sql,
                &format!("{name}: doomed session 2"),
            )
            .await
        };
        expect_outcome(
            control.query(cx, &kill_sql(doomed), &[]).await,
            &format!("{name}: kill session {doomed} (no validation)"),
        );
        std::thread::sleep(Duration::from_millis(200));
        let lease = expect_outcome(
            pool.acquire(cx, factory).await,
            &format!("{name}: acquire the dead connection (no validation)"),
        );
        assert_eq!(
            pool.stats().connections_created,
            1,
            "{name}: without validation the pool cannot know the connection is dead"
        );
        let error = match lease.query(cx, "SELECT 1", &[]).await {
            Outcome::Err(e) => e,
            other => panic!("{name}: a dead lease must fail its first statement, got {other:?}"),
        };
        eprintln!("{name}: dead lease failed with: {error}");
        assert!(
            matches!(error, Error::Connection(_)),
            "{name}: the driver's connection error, not a pool error: {error}"
        );
        drop(lease.detach());
        let stats = pool.stats();
        assert_eq!(
            stats.total_connections, 0,
            "{name}: detached connection left the pool"
        );
        assert_eq!(stats.active_connections, 0, "{name}");
        let lease = expect_outcome(
            pool.acquire(cx, factory).await,
            &format!("{name}: acquire after detaching the dead lease"),
        );
        let fresh = session_id(cx, &*lease, session_sql, &format!("{name}: fresh session")).await;
        assert_ne!(fresh, doomed, "{name}: a new server session after detach");
        expect_outcome(
            lease.query(cx, "SELECT 1", &[]).await,
            &format!("{name}: query on the fresh session"),
        );
        drop(lease);
        assert_eq!(pool.stats().connections_created, 2, "{name}");
        expect_outcome(
            pool.close_and_drain(cx).await,
            &format!("{name}: close no-validation pool"),
        );
        drop(control);
    });
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
                let factory = || {
                    let p = p.clone();
                    async move {
                        match SqliteConnection::open_file(p) {
                            Ok(c) => Outcome::Ok(c),
                            Err(e) => Outcome::Err(e),
                        }
                    }
                };
                rt.block_on(exercise(&cx, name, &factory));
                exercise_contention(&rt, &cx, name, &factory);
            }
            DriverUnderTest::Franken(path) => {
                let p = path.to_string_lossy().into_owned();
                let factory = || {
                    let p = p.clone();
                    async move {
                        match FrankenConnection::open_file(p) {
                            Ok(c) => Outcome::Ok(c),
                            Err(e) => Outcome::Err(e),
                        }
                    }
                };
                rt.block_on(exercise(&cx, name, &factory));
                exercise_contention(&rt, &cx, name, &factory);
            }
            DriverUnderTest::Postgres(cfg) => {
                let cfg = cfg.clone();
                let cx2 = cx.clone();
                let factory = || {
                    let cfg = cfg.clone();
                    let cx = cx2.clone();
                    async move { SharedPgConnection::connect(&cx, cfg).await }
                };
                rt.block_on(exercise(&cx, name, &factory));
                exercise_contention(&rt, &cx, name, &factory);
                exercise_server_sessions(
                    &rt,
                    &cx,
                    name,
                    &factory,
                    "SELECT pg_backend_pid()",
                    |id| format!("SELECT pg_terminate_backend({id})"),
                );
            }
            DriverUnderTest::MySql(cfg) | DriverUnderTest::MariaDb(cfg) => {
                let cfg = cfg.clone();
                let cx2 = cx.clone();
                let factory = || {
                    let cfg = cfg.clone();
                    let cx = cx2.clone();
                    async move { SharedMySqlConnection::connect(&cx, cfg).await }
                };
                rt.block_on(exercise(&cx, name, &factory));
                exercise_contention(&rt, &cx, name, &factory);
                exercise_server_sessions(
                    &rt,
                    &cx,
                    name,
                    &factory,
                    "SELECT CONNECTION_ID()",
                    |id| format!("KILL CONNECTION {id}"),
                );
            }
        }
        ran.push(name);
    }
    eprintln!("sqlmodel-e2e: pool ran on {}", ran.join(", "));
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
    assert!(ran.contains(&"c-sqlite(file)"), "{ran:?}");
}
