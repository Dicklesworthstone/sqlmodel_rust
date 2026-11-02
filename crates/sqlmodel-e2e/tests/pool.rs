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

// The all-driver fan-out witness nests the cancellation-aware acquisition
// future inside spawned tasks. Allow the compiler to prove its Send bounds
// without suppressing the recursion-depth warning or changing the witness.
#![recursion_limit = "256"]

use std::future::Future;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use asupersync::runtime::{Runtime, RuntimeBuilder};
use asupersync::{Budget, Cx, Outcome};
use sqlmodel::prelude::*;
use sqlmodel::{Pool, PoolConfig};
use sqlmodel_core::error::PoolErrorKind;
use sqlmodel_e2e::{DriverUnderTest, expect_outcome, parse_postgres_url};
use sqlmodel_frankensqlite::FrankenConnection;
use sqlmodel_mysql::SharedMySqlConnection;
use sqlmodel_postgres::{PgConfig, SharedPgConnection};
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

/// The single integer a scalar query returns (MySQL reports unsigned and
/// COUNT values as text/decimal on the text protocol).
async fn scalar_i64<C: Connection>(
    cx: &Cx,
    conn: &C,
    sql: &str,
    params: &[Value],
    label: &str,
) -> i64 {
    let rows = expect_outcome(conn.query(cx, sql, params).await, label);
    let value = rows[0]
        .get(0)
        .unwrap_or_else(|| panic!("{label}: no value"));
    match value {
        Value::Text(s) | Value::Decimal(s) => s.parse().unwrap_or_else(|_| panic!("{label}: {s}")),
        other => other
            .as_i64()
            .unwrap_or_else(|| panic!("{label}: not an integer: {other:?}")),
    }
}

/// A server session id as reported by `SELECT pg_backend_pid()` /
/// `SELECT CONNECTION_ID()`.
async fn session_id<C: Connection>(cx: &Cx, conn: &C, sql: &str, label: &str) -> i64 {
    scalar_i64(cx, conn, sql, &[], label).await
}

/// How a control connection counts this pool's sessions on the server.
struct ServerCount<C> {
    control: C,
    sql: String,
    params: Vec<Value>,
    /// Sessions the count includes besides the pool's (the control
    /// connection itself when it cannot be told apart).
    extra: usize,
}

/// 4 x MAX acquirers as tasks under one asupersync runtime (the pool's
/// intended use), every one served without a timeout and without a
/// connection beyond MAX; while MAX leases are held the server itself
/// reports exactly MAX sessions for the pool.
fn exercise_fan_out<C, F, Fut>(
    rt: &Runtime,
    cx: &Cx,
    name: &str,
    factory: F,
    server_count: Option<ServerCount<C>>,
) where
    C: Connection + 'static,
    F: Fn() -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Outcome<C, Error>> + Send + 'static,
{
    let pool: Arc<Pool<C>> = Arc::new(Pool::new(
        PoolConfig::new(MAX)
            .min_connections(0)
            .acquire_timeout(10_000),
    ));

    // Server-side count while every lease is out.
    rt.block_on(async {
        let mut leases = Vec::new();
        for i in 0..MAX {
            leases.push(expect_outcome(
                pool.acquire(cx, &factory).await,
                &format!("{name}: fan-out fill {i}"),
            ));
        }
        if let Some(count) = &server_count {
            let seen = scalar_i64(
                cx,
                &count.control,
                &count.sql,
                &count.params,
                &format!("{name}: server-side session count"),
            )
            .await;
            eprintln!(
                "{name}: server reports {seen} sessions with {MAX} leases held (+{} control)",
                count.extra
            );
            assert_eq!(
                seen,
                i64::try_from(MAX + count.extra).expect("small"),
                "{name}: the server must see exactly max connections for the pool"
            );
        } else {
            eprintln!(
                "{name}: no server sessions to count; the pool's own stats are the only observable"
            );
        }
        drop(leases);
    });

    // 4 x MAX tasks, each acquiring, holding the lease across two statements,
    // releasing. Structured under one runtime: no OS threads involved.
    let handle = rt.handle();
    let tasks: Vec<_> = (0..4 * MAX)
        .map(|i| {
            let pool = Arc::clone(&pool);
            let factory = factory.clone();
            handle.spawn(async move {
                let cx = Cx::for_testing();
                let lease = match pool.acquire(&cx, &factory).await {
                    Outcome::Ok(lease) => lease,
                    Outcome::Err(e) => return Err(format!("task {i}: acquire: {e}")),
                    Outcome::Cancelled(r) => {
                        return Err(format!("task {i}: acquire cancelled: {r:?}"));
                    }
                    Outcome::Panicked(p) => {
                        return Err(format!("task {i}: acquire panicked: {p:?}"));
                    }
                };
                for sql in ["SELECT 1", "SELECT 2"] {
                    match lease.query(&cx, sql, &[]).await {
                        Outcome::Ok(rows) if rows.len() == 1 => {}
                        other => return Err(format!("task {i}: {sql}: {other:?}")),
                    }
                }
                Ok(())
            })
        })
        .collect();
    let results = rt.block_on(async {
        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            results.push(task.await);
        }
        results
    });
    // (`Result::err` would resolve to the prelude's `sqlmodel::Result` alias.)
    let failures: Vec<String> = results.into_iter().filter_map(|r| r.err()).collect();
    assert!(
        failures.is_empty(),
        "{name}: fan-out failures: {failures:#?}"
    );
    let stats = pool.stats();
    eprintln!("{name}: fan-out {stats:?}");
    assert_eq!(
        stats.acquires,
        (5 * MAX) as u64,
        "{name}: every acquire counted"
    );
    assert_eq!(stats.timeouts, 0, "{name}: no task timed out");
    assert!(
        stats.connections_created <= MAX as u64,
        "{name}: fan-out created {} connections",
        stats.connections_created
    );
    assert_eq!(stats.active_connections, 0, "{name}: every lease returned");
    rt.block_on(async {
        expect_outcome(
            pool.close_and_drain(cx).await,
            &format!("{name}: close fan-out pool"),
        );
        if let Some(count) = server_count {
            drop(count.control);
        }
    });
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

/// A lease holder that panics returns its connection during unwinding: the
/// pool keeps serving with consistent accounting and still drains.
fn exercise_panicking_holder<C, F, Fut>(rt: &Runtime, cx: &Cx, name: &str, factory: &F)
where
    C: Connection + 'static,
    F: Fn() -> Fut + Sync,
    Fut: Future<Output = Outcome<C, Error>> + Send,
{
    let pool: Arc<Pool<C>> = Arc::new(Pool::new(
        PoolConfig::new(MAX)
            .min_connections(0)
            .acquire_timeout(2_000),
    ));
    let joined = std::thread::scope(|scope| {
        let pool = Arc::clone(&pool);
        scope
            .spawn(move || {
                let rt = RuntimeBuilder::current_thread()
                    .build()
                    .expect("asupersync runtime");
                let cx = Cx::for_testing();
                rt.block_on(async {
                    let lease = expect_outcome(
                        pool.acquire(&cx, factory).await,
                        "panicking holder: acquire",
                    );
                    expect_outcome(
                        lease.query(&cx, "SELECT 1", &[]).await,
                        "panicking holder: query",
                    );
                    assert_eq!(pool.stats().active_connections, 1);
                    panic!("lease holder panics on purpose (expected in this test)");
                });
            })
            .join()
    });
    assert!(
        joined.is_err(),
        "{name}: the holder thread must have panicked"
    );
    let stats = pool.stats();
    eprintln!("{name}: after a panicking holder {stats:?}");
    assert_eq!(
        stats.active_connections, 0,
        "{name}: the lease was returned during unwinding"
    );
    assert_eq!(stats.total_connections, 1, "{name}: still pooled");
    assert!(stats.total_connections <= MAX, "{name}");
    rt.block_on(async {
        let lease = expect_outcome(
            pool.acquire(cx, factory).await,
            &format!("{name}: acquire after the panic"),
        );
        let rows = expect_outcome(
            lease.query(cx, "SELECT 1", &[]).await,
            &format!("{name}: query after the panic"),
        );
        assert_eq!(rows.len(), 1, "{name}");
        drop(lease);
        assert_eq!(
            pool.stats().connections_created,
            1,
            "{name}: the returned connection was reused, not replaced"
        );
        expect_outcome(
            pool.close_and_drain(cx).await,
            &format!("{name}: close_and_drain after the panic"),
        );
    });
    assert_eq!(pool.stats().total_connections, 0, "{name}: drained");
}

/// Production-context witnesses for GH#43. These deliberately do not use
/// `Cx::for_testing`: each connect, checkout and query belongs to the runtime
/// that actually polls it. The original all-driver success contract below is
/// retained, including its current-thread handoff after a holder panic.
#[derive(Clone, Copy, Debug)]
enum HandoffRuntime {
    CurrentThread,
    TwoWorkers,
}

impl HandoffRuntime {
    fn build(self) -> Runtime {
        match self {
            Self::CurrentThread => RuntimeBuilder::current_thread(),
            Self::TwoWorkers => RuntimeBuilder::new().worker_threads(2),
        }
        .build()
        .expect("production handoff runtime")
    }
}

/// An independent wall-clock watchdog catches a regression even if the
/// runtime's own timer stops progressing. Completion cancels and joins it;
/// no detached sleeping watchdog survives a successful case.
struct HandoffWatchdog {
    done: mpsc::Sender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl HandoffWatchdog {
    fn start(label: String) -> Self {
        let (done, completion) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            if completion.recv_timeout(Duration::from_secs(30))
                == Err(mpsc::RecvTimeoutError::Timeout)
            {
                eprintln!("{label}: production pool regression exceeded the 30-second watchdog");
                std::process::abort();
            }
        });
        Self {
            done,
            thread: Some(thread),
        }
    }
}

impl Drop for HandoffWatchdog {
    fn drop(&mut self) {
        let _ = self.done.send(());
        if let Some(thread) = self.thread.take() {
            thread.join().expect("handoff watchdog");
        }
    }
}

fn postgres_handoff_config() -> Option<PgConfig> {
    match std::env::var("SQLMODEL_TEST_POSTGRES_URL") {
        Ok(url) => Some(parse_postgres_url(&url).expect("valid SQLMODEL_TEST_POSTGRES_URL")),
        Err(std::env::VarError::NotPresent) => {
            eprintln!(
                "skipping Postgres integration tests (production pool handoff): set SQLMODEL_TEST_POSTGRES_URL"
            );
            assert_ne!(
                std::env::var("SQLMODEL_TEST_REQUIRE_SERVICES").as_deref(),
                Ok("1"),
                "PostgreSQL production pool witnesses require SQLMODEL_TEST_POSTGRES_URL"
            );
            None
        }
        Err(error) => panic!("invalid SQLMODEL_TEST_POSTGRES_URL: {error}"),
    }
}

/// A timeout proves bounded acquisition and retirement, never successful
/// handoff. On Asupersync 0.5.0 the current-thread cases take that branch;
/// a runtime carrying the upstream fix must reuse the original backend.
fn exercise_postgres_production_handoff(
    cfg: &PgConfig,
    flavor: HandoffRuntime,
    panic_holder: bool,
) {
    const TIMEOUT_MS: u64 = 2_000;
    const HOLDER_PANIC: &str = "production pool holder panics intentionally";

    let label = format!("postgres production {flavor:?}, panic_holder={panic_holder}");
    let _watchdog = HandoffWatchdog::start(label.clone());
    let pool: Arc<Pool<SharedPgConnection>> = Arc::new(Pool::new(
        PoolConfig::new(MAX)
            .min_connections(0)
            .acquire_timeout(TIMEOUT_MS),
    ));
    let (backend_tx, backend_rx) = mpsc::channel();
    let holder_pool = Arc::clone(&pool);
    let holder_cfg = cfg.clone();
    let holder = std::thread::spawn(move || {
        let rt = flavor.build();
        let cx = rt.request_cx_with_budget(Budget::INFINITE);
        rt.block_on(async {
            let lease = expect_outcome(
                holder_pool
                    .acquire(&cx, || SharedPgConnection::connect(&cx, holder_cfg.clone()))
                    .await,
                "production holder: acquire",
            );
            let backend =
                session_id(&cx, &*lease, "SELECT pg_backend_pid()", "holder backend").await;
            // Force the read to park under the holder's reactor before it
            // exits; an already-readable socket must not hide a handoff bug.
            let rows = expect_outcome(
                lease.query(&cx, "SELECT 1 FROM pg_sleep(0.02)", &[]).await,
                "holder SELECT 1 with a readiness-registration witness",
            );
            assert_eq!(rows[0].get_as::<i64>(0).expect("SELECT 1"), 1);
            backend_tx.send(backend).expect("report holder backend");
            assert_eq!(holder_pool.stats().active_connections, 1);
            if panic_holder {
                std::panic::panic_any(HOLDER_PANIC);
            }
        });
    });
    let joined = holder.join();
    if panic_holder {
        let payload = joined.expect_err("holder must panic intentionally");
        assert_eq!(
            payload.downcast_ref::<&str>().copied(),
            Some(HOLDER_PANIC),
            "{label}: an unrelated holder failure is not the expected panic"
        );
    } else {
        joined.expect("holder must return normally");
    }
    let original_backend = backend_rx.recv().expect("holder reached its query");
    let returned = pool.stats();
    eprintln!("{label}: holder joined, runtime dropped; {returned:?}");
    assert_eq!(returned.total_connections, 1, "{label}");
    assert_eq!(returned.idle_connections, 1, "{label}");
    assert_eq!(returned.active_connections, 0, "{label}");
    assert_eq!(returned.pending_requests, 0, "{label}");
    assert_eq!(returned.connections_created, 1, "{label}");
    assert_eq!(returned.connections_closed, 0, "{label}");

    let rt = flavor.build();
    let cx = rt.request_cx_with_budget(Budget::INFINITE);
    rt.block_on(async {
        let started = Instant::now();
        let acquired = pool
            .acquire(&cx, || SharedPgConnection::connect(&cx, cfg.clone()))
            .await;
        let waited = started.elapsed();
        assert!(
            waited < Duration::from_secs(5),
            "{label}: acquire exceeded its {TIMEOUT_MS} ms deadline: {waited:?}"
        );
        match acquired {
            Outcome::Ok(lease) => {
                let backend = session_id(&cx, &*lease, "SELECT pg_backend_pid()", "reused backend")
                    .await;
                assert_eq!(backend, original_backend, "{label}: same physical connection");
                let rows = expect_outcome(
                    lease.query(&cx, "SELECT 1", &[]).await,
                    "production consumer SELECT 1",
                );
                assert_eq!(rows[0].get_as::<i64>(0).expect("SELECT 1"), 1);
                drop(lease);
                let stats = pool.stats();
                assert_eq!(stats.connections_created, 1, "{label}: actual reuse");
                assert_eq!(stats.connections_closed, 0, "{label}");
                assert_eq!(stats.acquires, 2, "{label}");
                assert_eq!(stats.timeouts, 0, "{label}");
                eprintln!("{label}: SUCCESSFUL REUSE of backend {backend} after {waited:?}");
            }
            Outcome::Err(error) if matches!(flavor, HandoffRuntime::CurrentThread) => {
                assert_eq!(pool_error_kind(&error), Some(PoolErrorKind::Timeout), "{label}: {error}");
                assert!(waited >= Duration::from_millis(TIMEOUT_MS), "{label}: early timeout");
                let stats = pool.stats();
                assert_eq!(stats.total_connections, 0, "{label}: failed validation retired");
                assert_eq!(stats.idle_connections, 0, "{label}: no unusable idle connection");
                assert_eq!(stats.active_connections, 0, "{label}: no leaked lease");
                assert_eq!(stats.pending_requests, 0, "{label}: no leaked waiter");
                assert_eq!(stats.connections_created, 1, "{label}");
                assert_eq!(stats.connections_closed, 1, "{label}: retired exactly once");
                assert_eq!(stats.acquires, 1, "{label}: failed checkout is not an acquire");
                assert_eq!(stats.timeouts, 1, "{label}: timeout counted exactly once");
                eprintln!(
                    "{label}: BOUNDED FAILURE after {waited:?}; backend {original_backend} retired. This is not successful runtime handoff (GH#43 / asupersync#85)."
                );

                let fresh = expect_outcome(
                    pool.acquire(&cx, || SharedPgConnection::connect(&cx, cfg.clone())).await,
                    "acquire after timed-out validation",
                );
                let backend = session_id(&cx, &*fresh, "SELECT pg_backend_pid()", "replacement backend").await;
                assert_ne!(backend, original_backend, "{label}: stale socket was not returned");
                let rows = expect_outcome(fresh.query(&cx, "SELECT 1", &[]).await, "replacement SELECT 1");
                assert_eq!(rows[0].get_as::<i64>(0).expect("SELECT 1"), 1);
                assert_eq!(pool.stats().connections_created, 2, "{label}");

                // The server must observe the retired socket closing too. A
                // fresh query snapshot is taken on every iteration; the outer
                // timeout is only a test watchdog, not the acquisition bound.
                asupersync::time::timeout(
                    asupersync::time::wall_now(),
                    Duration::from_secs(5),
                    async {
                        loop {
                            let live = scalar_i64(
                                &cx,
                                &*fresh,
                                "SELECT count(*) FROM pg_stat_activity WHERE pid = $1::bigint",
                                &[Value::BigInt(original_backend)],
                                "retired backend count",
                            ).await;
                            if live == 0 {
                                break;
                            }
                            asupersync::runtime::yield_now::yield_now().await;
                        }
                    },
                ).await.expect("retired connection must disappear from pg_stat_activity");
                drop(fresh);
            }
            other => panic!("{label}: successful handoff required, got {other:?}"),
        }
        expect_outcome(pool.close_and_drain(&cx).await, "production handoff drain");
        let drained = pool.stats();
        assert_eq!(drained.total_connections, 0, "{label}");
        assert_eq!(drained.active_connections, 0, "{label}");
        assert_eq!(drained.idle_connections, 0, "{label}");
        assert_eq!(drained.pending_requests, 0, "{label}");
        assert_eq!(drained.connections_created, drained.connections_closed, "{label}");
    });
}

#[test]
fn postgres_current_thread_handoff_has_bounded_acquisition_and_retirement() {
    let Some(cfg) = postgres_handoff_config() else {
        return;
    };
    for panic_holder in [false, true] {
        exercise_postgres_production_handoff(&cfg, HandoffRuntime::CurrentThread, panic_holder);
    }
}

#[test]
fn postgres_two_worker_handoff_successfully_reuses_connection_after_return_and_panic() {
    let Some(cfg) = postgres_handoff_config() else {
        return;
    };
    for panic_holder in [false, true] {
        exercise_postgres_production_handoff(&cfg, HandoffRuntime::TwoWorkers, panic_holder);
    }
}

#[test]
fn postgres_current_thread_same_runtime_successfully_reuses_connection() {
    let Some(cfg) = postgres_handoff_config() else {
        return;
    };
    let _watchdog = HandoffWatchdog::start("postgres same-runtime reuse".into());
    let rt = HandoffRuntime::CurrentThread.build();
    let cx = rt.request_cx_with_budget(Budget::INFINITE);
    let pool: Pool<SharedPgConnection> =
        Pool::new(PoolConfig::new(1).min_connections(0).acquire_timeout(2_000));
    rt.block_on(async {
        let mut original_backend = None;
        for _ in 0..3 {
            let lease = expect_outcome(
                pool.acquire(&cx, || SharedPgConnection::connect(&cx, cfg.clone()))
                    .await,
                "same-runtime acquire",
            );
            let backend = session_id(
                &cx,
                &*lease,
                "SELECT pg_backend_pid()",
                "same-runtime backend",
            )
            .await;
            assert_eq!(*original_backend.get_or_insert(backend), backend);
            let rows = expect_outcome(
                lease.query(&cx, "SELECT 1", &[]).await,
                "same-runtime SELECT 1",
            );
            assert_eq!(rows[0].get_as::<i64>(0).expect("SELECT 1"), 1);
        }
        let stats = pool.stats();
        assert_eq!(stats.connections_created, 1);
        assert_eq!(stats.acquires, 3);
        assert_eq!(stats.timeouts, 0);
        expect_outcome(pool.close_and_drain(&cx).await, "same-runtime drain");
        assert_eq!(pool.stats().total_connections, 0);
        assert_eq!(pool.stats().connections_closed, 1);
    });
}

#[test]
fn postgres_production_checkout_retires_expired_and_server_terminated_connections() {
    let Some(cfg) = postgres_handoff_config() else {
        return;
    };
    let _watchdog = HandoffWatchdog::start("postgres production retirement and replacement".into());
    let rt = HandoffRuntime::CurrentThread.build();
    let cx = rt.request_cx_with_budget(Budget::INFINITE);
    let factory = || SharedPgConnection::connect(&cx, cfg.clone());

    // Run independently of the all-driver handoff contract: Asupersync 0.5's
    // known current-thread handoff failure must not prevent the existing
    // server-side retirement and replacement assertions from being reached.
    exercise_server_sessions(
        &rt,
        &cx,
        "postgres production retirement",
        &factory,
        "SELECT pg_backend_pid()",
        |id| format!("SELECT pg_terminate_backend({id})"),
    );
}

/// A cancelled `close_and_drain` reports `Cancelled` but leaves the pool
/// closed: acquires are refused, the active lease still returns, and a
/// later drain completes.
fn exercise_cancelled_drain<C, F, Fut>(rt: &Runtime, cx: &Cx, name: &str, factory: &F)
where
    C: Connection + 'static,
    F: Fn() -> Fut + Sync,
    Fut: Future<Output = Outcome<C, Error>> + Send,
{
    let pool: Pool<C> = Pool::new(
        PoolConfig::new(MAX)
            .min_connections(0)
            .acquire_timeout(ACQUIRE_TIMEOUT_MS),
    );
    rt.block_on(async {
        let lease = expect_outcome(
            pool.acquire(cx, factory).await,
            &format!("{name}: acquire before the cancelled drain"),
        );
        let cancelled = Cx::for_testing();
        cancelled.set_cancel_requested(true);
        match pool.close_and_drain(&cancelled).await {
            Outcome::Cancelled(reason) => {
                eprintln!("{name}: drain cancelled as requested: {reason:?}");
            }
            Outcome::Ok(()) => {
                panic!("{name}: a cancelled drain with an active lease must not report Ok")
            }
            Outcome::Err(e) => panic!("{name}: cancelled drain failed: {e}"),
            Outcome::Panicked(p) => panic!("{name}: cancelled drain panicked: {p:?}"),
        }
        assert!(pool.is_closed(), "{name}: the pool stays closed");
        match pool.acquire(cx, factory).await {
            Outcome::Err(e) => assert_eq!(
                pool_error_kind(&e),
                Some(PoolErrorKind::Closed),
                "{name}: acquire after a cancelled drain must report Closed, got {e}"
            ),
            Outcome::Ok(_) => panic!("{name}: acquire after a cancelled drain succeeded"),
            Outcome::Cancelled(r) => panic!("{name}: acquire cancelled: {r:?}"),
            Outcome::Panicked(p) => panic!("{name}: acquire panicked: {p:?}"),
        }
        expect_outcome(
            lease.query(cx, "SELECT 1", &[]).await,
            &format!("{name}: the active lease still works while the pool drains"),
        );
        drop(lease);
        expect_outcome(
            pool.close_and_drain(cx).await,
            &format!("{name}: drain after the cancelled one"),
        );
    });
    let stats = pool.stats();
    eprintln!("{name}: after the cancelled drain {stats:?}");
    assert_eq!(stats.total_connections, 0, "{name}: drained");
    assert_eq!(stats.active_connections, 0, "{name}");
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
                let factory = move || {
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
                exercise_panicking_holder(&rt, &cx, name, &factory);
                exercise_cancelled_drain(&rt, &cx, name, &factory);
                exercise_fan_out(&rt, &cx, name, factory.clone(), None);
            }
            DriverUnderTest::Franken(path) => {
                let p = path.to_string_lossy().into_owned();
                let factory = move || {
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
                exercise_panicking_holder(&rt, &cx, name, &factory);
                exercise_cancelled_drain(&rt, &cx, name, &factory);
                exercise_fan_out(&rt, &cx, name, factory.clone(), None);
            }
            DriverUnderTest::Postgres(cfg) => {
                let cfg = cfg.clone();
                let factory_cfg = cfg.clone();
                let cx2 = cx.clone();
                let factory = move || {
                    let cfg = factory_cfg.clone();
                    let cx = cx2.clone();
                    async move { SharedPgConnection::connect(&cx, cfg).await }
                };
                rt.block_on(exercise(&cx, name, &factory));
                exercise_contention(&rt, &cx, name, &factory);
                exercise_panicking_holder(&rt, &cx, name, &factory);
                exercise_cancelled_drain(&rt, &cx, name, &factory);
                exercise_server_sessions(
                    &rt,
                    &cx,
                    name,
                    &factory,
                    "SELECT pg_backend_pid()",
                    |id| format!("SELECT pg_terminate_backend({id})"),
                );
                // The pool's sessions carry an application_name so
                // pg_stat_activity can count exactly them.
                let tag = format!("sqlmodel-e2e-pool-{}", std::process::id());
                let tagged_cfg = cfg.clone().application_name(tag.clone());
                let cx3 = cx.clone();
                let tagged = move || {
                    let cfg = tagged_cfg.clone();
                    let cx = cx3.clone();
                    async move { SharedPgConnection::connect(&cx, cfg).await }
                };
                let control = rt.block_on(async {
                    expect_outcome(
                        SharedPgConnection::connect(&cx, cfg.clone()).await,
                        &format!("{name}: control connection for counting"),
                    )
                });
                exercise_fan_out(
                    &rt,
                    &cx,
                    name,
                    tagged,
                    Some(ServerCount {
                        control,
                        sql: "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1"
                            .into(),
                        params: vec![Value::Text(tag)],
                        extra: 0,
                    }),
                );
            }
            DriverUnderTest::MySql(cfg) | DriverUnderTest::MariaDb(cfg) => {
                let cfg = cfg.clone();
                let factory_cfg = cfg.clone();
                let cx2 = cx.clone();
                let factory = move || {
                    let cfg = factory_cfg.clone();
                    let cx = cx2.clone();
                    async move { SharedMySqlConnection::connect(&cx, cfg).await }
                };
                rt.block_on(exercise(&cx, name, &factory));
                exercise_contention(&rt, &cx, name, &factory);
                exercise_panicking_holder(&rt, &cx, name, &factory);
                exercise_cancelled_drain(&rt, &cx, name, &factory);
                exercise_server_sessions(
                    &rt,
                    &cx,
                    name,
                    &factory,
                    "SELECT CONNECTION_ID()",
                    |id| format!("KILL CONNECTION {id}"),
                );
                // MySQL sessions are counted by user and database; the control
                // connection is one of them.
                let control = rt.block_on(async {
                    expect_outcome(
                        SharedMySqlConnection::connect(&cx, cfg.clone()).await,
                        &format!("{name}: control connection for counting"),
                    )
                });
                let database = cfg.database.clone().unwrap_or_default();
                exercise_fan_out(
                    &rt,
                    &cx,
                    name,
                    factory.clone(),
                    Some(ServerCount {
                        control,
                        sql: "SELECT count(*) FROM information_schema.processlist \
                              WHERE user = ? AND db = ?"
                            .into(),
                        params: vec![Value::Text(cfg.user.clone()), Value::Text(database)],
                        extra: 1,
                    }),
                );
            }
        }
        ran.push(name);
    }
    eprintln!("sqlmodel-e2e: pool ran on {}", ran.join(", "));
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
    assert!(ran.contains(&"c-sqlite(file)"), "{ran:?}");
}
