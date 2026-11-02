use std::future::{Future, poll_fn};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Budget, CancelKind, Cx, Outcome};

use sqlmodel_core::error::{ConnectionErrorKind, QueryErrorKind, TransactionErrorKind};
use sqlmodel_core::{Connection, Error, IsolationLevel, TransactionOps, Value};

use sqlmodel_postgres::{PgConfig, SharedPgConnection, SslMode};
use sqlmodel_schema::introspect::{Dialect, Introspector};

const POSTGRES_URL_ENV: &str = "SQLMODEL_TEST_POSTGRES_URL";

fn postgres_test_config() -> Option<PgConfig> {
    let raw = std::env::var(POSTGRES_URL_ENV).ok()?;
    let cfg = parse_postgres_url(&raw)?;
    if cfg.database.is_empty() {
        eprintln!(
            "skipping Postgres integration tests: {POSTGRES_URL_ENV} must include a database name (postgres://user:pass@host:5432/db)"
        );
        return None;
    }
    Some(
        cfg.connect_timeout(Duration::from_secs(10))
            .ssl_mode(SslMode::Disable),
    )
}

fn parse_postgres_url(url: &str) -> Option<PgConfig> {
    let url = url.trim();
    if url.is_empty() {
        return None;
    }

    let rest = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))?;

    let (auth, host_and_path) = rest.split_once('@')?;
    let (user, password) = match auth.split_once(':') {
        Some((u, p)) => (u, Some(p)),
        None => (auth, None),
    };

    let (host_port, db) = host_and_path.split_once('/')?;
    let db = db
        .split_once('?')
        .map_or(db, |(left, _)| left)
        .trim_matches('/');

    let (host, port) = parse_host_port(host_port)?;
    let mut cfg = PgConfig::new(host, user, db).port(port);
    if let Some(pw) = password.filter(|p| !p.is_empty()) {
        cfg = cfg.password(pw);
    }
    Some(cfg)
}

fn parse_host_port(input: &str) -> Option<(&str, u16)> {
    if let Some(rest) = input.strip_prefix('[') {
        let end = rest.find(']')?;
        let host = &rest[..end];
        let after = &rest[end + 1..];
        let port = after
            .strip_prefix(':')
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(5432);
        return Some((host, port));
    }

    match input.rsplit_once(':') {
        Some((host, port_str)) if port_str.chars().all(|c| c.is_ascii_digit()) => {
            Some((host, port_str.parse::<u16>().ok()?))
        }
        _ => Some((input, 5432)),
    }
}

fn unwrap_outcome<T>(outcome: Outcome<T, Error>) -> T {
    match outcome {
        Outcome::Ok(v) => v,
        Outcome::Err(e) => {
            eprintln!("unexpected error: {e}");
            std::process::abort();
        }
        Outcome::Cancelled(r) => {
            eprintln!("cancelled: {r:?}");
            std::process::abort();
        }
        Outcome::Panicked(p) => {
            eprintln!("panicked: {p:?}");
            std::process::abort();
        }
    }
}

fn compact_sql_fragment(input: &str) -> String {
    input
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '"' && *c != '`')
        .collect::<String>()
        .to_ascii_lowercase()
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos()
}

fn test_table_name(prefix: &str) -> String {
    format!("{prefix}_{}", unique_suffix())
}

#[test]
fn postgres_connect_select_1() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg).await);
        let rows = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(rows.len(), 1);
        let one: i64 = rows[0].get_as(0).expect("row[0] as i64");
        assert_eq!(one, 1);
    });
}

#[test]
fn postgres_dropped_query_future_invalidates_unfinished_exchange() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("production runtime");
    let cx = rt.request_cx_with_budget(Budget::INFINITE);

    rt.block_on(async {
        for prepared in [false, true] {
            let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg.clone()).await);
            let sql = "SELECT 424242 AS abandoned_marker FROM pg_sleep(0.2)";
            let incomplete = if prepared {
                let statement = unwrap_outcome(conn.prepare(&cx, sql).await);
                asupersync::time::timeout(
                    asupersync::time::wall_now(),
                    Duration::from_millis(20),
                    conn.query_prepared(&cx, &statement, &[]),
                )
                .await
            } else {
                asupersync::time::timeout(
                    asupersync::time::wall_now(),
                    Duration::from_millis(20),
                    conn.query(&cx, sql, &[]),
                )
                .await
            };
            assert!(incomplete.is_err(), "the response must still be pending");

            // A future drop cannot leave the old response available for a
            // different request, even when the caller retains the connection.
            let next = asupersync::time::timeout(
                asupersync::time::wall_now(),
                Duration::from_secs(2),
                conn.query(&cx, "SELECT 7 AS fresh_marker", &[]),
            )
            .await
            .expect("an interrupted connection must reject new work promptly");
            match next {
                Outcome::Err(Error::Connection(error)) => {
                    assert_eq!(error.kind, ConnectionErrorKind::Disconnected);
                }
                other => panic!(
                    "prepared={prepared}: unfinished exchange was reused; SELECT 7 must not consume the abandoned response: {other:?}"
                ),
            }
        }
    });
}

#[test]
fn postgres_pending_wire_read_observes_cancellation_and_request_deadline() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("production runtime");
    let cx = rt.request_cx_with_budget(Budget::INFINITE);

    rt.block_on(async {
        for deadline in [false, true] {
            let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg.clone()).await);
            let budget = if deadline {
                Budget::new().with_deadline(
                    asupersync::time::wall_now() + Duration::from_millis(20),
                )
            } else {
                Budget::INFINITE
            };
            let query_cx = rt.request_cx_with_budget(budget);
            let mut query = Box::pin(conn.query(
                &query_cx,
                "SELECT 424242 AS abandoned_marker FROM pg_sleep(1)",
                &[],
            ));
            let (mut pending, canceller) = if deadline {
                (None, None)
            } else {
                let (pending_tx, pending_rx) = std::sync::mpsc::channel();
                let cancel_cx = query_cx.clone();
                let canceller = std::thread::spawn(move || {
                    pending_rx
                        .recv_timeout(Duration::from_secs(2))
                        .expect("query entered a pending wire exchange");
                    std::thread::sleep(Duration::from_millis(20));
                    cancel_cx.cancel_with(CancelKind::User, Some("cancel pending PostgreSQL read"));
                });
                (Some(pending_tx), Some(canceller))
            };
            let started = std::time::Instant::now();
            let result = asupersync::time::timeout(
                asupersync::time::wall_now(),
                Duration::from_millis(500),
                poll_fn(|task_cx| {
                    let outcome = query.as_mut().poll(task_cx);
                    if outcome.is_pending()
                        && let Some(pending) = pending.take()
                    {
                        pending.send(()).expect("notify cancellation thread");
                    }
                    outcome
                }),
            )
            .await;
            if let Some(canceller) = canceller {
                canceller.join().expect("cancellation thread completed");
            }
            let result = result.expect("cancellation must finish before the one-second server query");
            assert!(
                started.elapsed() < Duration::from_millis(400),
                "the query must be woken by its Cx, before the outer watchdog"
            );
            match result {
                Outcome::Cancelled(reason) if deadline => assert_eq!(reason.kind, CancelKind::Deadline),
                Outcome::Cancelled(reason) => assert_eq!(reason, query_cx.cancel_reason().expect("explicit cancellation reason")),
                other => panic!("pending wire read did not propagate cancellation: {other:?}"),
            }
            // Keep the completed future allocated: ownership must already be
            // released when it reports Cancelled, not deferred to its Drop.
            assert!(matches!(
                conn.query(&cx, "SELECT 7", &[]).await,
                Outcome::Err(Error::Connection(error)) if error.kind == ConnectionErrorKind::Disconnected
            ));
            drop(query);
        }
    });
}

#[test]
fn postgres_completed_or_unstarted_exchange_remains_reusable() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("production runtime");
    let cx = rt.request_cx_with_budget(Budget::INFINITE);

    rt.block_on(async {
        let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg).await);
        let before = unwrap_outcome(conn.query(&cx, "SELECT pg_backend_pid()", &[]).await)[0]
            .get_as::<i64>(0)
            .expect("original backend PID");
        let timely = rt.request_cx_with_budget(
            Budget::new().with_deadline(asupersync::time::wall_now() + Duration::from_secs(2)),
        );
        let rows = unwrap_outcome(conn.query(&timely, "SELECT 7", &[]).await);
        assert_eq!(rows[0].get_as::<i64>(0).expect("completed query"), 7);
        let cancelled = rt.request_cx_with_budget(Budget::INFINITE);
        cancelled.cancel_with(CancelKind::User, Some("cancel before any wire write"));
        match conn.query(&cancelled, "SELECT 999", &[]).await {
            Outcome::Cancelled(reason) => assert_eq!(
                reason,
                cancelled
                    .cancel_reason()
                    .expect("preflight cancellation reason")
            ),
            other => panic!("preflight cancellation was not observed: {other:?}"),
        }
        let rows = unwrap_outcome(conn.query(&cx, "SELECT 7, pg_backend_pid()", &[]).await);
        assert_eq!(rows[0].get_as::<i64>(0).expect("fresh result"), 7);
        assert_eq!(rows[0].get_as::<i64>(1).expect("same backend PID"), before);
    });
}

#[test]
fn postgres_fully_drained_server_errors_preserve_connection_reuse() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("production runtime");
    let cx = rt.request_cx_with_budget(Budget::INFINITE);

    rt.block_on(async {
        let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg).await);
        let before = unwrap_outcome(conn.query(&cx, "SELECT pg_backend_pid()", &[]).await)[0]
            .get_as::<i64>(0)
            .expect("original backend PID");
        assert!(matches!(
            conn.query(&cx, "SELECT FROM", &[]).await,
            Outcome::Err(Error::Query(_))
        ));
        assert!(matches!(
            conn.prepare(&cx, "SELECT FROM").await,
            Outcome::Err(Error::Query(_))
        ));
        unwrap_outcome(
            conn.execute(
                &cx,
                "CREATE TEMP TABLE sqlmodel_error_recovery (id INTEGER PRIMARY KEY)",
                &[],
            )
            .await,
        );
        unwrap_outcome(
            conn.execute(&cx, "INSERT INTO sqlmodel_error_recovery VALUES (1)", &[])
                .await,
        );
        match conn
            .execute(&cx, "INSERT INTO sqlmodel_error_recovery VALUES (1)", &[])
            .await
        {
            Outcome::Err(Error::Query(error)) => {
                assert_eq!(error.sqlstate.as_deref(), Some("23505"));
            }
            other => panic!("expected a fully drained unique violation: {other:?}"),
        }
        let rows = unwrap_outcome(conn.query(&cx, "SELECT 7, pg_backend_pid()", &[]).await);
        assert_eq!(rows[0].get_as::<i64>(0).expect("fresh result"), 7);
        assert_eq!(
            rows[0].get_as::<i64>(1).expect("reused backend PID"),
            before
        );
    });
}

#[test]
fn postgres_insert_and_select_roundtrip() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg).await);

        let table = test_table_name("sqlmodel_pg_roundtrip");
        let create_sql = format!(
            "CREATE TABLE \"{table}\" (\
             id BIGSERIAL PRIMARY KEY,\
             name TEXT NOT NULL\
             )"
        );
        let insert_sql = format!("INSERT INTO \"{table}\" (name) VALUES ($1) RETURNING id");
        let select_sql = format!("SELECT id, name FROM \"{table}\" WHERE id = $1");
        let drop_sql = format!("DROP TABLE IF EXISTS \"{table}\"");

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
        unwrap_outcome(conn.execute(&cx, &create_sql, &[]).await);

        let id = unwrap_outcome(
            conn.insert(&cx, &insert_sql, &[Value::Text("Alice".into())])
                .await,
        );
        assert!(id > 0);

        let rows = unwrap_outcome(conn.query(&cx, &select_sql, &[Value::BigInt(id)]).await);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_as::<i64>(0).expect("id"), id);
        assert_eq!(rows[0].get_as::<String>(1).expect("name"), "Alice");

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
    });
}

#[test]
fn postgres_transaction_rollback_discards_changes() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg).await);

        let table = test_table_name("sqlmodel_pg_tx");
        let create_sql = format!(
            "CREATE TABLE \"{table}\" (\
             id BIGSERIAL PRIMARY KEY,\
             name TEXT NOT NULL\
             )"
        );
        let insert_sql = format!("INSERT INTO \"{table}\" (name) VALUES ($1)");
        let count_sql = format!("SELECT COUNT(*) FROM \"{table}\" WHERE name = $1");
        let drop_sql = format!("DROP TABLE IF EXISTS \"{table}\"");

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
        unwrap_outcome(conn.execute(&cx, &create_sql, &[]).await);

        let tx = unwrap_outcome(conn.begin(&cx).await);
        unwrap_outcome(
            tx.execute(&cx, &insert_sql, &[Value::Text("Bob".into())])
                .await,
        );
        unwrap_outcome(tx.rollback(&cx).await);

        let rows = unwrap_outcome(
            conn.query(&cx, &count_sql, &[Value::Text("Bob".into())])
                .await,
        );
        assert_eq!(rows.len(), 1);
        let count: i64 = rows[0].get_as(0).expect("COUNT(*) as i64");
        assert_eq!(count, 0);

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
    });
}

#[test]
fn postgres_transaction_isolation_is_applied_by_begin() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("production runtime");
    let cx = rt.request_cx_with_budget(Budget::INFINITE);

    rt.block_on(async {
        let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg).await);
        for isolation in [
            IsolationLevel::Serializable,
            IsolationLevel::RepeatableRead,
            IsolationLevel::ReadCommitted,
        ] {
            let tx = unwrap_outcome(conn.begin_with(&cx, isolation).await);
            let rows = unwrap_outcome(tx.query(&cx, "SHOW transaction_isolation", &[]).await);
            let observed = rows[0].get_as::<String>(0).expect("transaction isolation");
            unwrap_outcome(tx.rollback(&cx).await);
            assert_eq!(observed, isolation.as_sql().to_ascii_lowercase());
        }
    });
}

#[test]
fn postgres_transaction_excludes_connection_clones_until_finalized() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("production runtime");
    let cx = rt.request_cx_with_budget(Budget::INFINITE);

    rt.block_on(async {
        let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg).await);
        let clone = conn.clone();
        let before = unwrap_outcome(conn.query(&cx, "SELECT pg_backend_pid()", &[]).await)[0]
            .get_as::<i64>(0)
            .expect("original backend PID");
        unwrap_outcome(
            conn.execute(
                &cx,
                "CREATE TEMP TABLE sqlmodel_tx_exclusion (id INTEGER)",
                &[],
            )
            .await,
        );
        let tx = unwrap_outcome(conn.begin(&cx).await);
        unwrap_outcome(
            tx.execute(
                &cx,
                "INSERT INTO sqlmodel_tx_exclusion VALUES (424242)",
                &[],
            )
            .await,
        );
        let started = std::time::Instant::now();
        let waiter_cx = rt.request_cx_with_budget(
            Budget::new().with_deadline(asupersync::time::wall_now() + Duration::from_millis(30)),
        );
        let mut waiting = Box::pin(clone.query(
            &waiter_cx,
            "SELECT count(*) FROM sqlmodel_tx_exclusion",
            &[],
        ));
        let outcome = asupersync::time::timeout(
            asupersync::time::wall_now(),
            Duration::from_millis(500),
            waiting.as_mut(),
        )
        .await
        .expect("a waiting connection clone must honor its request deadline");
        match outcome {
            Outcome::Cancelled(reason) => assert_eq!(reason.kind, CancelKind::Deadline),
            other => panic!("connection clone entered another owner's transaction: {other:?}"),
        }
        assert!(started.elapsed() >= Duration::from_millis(30));
        assert!(started.elapsed() < Duration::from_millis(400));

        // The cancelled waiter must leave its owner and the connection alone,
        // even while the caller retains the completed waiting future.
        let rows = unwrap_outcome(
            tx.query(&cx, "SELECT count(*) FROM sqlmodel_tx_exclusion", &[])
                .await,
        );
        assert_eq!(
            rows[0].get_as::<i64>(0).expect("owner's uncommitted row"),
            1
        );

        let cancelled_cx = rt.request_cx_with_budget(Budget::INFINITE);
        let (pending_tx, pending_rx) = std::sync::mpsc::channel();
        let cancel_cx = cancelled_cx.clone();
        let canceller = std::thread::spawn(move || {
            pending_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("clone waits for transaction owner");
            std::thread::sleep(Duration::from_millis(20));
            cancel_cx.cancel_with(CancelKind::User, Some("cancel waiting connection clone"));
        });
        let mut cancel_waiter = Box::pin(clone.query(&cancelled_cx, "SELECT 7", &[]));
        let mut pending = Some(pending_tx);
        let started = std::time::Instant::now();
        let outcome = asupersync::time::timeout(
            asupersync::time::wall_now(),
            Duration::from_millis(500),
            poll_fn(|task_cx| {
                let outcome = cancel_waiter.as_mut().poll(task_cx);
                if outcome.is_pending()
                    && let Some(pending) = pending.take()
                {
                    pending.send(()).expect("notify cancellation thread");
                }
                outcome
            }),
        )
        .await;
        canceller.join().expect("cancel waiting clone");
        match outcome.expect("explicit cancellation must wake the connection-lock waiter") {
            Outcome::Cancelled(reason) => assert_eq!(
                reason,
                cancelled_cx
                    .cancel_reason()
                    .expect("explicit cancellation reason")
            ),
            other => panic!("connection-lock waiter lost cancellation: {other:?}"),
        }
        assert!(started.elapsed() < Duration::from_millis(400));
        unwrap_outcome(tx.rollback(&cx).await);
        let rows = unwrap_outcome(
            clone
                .query(
                    &cx,
                    "SELECT pg_backend_pid(), (SELECT count(*) FROM sqlmodel_tx_exclusion)",
                    &[],
                )
                .await,
        );
        assert_eq!(
            rows[0].get_as::<i64>(0).expect("reused backend PID"),
            before
        );
        assert_eq!(rows[0].get_as::<i64>(1).expect("rolled-back row count"), 0);
        drop(cancel_waiter);
        drop(waiting);
    });
}

#[test]
fn postgres_transaction_commit_and_rollback_preserve_backend() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("production runtime");
    let cx = rt.request_cx_with_budget(Budget::INFINITE);

    rt.block_on(async {
        let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg).await);
        let before = unwrap_outcome(conn.query(&cx, "SELECT pg_backend_pid()", &[]).await)[0]
            .get_as::<i64>(0)
            .expect("original backend PID");
        unwrap_outcome(
            conn.execute(
                &cx,
                "CREATE TEMP TABLE sqlmodel_tx_finalization (id INTEGER PRIMARY KEY DEFERRABLE INITIALLY IMMEDIATE)",
                &[],
            )
            .await,
        );
        for rollback in [false, true] {
            let tx = unwrap_outcome(conn.begin(&cx).await);
            let id = if rollback { 2 } else { 1 };
            unwrap_outcome(
                tx.execute(
                    &cx,
                    "INSERT INTO sqlmodel_tx_finalization VALUES ($1)",
                    &[Value::Int(id)],
                )
                .await,
            );
            unwrap_outcome(tx.savepoint(&cx, "recovery_point").await);
            unwrap_outcome(
                tx.execute(&cx, "INSERT INTO sqlmodel_tx_finalization VALUES (99)", &[])
                    .await,
            );
            unwrap_outcome(tx.rollback_to(&cx, "recovery_point").await);
            unwrap_outcome(tx.release(&cx, "recovery_point").await);

            let mut finish: std::pin::Pin<
                Box<dyn Future<Output = Outcome<(), Error>> + Send + '_>,
            > = if rollback {
                Box::pin(tx.rollback(&cx))
            } else {
                Box::pin(tx.commit(&cx))
            };
            unwrap_outcome(finish.as_mut().await);
            // Keep the completed finalization future allocated. It must no
            // longer retain the transaction's exclusive connection guard.
            let rows = unwrap_outcome(
                asupersync::time::timeout(
                    asupersync::time::wall_now(),
                    Duration::from_millis(500),
                    conn.query(
                        &cx,
                        "SELECT pg_backend_pid(), (SELECT count(*) FROM sqlmodel_tx_finalization)",
                        &[],
                    ),
                )
                .await
                .expect("completed finalization must release connection ownership"),
            );
            assert_eq!(rows[0].get_as::<i64>(0).expect("same backend PID"), before);
            assert_eq!(
                rows[0]
                    .get_as::<i64>(1)
                    .expect("only committed row remains"),
                1
            );
            drop(finish);
        }

        for deferred in [false, true] {
            let tx = unwrap_outcome(conn.begin(&cx).await);
            if deferred {
                unwrap_outcome(tx.execute(&cx, "SET CONSTRAINTS ALL DEFERRED", &[]).await);
            }
            let duplicate = tx.execute(&cx, "INSERT INTO sqlmodel_tx_finalization VALUES (1)", &[]).await;
            if deferred {
                unwrap_outcome(duplicate);
            } else {
                assert!(matches!(duplicate, Outcome::Err(Error::Query(error)) if error.sqlstate.as_deref() == Some("23505")));
            }
            let mut commit = Box::pin(tx.commit(&cx));
            match commit.as_mut().await {
                Outcome::Err(Error::Query(error)) if deferred => assert_eq!(error.sqlstate.as_deref(), Some("23505")),
                Outcome::Err(Error::Transaction(error)) if !deferred => assert_eq!(error.kind, TransactionErrorKind::AlreadyRolledBack),
                other => panic!("commit must report its real server outcome, deferred={deferred}: {other:?}"),
            }
            let rows = unwrap_outcome(
                asupersync::time::timeout(
                    asupersync::time::wall_now(),
                    Duration::from_millis(500),
                    conn.query(&cx, "SELECT pg_backend_pid(), (SELECT count(*) FROM sqlmodel_tx_finalization)", &[]),
                )
                .await
                .expect("fully drained failed commit must release ownership"),
            );
            assert_eq!(rows[0].get_as::<i64>(0).expect("same backend after failed commit"), before);
            assert_eq!(rows[0].get_as::<i64>(1).expect("failed commit preserves prior rows"), 1);
            drop(commit);
        }
    });
}

#[test]
fn postgres_transaction_cancelled_or_unpolled_finish_closes_connection() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("production runtime");
    let cx = rt.request_cx_with_budget(Budget::INFINITE);

    rt.block_on(async {
        let admin = unwrap_outcome(SharedPgConnection::connect(&cx, cfg.clone()).await);
        let table = test_table_name("sqlmodel_pg_abandoned_finish");
        unwrap_outcome(admin.execute(&cx, &format!("CREATE TABLE \"{table}\" (id INTEGER)"), &[]).await);
        for rollback in [false, true] {
            for cancelled in [false, true] {
                let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg.clone()).await);
                let tx = unwrap_outcome(conn.begin(&cx).await);
                unwrap_outcome(tx.execute(&cx, &format!("INSERT INTO \"{table}\" VALUES (424242)"), &[]).await);
                let finish_cx = rt.request_cx_with_budget(Budget::INFINITE);
                if cancelled {
                    finish_cx.cancel_with(CancelKind::User, Some("abandon transaction finalization"));
                }
                let finish: std::pin::Pin<Box<dyn Future<Output = Outcome<(), Error>> + Send + '_>> =
                    if rollback {
                        Box::pin(tx.rollback(&finish_cx))
                    } else {
                        Box::pin(tx.commit(&finish_cx))
                    };
                let mut finish = Some(finish);
                if cancelled {
                    match finish.as_mut().expect("finalization future").as_mut().await {
                        Outcome::Cancelled(reason) => assert_eq!(reason, finish_cx.cancel_reason().expect("explicit cancellation")),
                        other => panic!("cancelled finalization lost cancellation: {other:?}"),
                    }
                } else {
                    drop(finish.take());
                }
                let next = asupersync::time::timeout(
                    asupersync::time::wall_now(),
                    Duration::from_millis(500),
                    conn.query(&cx, "SELECT 7", &[]),
                )
                .await
                .expect("abandoned finalization must release its ownership promptly");
                assert!(matches!(next, Outcome::Err(Error::Connection(_))), "rollback={rollback}, cancelled={cancelled}: abandoned transaction remained usable: {next:?}");
                // For cancellation, the completed finalization future is
                // still allocated until after the connection is invalidated.
                drop(finish);
                let rows = unwrap_outcome(admin.query(&cx, &format!("SELECT count(*) FROM \"{table}\""), &[]).await);
                assert_eq!(rows[0].get_as::<i64>(0).expect("uncommitted rows"), 0);
            }
        }
        unwrap_outcome(admin.execute(&cx, &format!("DROP TABLE \"{table}\""), &[]).await);
    });
}

#[test]
fn postgres_unique_violation_maps_to_constraint() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg).await);

        let table = test_table_name("sqlmodel_pg_unique");
        let create_sql = format!(
            "CREATE TABLE \"{table}\" (\
             id BIGSERIAL PRIMARY KEY,\
             name TEXT NOT NULL UNIQUE\
             )"
        );
        let insert_sql = format!("INSERT INTO \"{table}\" (name) VALUES ($1)");
        let drop_sql = format!("DROP TABLE IF EXISTS \"{table}\"");

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
        unwrap_outcome(conn.execute(&cx, &create_sql, &[]).await);

        unwrap_outcome(
            conn.execute(&cx, &insert_sql, &[Value::Text("x".into())])
                .await,
        );
        let outcome = conn
            .execute(&cx, &insert_sql, &[Value::Text("x".into())])
            .await;
        assert!(
            matches!(&outcome, Outcome::Err(Error::Query(q)) if q.kind == QueryErrorKind::Constraint),
            "expected constraint error, got: {outcome:?}"
        );

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
    });
}

#[test]
fn postgres_syntax_error_maps_to_syntax() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg).await);
        let outcome = conn.query(&cx, "SELEC 1", &[]).await;
        assert!(
            matches!(&outcome, Outcome::Err(Error::Query(q)) if q.kind == QueryErrorKind::Syntax),
            "expected syntax error, got: {outcome:?}"
        );
    });
}

#[test]
fn postgres_introspection_reports_check_constraints_and_table_comment() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg).await);

        let schema = test_table_name("sqlmodel_pg_schema");
        let table = test_table_name("sqlmodel_pg_intro");
        let create_schema_sql = format!("CREATE SCHEMA \"{schema}\"");
        let set_search_path_sql = format!("SET search_path TO \"{schema}\", public");
        let reset_search_path_sql = "RESET search_path";
        let create_sql = format!(
            "CREATE TABLE \"{schema}\".\"{table}\" (\
             id BIGSERIAL PRIMARY KEY,\
             age INTEGER NOT NULL,\
             CONSTRAINT age_non_negative CHECK (age >= 0),\
             CHECK (age <= 150)\
             )"
        );
        let comment_sql =
            format!("COMMENT ON TABLE \"{schema}\".\"{table}\" IS 'hero table comment'");
        let drop_schema_sql = format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE");

        let _ = conn.execute(&cx, &drop_schema_sql, &[]).await;
        unwrap_outcome(conn.execute(&cx, &create_schema_sql, &[]).await);
        unwrap_outcome(conn.execute(&cx, &set_search_path_sql, &[]).await);
        unwrap_outcome(conn.execute(&cx, &create_sql, &[]).await);
        unwrap_outcome(conn.execute(&cx, &comment_sql, &[]).await);

        let introspector = Introspector::new(Dialect::Postgres);
        let table_names = unwrap_outcome(introspector.table_names(&cx, &conn).await);
        assert!(
            table_names.iter().any(|name| name == &table),
            "expected table {table} in current schema table list, got {:?}",
            table_names
        );
        let table_info = unwrap_outcome(introspector.table_info(&cx, &conn, &table).await);

        assert_eq!(table_info.comment.as_deref(), Some("hero table comment"));
        assert!(
            table_info.check_constraints.len() >= 2,
            "expected >=2 check constraints, got {:?}",
            table_info
                .check_constraints
                .iter()
                .map(|c| (&c.name, &c.expression))
                .collect::<Vec<_>>()
        );

        let named_check = table_info
            .check_constraints
            .iter()
            .find(|c| c.name.as_deref() == Some("age_non_negative"));
        assert!(
            named_check.is_some(),
            "missing age_non_negative check in {:?}",
            table_info
                .check_constraints
                .iter()
                .map(|c| (&c.name, &c.expression))
                .collect::<Vec<_>>()
        );
        let named_check = named_check.expect("named check should exist");
        let normalized = compact_sql_fragment(&named_check.expression);
        assert!(
            normalized.contains("age>=0"),
            "unexpected normalized expression for age_non_negative: {}",
            named_check.expression
        );

        for check in &table_info.check_constraints {
            let expr = check.expression.trim_start();
            assert!(
                !expr
                    .get(..5)
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CHECK")),
                "expression should be normalized without CHECK prefix: {}",
                check.expression
            );
        }

        let _ = conn.execute(&cx, reset_search_path_sql, &[]).await;
        let _ = conn.execute(&cx, &drop_schema_sql, &[]).await;
    });
}

#[test]
fn postgres_introspection_preserves_composite_index_column_order() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg).await);

        let table = test_table_name("sqlmodel_pg_idx_order");
        let index = format!("{table}_c_a_idx");
        let create_sql = format!(
            "CREATE TABLE \"{table}\" (\
             id BIGSERIAL PRIMARY KEY,\
             a INTEGER NOT NULL,\
             b INTEGER NOT NULL,\
             c INTEGER NOT NULL\
             )"
        );
        let create_index_sql = format!("CREATE INDEX \"{index}\" ON \"{table}\" (c, a)");
        let drop_sql = format!("DROP TABLE IF EXISTS \"{table}\"");

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
        unwrap_outcome(conn.execute(&cx, &create_sql, &[]).await);
        unwrap_outcome(conn.execute(&cx, &create_index_sql, &[]).await);

        let introspector = Introspector::new(Dialect::Postgres);
        let table_info = unwrap_outcome(introspector.table_info(&cx, &conn, &table).await);
        let index_info = table_info.indexes.iter().find(|idx| idx.name == index);
        assert!(
            index_info.is_some(),
            "missing expected index {index} in {:?}",
            table_info.indexes
        );
        let index_info = index_info.expect("checked above");

        assert_eq!(
            index_info.columns,
            vec!["c".to_string(), "a".to_string()],
            "composite index columns should preserve defined order"
        );

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
    });
}

/// Parameterized `query`/`execute` calls go through named prepared statements
/// with a per-connection cache: `pg_prepared_statements` shows one statement
/// per distinct (SQL, param types), no duplicate prepares on cache hits,
/// and LRU eviction closing statements when exceeding capacity.
#[test]
fn postgres_parameterized_queries_are_prepared_once_and_cached() {
    let Some(cfg) = postgres_test_config() else {
        eprintln!("skipping Postgres integration tests: set {POSTGRES_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedPgConnection::connect(&cx, cfg).await);

        // Helper to count prepared statements created by sqlmodel on this session
        let count_sqlmodel_stmts = || {
            let conn = &conn;
            let cx = &cx;
            async move {
                let rows = unwrap_outcome(
                    conn.query(
                        cx,
                        "SELECT count(*) FROM pg_prepared_statements WHERE name LIKE 'sqlmodel_s%'",
                        &[],
                    )
                    .await,
                );
                rows[0].get_as::<i64>(0).expect("count column")
            }
        };

        let initial_count = count_sqlmodel_stmts().await;

        // 1. Same query with same param type executed 5 times:
        // Cache should grow by exactly 1 on the first call, and stay unchanged for subsequent calls.
        for i in 0..5 {
            let rows = unwrap_outcome(
                conn.query(&cx, "SELECT $1::bigint + 1", &[Value::BigInt(i)])
                    .await,
            );
            assert_eq!(rows[0].get_as::<i64>(0).unwrap(), i + 1);
        }

        assert_eq!(
            count_sqlmodel_stmts().await - initial_count,
            1,
            "exactly one prepared statement created for 5 identical-signature query calls"
        );
        assert_eq!(conn.statement_cache_len(&cx).await, 1);

        // 2. Different query with parameters:
        let rows = unwrap_outcome(
            conn.query(&cx, "SELECT $1::bigint * 2", &[Value::BigInt(21)])
                .await,
        );
        assert_eq!(rows[0].get_as::<i64>(0).unwrap(), 42);
        assert_eq!(
            count_sqlmodel_stmts().await - initial_count,
            2,
            "one additional statement for distinct query"
        );

        // 3. Same query with DIFFERENT parameter types (Int vs BigInt):
        // Statement cache key includes param OIDs, so alternating types must not conflict or error.
        let rows1 = unwrap_outcome(conn.query(&cx, "SELECT $1 + 10", &[Value::Int(5)]).await);
        assert_eq!(rows1[0].get_as::<i32>(0).unwrap(), 15);

        let rows2 = unwrap_outcome(
            conn.query(&cx, "SELECT $1 + 10", &[Value::BigInt(50)])
                .await,
        );
        assert_eq!(rows2[0].get_as::<i64>(0).unwrap(), 60);

        // 4. Parameterless queries do not create cached named statements:
        let count_before = count_sqlmodel_stmts().await;
        let rows = unwrap_outcome(conn.query(&cx, "SELECT 42", &[]).await);
        assert_eq!(rows[0].get_as::<i32>(0).unwrap(), 42);
        assert_eq!(
            count_sqlmodel_stmts().await,
            count_before,
            "parameterless query must not create a cached statement"
        );

        // 5. Cache capacity and eviction:
        // Execute enough distinct statements to exceed capacity (64 + 8).
        for i in 0..(sqlmodel_postgres::STATEMENT_CACHE_CAPACITY + 8) {
            unwrap_outcome(
                conn.query(
                    &cx,
                    &format!("SELECT $1::bigint + {i}"),
                    &[Value::BigInt(1)],
                )
                .await,
            );
        }

        // Cache length in memory is bounded by capacity
        assert_eq!(
            conn.statement_cache_len(&cx).await,
            sqlmodel_postgres::STATEMENT_CACHE_CAPACITY
        );

        // 6. Invalidation on DISCARD ALL:
        unwrap_outcome(conn.execute(&cx, "DISCARD ALL", &[]).await);
        assert_eq!(conn.statement_cache_len(&cx).await, 0);
    });
}
