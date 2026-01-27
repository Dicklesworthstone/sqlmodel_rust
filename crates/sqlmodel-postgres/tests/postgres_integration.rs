use std::time::{Duration, SystemTime, UNIX_EPOCH};

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};

use sqlmodel_core::error::QueryErrorKind;
use sqlmodel_core::{Connection, Error, TransactionOps, Value};

use sqlmodel_postgres::{PgConfig, SharedPgConnection, SslMode};

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
        Outcome::Err(e) => panic!("unexpected error: {e}"),
        Outcome::Cancelled(r) => panic!("cancelled: {r:?}"),
        Outcome::Panicked(p) => panic!("panicked: {p:?}"),
    }
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

        match outcome {
            Outcome::Err(Error::Query(q)) => assert_eq!(q.kind, QueryErrorKind::Constraint),
            other => panic!("expected constraint error, got: {other:?}"),
        }

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
        match outcome {
            Outcome::Err(Error::Query(q)) => assert_eq!(q.kind, QueryErrorKind::Syntax),
            other => panic!("expected syntax error, got: {other:?}"),
        }
    });
}
