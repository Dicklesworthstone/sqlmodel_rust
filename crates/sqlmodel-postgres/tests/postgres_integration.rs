use std::time::{Duration, SystemTime, UNIX_EPOCH};

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};

use sqlmodel_core::error::QueryErrorKind;
use sqlmodel_core::{Connection, Error, TransactionOps, Value};

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
