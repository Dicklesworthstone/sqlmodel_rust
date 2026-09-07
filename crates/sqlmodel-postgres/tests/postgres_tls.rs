#![cfg(feature = "tls")]

use std::path::{Path, PathBuf};
use std::time::Duration;

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};

use sqlmodel_core::{Connection, Error};
use sqlmodel_postgres::{PgConfig, SharedPgConnection, SslMode};

const POSTGRES_URL_ENV: &str = "SQLMODEL_TEST_POSTGRES_URL";
const POSTGRES_CA_ENV: &str = "SQLMODEL_TEST_POSTGRES_CA";
const POSTGRES_REQUIRE_SSL_ENV: &str = "SQLMODEL_TEST_POSTGRES_REQUIRE_SSL";

fn postgres_base_config() -> Option<PgConfig> {
    let raw = std::env::var(POSTGRES_URL_ENV).ok()?;
    let cfg = parse_postgres_url(&raw)?;
    if cfg.database.is_empty() {
        eprintln!("skipping Postgres TLS tests: {POSTGRES_URL_ENV} must include a database name");
        return None;
    }
    Some(cfg.connect_timeout(Duration::from_secs(10)))
}

fn postgres_ca_path() -> Option<PathBuf> {
    if let Ok(ca) = std::env::var(POSTGRES_CA_ENV) {
        let p = PathBuf::from(ca);
        if p.exists() {
            return Some(p);
        }
    }
    let fallback = PathBuf::from("/tmp/pg-test-certs/ca.crt");
    if fallback.exists() {
        return Some(fallback);
    }
    let local = PathBuf::from("pg-ca.crt");
    if local.exists() {
        return Some(local);
    }
    None
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
            panic!("unexpected error: {e}");
        }
        Outcome::Cancelled(r) => {
            panic!("cancelled: {r:?}");
        }
        Outcome::Panicked(p) => {
            panic!("panicked: {p:?}");
        }
    }
}

#[test]
fn postgres_tls_prefer_connect() {
    let Some(cfg) = postgres_base_config() else {
        eprintln!("skipping Postgres TLS tests: set {POSTGRES_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn =
            unwrap_outcome(SharedPgConnection::connect(&cx, cfg.ssl_mode(SslMode::Prefer)).await);
        let rows = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(rows.len(), 1);
        let val: i64 = rows[0].get_as(0).expect("row[0] as i64");
        assert_eq!(val, 1);
    });
}

#[test]
fn postgres_tls_require_connect() {
    let Some(cfg) = postgres_base_config() else {
        eprintln!("skipping Postgres TLS tests: set {POSTGRES_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn =
            unwrap_outcome(SharedPgConnection::connect(&cx, cfg.ssl_mode(SslMode::Require)).await);
        let rows = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(rows.len(), 1);
        let val: i64 = rows[0].get_as(0).expect("row[0] as i64");
        assert_eq!(val, 1);
    });
}

#[test]
fn postgres_tls_verify_ca_success() {
    let Some(cfg) = postgres_base_config() else {
        eprintln!("skipping Postgres TLS tests: set {POSTGRES_URL_ENV}");
        return;
    };
    let Some(ca_path) = postgres_ca_path() else {
        eprintln!("skipping VerifyCa test: CA certificate not found (set {POSTGRES_CA_ENV})");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let test_cfg = cfg.ssl_mode(SslMode::VerifyCa).root_cert_path(&ca_path);
        let conn = unwrap_outcome(SharedPgConnection::connect(&cx, test_cfg).await);
        let rows = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(rows.len(), 1);
        let val: i64 = rows[0].get_as(0).expect("row[0] as i64");
        assert_eq!(val, 1);
    });
}

#[test]
fn postgres_tls_verify_ca_rejects_untrusted_ca() {
    let Some(cfg) = postgres_base_config() else {
        eprintln!("skipping Postgres TLS tests: set {POSTGRES_URL_ENV}");
        return;
    };

    let untrusted_ca_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("untrusted_ca.crt");

    if !untrusted_ca_path.exists() {
        eprintln!("skipping untrusted_ca test: fixture file missing");
        return;
    }

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let test_cfg = cfg
            .ssl_mode(SslMode::VerifyCa)
            .root_cert_path(&untrusted_ca_path);
        let outcome = SharedPgConnection::connect(&cx, test_cfg).await;
        match outcome {
            Outcome::Err(e) => {
                let err_str = e.to_string();
                assert!(
                    err_str.contains("TLS")
                        || err_str.contains("certificate")
                        || err_str.contains("handshake")
                        || err_str.contains("UnknownIssuer"),
                    "expected certificate verification failure, got: {err_str}"
                );
            }
            Outcome::Ok(_) => panic!("expected connection failure with untrusted CA"),
            other => panic!("unexpected outcome: {other:?}"),
        }
    });
}

#[test]
fn postgres_tls_verify_full_connect() {
    let Some(cfg) = postgres_base_config() else {
        eprintln!("skipping Postgres TLS tests: set {POSTGRES_URL_ENV}");
        return;
    };
    let Some(ca_path) = postgres_ca_path() else {
        eprintln!("skipping VerifyFull test: CA certificate not found (set {POSTGRES_CA_ENV})");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let test_cfg = cfg.ssl_mode(SslMode::VerifyFull).root_cert_path(&ca_path);
        let outcome = SharedPgConnection::connect(&cx, test_cfg).await;
        match outcome {
            Outcome::Ok(conn) => {
                let rows = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
                assert_eq!(rows.len(), 1);
            }
            Outcome::Err(e) => {
                let err_str = e.to_string();
                assert!(
                    err_str.contains("certificate")
                        || err_str.contains("TLS")
                        || err_str.contains("NotValidForName"),
                    "expected hostname mismatch or TLS error, got: {err_str}"
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    });
}

#[test]
fn postgres_tls_disable_when_ssl_required() {
    let Some(cfg) = postgres_base_config() else {
        eprintln!("skipping Postgres TLS tests: set {POSTGRES_URL_ENV}");
        return;
    };

    let require_ssl =
        std::env::var(POSTGRES_REQUIRE_SSL_ENV).is_ok_and(|v| v == "1" || v == "true");
    if !require_ssl {
        eprintln!("skipping disable_when_ssl_required test: {POSTGRES_REQUIRE_SSL_ENV} not set");
        return;
    }

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let outcome = SharedPgConnection::connect(&cx, cfg.ssl_mode(SslMode::Disable)).await;
        match outcome {
            Outcome::Err(e) => {
                eprintln!("Disable rejected as expected: {e}");
            }
            Outcome::Ok(_) => panic!("expected connection failure when SSL is required on server"),
            other => panic!("unexpected outcome: {other:?}"),
        }
    });
}
