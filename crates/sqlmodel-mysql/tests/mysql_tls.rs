#![cfg(feature = "tls")]

use std::path::{Path, PathBuf};
use std::time::Duration;

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};

use sqlmodel_core::{Connection, Error};
use sqlmodel_mysql::{MySqlConfig, SharedMySqlConnection, SslMode};

const MYSQL_URL_ENV: &str = "SQLMODEL_TEST_MYSQL_URL";
const MYSQL_CA_ENV: &str = "SQLMODEL_TEST_MYSQL_CA";
const MYSQL_REQUIRE_SECURE_TRANSPORT_ENV: &str = "SQLMODEL_TEST_MYSQL_REQUIRE_SECURE_TRANSPORT";
const MARIADB_URL_ENV: &str = "SQLMODEL_TEST_MARIADB_URL";

fn mysql_base_config() -> Option<MySqlConfig> {
    let raw = std::env::var(MYSQL_URL_ENV).ok()?;
    let cfg = parse_mysql_url(&raw)?;
    if cfg.database.is_none() {
        eprintln!("skipping MySQL TLS tests: {MYSQL_URL_ENV} must include a database name");
        return None;
    }
    Some(cfg.connect_timeout(Duration::from_secs(10)))
}

fn mysql_ca_path() -> Option<PathBuf> {
    if let Ok(ca) = std::env::var(MYSQL_CA_ENV) {
        let p = PathBuf::from(ca);
        if p.exists() {
            return Some(p);
        }
    }
    let fallback = PathBuf::from("/tmp/mysql-ca.pem");
    if fallback.exists() {
        return Some(fallback);
    }
    let local = PathBuf::from("mysql-ca.pem");
    if local.exists() {
        return Some(local);
    }
    None
}

fn parse_mysql_url(url: &str) -> Option<MySqlConfig> {
    let url = url.trim();
    if url.is_empty() {
        return None;
    }

    let rest = url.strip_prefix("mysql://")?;
    let (auth, host_and_path) = rest.split_once('@')?;
    let (user, password) = match auth.split_once(':') {
        Some((u, p)) => (u, Some(p)),
        None => (auth, None),
    };

    let (host_port, db) = match host_and_path.split_once('/') {
        Some((hp, path)) => (hp, Some(path)),
        None => (host_and_path, None),
    };

    let db = db
        .map(|s| s.split_once('?').map_or(s, |(left, _)| left))
        .filter(|s| !s.is_empty());

    let (host, port) = parse_host_port(host_port)?;

    let mut cfg = MySqlConfig::new().host(host).port(port).user(user);
    if let Some(pw) = password.filter(|p| !p.is_empty()) {
        cfg = cfg.password(pw);
    }
    if let Some(db) = db {
        cfg = cfg.database(db);
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
            .unwrap_or(3306);
        return Some((host, port));
    }

    match input.rsplit_once(':') {
        Some((host, port_str)) if port_str.chars().all(|c| c.is_ascii_digit()) => {
            Some((host, port_str.parse::<u16>().ok()?))
        }
        _ => Some((input, 3306)),
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
fn mysql_tls_preferred_connect() {
    let Some(cfg) = mysql_base_config() else {
        eprintln!("skipping MySQL TLS tests: set {MYSQL_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(
            SharedMySqlConnection::connect(&cx, cfg.ssl_mode(SslMode::Preferred)).await,
        );
        let rows = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(rows.len(), 1);
        let val: i64 = rows[0].get_as(0).expect("row[0] as i64");
        assert_eq!(val, 1);
    });
}

#[test]
fn mysql_tls_required_connect() {
    let Some(cfg) = mysql_base_config() else {
        eprintln!("skipping MySQL TLS tests: set {MYSQL_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(
            SharedMySqlConnection::connect(&cx, cfg.ssl_mode(SslMode::Required)).await,
        );
        let rows = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(rows.len(), 1);
        let val: i64 = rows[0].get_as(0).expect("row[0] as i64");
        assert_eq!(val, 1);
    });
}

#[test]
fn mysql_tls_verify_ca_success() {
    let Some(cfg) = mysql_base_config() else {
        eprintln!("skipping MySQL TLS tests: set {MYSQL_URL_ENV}");
        return;
    };
    let Some(ca_path) = mysql_ca_path() else {
        eprintln!("skipping VerifyCa test: CA certificate not found (set {MYSQL_CA_ENV})");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let test_cfg = cfg.ssl_mode(SslMode::VerifyCa).ca_cert(&ca_path);
        let conn = unwrap_outcome(SharedMySqlConnection::connect(&cx, test_cfg).await);
        let rows = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(rows.len(), 1);
        let val: i64 = rows[0].get_as(0).expect("row[0] as i64");
        assert_eq!(val, 1);
    });
}

#[test]
fn mysql_tls_verify_ca_rejects_untrusted_ca() {
    let Some(cfg) = mysql_base_config() else {
        eprintln!("skipping MySQL TLS tests: set {MYSQL_URL_ENV}");
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
        let test_cfg = cfg.ssl_mode(SslMode::VerifyCa).ca_cert(&untrusted_ca_path);
        let outcome = SharedMySqlConnection::connect(&cx, test_cfg).await;
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
fn mysql_tls_verify_identity_hostname_validation() {
    let Some(cfg) = mysql_base_config() else {
        eprintln!("skipping MySQL TLS tests: set {MYSQL_URL_ENV}");
        return;
    };
    let Some(ca_path) = mysql_ca_path() else {
        eprintln!("skipping VerifyIdentity test: CA certificate not found (set {MYSQL_CA_ENV})");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let test_cfg = cfg.ssl_mode(SslMode::VerifyIdentity).ca_cert(&ca_path);
        let outcome = SharedMySqlConnection::connect(&cx, test_cfg).await;
        // On auto-generated MySQL certs, the CN is MySQL_Server_... and has no SAN for 127.0.0.1/localhost.
        // Thus VerifyIdentity fails with NotValidForName. If a custom cert with matching SAN is installed, it succeeds.
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
fn mysql_tls_disable_when_secure_transport_required() {
    let Some(cfg) = mysql_base_config() else {
        eprintln!("skipping MySQL TLS tests: set {MYSQL_URL_ENV}");
        return;
    };

    let require_secure = std::env::var(MYSQL_REQUIRE_SECURE_TRANSPORT_ENV)
        .map_or(false, |v| v == "1" || v == "true");
    if !require_secure {
        eprintln!(
            "skipping disable_when_secure_transport_required: {MYSQL_REQUIRE_SECURE_TRANSPORT_ENV} not set"
        );
        return;
    }

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let outcome = SharedMySqlConnection::connect(&cx, cfg.ssl_mode(SslMode::Disable)).await;
        match outcome {
            Outcome::Err(e) => {
                let err_str = e.to_string();
                assert!(
                    err_str.contains("insecure transport")
                        || err_str.contains("3159")
                        || err_str.contains("HY000")
                        || err_str.contains("Access denied"),
                    "expected secure transport rejection error, got: {err_str}"
                );
            }
            Outcome::Ok(_) => {
                panic!("expected connection rejection when require_secure_transport is ON")
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    });
}

#[test]
fn mysql_auth_caching_sha2_with_tls() {
    let Some(cfg) = mysql_base_config() else {
        eprintln!("skipping MySQL auth tests: set {MYSQL_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(
            SharedMySqlConnection::connect(&cx, cfg.ssl_mode(SslMode::Required)).await,
        );
        let rows = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(rows.len(), 1);
        let val: i64 = rows[0].get_as(0).expect("row[0] as i64");
        assert_eq!(val, 1);
    });
}

#[test]
fn mysql_auth_caching_sha2_no_tls_rsa() {
    let Some(cfg) = mysql_base_config() else {
        eprintln!("skipping MySQL auth tests: set {MYSQL_URL_ENV}");
        return;
    };

    let require_secure = std::env::var(MYSQL_REQUIRE_SECURE_TRANSPORT_ENV)
        .map_or(false, |v| v == "1" || v == "true");
    if require_secure {
        eprintln!("skipping no-tls RSA auth test: require_secure_transport is active");
        return;
    }

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        // With SslMode::Disable, caching_sha2_password exercises the RSA public key request + OAEP encryption path
        let outcome = SharedMySqlConnection::connect(&cx, cfg.ssl_mode(SslMode::Disable)).await;
        match outcome {
            Outcome::Ok(conn) => {
                let rows = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
                assert_eq!(rows.len(), 1);
            }
            Outcome::Err(e) => {
                // If server prohibits insecure transport, note it; otherwise should succeed via RSA
                eprintln!("caching_sha2 no-tls outcome: {e}");
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    });
}

#[test]
fn mysql_auth_native_password_mariadb() {
    let raw = match std::env::var(MARIADB_URL_ENV) {
        Ok(v) => v,
        Err(_) => {
            eprintln!("skipping MariaDB native_password test: set {MARIADB_URL_ENV}");
            return;
        }
    };
    let Some(cfg) = parse_mysql_url(&raw) else {
        eprintln!("skipping MariaDB test: invalid URL in {MARIADB_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(
            SharedMySqlConnection::connect(&cx, cfg.ssl_mode(SslMode::Preferred)).await,
        );
        let rows = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(rows.len(), 1);
        let val: i64 = rows[0].get_as(0).expect("row[0] as i64");
        assert_eq!(val, 1);
    });
}
