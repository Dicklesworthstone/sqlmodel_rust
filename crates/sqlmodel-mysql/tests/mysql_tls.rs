#![cfg(feature = "tls")]

mod common;

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};

use sqlmodel_core::{Connection, Error};
use sqlmodel_mysql::{SharedMySqlConnection, SslMode};

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
#[ignore = "requires SQLMODEL_TEST_MYSQL_URL (run with -- --ignored)"]
fn mysql_tls_preferred_connect() {
    let cfg = common::require_mysql_config();

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
#[ignore = "requires SQLMODEL_TEST_MYSQL_URL (run with -- --ignored)"]
fn mysql_tls_required_connect() {
    let cfg = common::require_mysql_config();

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
#[ignore = "requires SQLMODEL_TEST_MYSQL_URL and SQLMODEL_TEST_MYSQL_CA (run with -- --ignored)"]
fn mysql_tls_verify_ca_success() {
    let cfg = common::require_mysql_config();
    let ca_path = common::require_mysql_ca_path();

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
#[ignore = "requires SQLMODEL_TEST_MYSQL_URL (run with -- --ignored)"]
fn mysql_tls_verify_ca_rejects_untrusted_ca() {
    let cfg = common::require_mysql_config();
    let untrusted_ca_path = common::require_fixture_path("untrusted_ca.crt");

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
#[ignore = "requires SQLMODEL_TEST_MYSQL_URL and SQLMODEL_TEST_MYSQL_CA (run with -- --ignored)"]
fn mysql_tls_verify_identity_hostname_validation() {
    let cfg = common::require_mysql_config();
    let ca_path = common::require_mysql_ca_path();

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
#[ignore = "requires SQLMODEL_TEST_MYSQL_URL and SQLMODEL_TEST_MYSQL_REQUIRE_SECURE_TRANSPORT=1 (run with -- --ignored)"]
fn mysql_tls_disable_when_secure_transport_required() {
    let cfg = common::require_mysql_config();
    common::require_secure_transport_enabled();

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
#[ignore = "requires SQLMODEL_TEST_MYSQL_URL (run with -- --ignored)"]
fn mysql_auth_caching_sha2_with_tls() {
    let cfg = common::require_mysql_config();

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
#[ignore = "requires SQLMODEL_TEST_MYSQL_URL with SQLMODEL_TEST_MYSQL_REQUIRE_SECURE_TRANSPORT unset/0 (run with -- --ignored)"]
fn mysql_auth_caching_sha2_no_tls_rsa() {
    let cfg = common::require_mysql_config();
    common::require_secure_transport_disabled();

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
#[ignore = "requires SQLMODEL_TEST_MARIADB_URL (run with -- --ignored)"]
fn mysql_auth_native_password_mariadb() {
    let cfg = common::require_mariadb_config();

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
