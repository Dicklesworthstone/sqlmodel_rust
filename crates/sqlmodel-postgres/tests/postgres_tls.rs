#![cfg(feature = "tls")]

mod common;

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};

use sqlmodel_core::{Connection, Error};
use sqlmodel_postgres::{SharedPgConnection, SslMode};

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
#[ignore = "requires SQLMODEL_TEST_POSTGRES_URL (run with -- --ignored)"]
fn postgres_tls_prefer_connect() {
    let cfg = common::require_postgres_tls_config();

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
#[ignore = "requires SQLMODEL_TEST_POSTGRES_URL (run with -- --ignored)"]
fn postgres_tls_require_connect() {
    let cfg = common::require_postgres_tls_config();

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
#[ignore = "requires SQLMODEL_TEST_POSTGRES_URL and SQLMODEL_TEST_POSTGRES_CA (run with -- --ignored)"]
fn postgres_tls_verify_ca_success() {
    let cfg = common::require_postgres_tls_config();
    let ca_path = common::require_postgres_ca_path();

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
#[ignore = "requires SQLMODEL_TEST_POSTGRES_URL (run with -- --ignored)"]
fn postgres_tls_verify_ca_rejects_untrusted_ca() {
    let cfg = common::require_postgres_tls_config();
    let untrusted_ca_path = common::require_fixture_path("untrusted_ca.crt");

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
#[ignore = "requires SQLMODEL_TEST_POSTGRES_URL and SQLMODEL_TEST_POSTGRES_CA (run with -- --ignored)"]
fn postgres_tls_verify_full_connect() {
    let cfg = common::require_postgres_tls_config();
    let ca_path = common::require_postgres_ca_path();

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
#[ignore = "requires SQLMODEL_TEST_POSTGRES_URL and SQLMODEL_TEST_POSTGRES_REQUIRE_SSL=1 (run with -- --ignored)"]
fn postgres_tls_disable_when_ssl_required() {
    let cfg = common::require_postgres_tls_config();
    common::require_postgres_ssl_enabled();

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
