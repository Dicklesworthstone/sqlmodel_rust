//! Unit tests for the `sqlmodel-postgres` integration/TLS environment-gate helper.
//!
//! Kept in a separate test binary so `postgres_integration` and `postgres_tls`
//! report `0 passed` when no database server environment variables are configured.

mod common;

use std::env::VarError;
use std::path::Path;

#[test]
fn postgres_url_missing_errors_naming_env_var() {
    let err = common::resolve_postgres_config_from_env(
        Err(VarError::NotPresent),
        common::POSTGRES_URL_ENV,
    )
    .expect_err("missing env var must error, not skip");
    assert!(
        err.as_str().contains(common::POSTGRES_URL_ENV),
        "unexpected error: {err}"
    );
}

#[test]
fn postgres_url_malformed_errors_without_leaking_password() {
    let marker = "redacted-credential-marker-554433";
    let err = common::resolve_postgres_config_from_env(
        Ok(format!("mysql://user:{marker}@127.0.0.1:5432/db")),
        common::POSTGRES_URL_ENV,
    )
    .expect_err("wrong scheme must error");
    assert!(
        err.as_str().contains(common::POSTGRES_URL_ENV),
        "unexpected error: {err}"
    );
    assert!(
        !err.as_str().contains(marker),
        "error message must not echo raw credentials: {err}"
    );
}

#[test]
fn postgres_url_missing_database_errors() {
    let err = common::resolve_postgres_config_from_env(
        Ok("postgres://sqlmodel:sqlmodel@127.0.0.1:5432/".to_owned()),
        common::POSTGRES_URL_ENV,
    )
    .expect_err("empty database name must error");
    assert!(
        err.as_str().contains("database name"),
        "unexpected error: {err}"
    );
}

#[test]
fn postgres_url_valid_parses_both_schemes() {
    for scheme in ["postgres", "postgresql"] {
        let cfg = common::resolve_postgres_config_from_env(
            Ok(format!(
                "{scheme}://sqlmodel:pw@127.0.0.1:5432/sqlmodel_test?sslmode=disable"
            )),
            common::POSTGRES_URL_ENV,
        )
        .expect("valid URL should parse");
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.user, "sqlmodel");
        assert_eq!(cfg.database, "sqlmodel_test");
    }
}

#[test]
fn postgres_ca_path_missing_env_and_fallbacks_errors_naming_env() {
    let missing = [
        Path::new("/nonexistent/sqlmodel-pg-ca-1.crt"),
        Path::new("/nonexistent/sqlmodel-pg-ca-2.crt"),
    ];
    let err = common::resolve_ca_path_from_env(
        Err(VarError::NotPresent),
        common::POSTGRES_CA_ENV,
        &missing,
    )
    .expect_err("unset CA env without fallback must error");
    assert!(
        err.as_str().contains(common::POSTGRES_CA_ENV),
        "unexpected error: {err}"
    );
}

#[test]
fn postgres_ca_path_explicit_nonexistent_does_not_silently_fallback() {
    let fixture =
        common::resolve_fixture_path(Path::new(env!("CARGO_MANIFEST_DIR")), "untrusted_ca.crt")
            .expect("checked-in fixture exists");
    let err = common::resolve_ca_path_from_env(
        Ok("/nonexistent/explicit-pg-ca.crt".to_owned()),
        common::POSTGRES_CA_ENV,
        &[fixture.as_path()],
    )
    .expect_err("broken explicit CA path must not silently fall back");
    assert!(
        err.as_str().contains(common::POSTGRES_CA_ENV),
        "unexpected error: {err}"
    );
    assert!(
        err.as_str().contains("/nonexistent/explicit-pg-ca.crt"),
        "error should name broken path: {err}"
    );
}

#[test]
fn postgres_ca_path_explicit_and_fallback_succeed_when_file_exists() {
    let fixture =
        common::resolve_fixture_path(Path::new(env!("CARGO_MANIFEST_DIR")), "untrusted_ca.crt")
            .expect("checked-in fixture exists");

    let explicit = common::resolve_ca_path_from_env(
        Ok(fixture.display().to_string()),
        common::POSTGRES_CA_ENV,
        &[],
    )
    .expect("explicit existing CA file must succeed");
    assert_eq!(explicit, fixture);

    let fallback = common::resolve_ca_path_from_env(
        Err(VarError::NotPresent),
        common::POSTGRES_CA_ENV,
        &[fixture.as_path()],
    )
    .expect("existing fallback CA file must succeed when env unset");
    assert_eq!(fallback, fixture);
}

#[test]
fn postgres_require_ssl_mode_enforces_precondition() {
    assert!(common::check_require_ssl_enabled(&Ok("1".to_owned())).is_ok());
    assert!(common::check_require_ssl_enabled(&Ok("true".to_owned())).is_ok());

    for val in [
        Err(VarError::NotPresent),
        Ok("0".to_owned()),
        Ok("false".to_owned()),
    ] {
        let err = common::check_require_ssl_enabled(&val)
            .expect_err("non-enabled SSL requirement must error");
        assert!(err.as_str().contains(common::POSTGRES_REQUIRE_SSL_ENV));
    }
}

#[test]
fn postgres_fixture_path_errors_when_missing() {
    let err = common::resolve_fixture_path(
        Path::new(env!("CARGO_MANIFEST_DIR")),
        "does_not_exist_ca.crt",
    )
    .expect_err("missing fixture must error");
    assert!(
        err.as_str().contains("does_not_exist_ca.crt"),
        "unexpected error: {err}"
    );
}

#[test]
fn postgres_url_ipv6_and_malformed_host_port_validation() {
    let ipv6_default = common::resolve_postgres_config_from_env(
        Ok("postgres://sqlmodel:pw@[::1]/sqlmodel_test".to_owned()),
        common::POSTGRES_URL_ENV,
    )
    .expect("valid IPv6 URL with default port should parse");
    assert_eq!(ipv6_default.host, "::1");
    assert_eq!(ipv6_default.port, 5432);

    let ipv6_explicit = common::resolve_postgres_config_from_env(
        Ok("postgresql://sqlmodel:pw@[::1]:5433/sqlmodel_test".to_owned()),
        common::POSTGRES_URL_ENV,
    )
    .expect("valid IPv6 URL with explicit port should parse");
    assert_eq!(ipv6_explicit.host, "::1");
    assert_eq!(ipv6_explicit.port, 5433);

    let default_port = common::resolve_postgres_config_from_env(
        Ok("postgres://sqlmodel:pw@localhost/sqlmodel_test".to_owned()),
        common::POSTGRES_URL_ENV,
    )
    .expect("valid hostname URL without port should use default 5432");
    assert_eq!(default_port.host, "localhost");
    assert_eq!(default_port.port, 5432);

    for bad_url in [
        "postgres://u:p@[::1]:abc/db",
        "postgres://u:p@[::1]:/db",
        "postgres://u:p@[::1]:99999/db",
        "postgres://u:p@[::1]:0/db",
        "postgres://u:p@[::1]:+5432/db",
        "postgres://u:p@[::1]junk/db",
        "postgres://u:p@[]/db",
        "postgres://u:p@[]:5432/db",
        "postgres://u:p@[::1/db",
        "postgres://u:p@/db",
        "postgres://u:p@:5432/db",
        "postgres://u:p@localhost:notaport/db",
        "postgres://u:p@localhost:/db",
        "postgres://u:p@localhost:99999/db",
        "postgres://u:p@localhost:0/db",
        "postgres://u:p@localhost:+5432/db",
        "postgres://u:p@localhost:5432:extra/db",
        "postgres://u:p@::1/db",
        "postgres://u:p@bad host:5432/db",
    ] {
        assert!(
            common::parse_postgres_url(bad_url).is_none(),
            "malformed URL must be rejected: {bad_url}"
        );
        let err = common::resolve_postgres_config_from_env(
            Ok(bad_url.to_owned()),
            common::POSTGRES_URL_ENV,
        )
        .expect_err("malformed URL must return Err");
        assert!(
            err.as_str().contains(common::POSTGRES_URL_ENV),
            "error for {bad_url} must name env var: {err}"
        );
    }
}
