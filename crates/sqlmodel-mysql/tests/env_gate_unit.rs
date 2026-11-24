//! Unit tests for the `sqlmodel-mysql` integration/TLS environment-gate helper.
//!
//! Kept in a separate test binary so `mysql_integration` and `mysql_tls` report
//! `0 passed` when no database server environment variables are configured.

mod common;

use std::env::VarError;
use std::path::Path;

#[test]
fn mysql_url_missing_errors_naming_env_var() {
    let err = common::resolve_mysql_config_from_env(
        Err(VarError::NotPresent),
        common::MYSQL_URL_ENV,
        true,
    )
    .expect_err("missing env var must error, not skip");
    assert!(
        err.as_str().contains(common::MYSQL_URL_ENV),
        "unexpected error: {err}"
    );
}

#[test]
fn mysql_url_malformed_errors_without_leaking_password() {
    let marker = "redacted-credential-marker-998877";
    let err = common::resolve_mysql_config_from_env(
        Ok(format!("postgres://user:{marker}@127.0.0.1:3306/db")),
        common::MYSQL_URL_ENV,
        true,
    )
    .expect_err("wrong scheme must error");
    assert!(
        err.as_str().contains(common::MYSQL_URL_ENV),
        "unexpected error: {err}"
    );
    assert!(
        !err.as_str().contains(marker),
        "error message must not echo raw credentials: {err}"
    );
}

#[test]
fn mysql_url_missing_database_enforced_when_required() {
    let err = common::resolve_mysql_config_from_env(
        Ok("mysql://root:pass@127.0.0.1:3306".to_owned()),
        common::MYSQL_URL_ENV,
        true,
    )
    .expect_err("missing db must error when required");
    assert!(
        err.as_str().contains("database name"),
        "unexpected error: {err}"
    );

    let cfg = common::resolve_mysql_config_from_env(
        Ok("mysql://root:pass@127.0.0.1:3307".to_owned()),
        common::MARIADB_URL_ENV,
        false,
    )
    .expect("mariadb config without database should succeed when optional");
    assert_eq!(cfg.host, "127.0.0.1");
    assert_eq!(cfg.port, 3307);
    assert_eq!(cfg.database, None);
}

#[test]
fn mysql_url_valid_parses_fields() {
    let cfg = common::resolve_mysql_config_from_env(
        Ok("mysql://root:pw@127.0.0.1:3306/sqlmodel_test?ssl=1".to_owned()),
        common::MYSQL_URL_ENV,
        true,
    )
    .expect("valid URL should parse");
    assert_eq!(cfg.host, "127.0.0.1");
    assert_eq!(cfg.port, 3306);
    assert_eq!(cfg.user, "root");
    assert_eq!(cfg.database.as_deref(), Some("sqlmodel_test"));
}

#[test]
fn mysql_ca_path_missing_env_and_fallbacks_errors_naming_env() {
    let missing = [
        Path::new("/nonexistent/sqlmodel-test-ca-1.pem"),
        Path::new("/nonexistent/sqlmodel-test-ca-2.pem"),
    ];
    let err =
        common::resolve_ca_path_from_env(Err(VarError::NotPresent), common::MYSQL_CA_ENV, &missing)
            .expect_err("unset CA env without fallback must error");
    assert!(
        err.as_str().contains(common::MYSQL_CA_ENV),
        "unexpected error: {err}"
    );
}

#[test]
fn mysql_ca_path_explicit_nonexistent_does_not_silently_fallback() {
    let fixture = common::resolve_fixture_path(&common::manifest_dir(), "untrusted_ca.crt")
        .expect("checked-in fixture exists");
    let err = common::resolve_ca_path_from_env(
        Ok("/nonexistent/explicit-mysql-ca.pem".to_owned()),
        common::MYSQL_CA_ENV,
        &[fixture.as_path()],
    )
    .expect_err("broken explicit CA path must not silently fall back");
    assert!(
        err.as_str().contains(common::MYSQL_CA_ENV),
        "unexpected error: {err}"
    );
    assert!(
        err.as_str().contains("/nonexistent/explicit-mysql-ca.pem"),
        "error should name broken path: {err}"
    );
}

#[test]
fn mysql_ca_path_explicit_and_fallback_succeed_when_file_exists() {
    let fixture = common::resolve_fixture_path(&common::manifest_dir(), "untrusted_ca.crt")
        .expect("checked-in fixture exists");

    let explicit = common::resolve_ca_path_from_env(
        Ok(fixture.display().to_string()),
        common::MYSQL_CA_ENV,
        &[],
    )
    .expect("explicit existing CA file must succeed");
    assert_eq!(explicit, fixture);

    let fallback = common::resolve_ca_path_from_env(
        Err(VarError::NotPresent),
        common::MYSQL_CA_ENV,
        &[fixture.as_path()],
    )
    .expect("existing fallback CA file must succeed when env unset");
    assert_eq!(fallback, fixture);
}

#[test]
fn mysql_secure_transport_mode_enforces_preconditions() {
    assert!(common::check_secure_transport_mode(&Ok("1".to_owned()), true).is_ok());
    assert!(common::check_secure_transport_mode(&Ok("true".to_owned()), true).is_ok());

    let err_need_on = common::check_secure_transport_mode(&Ok("0".to_owned()), true)
        .expect_err("expect_enabled=true must reject 0");
    assert!(
        err_need_on
            .as_str()
            .contains(common::MYSQL_REQUIRE_SECURE_TRANSPORT_ENV)
    );

    let err_need_off = common::check_secure_transport_mode(&Ok("1".to_owned()), false)
        .expect_err("expect_enabled=false must reject 1");
    assert!(
        err_need_off
            .as_str()
            .contains(common::MYSQL_REQUIRE_SECURE_TRANSPORT_ENV)
    );

    assert!(common::check_secure_transport_mode(&Err(VarError::NotPresent), false).is_ok());
    assert!(common::check_secure_transport_mode(&Ok("0".to_owned()), false).is_ok());
}

#[test]
fn mysql_fixture_path_errors_when_missing() {
    let err = common::resolve_fixture_path(&common::manifest_dir(), "does_not_exist_ca.crt")
        .expect_err("missing fixture must error");
    assert!(
        err.as_str().contains("does_not_exist_ca.crt"),
        "unexpected error: {err}"
    );
}

#[test]
fn mysql_url_ipv6_and_malformed_host_port_validation() {
    let ipv6_default = common::resolve_mysql_config_from_env(
        Ok("mysql://root:pw@[::1]/sqlmodel_test".to_owned()),
        common::MYSQL_URL_ENV,
        true,
    )
    .expect("valid IPv6 URL with default port should parse");
    assert_eq!(ipv6_default.host, "::1");
    assert_eq!(ipv6_default.port, 3306);

    let ipv6_explicit = common::resolve_mysql_config_from_env(
        Ok("mysql://root:pw@[::1]:3307/sqlmodel_test".to_owned()),
        common::MYSQL_URL_ENV,
        true,
    )
    .expect("valid IPv6 URL with explicit port should parse");
    assert_eq!(ipv6_explicit.host, "::1");
    assert_eq!(ipv6_explicit.port, 3307);

    let default_port = common::resolve_mysql_config_from_env(
        Ok("mysql://root:pw@localhost/sqlmodel_test".to_owned()),
        common::MYSQL_URL_ENV,
        true,
    )
    .expect("valid hostname URL without port should use default 3306");
    assert_eq!(default_port.host, "localhost");
    assert_eq!(default_port.port, 3306);

    for bad_url in [
        "mysql://root:pw@[::1]:abc/db",
        "mysql://root:pw@[::1]:/db",
        "mysql://root:pw@[::1]:99999/db",
        "mysql://root:pw@[::1]:0/db",
        "mysql://root:pw@[::1]:+3306/db",
        "mysql://root:pw@[::1]junk/db",
        "mysql://root:pw@[]/db",
        "mysql://root:pw@[]:3306/db",
        "mysql://root:pw@[::1/db",
        "mysql://root:pw@/db",
        "mysql://root:pw@:3306/db",
        "mysql://root:pw@localhost:notaport/db",
        "mysql://root:pw@localhost:/db",
        "mysql://root:pw@localhost:99999/db",
        "mysql://root:pw@localhost:0/db",
        "mysql://root:pw@localhost:+3306/db",
        "mysql://root:pw@localhost:3306:extra/db",
        "mysql://root:pw@::1/db",
        "mysql://root:pw@bad host:3306/db",
    ] {
        assert!(
            common::parse_mysql_url(bad_url).is_none(),
            "malformed URL must be rejected: {bad_url}"
        );
        let err = common::resolve_mysql_config_from_env(
            Ok(bad_url.to_owned()),
            common::MYSQL_URL_ENV,
            true,
        )
        .expect_err("malformed URL must return Err");
        assert!(
            err.as_str().contains(common::MYSQL_URL_ENV),
            "error for {bad_url} must name env var: {err}"
        );
    }
}

#[test]
fn mysql_manifest_dir_prefers_runtime_override_and_falls_back() {
    let runtime = common::resolve_manifest_dir(
        Some(std::ffi::OsString::from("/custom/runtime/crate")),
        "/compile/time/crate",
    );
    assert_eq!(runtime, Path::new("/custom/runtime/crate"));

    let fallback_none = common::resolve_manifest_dir(None, "/compile/time/crate");
    assert_eq!(fallback_none, Path::new("/compile/time/crate"));

    let fallback_blank =
        common::resolve_manifest_dir(Some(std::ffi::OsString::from("   ")), "/compile/time/crate");
    assert_eq!(fallback_blank, Path::new("/compile/time/crate"));
}
