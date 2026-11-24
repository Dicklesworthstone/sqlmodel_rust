//! Shared environment-gate helpers for `sqlmodel-mysql` integration and TLS tests.
//!
//! Every live-database test in `mysql_integration` and `mysql_tls` is marked
//! `#[ignore = "..."]` so default `cargo test` runs report them honestly as
//! `ignored` rather than silently returning `ok`. Once explicitly invoked with
//! `--ignored` (or `--include-ignored`), missing or malformed prerequisites
//! fail immediately with an explicit diagnostic naming the required variable.

use std::env::VarError;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use sqlmodel_mysql::MySqlConfig;

pub const MYSQL_URL_ENV: &str = "SQLMODEL_TEST_MYSQL_URL";
#[allow(dead_code)] // Used by sibling test binaries (`mysql_tls`, `env_gate_unit`).
pub const MYSQL_CA_ENV: &str = "SQLMODEL_TEST_MYSQL_CA";
#[allow(dead_code)] // Used by sibling test binaries (`mysql_tls`, `env_gate_unit`).
pub const MYSQL_REQUIRE_SECURE_TRANSPORT_ENV: &str = "SQLMODEL_TEST_MYSQL_REQUIRE_SECURE_TRANSPORT";
#[allow(dead_code)] // Used by sibling test binaries (`mysql_tls`, `env_gate_unit`).
pub const MARIADB_URL_ENV: &str = "SQLMODEL_TEST_MARIADB_URL";
#[allow(dead_code)] // Used by sibling test binaries (`mysql_tls`, `env_gate_unit`).
pub const MYSQL_CA_FALLBACKS: [&str; 2] = ["/tmp/mysql-ca.pem", "mysql-ca.pem"];

/// Diagnostic for a missing or invalid integration/TLS test prerequisite.
#[derive(Clone, PartialEq, Eq)]
pub struct PrerequisiteError(String);

impl PrerequisiteError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    #[allow(dead_code)] // Used by `env_gate_unit`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PrerequisiteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for PrerequisiteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PrerequisiteError {}

pub fn parse_mysql_url(url: &str) -> Option<MySqlConfig> {
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
    if input.is_empty() {
        return None;
    }
    if let Some(rest) = input.strip_prefix('[') {
        let end = rest.find(']')?;
        let host = &rest[..end];
        if host.is_empty() || host.chars().any(char::is_whitespace) {
            return None;
        }
        let after = &rest[end + 1..];
        let port = if after.is_empty() {
            3306
        } else {
            let port_str = after.strip_prefix(':')?;
            if port_str.is_empty() || !port_str.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let parsed = port_str.parse::<u16>().ok()?;
            if parsed == 0 {
                return None;
            }
            parsed
        };
        return Some((host, port));
    }

    if input.contains('[') || input.contains(']') {
        return None;
    }

    if let Some((host, port_str)) = input.split_once(':') {
        if host.is_empty()
            || host.chars().any(char::is_whitespace)
            || port_str.is_empty()
            || !port_str.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let port = port_str.parse::<u16>().ok()?;
        if port == 0 {
            return None;
        }
        Some((host, port))
    } else {
        if input.chars().any(char::is_whitespace) {
            return None;
        }
        Some((input, 3306))
    }
}

pub fn resolve_mysql_config_from_env(
    raw: Result<String, VarError>,
    env_name: &str,
    require_database: bool,
) -> Result<MySqlConfig, PrerequisiteError> {
    let raw = match raw {
        Ok(value) => value,
        Err(VarError::NotPresent) => {
            return Err(PrerequisiteError::new(format!(
                "{env_name} is not set (expected mysql://user:pass@host:3306/db)"
            )));
        }
        Err(VarError::NotUnicode(_)) => {
            return Err(PrerequisiteError::new(format!(
                "{env_name} is not valid UTF-8"
            )));
        }
    };

    let Some(cfg) = parse_mysql_url(&raw) else {
        return Err(PrerequisiteError::new(format!(
            "{env_name} is malformed or empty (expected mysql://user:pass@host:3306/db)"
        )));
    };

    if require_database && cfg.database.is_none() {
        return Err(PrerequisiteError::new(format!(
            "{env_name} must include a database name (mysql://user:pass@host:3306/db)"
        )));
    }

    Ok(cfg.connect_timeout(Duration::from_secs(10)))
}

#[allow(dead_code)] // Used by sibling test binaries (`mysql_tls`, `env_gate_unit`).
pub fn resolve_ca_path_from_env(
    raw: Result<String, VarError>,
    env_name: &str,
    fallbacks: &[&Path],
) -> Result<PathBuf, PrerequisiteError> {
    match raw {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Err(PrerequisiteError::new(format!(
                    "{env_name} is set but empty; set it to a readable CA certificate path"
                )));
            }
            let path = PathBuf::from(trimmed);
            if path.is_file() {
                Ok(path)
            } else {
                Err(PrerequisiteError::new(format!(
                    "{env_name} points to {}, which does not exist or is not a file",
                    path.display()
                )))
            }
        }
        Err(VarError::NotUnicode(_)) => Err(PrerequisiteError::new(format!(
            "{env_name} is not valid UTF-8"
        ))),
        Err(VarError::NotPresent) => {
            for fallback in fallbacks {
                if fallback.is_file() {
                    return Ok((*fallback).to_path_buf());
                }
            }
            let checked = fallbacks
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            Err(PrerequisiteError::new(format!(
                "{env_name} is not set and no fallback CA certificate was found at [{checked}]"
            )))
        }
    }
}

#[allow(dead_code)] // Used by sibling test binaries (`mysql_tls`, `env_gate_unit`).
pub fn resolve_fixture_path(
    manifest_dir: &Path,
    filename: &str,
) -> Result<PathBuf, PrerequisiteError> {
    let path = manifest_dir.join("tests").join("fixtures").join(filename);
    if path.is_file() {
        Ok(path)
    } else {
        Err(PrerequisiteError::new(format!(
            "required test fixture missing at {}",
            path.display()
        )))
    }
}

#[allow(dead_code)] // Used by sibling test binaries (`mysql_tls`, `env_gate_unit`).
pub fn env_flag_is_true(raw: &Result<String, VarError>) -> bool {
    raw.as_ref().is_ok_and(|v| matches!(v.trim(), "1" | "true"))
}

#[allow(dead_code)] // Used by sibling test binaries (`mysql_tls`, `env_gate_unit`).
pub fn check_secure_transport_mode(
    raw: &Result<String, VarError>,
    expect_enabled: bool,
) -> Result<(), PrerequisiteError> {
    let enabled = env_flag_is_true(raw);
    if expect_enabled && !enabled {
        return Err(PrerequisiteError::new(format!(
            "{MYSQL_REQUIRE_SECURE_TRANSPORT_ENV} must be set to 1 or true for this test \
             (pass `--skip mysql_tls_disable_when_secure_transport_required` when testing a \
             server with require_secure_transport=OFF)"
        )));
    }
    if !expect_enabled && enabled {
        return Err(PrerequisiteError::new(format!(
            "{MYSQL_REQUIRE_SECURE_TRANSPORT_ENV} is active (1/true), so the server prohibits \
             non-TLS RSA auth (pass `--skip mysql_auth_caching_sha2_no_tls_rsa` when testing a \
             server with require_secure_transport=ON)"
        )));
    }
    Ok(())
}

#[allow(dead_code)] // Used by `mysql_integration` and `mysql_tls`.
pub fn require_mysql_config() -> MySqlConfig {
    resolve_mysql_config_from_env(std::env::var(MYSQL_URL_ENV), MYSQL_URL_ENV, true)
        .expect("MySQL test prerequisite missing")
}

#[allow(dead_code)] // Used by `mysql_tls`.
pub fn require_mariadb_config() -> MySqlConfig {
    resolve_mysql_config_from_env(std::env::var(MARIADB_URL_ENV), MARIADB_URL_ENV, false)
        .expect("MariaDB test prerequisite missing")
}

#[allow(dead_code)] // Used by `mysql_tls`.
pub fn require_mysql_ca_path() -> PathBuf {
    let fallbacks = [
        Path::new(MYSQL_CA_FALLBACKS[0]),
        Path::new(MYSQL_CA_FALLBACKS[1]),
    ];
    resolve_ca_path_from_env(std::env::var(MYSQL_CA_ENV), MYSQL_CA_ENV, &fallbacks)
        .expect("MySQL TLS test prerequisite missing")
}

#[allow(dead_code)] // Used by `mysql_tls`.
pub fn require_fixture_path(filename: &str) -> PathBuf {
    resolve_fixture_path(Path::new(env!("CARGO_MANIFEST_DIR")), filename)
        .expect("MySQL TLS test prerequisite missing")
}

#[allow(dead_code)] // Used by `mysql_tls`.
pub fn require_secure_transport_enabled() {
    check_secure_transport_mode(&std::env::var(MYSQL_REQUIRE_SECURE_TRANSPORT_ENV), true)
        .expect("MySQL TLS test prerequisite missing");
}

#[allow(dead_code)] // Used by `mysql_tls`.
pub fn require_secure_transport_disabled() {
    check_secure_transport_mode(&std::env::var(MYSQL_REQUIRE_SECURE_TRANSPORT_ENV), false)
        .expect("MySQL TLS test prerequisite missing");
}
