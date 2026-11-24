//! Shared environment-gate helpers for `sqlmodel-postgres` integration and TLS tests.
//!
//! Every live-database test in `postgres_integration` and `postgres_tls` is marked
//! `#[ignore = "..."]` so default `cargo test` runs report them honestly as
//! `ignored` rather than silently returning `ok`. Once explicitly invoked with
//! `--ignored` (or `--include-ignored`), missing or malformed prerequisites
//! fail immediately with an explicit diagnostic naming the required variable.

use std::env::VarError;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use sqlmodel_postgres::{PgConfig, SslMode};

pub const POSTGRES_URL_ENV: &str = "SQLMODEL_TEST_POSTGRES_URL";
#[allow(dead_code)] // Used by sibling test binaries (`postgres_tls`, `env_gate_unit`).
pub const POSTGRES_CA_ENV: &str = "SQLMODEL_TEST_POSTGRES_CA";
#[allow(dead_code)] // Used by sibling test binaries (`postgres_tls`, `env_gate_unit`).
pub const POSTGRES_REQUIRE_SSL_ENV: &str = "SQLMODEL_TEST_POSTGRES_REQUIRE_SSL";
#[allow(dead_code)] // Used by sibling test binaries (`postgres_tls`, `env_gate_unit`).
pub const POSTGRES_CA_FALLBACKS: [&str; 2] = ["/tmp/pg-test-certs/ca.crt", "pg-ca.crt"];

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

pub fn parse_postgres_url(url: &str) -> Option<PgConfig> {
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
            5432
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
        Some((input, 5432))
    }
}

pub fn resolve_postgres_config_from_env(
    raw: Result<String, VarError>,
    env_name: &str,
) -> Result<PgConfig, PrerequisiteError> {
    let raw = match raw {
        Ok(value) => value,
        Err(VarError::NotPresent) => {
            return Err(PrerequisiteError::new(format!(
                "{env_name} is not set (expected postgres://user:pass@host:5432/db)"
            )));
        }
        Err(VarError::NotUnicode(_)) => {
            return Err(PrerequisiteError::new(format!(
                "{env_name} is not valid UTF-8"
            )));
        }
    };

    let Some(cfg) = parse_postgres_url(&raw) else {
        return Err(PrerequisiteError::new(format!(
            "{env_name} is malformed or empty (expected postgres://user:pass@host:5432/db)"
        )));
    };

    if cfg.database.is_empty() {
        return Err(PrerequisiteError::new(format!(
            "{env_name} must include a database name (postgres://user:pass@host:5432/db)"
        )));
    }

    Ok(cfg.connect_timeout(Duration::from_secs(10)))
}

#[allow(dead_code)] // Used by sibling test binaries (`postgres_tls`, `env_gate_unit`).
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

#[allow(dead_code)] // Used by sibling test binaries (`postgres_tls`, `env_gate_unit`).
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

#[allow(dead_code)] // Used by sibling test binaries (`postgres_tls`, `env_gate_unit`).
pub fn check_require_ssl_enabled(raw: &Result<String, VarError>) -> Result<(), PrerequisiteError> {
    let enabled = raw.as_ref().is_ok_and(|v| matches!(v.trim(), "1" | "true"));
    if !enabled {
        return Err(PrerequisiteError::new(format!(
            "{POSTGRES_REQUIRE_SSL_ENV} must be set to 1 or true for this test \
             (pass `--skip postgres_tls_disable_when_ssl_required` when testing a server \
             that allows non-SSL connections)"
        )));
    }
    Ok(())
}

#[allow(dead_code)] // Used by `postgres_integration`.
pub fn require_postgres_integration_config() -> PgConfig {
    resolve_postgres_config_from_env(std::env::var(POSTGRES_URL_ENV), POSTGRES_URL_ENV)
        .expect("PostgreSQL integration test prerequisite missing")
        .ssl_mode(SslMode::Disable)
}

#[allow(dead_code)] // Used by `postgres_tls`.
pub fn require_postgres_tls_config() -> PgConfig {
    resolve_postgres_config_from_env(std::env::var(POSTGRES_URL_ENV), POSTGRES_URL_ENV)
        .expect("PostgreSQL TLS test prerequisite missing")
}

#[allow(dead_code)] // Used by `postgres_tls`.
pub fn require_postgres_ca_path() -> PathBuf {
    let fallbacks = [
        Path::new(POSTGRES_CA_FALLBACKS[0]),
        Path::new(POSTGRES_CA_FALLBACKS[1]),
    ];
    resolve_ca_path_from_env(std::env::var(POSTGRES_CA_ENV), POSTGRES_CA_ENV, &fallbacks)
        .expect("PostgreSQL TLS test prerequisite missing")
}

#[allow(dead_code)] // Used by `postgres_tls`.
pub fn require_fixture_path(filename: &str) -> PathBuf {
    resolve_fixture_path(Path::new(env!("CARGO_MANIFEST_DIR")), filename)
        .expect("PostgreSQL TLS test prerequisite missing")
}

#[allow(dead_code)] // Used by `postgres_tls`.
pub fn require_postgres_ssl_enabled() {
    check_require_ssl_enabled(&std::env::var(POSTGRES_REQUIRE_SSL_ENV))
        .expect("PostgreSQL TLS test prerequisite missing");
}
