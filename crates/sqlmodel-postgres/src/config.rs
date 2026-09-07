//! PostgreSQL connection configuration.
//!
//! Provides connection parameters for establishing PostgreSQL connections
//! including authentication, SSL, and connection options.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

/// SSL mode for PostgreSQL connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    /// Do not use SSL
    #[default]
    Disable,
    /// Try SSL, fall back to non-SSL if unavailable
    Prefer,
    /// Require SSL connection
    Require,
    /// Require SSL and verify server certificate
    VerifyCa,
    /// Require SSL and verify server certificate matches hostname
    VerifyFull,
}

impl SslMode {
    /// Check if SSL should be attempted.
    pub const fn should_try_ssl(self) -> bool {
        !matches!(self, SslMode::Disable)
    }

    /// Check if SSL is required.
    pub const fn is_required(self) -> bool {
        matches!(
            self,
            SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull
        )
    }
}

/// PostgreSQL connection configuration.
#[derive(Debug, Clone)]
pub struct PgConfig {
    /// Hostname or IP address
    pub host: String,
    /// Port number (default: 5432)
    pub port: u16,
    /// Username for authentication
    pub user: String,
    /// Password for authentication (optional for trust auth)
    pub password: Option<String>,
    /// Database name to connect to
    pub database: String,
    /// Application name (visible in pg_stat_activity)
    pub application_name: Option<String>,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// SSL mode
    pub ssl_mode: SslMode,
    /// Path to custom root CA certificate file (PEM format) for SSL verification
    pub root_cert_path: Option<PathBuf>,
    /// Additional connection parameters
    pub options: HashMap<String, String>,
}

impl Default for PgConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            user: String::new(),
            password: None,
            database: String::new(),
            application_name: None,
            connect_timeout: Duration::from_secs(30),
            ssl_mode: SslMode::default(),
            root_cert_path: None,
            options: HashMap::new(),
        }
    }
}

impl PgConfig {
    /// Create a new configuration with the given connection string components.
    pub fn new(
        host: impl Into<String>,
        user: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            user: user.into(),
            database: database.into(),
            ..Default::default()
        }
    }

    /// Set the port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the password.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set the application name.
    pub fn application_name(mut self, name: impl Into<String>) -> Self {
        self.application_name = Some(name.into());
        self
    }

    /// Set the connection timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the SSL mode.
    pub fn ssl_mode(mut self, mode: SslMode) -> Self {
        self.ssl_mode = mode;
        self
    }

    /// Set custom root CA certificate path for SSL verification.
    pub fn root_cert_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.root_cert_path = Some(path.into());
        self
    }

    /// Alias for [`Self::root_cert_path`] matching PostgreSQL's `sslrootcert` option name.
    pub fn ssl_root_cert(self, path: impl Into<PathBuf>) -> Self {
        self.root_cert_path(path)
    }

    /// Set an additional connection option.
    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Build the startup parameters to send to the server.
    pub fn startup_params(&self) -> Vec<(String, String)> {
        let mut params = vec![
            ("user".to_string(), self.user.clone()),
            ("database".to_string(), self.database.clone()),
            ("client_encoding".to_string(), "UTF8".to_string()),
        ];

        if let Some(app_name) = &self.application_name {
            params.push(("application_name".to_string(), app_name.clone()));
        }

        for (k, v) in &self.options {
            params.push((k.clone(), v.clone()));
        }

        params
    }

    /// Get the socket address string for connection.
    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = PgConfig::new("localhost", "postgres", "testdb")
            .port(5433)
            .password("secret")
            .application_name("myapp")
            .connect_timeout(Duration::from_secs(10))
            .ssl_mode(SslMode::Prefer)
            .root_cert_path("/path/to/ca.pem")
            .option("timezone", "UTC");

        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5433);
        assert_eq!(config.user, "postgres");
        assert_eq!(config.database, "testdb");
        assert_eq!(config.password, Some("secret".to_string()));
        assert_eq!(config.application_name, Some("myapp".to_string()));
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.ssl_mode, SslMode::Prefer);
        assert_eq!(
            config.root_cert_path,
            Some(PathBuf::from("/path/to/ca.pem"))
        );
        assert_eq!(config.options.get("timezone"), Some(&"UTC".to_string()));

        let config2 = PgConfig::default().ssl_root_cert("/other/ca.pem");
        assert_eq!(config2.root_cert_path, Some(PathBuf::from("/other/ca.pem")));
    }

    #[test]
    fn test_startup_params() {
        let config = PgConfig::new("localhost", "postgres", "testdb")
            .application_name("myapp")
            .option("timezone", "UTC");

        let params = config.startup_params();

        assert!(params.iter().any(|(k, v)| k == "user" && v == "postgres"));
        assert!(params.iter().any(|(k, v)| k == "database" && v == "testdb"));
        assert!(
            params
                .iter()
                .any(|(k, v)| k == "client_encoding" && v == "UTF8")
        );
        assert!(
            params
                .iter()
                .any(|(k, v)| k == "application_name" && v == "myapp")
        );
        assert!(params.iter().any(|(k, v)| k == "timezone" && v == "UTC"));
    }

    #[test]
    fn test_socket_addr() {
        let config = PgConfig::new("db.example.com", "user", "db").port(5433);
        assert_eq!(config.socket_addr(), "db.example.com:5433");
    }

    #[test]
    fn test_ssl_mode_properties() {
        assert!(!SslMode::Disable.should_try_ssl());
        assert!(!SslMode::Disable.is_required());

        assert!(SslMode::Prefer.should_try_ssl());
        assert!(!SslMode::Prefer.is_required());

        assert!(SslMode::Require.should_try_ssl());
        assert!(SslMode::Require.is_required());

        assert!(SslMode::VerifyCa.should_try_ssl());
        assert!(SslMode::VerifyCa.is_required());

        assert!(SslMode::VerifyFull.should_try_ssl());
        assert!(SslMode::VerifyFull.is_required());
    }
}
