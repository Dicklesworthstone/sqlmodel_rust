//! MySQL connection configuration.
//!
//! Provides connection parameters for establishing MySQL connections
//! including authentication, SSL, and connection options.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

/// TLS/SSL configuration for MySQL connections.
///
/// This struct holds the certificate and key paths for TLS connections.
/// The actual TLS implementation requires the `tls` feature to be enabled.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Path to CA certificate file (PEM format) for server verification.
    /// Required for `SslMode::VerifyCa` and `SslMode::VerifyIdentity`.
    pub ca_cert_path: Option<PathBuf>,

    /// Path to client certificate file (PEM format) for mutual TLS.
    /// Optional - only needed if server requires client certificate.
    pub client_cert_path: Option<PathBuf>,

    /// Path to client private key file (PEM format) for mutual TLS.
    /// Required if `client_cert_path` is set.
    pub client_key_path: Option<PathBuf>,

    /// Skip server certificate verification.
    ///
    /// # Security Warning
    /// Setting this to `true` disables certificate verification, making the
    /// connection vulnerable to man-in-the-middle attacks. Only use for
    /// development/testing with self-signed certificates.
    pub danger_skip_verify: bool,

    /// Server name for SNI (Server Name Indication).
    /// If not set, defaults to the connection hostname.
    pub server_name: Option<String>,
}

impl TlsConfig {
    /// Create a new TLS configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the CA certificate path.
    pub fn ca_cert(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca_cert_path = Some(path.into());
        self
    }

    /// Set the client certificate path.
    pub fn client_cert(mut self, path: impl Into<PathBuf>) -> Self {
        self.client_cert_path = Some(path.into());
        self
    }

    /// Set the client key path.
    pub fn client_key(mut self, path: impl Into<PathBuf>) -> Self {
        self.client_key_path = Some(path.into());
        self
    }

    /// Skip server certificate verification (dangerous!).
    pub fn skip_verify(mut self, skip: bool) -> Self {
        self.danger_skip_verify = skip;
        self
    }

    /// Set the server name for SNI.
    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    /// Check if mutual TLS (client certificate) is configured.
    pub fn has_client_cert(&self) -> bool {
        self.client_cert_path.is_some() && self.client_key_path.is_some()
    }
}

/// SSL mode for MySQL connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    /// Do not use SSL
    #[default]
    Disable,
    /// Prefer SSL if available, fall back to non-SSL
    Preferred,
    /// Require SSL connection
    Required,
    /// Require SSL and verify server certificate
    VerifyCa,
    /// Require SSL and verify server certificate matches hostname
    VerifyIdentity,
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
            SslMode::Required | SslMode::VerifyCa | SslMode::VerifyIdentity
        )
    }
}

/// MySQL connection configuration.
#[derive(Debug, Clone)]
pub struct MySqlConfig {
    /// Hostname or IP address
    pub host: String,
    /// Port number (default: 3306)
    pub port: u16,
    /// Username for authentication
    pub user: String,
    /// Password for authentication
    pub password: Option<String>,
    /// Database name to connect to (optional at connect time)
    pub database: Option<String>,
    /// Character set (default: utf8mb4)
    pub charset: u8,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// SSL mode
    pub ssl_mode: SslMode,
    /// TLS configuration (certificates, keys, etc.)
    pub tls_config: TlsConfig,
    /// Enable compression (CLIENT_COMPRESS capability)
    pub compression: bool,
    /// Additional connection attributes
    pub attributes: HashMap<String, String>,
    /// Local infile handling (disabled by default for security)
    pub local_infile: bool,
    /// Max allowed packet size (default: 64MB)
    pub max_packet_size: u32,
}

impl Default for MySqlConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 3306,
            user: String::new(),
            password: None,
            database: None,
            charset: crate::protocol::charset::UTF8MB4_0900_AI_CI,
            connect_timeout: Duration::from_secs(30),
            ssl_mode: SslMode::default(),
            tls_config: TlsConfig::default(),
            compression: false,
            attributes: HashMap::new(),
            local_infile: false,
            max_packet_size: 64 * 1024 * 1024, // 64MB
        }
    }
}

impl MySqlConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the hostname.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    /// Set the port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the username.
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = user.into();
        self
    }

    /// Set the password.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set the database.
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Set the character set.
    pub fn charset(mut self, charset: u8) -> Self {
        self.charset = charset;
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

    /// Set the TLS configuration.
    pub fn tls_config(mut self, config: TlsConfig) -> Self {
        self.tls_config = config;
        self
    }

    /// Set the CA certificate path for TLS.
    ///
    /// This is a convenience method equivalent to:
    /// ```ignore
    /// config.tls_config(TlsConfig::new().ca_cert(path))
    /// ```
    pub fn ca_cert(mut self, path: impl Into<PathBuf>) -> Self {
        self.tls_config.ca_cert_path = Some(path.into());
        self
    }

    /// Set client certificate and key paths for mutual TLS.
    ///
    /// Both cert and key must be provided for client authentication.
    pub fn client_cert(mut self, cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        self.tls_config.client_cert_path = Some(cert_path.into());
        self.tls_config.client_key_path = Some(key_path.into());
        self
    }

    /// Enable or disable compression.
    pub fn compression(mut self, enabled: bool) -> Self {
        self.compression = enabled;
        self
    }

    /// Set a connection attribute.
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Enable or disable local infile handling.
    ///
    /// # Security Warning
    /// Enabling local infile can be a security risk. Only enable if you
    /// trust the server and understand the implications.
    pub fn local_infile(mut self, enabled: bool) -> Self {
        self.local_infile = enabled;
        self
    }

    /// Set the max allowed packet size.
    pub fn max_packet_size(mut self, size: u32) -> Self {
        self.max_packet_size = size;
        self
    }

    /// Get the socket address string for connection.
    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Build capability flags based on configuration.
    pub fn capability_flags(&self) -> u32 {
        use crate::protocol::capabilities::{
            CLIENT_COMPRESS, CLIENT_CONNECT_ATTRS, CLIENT_CONNECT_WITH_DB, CLIENT_LOCAL_FILES,
            CLIENT_SSL, DEFAULT_CLIENT_FLAGS,
        };

        let mut flags = DEFAULT_CLIENT_FLAGS;

        if self.database.is_some() {
            flags |= CLIENT_CONNECT_WITH_DB;
        }

        if self.ssl_mode.should_try_ssl() {
            flags |= CLIENT_SSL;
        }

        if self.compression {
            flags |= CLIENT_COMPRESS;
        }

        if self.local_infile {
            flags |= CLIENT_LOCAL_FILES;
        }

        if !self.attributes.is_empty() {
            flags |= CLIENT_CONNECT_ATTRS;
        }

        flags
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = MySqlConfig::new()
            .host("db.example.com")
            .port(3307)
            .user("myuser")
            .password("secret")
            .database("testdb")
            .connect_timeout(Duration::from_secs(10))
            .ssl_mode(SslMode::Required)
            .compression(true)
            .attribute("program_name", "myapp");

        assert_eq!(config.host, "db.example.com");
        assert_eq!(config.port, 3307);
        assert_eq!(config.user, "myuser");
        assert_eq!(config.password, Some("secret".to_string()));
        assert_eq!(config.database, Some("testdb".to_string()));
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.ssl_mode, SslMode::Required);
        assert!(config.compression);
        assert_eq!(
            config.attributes.get("program_name"),
            Some(&"myapp".to_string())
        );
    }

    #[test]
    fn test_socket_addr() {
        let config = MySqlConfig::new().host("db.example.com").port(3307);
        assert_eq!(config.socket_addr(), "db.example.com:3307");
    }

    #[test]
    fn test_ssl_mode_properties() {
        assert!(!SslMode::Disable.should_try_ssl());
        assert!(!SslMode::Disable.is_required());

        assert!(SslMode::Preferred.should_try_ssl());
        assert!(!SslMode::Preferred.is_required());

        assert!(SslMode::Required.should_try_ssl());
        assert!(SslMode::Required.is_required());

        assert!(SslMode::VerifyCa.should_try_ssl());
        assert!(SslMode::VerifyCa.is_required());

        assert!(SslMode::VerifyIdentity.should_try_ssl());
        assert!(SslMode::VerifyIdentity.is_required());
    }

    #[test]
    fn test_capability_flags() {
        use crate::protocol::capabilities::*;

        let config = MySqlConfig::new().database("test").compression(true);
        let flags = config.capability_flags();

        assert!(flags & CLIENT_CONNECT_WITH_DB != 0);
        assert!(flags & CLIENT_COMPRESS != 0);
        assert!(flags & CLIENT_PROTOCOL_41 != 0);
        assert!(flags & CLIENT_SECURE_CONNECTION != 0);
    }

    #[test]
    fn test_default_config() {
        let config = MySqlConfig::default();

        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 3306);
        assert_eq!(config.ssl_mode, SslMode::Disable);
        assert!(!config.compression);
        assert!(!config.local_infile);
    }

    #[test]
    fn test_tls_config_builder() {
        let tls = TlsConfig::new()
            .ca_cert("/path/to/ca.pem")
            .client_cert("/path/to/client.pem")
            .client_key("/path/to/client-key.pem")
            .server_name("db.example.com");

        assert_eq!(tls.ca_cert_path, Some(PathBuf::from("/path/to/ca.pem")));
        assert_eq!(tls.client_cert_path, Some(PathBuf::from("/path/to/client.pem")));
        assert_eq!(tls.client_key_path, Some(PathBuf::from("/path/to/client-key.pem")));
        assert_eq!(tls.server_name, Some("db.example.com".to_string()));
        assert!(!tls.danger_skip_verify);
        assert!(tls.has_client_cert());
    }

    #[test]
    fn test_tls_config_skip_verify() {
        let tls = TlsConfig::new().skip_verify(true);
        assert!(tls.danger_skip_verify);
    }

    #[test]
    fn test_mysql_config_with_tls() {
        let config = MySqlConfig::new()
            .host("db.example.com")
            .ssl_mode(SslMode::VerifyCa)
            .ca_cert("/etc/ssl/certs/ca.pem")
            .client_cert("/home/user/.mysql/client-cert.pem", "/home/user/.mysql/client-key.pem");

        assert_eq!(config.ssl_mode, SslMode::VerifyCa);
        assert_eq!(
            config.tls_config.ca_cert_path,
            Some(PathBuf::from("/etc/ssl/certs/ca.pem"))
        );
        assert!(config.tls_config.has_client_cert());
    }

    #[test]
    fn test_tls_config_no_client_cert() {
        let tls = TlsConfig::new().ca_cert("/path/to/ca.pem");
        assert!(!tls.has_client_cert());

        // Only cert, no key
        let tls = TlsConfig::new()
            .ca_cert("/path/to/ca.pem")
            .client_cert("/path/to/client.pem");
        assert!(!tls.has_client_cert());
    }
}
