//! TLS/SSL support for MySQL connections.
//!
//! This module implements the TLS handshake for MySQL connections using rustls.
//!
//! # MySQL TLS Handshake Flow
//!
//! 1. Server sends initial handshake with `CLIENT_SSL` capability
//! 2. If SSL is requested, client sends short SSL request packet:
//!    - 4 bytes: capability flags (with `CLIENT_SSL`)
//!    - 4 bytes: max packet size
//!    - 1 byte: character set
//!    - 23 bytes: reserved (zeros)
//! 3. Client performs TLS handshake
//! 4. Client sends full handshake response over TLS
//! 5. Server sends auth result over TLS
//!
//! # Feature Flag
//!
//! TLS support requires the `tls` feature to be enabled:
//!
//! ```toml
//! [dependencies]
//! sqlmodel-mysql = { version = "0.1", features = ["tls"] }
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use sqlmodel_mysql::{MySqlConfig, SslMode, TlsConfig};
//!
//! let config = MySqlConfig::new()
//!     .host("db.example.com")
//!     .ssl_mode(SslMode::VerifyCa)
//!     .tls_config(TlsConfig::new()
//!         .ca_cert("/etc/ssl/certs/ca.pem"));
//!
//! // Connection will use TLS after initial handshake
//! let conn = MySqlConnection::connect(config)?;
//! ```

#![allow(clippy::cast_possible_truncation)]
// The Error type is intentionally large to carry full context
#![allow(clippy::result_large_err)]
// Placeholder function takes stream by value since it just returns error
#![allow(clippy::needless_pass_by_value)]

use crate::config::{SslMode, TlsConfig};
use crate::protocol::{PacketWriter, capabilities};
use sqlmodel_core::Error;
use sqlmodel_core::error::{ConnectionError, ConnectionErrorKind};

#[cfg(feature = "tls")]
use std::io::{Read, Write};
#[cfg(feature = "tls")]
use std::sync::Arc;

/// Build an SSL request packet.
///
/// This packet is sent after receiving the server handshake and before
/// performing the TLS handshake. It tells the server that we want to
/// upgrade to TLS.
///
/// # Format
///
/// - capability_flags (4 bytes): Client capabilities with CLIENT_SSL set
/// - max_packet_size (4 bytes): Maximum packet size
/// - character_set (1 byte): Character set code
/// - reserved (23 bytes): All zeros
///
/// Total: 32 bytes
pub fn build_ssl_request_packet(
    client_caps: u32,
    max_packet_size: u32,
    character_set: u8,
    sequence_id: u8,
) -> Vec<u8> {
    let mut writer = PacketWriter::with_capacity(32);

    // Capability flags with CLIENT_SSL
    let caps_with_ssl = client_caps | capabilities::CLIENT_SSL;
    writer.write_u32_le(caps_with_ssl);

    // Max packet size
    writer.write_u32_le(max_packet_size);

    // Character set
    writer.write_u8(character_set);

    // Reserved (23 bytes of zeros)
    writer.write_zeros(23);

    writer.build_packet(sequence_id)
}

/// Check if the server supports SSL/TLS.
///
/// # Arguments
///
/// * `server_caps` - Server capability flags from handshake
///
/// # Returns
///
/// `true` if the server has the CLIENT_SSL capability flag set.
pub const fn server_supports_ssl(server_caps: u32) -> bool {
    server_caps & capabilities::CLIENT_SSL != 0
}

/// Validate SSL mode against server capabilities.
///
/// # Returns
///
/// - `Ok(true)` if SSL should be used
/// - `Ok(false)` if SSL should not be used
/// - `Err(_)` if SSL is required but not supported by server
pub fn validate_ssl_mode(ssl_mode: SslMode, server_caps: u32) -> Result<bool, Error> {
    let server_supports = server_supports_ssl(server_caps);

    match ssl_mode {
        SslMode::Disable => Ok(false),
        SslMode::Preferred => Ok(server_supports),
        SslMode::Required | SslMode::VerifyCa | SslMode::VerifyIdentity => {
            if server_supports {
                Ok(true)
            } else {
                Err(tls_error("SSL required but server does not support it"))
            }
        }
    }
}

/// Validate TLS configuration for the given SSL mode.
///
/// # Arguments
///
/// * `ssl_mode` - The requested SSL mode
/// * `tls_config` - The TLS configuration
///
/// # Returns
///
/// `Ok(())` if configuration is valid, `Err(_)` with details if not.
pub fn validate_tls_config(ssl_mode: SslMode, tls_config: &TlsConfig) -> Result<(), Error> {
    match ssl_mode {
        SslMode::Disable | SslMode::Preferred | SslMode::Required => {
            // No certificate validation required for these modes
            Ok(())
        }
        SslMode::VerifyCa | SslMode::VerifyIdentity => {
            // Need CA certificate for server verification
            if tls_config.ca_cert_path.is_none() && !tls_config.danger_skip_verify {
                return Err(tls_error(
                    "CA certificate required for VerifyCa/VerifyIdentity mode. \
                     Set ca_cert_path or danger_skip_verify.",
                ));
            }

            // If client cert is provided, key must also be provided
            if tls_config.client_cert_path.is_some() && tls_config.client_key_path.is_none() {
                return Err(tls_error(
                    "Client certificate provided without client key. \
                     Both must be set for mutual TLS.",
                ));
            }

            Ok(())
        }
    }
}

/// Create a TLS-related connection error.
fn tls_error(message: impl Into<String>) -> Error {
    Error::Connection(ConnectionError {
        kind: ConnectionErrorKind::Ssl,
        message: message.into(),
        source: None,
    })
}

// ============================================================================
// TLS Stream Implementation (feature-gated)
// ============================================================================

/// TLS connection wrapper using rustls.
///
/// This struct wraps a TCP stream with TLS encryption using the rustls library.
/// It implements `Read` and `Write` traits to provide transparent encryption.
///
/// # SSL Modes
///
/// The implementation supports all MySQL SSL modes:
/// - `Disable`: No TLS (TlsStream is not used)
/// - `Preferred`: TLS if available, no cert verification
/// - `Required`: TLS required, no cert verification
/// - `VerifyCa`: Verify server certificate with CA
/// - `VerifyIdentity`: Verify server cert + hostname
#[cfg(feature = "tls")]
pub struct TlsStream<S: Read + Write> {
    /// The rustls connection state
    conn: rustls::ClientConnection,
    /// The underlying TCP stream
    stream: S,
}

#[cfg(feature = "tls")]
impl<S: Read + Write> std::fmt::Debug for TlsStream<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsStream")
            .field("protocol_version", &self.conn.protocol_version())
            .field("is_handshaking", &self.conn.is_handshaking())
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "tls")]
impl<S: Read + Write> TlsStream<S> {
    /// Create a new TLS stream and perform the handshake.
    ///
    /// # Arguments
    ///
    /// * `stream` - The underlying TCP stream (already connected)
    /// * `tls_config` - TLS configuration (certificates, verification options)
    /// * `server_name` - Server hostname for SNI and certificate verification
    /// * `ssl_mode` - The SSL mode to use for verification
    ///
    /// # Returns
    ///
    /// A new `TlsStream` with encryption enabled, or an error if the handshake fails.
    pub fn new(
        mut stream: S,
        tls_config: &TlsConfig,
        server_name: &str,
        ssl_mode: SslMode,
    ) -> Result<Self, Error> {
        // Build the rustls ClientConfig based on SSL mode
        let config = build_client_config(tls_config, ssl_mode)?;

        // Parse the server name for SNI
        let sni_name = tls_config.server_name.as_deref().unwrap_or(server_name);

        let server_name = sni_name
            .to_string()
            .try_into()
            .map_err(|e| tls_error(format!("Invalid server name '{}': {}", sni_name, e)))?;

        // Create the rustls client connection
        let mut conn = rustls::ClientConnection::new(Arc::new(config), server_name)
            .map_err(|e| tls_error(format!("Failed to create TLS connection: {}", e)))?;

        // Perform the TLS handshake synchronously
        // This writes/reads until the handshake completes
        while conn.is_handshaking() {
            // Write any pending TLS data to the stream
            while conn.wants_write() {
                conn.write_tls(&mut stream)
                    .map_err(|e| tls_error(format!("TLS handshake write error: {}", e)))?;
            }

            // Read TLS data from the stream if needed
            if conn.wants_read() {
                conn.read_tls(&mut stream)
                    .map_err(|e| tls_error(format!("TLS handshake read error: {}", e)))?;

                // Process the TLS data
                conn.process_new_packets()
                    .map_err(|e| tls_error(format!("TLS handshake error: {}", e)))?;
            }
        }

        Ok(TlsStream { conn, stream })
    }

    /// Get the negotiated protocol version.
    pub fn protocol_version(&self) -> Option<rustls::ProtocolVersion> {
        self.conn.protocol_version()
    }

    /// Get the negotiated cipher suite.
    pub fn negotiated_cipher_suite(&self) -> Option<rustls::SupportedCipherSuite> {
        self.conn.negotiated_cipher_suite()
    }

    /// Check if the connection is using TLS 1.3.
    pub fn is_tls13(&self) -> bool {
        self.conn.protocol_version() == Some(rustls::ProtocolVersion::TLSv1_3)
    }
}

#[cfg(feature = "tls")]
impl<S: Read + Write> Read for TlsStream<S> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Try to read decrypted data from the rustls buffer
        loop {
            // First, try to read from the plaintext buffer
            match self.conn.reader().read(buf) {
                Ok(n) if n > 0 => return Ok(n),
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }

            // If no data available, read more TLS records
            if self.conn.wants_read() {
                let n = self.conn.read_tls(&mut self.stream)?;
                if n == 0 {
                    return Ok(0); // EOF
                }

                // Process the TLS records
                self.conn
                    .process_new_packets()
                    .map_err(|e| std::io::Error::other(format!("TLS error: {}", e)))?;
            } else {
                return Ok(0);
            }
        }
    }
}

#[cfg(feature = "tls")]
impl<S: Read + Write> Write for TlsStream<S> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Write plaintext to the rustls buffer
        let n = self.conn.writer().write(buf)?;

        // Flush TLS data to the underlying stream
        while self.conn.wants_write() {
            self.conn.write_tls(&mut self.stream)?;
        }

        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.conn.writer().flush()?;
        while self.conn.wants_write() {
            self.conn.write_tls(&mut self.stream)?;
        }
        self.stream.flush()
    }
}

/// Build a rustls ClientConfig based on TLS configuration and SSL mode.
#[cfg(feature = "tls")]
pub(crate) fn build_client_config(
    tls_config: &TlsConfig,
    ssl_mode: SslMode,
) -> Result<rustls::ClientConfig, Error> {
    // Get the default crypto provider (ring when that feature is enabled)
    let provider = Arc::new(rustls::crypto::ring::default_provider());

    match ssl_mode {
        SslMode::Disable => {
            // This shouldn't happen - TlsStream shouldn't be created for Disable mode
            Err(tls_error("TlsStream created with SslMode::Disable"))
        }

        SslMode::Preferred | SslMode::Required => {
            // No certificate verification - accept any server certificate
            // This is common for MySQL deployments with self-signed certs
            if tls_config.danger_skip_verify {
                build_no_verify_config(&provider)
            } else {
                // Use webpki-roots for standard CA verification
                build_webpki_config(&provider, tls_config)
            }
        }

        SslMode::VerifyCa | SslMode::VerifyIdentity => {
            if tls_config.danger_skip_verify {
                // User explicitly wants to skip verification (dangerous!)
                build_no_verify_config(&provider)
            } else if let Some(ca_path) = &tls_config.ca_cert_path {
                // Use custom CA certificate
                build_custom_ca_config(&provider, tls_config, ca_path)
            } else {
                // Use webpki-roots (standard CA bundle)
                build_webpki_config(&provider, tls_config)
            }
        }
    }
}

/// Build a ClientConfig that skips certificate verification (dangerous!).
#[cfg(feature = "tls")]
fn build_no_verify_config(
    provider: &Arc<rustls::crypto::CryptoProvider>,
) -> Result<rustls::ClientConfig, Error> {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, Error as RustlsError, SignatureScheme};

    /// A certificate verifier that accepts any certificate (insecure!).
    #[derive(Debug)]
    struct NoVerifier;

    impl ServerCertVerifier for NoVerifier {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, RustlsError> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, RustlsError> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, RustlsError> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::ECDSA_NISTP521_SHA512,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::ED25519,
            ]
        }
    }

    let config = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS12, &rustls::version::TLS13])
        .map_err(|e| tls_error(format!("Failed to set TLS versions: {}", e)))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier))
        .with_no_client_auth();

    Ok(config)
}

/// Build a ClientConfig using webpki-roots CA bundle.
#[cfg(feature = "tls")]
fn build_webpki_config(
    provider: &Arc<rustls::crypto::CryptoProvider>,
    tls_config: &TlsConfig,
) -> Result<rustls::ClientConfig, Error> {
    use rustls::RootCertStore;

    let mut root_store = RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS12, &rustls::version::TLS13])
        .map_err(|e| tls_error(format!("Failed to set TLS versions: {}", e)))?
        .with_root_certificates(root_store);

    // Add client certificate if configured
    let config = add_client_auth(builder, tls_config)?;

    Ok(config)
}

/// Build a ClientConfig using a custom CA certificate.
#[cfg(feature = "tls")]
fn build_custom_ca_config(
    provider: &Arc<rustls::crypto::CryptoProvider>,
    tls_config: &TlsConfig,
    ca_path: &std::path::Path,
) -> Result<rustls::ClientConfig, Error> {
    use rustls::RootCertStore;
    use std::fs::File;
    use std::io::BufReader;

    // Load CA certificate(s)
    let ca_file = File::open(ca_path).map_err(|e| {
        tls_error(format!(
            "Failed to open CA certificate '{}': {}",
            ca_path.display(),
            e
        ))
    })?;
    let mut reader = BufReader::new(ca_file);

    let certs = read_pem_certificates(&mut reader)
        .map_err(|e| tls_error(format!("Failed to parse CA certificate: {}", e)))?;

    if certs.is_empty() {
        return Err(tls_error(format!(
            "No certificates found in CA file '{}'",
            ca_path.display()
        )));
    }

    let mut root_store = RootCertStore::empty();
    for cert in certs {
        root_store
            .add(cert)
            .map_err(|e| tls_error(format!("Failed to add CA certificate: {}", e)))?;
    }

    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS12, &rustls::version::TLS13])
        .map_err(|e| tls_error(format!("Failed to set TLS versions: {}", e)))?
        .with_root_certificates(root_store);

    // Add client certificate if configured
    let config = add_client_auth(builder, tls_config)?;

    Ok(config)
}

/// Add client authentication if configured.
#[cfg(feature = "tls")]
fn add_client_auth(
    builder: rustls::ConfigBuilder<rustls::ClientConfig, rustls::client::WantsClientCert>,
    tls_config: &TlsConfig,
) -> Result<rustls::ClientConfig, Error> {
    use std::fs::File;
    use std::io::BufReader;

    if let (Some(cert_path), Some(key_path)) =
        (&tls_config.client_cert_path, &tls_config.client_key_path)
    {
        // Load client certificate
        let cert_file = File::open(cert_path).map_err(|e| {
            tls_error(format!(
                "Failed to open client cert '{}': {}",
                cert_path.display(),
                e
            ))
        })?;
        let mut cert_reader = BufReader::new(cert_file);

        let certs = read_pem_certificates(&mut cert_reader)
            .map_err(|e| tls_error(format!("Failed to parse client certificate: {}", e)))?;

        if certs.is_empty() {
            return Err(tls_error(format!(
                "No certificates found in client cert file '{}'",
                cert_path.display()
            )));
        }

        // Load client private key
        let key_file = File::open(key_path).map_err(|e| {
            tls_error(format!(
                "Failed to open client key '{}': {}",
                key_path.display(),
                e
            ))
        })?;
        let mut key_reader = BufReader::new(key_file);

        let key = read_pem_private_key(&mut key_reader).map_err(|e| match e {
            PemError::NoItemsFound => {
                tls_error(format!("No private key found in '{}'", key_path.display()))
            }
            other => tls_error(format!("Failed to parse client key: {}", other)),
        })?;

        builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| tls_error(format!("Failed to configure client auth: {}", e)))
    } else {
        Ok(builder.with_no_client_auth())
    }
}

#[cfg(feature = "tls")]
use rustls::pki_types::pem::Error as PemError;

/// Read every `CERTIFICATE` section from PEM text.
///
/// Non-certificate sections (keys, comments, unrelated blocks) are skipped;
/// a malformed section is an error. Uses `rustls-pki-types`' PEM parser (the
/// maintained successor of `rustls-pemfile`).
#[cfg(feature = "tls")]
fn read_pem_certificates(
    reader: &mut dyn std::io::BufRead,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, PemError> {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    CertificateDer::pem_reader_iter(reader).collect()
}

/// Read the first private key (PKCS#8, PKCS#1/RSA, or SEC1/EC) from PEM text.
///
/// Returns [`PemError::NoItemsFound`] when the input has no private-key
/// section (for example a certificate file passed by mistake). Encrypted
/// PKCS#8 keys are not supported and surface as a parse error.
#[cfg(feature = "tls")]
fn read_pem_private_key(
    reader: &mut dyn std::io::BufRead,
) -> Result<rustls::pki_types::PrivateKeyDer<'static>, PemError> {
    use rustls::pki_types::PrivateKeyDer;
    use rustls::pki_types::pem::PemObject;
    PrivateKeyDer::from_pem_reader(reader)
}

// ============================================================================
// Stand-in types when TLS feature is disabled
// ============================================================================

/// TLS connection wrapper when `tls` feature is disabled.
#[cfg(not(feature = "tls"))]
#[derive(Debug)]
pub struct TlsStream<S> {
    /// The underlying stream
    #[allow(dead_code)]
    inner: S,
}

#[cfg(not(feature = "tls"))]
impl<S> TlsStream<S> {
    /// Create a new TLS stream.
    ///
    /// # Note
    ///
    /// This always returns an error when the `tls` feature is disabled.
    /// Enable the `tls` feature in Cargo.toml to use TLS connections.
    #[allow(unused_variables)]
    pub fn new(
        stream: S,
        tls_config: &TlsConfig,
        server_name: &str,
        ssl_mode: SslMode,
    ) -> Result<Self, Error> {
        Err(tls_error(
            "TLS support requires the 'tls' feature. \
             Add `sqlmodel-mysql = { features = [\"tls\"] }` to your Cargo.toml.",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::charset;

    #[test]
    fn test_build_ssl_request_packet() {
        let packet = build_ssl_request_packet(
            capabilities::DEFAULT_CLIENT_FLAGS,
            16 * 1024 * 1024, // 16MB
            charset::UTF8MB4_0900_AI_CI,
            1,
        );

        // Header (4) + payload (32) = 36 bytes
        assert_eq!(packet.len(), 36);

        // Check header
        assert_eq!(packet[0], 32); // payload length low byte
        assert_eq!(packet[1], 0); // payload length mid byte
        assert_eq!(packet[2], 0); // payload length high byte
        assert_eq!(packet[3], 1); // sequence id

        // Check that CLIENT_SSL is set in the capability flags
        let caps = u32::from_le_bytes([packet[4], packet[5], packet[6], packet[7]]);
        assert!(caps & capabilities::CLIENT_SSL != 0);
    }

    #[test]
    fn test_server_supports_ssl() {
        assert!(server_supports_ssl(capabilities::CLIENT_SSL));
        assert!(server_supports_ssl(
            capabilities::CLIENT_SSL | capabilities::CLIENT_PROTOCOL_41
        ));
        assert!(!server_supports_ssl(0));
        assert!(!server_supports_ssl(capabilities::CLIENT_PROTOCOL_41));
    }

    #[test]
    fn test_validate_ssl_mode_disable() {
        assert!(!validate_ssl_mode(SslMode::Disable, 0).unwrap());
        assert!(!validate_ssl_mode(SslMode::Disable, capabilities::CLIENT_SSL).unwrap());
    }

    #[test]
    fn test_validate_ssl_mode_preferred() {
        // Preferred without SSL support -> no SSL
        assert!(!validate_ssl_mode(SslMode::Preferred, 0).unwrap());
        // Preferred with SSL support -> use SSL
        assert!(validate_ssl_mode(SslMode::Preferred, capabilities::CLIENT_SSL).unwrap());
    }

    #[test]
    fn test_validate_ssl_mode_required() {
        // Required without SSL support -> error
        assert!(validate_ssl_mode(SslMode::Required, 0).is_err());
        // Required with SSL support -> use SSL
        assert!(validate_ssl_mode(SslMode::Required, capabilities::CLIENT_SSL).unwrap());
    }

    #[test]
    fn test_validate_ssl_mode_verify() {
        // VerifyCa/VerifyIdentity without SSL support -> error
        assert!(validate_ssl_mode(SslMode::VerifyCa, 0).is_err());
        assert!(validate_ssl_mode(SslMode::VerifyIdentity, 0).is_err());

        // With SSL support -> use SSL
        assert!(validate_ssl_mode(SslMode::VerifyCa, capabilities::CLIENT_SSL).unwrap());
        assert!(validate_ssl_mode(SslMode::VerifyIdentity, capabilities::CLIENT_SSL).unwrap());
    }

    #[test]
    fn test_validate_tls_config_basic_modes() {
        let config = TlsConfig::new();

        // Basic modes don't require CA cert
        assert!(validate_tls_config(SslMode::Disable, &config).is_ok());
        assert!(validate_tls_config(SslMode::Preferred, &config).is_ok());
        assert!(validate_tls_config(SslMode::Required, &config).is_ok());
    }

    #[test]
    fn test_validate_tls_config_verify_modes() {
        // VerifyCa without CA cert -> error
        let config = TlsConfig::new();
        assert!(validate_tls_config(SslMode::VerifyCa, &config).is_err());
        assert!(validate_tls_config(SslMode::VerifyIdentity, &config).is_err());

        // With CA cert -> ok
        let config = TlsConfig::new().ca_cert("/path/to/ca.pem");
        assert!(validate_tls_config(SslMode::VerifyCa, &config).is_ok());
        assert!(validate_tls_config(SslMode::VerifyIdentity, &config).is_ok());

        // With skip_verify -> ok (dangerous but valid config)
        let config = TlsConfig::new().skip_verify(true);
        assert!(validate_tls_config(SslMode::VerifyCa, &config).is_ok());
    }

    #[test]
    fn test_validate_tls_config_client_cert() {
        // Client cert without key -> error
        let config = TlsConfig::new()
            .ca_cert("/path/to/ca.pem")
            .client_cert("/path/to/client.pem");
        assert!(validate_tls_config(SslMode::VerifyCa, &config).is_err());

        // Client cert with key -> ok
        let config = TlsConfig::new()
            .ca_cert("/path/to/ca.pem")
            .client_cert("/path/to/client.pem")
            .client_key("/path/to/client-key.pem");
        assert!(validate_tls_config(SslMode::VerifyCa, &config).is_ok());
    }
}

/// PEM parsing tests for the rustls-pki-types based loaders that replaced
/// `rustls-pemfile`. The fixtures are throwaway keys and self-signed
/// certificates generated with OpenSSL for these tests only; they secure
/// nothing.
#[cfg(all(test, feature = "tls"))]
mod pem_tests {
    use super::{PemError, read_pem_certificates, read_pem_private_key};
    use rustls::pki_types::PrivateKeyDer;
    use std::io::Cursor;

    const EC_SEC1_KEY: &str = "-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIC2fKWVerV0o9016nVpKkeKanx3CQCLZrGT06XA3AMz5oAoGCCqGSM49
AwEHoUQDQgAEo5udO1PuDK/uQRFkMICCDeNvxpWnNWvZyaxN6T3q5hJjpP14CeUN
BXcUZChtKdS9H5xNgdQ6QNmINcdYumgjKA==
-----END EC PRIVATE KEY-----
";

    const EC_PKCS8_KEY: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgLZ8pZV6tXSj3TXqd
WkqR4pqfHcJAItmsZPTpcDcAzPmhRANCAASjm507U+4Mr+5BEWQwgIIN42/Glac1
a9nJrE3pPermEmOk/XgJ5Q0FdxRkKG0p1L0fnE2B1DpA2Yg1x1i6aCMo
-----END PRIVATE KEY-----
";

    const RSA_PKCS1_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----
MIICXQIBAAKBgQDLY+qnjclMlTAEveDAcCS34cElboRyMRsCVgXB43fmBeItbr6A
9P9YAPGlfffP6+O4MbLnEXkdxz0o6/IWIm+B6ldVrQEWncEXqy1L8z/MGH0o7pWQ
G63OI9nxa8zEnBjJTqE5C4f22g8OfpB5lqLX1jDsA6RE7B2ryijf+BOPgQIDAQAB
AoGBAIOaxW1hm01Ig2euDU23wqqRE09LMbxJ9fYO/26z5xMZ334SWIZNASRKiBHT
bpRFSHYZAm/tqHcSQorGEUEtSwSW5mk2bEQ8AnZtHHnLwUB3jj9ZnuAtdZ9Atw32
Luz6vT1bMZpBH/GHpEkJzNFo/tIqq2JpnL/cVh9ejXc4nvoZAkEA+GcmD01dhoIn
jhGuTbmMOIhMJUYGrFfLlKz5iYs7VZUj3poddSUhVZ60KxNGNcmCF6Nz3ENnWunX
b8b7HFpcZwJBANGcWFkNw+IRnEOU3fx8Q18QlA4Ifv00MvwwFEkxCVTw6cjZQUPo
tDhsb9Q4a9YboxRytRWGvnuXLJibBC1XQ9cCQQCZCTNxigBsthMYe9wfFolE6vO+
ov3Jf+10k3zJOHY9q7yFj/1GBrIaxcPKJf3DdXoohhMDSKOMZzTLMJPUS/dRAkAe
Te9L+LyAO7GO57/sV/7ZiKkPGlVZwCk64qycJFXIDQiPvDE+Yy9jFPJaCUo160r5
ktfxY8i4T5PoAElrULmDAkAvB7jmQ/CnAT2fD9HQpWuOhKpzKYmnZVo/I9dW9TSQ
fQaudez852RpY/hvOBwyptzADnglItjscMKzqnYLQHzy
-----END RSA PRIVATE KEY-----
";

    const ENCRYPTED_PKCS8_KEY: &str = "-----BEGIN ENCRYPTED PRIVATE KEY-----
MIH0MF8GCSqGSIb3DQEFDTBSMDEGCSqGSIb3DQEFDDAkBBDsh60Px5uYlDv6iq1r
alPgAgIIADAMBggqhkiG9w0CCQUAMB0GCWCGSAFlAwQBKgQQLHWjHeHXNDatiQjc
Y2YlGwSBkLtB+wbPRRlaDa43B+XgeT/hgAKm5P1udztbDd0sRKC3RyuX2mg3IQYh
rLoKaG3lnZs5kh9iGPUFCyc5MsAGH5zZK4riJwxLcNcmttdKhfk3weshUp8IY4Bf
Od9wWs4JTIavcTN08xjXl7KYLIlssfxlZbsW1QEipxcBYG2/xDFmAD3GXmER0qB5
XHlpWJ+KUg==
-----END ENCRYPTED PRIVATE KEY-----
";

    const CERT_EC: &str = "-----BEGIN CERTIFICATE-----
MIIBizCCATGgAwIBAgIUdzI6g0GsVlMulSGYwbyO14Cae9QwCgYIKoZIzj0EAwIw
GzEZMBcGA1UEAwwQc3FsbW9kZWwtdGVzdC1jYTAeFw0yNjA5MDIwNDQ1NDhaFw0y
NjA5MDMwNDQ1NDhaMBsxGTAXBgNVBAMMEHNxbG1vZGVsLXRlc3QtY2EwWTATBgcq
hkjOPQIBBggqhkjOPQMBBwNCAASjm507U+4Mr+5BEWQwgIIN42/Glac1a9nJrE3p
PermEmOk/XgJ5Q0FdxRkKG0p1L0fnE2B1DpA2Yg1x1i6aCMoo1MwUTAdBgNVHQ4E
FgQUnRLuUrUZhNJkXyFKSKXZOej3D/QwHwYDVR0jBBgwFoAUnRLuUrUZhNJkXyFK
SKXZOej3D/QwDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBFAiAPv7+R
gI2ZcA1QofsPWmrvyiZ1e1CAkWTVDXOsHGmo9QIhAJf5PthsrHpNCbTjlQFUjf3B
bf05cSBBV4ynUoQYyHjU
-----END CERTIFICATE-----
";

    const CERT_RSA: &str = "-----BEGIN CERTIFICATE-----
MIICFDCCAX2gAwIBAgIUXLxhpjqIdor1v4rnL9uKTTnsaFEwDQYJKoZIhvcNAQEL
BQAwHDEaMBgGA1UEAwwRc3FsbW9kZWwtdGVzdC1jYTIwHhcNMjYwOTAyMDQ0NTQ4
WhcNMjYwOTAzMDQ0NTQ4WjAcMRowGAYDVQQDDBFzcWxtb2RlbC10ZXN0LWNhMjCB
nzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEAy2Pqp43JTJUwBL3gwHAkt+HBJW6E
cjEbAlYFweN35gXiLW6+gPT/WADxpX33z+vjuDGy5xF5Hcc9KOvyFiJvgepXVa0B
Fp3BF6stS/M/zBh9KO6VkButziPZ8WvMxJwYyU6hOQuH9toPDn6QeZai19Yw7AOk
ROwdq8oo3/gTj4ECAwEAAaNTMFEwHQYDVR0OBBYEFNafV3XvPPr5TeoDHU49z7Sj
RCXMMB8GA1UdIwQYMBaAFNafV3XvPPr5TeoDHU49z7SjRCXMMA8GA1UdEwEB/wQF
MAMBAf8wDQYJKoZIhvcNAQELBQADgYEAWoiLxWCh9oeMCXj/phrm6cugPDL29pNm
CEb2+znfw+C3ZK7fP/vtzjZfZ7ZIarU9b+WDd1/+33G2weT5B2RVcJA1hRQovmLO
G1euYZaoD1WUYYMQFMsBgnsGSrkeNSAnXGNEEWqweFLwSzZ6jUGAT95IEodL7J9j
8nN1O8VELb8=
-----END CERTIFICATE-----
";

    fn cursor(s: &str) -> Cursor<Vec<u8>> {
        Cursor::new(s.as_bytes().to_vec())
    }

    #[test]
    fn ca_bundle_with_two_certs_comments_and_crlf_parses_both() {
        let bundle = format!(
            "# corporate roots\r\n{}\r\n# second root\r\n{}",
            CERT_EC.replace('\n', "\r\n"),
            CERT_RSA.replace('\n', "\r\n")
        );
        let certs = read_pem_certificates(&mut cursor(&bundle)).expect("two certificates");
        assert_eq!(certs.len(), 2);
        // DER of a self-signed cert is well over 200 bytes for either key type.
        assert!(certs.iter().all(|c| c.len() > 200));
    }

    #[test]
    fn certificate_loader_skips_key_sections_and_returns_empty_for_key_only_input() {
        let certs = read_pem_certificates(&mut cursor(EC_PKCS8_KEY)).expect("no error");
        assert!(certs.is_empty(), "a key file has no certificates");

        let mixed = format!("{EC_PKCS8_KEY}{CERT_EC}");
        let certs = read_pem_certificates(&mut cursor(&mixed)).expect("mixed input parses");
        assert_eq!(certs.len(), 1, "only the certificate section is returned");
    }

    #[test]
    fn certificate_loader_returns_empty_for_empty_input() {
        let certs = read_pem_certificates(&mut cursor("")).expect("empty is not an error");
        assert!(certs.is_empty());
    }

    #[test]
    fn private_key_loader_accepts_pkcs8_pkcs1_and_sec1() {
        match read_pem_private_key(&mut cursor(EC_PKCS8_KEY)).expect("pkcs8") {
            PrivateKeyDer::Pkcs8(_) => {}
            other => panic!("expected PKCS#8, got {other:?}"),
        }
        match read_pem_private_key(&mut cursor(RSA_PKCS1_KEY)).expect("pkcs1") {
            PrivateKeyDer::Pkcs1(_) => {}
            other => panic!("expected PKCS#1, got {other:?}"),
        }
        match read_pem_private_key(&mut cursor(EC_SEC1_KEY)).expect("sec1") {
            PrivateKeyDer::Sec1(_) => {}
            other => panic!("expected SEC1, got {other:?}"),
        }
    }

    #[test]
    fn private_key_loader_reports_no_items_when_given_a_certificate() {
        let err = read_pem_private_key(&mut cursor(CERT_EC)).expect_err("cert is not a key");
        assert!(matches!(err, PemError::NoItemsFound), "got {err:?}");
    }

    #[test]
    fn private_key_loader_rejects_encrypted_pkcs8_instead_of_guessing() {
        // rustls-pki-types does not decrypt keys; an ENCRYPTED PRIVATE KEY
        // section must not be silently accepted as a usable key.
        let result = read_pem_private_key(&mut cursor(ENCRYPTED_PKCS8_KEY));
        assert!(result.is_err(), "encrypted key must not parse as a private key");
    }

    #[test]
    fn private_key_loader_returns_first_key_when_cert_precedes_it() {
        let combined = format!("{CERT_RSA}{RSA_PKCS1_KEY}");
        match read_pem_private_key(&mut cursor(&combined)).expect("key after cert") {
            PrivateKeyDer::Pkcs1(_) => {}
            other => panic!("expected PKCS#1, got {other:?}"),
        }
    }

    #[test]
    fn truncated_base64_is_a_parse_error_not_a_panic() {
        let truncated = "-----BEGIN CERTIFICATE-----\nMIIBizCCATGgAwIBAgIU\n-----END CERTIFICATE-----\n";
        let result = read_pem_certificates(&mut cursor(truncated));
        assert!(result.is_err());
    }
}
