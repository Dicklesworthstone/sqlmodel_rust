//! TLS support for PostgreSQL connections (feature-gated).
//!
//! PostgreSQL TLS is negotiated by sending an `SSLRequest` message and then
//! upgrading the underlying TCP stream to a TLS stream using rustls.

#[cfg(feature = "tls")]
use sqlmodel_core::Error;
#[cfg(feature = "tls")]
use sqlmodel_core::error::{ConnectionError, ConnectionErrorKind};

#[cfg(feature = "tls")]
use crate::config::SslMode;

#[cfg(feature = "tls")]
use std::sync::Arc;

#[cfg(feature = "tls")]
fn tls_error(message: impl Into<String>) -> Error {
    Error::Connection(ConnectionError {
        kind: ConnectionErrorKind::Ssl,
        message: message.into(),
        source: None,
    })
}

// `sqlmodel_core::Error` is ~160 bytes; it is the crate-wide error type, so
// boxing it just for these helpers would fragment the error surface.
#[allow(clippy::result_large_err)]
#[cfg(feature = "tls")]
pub(crate) fn server_name(host: &str) -> Result<rustls::pki_types::ServerName<'static>, Error> {
    host.to_string()
        .try_into()
        .map_err(|e| tls_error(format!("Invalid server name '{host}': {e}")))
}

/// Build a rustls ClientConfig based on PostgreSQL SSL mode.
///
/// Semantics:
/// - Disable: not applicable (should not call)
/// - Prefer/Require: encrypt, do not verify certificates
/// - VerifyCa/VerifyFull: verify against webpki-roots CA bundle
#[allow(clippy::result_large_err)]
#[cfg(feature = "tls")]
pub(crate) fn build_client_config(ssl_mode: SslMode) -> Result<rustls::ClientConfig, Error> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());

    match ssl_mode {
        SslMode::Disable => Err(tls_error("TLS config requested with SslMode::Disable")),
        SslMode::Prefer | SslMode::Require => build_no_verify_config(&provider),
        SslMode::VerifyCa | SslMode::VerifyFull => build_webpki_config(&provider),
    }
}

/// Build a ClientConfig that skips certificate verification (dangerous!).
#[allow(clippy::result_large_err)]
#[cfg(feature = "tls")]
fn build_no_verify_config(
    provider: &Arc<rustls::crypto::CryptoProvider>,
) -> Result<rustls::ClientConfig, Error> {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, Error as RustlsError, SignatureScheme};

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
        .map_err(|e| tls_error(format!("Failed to set TLS versions: {e}")))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier))
        .with_no_client_auth();

    Ok(config)
}

/// Build a ClientConfig using webpki-roots CA bundle.
#[allow(clippy::result_large_err)]
#[cfg(feature = "tls")]
fn build_webpki_config(
    provider: &Arc<rustls::crypto::CryptoProvider>,
) -> Result<rustls::ClientConfig, Error> {
    use rustls::RootCertStore;

    let mut root_store = RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS12, &rustls::version::TLS13])
        .map_err(|e| tls_error(format!("Failed to set TLS versions: {e}")))?
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(config)
}
