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

#[cfg(feature = "tls")]
use std::path::Path;

/// Build a rustls ClientConfig based on PostgreSQL SSL mode.
///
/// Semantics:
/// - Disable: not applicable (returns error)
/// - Prefer/Require: encrypt, do not verify certificates
/// - VerifyCa: verify certificate against CA, ignore hostname mismatch
/// - VerifyFull: verify certificate against CA AND enforce hostname match
#[allow(clippy::result_large_err)]
#[cfg(feature = "tls")]
pub(crate) fn build_client_config(
    ssl_mode: SslMode,
    root_cert_path: Option<&Path>,
) -> Result<rustls::ClientConfig, Error> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());

    match ssl_mode {
        SslMode::Disable => Err(tls_error("TLS config requested with SslMode::Disable")),
        SslMode::Prefer | SslMode::Require => build_no_verify_config(&provider),
        SslMode::VerifyCa => build_ca_only_config(&provider, root_cert_path),
        SslMode::VerifyFull => build_verify_full_config(&provider, root_cert_path),
    }
}

/// Certificate verifier for `VerifyCa` mode.
///
/// Verifies the certificate chain against the trusted root store, but ignores
/// any server name / SAN mismatch errors (`NotValidForName`).
#[derive(Debug)]
#[cfg(feature = "tls")]
struct CaOnlyVerifier {
    inner: Arc<dyn rustls::client::danger::ServerCertVerifier>,
}

#[cfg(feature = "tls")]
impl rustls::client::danger::ServerCertVerifier for CaOnlyVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Ok(v) => Ok(v),
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForName
                | rustls::CertificateError::NotValidForNameContext { .. },
            )) => Ok(rustls::client::danger::ServerCertVerified::assertion()),
            Err(e) => Err(e),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Load root certificates from an optional file path or webpki-roots bundle.
#[allow(clippy::result_large_err)]
#[cfg(feature = "tls")]
fn load_root_store(root_cert_path: Option<&Path>) -> Result<rustls::RootCertStore, Error> {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    use std::fs::File;
    use std::io::BufReader;

    let mut root_store = rustls::RootCertStore::empty();

    if let Some(path) = root_cert_path {
        let file = File::open(path).map_err(|e| {
            tls_error(format!(
                "Failed to open root certificate '{}': {e}",
                path.display()
            ))
        })?;
        let mut reader = BufReader::new(file);
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_reader_iter(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                tls_error(format!(
                    "Failed to parse root certificate '{}': {e}",
                    path.display()
                ))
            })?;

        if certs.is_empty() {
            return Err(tls_error(format!(
                "No certificates found in root certificate file '{}'",
                path.display()
            )));
        }

        for cert in certs {
            root_store
                .add(cert)
                .map_err(|e| tls_error(format!("Failed to add root certificate: {e}")))?;
        }
    } else {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    Ok(root_store)
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

/// Build a ClientConfig that verifies CA but ignores hostname mismatch (`VerifyCa`).
#[allow(clippy::result_large_err)]
#[cfg(feature = "tls")]
fn build_ca_only_config(
    provider: &Arc<rustls::crypto::CryptoProvider>,
    root_cert_path: Option<&Path>,
) -> Result<rustls::ClientConfig, Error> {
    let root_store = load_root_store(root_cert_path)?;
    let verifier_builder = rustls::client::WebPkiServerVerifier::builder_with_provider(
        Arc::new(root_store),
        provider.clone(),
    );
    let inner_verifier = verifier_builder
        .build()
        .map_err(|e| tls_error(format!("Failed to build certificate verifier: {e}")))?;

    let ca_verifier = Arc::new(CaOnlyVerifier {
        inner: inner_verifier,
    });

    let config = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS12, &rustls::version::TLS13])
        .map_err(|e| tls_error(format!("Failed to set TLS versions: {e}")))?
        .dangerous()
        .with_custom_certificate_verifier(ca_verifier)
        .with_no_client_auth();

    Ok(config)
}

/// Build a ClientConfig that verifies CA and enforces hostname match (`VerifyFull`).
#[allow(clippy::result_large_err)]
#[cfg(feature = "tls")]
fn build_verify_full_config(
    provider: &Arc<rustls::crypto::CryptoProvider>,
    root_cert_path: Option<&Path>,
) -> Result<rustls::ClientConfig, Error> {
    let root_store = load_root_store(root_cert_path)?;

    let config = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS12, &rustls::version::TLS13])
        .map_err(|e| tls_error(format!("Failed to set TLS versions: {e}")))?
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(config)
}

#[cfg(all(test, feature = "tls"))]
mod tests {
    use super::*;

    #[test]
    fn test_build_client_config_ssl_modes() {
        assert!(build_client_config(SslMode::Disable, None).is_err());
        assert!(build_client_config(SslMode::Prefer, None).is_ok());
        assert!(build_client_config(SslMode::Require, None).is_ok());
        assert!(build_client_config(SslMode::VerifyCa, None).is_ok());
        assert!(build_client_config(SslMode::VerifyFull, None).is_ok());

        assert!(
            build_client_config(
                SslMode::VerifyCa,
                Some(Path::new("/nonexistent/file/path/ca.crt"))
            )
            .is_err()
        );
        assert!(
            build_client_config(
                SslMode::VerifyFull,
                Some(Path::new("/nonexistent/file/path/ca.crt"))
            )
            .is_err()
        );
    }
}
