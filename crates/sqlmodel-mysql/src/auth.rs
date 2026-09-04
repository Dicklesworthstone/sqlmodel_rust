//! MySQL authentication implementations.
//!
//! This module implements the MySQL authentication plugins:
//! - `mysql_native_password`: SHA1-based (legacy, MySQL < 8.0 default)
//! - `caching_sha2_password`: SHA256-based (MySQL 8.0+ default)
//!
//! # mysql_native_password
//!
//! Password scramble algorithm:
//! ```text
//! SHA1(password) XOR SHA1(seed + SHA1(SHA1(password)))
//! ```
//!
//! # caching_sha2_password
//!
//! Fast auth (if cached on server):
//! ```text
//! XOR(SHA256(password), SHA256(SHA256(SHA256(password)) + seed))
//! ```
//!
//! Full auth requires TLS or RSA public key encryption.

use sha1::Sha1;
use sha2::{Digest, Sha256};

use rand::rand_core::UnwrapErr;
use rand::rngs::SysRng;

// RSA is only needed for the no-TLS full-auth path; the `rsa-auth` feature
// (on by default) controls it so TLS-only deployments can drop the dependency.
#[cfg(feature = "rsa-auth")]
use rsa::RsaPublicKey;
#[cfg(feature = "rsa-auth")]
use rsa::pkcs1::DecodeRsaPublicKey;
#[cfg(feature = "rsa-auth")]
use rsa::pkcs8::DecodePublicKey;

/// Well-known authentication plugin names.
pub mod plugins {
    /// SHA1-based authentication (legacy default)
    pub const MYSQL_NATIVE_PASSWORD: &str = "mysql_native_password";
    /// SHA256-based authentication (MySQL 8.0+ default)
    pub const CACHING_SHA2_PASSWORD: &str = "caching_sha2_password";
    /// RSA-based SHA256 authentication
    pub const SHA256_PASSWORD: &str = "sha256_password";
    /// MySQL clear password (for debugging/testing only)
    pub const MYSQL_CLEAR_PASSWORD: &str = "mysql_clear_password";
}

/// Response codes for caching_sha2_password protocol.
pub mod caching_sha2 {
    /// First byte of every AuthMoreData packet the server sends during
    /// `caching_sha2_password` / `sha256_password` continuation (the status
    /// byte or the RSA public key follows it).
    pub const AUTH_MORE_DATA: u8 = 0x01;
    /// Request for public key (client should send 0x02)
    pub const REQUEST_PUBLIC_KEY: u8 = 0x02;
    /// Fast auth success
    pub const FAST_AUTH_SUCCESS: u8 = 0x03;
    /// Full auth needed (switch to secure channel or RSA)
    pub const PERFORM_FULL_AUTH: u8 = 0x04;
}

/// Strip the AuthMoreData marker (`0x01`) that MySQL prepends to
/// `caching_sha2_password` / `sha256_password` continuation payloads.
///
/// A live MySQL 8 server frames the fast-auth status as `[0x01, 0x03]` (fast
/// auth succeeded, an OK packet follows) or `[0x01, 0x04]` (perform full
/// authentication), and the RSA public key as `[0x01, <PEM bytes>]`. The
/// status bytes (`0x03`/`0x04`), a PEM key (`-`), an OK packet (`0x00`) and an
/// EOF packet (`0xFE`) never start with `0x01`, so a payload that does not
/// begin with the marker is returned unchanged. A lone `[0x01]` becomes empty,
/// which callers report as a protocol error.
#[must_use]
pub fn strip_auth_more_data_marker(payload: &[u8]) -> &[u8] {
    match payload {
        [caching_sha2::AUTH_MORE_DATA, rest @ ..] => rest,
        _ => payload,
    }
}

/// Compute mysql_native_password authentication response.
///
/// Algorithm: `SHA1(password) XOR SHA1(seed + SHA1(SHA1(password)))`
///
/// # Arguments
/// * `password` - The user's password (UTF-8)
/// * `auth_data` - The 20-byte scramble from the server
///
/// # Returns
/// The 20-byte authentication response, or empty vec if password is empty.
pub fn mysql_native_password(password: &str, auth_data: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return vec![];
    }

    // Ensure we only use first 20 bytes of auth_data
    let seed = if auth_data.len() > 20 {
        &auth_data[..20]
    } else {
        auth_data
    };

    // Stage 1: SHA1(password)
    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let stage1: [u8; 20] = hasher.finalize().into();

    // Stage 2: SHA1(SHA1(password))
    let mut hasher = Sha1::new();
    hasher.update(stage1);
    let stage2: [u8; 20] = hasher.finalize().into();

    // Stage 3: SHA1(seed + stage2)
    let mut hasher = Sha1::new();
    hasher.update(seed);
    hasher.update(stage2);
    let stage3: [u8; 20] = hasher.finalize().into();

    // Final: stage1 XOR stage3
    stage1
        .iter()
        .zip(stage3.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

/// Compute caching_sha2_password fast authentication response.
///
/// Algorithm: `XOR(SHA256(password), SHA256(SHA256(SHA256(password)) + seed))`
///
/// # Arguments
/// * `password` - The user's password (UTF-8)
/// * `auth_data` - The scramble from the server (typically 20 bytes + NUL)
///
/// # Returns
/// The 32-byte authentication response, or empty vec if password is empty.
pub fn caching_sha2_password(password: &str, auth_data: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return vec![];
    }

    // Remove trailing NUL if present (MySQL sends 20-byte scramble + NUL = 21 bytes)
    // Only strip if length is 21 and ends with NUL, to avoid modifying valid 20-byte seeds
    let seed = if auth_data.len() == 21 && auth_data.last() == Some(&0) {
        &auth_data[..20]
    } else {
        auth_data
    };

    // SHA256(password)
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    let password_hash: [u8; 32] = hasher.finalize().into();

    // SHA256(SHA256(password))
    let mut hasher = Sha256::new();
    hasher.update(password_hash);
    let password_hash_hash: [u8; 32] = hasher.finalize().into();

    // SHA256(SHA256(SHA256(password)) + seed)
    let mut hasher = Sha256::new();
    hasher.update(password_hash_hash);
    hasher.update(seed);
    let scramble: [u8; 32] = hasher.finalize().into();

    // XOR(SHA256(password), scramble)
    password_hash
        .iter()
        .zip(scramble.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

/// Generate a random nonce for client-side use.
///
/// Uses `SysRng` (OS entropy; renamed from `OsRng` in rand 0.10) for
/// cryptographically secure random generation. `SysRng` is fallible-only
/// (`TryRng`) in rand 0.10; `UnwrapErr` adapts it to the infallible `Rng`
/// surface `fill_bytes` needs.
pub fn generate_nonce(length: usize) -> Vec<u8> {
    use rand::Rng;
    let mut bytes = vec![0u8; length];
    UnwrapErr(SysRng).fill_bytes(&mut bytes);
    bytes
}

/// Scramble password for sha256_password plugin using RSA encryption.
///
/// This is used when full authentication is required for caching_sha2_password
/// or sha256_password plugins without TLS.
///
/// # Arguments
/// * `password` - The user's password
/// * `seed` - The authentication seed from server
/// * `public_key` - RSA public key from server (PEM format)
///
/// # Returns
/// The encrypted password, or error if encryption fails.
///
/// This is used for full authentication for `caching_sha2_password`/`sha256_password`
/// when the connection is not secured by TLS.
#[cfg(feature = "rsa-auth")]
pub fn sha256_password_rsa(
    password: &str,
    seed: &[u8],
    public_key_pem: &[u8],
    use_oaep: bool,
) -> Result<Vec<u8>, String> {
    // MySQL expects: RSA_encrypt(password_with_nul XOR seed_rotation)
    let mut pw = password.as_bytes().to_vec();
    pw.push(0); // NUL terminator

    if seed.is_empty() {
        return Err("Seed is empty".to_string());
    }

    for (i, b) in pw.iter_mut().enumerate() {
        *b ^= seed[i % seed.len()];
    }

    // Server usually returns a PEM public key for sha256_password/caching_sha2_password.
    let pem = std::str::from_utf8(public_key_pem)
        .map_err(|e| format!("Public key is not valid UTF-8 PEM: {e}"))?;

    // Try both common encodings.
    let pub_key = RsaPublicKey::from_public_key_pem(pem)
        .or_else(|_| RsaPublicKey::from_pkcs1_pem(pem))
        .map_err(|e| format!("Failed to parse RSA public key PEM: {e}"))?;

    let encrypted = if use_oaep {
        // MySQL 8.0.5+ uses OAEP padding for caching_sha2_password.
        let padding = rsa::Oaep::<Sha1>::new();
        pub_key
            .encrypt(&mut UnwrapErr(SysRng), padding, &pw)
            .map_err(|e| format!("RSA OAEP encryption failed: {e}"))?
    } else {
        let padding = rsa::Pkcs1v15Encrypt;
        pub_key
            .encrypt(&mut UnwrapErr(SysRng), padding, &pw)
            .map_err(|e| format!("RSA PKCS1v1.5 encryption failed: {e}"))?
    };

    Ok(encrypted)
}

/// Without the `rsa-auth` feature the no-TLS full-auth path is unavailable
/// by design: the caller surfaces this message as an
/// `ConnectionErrorKind::Authentication` error telling the deployment to use
/// TLS or re-enable the feature.
#[cfg(not(feature = "rsa-auth"))]
pub fn sha256_password_rsa(
    _password: &str,
    _seed: &[u8],
    _public_key_pem: &[u8],
    _use_oaep: bool,
) -> Result<Vec<u8>, String> {
    Err(
        "MySQL full authentication requires TLS (SslMode::Required or stronger) \
         or the `rsa-auth` feature; the server requested RSA password exchange \
         on a plaintext connection"
            .to_owned(),
    )
}

/// XOR password with seed for cleartext transmission over TLS.
///
/// When the connection is secured with TLS, some auth methods allow sending
/// the password XOR'd with the seed (or even cleartext).
pub fn xor_password_with_seed(password: &str, seed: &[u8]) -> Vec<u8> {
    let password_bytes = password.as_bytes();
    let mut result = Vec::with_capacity(password_bytes.len() + 1);

    for (i, &byte) in password_bytes.iter().enumerate() {
        let seed_byte = seed.get(i % seed.len()).copied().unwrap_or(0);
        result.push(byte ^ seed_byte);
    }

    // NUL terminator
    result.push(0);

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_more_data_marker_is_stripped_from_live_server_frames() {
        // Exactly what MySQL 8.4 sends after a cached caching_sha2 login.
        assert_eq!(
            strip_auth_more_data_marker(&[
                caching_sha2::AUTH_MORE_DATA,
                caching_sha2::FAST_AUTH_SUCCESS
            ]),
            &[caching_sha2::FAST_AUTH_SUCCESS]
        );
        assert_eq!(
            strip_auth_more_data_marker(&[
                caching_sha2::AUTH_MORE_DATA,
                caching_sha2::PERFORM_FULL_AUTH
            ]),
            &[caching_sha2::PERFORM_FULL_AUTH]
        );
        // RSA public key frame: marker followed by PEM.
        let mut frame = vec![caching_sha2::AUTH_MORE_DATA];
        frame.extend_from_slice(b"-----BEGIN PUBLIC KEY-----\n");
        assert_eq!(
            strip_auth_more_data_marker(&frame),
            b"-----BEGIN PUBLIC KEY-----\n"
        );
    }

    #[test]
    fn auth_more_data_marker_stripping_leaves_other_frames_alone() {
        // Already-unwrapped status bytes pass through untouched.
        assert_eq!(
            strip_auth_more_data_marker(&[caching_sha2::FAST_AUTH_SUCCESS]),
            &[caching_sha2::FAST_AUTH_SUCCESS]
        );
        // OK (0x00) and EOF (0xFE) packets are not AuthMoreData.
        assert_eq!(
            strip_auth_more_data_marker(&[0x00, 0x00, 0x00]),
            &[0x00, 0x00, 0x00]
        );
        assert_eq!(strip_auth_more_data_marker(&[0xFE, 0x00]), &[0xFE, 0x00]);
        // Degenerate frames: a lone marker becomes empty; empty stays empty.
        assert!(strip_auth_more_data_marker(&[caching_sha2::AUTH_MORE_DATA]).is_empty());
        assert!(strip_auth_more_data_marker(&[]).is_empty());
    }

    #[test]
    fn test_mysql_native_password_empty() {
        let result = mysql_native_password("", &[0; 20]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_mysql_native_password() {
        // Known test vector from MySQL protocol documentation
        // Seed: 20 bytes of zeros
        // Password: "secret"
        let seed = [0u8; 20];
        let result = mysql_native_password("secret", &seed);

        // Should produce 20 bytes
        assert_eq!(result.len(), 20);

        // The result should be deterministic
        let result2 = mysql_native_password("secret", &seed);
        assert_eq!(result, result2);
    }

    #[test]
    fn test_mysql_native_password_real_seed() {
        // Test with a realistic scramble
        let seed = [
            0x3d, 0x4c, 0x5e, 0x2f, 0x1a, 0x0b, 0x7c, 0x8d, 0x9e, 0xaf, 0x10, 0x21, 0x32, 0x43,
            0x54, 0x65, 0x76, 0x87, 0x98, 0xa9,
        ];

        let result = mysql_native_password("mypassword", &seed);
        assert_eq!(result.len(), 20);

        // Different password should give different result
        let result2 = mysql_native_password("otherpassword", &seed);
        assert_ne!(result, result2);
    }

    #[test]
    fn test_caching_sha2_password_empty() {
        let result = caching_sha2_password("", &[0; 20]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_caching_sha2_password() {
        let seed = [0u8; 20];
        let result = caching_sha2_password("secret", &seed);

        // Should produce 32 bytes (SHA-256 output)
        assert_eq!(result.len(), 32);

        // Should be deterministic
        let result2 = caching_sha2_password("secret", &seed);
        assert_eq!(result, result2);
    }

    #[test]
    fn test_caching_sha2_password_with_nul() {
        // MySQL often sends seed with trailing NUL
        let mut seed = vec![0u8; 20];
        seed.push(0); // Trailing NUL

        let result = caching_sha2_password("secret", &seed);
        assert_eq!(result.len(), 32);

        // Should be same as without NUL
        let result2 = caching_sha2_password("secret", &seed[..20]);
        assert_eq!(result, result2);
    }

    #[test]
    fn test_generate_nonce() {
        let nonce1 = generate_nonce(20);
        let nonce2 = generate_nonce(20);

        assert_eq!(nonce1.len(), 20);
        assert_eq!(nonce2.len(), 20);

        // Should be different (extremely high probability)
        assert_ne!(nonce1, nonce2);
    }

    #[test]
    fn test_xor_password_with_seed() {
        let password = "test";
        let seed = [1, 2, 3, 4, 5, 6, 7, 8];

        let result = xor_password_with_seed(password, &seed);

        // Should be password length + 1 (NUL terminator)
        assert_eq!(result.len(), 5);

        // Last byte should be NUL
        assert_eq!(result[4], 0);

        // XOR is reversible
        let recovered: Vec<u8> = result[..4]
            .iter()
            .enumerate()
            .map(|(i, &b)| b ^ seed[i % seed.len()])
            .collect();
        assert_eq!(recovered, password.as_bytes());
    }

    #[cfg(feature = "rsa-auth")]
    /// Test-only RSA public key in SPKI (`BEGIN PUBLIC KEY`) form — the shape
    /// MySQL serves for `caching_sha2_password`/`sha256_password` full auth.
    const SPKI_PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArJ59U1RtRan3BEJqsItb\n\
tD3nDU7ThwlNaJ42vRSEt/UzFO5/yemxUz3ogTUcsDMgXeLVjQjwwV0+lh+s9IWc\n\
9fU6nu4Q8yW7Pc/SDpDkFBdEtLAOIjfSMv0CzoPQB0A0njVFe7l7SuyrMWQ/19N5\n\
iEtqZQmP2y7h5a23XgPGogXHm0XnKpueZn9KFXhK2lNhZj9IUuQLmhzrH0pov8Ae\n\
FknazXZxL5aoAG+cJIoHKsf9NcGTzj0Hewb36YlgVi+yZ5NRkhgjklQ8E6IL+aaW\n\
yEOsCBtS8kCd/nHP4t6ZeyExdpggPGQ2nJq18jG+sttI+3AnzQhXtR2Adq9LKVdN\n\
RQIDAQAB\n\
-----END PUBLIC KEY-----\n";
    #[cfg(feature = "rsa-auth")]
    /// The same key in PKCS#1 (`BEGIN RSA PUBLIC KEY`) form — the fallback
    /// encoding `sha256_password_rsa` accepts.
    const PKCS1_PUBLIC_KEY: &str = "-----BEGIN RSA PUBLIC KEY-----\n\
MIIBCgKCAQEArJ59U1RtRan3BEJqsItbtD3nDU7ThwlNaJ42vRSEt/UzFO5/yemx\n\
Uz3ogTUcsDMgXeLVjQjwwV0+lh+s9IWc9fU6nu4Q8yW7Pc/SDpDkFBdEtLAOIjfS\n\
Mv0CzoPQB0A0njVFe7l7SuyrMWQ/19N5iEtqZQmP2y7h5a23XgPGogXHm0XnKpue\n\
Zn9KFXhK2lNhZj9IUuQLmhzrH0pov8AeFknazXZxL5aoAG+cJIoHKsf9NcGTzj0H\n\
ewb36YlgVi+yZ5NRkhgjklQ8E6IL+aaWyEOsCBtS8kCd/nHP4t6ZeyExdpggPGQ2\n\
nJq18jG+sttI+3AnzQhXtR2Adq9LKVdNRQIDAQAB\n\
-----END RSA PUBLIC KEY-----\n";

    /// The RSA full-auth path must keep working across `rsa` major bumps: the
    /// `pem` feature was folded into `encoding` in 0.10, so both PEM encodings
    /// MySQL can serve must still parse, and both padding modes must produce a
    /// modulus-sized, randomized ciphertext.
    #[cfg(feature = "rsa-auth")]
    #[test]
    fn test_sha256_password_rsa_accepts_both_pem_encodings() {
        let seed = [
            0x3d, 0x4c, 0x5e, 0x2f, 0x1a, 0x0b, 0x7c, 0x8d, 0x9e, 0xaf, 0x10, 0x21, 0x32, 0x43,
            0x54, 0x65, 0x76, 0x87, 0x98, 0xa9,
        ];

        for pem in [SPKI_PUBLIC_KEY, PKCS1_PUBLIC_KEY] {
            for use_oaep in [true, false] {
                let out = sha256_password_rsa("hunter2", &seed, pem.as_bytes(), use_oaep)
                    .expect("RSA encryption with a 2048-bit MySQL-style key");
                // RSA output is always exactly the modulus size (2048 bits).
                assert_eq!(out.len(), 256, "pem={pem} oaep={use_oaep}");
                // Both paddings are randomized: two encryptions must differ.
                let again = sha256_password_rsa("hunter2", &seed, pem.as_bytes(), use_oaep)
                    .expect("second encryption");
                assert_ne!(out, again, "padding must be randomized");
            }
        }
    }

    #[cfg(feature = "rsa-auth")]
    #[test]
    fn test_sha256_password_rsa_rejects_bad_input() {
        assert!(sha256_password_rsa("pw", &[], SPKI_PUBLIC_KEY.as_bytes(), true).is_err());
        assert!(sha256_password_rsa("pw", &[1, 2, 3], b"not a pem", true).is_err());
    }

    /// Without the `rsa-auth` feature the helper refuses by name, and the
    /// caller (`auth_error`) surfaces that message as an Authentication
    /// connection error.
    #[cfg(not(feature = "rsa-auth"))]
    #[test]
    fn test_sha256_password_rsa_requires_tls_or_feature() {
        let error = sha256_password_rsa("pw", &[1, 2, 3], b"unused", true)
            .expect_err("no-TLS full auth is unavailable without the rsa-auth feature");
        assert!(
            error.contains("requires TLS")
                && error.contains("rsa-auth")
                && error.contains("RSA password exchange"),
            "error must name the remediation: {error}"
        );
    }

    #[test]
    fn test_plugin_names() {
        assert_eq!(plugins::MYSQL_NATIVE_PASSWORD, "mysql_native_password");
        assert_eq!(plugins::CACHING_SHA2_PASSWORD, "caching_sha2_password");
        assert_eq!(plugins::SHA256_PASSWORD, "sha256_password");
    }

    /// RUSTSEC-2023-0071 (Marvin) is a timing side channel on RSA *private-key*
    /// decryption. It is ignored in `.cargo/audit.toml` on the grounds that this
    /// crate only ever encrypts with the server's public key. This test keeps that
    /// argument true: if anyone introduces a private-key operation here, the audit
    /// ignore must be revisited, and this test fails first.
    #[test]
    fn rsa_is_used_for_public_key_encryption_only() {
        let sources = [
            include_str!("auth.rs"),
            include_str!("connection.rs"),
            include_str!("async_connection.rs"),
            include_str!("tls.rs"),
        ];
        // Assembled with concat! so this test's own source never contains the
        // forbidden tokens verbatim (it scans itself, too).
        let forbidden_tokens = [
            concat!("RsaPriv", "ateKey"),
            concat!(".decr", "ypt("),
            concat!("Signing", "Key"),
            concat!(".si", "gn("),
            concat!("DecodePriv", "ateKey"),
        ];
        for (name, src) in ["auth.rs", "connection.rs", "async_connection.rs", "tls.rs"]
            .iter()
            .zip(sources)
        {
            for forbidden in forbidden_tokens {
                let hit = src
                    .lines()
                    .filter(|l| !l.trim_start().starts_with("//"))
                    .any(|l| l.contains(forbidden));
                assert!(
                    !hit,
                    "{name} references `{forbidden}`: the RUSTSEC-2023-0071 audit ignore assumes \
                     rsa is used only for public-key encryption; update .cargo/audit.toml"
                );
            }
        }
    }
}
