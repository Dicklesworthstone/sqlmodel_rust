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
    /// Request for public key (client should send 0x02)
    pub const REQUEST_PUBLIC_KEY: u8 = 0x02;
    /// Fast auth success
    pub const FAST_AUTH_SUCCESS: u8 = 0x03;
    /// Full auth needed (switch to secure channel or RSA)
    pub const PERFORM_FULL_AUTH: u8 = 0x04;
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
/// Uses `OsRng` for cryptographically secure random generation.
pub fn generate_nonce(length: usize) -> Vec<u8> {
    use rand::RngCore;
    use rand::rngs::OsRng;
    let mut bytes = vec![0u8; length];
    OsRng.fill_bytes(&mut bytes);
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
/// Note: This is a placeholder - RSA encryption requires additional dependencies.
/// In practice, prefer using TLS connections which don't require RSA encryption.
pub fn sha256_password_rsa(
    _password: &str,
    _seed: &[u8],
    _public_key: &[u8],
) -> Result<Vec<u8>, String> {
    // RSA encryption requires the `rsa` crate which adds significant dependencies.
    // For now, we recommend using TLS connections instead.
    // When TLS is established, the password can be sent in cleartext (still secure).
    Err("RSA encryption not implemented - use TLS connection instead".to_string())
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

    #[test]
    fn test_plugin_names() {
        assert_eq!(plugins::MYSQL_NATIVE_PASSWORD, "mysql_native_password");
        assert_eq!(plugins::CACHING_SHA2_PASSWORD, "caching_sha2_password");
        assert_eq!(plugins::SHA256_PASSWORD, "sha256_password");
    }
}
