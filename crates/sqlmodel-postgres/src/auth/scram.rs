//! SCRAM-SHA-256 Authentication implementation.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use hmac::{Hmac, Mac};
use rand::{Rng, distributions::Alphanumeric, thread_rng};
use sha2::{Digest, Sha256};
use sqlmodel_core::Error;
use sqlmodel_core::error::{ConnectionError, ConnectionErrorKind, ProtocolError};

type HmacSha256 = Hmac<Sha256>;

pub struct ScramClient {
    username: String,
    password: String,
    client_nonce: String,

    // State from server
    server_nonce: Option<String>,
    salt: Option<Vec<u8>>,
    iterations: Option<u32>,

    // Derived keys
    salted_password: Option<[u8; 32]>,
    auth_message: Option<String>,
}

impl ScramClient {
    pub fn new(username: &str, password: &str) -> Self {
        let client_nonce: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(24)
            .map(char::from)
            .collect();

        Self {
            username: username.to_string(),
            password: password.to_string(),
            client_nonce,
            server_nonce: None,
            salt: None,
            iterations: None,
            salted_password: None,
            auth_message: None,
        }
    }

    /// Generate client-first message
    pub fn client_first(&self) -> Vec<u8> {
        // gs2-header: "n,," (no channel binding, no authzid)
        // client-first-message-bare: "n=<user>,r=<nonce>"
        // Note: SCRAM requires strict handling of "," in usernames but Postgres usually forbids it or requires escaping.
        // For now we assume standard username.
        format!("n,,n={},r={}", self.username, self.client_nonce).into_bytes()
    }

    /// Process server-first message and generate client-final
    pub fn process_server_first(&mut self, data: &[u8]) -> Result<Vec<u8>, Error> {
        let msg = std::str::from_utf8(data)
            .map_err(|e| protocol_error(format!("Invalid UTF-8 in SASL continue: {}", e)))?;

        // Parse server-first: r=<nonce>,s=<salt>,i=<iterations>
        let mut combined_nonce = None;
        let mut salt = None;
        let mut iterations = None;

        for part in msg.split(',') {
            if let Some(value) = part.strip_prefix("r=") {
                combined_nonce = Some(value.to_string());
            } else if let Some(value) = part.strip_prefix("s=") {
                salt = Some(
                    BASE64
                        .decode(value)
                        .map_err(|e| protocol_error(format!("Invalid base64 salt: {}", e)))?,
                );
            } else if let Some(value) = part.strip_prefix("i=") {
                iterations = Some(
                    value
                        .parse()
                        .map_err(|e| protocol_error(format!("Invalid iterations: {}", e)))?,
                );
            }
        }

        let combined_nonce = combined_nonce.ok_or_else(|| protocol_error("Missing nonce"))?;
        let salt = salt.ok_or_else(|| protocol_error("Missing salt"))?;
        let iterations = iterations.ok_or_else(|| protocol_error("Missing iterations"))?;

        // Verify nonce starts with our client nonce
        if !combined_nonce.starts_with(&self.client_nonce) {
            return Err(protocol_error("Invalid server nonce"));
        }

        // Derive salted password using PBKDF2
        let mut salted_password = [0u8; 32];
        pbkdf2::pbkdf2::<HmacSha256>(
            self.password.as_bytes(),
            &salt,
            iterations,
            &mut salted_password,
        )
        .map_err(|e| protocol_error(format!("PBKDF2 failed: {}", e)))?;

        // Build auth message
        let client_first_bare = format!("n={},r={}", self.username, self.client_nonce);
        let client_final_without_proof = format!("c=biws,r={}", combined_nonce); // biws = base64("n,,")
        let auth_message = format!(
            "{},{},{}",
            client_first_bare, msg, client_final_without_proof
        );

        // Calculate client proof
        let client_key = hmac_sha256(&salted_password, b"Client Key")?;
        let stored_key = sha256(&client_key);
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes())?;

        let client_proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();

        // Store for verification
        self.server_nonce = Some(combined_nonce.clone());
        self.salted_password = Some(salted_password);
        self.auth_message = Some(auth_message);
        self.salt = Some(salt);
        self.iterations = Some(iterations);

        // Build client-final message
        let client_final = format!(
            "c=biws,r={},p={}",
            combined_nonce,
            BASE64.encode(&client_proof)
        );

        Ok(client_final.into_bytes())
    }

    /// Verify server-final message
    pub fn verify_server_final(&self, data: &[u8]) -> Result<(), Error> {
        let msg = std::str::from_utf8(data)
            .map_err(|e| protocol_error(format!("Invalid UTF-8 in SASL final: {}", e)))?;

        let server_signature_b64 = msg
            .strip_prefix("v=")
            .ok_or_else(|| protocol_error("Invalid server-final format"))?;

        let server_signature = BASE64
            .decode(server_signature_b64)
            .map_err(|e| protocol_error(format!("Invalid base64 server signature: {}", e)))?;

        // Calculate expected server signature
        let salted_password = self
            .salted_password
            .as_ref()
            .ok_or_else(|| protocol_error("Missing salted password state"))?;
        let auth_message = self
            .auth_message
            .as_ref()
            .ok_or_else(|| protocol_error("Missing auth message state"))?;

        let server_key = hmac_sha256(salted_password, b"Server Key")?;
        let expected_signature = hmac_sha256(&server_key, auth_message.as_bytes())?;

        if server_signature != expected_signature.to_vec() {
            return Err(auth_error("Server signature mismatch"));
        }

        Ok(())
    }
}

// Helpers

fn protocol_error(msg: impl Into<String>) -> Error {
    Error::Protocol(ProtocolError {
        message: msg.into(),
        raw_data: None,
        source: None,
    })
}

fn auth_error(msg: impl Into<String>) -> Error {
    Error::Connection(ConnectionError {
        kind: ConnectionErrorKind::Authentication,
        message: msg.into(),
        source: None,
    })
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<[u8; 32], Error> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|e| protocol_error(format!("HMAC init failed: {}", e)))?;
    mac.update(data);
    let result = mac.finalize();
    let bytes = result.into_bytes();
    Ok(bytes.into())
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}
