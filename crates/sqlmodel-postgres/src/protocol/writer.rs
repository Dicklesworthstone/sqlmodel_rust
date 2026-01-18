//! PostgreSQL message encoder.
//!
//! This module handles encoding frontend messages into the wire protocol format.

use super::messages::{
    CANCEL_REQUEST_CODE, DescribeKind, FrontendMessage, PROTOCOL_VERSION, SSL_REQUEST_CODE,
    frontend_type,
};

/// Buffer for writing PostgreSQL protocol messages.
///
/// All multi-byte integers are written in big-endian (network) byte order.
#[derive(Debug, Clone)]
pub struct MessageWriter {
    /// Internal buffer for message data
    buf: Vec<u8>,
}

impl Default for MessageWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageWriter {
    /// Create a new message writer with default capacity.
    pub fn new() -> Self {
        Self::with_capacity(1024)
    }

    /// Create a new message writer with specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
        }
    }

    /// Clear the internal buffer.
    pub fn clear(&mut self) {
        self.buf.clear();
    }

    /// Get the current buffer contents.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Take ownership of the buffer, leaving an empty one in its place.
    pub fn take(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buf)
    }

    /// Encode a frontend message into the buffer.
    ///
    /// Returns a slice to the encoded message data.
    pub fn write(&mut self, msg: &FrontendMessage) -> &[u8] {
        self.buf.clear();

        match msg {
            FrontendMessage::Startup { version, params } => {
                self.write_startup(*version, params);
            }
            FrontendMessage::PasswordMessage(password) => {
                self.write_password(password);
            }
            FrontendMessage::SASLInitialResponse { mechanism, data } => {
                self.write_sasl_initial(mechanism, data);
            }
            FrontendMessage::SASLResponse(data) => {
                self.write_sasl_response(data);
            }
            FrontendMessage::Query(query) => {
                self.write_query(query);
            }
            FrontendMessage::Parse {
                name,
                query,
                param_types,
            } => {
                self.write_parse(name, query, param_types);
            }
            FrontendMessage::Bind {
                portal,
                statement,
                param_formats,
                params,
                result_formats,
            } => {
                self.write_bind(portal, statement, param_formats, params, result_formats);
            }
            FrontendMessage::Describe { kind, name } => {
                self.write_describe(*kind, name);
            }
            FrontendMessage::Execute { portal, max_rows } => {
                self.write_execute(portal, *max_rows);
            }
            FrontendMessage::Close { kind, name } => {
                self.write_close(*kind, name);
            }
            FrontendMessage::Sync => {
                self.write_sync();
            }
            FrontendMessage::Flush => {
                self.write_flush();
            }
            FrontendMessage::CopyData(data) => {
                self.write_copy_data(data);
            }
            FrontendMessage::CopyDone => {
                self.write_copy_done();
            }
            FrontendMessage::CopyFail(message) => {
                self.write_copy_fail(message);
            }
            FrontendMessage::Terminate => {
                self.write_terminate();
            }
            FrontendMessage::CancelRequest {
                process_id,
                secret_key,
            } => {
                self.write_cancel_request(*process_id, *secret_key);
            }
            FrontendMessage::SSLRequest => {
                self.write_ssl_request();
            }
        }

        &self.buf
    }

    // ==================== Message Encoders ====================

    /// Write a startup message (no type byte).
    fn write_startup(&mut self, version: i32, params: &[(String, String)]) {
        // Calculate body length
        let mut body_len = 4; // version
        for (key, value) in params {
            body_len += key.len() + 1 + value.len() + 1;
        }
        body_len += 1; // terminating null

        // Write length (includes itself)
        let total_len = (body_len + 4) as i32;
        self.buf.extend_from_slice(&total_len.to_be_bytes());

        // Write version
        self.buf.extend_from_slice(&version.to_be_bytes());

        // Write parameters
        for (key, value) in params {
            self.buf.extend_from_slice(key.as_bytes());
            self.buf.push(0);
            self.buf.extend_from_slice(value.as_bytes());
            self.buf.push(0);
        }

        // Terminating null
        self.buf.push(0);
    }

    /// Write a password message.
    fn write_password(&mut self, password: &str) {
        self.write_simple_string_message(frontend_type::PASSWORD, password);
    }

    /// Write SASL initial response.
    fn write_sasl_initial(&mut self, mechanism: &str, data: &[u8]) {
        // Type byte
        self.buf.push(frontend_type::PASSWORD);

        // Calculate length: 4 (length) + mechanism + null + 4 (data length) + data
        let body_len = mechanism.len() + 1 + 4 + data.len();
        let total_len = (body_len + 4) as i32;
        self.buf.extend_from_slice(&total_len.to_be_bytes());

        // Mechanism name
        self.buf.extend_from_slice(mechanism.as_bytes());
        self.buf.push(0);

        // Data length (-1 if no data)
        if data.is_empty() {
            self.buf.extend_from_slice(&(-1_i32).to_be_bytes());
        } else {
            let data_len = data.len() as i32;
            self.buf.extend_from_slice(&data_len.to_be_bytes());
            self.buf.extend_from_slice(data);
        }
    }

    /// Write SASL response.
    fn write_sasl_response(&mut self, data: &[u8]) {
        self.buf.push(frontend_type::PASSWORD);
        let len = (data.len() + 4) as i32;
        self.buf.extend_from_slice(&len.to_be_bytes());
        self.buf.extend_from_slice(data);
    }

    /// Write a simple query message.
    fn write_query(&mut self, query: &str) {
        self.write_simple_string_message(frontend_type::QUERY, query);
    }

    /// Write a Parse message (prepare statement).
    fn write_parse(&mut self, name: &str, query: &str, param_types: &[u32]) {
        self.buf.push(frontend_type::PARSE);

        // Calculate length
        let body_len = name.len() + 1 + query.len() + 1 + 2 + (param_types.len() * 4);
        let total_len = (body_len + 4) as i32;
        self.buf.extend_from_slice(&total_len.to_be_bytes());

        // Statement name
        self.buf.extend_from_slice(name.as_bytes());
        self.buf.push(0);

        // Query string
        self.buf.extend_from_slice(query.as_bytes());
        self.buf.push(0);

        // Parameter types
        let num_params = param_types.len() as i16;
        self.buf.extend_from_slice(&num_params.to_be_bytes());
        for &oid in param_types {
            self.buf.extend_from_slice(&oid.to_be_bytes());
        }
    }

    /// Write a Bind message.
    fn write_bind(
        &mut self,
        portal: &str,
        statement: &str,
        param_formats: &[i16],
        params: &[Option<Vec<u8>>],
        result_formats: &[i16],
    ) {
        self.buf.push(frontend_type::BIND);

        // Calculate body length
        let mut body_len = portal.len() + 1 + statement.len() + 1;
        body_len += 2 + (param_formats.len() * 2); // format codes
        body_len += 2; // num params

        for param in params {
            body_len += 4; // length
            if let Some(data) = param {
                body_len += data.len();
            }
        }

        body_len += 2 + (result_formats.len() * 2); // result format codes

        let total_len = (body_len + 4) as i32;
        self.buf.extend_from_slice(&total_len.to_be_bytes());

        // Portal name
        self.buf.extend_from_slice(portal.as_bytes());
        self.buf.push(0);

        // Statement name
        self.buf.extend_from_slice(statement.as_bytes());
        self.buf.push(0);

        // Parameter format codes
        let num_formats = param_formats.len() as i16;
        self.buf.extend_from_slice(&num_formats.to_be_bytes());
        for &fmt in param_formats {
            self.buf.extend_from_slice(&fmt.to_be_bytes());
        }

        // Parameter values
        let num_params = params.len() as i16;
        self.buf.extend_from_slice(&num_params.to_be_bytes());
        for param in params {
            match param {
                Some(data) => {
                    let len = data.len() as i32;
                    self.buf.extend_from_slice(&len.to_be_bytes());
                    self.buf.extend_from_slice(data);
                }
                None => {
                    // NULL value
                    self.buf.extend_from_slice(&(-1_i32).to_be_bytes());
                }
            }
        }

        // Result format codes
        let num_result_formats = result_formats.len() as i16;
        self.buf
            .extend_from_slice(&num_result_formats.to_be_bytes());
        for &fmt in result_formats {
            self.buf.extend_from_slice(&fmt.to_be_bytes());
        }
    }

    /// Write a Describe message.
    fn write_describe(&mut self, kind: DescribeKind, name: &str) {
        self.buf.push(frontend_type::DESCRIBE);
        let body_len = 1 + name.len() + 1;
        let total_len = (body_len + 4) as i32;
        self.buf.extend_from_slice(&total_len.to_be_bytes());
        self.buf.push(kind.as_byte());
        self.buf.extend_from_slice(name.as_bytes());
        self.buf.push(0);
    }

    /// Write an Execute message.
    fn write_execute(&mut self, portal: &str, max_rows: i32) {
        self.buf.push(frontend_type::EXECUTE);
        let body_len = portal.len() + 1 + 4;
        let total_len = (body_len + 4) as i32;
        self.buf.extend_from_slice(&total_len.to_be_bytes());
        self.buf.extend_from_slice(portal.as_bytes());
        self.buf.push(0);
        self.buf.extend_from_slice(&max_rows.to_be_bytes());
    }

    /// Write a Close message.
    fn write_close(&mut self, kind: DescribeKind, name: &str) {
        self.buf.push(frontend_type::CLOSE);
        let body_len = 1 + name.len() + 1;
        let total_len = (body_len + 4) as i32;
        self.buf.extend_from_slice(&total_len.to_be_bytes());
        self.buf.push(kind.as_byte());
        self.buf.extend_from_slice(name.as_bytes());
        self.buf.push(0);
    }

    /// Write a Sync message.
    fn write_sync(&mut self) {
        self.write_empty_message(frontend_type::SYNC);
    }

    /// Write a Flush message.
    fn write_flush(&mut self) {
        self.write_empty_message(frontend_type::FLUSH);
    }

    /// Write COPY data.
    fn write_copy_data(&mut self, data: &[u8]) {
        self.buf.push(frontend_type::COPY_DATA);
        let len = (data.len() + 4) as i32;
        self.buf.extend_from_slice(&len.to_be_bytes());
        self.buf.extend_from_slice(data);
    }

    /// Write COPY done.
    fn write_copy_done(&mut self) {
        self.write_empty_message(frontend_type::COPY_DONE);
    }

    /// Write COPY fail.
    fn write_copy_fail(&mut self, message: &str) {
        self.write_simple_string_message(frontend_type::COPY_FAIL, message);
    }

    /// Write Terminate message.
    fn write_terminate(&mut self) {
        self.write_empty_message(frontend_type::TERMINATE);
    }

    /// Write cancel request (special format, no type byte).
    fn write_cancel_request(&mut self, process_id: i32, secret_key: i32) {
        // Length (16 bytes total)
        self.buf.extend_from_slice(&16_i32.to_be_bytes());
        // Cancel request code
        self.buf
            .extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
        // Process ID
        self.buf.extend_from_slice(&process_id.to_be_bytes());
        // Secret key
        self.buf.extend_from_slice(&secret_key.to_be_bytes());
    }

    /// Write SSL request (special format, no type byte).
    fn write_ssl_request(&mut self) {
        // Length (8 bytes total)
        self.buf.extend_from_slice(&8_i32.to_be_bytes());
        // SSL request code
        self.buf.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
    }

    // ==================== Helper Methods ====================

    /// Write a message with just a type byte and length (no body).
    fn write_empty_message(&mut self, type_byte: u8) {
        self.buf.push(type_byte);
        self.buf.extend_from_slice(&4_i32.to_be_bytes());
    }

    /// Write a message containing a single null-terminated string.
    fn write_simple_string_message(&mut self, type_byte: u8, s: &str) {
        self.buf.push(type_byte);
        let len = (s.len() + 5) as i32; // 4 for length + string + null
        self.buf.extend_from_slice(&len.to_be_bytes());
        self.buf.extend_from_slice(s.as_bytes());
        self.buf.push(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_startup_message() {
        let mut writer = MessageWriter::new();
        let msg = FrontendMessage::Startup {
            version: PROTOCOL_VERSION,
            params: vec![
                ("user".to_string(), "postgres".to_string()),
                ("database".to_string(), "test".to_string()),
            ],
        };

        let data = writer.write(&msg);

        // Verify structure
        let len = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        assert!(len > 0);

        let version = i32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        assert_eq!(version, PROTOCOL_VERSION);

        // Check parameters are null-terminated
        assert!(data.ends_with(&[0]));
    }

    #[test]
    fn test_query_message() {
        let mut writer = MessageWriter::new();
        let msg = FrontendMessage::Query("SELECT 1".to_string());

        let data = writer.write(&msg);

        assert_eq!(data[0], b'Q');
        let len = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
        assert_eq!(len, 4 + 8 + 1); // length field + "SELECT 1" + null

        // Check null terminator
        assert_eq!(data[len], 0);
    }

    #[test]
    fn test_sync_message() {
        let mut writer = MessageWriter::new();
        let msg = FrontendMessage::Sync;

        let data = writer.write(&msg);

        assert_eq!(data, &[b'S', 0, 0, 0, 4]);
    }

    #[test]
    fn test_flush_message() {
        let mut writer = MessageWriter::new();
        let msg = FrontendMessage::Flush;

        let data = writer.write(&msg);

        assert_eq!(data, &[b'H', 0, 0, 0, 4]);
    }

    #[test]
    fn test_terminate_message() {
        let mut writer = MessageWriter::new();
        let msg = FrontendMessage::Terminate;

        let data = writer.write(&msg);

        assert_eq!(data, &[b'X', 0, 0, 0, 4]);
    }

    #[test]
    fn test_parse_message() {
        let mut writer = MessageWriter::new();
        let msg = FrontendMessage::Parse {
            name: "stmt1".to_string(),
            query: "SELECT $1".to_string(),
            param_types: vec![23], // int4
        };

        let data = writer.write(&msg);

        assert_eq!(data[0], b'P');

        // Find the statement name
        let name_start = 5;
        let name_end = data[name_start..].iter().position(|&b| b == 0).unwrap() + name_start;
        assert_eq!(&data[name_start..name_end], b"stmt1");
    }

    #[test]
    fn test_describe_statement() {
        let mut writer = MessageWriter::new();
        let msg = FrontendMessage::Describe {
            kind: DescribeKind::Statement,
            name: "stmt1".to_string(),
        };

        let data = writer.write(&msg);

        assert_eq!(data[0], b'D');
        assert_eq!(data[5], b'S'); // Statement kind
    }

    #[test]
    fn test_describe_portal() {
        let mut writer = MessageWriter::new();
        let msg = FrontendMessage::Describe {
            kind: DescribeKind::Portal,
            name: "portal1".to_string(),
        };

        let data = writer.write(&msg);

        assert_eq!(data[0], b'D');
        assert_eq!(data[5], b'P'); // Portal kind
    }

    #[test]
    fn test_execute_message() {
        let mut writer = MessageWriter::new();
        let msg = FrontendMessage::Execute {
            portal: "".to_string(),
            max_rows: 0,
        };

        let data = writer.write(&msg);

        assert_eq!(data[0], b'E');

        // Check max_rows (0 = no limit)
        let max_rows_offset = 5 + 1; // type + length + empty string + null
        let max_rows = i32::from_be_bytes([
            data[max_rows_offset],
            data[max_rows_offset + 1],
            data[max_rows_offset + 2],
            data[max_rows_offset + 3],
        ]);
        assert_eq!(max_rows, 0);
    }

    #[test]
    fn test_cancel_request() {
        let mut writer = MessageWriter::new();
        let msg = FrontendMessage::CancelRequest {
            process_id: 12345,
            secret_key: 67890,
        };

        let data = writer.write(&msg);

        // Length
        let len = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        assert_eq!(len, 16);

        // Cancel code
        let code = i32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        assert_eq!(code, CANCEL_REQUEST_CODE);

        // Process ID
        let pid = i32::from_be_bytes([data[8], data[9], data[10], data[11]]);
        assert_eq!(pid, 12345);

        // Secret key
        let key = i32::from_be_bytes([data[12], data[13], data[14], data[15]]);
        assert_eq!(key, 67890);
    }

    #[test]
    fn test_ssl_request() {
        let mut writer = MessageWriter::new();
        let msg = FrontendMessage::SSLRequest;

        let data = writer.write(&msg);

        let len = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        assert_eq!(len, 8);

        let code = i32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        assert_eq!(code, SSL_REQUEST_CODE);
    }

    #[test]
    fn test_bind_with_null_params() {
        let mut writer = MessageWriter::new();
        let msg = FrontendMessage::Bind {
            portal: "".to_string(),
            statement: "stmt1".to_string(),
            param_formats: vec![0],
            params: vec![None], // NULL parameter
            result_formats: vec![],
        };

        let data = writer.write(&msg);
        assert_eq!(data[0], b'B');

        // Look for -1 (NULL indicator) in the parameter section
        let null_indicator = (-1_i32).to_be_bytes();
        assert!(data.windows(4).any(|w| w == null_indicator));
    }

    #[test]
    fn test_copy_data() {
        let mut writer = MessageWriter::new();
        let payload = b"hello\nworld\n";
        let msg = FrontendMessage::CopyData(payload.to_vec());

        let data = writer.write(&msg);

        assert_eq!(data[0], b'd');
        let len = i32::from_be_bytes([data[1], data[2], data[3], data[4]]);
        assert_eq!(len, (4 + payload.len()) as i32);
        assert_eq!(&data[5..], payload);
    }

    #[test]
    fn test_writer_reuse() {
        let mut writer = MessageWriter::new();

        // First message
        writer.write(&FrontendMessage::Sync);
        assert_eq!(writer.as_bytes(), &[b'S', 0, 0, 0, 4]);

        // Second message - should replace first
        writer.write(&FrontendMessage::Flush);
        assert_eq!(writer.as_bytes(), &[b'H', 0, 0, 0, 4]);
    }
}
