//! Message definitions for PostgreSQL protocol.

use std::fmt;

/// Protocol version 3.0.
pub const PROTOCOL_VERSION: i32 = 196_608; // 3 << 16

/// Cancel request code.
pub const CANCEL_REQUEST_CODE: i32 = 80_877_102; // 1234 << 16 | 5678

/// SSL request code.
pub const SSL_REQUEST_CODE: i32 = 80_877_103; // 1234 << 16 | 5679
// ==================== Frontend Messages (Client -> Server) ====================

/// Messages sent from the client to the PostgreSQL server.
#[derive(Debug, Clone, PartialEq)]
pub enum FrontendMessage {
    /// Startup message (no type byte) - first message sent after connecting
    Startup {
        /// Protocol version (196608 for 3.0)
        version: i32,
        /// Connection parameters (user, database, etc.)
        params: Vec<(String, String)>,
    },

    /// Password response for authentication
    PasswordMessage(String),

    /// SASL initial response (mechanism selection and initial data)
    SASLInitialResponse {
        /// SASL mechanism name (e.g., "SCRAM-SHA-256")
        mechanism: String,
        /// Initial response data
        data: Vec<u8>,
    },

    /// SASL response (continuation data)
    SASLResponse(Vec<u8>),

    /// Simple query (single SQL string, returns text format)
    Query(String),

    /// Parse a prepared statement (extended query protocol)
    Parse {
        /// Statement name ("" for unnamed)
        name: String,
        /// SQL query with $1, $2, etc. placeholders
        query: String,
        /// Parameter type OIDs (0 for server to infer)
        param_types: Vec<u32>,
    },

    /// Bind parameters to a prepared statement
    Bind {
        /// Portal name ("" for unnamed)
        portal: String,
        /// Statement name to bind to
        statement: String,
        /// Parameter format codes (0=text, 1=binary)
        param_formats: Vec<i16>,
        /// Parameter values (None for NULL)
        params: Vec<Option<Vec<u8>>>,
        /// Result format codes (0=text, 1=binary)
        result_formats: Vec<i16>,
    },

    /// Describe a prepared statement or portal
    Describe {
        /// 'S' for statement, 'P' for portal
        kind: DescribeKind,
        /// Name of statement/portal
        name: String,
    },

    /// Execute a bound portal
    Execute {
        /// Portal name
        portal: String,
        /// Maximum rows to return (0 for all)
        max_rows: i32,
    },

    /// Close a prepared statement or portal
    Close {
        /// 'S' for statement, 'P' for portal
        kind: DescribeKind,
        /// Name of statement/portal
        name: String,
    },

    /// Sync - marks end of extended query, requests ReadyForQuery
    Sync,

    /// Flush - request server to send all pending output
    Flush,

    /// COPY data chunk
    CopyData(Vec<u8>),

    /// COPY operation complete
    CopyDone,

    /// COPY operation failed
    CopyFail(String),

    /// Terminate the connection
    Terminate,

    /// Cancel a running query (sent on a separate connection)
    CancelRequest {
        /// Backend process ID
        process_id: i32,
        /// Secret key from BackendKeyData
        secret_key: i32,
    },

    /// SSL negotiation request
    SSLRequest,
}

/// Kind for Describe/Close messages
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeKind {
    /// Statement ('S')
    Statement,
    /// Portal ('P')
    Portal,
}

impl DescribeKind {
    /// Get the wire protocol byte for this kind.
    pub const fn as_byte(self) -> u8 {
        match self {
            DescribeKind::Statement => b'S',
            DescribeKind::Portal => b'P',
        }
    }

    /// Parse from wire protocol byte.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            b'S' => Some(DescribeKind::Statement),
            b'P' => Some(DescribeKind::Portal),
            _ => None,
        }
    }
}

// ==================== Backend Messages (Server -> Client) ====================

/// Messages sent from the PostgreSQL server to the client.
#[derive(Debug, Clone, PartialEq)]
pub enum BackendMessage {
    // Authentication messages
    /// Authentication successful
    AuthenticationOk,
    /// Server requests cleartext password
    AuthenticationCleartextPassword,
    /// Server requests MD5-hashed password with salt
    AuthenticationMD5Password([u8; 4]),
    /// Server requests SASL authentication (lists mechanisms)
    AuthenticationSASL(Vec<String>),
    /// SASL authentication continuation data
    AuthenticationSASLContinue(Vec<u8>),
    /// SASL authentication final data
    AuthenticationSASLFinal(Vec<u8>),

    // Connection info
    /// Backend process ID and secret key for cancellation
    BackendKeyData {
        /// Process ID
        process_id: i32,
        /// Secret key
        secret_key: i32,
    },
    /// Server parameter status (e.g., server_encoding, TimeZone)
    ParameterStatus {
        /// Parameter name
        name: String,
        /// Parameter value
        value: String,
    },
    /// Server is ready for a new query
    ReadyForQuery(TransactionStatus),

    // Query results
    /// Describes the columns of a result set
    RowDescription(Vec<FieldDescription>),
    /// A single data row
    DataRow(Vec<Option<Vec<u8>>>),
    /// Query completed successfully
    CommandComplete(String),
    /// Empty query response
    EmptyQueryResponse,

    // Extended query protocol responses
    /// Parse completed successfully
    ParseComplete,
    /// Bind completed successfully
    BindComplete,
    /// Close completed successfully
    CloseComplete,
    /// Describes parameter types for a prepared statement
    ParameterDescription(Vec<u32>),
    /// No data will be returned
    NoData,
    /// Portal execution suspended (reached max_rows)
    PortalSuspended,

    // Errors and notices
    /// Error response with details
    ErrorResponse(ErrorFields),
    /// Notice (warning) with details
    NoticeResponse(ErrorFields),

    // COPY protocol
    /// Server is ready to receive COPY data
    CopyInResponse {
        /// Overall COPY format (0=text, 1=binary)
        format: i8,
        /// Per-column format codes
        column_formats: Vec<i16>,
    },
    /// Server is sending COPY data
    CopyOutResponse {
        /// Overall COPY format (0=text, 1=binary)
        format: i8,
        /// Per-column format codes
        column_formats: Vec<i16>,
    },
    /// COPY data chunk
    CopyData(Vec<u8>),
    /// COPY operation complete
    CopyDone,
    /// COPY data format information for both directions
    CopyBothResponse {
        /// Overall COPY format (0=text, 1=binary)
        format: i8,
        /// Per-column format codes
        column_formats: Vec<i16>,
    },

    // Notifications
    /// Asynchronous notification (from LISTEN/NOTIFY)
    NotificationResponse {
        /// Backend process ID that sent the notification
        process_id: i32,
        /// Channel name
        channel: String,
        /// Payload string
        payload: String,
    },

    // Function call (legacy, rarely used)
    /// Function call result
    FunctionCallResponse(Option<Vec<u8>>),

    // Negotiate protocol version
    /// Server doesn't support requested protocol features
    NegotiateProtocolVersion {
        /// Server's newest supported minor version
        newest_minor: i32,
        /// Unrecognized options
        unrecognized: Vec<String>,
    },
}

/// Transaction status indicator from ReadyForQuery
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionStatus {
    /// Idle - not in a transaction block
    #[default]
    Idle,
    /// In a transaction block
    Transaction,
    /// In a failed transaction block
    Error,
}

impl TransactionStatus {
    /// Get the wire protocol byte for this status.
    pub const fn as_byte(self) -> u8 {
        match self {
            TransactionStatus::Idle => b'I',
            TransactionStatus::Transaction => b'T',
            TransactionStatus::Error => b'E',
        }
    }

    /// Parse from wire protocol byte.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            b'I' => Some(TransactionStatus::Idle),
            b'T' => Some(TransactionStatus::Transaction),
            b'E' => Some(TransactionStatus::Error),
            _ => None,
        }
    }
}

impl fmt::Display for TransactionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionStatus::Idle => write!(f, "idle"),
            TransactionStatus::Transaction => write!(f, "in transaction"),
            TransactionStatus::Error => write!(f, "in failed transaction"),
        }
    }
}

/// Describes a single field (column) in a row description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDescription {
    /// Column name
    pub name: String,
    /// OID of the table (0 if not from a table)
    pub table_oid: u32,
    /// Attribute number in the table (0 if not from a table)
    pub column_id: i16,
    /// OID of the column's data type
    pub type_oid: u32,
    /// Data type size (-1 for variable-length types)
    pub type_size: i16,
    /// Type modifier (e.g., precision for NUMERIC)
    pub type_modifier: i32,
    /// Format code (0=text, 1=binary)
    pub format: i16,
}

impl FieldDescription {
    /// Check if this field uses binary format.
    pub const fn is_binary(&self) -> bool {
        self.format == 1
    }

    /// Check if this field uses text format.
    pub const fn is_text(&self) -> bool {
        self.format == 0
    }
}

/// Error and notice response fields.
///
/// PostgreSQL error responses contain multiple fields identified by single-byte codes.
/// All fields are optional except severity, code, and message.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ErrorFields {
    /// Severity (ERROR, FATAL, PANIC, WARNING, NOTICE, DEBUG, INFO, LOG)
    pub severity: String,
    /// Localized severity (for display)
    pub severity_localized: Option<String>,
    /// SQLSTATE code (e.g., "23505" for unique_violation)
    pub code: String,
    /// Primary error message
    pub message: String,
    /// Optional secondary message with more detail
    pub detail: Option<String>,
    /// Optional suggestion for fixing the problem
    pub hint: Option<String>,
    /// Position in query string (1-based)
    pub position: Option<i32>,
    /// Position in internal query
    pub internal_position: Option<i32>,
    /// Internal query that generated the error
    pub internal_query: Option<String>,
    /// Call stack context
    pub where_: Option<String>,
    /// Schema name
    pub schema: Option<String>,
    /// Table name
    pub table: Option<String>,
    /// Column name
    pub column: Option<String>,
    /// Data type name
    pub data_type: Option<String>,
    /// Constraint name
    pub constraint: Option<String>,
    /// Source file name
    pub file: Option<String>,
    /// Source line number
    pub line: Option<i32>,
    /// Source routine name
    pub routine: Option<String>,
}

impl ErrorFields {
    /// Check if this is a fatal error.
    pub fn is_fatal(&self) -> bool {
        self.severity == "FATAL" || self.severity == "PANIC"
    }

    /// Check if this is a regular error.
    pub fn is_error(&self) -> bool {
        self.severity == "ERROR"
    }

    /// Check if this is a warning or notice.
    pub fn is_warning(&self) -> bool {
        matches!(
            self.severity.as_str(),
            "WARNING" | "NOTICE" | "DEBUG" | "INFO" | "LOG"
        )
    }

    /// Get the SQLSTATE error class (first two characters).
    pub fn error_class(&self) -> &str {
        if self.code.len() >= 2 {
            &self.code[..2]
        } else {
            &self.code
        }
    }
}

impl fmt::Display for ErrorFields {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {} ({})", self.severity, self.message, self.code)?;
        if let Some(detail) = &self.detail {
            write!(f, "\nDETAIL: {detail}")?;
        }
        if let Some(hint) = &self.hint {
            write!(f, "\nHINT: {hint}")?;
        }
        if let Some(pos) = self.position {
            write!(f, "\nPOSITION: {pos}")?;
        }
        if let Some(where_) = &self.where_ {
            write!(f, "\nCONTEXT: {where_}")?;
        }
        Ok(())
    }
}

// ==================== Message Type Bytes ====================

/// Message type bytes for frontend messages.
pub mod frontend_type {
    pub const PASSWORD: u8 = b'p';
    pub const QUERY: u8 = b'Q';
    pub const PARSE: u8 = b'P';
    pub const BIND: u8 = b'B';
    pub const DESCRIBE: u8 = b'D';
    pub const EXECUTE: u8 = b'E';
    pub const CLOSE: u8 = b'C';
    pub const SYNC: u8 = b'S';
    pub const FLUSH: u8 = b'H';
    pub const COPY_DATA: u8 = b'd';
    pub const COPY_DONE: u8 = b'c';
    pub const COPY_FAIL: u8 = b'f';
    pub const TERMINATE: u8 = b'X';
}

/// Message type bytes for backend messages.
pub mod backend_type {
    pub const AUTHENTICATION: u8 = b'R';
    pub const BACKEND_KEY_DATA: u8 = b'K';
    pub const PARAMETER_STATUS: u8 = b'S';
    pub const READY_FOR_QUERY: u8 = b'Z';
    pub const ROW_DESCRIPTION: u8 = b'T';
    pub const DATA_ROW: u8 = b'D';
    pub const COMMAND_COMPLETE: u8 = b'C';
    pub const EMPTY_QUERY: u8 = b'I';
    pub const PARSE_COMPLETE: u8 = b'1';
    pub const BIND_COMPLETE: u8 = b'2';
    pub const CLOSE_COMPLETE: u8 = b'3';
    pub const PARAMETER_DESCRIPTION: u8 = b't';
    pub const NO_DATA: u8 = b'n';
    pub const PORTAL_SUSPENDED: u8 = b's';
    pub const ERROR_RESPONSE: u8 = b'E';
    pub const NOTICE_RESPONSE: u8 = b'N';
    pub const COPY_IN_RESPONSE: u8 = b'G';
    pub const COPY_OUT_RESPONSE: u8 = b'H';
    pub const COPY_DATA: u8 = b'd';
    pub const COPY_DONE: u8 = b'c';
    pub const COPY_BOTH_RESPONSE: u8 = b'W';
    pub const NOTIFICATION_RESPONSE: u8 = b'A';
    pub const FUNCTION_CALL_RESPONSE: u8 = b'V';
    pub const NEGOTIATE_PROTOCOL_VERSION: u8 = b'v';
}

// ==================== Authentication Type Codes ====================

/// Authentication method codes from the server.
pub mod auth_type {
    pub const OK: i32 = 0;
    pub const KERBEROS_V5: i32 = 2;
    pub const CLEARTEXT_PASSWORD: i32 = 3;
    pub const MD5_PASSWORD: i32 = 5;
    pub const SCM_CREDENTIAL: i32 = 6;
    pub const GSS: i32 = 7;
    pub const GSS_CONTINUE: i32 = 8;
    pub const SSPI: i32 = 9;
    pub const SASL: i32 = 10;
    pub const SASL_CONTINUE: i32 = 11;
    pub const SASL_FINAL: i32 = 12;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_status_roundtrip() {
        for status in [
            TransactionStatus::Idle,
            TransactionStatus::Transaction,
            TransactionStatus::Error,
        ] {
            let byte = status.as_byte();
            let parsed = TransactionStatus::from_byte(byte).unwrap();
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn test_describe_kind_roundtrip() {
        for kind in [DescribeKind::Statement, DescribeKind::Portal] {
            let byte = kind.as_byte();
            let parsed = DescribeKind::from_byte(byte).unwrap();
            assert_eq!(kind, parsed);
        }
    }

    #[test]
    fn test_error_fields_display() {
        let err = ErrorFields {
            severity: "ERROR".to_string(),
            code: "23505".to_string(),
            message: "duplicate key value violates unique constraint".to_string(),
            detail: Some("Key (id)=(1) already exists.".to_string()),
            hint: None,
            ..Default::default()
        };

        let display = format!("{err}");
        assert!(display.contains("ERROR"));
        assert!(display.contains("23505"));
        assert!(display.contains("duplicate key"));
        assert!(display.contains("Key (id)=(1)"));
    }

    #[test]
    fn test_error_fields_classification() {
        let fatal = ErrorFields {
            severity: "FATAL".to_string(),
            code: "XX000".to_string(),
            message: "internal error".to_string(),
            ..Default::default()
        };
        assert!(fatal.is_fatal());
        assert!(!fatal.is_error());
        assert!(!fatal.is_warning());

        let error = ErrorFields {
            severity: "ERROR".to_string(),
            code: "23505".to_string(),
            message: "constraint violation".to_string(),
            ..Default::default()
        };
        assert!(!error.is_fatal());
        assert!(error.is_error());
        assert!(!error.is_warning());

        let warning = ErrorFields {
            severity: "WARNING".to_string(),
            code: "01000".to_string(),
            message: "deprecated feature".to_string(),
            ..Default::default()
        };
        assert!(!warning.is_fatal());
        assert!(!warning.is_error());
        assert!(warning.is_warning());
    }

    #[test]
    fn test_error_class() {
        let err = ErrorFields {
            code: "23505".to_string(),
            ..Default::default()
        };
        assert_eq!(err.error_class(), "23");
    }

    #[test]
    fn test_field_description_format() {
        let text_field = FieldDescription {
            name: "id".to_string(),
            table_oid: 0,
            column_id: 0,
            type_oid: 23,
            type_size: 4,
            type_modifier: -1,
            format: 0,
        };
        assert!(text_field.is_text());
        assert!(!text_field.is_binary());

        let binary_field = FieldDescription {
            format: 1,
            ..text_field
        };
        assert!(!binary_field.is_text());
        assert!(binary_field.is_binary());
    }
}
