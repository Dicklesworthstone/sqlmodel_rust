//! Error types for SQLModel operations.

use std::fmt;

/// The primary error type for all SQLModel operations.
#[derive(Debug)]
pub enum Error {
    /// Connection-related errors (connect, disconnect, timeout)
    Connection(ConnectionError),
    /// Query execution errors
    Query(QueryError),
    /// Type conversion errors
    Type(TypeError),
    /// Transaction errors
    Transaction(TransactionError),
    /// Protocol errors (wire-level)
    Protocol(ProtocolError),
    /// Pool errors
    Pool(PoolError),
    /// Schema/migration errors
    Schema(SchemaError),
    /// Configuration errors
    Config(ConfigError),
    /// Validation errors
    Validation(ValidationError),
    /// I/O errors
    Io(std::io::Error),
    /// Operation timed out
    Timeout,
    /// Operation was cancelled via asupersync
    Cancelled,
    /// Serialization/deserialization errors
    Serde(String),
    /// Custom error with message
    Custom(String),
}

#[derive(Debug)]
pub struct ConnectionError {
    pub kind: ConnectionErrorKind,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionErrorKind {
    /// Failed to establish connection
    Connect,
    /// Authentication failed
    Authentication,
    /// Connection lost during operation
    Disconnected,
    /// SSL/TLS negotiation failed
    Ssl,
    /// DNS resolution failed
    DnsResolution,
    /// Connection refused
    Refused,
    /// Connection pool exhausted
    PoolExhausted,
}

#[derive(Debug)]
pub struct QueryError {
    pub kind: QueryErrorKind,
    pub sql: Option<String>,
    pub sqlstate: Option<String>,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<usize>,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryErrorKind {
    /// Syntax error in SQL
    Syntax,
    /// Constraint violation (unique, foreign key, etc.)
    Constraint,
    /// Table or column not found
    NotFound,
    /// Permission denied
    Permission,
    /// Data too large for column
    DataTruncation,
    /// Deadlock detected
    Deadlock,
    /// Serialization failure (retry may succeed)
    Serialization,
    /// Statement timeout
    Timeout,
    /// Cancelled
    Cancelled,
    /// Other database error
    Database,
}

#[derive(Debug)]
pub struct TypeError {
    pub expected: &'static str,
    pub actual: String,
    pub column: Option<String>,
    pub rust_type: Option<&'static str>,
}

#[derive(Debug)]
pub struct TransactionError {
    pub kind: TransactionErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy)]
pub enum TransactionErrorKind {
    /// Already committed
    AlreadyCommitted,
    /// Already rolled back
    AlreadyRolledBack,
    /// Savepoint not found
    SavepointNotFound,
    /// Nested transaction not supported
    NestedNotSupported,
}

#[derive(Debug)]
pub struct ProtocolError {
    pub message: String,
    pub raw_data: Option<Vec<u8>>,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

#[derive(Debug)]
pub struct PoolError {
    pub kind: PoolErrorKind,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

#[derive(Debug, Clone, Copy)]
pub enum PoolErrorKind {
    /// Pool exhausted (no available connections)
    Exhausted,
    /// Connection checkout timeout
    Timeout,
    /// Pool is closed
    Closed,
    /// Configuration error
    Config,
}

#[derive(Debug)]
pub struct SchemaError {
    pub kind: SchemaErrorKind,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

#[derive(Debug, Clone, Copy)]
pub enum SchemaErrorKind {
    /// Table already exists
    TableExists,
    /// Table not found
    TableNotFound,
    /// Column already exists
    ColumnExists,
    /// Column not found
    ColumnNotFound,
    /// Invalid schema definition
    Invalid,
    /// Migration error
    Migration,
}

#[derive(Debug)]
pub struct ConfigError {
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Validation error for field-level and model-level validation.
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// The errors grouped by field name (or "_model" for model-level)
    pub errors: Vec<FieldValidationError>,
}

/// A single validation error for a field.
#[derive(Debug, Clone)]
pub struct FieldValidationError {
    /// The field name that failed validation
    pub field: String,
    /// The kind of validation that failed
    pub kind: ValidationErrorKind,
    /// Human-readable error message
    pub message: String,
}

/// The type of validation constraint that was violated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationErrorKind {
    /// Value is below minimum
    Min,
    /// Value is above maximum
    Max,
    /// String is shorter than minimum length
    MinLength,
    /// String is longer than maximum length
    MaxLength,
    /// Value doesn't match regex pattern
    Pattern,
    /// Required field is missing/null
    Required,
    /// Custom validation failed
    Custom,
}

impl ValidationError {
    /// Create a new empty validation error container.
    pub fn new() -> Self {
        Self { errors: Vec::new() }
    }

    /// Check if there are any validation errors.
    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }

    /// Add a field validation error.
    pub fn add(
        &mut self,
        field: impl Into<String>,
        kind: ValidationErrorKind,
        message: impl Into<String>,
    ) {
        self.errors.push(FieldValidationError {
            field: field.into(),
            kind,
            message: message.into(),
        });
    }

    /// Add a min value error.
    pub fn add_min(
        &mut self,
        field: impl Into<String>,
        min: impl std::fmt::Display,
        actual: impl std::fmt::Display,
    ) {
        self.add(
            field,
            ValidationErrorKind::Min,
            format!("must be at least {min}, got {actual}"),
        );
    }

    /// Add a max value error.
    pub fn add_max(
        &mut self,
        field: impl Into<String>,
        max: impl std::fmt::Display,
        actual: impl std::fmt::Display,
    ) {
        self.add(
            field,
            ValidationErrorKind::Max,
            format!("must be at most {max}, got {actual}"),
        );
    }

    /// Add a min length error.
    pub fn add_min_length(&mut self, field: impl Into<String>, min: usize, actual: usize) {
        self.add(
            field,
            ValidationErrorKind::MinLength,
            format!("must be at least {min} characters, got {actual}"),
        );
    }

    /// Add a max length error.
    pub fn add_max_length(&mut self, field: impl Into<String>, max: usize, actual: usize) {
        self.add(
            field,
            ValidationErrorKind::MaxLength,
            format!("must be at most {max} characters, got {actual}"),
        );
    }

    /// Add a pattern match error.
    pub fn add_pattern(&mut self, field: impl Into<String>, pattern: &str) {
        self.add(
            field,
            ValidationErrorKind::Pattern,
            format!("must match pattern '{pattern}'"),
        );
    }

    /// Add a required field error.
    pub fn add_required(&mut self, field: impl Into<String>) {
        self.add(
            field,
            ValidationErrorKind::Required,
            "is required".to_string(),
        );
    }

    /// Add a custom validation error.
    pub fn add_custom(&mut self, field: impl Into<String>, message: impl Into<String>) {
        self.add(field, ValidationErrorKind::Custom, message);
    }

    /// Convert to Result, returning Ok(()) if no errors, Err(self) otherwise.
    pub fn into_result(self) -> std::result::Result<(), Self> {
        if self.is_empty() { Ok(()) } else { Err(self) }
    }
}

impl Default for ValidationError {
    fn default() -> Self {
        Self::new()
    }
}

impl Error {
    /// Is this a retryable error (deadlock, serialization, pool exhausted, timeouts)?
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Query(q) => matches!(
                q.kind,
                QueryErrorKind::Deadlock | QueryErrorKind::Serialization | QueryErrorKind::Timeout
            ),
            Error::Pool(p) => matches!(p.kind, PoolErrorKind::Exhausted | PoolErrorKind::Timeout),
            Error::Connection(c) => matches!(c.kind, ConnectionErrorKind::PoolExhausted),
            Error::Timeout => true,
            _ => false,
        }
    }

    /// Is this a connection error that likely requires reconnection?
    pub fn is_connection_error(&self) -> bool {
        match self {
            Error::Connection(c) => matches!(
                c.kind,
                ConnectionErrorKind::Connect
                    | ConnectionErrorKind::Authentication
                    | ConnectionErrorKind::Disconnected
                    | ConnectionErrorKind::Ssl
                    | ConnectionErrorKind::DnsResolution
                    | ConnectionErrorKind::Refused
            ),
            Error::Protocol(_) | Error::Io(_) => true,
            _ => false,
        }
    }

    /// Get SQLSTATE if available (e.g., "23505" for unique violation)
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            Error::Query(q) => q.sqlstate.as_deref(),
            _ => None,
        }
    }

    /// Get the SQL that caused this error, if available
    pub fn sql(&self) -> Option<&str> {
        match self {
            Error::Query(q) => q.sql.as_deref(),
            _ => None,
        }
    }
}

impl QueryError {
    /// Is this a unique constraint violation?
    pub fn is_unique_violation(&self) -> bool {
        self.sqlstate.as_deref() == Some("23505")
    }

    /// Is this a foreign key violation?
    pub fn is_foreign_key_violation(&self) -> bool {
        self.sqlstate.as_deref() == Some("23503")
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Connection(e) => write!(f, "Connection error: {}", e.message),
            Error::Query(e) => {
                if let Some(sqlstate) = &e.sqlstate {
                    write!(f, "Query error (SQLSTATE {}): {}", sqlstate, e.message)
                } else {
                    write!(f, "Query error: {}", e.message)
                }
            }
            Error::Type(e) => {
                if let Some(col) = &e.column {
                    write!(
                        f,
                        "Type error in column '{}': expected {}, found {}",
                        col, e.expected, e.actual
                    )
                } else {
                    write!(f, "Type error: expected {}, found {}", e.expected, e.actual)
                }
            }
            Error::Transaction(e) => write!(f, "Transaction error: {}", e.message),
            Error::Protocol(e) => write!(f, "Protocol error: {}", e.message),
            Error::Pool(e) => write!(f, "Pool error: {}", e.message),
            Error::Schema(e) => write!(f, "Schema error: {}", e.message),
            Error::Config(e) => write!(f, "Configuration error: {}", e.message),
            Error::Validation(e) => write!(f, "Validation error: {}", e),
            Error::Io(e) => write!(f, "I/O error: {}", e),
            Error::Timeout => write!(f, "Operation timed out"),
            Error::Cancelled => write!(f, "Operation cancelled"),
            Error::Serde(msg) => write!(f, "Serialization error: {}", msg),
            Error::Custom(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Connection(e) => e
                .source
                .as_deref()
                .map(|err| err as &(dyn std::error::Error + 'static)),
            Error::Query(e) => e
                .source
                .as_deref()
                .map(|err| err as &(dyn std::error::Error + 'static)),
            Error::Protocol(e) => e
                .source
                .as_deref()
                .map(|err| err as &(dyn std::error::Error + 'static)),
            Error::Pool(e) => e
                .source
                .as_deref()
                .map(|err| err as &(dyn std::error::Error + 'static)),
            Error::Schema(e) => e
                .source
                .as_deref()
                .map(|err| err as &(dyn std::error::Error + 'static)),
            Error::Config(e) => e
                .source
                .as_deref()
                .map(|err| err as &(dyn std::error::Error + 'static)),
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(sqlstate) = &self.sqlstate {
            write!(f, "{} (SQLSTATE {})", self.message, sqlstate)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl fmt::Display for TypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(col) = &self.column {
            write!(
                f,
                "expected {} for column '{}', found {}",
                self.expected, col, self.actual
            )
        } else {
            write!(f, "expected {}, found {}", self.expected, self.actual)
        }
    }
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.errors.is_empty() {
            write!(f, "validation passed")
        } else if self.errors.len() == 1 {
            let err = &self.errors[0];
            write!(f, "validation error on '{}': {}", err.field, err.message)
        } else {
            writeln!(f, "validation errors:")?;
            for err in &self.errors {
                writeln!(f, "  - {}: {}", err.field, err.message)?;
            }
            Ok(())
        }
    }
}

impl std::error::Error for ValidationError {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<ConnectionError> for Error {
    fn from(err: ConnectionError) -> Self {
        Error::Connection(err)
    }
}

impl From<QueryError> for Error {
    fn from(err: QueryError) -> Self {
        Error::Query(err)
    }
}

impl From<TypeError> for Error {
    fn from(err: TypeError) -> Self {
        Error::Type(err)
    }
}

impl From<TransactionError> for Error {
    fn from(err: TransactionError) -> Self {
        Error::Transaction(err)
    }
}

impl From<ProtocolError> for Error {
    fn from(err: ProtocolError) -> Self {
        Error::Protocol(err)
    }
}

impl From<PoolError> for Error {
    fn from(err: PoolError) -> Self {
        Error::Pool(err)
    }
}

impl From<SchemaError> for Error {
    fn from(err: SchemaError) -> Self {
        Error::Schema(err)
    }
}

impl From<ConfigError> for Error {
    fn from(err: ConfigError) -> Self {
        Error::Config(err)
    }
}

impl From<ValidationError> for Error {
    fn from(err: ValidationError) -> Self {
        Error::Validation(err)
    }
}

/// Result type alias for SQLModel operations.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlstate_helpers() {
        let query = QueryError {
            kind: QueryErrorKind::Constraint,
            sql: Some("SELECT 1".to_string()),
            sqlstate: Some("23505".to_string()),
            message: "unique violation".to_string(),
            detail: None,
            hint: None,
            position: None,
            source: None,
        };

        assert!(query.is_unique_violation());
        assert!(!query.is_foreign_key_violation());

        let err = Error::Query(query);
        assert_eq!(err.sqlstate(), Some("23505"));
        assert_eq!(err.sql(), Some("SELECT 1"));
    }

    #[test]
    fn retryable_and_connection_flags() {
        let retryable_query = Error::Query(QueryError {
            kind: QueryErrorKind::Deadlock,
            sql: None,
            sqlstate: None,
            message: "deadlock detected".to_string(),
            detail: None,
            hint: None,
            position: None,
            source: None,
        });

        let pool_exhausted = Error::Pool(PoolError {
            kind: PoolErrorKind::Exhausted,
            message: "pool exhausted".to_string(),
            source: None,
        });

        let conn_exhausted = Error::Connection(ConnectionError {
            kind: ConnectionErrorKind::PoolExhausted,
            message: "pool exhausted".to_string(),
            source: None,
        });

        assert!(retryable_query.is_retryable());
        assert!(pool_exhausted.is_retryable());
        assert!(conn_exhausted.is_retryable());

        let conn_error = Error::Connection(ConnectionError {
            kind: ConnectionErrorKind::Disconnected,
            message: "lost connection".to_string(),
            source: None,
        });
        assert!(conn_error.is_connection_error());
    }
}
