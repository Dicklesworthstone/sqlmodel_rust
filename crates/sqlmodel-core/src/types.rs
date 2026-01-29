//! SQL type definitions and mapping.

/// SQL data types supported by SQLModel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlType {
    // Integer types
    TinyInt,
    SmallInt,
    Integer,
    BigInt,

    // Floating point
    Real,
    Double,

    // Fixed precision
    Numeric { precision: u8, scale: u8 },
    Decimal { precision: u8, scale: u8 },

    // Boolean
    Boolean,

    // String types
    Char(u32),
    VarChar(u32),
    Text,

    // Binary types
    Binary(u32),
    VarBinary(u32),
    Blob,

    // Date/time types
    Date,
    Time,
    DateTime,
    Timestamp,
    TimestampTz,

    // UUID
    Uuid,

    // JSON
    Json,
    JsonB,

    // Arrays (PostgreSQL)
    Array(Box<SqlType>),

    // Enum type with allowed values
    Enum(Vec<&'static str>),

    // Custom type name
    Custom(&'static str),
}

impl SqlType {
    /// Get the SQL type name for this type.
    pub fn sql_name(&self) -> String {
        match self {
            SqlType::TinyInt => "TINYINT".to_string(),
            SqlType::SmallInt => "SMALLINT".to_string(),
            SqlType::Integer => "INTEGER".to_string(),
            SqlType::BigInt => "BIGINT".to_string(),
            SqlType::Real => "REAL".to_string(),
            SqlType::Double => "DOUBLE PRECISION".to_string(),
            SqlType::Numeric { precision, scale } => format!("NUMERIC({}, {})", precision, scale),
            SqlType::Decimal { precision, scale } => format!("DECIMAL({}, {})", precision, scale),
            SqlType::Boolean => "BOOLEAN".to_string(),
            SqlType::Char(len) => format!("CHAR({})", len),
            SqlType::VarChar(len) => format!("VARCHAR({})", len),
            SqlType::Text => "TEXT".to_string(),
            SqlType::Binary(len) => format!("BINARY({})", len),
            SqlType::VarBinary(len) => format!("VARBINARY({})", len),
            SqlType::Blob => "BLOB".to_string(),
            SqlType::Date => "DATE".to_string(),
            SqlType::Time => "TIME".to_string(),
            SqlType::DateTime => "DATETIME".to_string(),
            SqlType::Timestamp => "TIMESTAMP".to_string(),
            SqlType::TimestampTz => "TIMESTAMPTZ".to_string(),
            SqlType::Uuid => "UUID".to_string(),
            SqlType::Json => "JSON".to_string(),
            SqlType::JsonB => "JSONB".to_string(),
            SqlType::Enum(_) => {
                // Default: just use TEXT; dialect-specific DDL handles the real type
                "TEXT".to_string()
            }
            SqlType::Array(inner) => format!("{}[]", inner.sql_name()),
            SqlType::Custom(name) => name.to_string(),
        }
    }

    /// Check if this type is numeric.
    pub const fn is_numeric(&self) -> bool {
        matches!(
            self,
            SqlType::TinyInt
                | SqlType::SmallInt
                | SqlType::Integer
                | SqlType::BigInt
                | SqlType::Real
                | SqlType::Double
                | SqlType::Numeric { .. }
                | SqlType::Decimal { .. }
        )
    }

    /// Check if this type is text-based.
    pub const fn is_text(&self) -> bool {
        matches!(self, SqlType::Char(_) | SqlType::VarChar(_) | SqlType::Text)
    }

    /// Check if this type is a date/time type.
    pub const fn is_temporal(&self) -> bool {
        matches!(
            self,
            SqlType::Date
                | SqlType::Time
                | SqlType::DateTime
                | SqlType::Timestamp
                | SqlType::TimestampTz
        )
    }
}

/// Trait for Rust enums that map to SQL enum types.
///
/// Implement this trait to enable automatic conversion between Rust enums
/// and their SQL string representations. The `SqlEnum` derive macro
/// generates this implementation automatically.
///
/// # Example
///
/// ```ignore
/// #[derive(SqlEnum)]
/// enum Status {
///     Active,
///     Inactive,
///     Pending,
/// }
/// ```
pub trait SqlEnum: Sized {
    /// All valid string values for this enum.
    const VARIANTS: &'static [&'static str];

    /// The SQL enum type name (typically the enum's snake_case name).
    const TYPE_NAME: &'static str;

    /// Convert the enum to its string representation.
    fn to_sql_str(&self) -> &'static str;

    /// Parse from a string representation.
    fn from_sql_str(s: &str) -> Result<Self, String>;
}

/// Trait for types that have a corresponding SQL type.
pub trait TypeInfo {
    /// The SQL type for this Rust type.
    const SQL_TYPE: SqlType;

    /// Whether this type is nullable by default.
    const NULLABLE: bool = false;
}

// Implement TypeInfo for common Rust types
impl TypeInfo for i8 {
    const SQL_TYPE: SqlType = SqlType::TinyInt;
}

impl TypeInfo for i16 {
    const SQL_TYPE: SqlType = SqlType::SmallInt;
}

impl TypeInfo for i32 {
    const SQL_TYPE: SqlType = SqlType::Integer;
}

impl TypeInfo for i64 {
    const SQL_TYPE: SqlType = SqlType::BigInt;
}

impl TypeInfo for f32 {
    const SQL_TYPE: SqlType = SqlType::Real;
}

impl TypeInfo for f64 {
    const SQL_TYPE: SqlType = SqlType::Double;
}

impl TypeInfo for bool {
    const SQL_TYPE: SqlType = SqlType::Boolean;
}

impl TypeInfo for String {
    const SQL_TYPE: SqlType = SqlType::Text;
}

impl TypeInfo for Vec<u8> {
    const SQL_TYPE: SqlType = SqlType::Blob;
}

impl<T: TypeInfo> TypeInfo for Option<T> {
    const SQL_TYPE: SqlType = T::SQL_TYPE;
    const NULLABLE: bool = true;
}
