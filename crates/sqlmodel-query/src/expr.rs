//! SQL expressions for query building.
//!
//! This module provides a type-safe expression system for building
//! WHERE clauses, ORDER BY, computed columns, and other SQL expressions.

use crate::clause::{OrderBy, OrderDirection};
use sqlmodel_core::Value;

/// SQL dialect for generating dialect-specific SQL.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Dialect {
    /// PostgreSQL dialect (uses $1, $2 placeholders)
    #[default]
    Postgres,
    /// SQLite dialect (uses ?1, ?2 placeholders)
    Sqlite,
    /// MySQL dialect (uses ? placeholders)
    Mysql,
}

impl Dialect {
    /// Generate a placeholder for the given parameter index (1-based).
    pub fn placeholder(self, index: usize) -> String {
        match self {
            Dialect::Postgres => format!("${index}"),
            Dialect::Sqlite => format!("?{index}"),
            Dialect::Mysql => "?".to_string(),
        }
    }

    /// Get the string concatenation operator for this dialect.
    pub const fn concat_op(self) -> &'static str {
        match self {
            Dialect::Postgres | Dialect::Sqlite => "||",
            Dialect::Mysql => "", // MySQL uses CONCAT() function
        }
    }

    /// Check if this dialect supports ILIKE.
    pub const fn supports_ilike(self) -> bool {
        matches!(self, Dialect::Postgres)
    }

    /// Quote an identifier for this dialect.
    ///
    /// Properly escapes embedded quote characters by doubling them:
    /// - For Postgres/SQLite: `"` becomes `""`
    /// - For MySQL: `` ` `` becomes ``` `` ```
    pub fn quote_identifier(self, name: &str) -> String {
        match self {
            Dialect::Postgres | Dialect::Sqlite => {
                let escaped = name.replace('"', "\"\"");
                format!("\"{}\"", escaped)
            }
            Dialect::Mysql => {
                let escaped = name.replace('`', "``");
                format!("`{}`", escaped)
            }
        }
    }
}

/// A SQL expression that can be used in WHERE, HAVING, etc.
#[derive(Debug, Clone)]
pub enum Expr {
    /// Column reference with optional table qualifier
    Column {
        /// Optional table name or alias
        table: Option<String>,
        /// Column name
        name: String,
    },

    /// Literal value
    Literal(Value),

    /// Explicit placeholder for bound parameters
    Placeholder(usize),

    /// Binary operation (e.g., a = b, a > b)
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },

    /// Unary operation (e.g., NOT a, -a)
    Unary { op: UnaryOp, expr: Box<Expr> },

    /// Function call (e.g., COUNT(*), UPPER(name))
    Function { name: String, args: Vec<Expr> },

    /// CASE WHEN ... THEN ... ELSE ... END
    Case {
        /// List of (condition, result) pairs
        when_clauses: Vec<(Expr, Expr)>,
        /// Optional ELSE clause
        else_clause: Option<Box<Expr>>,
    },

    /// IN expression
    In {
        expr: Box<Expr>,
        values: Vec<Expr>,
        negated: bool,
    },

    /// BETWEEN expression
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },

    /// IS NULL / IS NOT NULL
    IsNull { expr: Box<Expr>, negated: bool },

    /// IS DISTINCT FROM / IS NOT DISTINCT FROM (NULL-safe comparison)
    IsDistinctFrom {
        left: Box<Expr>,
        right: Box<Expr>,
        negated: bool,
    },

    /// CAST(expr AS type)
    Cast { expr: Box<Expr>, type_name: String },

    /// LIKE / NOT LIKE pattern
    Like {
        expr: Box<Expr>,
        pattern: String,
        negated: bool,
        case_insensitive: bool,
    },

    /// Subquery (stores the SQL string)
    Subquery(String),

    /// EXISTS (subquery) / NOT EXISTS (subquery)
    ///
    /// Used for subquery existence checks in WHERE clauses.
    Exists {
        /// The subquery SQL string
        subquery: String,
        /// Parameters for the subquery
        params: Vec<Value>,
        /// Whether this is NOT EXISTS
        negated: bool,
    },

    /// Raw SQL fragment (escape hatch)
    Raw(String),

    /// Parenthesized expression
    Paren(Box<Expr>),

    /// Special aggregate: COUNT(*)
    CountStar,

    /// Window function with OVER clause
    Window {
        /// The function expression (aggregate or window function)
        function: Box<Expr>,
        /// PARTITION BY expressions
        partition_by: Vec<Expr>,
        /// ORDER BY clauses within the window
        order_by: Vec<OrderBy>,
        /// Frame specification (ROWS or RANGE)
        frame: Option<WindowFrame>,
    },

    /// JSON path extraction (returns JSON value)
    ///
    /// - PostgreSQL: `expr -> 'path'` or `expr -> path_expr`
    /// - MySQL: `JSON_EXTRACT(expr, '$.path')`
    /// - SQLite: `json_extract(expr, '$.path')`
    JsonExtract {
        /// The JSON expression to extract from
        expr: Box<Expr>,
        /// The path to extract (can be a key name or array index)
        path: JsonPath,
    },

    /// JSON path extraction as text (returns text/string)
    ///
    /// - PostgreSQL: `expr ->> 'path'`
    /// - MySQL: `JSON_UNQUOTE(JSON_EXTRACT(expr, '$.path'))`
    /// - SQLite: `json_extract(expr, '$.path')` (SQLite returns text by default)
    JsonExtractText {
        /// The JSON expression to extract from
        expr: Box<Expr>,
        /// The path to extract
        path: JsonPath,
    },

    /// JSON path extraction with nested path (returns JSON)
    ///
    /// - PostgreSQL: `expr #> '{path, to, value}'`
    /// - MySQL/SQLite: `JSON_EXTRACT(expr, '$.path.to.value')`
    JsonExtractPath {
        /// The JSON expression
        expr: Box<Expr>,
        /// Nested path segments
        path: Vec<String>,
    },

    /// JSON path extraction with nested path as text
    ///
    /// - PostgreSQL: `expr #>> '{path, to, value}'`
    /// - MySQL: `JSON_UNQUOTE(JSON_EXTRACT(expr, '$.path.to.value'))`
    /// - SQLite: `json_extract(expr, '$.path.to.value')`
    JsonExtractPathText {
        /// The JSON expression
        expr: Box<Expr>,
        /// Nested path segments
        path: Vec<String>,
    },

    /// JSON containment check (left contains right)
    ///
    /// - PostgreSQL: `expr @> other` (JSONB only)
    /// - MySQL: `JSON_CONTAINS(expr, other)`
    /// - SQLite: Not directly supported (requires json_each workaround)
    JsonContains {
        /// The JSON expression to check
        expr: Box<Expr>,
        /// The JSON value to check for
        other: Box<Expr>,
    },

    /// JSON contained-by check (left is contained by right)
    ///
    /// - PostgreSQL: `expr <@ other` (JSONB only)
    /// - MySQL: `JSON_CONTAINS(other, expr)`
    /// - SQLite: Not directly supported
    JsonContainedBy {
        /// The JSON expression to check
        expr: Box<Expr>,
        /// The containing JSON value
        other: Box<Expr>,
    },

    /// JSON key existence check
    ///
    /// - PostgreSQL: `expr ? 'key'` (JSONB only)
    /// - MySQL: `JSON_CONTAINS_PATH(expr, 'one', '$.key')`
    /// - SQLite: `json_type(expr, '$.key') IS NOT NULL`
    JsonHasKey {
        /// The JSON expression
        expr: Box<Expr>,
        /// The key to check for
        key: String,
    },

    /// JSON any key existence (has any of the keys)
    ///
    /// - PostgreSQL: `expr ?| array['key1', 'key2']` (JSONB only)
    /// - MySQL: `JSON_CONTAINS_PATH(expr, 'one', '$.key1', '$.key2')`
    /// - SQLite: Requires OR of json_type checks
    JsonHasAnyKey {
        /// The JSON expression
        expr: Box<Expr>,
        /// The keys to check for
        keys: Vec<String>,
    },

    /// JSON all keys existence (has all of the keys)
    ///
    /// - PostgreSQL: `expr ?& array['key1', 'key2']` (JSONB only)
    /// - MySQL: `JSON_CONTAINS_PATH(expr, 'all', '$.key1', '$.key2')`
    /// - SQLite: Requires AND of json_type checks
    JsonHasAllKeys {
        /// The JSON expression
        expr: Box<Expr>,
        /// The keys to check for
        keys: Vec<String>,
    },

    /// JSON array length
    ///
    /// - PostgreSQL: `jsonb_array_length(expr)`
    /// - MySQL: `JSON_LENGTH(expr)`
    /// - SQLite: `json_array_length(expr)`
    JsonArrayLength {
        /// The JSON array expression
        expr: Box<Expr>,
    },

    /// JSON typeof (returns the type of the JSON value)
    ///
    /// - PostgreSQL: `jsonb_typeof(expr)`
    /// - MySQL: `JSON_TYPE(expr)`
    /// - SQLite: `json_type(expr)`
    JsonTypeof {
        /// The JSON expression
        expr: Box<Expr>,
    },
}

/// JSON path segment for extraction operations.
#[derive(Debug, Clone)]
pub enum JsonPath {
    /// Object key access (e.g., `-> 'name'`)
    Key(String),
    /// Array index access (e.g., `-> 0`)
    Index(i64),
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    // Comparison
    /// Equal (=)
    Eq,
    /// Not equal (<>)
    Ne,
    /// Less than (<)
    Lt,
    /// Less than or equal (<=)
    Le,
    /// Greater than (>)
    Gt,
    /// Greater than or equal (>=)
    Ge,

    // Logical
    /// Logical AND
    And,
    /// Logical OR
    Or,

    // Arithmetic
    /// Addition (+)
    Add,
    /// Subtraction (-)
    Sub,
    /// Multiplication (*)
    Mul,
    /// Division (/)
    Div,
    /// Modulo (%)
    Mod,

    // Bitwise
    /// Bitwise AND (&)
    BitAnd,
    /// Bitwise OR (|)
    BitOr,
    /// Bitwise XOR (^)
    BitXor,

    // String
    /// String concatenation (||)
    Concat,

    // Array (PostgreSQL)
    /// Array contains (@>)
    ArrayContains,
    /// Array is contained by (<@)
    ArrayContainedBy,
    /// Array overlap (&&)
    ArrayOverlap,
}

impl BinaryOp {
    /// Get the SQL representation of this operator.
    pub const fn as_str(self) -> &'static str {
        match self {
            BinaryOp::Eq => "=",
            BinaryOp::Ne => "<>",
            BinaryOp::Lt => "<",
            BinaryOp::Le => "<=",
            BinaryOp::Gt => ">",
            BinaryOp::Ge => ">=",
            BinaryOp::And => "AND",
            BinaryOp::Or => "OR",
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::Mod => "%",
            BinaryOp::BitAnd => "&",
            BinaryOp::BitOr => "|",
            BinaryOp::BitXor => "^",
            BinaryOp::Concat => "||",
            BinaryOp::ArrayContains => "@>",
            BinaryOp::ArrayContainedBy => "<@",
            BinaryOp::ArrayOverlap => "&&",
        }
    }

    /// Get the precedence of this operator (higher = binds tighter).
    pub const fn precedence(self) -> u8 {
        match self {
            BinaryOp::Or => 1,
            BinaryOp::And => 2,
            BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Lt
            | BinaryOp::Le
            | BinaryOp::Gt
            | BinaryOp::Ge
            | BinaryOp::ArrayContains
            | BinaryOp::ArrayContainedBy
            | BinaryOp::ArrayOverlap => 3,
            BinaryOp::BitOr => 4,
            BinaryOp::BitXor => 5,
            BinaryOp::BitAnd => 6,
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Concat => 7,
            BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => 8,
        }
    }
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
    BitwiseNot,
}

impl UnaryOp {
    /// Get the SQL representation of this operator.
    pub const fn as_str(&self) -> &'static str {
        match self {
            UnaryOp::Not => "NOT",
            UnaryOp::Neg => "-",
            UnaryOp::BitwiseNot => "~",
        }
    }
}

// ==================== Window Frame ====================

/// Window frame specification for OVER clause.
#[derive(Debug, Clone)]
pub struct WindowFrame {
    /// Frame type: ROWS or RANGE
    pub frame_type: WindowFrameType,
    /// Frame start bound
    pub start: WindowFrameBound,
    /// Frame end bound (if BETWEEN is used)
    pub end: Option<WindowFrameBound>,
}

/// Window frame type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameType {
    /// ROWS - physical rows
    Rows,
    /// RANGE - logical range based on ORDER BY values
    Range,
    /// GROUPS - groups of peer rows (PostgreSQL 11+)
    Groups,
}

impl WindowFrameType {
    /// Get the SQL keyword for this frame type.
    pub const fn as_str(self) -> &'static str {
        match self {
            WindowFrameType::Rows => "ROWS",
            WindowFrameType::Range => "RANGE",
            WindowFrameType::Groups => "GROUPS",
        }
    }
}

/// Window frame bound specification.
#[derive(Debug, Clone)]
pub enum WindowFrameBound {
    /// UNBOUNDED PRECEDING
    UnboundedPreceding,
    /// UNBOUNDED FOLLOWING
    UnboundedFollowing,
    /// CURRENT ROW
    CurrentRow,
    /// N PRECEDING
    Preceding(u64),
    /// N FOLLOWING
    Following(u64),
}

impl WindowFrameBound {
    /// Generate SQL for this frame bound.
    pub fn to_sql(&self) -> String {
        match self {
            WindowFrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING".to_string(),
            WindowFrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_string(),
            WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
            WindowFrameBound::Preceding(n) => format!("{n} PRECEDING"),
            WindowFrameBound::Following(n) => format!("{n} FOLLOWING"),
        }
    }
}

impl Expr {
    // ==================== Constructors ====================

    /// Create a column reference expression.
    pub fn col(name: impl Into<String>) -> Self {
        Expr::Column {
            table: None,
            name: name.into(),
        }
    }

    /// Create a qualified column reference (table.column).
    pub fn qualified(table: impl Into<String>, column: impl Into<String>) -> Self {
        Expr::Column {
            table: Some(table.into()),
            name: column.into(),
        }
    }

    /// Create a literal value expression.
    pub fn lit(value: impl Into<Value>) -> Self {
        Expr::Literal(value.into())
    }

    /// Create a NULL literal.
    pub fn null() -> Self {
        Expr::Literal(Value::Null)
    }

    /// Create a raw SQL expression (escape hatch).
    pub fn raw(sql: impl Into<String>) -> Self {
        Expr::Raw(sql.into())
    }

    /// Create a placeholder for bound parameters.
    pub fn placeholder(index: usize) -> Self {
        Expr::Placeholder(index)
    }

    // ==================== Comparison Operators ====================

    /// Equal to (=)
    pub fn eq(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Eq,
            right: Box::new(other.into()),
        }
    }

    /// Not equal to (<>)
    pub fn ne(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Ne,
            right: Box::new(other.into()),
        }
    }

    /// Less than (<)
    pub fn lt(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Lt,
            right: Box::new(other.into()),
        }
    }

    /// Less than or equal to (<=)
    pub fn le(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Le,
            right: Box::new(other.into()),
        }
    }

    /// Greater than (>)
    pub fn gt(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Gt,
            right: Box::new(other.into()),
        }
    }

    /// Greater than or equal to (>=)
    pub fn ge(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Ge,
            right: Box::new(other.into()),
        }
    }

    // ==================== Logical Operators ====================

    /// Logical AND
    pub fn and(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::And,
            right: Box::new(other.into()),
        }
    }

    /// Logical OR
    pub fn or(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Or,
            right: Box::new(other.into()),
        }
    }

    /// Logical NOT
    pub fn not(self) -> Self {
        Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(self),
        }
    }

    // ==================== Null Checks ====================

    /// IS NULL
    pub fn is_null(self) -> Self {
        Expr::IsNull {
            expr: Box::new(self),
            negated: false,
        }
    }

    /// IS NOT NULL
    pub fn is_not_null(self) -> Self {
        Expr::IsNull {
            expr: Box::new(self),
            negated: true,
        }
    }

    /// IS DISTINCT FROM (NULL-safe comparison: returns TRUE/FALSE, never NULL)
    ///
    /// Unlike `!=`, this returns TRUE when comparing NULL to a non-NULL value,
    /// and FALSE when comparing NULL to NULL.
    pub fn is_distinct_from(self, other: impl Into<Expr>) -> Self {
        Expr::IsDistinctFrom {
            left: Box::new(self),
            right: Box::new(other.into()),
            negated: false,
        }
    }

    /// IS NOT DISTINCT FROM (NULL-safe equality: returns TRUE/FALSE, never NULL)
    ///
    /// Unlike `=`, this returns TRUE when comparing NULL to NULL,
    /// and FALSE when comparing NULL to a non-NULL value.
    pub fn is_not_distinct_from(self, other: impl Into<Expr>) -> Self {
        Expr::IsDistinctFrom {
            left: Box::new(self),
            right: Box::new(other.into()),
            negated: true,
        }
    }

    // ==================== Type Casting ====================

    /// CAST expression to a specific SQL type.
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("price").cast("DECIMAL(10, 2)")
    /// // Generates: CAST("price" AS DECIMAL(10, 2))
    /// ```
    pub fn cast(self, type_name: impl Into<String>) -> Self {
        Expr::Cast {
            expr: Box::new(self),
            type_name: type_name.into(),
        }
    }

    // ==================== Pattern Matching ====================

    /// LIKE pattern match
    pub fn like(self, pattern: impl Into<String>) -> Self {
        Expr::Like {
            expr: Box::new(self),
            pattern: pattern.into(),
            negated: false,
            case_insensitive: false,
        }
    }

    /// NOT LIKE pattern match
    pub fn not_like(self, pattern: impl Into<String>) -> Self {
        Expr::Like {
            expr: Box::new(self),
            pattern: pattern.into(),
            negated: true,
            case_insensitive: false,
        }
    }

    /// ILIKE (case-insensitive) pattern match (PostgreSQL)
    pub fn ilike(self, pattern: impl Into<String>) -> Self {
        Expr::Like {
            expr: Box::new(self),
            pattern: pattern.into(),
            negated: false,
            case_insensitive: true,
        }
    }

    /// NOT ILIKE pattern match (PostgreSQL)
    pub fn not_ilike(self, pattern: impl Into<String>) -> Self {
        Expr::Like {
            expr: Box::new(self),
            pattern: pattern.into(),
            negated: true,
            case_insensitive: true,
        }
    }

    /// Check if column contains the given substring (LIKE '%pattern%').
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("name").contains("man")
    /// // Generates: "name" LIKE '%man%'
    /// ```
    pub fn contains(self, pattern: impl AsRef<str>) -> Self {
        let pattern = format!("%{}%", pattern.as_ref());
        Expr::Like {
            expr: Box::new(self),
            pattern,
            negated: false,
            case_insensitive: false,
        }
    }

    /// Check if column starts with the given prefix (LIKE 'pattern%').
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("name").starts_with("Spider")
    /// // Generates: "name" LIKE 'Spider%'
    /// ```
    pub fn starts_with(self, pattern: impl AsRef<str>) -> Self {
        let pattern = format!("{}%", pattern.as_ref());
        Expr::Like {
            expr: Box::new(self),
            pattern,
            negated: false,
            case_insensitive: false,
        }
    }

    /// Check if column ends with the given suffix (LIKE '%pattern').
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("name").ends_with("man")
    /// // Generates: "name" LIKE '%man'
    /// ```
    pub fn ends_with(self, pattern: impl AsRef<str>) -> Self {
        let pattern = format!("%{}", pattern.as_ref());
        Expr::Like {
            expr: Box::new(self),
            pattern,
            negated: false,
            case_insensitive: false,
        }
    }

    /// Case-insensitive contains (ILIKE '%pattern%' or LOWER fallback).
    pub fn icontains(self, pattern: impl AsRef<str>) -> Self {
        let pattern = format!("%{}%", pattern.as_ref());
        Expr::Like {
            expr: Box::new(self),
            pattern,
            negated: false,
            case_insensitive: true,
        }
    }

    /// Case-insensitive starts_with (ILIKE 'pattern%' or LOWER fallback).
    pub fn istarts_with(self, pattern: impl AsRef<str>) -> Self {
        let pattern = format!("{}%", pattern.as_ref());
        Expr::Like {
            expr: Box::new(self),
            pattern,
            negated: false,
            case_insensitive: true,
        }
    }

    /// Case-insensitive ends_with (ILIKE '%pattern' or LOWER fallback).
    pub fn iends_with(self, pattern: impl AsRef<str>) -> Self {
        let pattern = format!("%{}", pattern.as_ref());
        Expr::Like {
            expr: Box::new(self),
            pattern,
            negated: false,
            case_insensitive: true,
        }
    }

    // ==================== IN Expressions ====================

    /// IN list of values
    pub fn in_list(self, values: Vec<impl Into<Expr>>) -> Self {
        if values.is_empty() {
            return Expr::raw("1 = 0");
        }
        Expr::In {
            expr: Box::new(self),
            values: values.into_iter().map(Into::into).collect(),
            negated: false,
        }
    }

    /// NOT IN list of values
    pub fn not_in_list(self, values: Vec<impl Into<Expr>>) -> Self {
        if values.is_empty() {
            return Expr::raw("1 = 1");
        }
        Expr::In {
            expr: Box::new(self),
            values: values.into_iter().map(Into::into).collect(),
            negated: true,
        }
    }

    // ==================== BETWEEN ====================

    /// BETWEEN low AND high
    pub fn between(self, low: impl Into<Expr>, high: impl Into<Expr>) -> Self {
        Expr::Between {
            expr: Box::new(self),
            low: Box::new(low.into()),
            high: Box::new(high.into()),
            negated: false,
        }
    }

    /// NOT BETWEEN low AND high
    pub fn not_between(self, low: impl Into<Expr>, high: impl Into<Expr>) -> Self {
        Expr::Between {
            expr: Box::new(self),
            low: Box::new(low.into()),
            high: Box::new(high.into()),
            negated: true,
        }
    }

    // ==================== Arithmetic Operators ====================

    /// Addition (+)
    pub fn add(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Add,
            right: Box::new(other.into()),
        }
    }

    /// Subtraction (-)
    pub fn sub(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Sub,
            right: Box::new(other.into()),
        }
    }

    /// Multiplication (*)
    pub fn mul(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Mul,
            right: Box::new(other.into()),
        }
    }

    /// Division (/)
    pub fn div(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Div,
            right: Box::new(other.into()),
        }
    }

    /// Modulo (%)
    pub fn modulo(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Mod,
            right: Box::new(other.into()),
        }
    }

    /// Negation (unary -)
    pub fn neg(self) -> Self {
        Expr::Unary {
            op: UnaryOp::Neg,
            expr: Box::new(self),
        }
    }

    // ==================== String Operations ====================

    /// String concatenation (||)
    pub fn concat(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Concat,
            right: Box::new(other.into()),
        }
    }

    // ==================== Array Operations (PostgreSQL) ====================

    /// Array contains (@>). Tests if this array contains all elements of `other`.
    pub fn array_contains(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::ArrayContains,
            right: Box::new(other.into()),
        }
    }

    /// Array contained by (<@). Tests if this array is contained by `other`.
    pub fn array_contained_by(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::ArrayContainedBy,
            right: Box::new(other.into()),
        }
    }

    /// Array overlap (&&). Tests if this array has any elements in common with `other`.
    pub fn array_overlap(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::ArrayOverlap,
            right: Box::new(other.into()),
        }
    }

    /// ANY(array) = value. Tests if any element of the array equals the value.
    ///
    /// Generates: `value = ANY(array_column)`
    pub fn array_any_eq(self, value: impl Into<Expr>) -> Self {
        let val = value.into();
        Expr::Binary {
            left: Box::new(val),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Function {
                name: "ANY".to_string(),
                args: vec![self],
            }),
        }
    }

    // ==================== Bitwise Operators ====================

    /// Bitwise AND (&)
    pub fn bit_and(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::BitAnd,
            right: Box::new(other.into()),
        }
    }

    /// Bitwise OR (|)
    pub fn bit_or(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::BitOr,
            right: Box::new(other.into()),
        }
    }

    /// Bitwise XOR (^)
    pub fn bit_xor(self, other: impl Into<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::BitXor,
            right: Box::new(other.into()),
        }
    }

    /// Bitwise NOT (~)
    pub fn bit_not(self) -> Self {
        Expr::Unary {
            op: UnaryOp::BitwiseNot,
            expr: Box::new(self),
        }
    }

    // ==================== CASE Expression ====================

    /// Start building a CASE expression.
    ///
    /// # Example
    /// ```ignore
    /// Expr::case()
    ///     .when(Expr::col("status").eq("active"), "Yes")
    ///     .when(Expr::col("status").eq("pending"), "Maybe")
    ///     .otherwise("No")
    /// ```
    pub fn case() -> CaseBuilder {
        CaseBuilder {
            when_clauses: Vec::new(),
        }
    }

    // ==================== Aggregate Functions ====================

    /// COUNT(*) aggregate function.
    pub fn count_star() -> Self {
        Expr::CountStar
    }

    /// COUNT(expr) aggregate function.
    pub fn count(self) -> Self {
        Expr::Function {
            name: "COUNT".to_string(),
            args: vec![self],
        }
    }

    /// SUM(expr) aggregate function.
    pub fn sum(self) -> Self {
        Expr::Function {
            name: "SUM".to_string(),
            args: vec![self],
        }
    }

    /// AVG(expr) aggregate function.
    pub fn avg(self) -> Self {
        Expr::Function {
            name: "AVG".to_string(),
            args: vec![self],
        }
    }

    /// MIN(expr) aggregate function.
    pub fn min(self) -> Self {
        Expr::Function {
            name: "MIN".to_string(),
            args: vec![self],
        }
    }

    /// MAX(expr) aggregate function.
    pub fn max(self) -> Self {
        Expr::Function {
            name: "MAX".to_string(),
            args: vec![self],
        }
    }

    /// Create a generic function call.
    pub fn function(name: impl Into<String>, args: Vec<Expr>) -> Self {
        Expr::Function {
            name: name.into(),
            args,
        }
    }

    // ==================== Window Functions ====================

    /// ROW_NUMBER() window function.
    /// Returns the sequential number of a row within a partition.
    pub fn row_number() -> Self {
        Expr::Function {
            name: "ROW_NUMBER".to_string(),
            args: vec![],
        }
    }

    /// RANK() window function.
    /// Returns the rank of the current row with gaps.
    pub fn rank() -> Self {
        Expr::Function {
            name: "RANK".to_string(),
            args: vec![],
        }
    }

    /// DENSE_RANK() window function.
    /// Returns the rank of the current row without gaps.
    pub fn dense_rank() -> Self {
        Expr::Function {
            name: "DENSE_RANK".to_string(),
            args: vec![],
        }
    }

    /// PERCENT_RANK() window function.
    /// Returns the relative rank of the current row.
    pub fn percent_rank() -> Self {
        Expr::Function {
            name: "PERCENT_RANK".to_string(),
            args: vec![],
        }
    }

    /// CUME_DIST() window function.
    /// Returns the cumulative distribution of a value.
    pub fn cume_dist() -> Self {
        Expr::Function {
            name: "CUME_DIST".to_string(),
            args: vec![],
        }
    }

    /// NTILE(n) window function.
    /// Divides rows into n groups and returns the group number.
    pub fn ntile(n: i64) -> Self {
        Expr::Function {
            name: "NTILE".to_string(),
            args: vec![Expr::Literal(Value::BigInt(n))],
        }
    }

    /// LAG(expr) window function with default offset of 1.
    /// Returns the value of expr from the row that precedes the current row.
    pub fn lag(self) -> Self {
        Expr::Function {
            name: "LAG".to_string(),
            args: vec![self],
        }
    }

    /// LAG(expr, offset) window function.
    /// Returns the value of expr from the row at the given offset before current row.
    pub fn lag_offset(self, offset: i64) -> Self {
        Expr::Function {
            name: "LAG".to_string(),
            args: vec![self, Expr::Literal(Value::BigInt(offset))],
        }
    }

    /// LAG(expr, offset, default) window function.
    /// Returns the value of expr or default if the offset row doesn't exist.
    pub fn lag_with_default(self, offset: i64, default: impl Into<Expr>) -> Self {
        Expr::Function {
            name: "LAG".to_string(),
            args: vec![self, Expr::Literal(Value::BigInt(offset)), default.into()],
        }
    }

    /// LEAD(expr) window function with default offset of 1.
    /// Returns the value of expr from the row that follows the current row.
    pub fn lead(self) -> Self {
        Expr::Function {
            name: "LEAD".to_string(),
            args: vec![self],
        }
    }

    /// LEAD(expr, offset) window function.
    /// Returns the value of expr from the row at the given offset after current row.
    pub fn lead_offset(self, offset: i64) -> Self {
        Expr::Function {
            name: "LEAD".to_string(),
            args: vec![self, Expr::Literal(Value::BigInt(offset))],
        }
    }

    /// LEAD(expr, offset, default) window function.
    /// Returns the value of expr or default if the offset row doesn't exist.
    pub fn lead_with_default(self, offset: i64, default: impl Into<Expr>) -> Self {
        Expr::Function {
            name: "LEAD".to_string(),
            args: vec![self, Expr::Literal(Value::BigInt(offset)), default.into()],
        }
    }

    /// FIRST_VALUE(expr) window function.
    /// Returns the first value within the window frame.
    pub fn first_value(self) -> Self {
        Expr::Function {
            name: "FIRST_VALUE".to_string(),
            args: vec![self],
        }
    }

    /// LAST_VALUE(expr) window function.
    /// Returns the last value within the window frame.
    pub fn last_value(self) -> Self {
        Expr::Function {
            name: "LAST_VALUE".to_string(),
            args: vec![self],
        }
    }

    /// NTH_VALUE(expr, n) window function.
    /// Returns the nth value within the window frame.
    pub fn nth_value(self, n: i64) -> Self {
        Expr::Function {
            name: "NTH_VALUE".to_string(),
            args: vec![self, Expr::Literal(Value::BigInt(n))],
        }
    }

    // ==================== Window OVER Clause ====================

    /// Start building a window function with OVER clause.
    ///
    /// # Example
    /// ```ignore
    /// // ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC)
    /// Expr::row_number()
    ///     .over()
    ///     .partition_by(Expr::col("department"))
    ///     .order_by(Expr::col("salary").desc())
    ///     .build()
    ///
    /// // SUM(amount) OVER (PARTITION BY customer_id)
    /// Expr::col("amount").sum()
    ///     .over()
    ///     .partition_by(Expr::col("customer_id"))
    ///     .build()
    /// ```
    pub fn over(self) -> WindowBuilder {
        WindowBuilder {
            function: self,
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: None,
        }
    }

    // ==================== NULL Handling Functions ====================

    /// COALESCE function: returns the first non-NULL argument.
    ///
    /// # Example
    /// ```ignore
    /// Expr::coalesce(vec![Expr::col("nickname"), Expr::col("name"), Expr::lit("Anonymous")])
    /// // Generates: COALESCE("nickname", "name", 'Anonymous')
    /// ```
    pub fn coalesce(args: Vec<impl Into<Expr>>) -> Self {
        Expr::Function {
            name: "COALESCE".to_string(),
            args: args.into_iter().map(Into::into).collect(),
        }
    }

    /// NULLIF function: returns NULL if both arguments are equal, otherwise returns the first.
    ///
    /// # Example
    /// ```ignore
    /// Expr::nullif(Expr::col("value"), Expr::lit(0))
    /// // Generates: NULLIF("value", 0)
    /// ```
    pub fn nullif(expr1: impl Into<Expr>, expr2: impl Into<Expr>) -> Self {
        Expr::Function {
            name: "NULLIF".to_string(),
            args: vec![expr1.into(), expr2.into()],
        }
    }

    /// IFNULL/NVL function (dialect-specific): returns expr2 if expr1 is NULL.
    ///
    /// This generates IFNULL for SQLite/MySQL or COALESCE for PostgreSQL.
    pub fn ifnull(expr1: impl Into<Expr>, expr2: impl Into<Expr>) -> Self {
        // Use COALESCE as it's more portable
        Expr::Function {
            name: "COALESCE".to_string(),
            args: vec![expr1.into(), expr2.into()],
        }
    }

    // ==================== String Functions ====================

    /// UPPER function: converts string to uppercase.
    pub fn upper(self) -> Self {
        Expr::Function {
            name: "UPPER".to_string(),
            args: vec![self],
        }
    }

    /// LOWER function: converts string to lowercase.
    pub fn lower(self) -> Self {
        Expr::Function {
            name: "LOWER".to_string(),
            args: vec![self],
        }
    }

    /// LENGTH function: returns the length of a string.
    pub fn length(self) -> Self {
        Expr::Function {
            name: "LENGTH".to_string(),
            args: vec![self],
        }
    }

    /// TRIM function: removes leading and trailing whitespace.
    pub fn trim(self) -> Self {
        Expr::Function {
            name: "TRIM".to_string(),
            args: vec![self],
        }
    }

    /// LTRIM function: removes leading whitespace.
    pub fn ltrim(self) -> Self {
        Expr::Function {
            name: "LTRIM".to_string(),
            args: vec![self],
        }
    }

    /// RTRIM function: removes trailing whitespace.
    pub fn rtrim(self) -> Self {
        Expr::Function {
            name: "RTRIM".to_string(),
            args: vec![self],
        }
    }

    /// SUBSTR/SUBSTRING function: extracts a substring.
    ///
    /// # Arguments
    /// * `start` - 1-based start position
    /// * `length` - Optional length of substring
    pub fn substr(self, start: impl Into<Expr>, length: Option<impl Into<Expr>>) -> Self {
        let mut args = vec![self, start.into()];
        if let Some(len) = length {
            args.push(len.into());
        }
        Expr::Function {
            name: "SUBSTR".to_string(),
            args,
        }
    }

    /// REPLACE function: replaces occurrences of a substring.
    pub fn replace(self, from: impl Into<Expr>, to: impl Into<Expr>) -> Self {
        Expr::Function {
            name: "REPLACE".to_string(),
            args: vec![self, from.into(), to.into()],
        }
    }

    // ==================== Numeric Functions ====================

    /// ABS function: returns absolute value.
    pub fn abs(self) -> Self {
        Expr::Function {
            name: "ABS".to_string(),
            args: vec![self],
        }
    }

    /// ROUND function: rounds to specified decimal places.
    pub fn round(self, decimals: impl Into<Expr>) -> Self {
        Expr::Function {
            name: "ROUND".to_string(),
            args: vec![self, decimals.into()],
        }
    }

    /// FLOOR function: rounds down to nearest integer.
    pub fn floor(self) -> Self {
        Expr::Function {
            name: "FLOOR".to_string(),
            args: vec![self],
        }
    }

    /// CEIL/CEILING function: rounds up to nearest integer.
    pub fn ceil(self) -> Self {
        Expr::Function {
            name: "CEIL".to_string(),
            args: vec![self],
        }
    }

    // ==================== Ordering ====================

    /// Create an ascending ORDER BY expression.
    pub fn asc(self) -> OrderBy {
        OrderBy {
            expr: self,
            direction: OrderDirection::Asc,
            nulls: None,
        }
    }

    /// Create a descending ORDER BY expression.
    pub fn desc(self) -> OrderBy {
        OrderBy {
            expr: self,
            direction: OrderDirection::Desc,
            nulls: None,
        }
    }

    // ==================== Utility ====================

    /// Wrap expression in parentheses.
    pub fn paren(self) -> Self {
        Expr::Paren(Box::new(self))
    }

    /// Create a subquery expression.
    pub fn subquery(sql: impl Into<String>) -> Self {
        Expr::Subquery(sql.into())
    }

    // ==================== EXISTS Expressions ====================

    /// Create an EXISTS subquery expression.
    ///
    /// # Arguments
    /// * `subquery_sql` - The SELECT subquery SQL (without outer parentheses)
    /// * `params` - Parameters for the subquery
    ///
    /// # Example
    /// ```ignore
    /// // EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)
    /// Expr::exists(
    ///     "SELECT 1 FROM orders WHERE orders.customer_id = customers.id",
    ///     vec![]
    /// )
    /// ```
    pub fn exists(subquery_sql: impl Into<String>, params: Vec<Value>) -> Self {
        Expr::Exists {
            subquery: subquery_sql.into(),
            params,
            negated: false,
        }
    }

    /// Create a NOT EXISTS subquery expression.
    ///
    /// # Arguments
    /// * `subquery_sql` - The SELECT subquery SQL (without outer parentheses)
    /// * `params` - Parameters for the subquery
    ///
    /// # Example
    /// ```ignore
    /// // NOT EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)
    /// Expr::not_exists(
    ///     "SELECT 1 FROM orders WHERE orders.customer_id = customers.id",
    ///     vec![]
    /// )
    /// ```
    pub fn not_exists(subquery_sql: impl Into<String>, params: Vec<Value>) -> Self {
        Expr::Exists {
            subquery: subquery_sql.into(),
            params,
            negated: true,
        }
    }

    // ==================== JSON Functions ====================

    /// Extract a JSON value by key (returns JSON).
    ///
    /// Generates dialect-specific SQL:
    /// - PostgreSQL: `expr -> 'key'`
    /// - MySQL: `JSON_EXTRACT(expr, '$.key')`
    /// - SQLite: `json_extract(expr, '$.key')`
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("data").json_get("name")
    /// // PostgreSQL: "data" -> 'name'
    /// // MySQL: JSON_EXTRACT("data", '$.name')
    /// ```
    pub fn json_get(self, key: impl Into<String>) -> Self {
        Expr::JsonExtract {
            expr: Box::new(self),
            path: JsonPath::Key(key.into()),
        }
    }

    /// Extract a JSON value by array index (returns JSON).
    ///
    /// Generates dialect-specific SQL:
    /// - PostgreSQL: `expr -> 0`
    /// - MySQL: `JSON_EXTRACT(expr, '$[0]')`
    /// - SQLite: `json_extract(expr, '$[0]')`
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("items").json_get_index(0)
    /// // PostgreSQL: "items" -> 0
    /// ```
    pub fn json_get_index(self, index: i64) -> Self {
        Expr::JsonExtract {
            expr: Box::new(self),
            path: JsonPath::Index(index),
        }
    }

    /// Extract a JSON value as text by key.
    ///
    /// Generates dialect-specific SQL:
    /// - PostgreSQL: `expr ->> 'key'`
    /// - MySQL: `JSON_UNQUOTE(JSON_EXTRACT(expr, '$.key'))`
    /// - SQLite: `json_extract(expr, '$.key')` (returns text)
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("data").json_get_text("name")
    /// // PostgreSQL: "data" ->> 'name'
    /// ```
    pub fn json_get_text(self, key: impl Into<String>) -> Self {
        Expr::JsonExtractText {
            expr: Box::new(self),
            path: JsonPath::Key(key.into()),
        }
    }

    /// Extract a JSON value as text by array index.
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("items").json_get_text_index(0)
    /// // PostgreSQL: "items" ->> 0
    /// ```
    pub fn json_get_text_index(self, index: i64) -> Self {
        Expr::JsonExtractText {
            expr: Box::new(self),
            path: JsonPath::Index(index),
        }
    }

    /// Extract a nested JSON value by path (returns JSON).
    ///
    /// Generates dialect-specific SQL:
    /// - PostgreSQL: `expr #> '{path, to, value}'`
    /// - MySQL: `JSON_EXTRACT(expr, '$.path.to.value')`
    /// - SQLite: `json_extract(expr, '$.path.to.value')`
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("data").json_path(&["address", "city"])
    /// // PostgreSQL: "data" #> '{address, city}'
    /// ```
    pub fn json_path(self, path: &[&str]) -> Self {
        Expr::JsonExtractPath {
            expr: Box::new(self),
            path: path.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    /// Extract a nested JSON value by path as text.
    ///
    /// Generates dialect-specific SQL:
    /// - PostgreSQL: `expr #>> '{path, to, value}'`
    /// - MySQL: `JSON_UNQUOTE(JSON_EXTRACT(expr, '$.path.to.value'))`
    /// - SQLite: `json_extract(expr, '$.path.to.value')`
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("data").json_path_text(&["address", "city"])
    /// // PostgreSQL: "data" #>> '{address, city}'
    /// ```
    pub fn json_path_text(self, path: &[&str]) -> Self {
        Expr::JsonExtractPathText {
            expr: Box::new(self),
            path: path.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    /// Check if JSON contains another JSON value.
    ///
    /// Generates dialect-specific SQL:
    /// - PostgreSQL: `expr @> other` (JSONB only)
    /// - MySQL: `JSON_CONTAINS(expr, other)`
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("tags").json_contains(Expr::lit(r#"["rust"]"#))
    /// // PostgreSQL: "tags" @> '["rust"]'
    /// ```
    pub fn json_contains(self, other: impl Into<Expr>) -> Self {
        Expr::JsonContains {
            expr: Box::new(self),
            other: Box::new(other.into()),
        }
    }

    /// Check if JSON is contained by another JSON value.
    ///
    /// Generates dialect-specific SQL:
    /// - PostgreSQL: `expr <@ other` (JSONB only)
    /// - MySQL: `JSON_CONTAINS(other, expr)`
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("tags").json_contained_by(Expr::lit(r#"["rust", "python", "go"]"#))
    /// ```
    pub fn json_contained_by(self, other: impl Into<Expr>) -> Self {
        Expr::JsonContainedBy {
            expr: Box::new(self),
            other: Box::new(other.into()),
        }
    }

    /// Check if JSON object has a specific key.
    ///
    /// Generates dialect-specific SQL:
    /// - PostgreSQL: `expr ? 'key'` (JSONB only)
    /// - MySQL: `JSON_CONTAINS_PATH(expr, 'one', '$.key')`
    /// - SQLite: `json_type(expr, '$.key') IS NOT NULL`
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("data").json_has_key("email")
    /// // PostgreSQL: "data" ? 'email'
    /// ```
    pub fn json_has_key(self, key: impl Into<String>) -> Self {
        Expr::JsonHasKey {
            expr: Box::new(self),
            key: key.into(),
        }
    }

    /// Check if JSON object has any of the specified keys.
    ///
    /// Generates dialect-specific SQL:
    /// - PostgreSQL: `expr ?| array['key1', 'key2']` (JSONB only)
    /// - MySQL: `JSON_CONTAINS_PATH(expr, 'one', '$.key1', '$.key2')`
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("data").json_has_any_key(&["email", "phone"])
    /// // PostgreSQL: "data" ?| array['email', 'phone']
    /// ```
    pub fn json_has_any_key(self, keys: &[&str]) -> Self {
        Expr::JsonHasAnyKey {
            expr: Box::new(self),
            keys: keys.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    /// Check if JSON object has all of the specified keys.
    ///
    /// Generates dialect-specific SQL:
    /// - PostgreSQL: `expr ?& array['key1', 'key2']` (JSONB only)
    /// - MySQL: `JSON_CONTAINS_PATH(expr, 'all', '$.key1', '$.key2')`
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("data").json_has_all_keys(&["email", "phone"])
    /// // PostgreSQL: "data" ?& array['email', 'phone']
    /// ```
    pub fn json_has_all_keys(self, keys: &[&str]) -> Self {
        Expr::JsonHasAllKeys {
            expr: Box::new(self),
            keys: keys.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    /// Get the length of a JSON array.
    ///
    /// Generates dialect-specific SQL:
    /// - PostgreSQL: `jsonb_array_length(expr)`
    /// - MySQL: `JSON_LENGTH(expr)`
    /// - SQLite: `json_array_length(expr)`
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("items").json_array_length()
    /// ```
    pub fn json_array_length(self) -> Self {
        Expr::JsonArrayLength {
            expr: Box::new(self),
        }
    }

    /// Get the type of a JSON value.
    ///
    /// Generates dialect-specific SQL:
    /// - PostgreSQL: `jsonb_typeof(expr)`
    /// - MySQL: `JSON_TYPE(expr)`
    /// - SQLite: `json_type(expr)`
    ///
    /// # Example
    /// ```ignore
    /// Expr::col("data").json_typeof()
    /// ```
    pub fn json_typeof(self) -> Self {
        Expr::JsonTypeof {
            expr: Box::new(self),
        }
    }

    // ==================== SQL Generation ====================

    /// Build SQL string and collect parameters (default PostgreSQL dialect).
    pub fn build(&self, params: &mut Vec<Value>, offset: usize) -> String {
        self.build_with_dialect(Dialect::Postgres, params, offset)
    }

    /// Build SQL string with specific dialect.
    pub fn build_with_dialect(
        &self,
        dialect: Dialect,
        params: &mut Vec<Value>,
        offset: usize,
    ) -> String {
        match self {
            Expr::Column { table, name } => {
                if let Some(t) = table {
                    format!(
                        "{}.{}",
                        dialect.quote_identifier(t),
                        dialect.quote_identifier(name)
                    )
                } else {
                    dialect.quote_identifier(name)
                }
            }

            Expr::Literal(value) => {
                if matches!(value, Value::Default) {
                    "DEFAULT".to_string()
                } else {
                    params.push(value.clone());
                    dialect.placeholder(offset + params.len())
                }
            }

            Expr::Placeholder(idx) => dialect.placeholder(*idx),

            Expr::Binary { left, op, right } => {
                let left_sql = left.build_with_dialect(dialect, params, offset);
                let right_sql = right.build_with_dialect(dialect, params, offset);
                if *op == BinaryOp::Concat && dialect == Dialect::Mysql {
                    format!("CONCAT({left_sql}, {right_sql})")
                } else {
                    format!("{left_sql} {} {right_sql}", op.as_str())
                }
            }

            Expr::Unary { op, expr } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                match op {
                    UnaryOp::Not => format!("NOT {expr_sql}"),
                    UnaryOp::Neg => format!("-{expr_sql}"),
                    UnaryOp::BitwiseNot => format!("~{expr_sql}"),
                }
            }

            Expr::Function { name, args } => {
                let arg_sqls: Vec<_> = args
                    .iter()
                    .map(|a| a.build_with_dialect(dialect, params, offset))
                    .collect();
                format!("{name}({})", arg_sqls.join(", "))
            }

            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                let mut sql = String::from("CASE");
                for (condition, result) in when_clauses {
                    let cond_sql = condition.build_with_dialect(dialect, params, offset);
                    let result_sql = result.build_with_dialect(dialect, params, offset);
                    sql.push_str(&format!(" WHEN {cond_sql} THEN {result_sql}"));
                }
                if let Some(else_expr) = else_clause {
                    let else_sql = else_expr.build_with_dialect(dialect, params, offset);
                    sql.push_str(&format!(" ELSE {else_sql}"));
                }
                sql.push_str(" END");
                sql
            }

            Expr::In {
                expr,
                values,
                negated,
            } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                let value_sqls: Vec<_> = values
                    .iter()
                    .map(|v| v.build_with_dialect(dialect, params, offset))
                    .collect();
                let not_str = if *negated { "NOT " } else { "" };
                format!("{expr_sql} {not_str}IN ({})", value_sqls.join(", "))
            }

            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                let low_sql = low.build_with_dialect(dialect, params, offset);
                let high_sql = high.build_with_dialect(dialect, params, offset);
                let not_str = if *negated { "NOT " } else { "" };
                format!("{expr_sql} {not_str}BETWEEN {low_sql} AND {high_sql}")
            }

            Expr::IsNull { expr, negated } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                let not_str = if *negated { " NOT" } else { "" };
                format!("{expr_sql} IS{not_str} NULL")
            }

            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => {
                let left_sql = left.build_with_dialect(dialect, params, offset);
                let right_sql = right.build_with_dialect(dialect, params, offset);
                let not_str = if *negated { " NOT" } else { "" };
                // Standard SQL syntax supported by PostgreSQL, SQLite 3.39+, MySQL 8.0.16+
                format!("{left_sql} IS{not_str} DISTINCT FROM {right_sql}")
            }

            Expr::Cast { expr, type_name } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                format!("CAST({expr_sql} AS {type_name})")
            }

            Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                params.push(Value::Text(pattern.clone()));
                let param = dialect.placeholder(offset + params.len());
                let not_str = if *negated { "NOT " } else { "" };
                let op = if *case_insensitive && dialect.supports_ilike() {
                    "ILIKE"
                } else if *case_insensitive {
                    // Fallback for dialects without ILIKE
                    return format!("LOWER({expr_sql}) {not_str}LIKE LOWER({param})");
                } else {
                    "LIKE"
                };
                format!("{expr_sql} {not_str}{op} {param}")
            }

            Expr::Subquery(sql) => format!("({sql})"),

            Expr::Exists {
                subquery,
                params: subquery_params,
                negated,
            } => {
                // Add subquery parameters to the main params list
                // Note: The subquery SQL should use placeholders starting from 1,
                // and we'll adjust them based on the current offset
                let start_idx = offset + params.len();
                params.extend(subquery_params.iter().cloned());

                // Simple approach: if there are params, we need to adjust placeholder indices
                // in the subquery SQL. For now, we assume the subquery uses PostgreSQL-style $N
                // placeholders and adjust them.
                let adjusted_subquery = if subquery_params.is_empty() {
                    subquery.clone()
                } else {
                    // Rewrite $1, $2, etc. to $start_idx+1, $start_idx+2, etc.
                    adjust_placeholder_indices(subquery, start_idx, dialect)
                };

                let not_str = if *negated { "NOT " } else { "" };
                format!("{not_str}EXISTS ({adjusted_subquery})")
            }

            Expr::Raw(sql) => sql.clone(),

            Expr::Paren(expr) => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                format!("({expr_sql})")
            }

            Expr::CountStar => "COUNT(*)".to_string(),

            Expr::Window {
                function,
                partition_by,
                order_by,
                frame,
            } => {
                let func_sql = function.build_with_dialect(dialect, params, offset);
                let mut over_parts: Vec<String> = Vec::new();

                // PARTITION BY clause
                if !partition_by.is_empty() {
                    let partition_sqls: Vec<_> = partition_by
                        .iter()
                        .map(|e| e.build_with_dialect(dialect, params, offset))
                        .collect();
                    over_parts.push(format!("PARTITION BY {}", partition_sqls.join(", ")));
                }

                // ORDER BY clause
                if !order_by.is_empty() {
                    let order_sqls: Vec<_> = order_by
                        .iter()
                        .map(|o| {
                            let expr_sql = o.expr.build_with_dialect(dialect, params, offset);
                            let dir = match o.direction {
                                OrderDirection::Asc => "ASC",
                                OrderDirection::Desc => "DESC",
                            };
                            let nulls = match o.nulls {
                                Some(crate::clause::NullsOrder::First) => " NULLS FIRST",
                                Some(crate::clause::NullsOrder::Last) => " NULLS LAST",
                                None => "",
                            };
                            format!("{expr_sql} {dir}{nulls}")
                        })
                        .collect();
                    over_parts.push(format!("ORDER BY {}", order_sqls.join(", ")));
                }

                // Frame specification
                if let Some(f) = frame {
                    let frame_sql = if let Some(end) = &f.end {
                        format!(
                            "{} BETWEEN {} AND {}",
                            f.frame_type.as_str(),
                            f.start.to_sql(),
                            end.to_sql()
                        )
                    } else {
                        format!("{} {}", f.frame_type.as_str(), f.start.to_sql())
                    };
                    over_parts.push(frame_sql);
                }

                if over_parts.is_empty() {
                    format!("{func_sql} OVER ()")
                } else {
                    format!("{func_sql} OVER ({})", over_parts.join(" "))
                }
            }

            // ==================== JSON Expressions ====================
            Expr::JsonExtract { expr, path } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                match dialect {
                    Dialect::Postgres => match path {
                        JsonPath::Key(key) => format!("{expr_sql} -> '{key}'"),
                        JsonPath::Index(idx) => format!("{expr_sql} -> {idx}"),
                    },
                    Dialect::Mysql => {
                        let json_path = match path {
                            JsonPath::Key(key) => format!("$.{key}"),
                            JsonPath::Index(idx) => format!("$[{idx}]"),
                        };
                        format!("JSON_EXTRACT({expr_sql}, '{json_path}')")
                    }
                    Dialect::Sqlite => {
                        let json_path = match path {
                            JsonPath::Key(key) => format!("$.{key}"),
                            JsonPath::Index(idx) => format!("$[{idx}]"),
                        };
                        format!("json_extract({expr_sql}, '{json_path}')")
                    }
                }
            }

            Expr::JsonExtractText { expr, path } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                match dialect {
                    Dialect::Postgres => match path {
                        JsonPath::Key(key) => format!("{expr_sql} ->> '{key}'"),
                        JsonPath::Index(idx) => format!("{expr_sql} ->> {idx}"),
                    },
                    Dialect::Mysql => {
                        let json_path = match path {
                            JsonPath::Key(key) => format!("$.{key}"),
                            JsonPath::Index(idx) => format!("$[{idx}]"),
                        };
                        format!("JSON_UNQUOTE(JSON_EXTRACT({expr_sql}, '{json_path}'))")
                    }
                    Dialect::Sqlite => {
                        // SQLite's json_extract returns text for scalar values
                        let json_path = match path {
                            JsonPath::Key(key) => format!("$.{key}"),
                            JsonPath::Index(idx) => format!("$[{idx}]"),
                        };
                        format!("json_extract({expr_sql}, '{json_path}')")
                    }
                }
            }

            Expr::JsonExtractPath { expr, path } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                match dialect {
                    Dialect::Postgres => {
                        let path_array = path.join(", ");
                        format!("{expr_sql} #> '{{{path_array}}}'")
                    }
                    Dialect::Mysql | Dialect::Sqlite => {
                        let json_path = format!("$.{}", path.join("."));
                        let func = if dialect == Dialect::Mysql {
                            "JSON_EXTRACT"
                        } else {
                            "json_extract"
                        };
                        format!("{func}({expr_sql}, '{json_path}')")
                    }
                }
            }

            Expr::JsonExtractPathText { expr, path } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                match dialect {
                    Dialect::Postgres => {
                        let path_array = path.join(", ");
                        format!("{expr_sql} #>> '{{{path_array}}}'")
                    }
                    Dialect::Mysql => {
                        let json_path = format!("$.{}", path.join("."));
                        format!("JSON_UNQUOTE(JSON_EXTRACT({expr_sql}, '{json_path}'))")
                    }
                    Dialect::Sqlite => {
                        let json_path = format!("$.{}", path.join("."));
                        format!("json_extract({expr_sql}, '{json_path}')")
                    }
                }
            }

            Expr::JsonContains { expr, other } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                let other_sql = other.build_with_dialect(dialect, params, offset);
                match dialect {
                    Dialect::Postgres => format!("{expr_sql} @> {other_sql}"),
                    Dialect::Mysql => format!("JSON_CONTAINS({expr_sql}, {other_sql})"),
                    Dialect::Sqlite => {
                        // SQLite doesn't have direct containment, fallback to expression
                        format!(
                            "/* JSON containment not supported in SQLite */ ({expr_sql} = {other_sql})"
                        )
                    }
                }
            }

            Expr::JsonContainedBy { expr, other } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                let other_sql = other.build_with_dialect(dialect, params, offset);
                match dialect {
                    Dialect::Postgres => format!("{expr_sql} <@ {other_sql}"),
                    Dialect::Mysql => format!("JSON_CONTAINS({other_sql}, {expr_sql})"),
                    Dialect::Sqlite => {
                        format!(
                            "/* JSON contained-by not supported in SQLite */ ({expr_sql} = {other_sql})"
                        )
                    }
                }
            }

            Expr::JsonHasKey { expr, key } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                match dialect {
                    Dialect::Postgres => format!("{expr_sql} ? '{key}'"),
                    Dialect::Mysql => format!("JSON_CONTAINS_PATH({expr_sql}, 'one', '$.{key}')"),
                    Dialect::Sqlite => format!("json_type({expr_sql}, '$.{key}') IS NOT NULL"),
                }
            }

            Expr::JsonHasAnyKey { expr, keys } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                match dialect {
                    Dialect::Postgres => {
                        let keys_array = keys
                            .iter()
                            .map(|k| format!("'{k}'"))
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{expr_sql} ?| array[{keys_array}]")
                    }
                    Dialect::Mysql => {
                        let paths = keys
                            .iter()
                            .map(|k| format!("'$.{k}'"))
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("JSON_CONTAINS_PATH({expr_sql}, 'one', {paths})")
                    }
                    Dialect::Sqlite => {
                        let checks = keys
                            .iter()
                            .map(|k| format!("json_type({expr_sql}, '$.{k}') IS NOT NULL"))
                            .collect::<Vec<_>>()
                            .join(" OR ");
                        format!("({checks})")
                    }
                }
            }

            Expr::JsonHasAllKeys { expr, keys } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                match dialect {
                    Dialect::Postgres => {
                        let keys_array = keys
                            .iter()
                            .map(|k| format!("'{k}'"))
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{expr_sql} ?& array[{keys_array}]")
                    }
                    Dialect::Mysql => {
                        let paths = keys
                            .iter()
                            .map(|k| format!("'$.{k}'"))
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("JSON_CONTAINS_PATH({expr_sql}, 'all', {paths})")
                    }
                    Dialect::Sqlite => {
                        let checks = keys
                            .iter()
                            .map(|k| format!("json_type({expr_sql}, '$.{k}') IS NOT NULL"))
                            .collect::<Vec<_>>()
                            .join(" AND ");
                        format!("({checks})")
                    }
                }
            }

            Expr::JsonArrayLength { expr } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                match dialect {
                    Dialect::Postgres => format!("jsonb_array_length({expr_sql})"),
                    Dialect::Mysql => format!("JSON_LENGTH({expr_sql})"),
                    Dialect::Sqlite => format!("json_array_length({expr_sql})"),
                }
            }

            Expr::JsonTypeof { expr } => {
                let expr_sql = expr.build_with_dialect(dialect, params, offset);
                match dialect {
                    Dialect::Postgres => format!("jsonb_typeof({expr_sql})"),
                    Dialect::Mysql => format!("JSON_TYPE({expr_sql})"),
                    Dialect::Sqlite => format!("json_type({expr_sql})"),
                }
            }
        }
    }
}

// ==================== CASE Builder ====================

/// Builder for CASE WHEN expressions.
#[derive(Debug, Clone)]
pub struct CaseBuilder {
    when_clauses: Vec<(Expr, Expr)>,
}

impl CaseBuilder {
    /// Add a WHEN condition with its THEN result.
    pub fn when(mut self, condition: impl Into<Expr>, result: impl Into<Expr>) -> Self {
        self.when_clauses.push((condition.into(), result.into()));
        self
    }

    /// Finalize with an ELSE clause (optional).
    pub fn otherwise(self, else_result: impl Into<Expr>) -> Expr {
        Expr::Case {
            when_clauses: self.when_clauses,
            else_clause: Some(Box::new(else_result.into())),
        }
    }

    /// Finalize without an ELSE clause.
    pub fn end(self) -> Expr {
        Expr::Case {
            when_clauses: self.when_clauses,
            else_clause: None,
        }
    }
}

// ==================== Window Builder ====================

/// Builder for window functions with OVER clause.
#[derive(Debug, Clone)]
pub struct WindowBuilder {
    function: Expr,
    partition_by: Vec<Expr>,
    order_by: Vec<OrderBy>,
    frame: Option<WindowFrame>,
}

impl WindowBuilder {
    /// Add a PARTITION BY expression.
    ///
    /// Can be called multiple times to partition by multiple columns.
    pub fn partition_by(mut self, expr: impl Into<Expr>) -> Self {
        self.partition_by.push(expr.into());
        self
    }

    /// Add multiple PARTITION BY expressions at once.
    pub fn partition_by_many(mut self, exprs: Vec<impl Into<Expr>>) -> Self {
        self.partition_by.extend(exprs.into_iter().map(Into::into));
        self
    }

    /// Add an ORDER BY clause within the window.
    ///
    /// Can be called multiple times to order by multiple columns.
    pub fn order_by(mut self, order: OrderBy) -> Self {
        self.order_by.push(order);
        self
    }

    /// Add ORDER BY with ascending direction.
    pub fn order_by_asc(mut self, expr: impl Into<Expr>) -> Self {
        self.order_by.push(OrderBy {
            expr: expr.into(),
            direction: OrderDirection::Asc,
            nulls: None,
        });
        self
    }

    /// Add ORDER BY with descending direction.
    pub fn order_by_desc(mut self, expr: impl Into<Expr>) -> Self {
        self.order_by.push(OrderBy {
            expr: expr.into(),
            direction: OrderDirection::Desc,
            nulls: None,
        });
        self
    }

    /// Set frame specification: ROWS BETWEEN start AND end.
    ///
    /// # Example
    /// ```ignore
    /// // ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    /// .rows_between(WindowFrameBound::Preceding(2), WindowFrameBound::CurrentRow)
    /// ```
    pub fn rows_between(mut self, start: WindowFrameBound, end: WindowFrameBound) -> Self {
        self.frame = Some(WindowFrame {
            frame_type: WindowFrameType::Rows,
            start,
            end: Some(end),
        });
        self
    }

    /// Set frame specification: ROWS start (no end bound).
    ///
    /// # Example
    /// ```ignore
    /// // ROWS UNBOUNDED PRECEDING
    /// .rows(WindowFrameBound::UnboundedPreceding)
    /// ```
    pub fn rows(mut self, start: WindowFrameBound) -> Self {
        self.frame = Some(WindowFrame {
            frame_type: WindowFrameType::Rows,
            start,
            end: None,
        });
        self
    }

    /// Set frame specification: RANGE BETWEEN start AND end.
    ///
    /// # Example
    /// ```ignore
    /// // RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    /// .range_between(WindowFrameBound::UnboundedPreceding, WindowFrameBound::CurrentRow)
    /// ```
    pub fn range_between(mut self, start: WindowFrameBound, end: WindowFrameBound) -> Self {
        self.frame = Some(WindowFrame {
            frame_type: WindowFrameType::Range,
            start,
            end: Some(end),
        });
        self
    }

    /// Set frame specification: RANGE start (no end bound).
    pub fn range(mut self, start: WindowFrameBound) -> Self {
        self.frame = Some(WindowFrame {
            frame_type: WindowFrameType::Range,
            start,
            end: None,
        });
        self
    }

    /// Set frame specification: GROUPS BETWEEN start AND end (PostgreSQL 11+).
    pub fn groups_between(mut self, start: WindowFrameBound, end: WindowFrameBound) -> Self {
        self.frame = Some(WindowFrame {
            frame_type: WindowFrameType::Groups,
            start,
            end: Some(end),
        });
        self
    }

    /// Finalize and build the window expression.
    pub fn build(self) -> Expr {
        Expr::Window {
            function: Box::new(self.function),
            partition_by: self.partition_by,
            order_by: self.order_by,
            frame: self.frame,
        }
    }
}

// Conversion from Value to Expr
impl From<Value> for Expr {
    fn from(v: Value) -> Self {
        Expr::Literal(v)
    }
}

impl From<&str> for Expr {
    fn from(s: &str) -> Self {
        Expr::Literal(Value::Text(s.to_string()))
    }
}

impl From<String> for Expr {
    fn from(s: String) -> Self {
        Expr::Literal(Value::Text(s))
    }
}

impl From<i32> for Expr {
    fn from(n: i32) -> Self {
        Expr::Literal(Value::Int(n))
    }
}

impl From<i64> for Expr {
    fn from(n: i64) -> Self {
        Expr::Literal(Value::BigInt(n))
    }
}

impl From<bool> for Expr {
    fn from(b: bool) -> Self {
        Expr::Literal(Value::Bool(b))
    }
}

impl From<f64> for Expr {
    fn from(n: f64) -> Self {
        Expr::Literal(Value::Double(n))
    }
}

impl From<f32> for Expr {
    fn from(n: f32) -> Self {
        Expr::Literal(Value::Float(n))
    }
}

// ==================== Helper Functions ====================

/// Adjust placeholder indices in a SQL string.
///
/// Rewrites $1, $2, etc. to $offset+1, $offset+2, etc. for PostgreSQL,
/// or ?1, ?2, etc. for SQLite. MySQL always uses ? so no adjustment needed.
fn adjust_placeholder_indices(sql: &str, offset: usize, dialect: Dialect) -> String {
    if offset == 0 {
        return sql.to_string();
    }

    match dialect {
        Dialect::Postgres => {
            // Rewrite $N to $(N+offset)
            let mut result = String::with_capacity(sql.len() + 20);
            let mut chars = sql.chars().peekable();

            while let Some(c) = chars.next() {
                if c == '$' {
                    // Collect digits after $
                    let mut num_str = String::new();
                    while let Some(&d) = chars.peek() {
                        if d.is_ascii_digit() {
                            num_str.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                    if let Ok(n) = num_str.parse::<usize>() {
                        result.push_str(&format!("${}", n + offset));
                    } else {
                        result.push('$');
                        result.push_str(&num_str);
                    }
                } else {
                    result.push(c);
                }
            }
            result
        }
        Dialect::Sqlite => {
            // Rewrite ?N to ?(N+offset)
            let mut result = String::with_capacity(sql.len() + 20);
            let mut chars = sql.chars().peekable();

            while let Some(c) = chars.next() {
                if c == '?' {
                    // Collect digits after ?
                    let mut num_str = String::new();
                    while let Some(&d) = chars.peek() {
                        if d.is_ascii_digit() {
                            num_str.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                    if let Ok(n) = num_str.parse::<usize>() {
                        result.push_str(&format!("?{}", n + offset));
                    } else {
                        result.push('?');
                        result.push_str(&num_str);
                    }
                } else {
                    result.push(c);
                }
            }
            result
        }
        Dialect::Mysql => {
            // MySQL uses ? without indices, so no adjustment needed
            sql.to_string()
        }
    }
}

// ==================== Tests ====================

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== Column Tests ====================

    #[test]
    fn test_column_simple() {
        let expr = Expr::col("name");
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"name\"");
        assert!(params.is_empty());
    }

    #[test]
    fn test_column_qualified() {
        let expr = Expr::qualified("users", "name");
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"users\".\"name\"");
        assert!(params.is_empty());
    }

    // ==================== Literal Tests ====================

    #[test]
    fn test_literal_int() {
        let expr = Expr::lit(42);
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "$1");
        assert_eq!(params.len(), 1);
        assert_eq!(params[0], Value::Int(42));
    }

    #[test]
    fn test_literal_string() {
        let expr = Expr::lit("hello");
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "$1");
        assert_eq!(params[0], Value::Text("hello".to_string()));
    }

    #[test]
    fn test_literal_null() {
        let expr = Expr::null();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "$1");
        assert_eq!(params[0], Value::Null);
    }

    // ==================== Comparison Tests ====================

    #[test]
    fn test_eq() {
        let expr = Expr::col("age").eq(18);
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"age\" = $1");
        assert_eq!(params[0], Value::Int(18));
    }

    #[test]
    fn test_ne() {
        let expr = Expr::col("status").ne("deleted");
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"status\" <> $1");
    }

    #[test]
    fn test_lt_le_gt_ge() {
        let mut params = Vec::new();

        let lt = Expr::col("age").lt(18).build(&mut params, 0);
        assert_eq!(lt, "\"age\" < $1");

        params.clear();
        let le = Expr::col("age").le(18).build(&mut params, 0);
        assert_eq!(le, "\"age\" <= $1");

        params.clear();
        let gt = Expr::col("age").gt(18).build(&mut params, 0);
        assert_eq!(gt, "\"age\" > $1");

        params.clear();
        let ge = Expr::col("age").ge(18).build(&mut params, 0);
        assert_eq!(ge, "\"age\" >= $1");
    }

    // ==================== Logical Tests ====================

    #[test]
    fn test_and() {
        let expr = Expr::col("a").eq(1).and(Expr::col("b").eq(2));
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"a\" = $1 AND \"b\" = $2");
    }

    #[test]
    fn test_or() {
        let expr = Expr::col("a").eq(1).or(Expr::col("b").eq(2));
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"a\" = $1 OR \"b\" = $2");
    }

    #[test]
    fn test_not() {
        let expr = Expr::col("active").not();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "NOT \"active\"");
    }

    // ==================== Null Tests ====================

    #[test]
    fn test_is_null() {
        let expr = Expr::col("deleted_at").is_null();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"deleted_at\" IS NULL");
    }

    #[test]
    fn test_is_not_null() {
        let expr = Expr::col("name").is_not_null();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"name\" IS NOT NULL");
    }

    // ==================== Pattern Matching Tests ====================

    #[test]
    fn test_like() {
        let expr = Expr::col("name").like("%john%");
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"name\" LIKE $1");
        assert_eq!(params[0], Value::Text("%john%".to_string()));
    }

    #[test]
    fn test_not_like() {
        let expr = Expr::col("name").not_like("%test%");
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"name\" NOT LIKE $1");
    }

    #[test]
    fn test_ilike_postgres() {
        let expr = Expr::col("name").ilike("%JOHN%");
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"name\" ILIKE $1");
    }

    #[test]
    fn test_ilike_fallback_sqlite() {
        let expr = Expr::col("name").ilike("%JOHN%");
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Sqlite, &mut params, 0);
        assert_eq!(sql, "LOWER(\"name\") LIKE LOWER(?1)");
    }

    // ==================== IN Tests ====================

    #[test]
    fn test_in_list() {
        let expr = Expr::col("status").in_list(vec![1, 2, 3]);
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"status\" IN ($1, $2, $3)");
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_not_in_list() {
        let expr = Expr::col("status").not_in_list(vec![4, 5]);
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"status\" NOT IN ($1, $2)");
    }

    // ==================== BETWEEN Tests ====================

    #[test]
    fn test_between() {
        let expr = Expr::col("age").between(18, 65);
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"age\" BETWEEN $1 AND $2");
        assert_eq!(params[0], Value::Int(18));
        assert_eq!(params[1], Value::Int(65));
    }

    #[test]
    fn test_not_between() {
        let expr = Expr::col("age").not_between(0, 17);
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"age\" NOT BETWEEN $1 AND $2");
    }

    // ==================== Arithmetic Tests ====================

    #[test]
    fn test_arithmetic() {
        let mut params = Vec::new();

        let add = Expr::col("a").add(Expr::col("b")).build(&mut params, 0);
        assert_eq!(add, "\"a\" + \"b\"");

        let sub = Expr::col("a").sub(Expr::col("b")).build(&mut params, 0);
        assert_eq!(sub, "\"a\" - \"b\"");

        let mul = Expr::col("a").mul(Expr::col("b")).build(&mut params, 0);
        assert_eq!(mul, "\"a\" * \"b\"");

        let div = Expr::col("a").div(Expr::col("b")).build(&mut params, 0);
        assert_eq!(div, "\"a\" / \"b\"");

        let modulo = Expr::col("a").modulo(Expr::col("b")).build(&mut params, 0);
        assert_eq!(modulo, "\"a\" % \"b\"");
    }

    #[test]
    fn test_neg() {
        let expr = Expr::col("balance").neg();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "-\"balance\"");
    }

    // ==================== Bitwise Tests ====================

    #[test]
    fn test_bitwise() {
        let mut params = Vec::new();

        let bit_and = Expr::col("flags")
            .bit_and(Expr::lit(0xFF))
            .build(&mut params, 0);
        assert_eq!(bit_and, "\"flags\" & $1");

        params.clear();
        let or_sql = Expr::col("flags")
            .bit_or(Expr::lit(0x01))
            .build(&mut params, 0);
        assert_eq!(or_sql, "\"flags\" | $1");

        params.clear();
        let xor_sql = Expr::col("flags")
            .bit_xor(Expr::lit(0x0F))
            .build(&mut params, 0);
        assert_eq!(xor_sql, "\"flags\" ^ $1");

        let bit_not = Expr::col("flags").bit_not().build(&mut params, 0);
        assert_eq!(bit_not, "~\"flags\"");
    }

    // ==================== CASE Tests ====================

    #[test]
    fn test_case_simple() {
        let expr = Expr::case()
            .when(Expr::col("status").eq("active"), "Yes")
            .when(Expr::col("status").eq("pending"), "Maybe")
            .otherwise("No");

        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "CASE WHEN \"status\" = $1 THEN $2 WHEN \"status\" = $3 THEN $4 ELSE $5 END"
        );
        assert_eq!(params.len(), 5);
    }

    #[test]
    fn test_case_without_else() {
        let expr = Expr::case().when(Expr::col("age").gt(18), "adult").end();

        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "CASE WHEN \"age\" > $1 THEN $2 END");
    }

    // ==================== Aggregate Tests ====================

    #[test]
    fn test_count_star() {
        let expr = Expr::count_star();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "COUNT(*)");
    }

    #[test]
    fn test_count() {
        let expr = Expr::col("id").count();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "COUNT(\"id\")");
    }

    #[test]
    fn test_aggregates() {
        let mut params = Vec::new();

        let sum = Expr::col("amount").sum().build(&mut params, 0);
        assert_eq!(sum, "SUM(\"amount\")");

        let avg = Expr::col("price").avg().build(&mut params, 0);
        assert_eq!(avg, "AVG(\"price\")");

        let min = Expr::col("age").min().build(&mut params, 0);
        assert_eq!(min, "MIN(\"age\")");

        let max = Expr::col("score").max().build(&mut params, 0);
        assert_eq!(max, "MAX(\"score\")");
    }

    // ==================== Function Tests ====================

    #[test]
    fn test_function() {
        let expr = Expr::function("UPPER", vec![Expr::col("name")]);
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "UPPER(\"name\")");
    }

    #[test]
    fn test_function_multiple_args() {
        let expr = Expr::function("COALESCE", vec![Expr::col("name"), Expr::lit("Unknown")]);
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "COALESCE(\"name\", $1)");
    }

    // ==================== Order Expression Tests ====================

    #[test]
    fn test_order_asc() {
        let order = Expr::col("name").asc();
        let mut params = Vec::new();
        let sql = order.build(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"name\" ASC");
    }

    #[test]
    fn test_order_desc() {
        let order = Expr::col("created_at").desc();
        let mut params = Vec::new();
        let sql = order.build(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"created_at\" DESC");
    }

    #[test]
    fn test_order_nulls() {
        let order_first = Expr::col("name").asc().nulls_first();
        let mut params = Vec::new();
        let sql = order_first.build(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"name\" ASC NULLS FIRST");

        let order_last = Expr::col("name").desc().nulls_last();
        let sql = order_last.build(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"name\" DESC NULLS LAST");
    }

    // ==================== Dialect Tests ====================

    #[test]
    fn test_dialect_postgres() {
        let expr = Expr::col("id").eq(1);
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"id\" = $1");
    }

    #[test]
    fn test_dialect_sqlite() {
        let expr = Expr::col("id").eq(1);
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Sqlite, &mut params, 0);
        assert_eq!(sql, "\"id\" = ?1");
    }

    #[test]
    fn test_dialect_mysql() {
        let expr = Expr::col("id").eq(1);
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Mysql, &mut params, 0);
        assert_eq!(sql, "`id` = ?");
    }

    // ==================== Complex Expression Tests ====================

    #[test]
    fn test_complex_nested() {
        // (age > 18 AND status = 'active') OR is_admin = true
        let expr = Expr::col("age")
            .gt(18)
            .and(Expr::col("status").eq("active"))
            .paren()
            .or(Expr::col("is_admin").eq(true));

        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "(\"age\" > $1 AND \"status\" = $2) OR \"is_admin\" = $3"
        );
    }

    #[test]
    fn test_parameter_offset() {
        let expr = Expr::col("name").eq("test");
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 5);
        assert_eq!(sql, "\"name\" = $6");
    }

    // ==================== String Concat Tests ====================

    #[test]
    fn test_concat() {
        let expr = Expr::col("first_name")
            .concat(" ")
            .concat(Expr::col("last_name"));
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"first_name\" || $1 || \"last_name\"");
    }

    // ==================== Placeholder Tests ====================

    #[test]
    fn test_placeholder() {
        let expr = Expr::col("id").eq(Expr::placeholder(1));
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"id\" = $1");
        assert!(params.is_empty()); // Placeholder doesn't add to params
    }

    // ==================== Subquery Tests ====================

    #[test]
    fn test_subquery() {
        let expr = Expr::col("dept_id").in_list(vec![Expr::subquery(
            "SELECT id FROM departments WHERE active = true",
        )]);
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "\"dept_id\" IN ((SELECT id FROM departments WHERE active = true))"
        );
    }

    // ==================== Raw SQL Tests ====================

    #[test]
    fn test_raw() {
        let expr = Expr::raw("NOW()");
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "NOW()");
    }

    // ==================== Precedence Tests ====================

    #[test]
    fn test_precedence() {
        assert!(BinaryOp::Mul.precedence() > BinaryOp::Add.precedence());
        assert!(BinaryOp::And.precedence() > BinaryOp::Or.precedence());
        assert!(BinaryOp::Eq.precedence() > BinaryOp::And.precedence());
    }

    // ==================== Quote Escaping Tests ====================

    #[test]
    fn test_quote_identifier_escapes_postgres() {
        // Postgres/SQLite: double-quotes must be escaped by doubling
        assert_eq!(Dialect::Postgres.quote_identifier("simple"), "\"simple\"");
        assert_eq!(
            Dialect::Postgres.quote_identifier("with\"quote"),
            "\"with\"\"quote\""
        );
        assert_eq!(
            Dialect::Postgres.quote_identifier("multi\"\"quotes"),
            "\"multi\"\"\"\"quotes\""
        );
    }

    #[test]
    fn test_quote_identifier_escapes_sqlite() {
        // SQLite also uses double-quotes
        assert_eq!(Dialect::Sqlite.quote_identifier("simple"), "\"simple\"");
        assert_eq!(
            Dialect::Sqlite.quote_identifier("with\"quote"),
            "\"with\"\"quote\""
        );
    }

    #[test]
    fn test_quote_identifier_escapes_mysql() {
        // MySQL: backticks must be escaped by doubling
        assert_eq!(Dialect::Mysql.quote_identifier("simple"), "`simple`");
        assert_eq!(
            Dialect::Mysql.quote_identifier("with`backtick"),
            "`with``backtick`"
        );
        assert_eq!(
            Dialect::Mysql.quote_identifier("multi``ticks"),
            "`multi````ticks`"
        );
    }

    // ==================== Window Function Tests ====================

    #[test]
    fn test_window_row_number_empty_over() {
        let expr = Expr::row_number().over().build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "ROW_NUMBER() OVER ()");
    }

    #[test]
    fn test_window_row_number_order_by() {
        let expr = Expr::row_number()
            .over()
            .order_by_desc(Expr::col("created_at"))
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "ROW_NUMBER() OVER (ORDER BY \"created_at\" DESC)");
    }

    #[test]
    fn test_window_partition_by() {
        let expr = Expr::row_number()
            .over()
            .partition_by(Expr::col("department"))
            .order_by_asc(Expr::col("hire_date"))
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "ROW_NUMBER() OVER (PARTITION BY \"department\" ORDER BY \"hire_date\" ASC)"
        );
    }

    #[test]
    fn test_window_multiple_partition_by() {
        let expr = Expr::rank()
            .over()
            .partition_by(Expr::col("region"))
            .partition_by(Expr::col("product"))
            .order_by_desc(Expr::col("sales"))
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "RANK() OVER (PARTITION BY \"region\", \"product\" ORDER BY \"sales\" DESC)"
        );
    }

    #[test]
    fn test_window_dense_rank() {
        let expr = Expr::dense_rank()
            .over()
            .order_by_asc(Expr::col("score"))
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "DENSE_RANK() OVER (ORDER BY \"score\" ASC)");
    }

    #[test]
    fn test_window_ntile() {
        let expr = Expr::ntile(4)
            .over()
            .order_by_asc(Expr::col("salary"))
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "NTILE($1) OVER (ORDER BY \"salary\" ASC)");
        assert_eq!(params[0], Value::BigInt(4));
    }

    #[test]
    fn test_window_lag() {
        let expr = Expr::col("price")
            .lag()
            .over()
            .order_by_asc(Expr::col("date"))
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "LAG(\"price\") OVER (ORDER BY \"date\" ASC)");
    }

    #[test]
    fn test_window_lag_with_offset() {
        let expr = Expr::col("price")
            .lag_offset(3)
            .over()
            .order_by_asc(Expr::col("date"))
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "LAG(\"price\", $1) OVER (ORDER BY \"date\" ASC)");
        assert_eq!(params[0], Value::BigInt(3));
    }

    #[test]
    fn test_window_lead_with_default() {
        let expr = Expr::col("price")
            .lead_with_default(1, 0)
            .over()
            .order_by_asc(Expr::col("date"))
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "LEAD(\"price\", $1, $2) OVER (ORDER BY \"date\" ASC)");
        assert_eq!(params[0], Value::BigInt(1));
        assert_eq!(params[1], Value::Int(0));
    }

    #[test]
    fn test_window_first_value() {
        let expr = Expr::col("salary")
            .first_value()
            .over()
            .partition_by(Expr::col("department"))
            .order_by_desc(Expr::col("salary"))
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "FIRST_VALUE(\"salary\") OVER (PARTITION BY \"department\" ORDER BY \"salary\" DESC)"
        );
    }

    #[test]
    fn test_window_last_value() {
        let expr = Expr::col("amount")
            .last_value()
            .over()
            .order_by_asc(Expr::col("created_at"))
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "LAST_VALUE(\"amount\") OVER (ORDER BY \"created_at\" ASC)"
        );
    }

    #[test]
    fn test_window_nth_value() {
        let expr = Expr::col("score")
            .nth_value(3)
            .over()
            .order_by_desc(Expr::col("score"))
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "NTH_VALUE(\"score\", $1) OVER (ORDER BY \"score\" DESC)"
        );
        assert_eq!(params[0], Value::BigInt(3));
    }

    #[test]
    fn test_window_aggregate_sum() {
        let expr = Expr::col("amount")
            .sum()
            .over()
            .partition_by(Expr::col("customer_id"))
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "SUM(\"amount\") OVER (PARTITION BY \"customer_id\")");
    }

    #[test]
    fn test_window_aggregate_avg_running() {
        let expr = Expr::col("price")
            .avg()
            .over()
            .order_by_asc(Expr::col("date"))
            .rows_between(
                WindowFrameBound::UnboundedPreceding,
                WindowFrameBound::CurrentRow,
            )
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "AVG(\"price\") OVER (ORDER BY \"date\" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        );
    }

    #[test]
    fn test_window_frame_rows_preceding() {
        let expr = Expr::col("value")
            .sum()
            .over()
            .order_by_asc(Expr::col("idx"))
            .rows_between(WindowFrameBound::Preceding(2), WindowFrameBound::CurrentRow)
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "SUM(\"value\") OVER (ORDER BY \"idx\" ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"
        );
    }

    #[test]
    fn test_window_frame_rows_following() {
        let expr = Expr::col("value")
            .avg()
            .over()
            .order_by_asc(Expr::col("idx"))
            .rows_between(WindowFrameBound::CurrentRow, WindowFrameBound::Following(3))
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "AVG(\"value\") OVER (ORDER BY \"idx\" ASC ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING)"
        );
    }

    #[test]
    fn test_window_frame_range_unbounded() {
        let expr = Expr::col("total")
            .sum()
            .over()
            .partition_by(Expr::col("category"))
            .order_by_asc(Expr::col("date"))
            .range_between(
                WindowFrameBound::UnboundedPreceding,
                WindowFrameBound::UnboundedFollowing,
            )
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "SUM(\"total\") OVER (PARTITION BY \"category\" ORDER BY \"date\" ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
        );
    }

    #[test]
    fn test_window_frame_rows_single_bound() {
        let expr = Expr::col("value")
            .sum()
            .over()
            .order_by_asc(Expr::col("idx"))
            .rows(WindowFrameBound::UnboundedPreceding)
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "SUM(\"value\") OVER (ORDER BY \"idx\" ASC ROWS UNBOUNDED PRECEDING)"
        );
    }

    #[test]
    fn test_window_percent_rank() {
        let expr = Expr::percent_rank()
            .over()
            .order_by_asc(Expr::col("score"))
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "PERCENT_RANK() OVER (ORDER BY \"score\" ASC)");
    }

    #[test]
    fn test_window_cume_dist() {
        let expr = Expr::cume_dist()
            .over()
            .partition_by(Expr::col("group_id"))
            .order_by_asc(Expr::col("value"))
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "CUME_DIST() OVER (PARTITION BY \"group_id\" ORDER BY \"value\" ASC)"
        );
    }

    #[test]
    fn test_window_frame_groups() {
        let expr = Expr::col("amount")
            .sum()
            .over()
            .order_by_asc(Expr::col("group_rank"))
            .groups_between(
                WindowFrameBound::Preceding(1),
                WindowFrameBound::Following(1),
            )
            .build();
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "SUM(\"amount\") OVER (ORDER BY \"group_rank\" ASC GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
        );
    }

    // ==================== EXISTS Tests ====================

    #[test]
    fn test_exists_basic() {
        // EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)
        let expr = Expr::exists(
            "SELECT 1 FROM orders WHERE orders.customer_id = customers.id",
            vec![],
        );
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_not_exists() {
        // NOT EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)
        let expr = Expr::not_exists(
            "SELECT 1 FROM orders WHERE orders.customer_id = customers.id",
            vec![],
        );
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "NOT EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_exists_with_params() {
        // EXISTS (SELECT 1 FROM orders WHERE status = $1)
        let expr = Expr::exists(
            "SELECT 1 FROM orders WHERE status = $1",
            vec![Value::Text("active".to_string())],
        );
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "EXISTS (SELECT 1 FROM orders WHERE status = $1)");
        assert_eq!(params.len(), 1);
        assert_eq!(params[0], Value::Text("active".to_string()));
    }

    #[test]
    fn test_exists_with_params_offset() {
        // When there's an offset, placeholder indices should be adjusted
        let expr = Expr::exists(
            "SELECT 1 FROM orders WHERE status = $1 AND type = $2",
            vec![
                Value::Text("active".to_string()),
                Value::Text("online".to_string()),
            ],
        );
        let mut params = Vec::new();
        // Simulate offset of 3 (meaning params $1-$3 are already used by outer query)
        let sql = expr.build(&mut params, 3);
        assert_eq!(
            sql,
            "EXISTS (SELECT 1 FROM orders WHERE status = $4 AND type = $5)"
        );
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_exists_in_where_clause() {
        // Combining EXISTS with AND
        let exists_expr = Expr::exists("SELECT 1 FROM orders o WHERE o.customer_id = c.id", vec![]);
        let expr = Expr::col("active").eq(true).and(exists_expr);
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(
            sql,
            "\"active\" = $1 AND EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)"
        );
        assert_eq!(params[0], Value::Bool(true));
    }

    #[test]
    fn test_exists_sqlite_dialect() {
        let expr = Expr::exists(
            "SELECT 1 FROM orders WHERE status = ?1",
            vec![Value::Text("active".to_string())],
        );
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Sqlite, &mut params, 0);
        assert_eq!(sql, "EXISTS (SELECT 1 FROM orders WHERE status = ?1)");
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_exists_sqlite_with_offset() {
        let expr = Expr::exists(
            "SELECT 1 FROM orders WHERE status = ?1",
            vec![Value::Text("active".to_string())],
        );
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Sqlite, &mut params, 2);
        assert_eq!(sql, "EXISTS (SELECT 1 FROM orders WHERE status = ?3)");
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_exists_mysql_dialect() {
        // MySQL uses positional ? without numbers, so offset doesn't change the SQL
        let expr = Expr::exists(
            "SELECT 1 FROM orders WHERE status = ?",
            vec![Value::Text("active".to_string())],
        );
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Mysql, &mut params, 0);
        assert_eq!(sql, "EXISTS (SELECT 1 FROM orders WHERE status = ?)");
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_not_exists_with_offset() {
        let expr = Expr::not_exists(
            "SELECT 1 FROM orders WHERE status = $1",
            vec![Value::Text("pending".to_string())],
        );
        let mut params = Vec::new();
        let sql = expr.build(&mut params, 5);
        assert_eq!(sql, "NOT EXISTS (SELECT 1 FROM orders WHERE status = $6)");
        assert_eq!(params.len(), 1);
    }

    // ==================== Placeholder Adjustment Tests ====================

    #[test]
    fn test_adjust_placeholder_indices_postgres() {
        let sql = "SELECT * FROM t WHERE a = $1 AND b = $2";
        let adjusted = super::adjust_placeholder_indices(sql, 3, Dialect::Postgres);
        assert_eq!(adjusted, "SELECT * FROM t WHERE a = $4 AND b = $5");
    }

    #[test]
    fn test_adjust_placeholder_indices_sqlite() {
        let sql = "SELECT * FROM t WHERE a = ?1 AND b = ?2";
        let adjusted = super::adjust_placeholder_indices(sql, 3, Dialect::Sqlite);
        assert_eq!(adjusted, "SELECT * FROM t WHERE a = ?4 AND b = ?5");
    }

    #[test]
    fn test_adjust_placeholder_indices_zero_offset() {
        let sql = "SELECT * FROM t WHERE a = $1";
        let adjusted = super::adjust_placeholder_indices(sql, 0, Dialect::Postgres);
        assert_eq!(adjusted, sql);
    }

    #[test]
    fn test_adjust_placeholder_indices_mysql() {
        // MySQL uses ? without indices, so no adjustment
        let sql = "SELECT * FROM t WHERE a = ? AND b = ?";
        let adjusted = super::adjust_placeholder_indices(sql, 3, Dialect::Mysql);
        assert_eq!(adjusted, sql);
    }

    // ==================== JSON Expression Tests ====================

    #[test]
    fn test_json_get_key_postgres() {
        let expr = Expr::col("data").json_get("name");
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"data\" -> 'name'");
        assert!(params.is_empty());
    }

    #[test]
    fn test_json_get_key_mysql() {
        let expr = Expr::col("data").json_get("name");
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Mysql, &mut params, 0);
        assert_eq!(sql, "JSON_EXTRACT(`data`, '$.name')");
        assert!(params.is_empty());
    }

    #[test]
    fn test_json_get_key_sqlite() {
        let expr = Expr::col("data").json_get("name");
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Sqlite, &mut params, 0);
        assert_eq!(sql, "json_extract(\"data\", '$.name')");
        assert!(params.is_empty());
    }

    #[test]
    fn test_json_get_index_postgres() {
        let expr = Expr::col("items").json_get_index(0);
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"items\" -> 0");
    }

    #[test]
    fn test_json_get_index_mysql() {
        let expr = Expr::col("items").json_get_index(0);
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Mysql, &mut params, 0);
        assert_eq!(sql, "JSON_EXTRACT(`items`, '$[0]')");
    }

    #[test]
    fn test_json_get_text_postgres() {
        let expr = Expr::col("data").json_get_text("name");
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"data\" ->> 'name'");
    }

    #[test]
    fn test_json_get_text_mysql() {
        let expr = Expr::col("data").json_get_text("name");
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Mysql, &mut params, 0);
        assert_eq!(sql, "JSON_UNQUOTE(JSON_EXTRACT(`data`, '$.name'))");
    }

    #[test]
    fn test_json_path_postgres() {
        let expr = Expr::col("data").json_path(&["address", "city"]);
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"data\" #> '{address, city}'");
    }

    #[test]
    fn test_json_path_mysql() {
        let expr = Expr::col("data").json_path(&["address", "city"]);
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Mysql, &mut params, 0);
        assert_eq!(sql, "JSON_EXTRACT(`data`, '$.address.city')");
    }

    #[test]
    fn test_json_path_text_postgres() {
        let expr = Expr::col("data").json_path_text(&["address", "city"]);
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"data\" #>> '{address, city}'");
    }

    #[test]
    fn test_json_contains_postgres() {
        let expr = Expr::col("tags").json_contains(Expr::raw("'[\"rust\"]'"));
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"tags\" @> '[\"rust\"]'");
    }

    #[test]
    fn test_json_contains_mysql() {
        let expr = Expr::col("tags").json_contains(Expr::raw("'[\"rust\"]'"));
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Mysql, &mut params, 0);
        assert_eq!(sql, "JSON_CONTAINS(`tags`, '[\"rust\"]')");
    }

    #[test]
    fn test_json_has_key_postgres() {
        let expr = Expr::col("data").json_has_key("email");
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"data\" ? 'email'");
    }

    #[test]
    fn test_json_has_key_mysql() {
        let expr = Expr::col("data").json_has_key("email");
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Mysql, &mut params, 0);
        assert_eq!(sql, "JSON_CONTAINS_PATH(`data`, 'one', '$.email')");
    }

    #[test]
    fn test_json_has_key_sqlite() {
        let expr = Expr::col("data").json_has_key("email");
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Sqlite, &mut params, 0);
        assert_eq!(sql, "json_type(\"data\", '$.email') IS NOT NULL");
    }

    #[test]
    fn test_json_has_any_key_postgres() {
        let expr = Expr::col("data").json_has_any_key(&["email", "phone"]);
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"data\" ?| array['email', 'phone']");
    }

    #[test]
    fn test_json_has_all_keys_postgres() {
        let expr = Expr::col("data").json_has_all_keys(&["email", "phone"]);
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"data\" ?& array['email', 'phone']");
    }

    #[test]
    fn test_json_array_length_postgres() {
        let expr = Expr::col("items").json_array_length();
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "jsonb_array_length(\"items\")");
    }

    #[test]
    fn test_json_array_length_mysql() {
        let expr = Expr::col("items").json_array_length();
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Mysql, &mut params, 0);
        assert_eq!(sql, "JSON_LENGTH(`items`)");
    }

    #[test]
    fn test_json_array_length_sqlite() {
        let expr = Expr::col("items").json_array_length();
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Sqlite, &mut params, 0);
        assert_eq!(sql, "json_array_length(\"items\")");
    }

    #[test]
    fn test_json_typeof_postgres() {
        let expr = Expr::col("data").json_typeof();
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "jsonb_typeof(\"data\")");
    }

    #[test]
    fn test_json_typeof_mysql() {
        let expr = Expr::col("data").json_typeof();
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Mysql, &mut params, 0);
        assert_eq!(sql, "JSON_TYPE(`data`)");
    }

    #[test]
    fn test_json_chained_extraction() {
        // Test chaining: data -> 'user' ->> 'name'
        let expr = Expr::col("data").json_get("user").json_get_text("name");
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        // The inner expression is first evaluated, then text extraction
        // This tests nested JSON expressions
        assert_eq!(sql, "\"data\" -> 'user' ->> 'name'");
    }

    #[test]
    fn test_json_in_where_clause() {
        // Test JSON expression in a comparison
        let expr = Expr::col("data").json_get_text("status").eq("active");
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"data\" ->> 'status' = $1");
        assert_eq!(params.len(), 1);
    }

    // ==================== Array Operations ====================

    #[test]
    fn test_array_contains() {
        let expr = Expr::col("tags").array_contains(Expr::col("other_tags"));
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"tags\" @> \"other_tags\"");
    }

    #[test]
    fn test_array_contained_by() {
        let expr = Expr::col("tags").array_contained_by(Expr::col("all_tags"));
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"tags\" <@ \"all_tags\"");
    }

    #[test]
    fn test_array_overlap() {
        let expr = Expr::col("tags").array_overlap(Expr::col("search_tags"));
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "\"tags\" && \"search_tags\"");
    }

    #[test]
    fn test_array_any_eq() {
        let expr = Expr::col("tags").array_any_eq("admin");
        let mut params = Vec::new();
        let sql = expr.build_with_dialect(Dialect::Postgres, &mut params, 0);
        assert_eq!(sql, "$1 = ANY(\"tags\")");
        assert_eq!(params.len(), 1);
    }
}
