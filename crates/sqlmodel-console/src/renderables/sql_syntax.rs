//! SQL syntax highlighting for query display.
//!
//! Provides syntax highlighting for SQL queries with theme-based coloring.
//!
//! # Example
//!
//! ```rust
//! use sqlmodel_console::renderables::SqlHighlighter;
//! use sqlmodel_console::Theme;
//!
//! let highlighter = SqlHighlighter::new();
//! let sql = "SELECT * FROM users WHERE id = 1";
//!
//! // Get highlighted version
//! let highlighted = highlighter.highlight(sql);
//! println!("{}", highlighted);
//!
//! // Or plain version
//! let plain = highlighter.plain(sql);
//! println!("{}", plain);
//! ```

use crate::theme::Theme;

/// SQL token types for syntax highlighting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlToken {
    /// SQL keyword (SELECT, FROM, WHERE, etc.)
    Keyword,
    /// String literal ('value')
    String,
    /// Numeric literal (42, 3.14)
    Number,
    /// SQL comment (-- comment or /* comment */)
    Comment,
    /// SQL operator (=, <, >, AND, OR, etc.)
    Operator,
    /// Identifier (table name, column name)
    Identifier,
    /// Punctuation (, ; ( ))
    Punctuation,
    /// Whitespace
    Whitespace,
    /// Parameter placeholder ($1, ?, :name)
    Parameter,
}

/// A segment of SQL text with its token type.
#[derive(Debug, Clone)]
pub struct SqlSegment {
    /// The text content
    pub text: String,
    /// The token type
    pub token: SqlToken,
}

/// SQL syntax highlighter.
///
/// Tokenizes SQL queries and produces highlighted output using theme colors.
#[derive(Debug, Clone)]
pub struct SqlHighlighter {
    /// Theme for coloring
    theme: Theme,
}

impl SqlHighlighter {
    /// SQL keywords to highlight.
    const KEYWORDS: &'static [&'static str] = &[
        // DML
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "FROM",
        "WHERE",
        "SET",
        "VALUES",
        "INTO",
        "JOIN",
        "LEFT",
        "RIGHT",
        "INNER",
        "OUTER",
        "FULL",
        "CROSS",
        "ON",
        "USING",
        "AS",
        "DISTINCT",
        "ALL",
        "ORDER",
        "BY",
        "ASC",
        "DESC",
        "NULLS",
        "FIRST",
        "LAST",
        "LIMIT",
        "OFFSET",
        "FETCH",
        "NEXT",
        "ROWS",
        "ONLY",
        "GROUP",
        "HAVING",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "BETWEEN",
        "IN",
        "LIKE",
        "ILIKE",
        "SIMILAR",
        "TO",
        "EXISTS",
        "ANY",
        "SOME",
        "RETURNING",
        "WITH",
        "RECURSIVE",
        // DDL
        "CREATE",
        "ALTER",
        "DROP",
        "TRUNCATE",
        "TABLE",
        "INDEX",
        "VIEW",
        "SCHEMA",
        "DATABASE",
        "CONSTRAINT",
        "PRIMARY",
        "KEY",
        "FOREIGN",
        "REFERENCES",
        "UNIQUE",
        "CHECK",
        "DEFAULT",
        "NOT",
        "NULL",
        "AUTO_INCREMENT",
        "AUTOINCREMENT",
        "SERIAL",
        "IF",
        "CASCADE",
        "RESTRICT",
        // TCL
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "TRANSACTION",
        "START",
        "RELEASE",
        // Types
        "INTEGER",
        "INT",
        "BIGINT",
        "SMALLINT",
        "TINYINT",
        "REAL",
        "FLOAT",
        "DOUBLE",
        "PRECISION",
        "DECIMAL",
        "NUMERIC",
        "VARCHAR",
        "CHAR",
        "TEXT",
        "BLOB",
        "BYTEA",
        "BOOLEAN",
        "BOOL",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "INTERVAL",
        "UUID",
        "JSON",
        "JSONB",
        "ARRAY",
        // Functions
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "COALESCE",
        "NULLIF",
        "CAST",
        "EXTRACT",
        "NOW",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "LOWER",
        "UPPER",
        "TRIM",
        "SUBSTRING",
        "LENGTH",
        "CONCAT",
        "REPLACE",
    ];

    /// Create a new SQL highlighter with the default theme.
    #[must_use]
    pub fn new() -> Self {
        Self {
            theme: Theme::default(),
        }
    }

    /// Create a new SQL highlighter with a specific theme.
    #[must_use]
    pub fn with_theme(theme: Theme) -> Self {
        Self { theme }
    }

    /// Set the theme.
    #[must_use]
    pub fn theme(mut self, theme: Theme) -> Self {
        self.theme = theme;
        self
    }

    /// Check if a word is a SQL keyword.
    fn is_keyword(word: &str) -> bool {
        let upper = word.to_uppercase();
        Self::KEYWORDS.contains(&upper.as_str())
    }

    /// Check if a word is a SQL operator keyword.
    fn is_operator_keyword(word: &str) -> bool {
        let upper = word.to_uppercase();
        matches!(
            upper.as_str(),
            "AND" | "OR" | "NOT" | "IS" | "BETWEEN" | "LIKE" | "ILIKE" | "IN"
        )
    }

    /// Tokenize SQL into segments.
    #[must_use]
    pub fn tokenize(&self, sql: &str) -> Vec<SqlSegment> {
        let mut segments = Vec::new();
        let chars: Vec<char> = sql.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            let c = chars[i];

            // Whitespace
            if c.is_whitespace() {
                let start = i;
                while i < chars.len() && chars[i].is_whitespace() {
                    i += 1;
                }
                segments.push(SqlSegment {
                    text: chars[start..i].iter().collect(),
                    token: SqlToken::Whitespace,
                });
                continue;
            }

            // Single-line comment (-- ...)
            if c == '-' && i + 1 < chars.len() && chars[i + 1] == '-' {
                let start = i;
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
                segments.push(SqlSegment {
                    text: chars[start..i].iter().collect(),
                    token: SqlToken::Comment,
                });
                continue;
            }

            // Multi-line comment (/* ... */)
            if c == '/' && i + 1 < chars.len() && chars[i + 1] == '*' {
                let start = i;
                i += 2;
                while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                    i += 1;
                }
                if i + 1 < chars.len() {
                    i += 2; // Skip */
                }
                segments.push(SqlSegment {
                    text: chars[start..i].iter().collect(),
                    token: SqlToken::Comment,
                });
                continue;
            }

            // String literal ('...')
            if c == '\'' {
                let start = i;
                i += 1;
                while i < chars.len() {
                    if chars[i] == '\'' {
                        if i + 1 < chars.len() && chars[i + 1] == '\'' {
                            i += 2; // Escaped quote
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
                segments.push(SqlSegment {
                    text: chars[start..i].iter().collect(),
                    token: SqlToken::String,
                });
                continue;
            }

            // Double-quoted identifier ("...")
            if c == '"' {
                let start = i;
                i += 1;
                while i < chars.len() && chars[i] != '"' {
                    i += 1;
                }
                if i < chars.len() {
                    i += 1;
                }
                segments.push(SqlSegment {
                    text: chars[start..i].iter().collect(),
                    token: SqlToken::Identifier,
                });
                continue;
            }

            // Parameter placeholder ($1, $2, ?)
            if c == '$' || c == '?' {
                let start = i;
                i += 1;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                segments.push(SqlSegment {
                    text: chars[start..i].iter().collect(),
                    token: SqlToken::Parameter,
                });
                continue;
            }

            // Named parameter (:name)
            if c == ':'
                && i + 1 < chars.len()
                && (chars[i + 1].is_alphabetic() || chars[i + 1] == '_')
            {
                let start = i;
                i += 1;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                segments.push(SqlSegment {
                    text: chars[start..i].iter().collect(),
                    token: SqlToken::Parameter,
                });
                continue;
            }

            // Number
            if c.is_ascii_digit()
                || (c == '.' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit())
            {
                let start = i;
                let mut has_dot = c == '.';
                i += 1;
                while i < chars.len() {
                    if chars[i].is_ascii_digit() {
                        i += 1;
                    } else if chars[i] == '.' && !has_dot {
                        has_dot = true;
                        i += 1;
                    } else if chars[i] == 'e' || chars[i] == 'E' {
                        i += 1;
                        if i < chars.len() && (chars[i] == '+' || chars[i] == '-') {
                            i += 1;
                        }
                    } else {
                        break;
                    }
                }
                segments.push(SqlSegment {
                    text: chars[start..i].iter().collect(),
                    token: SqlToken::Number,
                });
                continue;
            }

            // Identifier or keyword
            if c.is_alphabetic() || c == '_' {
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                let token = if Self::is_operator_keyword(&word) {
                    SqlToken::Operator
                } else if Self::is_keyword(&word) {
                    SqlToken::Keyword
                } else {
                    SqlToken::Identifier
                };
                segments.push(SqlSegment { text: word, token });
                continue;
            }

            // Operators and punctuation
            if matches!(c, '=' | '<' | '>' | '!' | '+' | '-' | '*' | '/' | '%' | '|') {
                let start = i;
                i += 1;
                // Handle multi-char operators
                if i < chars.len() {
                    let next = chars[i];
                    let is_two_char_op =
                        matches!((c, next), ('<', '>' | '=') | ('>' | '!', '=') | ('|', '|'));
                    if is_two_char_op {
                        i += 1;
                    }
                }
                segments.push(SqlSegment {
                    text: chars[start..i].iter().collect(),
                    token: SqlToken::Operator,
                });
                continue;
            }

            // Punctuation
            if matches!(c, '(' | ')' | ',' | ';' | '.') {
                segments.push(SqlSegment {
                    text: c.to_string(),
                    token: SqlToken::Punctuation,
                });
                i += 1;
                continue;
            }

            // Unknown - treat as identifier
            segments.push(SqlSegment {
                text: c.to_string(),
                token: SqlToken::Identifier,
            });
            i += 1;
        }

        segments
    }

    /// Get the ANSI color code for a token type.
    fn color_for_token(&self, token: SqlToken) -> String {
        match token {
            SqlToken::Keyword => self.theme.sql_keyword.color_code(),
            SqlToken::String => self.theme.sql_string.color_code(),
            SqlToken::Number => self.theme.sql_number.color_code(),
            SqlToken::Comment => self.theme.sql_comment.color_code(),
            SqlToken::Operator => self.theme.sql_operator.color_code(),
            SqlToken::Identifier => self.theme.sql_identifier.color_code(),
            SqlToken::Parameter => self.theme.info.color_code(),
            SqlToken::Punctuation | SqlToken::Whitespace => String::new(),
        }
    }

    /// Highlight SQL with ANSI colors.
    #[must_use]
    pub fn highlight(&self, sql: &str) -> String {
        let segments = self.tokenize(sql);
        let reset = "\x1b[0m";

        segments
            .iter()
            .map(|seg| {
                let color = self.color_for_token(seg.token);
                if color.is_empty() {
                    seg.text.clone()
                } else {
                    format!("{}{}{}", color, seg.text, reset)
                }
            })
            .collect()
    }

    /// Return plain SQL (no highlighting).
    #[must_use]
    pub fn plain(&self, sql: &str) -> String {
        sql.to_string()
    }

    /// Format SQL with indentation (basic pretty-print).
    #[must_use]
    pub fn format(&self, sql: &str) -> String {
        let segments = self.tokenize(sql);
        let mut result = String::new();
        let mut indent = 0;
        let indent_str = "  ";
        let mut newline_before = false;

        for seg in segments {
            let upper = seg.text.to_uppercase();

            // Keywords that start a new line with same indentation
            if matches!(
                upper.as_str(),
                "SELECT"
                    | "FROM"
                    | "WHERE"
                    | "ORDER"
                    | "GROUP"
                    | "HAVING"
                    | "LIMIT"
                    | "OFFSET"
                    | "SET"
                    | "VALUES"
                    | "RETURNING"
                    | "UNION"
                    | "INTERSECT"
                    | "EXCEPT"
            ) {
                if !result.is_empty() && !result.ends_with('\n') {
                    result.push('\n');
                }
                result.push_str(&indent_str.repeat(indent));
                newline_before = false;
            }

            // Keywords that increase indentation
            if matches!(upper.as_str(), "(" | "CASE") {
                indent += 1;
            }

            // Keywords that decrease indentation
            if matches!(upper.as_str(), ")" | "END") {
                indent = indent.saturating_sub(1);
            }

            // Add keyword that needs newline before
            if matches!(upper.as_str(), "AND" | "OR")
                && !newline_before
                && !result.ends_with('\n')
                && !result.ends_with(' ')
            {
                result.push('\n');
                result.push_str(&indent_str.repeat(indent + 1));
            }

            // Handle JOIN keywords
            if matches!(
                upper.as_str(),
                "JOIN" | "LEFT" | "RIGHT" | "INNER" | "OUTER" | "CROSS" | "FULL"
            ) {
                if !result.ends_with('\n') && !result.ends_with(' ') && upper != "JOIN" {
                    // Keep LEFT/RIGHT/etc with JOIN on same line
                } else if upper == "JOIN" && !result.ends_with(' ') {
                    result.push(' ');
                }
            }

            // Append the text
            if seg.token == SqlToken::Whitespace {
                // Normalize whitespace
                if !result.ends_with(' ') && !result.ends_with('\n') {
                    result.push(' ');
                }
            } else {
                result.push_str(&seg.text);
            }

            newline_before = seg.token == SqlToken::Whitespace && seg.text.contains('\n');
        }

        result.trim().to_string()
    }
}

impl Default for SqlHighlighter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_highlighter_new() {
        let h = SqlHighlighter::new();
        assert!(h.highlight("SELECT 1").contains("SELECT"));
    }

    #[test]
    fn test_tokenize_select() {
        let h = SqlHighlighter::new();
        let segments = h.tokenize("SELECT * FROM users");

        let tokens: Vec<SqlToken> = segments.iter().map(|s| s.token).collect();
        assert!(tokens.contains(&SqlToken::Keyword));
        assert!(tokens.contains(&SqlToken::Identifier));
    }

    #[test]
    fn test_tokenize_string() {
        let h = SqlHighlighter::new();
        let segments = h.tokenize("SELECT 'hello'");

        let has_string = segments
            .iter()
            .any(|s| s.token == SqlToken::String && s.text == "'hello'");
        assert!(has_string);
    }

    #[test]
    fn test_tokenize_number() {
        let h = SqlHighlighter::new();
        let segments = h.tokenize("SELECT 42, 3.14");

        let numbers: Vec<&str> = segments
            .iter()
            .filter(|s| s.token == SqlToken::Number)
            .map(|s| s.text.as_str())
            .collect();
        assert!(numbers.contains(&"42"));
        assert!(numbers.contains(&"3.14"));
    }

    #[test]
    fn test_tokenize_comment_single() {
        let h = SqlHighlighter::new();
        let segments = h.tokenize("SELECT 1 -- comment");

        let has_comment = segments.iter().any(|s| s.token == SqlToken::Comment);
        assert!(has_comment);
    }

    #[test]
    fn test_tokenize_comment_multi() {
        let h = SqlHighlighter::new();
        let segments = h.tokenize("SELECT /* comment */ 1");

        let has_comment = segments.iter().any(|s| s.token == SqlToken::Comment);
        assert!(has_comment);
    }

    #[test]
    fn test_tokenize_parameter_positional() {
        let h = SqlHighlighter::new();
        let segments = h.tokenize("SELECT * FROM users WHERE id = $1");

        let has_param = segments
            .iter()
            .any(|s| s.token == SqlToken::Parameter && s.text == "$1");
        assert!(has_param);
    }

    #[test]
    fn test_tokenize_parameter_question() {
        let h = SqlHighlighter::new();
        let segments = h.tokenize("SELECT * FROM users WHERE id = ?");

        let has_param = segments
            .iter()
            .any(|s| s.token == SqlToken::Parameter && s.text == "?");
        assert!(has_param);
    }

    #[test]
    fn test_tokenize_parameter_named() {
        let h = SqlHighlighter::new();
        let segments = h.tokenize("SELECT * FROM users WHERE id = :user_id");

        let has_param = segments
            .iter()
            .any(|s| s.token == SqlToken::Parameter && s.text == ":user_id");
        assert!(has_param);
    }

    #[test]
    fn test_tokenize_operators() {
        let h = SqlHighlighter::new();
        let segments = h.tokenize("SELECT * FROM users WHERE age >= 18 AND active = true");

        let has_ge = segments
            .iter()
            .any(|s| s.token == SqlToken::Operator && s.text == ">=");
        let has_and = segments
            .iter()
            .any(|s| s.token == SqlToken::Operator && s.text.to_uppercase() == "AND");
        assert!(has_ge);
        assert!(has_and);
    }

    #[test]
    fn test_tokenize_quoted_identifier() {
        let h = SqlHighlighter::new();
        let segments = h.tokenize("SELECT \"user-name\" FROM users");

        let has_quoted = segments
            .iter()
            .any(|s| s.token == SqlToken::Identifier && s.text == "\"user-name\"");
        assert!(has_quoted);
    }

    #[test]
    fn test_highlight_produces_ansi() {
        let h = SqlHighlighter::new();
        let highlighted = h.highlight("SELECT 1");

        // Should contain ANSI escape codes
        assert!(highlighted.contains('\x1b'));
        // Should contain the text
        assert!(highlighted.contains("SELECT"));
        assert!(highlighted.contains('1'));
    }

    #[test]
    fn test_plain_no_change() {
        let h = SqlHighlighter::new();
        let sql = "SELECT * FROM users";
        assert_eq!(h.plain(sql), sql);
    }

    #[test]
    fn test_format_basic() {
        let h = SqlHighlighter::new();
        let sql = "SELECT * FROM users WHERE id = 1";
        let formatted = h.format(sql);

        // Should have newlines for major clauses
        assert!(formatted.contains("SELECT"));
        assert!(formatted.contains("FROM"));
        assert!(formatted.contains("WHERE"));
    }

    #[test]
    fn test_is_keyword() {
        assert!(SqlHighlighter::is_keyword("SELECT"));
        assert!(SqlHighlighter::is_keyword("select"));
        assert!(SqlHighlighter::is_keyword("Select"));
        assert!(!SqlHighlighter::is_keyword("users"));
    }

    #[test]
    fn test_is_operator_keyword() {
        assert!(SqlHighlighter::is_operator_keyword("AND"));
        assert!(SqlHighlighter::is_operator_keyword("or"));
        assert!(!SqlHighlighter::is_operator_keyword("SELECT"));
    }

    #[test]
    fn test_escaped_string() {
        let h = SqlHighlighter::new();
        let segments = h.tokenize("SELECT 'it''s'");

        let string_seg = segments.iter().find(|s| s.token == SqlToken::String);
        assert!(string_seg.is_some());
        assert_eq!(string_seg.unwrap().text, "'it''s'");
    }

    #[test]
    fn test_scientific_notation() {
        let h = SqlHighlighter::new();
        let segments = h.tokenize("SELECT 1.5e10");

        let has_num = segments
            .iter()
            .any(|s| s.token == SqlToken::Number && s.text.contains('e'));
        assert!(has_num);
    }

    #[test]
    fn test_with_theme() {
        let h = SqlHighlighter::with_theme(Theme::light());
        let highlighted = h.highlight("SELECT 1");
        assert!(highlighted.contains('\x1b'));
    }

    #[test]
    fn test_builder_theme() {
        let h = SqlHighlighter::new().theme(Theme::dark());
        let highlighted = h.highlight("SELECT 1");
        assert!(highlighted.contains('\x1b'));
    }
}
