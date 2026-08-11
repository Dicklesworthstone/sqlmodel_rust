//! Error panel renderable for beautiful error display.
//!
//! Provides a panel specifically designed for displaying errors with rich formatting
//! in styled mode and structured plain text in plain mode.
//!
//! # Example
//!
//! ```rust
//! use sqlmodel_console::renderables::{ErrorPanel, ErrorSeverity};
//!
//! let panel = ErrorPanel::new("SQL Syntax Error", "Unexpected token 'SELCT'")
//!     .severity(ErrorSeverity::Error)
//!     .with_sql("SELCT * FROM users WHERE id = $1")
//!     .with_position(1)
//!     .with_sqlstate("42601")
//!     .with_hint("Did you mean 'SELECT'?");
//!
//! // Plain mode output
//! println!("{}", panel.render_plain());
//! ```

use crate::theme::Theme;

/// Error severity level for styling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSeverity {
    /// Critical error - red, urgent
    Critical,
    /// Standard error - red
    Error,
    /// Warning - yellow
    Warning,
    /// Notice - cyan (informational)
    Notice,
}

impl ErrorSeverity {
    /// Get a human-readable severity string.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Critical => "CRITICAL",
            Self::Error => "ERROR",
            Self::Warning => "WARNING",
            Self::Notice => "NOTICE",
        }
    }

    /// Get the ANSI color code for this severity.
    #[must_use]
    pub fn color_code(&self) -> &'static str {
        match self {
            Self::Critical => "\x1b[91m", // Bright red
            Self::Error => "\x1b[31m",    // Red
            Self::Warning => "\x1b[33m",  // Yellow
            Self::Notice => "\x1b[36m",   // Cyan
        }
    }

    /// Get the ANSI reset code.
    #[must_use]
    pub fn reset_code() -> &'static str {
        "\x1b[0m"
    }
}

impl std::fmt::Display for ErrorSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A panel specifically designed for error display.
///
/// Provides rich formatting for error messages including SQL context,
/// position markers, hints, and SQLSTATE codes.
#[derive(Debug, Clone)]
pub struct ErrorPanel {
    /// Error severity for styling
    severity: ErrorSeverity,
    /// Panel title (e.g., "SQL Syntax Error")
    title: String,
    /// Main error message
    message: String,
    /// Optional SQL query that caused the error
    sql: Option<String>,
    /// Position in SQL where error occurred (1-indexed)
    sql_position: Option<usize>,
    /// SQLSTATE code (PostgreSQL error code)
    sqlstate: Option<String>,
    /// Additional detail from database
    detail: Option<String>,
    /// Hint for fixing the error
    hint: Option<String>,
    /// Additional context lines
    context: Vec<String>,
    /// Theme for styled output
    theme: Option<Theme>,
    /// Panel width for styled output
    width: Option<usize>,
}

impl ErrorPanel {
    /// Create a new error panel with title and message.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlmodel_console::renderables::ErrorPanel;
    ///
    /// let panel = ErrorPanel::new("Connection Error", "Failed to connect to database");
    /// ```
    #[must_use]
    pub fn new(title: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            severity: ErrorSeverity::Error,
            title: title.into(),
            message: message.into(),
            sql: None,
            sql_position: None,
            sqlstate: None,
            detail: None,
            hint: None,
            context: Vec::new(),
            theme: None,
            width: None,
        }
    }

    /// Set error severity.
    #[must_use]
    pub fn severity(mut self, severity: ErrorSeverity) -> Self {
        self.severity = severity;
        self
    }

    /// Add SQL query context.
    #[must_use]
    pub fn with_sql(mut self, sql: impl Into<String>) -> Self {
        self.sql = Some(sql.into());
        self
    }

    /// Add error position in SQL (1-indexed character position).
    #[must_use]
    pub fn with_position(mut self, position: usize) -> Self {
        self.sql_position = Some(position);
        self
    }

    /// Add SQLSTATE code.
    #[must_use]
    pub fn with_sqlstate(mut self, code: impl Into<String>) -> Self {
        self.sqlstate = Some(code.into());
        self
    }

    /// Add detail message.
    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// Add hint for fixing the error.
    #[must_use]
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    /// Add context line.
    #[must_use]
    pub fn add_context(mut self, line: impl Into<String>) -> Self {
        self.context.push(line.into());
        self
    }

    /// Set the theme for styled output.
    #[must_use]
    pub fn theme(mut self, theme: Theme) -> Self {
        self.theme = Some(theme);
        self
    }

    /// Set the panel width for styled output.
    #[must_use]
    pub fn width(mut self, width: usize) -> Self {
        self.width = Some(width);
        self
    }

    /// Get the severity level.
    #[must_use]
    pub fn get_severity(&self) -> ErrorSeverity {
        self.severity
    }

    /// Get the title.
    #[must_use]
    pub fn get_title(&self) -> &str {
        &self.title
    }

    /// Get the message.
    #[must_use]
    pub fn get_message(&self) -> &str {
        &self.message
    }

    /// Get the SQL query if set.
    #[must_use]
    pub fn get_sql(&self) -> Option<&str> {
        self.sql.as_deref()
    }

    /// Get the SQL position if set.
    #[must_use]
    pub fn get_position(&self) -> Option<usize> {
        self.sql_position
    }

    /// Get the SQLSTATE code if set.
    #[must_use]
    pub fn get_sqlstate(&self) -> Option<&str> {
        self.sqlstate.as_deref()
    }

    /// Get the detail message if set.
    #[must_use]
    pub fn get_detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    /// Get the hint if set.
    #[must_use]
    pub fn get_hint(&self) -> Option<&str> {
        self.hint.as_deref()
    }

    /// Get the context lines.
    #[must_use]
    pub fn get_context(&self) -> &[String] {
        &self.context
    }

    /// Render as plain text.
    ///
    /// Returns a structured plain text representation suitable for
    /// non-TTY environments or agent consumption.
    #[must_use]
    pub fn render_plain(&self) -> String {
        let mut lines = Vec::new();

        // Header with severity and title
        lines.push(format!("=== {} [{}] ===", self.title, self.severity));
        lines.push(String::new());
        lines.push(self.message.clone());

        // SQL context with position marker
        if let Some(ref sql) = self.sql {
            lines.push(String::new());
            lines.push("Query:".to_string());
            lines.push(format!("  {sql}"));

            if let Some(pos) = self.sql_position {
                let marker_pos = pos.saturating_sub(1);
                lines.push(format!("  {}^", " ".repeat(marker_pos)));
            }
        }

        // Detail
        if let Some(ref detail) = self.detail {
            lines.push(String::new());
            lines.push(format!("Detail: {detail}"));
        }

        // Hint
        if let Some(ref hint) = self.hint {
            lines.push(String::new());
            lines.push(format!("Hint: {hint}"));
        }

        // SQLSTATE
        if let Some(ref code) = self.sqlstate {
            lines.push(String::new());
            lines.push(format!("SQLSTATE: {code}"));
        }

        // Context lines
        for line in &self.context {
            lines.push(line.clone());
        }

        lines.join("\n")
    }

    /// Render as styled text with ANSI colors and box drawing.
    ///
    /// Returns a rich panel representation with colored borders
    /// and formatted content.
    #[must_use]
    pub fn render_styled(&self) -> String {
        let theme = self.theme.clone().unwrap_or_default();
        let width = self.width.unwrap_or(70).max(6);
        let inner_width = width.saturating_sub(4); // Account for borders and padding

        let color = self.severity.color_code();
        let reset = ErrorSeverity::reset_code();
        let dim = "\x1b[2m";

        let mut lines = Vec::new();

        // Top border with title
        let max_title_chars = width.saturating_sub(4);
        let title_text = self.truncate_plain_to_width(&self.title, max_title_chars);
        let title = format!(" {title_text} ");
        let title_len = title.chars().count();
        let border_space = width.saturating_sub(2);
        let total_pad = border_space.saturating_sub(title_len);
        let left_pad = total_pad / 2;
        let right_pad = total_pad.saturating_sub(left_pad);
        let top_border = format!(
            "{color}╭{}{}{}╮{reset}",
            "─".repeat(left_pad),
            title,
            "─".repeat(right_pad)
        );
        lines.push(top_border);

        // Empty line
        lines.push(format!(
            "{color}│{reset}{:width$}{color}│{reset}",
            "",
            width = width - 2
        ));

        // Severity badge
        let severity_line = format!(
            "  {}{}{} {}",
            color,
            self.severity.as_str(),
            reset,
            self.message
        );
        lines.push(self.wrap_line(&severity_line, width, color, reset));

        // Empty line after message
        lines.push(format!(
            "{color}│{reset}{:width$}{color}│{reset}",
            "",
            width = width - 2
        ));

        // SQL context with position marker
        if let Some(ref sql) = self.sql {
            // SQL box header
            let sql_header = format!(
                "{dim}┌─ Query ─{}┐{reset}",
                "─".repeat(inner_width.saturating_sub(12))
            );
            lines.push(format!("{color}│{reset} {sql_header} {color}│{reset}"));

            // SQL content (may need truncation for very long queries)
            let sql_content_width = inner_width.saturating_sub(4);
            let sql_display = self.truncate_plain_to_width(sql, sql_content_width);
            lines.push(format!(
                "{color}│{reset} {dim}│{reset} {:<width$} {dim}│{reset} {color}│{reset}",
                sql_display,
                width = sql_content_width
            ));

            // Position marker
            if let Some(pos) = self.sql_position {
                let marker_pos = pos.saturating_sub(1).min(inner_width.saturating_sub(5));
                let marker_line = format!("{}^", " ".repeat(marker_pos));
                lines.push(format!(
                    "{color}│{reset} {dim}│{reset} {}{:<width$}{reset} {dim}│{reset} {color}│{reset}",
                    theme.error.color_code(),
                    marker_line,
                    width = sql_content_width
                ));
            }

            // SQL box footer
            let sql_footer = format!(
                "{dim}└{}┘{reset}",
                "─".repeat(inner_width.saturating_sub(2))
            );
            lines.push(format!("{color}│{reset} {sql_footer} {color}│{reset}"));

            // Empty line after SQL
            lines.push(format!(
                "{color}│{reset}{:width$}{color}│{reset}",
                "",
                width = width - 2
            ));
        }

        // Detail
        if let Some(ref detail) = self.detail {
            let detail_line = format!("  Detail: {detail}");
            lines.push(self.wrap_line(&detail_line, width, color, reset));
        }

        // Hint with lightbulb
        if let Some(ref hint) = self.hint {
            let hint_color = self.get_hint_color(&theme);
            let hint_line = format!("  {hint_color}💡 Hint: {hint}{reset}");
            lines.push(self.wrap_line(&hint_line, width, color, reset));
        }

        // SQLSTATE
        if let Some(ref code) = self.sqlstate {
            let sqlstate_line = format!("  {dim}SQLSTATE: {code}{reset}");
            lines.push(self.wrap_line(&sqlstate_line, width, color, reset));
        }

        // Context lines
        for line in &self.context {
            let context_line = format!("  {line}");
            lines.push(self.wrap_line(&context_line, width, color, reset));
        }

        // Empty line before bottom border
        lines.push(format!(
            "{color}│{reset}{:width$}{color}│{reset}",
            "",
            width = width - 2
        ));

        // Bottom border
        let bottom_border = format!("{color}╰{}╯{reset}", "─".repeat(width - 2));
        lines.push(bottom_border);

        lines.join("\n")
    }

    /// Wrap a line to fit within the panel, accounting for ANSI codes.
    fn wrap_line(&self, content: &str, width: usize, border_color: &str, reset: &str) -> String {
        let inner_width = width.saturating_sub(2);
        let mut rendered = content.to_string();
        if self.visible_length(&rendered) > inner_width {
            rendered = self.truncate_ansi_to_width(&rendered, inner_width, reset);
        }
        let visible_len = self.visible_length(&rendered);
        let padding = inner_width.saturating_sub(visible_len);

        format!(
            "{border_color}│{reset}{rendered}{:padding$}{border_color}│{reset}",
            "",
            padding = padding
        )
    }

    fn truncate_ansi_to_width(&self, s: &str, max_visible: usize, reset: &str) -> String {
        let mut out = String::new();
        let mut visible = 0usize;
        let mut in_escape = false;

        for c in s.chars() {
            if c == '\x1b' {
                in_escape = true;
                out.push(c);
                continue;
            }
            if in_escape {
                out.push(c);
                if c == 'm' {
                    in_escape = false;
                }
                continue;
            }

            if visible >= max_visible {
                break;
            }

            out.push(c);
            if c == '💡' {
                visible = visible.saturating_add(2);
            } else {
                visible = visible.saturating_add(1);
            }
        }

        if s.contains('\x1b') && !out.ends_with(reset) {
            out.push_str(reset);
        }

        out
    }

    fn truncate_plain_to_width(&self, s: &str, max_visible: usize) -> String {
        if max_visible == 0 {
            return String::new();
        }

        let char_count = s.chars().count();
        if char_count <= max_visible {
            return s.to_string();
        }

        if max_visible <= 3 {
            return ".".repeat(max_visible);
        }

        let truncated: String = s.chars().take(max_visible - 3).collect();
        format!("{truncated}...")
    }

    /// Calculate visible length of a string (excluding ANSI codes).
    fn visible_length(&self, s: &str) -> usize {
        let mut len = 0;
        let mut in_escape = false;

        for c in s.chars() {
            if c == '\x1b' {
                in_escape = true;
            } else if in_escape {
                if c == 'm' {
                    in_escape = false;
                }
            } else {
                // Count emoji as 2 characters for terminal width
                if c == '💡' {
                    len += 2;
                } else {
                    len += 1;
                }
            }
        }
        len
    }

    /// Get the hint color code from theme.
    fn get_hint_color(&self, theme: &Theme) -> String {
        let (r, g, b) = theme.info.rgb();
        format!("\x1b[38;2;{r};{g};{b}m")
    }

    /// Render as JSON-serializable structure.
    ///
    /// Returns a JSON value suitable for structured logging or API responses.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "severity": self.severity.as_str(),
            "title": self.title,
            "message": self.message,
            "sql": self.sql,
            "position": self.sql_position,
            "sqlstate": self.sqlstate,
            "detail": self.detail,
            "hint": self.hint,
            "context": self.context,
        })
    }
}

impl Default for ErrorPanel {
    fn default() -> Self {
        Self::new("Error", "An error occurred")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_panel_basic() {
        let panel = ErrorPanel::new("Test Error", "Something went wrong");
        assert_eq!(panel.get_title(), "Test Error");
        assert_eq!(panel.get_message(), "Something went wrong");
        assert_eq!(panel.get_severity(), ErrorSeverity::Error);
    }

    #[test]
    fn test_error_panel_with_sql() {
        let panel = ErrorPanel::new("SQL Error", "Invalid query").with_sql("SELECT * FROM users");
        assert_eq!(panel.get_sql(), Some("SELECT * FROM users"));
    }

    #[test]
    fn test_error_panel_with_position() {
        let panel = ErrorPanel::new("SQL Error", "Syntax error")
            .with_sql("SELCT * FROM users")
            .with_position(1);
        assert_eq!(panel.get_position(), Some(1));
    }

    #[test]
    fn test_error_panel_severity_styles() {
        assert_eq!(
            ErrorPanel::new("", "")
                .severity(ErrorSeverity::Critical)
                .get_severity(),
            ErrorSeverity::Critical
        );
        assert_eq!(
            ErrorPanel::new("", "")
                .severity(ErrorSeverity::Warning)
                .get_severity(),
            ErrorSeverity::Warning
        );
        assert_eq!(
            ErrorPanel::new("", "")
                .severity(ErrorSeverity::Notice)
                .get_severity(),
            ErrorSeverity::Notice
        );
    }

    #[test]
    fn test_error_panel_with_hint() {
        let panel = ErrorPanel::new("Error", "Problem").with_hint("Try this instead");
        assert_eq!(panel.get_hint(), Some("Try this instead"));
    }

    #[test]
    fn test_error_panel_with_detail() {
        let panel = ErrorPanel::new("Error", "Problem").with_detail("More information here");
        assert_eq!(panel.get_detail(), Some("More information here"));
    }

    #[test]
    fn test_error_panel_with_sqlstate() {
        let panel = ErrorPanel::new("Error", "Problem").with_sqlstate("42601");
        assert_eq!(panel.get_sqlstate(), Some("42601"));
    }

    #[test]
    fn test_error_panel_add_context() {
        let panel = ErrorPanel::new("Error", "Problem")
            .add_context("Line 1")
            .add_context("Line 2");
        assert_eq!(panel.get_context(), &["Line 1", "Line 2"]);
    }

    #[test]
    fn test_error_panel_to_plain() {
        let panel = ErrorPanel::new("SQL Syntax Error", "Unexpected token")
            .with_sql("SELCT * FROM users")
            .with_position(1)
            .with_hint("Did you mean 'SELECT'?")
            .with_sqlstate("42601");

        let plain = panel.render_plain();

        assert!(plain.contains("SQL Syntax Error"));
        assert!(plain.contains("ERROR"));
        assert!(plain.contains("Unexpected token"));
        assert!(plain.contains("Query:"));
        assert!(plain.contains("SELCT * FROM users"));
        assert!(plain.contains('^')); // Position marker
        assert!(plain.contains("Hint:"));
        assert!(plain.contains("SQLSTATE: 42601"));
    }

    #[test]
    fn test_error_panel_to_plain_minimal() {
        let panel = ErrorPanel::new("Error", "Something failed");
        let plain = panel.render_plain();

        assert!(plain.contains("Error"));
        assert!(plain.contains("Something failed"));
        assert!(!plain.contains("Query:")); // No SQL
        assert!(!plain.contains("Hint:")); // No hint
        assert!(!plain.contains("SQLSTATE:")); // No SQLSTATE
    }

    #[test]
    fn test_error_panel_to_json() {
        let panel = ErrorPanel::new("Test", "Message")
            .with_sql("SELECT 1")
            .with_position(5)
            .with_sqlstate("00000")
            .with_hint("No hint needed");

        let json = panel.to_json();

        assert_eq!(json["severity"], "ERROR");
        assert_eq!(json["title"], "Test");
        assert_eq!(json["message"], "Message");
        assert_eq!(json["sql"], "SELECT 1");
        assert_eq!(json["position"], 5);
        assert_eq!(json["sqlstate"], "00000");
        assert_eq!(json["hint"], "No hint needed");
    }

    #[test]
    fn test_error_panel_to_json_null_fields() {
        let panel = ErrorPanel::new("Test", "Message");
        let json = panel.to_json();

        assert!(json["sql"].is_null());
        assert!(json["position"].is_null());
        assert!(json["sqlstate"].is_null());
        assert!(json["hint"].is_null());
        assert!(json["detail"].is_null());
    }

    #[test]
    fn test_error_panel_multiple_context() {
        let panel = ErrorPanel::new("Error", "Problem")
            .add_context("Context 1")
            .add_context("Context 2")
            .add_context("Context 3");

        let plain = panel.render_plain();
        assert!(plain.contains("Context 1"));
        assert!(plain.contains("Context 2"));
        assert!(plain.contains("Context 3"));
    }

    #[test]
    fn test_error_panel_empty_fields() {
        let panel = ErrorPanel::new("", "");
        assert_eq!(panel.get_title(), "");
        assert_eq!(panel.get_message(), "");
        assert!(panel.get_sql().is_none());
    }

    #[test]
    fn test_error_severity_as_str() {
        assert_eq!(ErrorSeverity::Critical.as_str(), "CRITICAL");
        assert_eq!(ErrorSeverity::Error.as_str(), "ERROR");
        assert_eq!(ErrorSeverity::Warning.as_str(), "WARNING");
        assert_eq!(ErrorSeverity::Notice.as_str(), "NOTICE");
    }

    #[test]
    fn test_error_severity_display() {
        assert_eq!(format!("{}", ErrorSeverity::Critical), "CRITICAL");
        assert_eq!(format!("{}", ErrorSeverity::Error), "ERROR");
    }

    #[test]
    fn test_error_severity_color_codes() {
        assert!(ErrorSeverity::Critical.color_code().contains("91")); // Bright red
        assert!(ErrorSeverity::Error.color_code().contains("31")); // Red
        assert!(ErrorSeverity::Warning.color_code().contains("33")); // Yellow
        assert!(ErrorSeverity::Notice.color_code().contains("36")); // Cyan
    }

    #[test]
    fn test_error_panel_render_styled_contains_box() {
        let panel = ErrorPanel::new("Test", "Message").width(60);
        let styled = panel.render_styled();

        assert!(styled.contains("╭")); // Top left
        assert!(styled.contains("╮")); // Top right
        assert!(styled.contains("╰")); // Bottom left
        assert!(styled.contains("╯")); // Bottom right
        assert!(styled.contains("│")); // Sides
    }

    #[test]
    fn test_error_panel_render_styled_contains_title() {
        let panel = ErrorPanel::new("My Error Title", "Message").width(60);
        let styled = panel.render_styled();

        assert!(styled.contains("My Error Title"));
    }

    #[test]
    fn test_error_panel_render_styled_with_sql() {
        let panel = ErrorPanel::new("SQL Error", "Syntax error")
            .with_sql("SELECT * FROM users")
            .with_position(8)
            .width(70);
        let styled = panel.render_styled();

        assert!(styled.contains("Query")); // SQL box header
        assert!(styled.contains("SELECT * FROM users"));
        assert!(styled.contains('^')); // Position marker
    }

    #[test]
    fn test_error_panel_render_styled_tiny_width_does_not_panic() {
        let panel = ErrorPanel::new("Tiny", "Narrow")
            .with_sql("SELECT * FROM t")
            .with_position(3)
            .width(1);
        let styled = panel.render_styled();

        assert_ne!(styled, "");
        assert!(styled.contains('╭'));
        assert!(styled.contains('╯'));
    }

    #[test]
    fn test_error_panel_render_styled_unicode_sql_truncation() {
        let panel = ErrorPanel::new("Unicode", "Syntax error")
            .with_sql("SELECT '🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥'")
            .width(26);
        let styled = panel.render_styled();

        assert!(styled.contains("Query"));
        assert!(styled.contains("..."));
    }

    #[test]
    fn test_error_panel_default() {
        let panel = ErrorPanel::default();
        assert_eq!(panel.get_title(), "Error");
        assert_eq!(panel.get_message(), "An error occurred");
    }

    #[test]
    fn test_error_panel_builder_chain() {
        let panel = ErrorPanel::new("Chain Test", "Testing builder")
            .severity(ErrorSeverity::Warning)
            .with_sql("SELECT 1")
            .with_position(7)
            .with_sqlstate("00000")
            .with_detail("Some detail")
            .with_hint("A hint")
            .add_context("Context line")
            .theme(Theme::dark())
            .width(80);

        assert_eq!(panel.get_severity(), ErrorSeverity::Warning);
        assert_eq!(panel.get_sql(), Some("SELECT 1"));
        assert_eq!(panel.get_position(), Some(7));
        assert_eq!(panel.get_sqlstate(), Some("00000"));
        assert_eq!(panel.get_detail(), Some("Some detail"));
        assert_eq!(panel.get_hint(), Some("A hint"));
        assert_eq!(panel.get_context().len(), 1);
    }

    #[test]
    fn test_render_plain_with_detail() {
        let panel = ErrorPanel::new("Error", "Problem").with_detail("Additional details here");
        let plain = panel.render_plain();

        assert!(plain.contains("Detail: Additional details here"));
    }

    #[test]
    fn test_position_marker_alignment() {
        // Position 1 should have no leading spaces before ^
        let panel = ErrorPanel::new("Error", "Msg")
            .with_sql("SELCT")
            .with_position(1);
        let plain = panel.render_plain();
        assert!(plain.contains("  ^")); // 2 spaces for indentation, then ^

        // Position 5 should have 4 spaces before ^
        let panel = ErrorPanel::new("Error", "Msg")
            .with_sql("SELCT")
            .with_position(5);
        let plain = panel.render_plain();
        assert!(plain.contains("      ^")); // 2 indent + 4 spaces + ^
    }
}
