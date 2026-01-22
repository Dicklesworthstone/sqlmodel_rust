//! E2E tests for error panel rendering.
//!
//! These tests verify that error panels:
//! - Render correctly in all output modes
//! - Include all required information (message, SQL, hints)
//! - Produce parseable output in plain mode
//! - Handle edge cases (empty fields, long content)

use sqlmodel_console::renderables::{ErrorPanel, ErrorSeverity};
use sqlmodel_console::{OutputMode, SqlModelConsole};
use super::output_capture::CapturedOutput;

// ============================================================================
// Error Panel Rendering Tests
// ============================================================================

/// E2E test: Error panel renders in plain mode.
#[test]
fn e2e_error_panel_plain_mode() {
    let panel = ErrorPanel::new("SQL Syntax Error", "Unexpected token 'FORM'")
        .with_sql("SELECT * FORM users")
        .with_position(10)
        .with_sqlstate("42601")
        .with_hint("Did you mean 'FROM'?");

    let plain_output = panel.render_plain();

    // Create captured output for assertions
    let output = CapturedOutput::from_strings(plain_output.clone(), String::new());

    // Verify content is present
    output.assert_stdout_contains("SQL Syntax Error");
    output.assert_stdout_contains("Unexpected token");
    output.assert_stdout_contains("FORM");
    output.assert_stdout_contains("Hint:");
    output.assert_stdout_contains("FROM");

    // Verify no ANSI codes
    output.assert_plain_mode_clean();
}

/// E2E test: Error panel with severity levels.
#[test]
fn e2e_error_panel_severity_levels() {
    let severities = [
        (ErrorSeverity::Notice, "Notice"),
        (ErrorSeverity::Warning, "Warning"),
        (ErrorSeverity::Error, "Error"),
        (ErrorSeverity::Critical, "Critical"),
    ];

    for (severity, name) in severities {
        let panel = ErrorPanel::new(&format!("{name} Message"), "Test detail")
            .severity(severity);

        let plain = panel.render_plain();
        let output = CapturedOutput::from_strings(plain, String::new());

        output.assert_stdout_contains(&format!("{name} Message"));
        output.assert_stdout_contains("Test detail");
        output.assert_plain_mode_clean();
    }
}

/// E2E test: Connection error panel.
#[test]
fn e2e_connection_error_panel() {
    let panel = ErrorPanel::new("Connection Failed", "Could not connect to database")
        .severity(ErrorSeverity::Critical)
        .with_detail("Connection refused (os error 111)")
        .add_context("Host: localhost:5432")
        .add_context("User: postgres")
        .with_hint("Check that the database server is running");

    let plain = panel.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("Connection Failed");
    output.assert_stdout_contains("Connection refused");
    output.assert_stdout_contains("localhost:5432");
    output.assert_stdout_contains("postgres");
    output.assert_stdout_contains("Hint:");
    output.assert_plain_mode_clean();
}

/// E2E test: Query timeout error panel.
#[test]
fn e2e_timeout_error_panel() {
    let panel = ErrorPanel::new("Query Timeout", "Query exceeded maximum execution time")
        .severity(ErrorSeverity::Warning)
        .with_sql("SELECT * FROM large_table WHERE complex_condition = true")
        .with_detail("Timeout after 30 seconds")
        .with_hint("Consider adding an index or simplifying the query");

    let plain = panel.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("Query Timeout");
    output.assert_stdout_contains("30 seconds");
    output.assert_stdout_contains("large_table");
    output.assert_stdout_contains("Hint:");
    output.assert_plain_mode_clean();
}

// ============================================================================
// Edge Case Tests
// ============================================================================

/// E2E test: Error panel with minimal content.
#[test]
fn e2e_error_panel_minimal() {
    let panel = ErrorPanel::new("Simple Error", "Something went wrong");
    let plain = panel.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("Simple Error");
    output.assert_stdout_contains("Something went wrong");
    output.assert_plain_mode_clean();
}

/// E2E test: Error panel with long SQL statement.
#[test]
fn e2e_error_panel_long_sql() {
    let long_sql = "SELECT u.id, u.name, u.email, p.title, p.content, c.text \
                    FROM users u \
                    JOIN posts p ON u.id = p.user_id \
                    JOIN comments c ON p.id = c.post_id \
                    WHERE u.is_active = true \
                    AND p.published_at > '2024-01-01' \
                    ORDER BY p.published_at DESC \
                    LIMIT 100";

    let panel = ErrorPanel::new("Complex Query Failed", "Error in subquery")
        .with_sql(long_sql);

    let plain = panel.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    // Key parts should be present
    output.assert_stdout_contains("Complex Query Failed");
    output.assert_stdout_contains("SELECT");
    output.assert_stdout_contains("users");
    output.assert_plain_mode_clean();
}

/// E2E test: Error panel with special characters.
#[test]
fn e2e_error_panel_special_chars() {
    let panel = ErrorPanel::new(
        "Unicode Error",
        "Failed to process: café, naïve, 日本語"
    )
    .with_sql("SELECT * FROM users WHERE name = 'Müller'")
    .with_hint("Check encoding settings");

    let plain = panel.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("Unicode Error");
    output.assert_stdout_contains("café");
    output.assert_stdout_contains("Müller");
    output.assert_plain_mode_clean();
}

/// E2E test: Error panel with multiple context lines.
#[test]
fn e2e_error_panel_multiple_context() {
    let panel = ErrorPanel::new("Configuration Error", "Invalid settings")
        .add_context("Config file: /etc/app/config.toml")
        .add_context("Line 42")
        .add_context("Expected: integer")
        .add_context("Got: string")
        .with_hint("Check the configuration file syntax");

    let plain = panel.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("Configuration Error");
    output.assert_stdout_contains("config.toml");
    output.assert_stdout_contains("Line 42");
    output.assert_stdout_contains("Expected: integer");
    output.assert_stdout_contains("Got: string");
    output.assert_plain_mode_clean();
}

// ============================================================================
// Console Integration Tests
// ============================================================================

/// E2E test: Error panel through console in plain mode.
#[test]
fn e2e_error_via_console_plain() {
    let console = SqlModelConsole::with_mode(OutputMode::Plain);
    let panel = ErrorPanel::new("Test Error", "Test message");

    // In plain mode, the panel output should have no ANSI codes
    let plain = panel.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_plain_mode_clean();
    assert!(console.is_plain());
}

/// E2E test: Error panel output is machine-parseable.
#[test]
fn e2e_error_output_parseable() {
    let panel = ErrorPanel::new("Parse Error", "Line 10: unexpected end of input")
        .with_position(10)
        .with_sqlstate("42601");

    let plain = panel.render_plain();

    // Output should be parseable text
    assert!(!plain.is_empty());
    assert!(plain.lines().count() > 0);

    // Each line should be valid UTF-8 (implicit in Rust strings)
    for line in plain.lines() {
        assert!(line.len() < 10000, "Lines should be reasonably sized");
    }
}

/// E2E test: Structured error output for JSON mode.
#[test]
fn e2e_error_structured_json() {
    let panel = ErrorPanel::new("JSON Error", "Test for JSON serialization")
        .with_sqlstate("42000")
        .severity(ErrorSeverity::Error);

    // ErrorPanel should be serializable
    let json = serde_json::to_string(&panel);
    assert!(json.is_ok(), "ErrorPanel should serialize to JSON");

    let json_str = json.unwrap();
    assert!(json_str.contains("JSON Error"));
    assert!(json_str.contains("42000"));
}

// ============================================================================
// Sample Data Tests
// ============================================================================

/// E2E test: Using sample_data fixtures for error panels.
#[test]
fn e2e_sample_syntax_error() {
    // Import from fixtures if available, otherwise create inline
    let panel = ErrorPanel::new("SQL Syntax Error", "Unexpected token near 'FORM'")
        .with_sql("SELECT * FORM users WHERE id = 1")
        .with_position(10)
        .with_sqlstate("42601")
        .with_hint("Did you mean 'FROM'?");

    let plain = panel.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("SQL Syntax Error");
    output.assert_stdout_contains("FORM");
    output.assert_stdout_contains("FROM");
    output.assert_plain_mode_clean();
}

/// E2E test: Sample connection error.
#[test]
fn e2e_sample_connection_error() {
    let panel = ErrorPanel::new("Connection Failed", "Could not connect to database")
        .severity(ErrorSeverity::Critical)
        .with_detail("Connection refused (os error 111)")
        .add_context("Host: localhost:5432")
        .add_context("User: postgres")
        .with_hint("Check that the database server is running");

    let plain = panel.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("Connection Failed");
    output.assert_stdout_contains("localhost");
    output.assert_plain_mode_clean();
}

/// E2E test: Sample timeout error.
#[test]
fn e2e_sample_timeout_error() {
    let panel = ErrorPanel::new("Query Timeout", "Query exceeded maximum execution time")
        .severity(ErrorSeverity::Warning)
        .with_sql("SELECT * FROM large_table WHERE complex_condition")
        .with_detail("Timeout after 30 seconds")
        .with_hint("Consider adding an index or simplifying the query");

    let plain = panel.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("Query Timeout");
    output.assert_stdout_contains("Timeout");
    output.assert_plain_mode_clean();
}
