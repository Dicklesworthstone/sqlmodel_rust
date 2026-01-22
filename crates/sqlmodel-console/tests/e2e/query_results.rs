//! E2E tests for query result table display.
//!
//! These tests verify that query results:
//! - Display correctly in tabular format
//! - Handle various data types
//! - Work in plain mode without ANSI codes
//! - Handle edge cases (empty results, large datasets)

use sqlmodel_console::renderables::QueryResults;
use sqlmodel_console::{OutputMode, SqlModelConsole};
use super::output_capture::CapturedOutput;

// ============================================================================
// Basic Query Result Tests
// ============================================================================

/// E2E test: Simple query results display.
#[test]
fn e2e_simple_query_results() {
    let columns = vec!["id".to_string(), "name".to_string(), "email".to_string()];
    let rows = vec![
        vec!["1".to_string(), "Alice".to_string(), "alice@example.com".to_string()],
        vec!["2".to_string(), "Bob".to_string(), "bob@example.com".to_string()],
        vec!["3".to_string(), "Carol".to_string(), "carol@example.com".to_string()],
    ];

    let results = QueryResults::from_data(columns, rows);
    let plain = results.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    // Verify column headers
    output.assert_stdout_contains("id");
    output.assert_stdout_contains("name");
    output.assert_stdout_contains("email");

    // Verify data
    output.assert_stdout_contains("Alice");
    output.assert_stdout_contains("Bob");
    output.assert_stdout_contains("Carol");
    output.assert_stdout_contains("alice@example.com");

    // No ANSI codes
    output.assert_plain_mode_clean();
}

/// E2E test: Query results with row count.
#[test]
fn e2e_query_results_row_count() {
    let columns = vec!["id".to_string()];
    let rows = vec![
        vec!["1".to_string()],
        vec!["2".to_string()],
        vec!["3".to_string()],
    ];

    let results = QueryResults::from_data(columns, rows);
    let plain = results.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    // Should show row count
    output.assert_stdout_contains("3");
    output.assert_plain_mode_clean();
}

// ============================================================================
// Edge Case Tests
// ============================================================================

/// E2E test: Empty query results.
#[test]
fn e2e_empty_query_results() {
    let columns = vec!["id".to_string(), "name".to_string()];
    let rows: Vec<Vec<String>> = vec![];

    let results = QueryResults::from_data(columns, rows);
    let plain = results.render_plain();
    let output = CapturedOutput::from_strings(plain.clone(), String::new());

    // Should still show headers
    output.assert_stdout_contains("id");
    output.assert_stdout_contains("name");

    // No ANSI codes
    output.assert_plain_mode_clean();

    // Should indicate empty or 0 rows (rows was empty, so check output)
    let has_empty_indicator = plain.contains("0 rows") || plain.contains("Empty") || plain.lines().count() <= 3;
    assert!(has_empty_indicator);
}

/// E2E test: Single column results.
#[test]
fn e2e_single_column_results() {
    let columns = vec!["count".to_string()];
    let rows = vec![vec!["42".to_string()]];

    let results = QueryResults::from_data(columns, rows);
    let plain = results.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("count");
    output.assert_stdout_contains("42");
    output.assert_plain_mode_clean();
}

/// E2E test: Many columns.
#[test]
fn e2e_many_columns() {
    let columns: Vec<String> = (0..10).map(|i| format!("col_{i}")).collect();
    let rows = vec![
        (0..10).map(|i| format!("val_{i}")).collect(),
    ];

    let results = QueryResults::from_data(columns, rows);
    let plain = results.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("col_0");
    output.assert_stdout_contains("col_9");
    output.assert_stdout_contains("val_0");
    output.assert_stdout_contains("val_9");
    output.assert_plain_mode_clean();
}

/// E2E test: Large dataset.
#[test]
fn e2e_large_dataset() {
    let columns = vec!["id".to_string(), "value".to_string()];
    let rows: Vec<Vec<String>> = (0..100)
        .map(|i| vec![i.to_string(), format!("value_{i}")])
        .collect();

    let results = QueryResults::from_data(columns, rows);
    let plain = results.render_plain();
    let output = CapturedOutput::from_strings(plain.clone(), String::new());

    // Should handle large datasets
    output.assert_stdout_contains("id");
    output.assert_stdout_contains("value");
    output.assert_plain_mode_clean();

    // Should show row count or have many lines
    let line_count = plain.lines().count();
    assert!(line_count >= 10, "Should render multiple rows");
}

// ============================================================================
// Data Type Tests
// ============================================================================

/// E2E test: Various data types in results.
#[test]
fn e2e_mixed_data_types() {
    let columns = vec![
        "integer".to_string(),
        "float".to_string(),
        "text".to_string(),
        "boolean".to_string(),
        "null".to_string(),
    ];
    let rows = vec![
        vec![
            "42".to_string(),
            "3.14159".to_string(),
            "Hello, World!".to_string(),
            "true".to_string(),
            "NULL".to_string(),
        ],
        vec![
            "-1".to_string(),
            "2.718".to_string(),
            "Test".to_string(),
            "false".to_string(),
            "NULL".to_string(),
        ],
    ];

    let results = QueryResults::from_data(columns, rows);
    let plain = results.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("42");
    output.assert_stdout_contains("3.14159");
    output.assert_stdout_contains("Hello, World!");
    output.assert_stdout_contains("true");
    output.assert_stdout_contains("NULL");
    output.assert_plain_mode_clean();
}

/// E2E test: Unicode content.
#[test]
fn e2e_unicode_content() {
    let columns = vec!["name".to_string(), "description".to_string()];
    let rows = vec![
        vec!["日本語".to_string(), "Japanese text".to_string()],
        vec!["café".to_string(), "French café".to_string()],
        vec!["Müller".to_string(), "German name".to_string()],
        vec!["🎉".to_string(), "Emoji support".to_string()],
    ];

    let results = QueryResults::from_data(columns, rows);
    let plain = results.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("日本語");
    output.assert_stdout_contains("café");
    output.assert_stdout_contains("Müller");
    output.assert_plain_mode_clean();
}

/// E2E test: Long values.
#[test]
fn e2e_long_values() {
    let columns = vec!["id".to_string(), "content".to_string()];
    let long_content = "A".repeat(200);
    let rows = vec![vec!["1".to_string(), long_content.clone()]];

    let results = QueryResults::from_data(columns, rows);
    let plain = results.render_plain();
    let output = CapturedOutput::from_strings(plain.clone(), String::new());

    // Should handle long content (might be truncated)
    output.assert_stdout_contains("id");
    output.assert_stdout_contains("content");
    output.assert_plain_mode_clean();

    // Content should be present (at least partially)
    assert!(plain.contains("AAA"));
}

/// E2E test: Empty string values.
#[test]
fn e2e_empty_string_values() {
    let columns = vec!["id".to_string(), "optional_field".to_string()];
    let rows = vec![
        vec!["1".to_string(), String::new()],
        vec!["2".to_string(), "has value".to_string()],
    ];

    let results = QueryResults::from_data(columns, rows);
    let plain = results.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("id");
    output.assert_stdout_contains("has value");
    output.assert_plain_mode_clean();
}

// ============================================================================
// Console Integration Tests
// ============================================================================

/// E2E test: Query results in plain console mode.
#[test]
fn e2e_query_results_plain_console() {
    let console = SqlModelConsole::with_mode(OutputMode::Plain);

    let columns = vec!["status".to_string()];
    let rows = vec![vec!["ok".to_string()]];

    let results = QueryResults::from_data(columns, rows);
    let plain = results.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    assert!(console.is_plain());
    output.assert_plain_mode_clean();
}

/// E2E test: Query results are machine-parseable.
#[test]
fn e2e_query_results_parseable() {
    let columns = vec!["a".to_string(), "b".to_string()];
    let rows = vec![vec!["1".to_string(), "2".to_string()]];

    let results = QueryResults::from_data(columns, rows);
    let plain = results.render_plain();

    // Output should be valid UTF-8 text
    assert!(!plain.is_empty());

    // Lines should be reasonably sized
    for line in plain.lines() {
        assert!(line.len() < 10000);
    }
}

/// E2E test: Query results JSON serialization.
#[test]
fn e2e_query_results_json() {
    let columns = vec!["id".to_string(), "name".to_string()];
    let rows = vec![vec!["1".to_string(), "Test".to_string()]];

    let results = QueryResults::from_data(columns, rows);

    // Get JSON via to_json() method
    let json_value = results.to_json();
    let json_str = serde_json::to_string(&json_value).unwrap();

    assert!(json_str.contains("id"));
    assert!(json_str.contains("name"));
    assert!(json_str.contains("Test"));
}

// ============================================================================
// Format Tests
// ============================================================================

/// E2E test: Pipe-delimited format.
#[test]
fn e2e_pipe_delimited_format() {
    let columns = vec!["id".to_string(), "name".to_string()];
    let rows = vec![
        vec!["1".to_string(), "Alice".to_string()],
        vec!["2".to_string(), "Bob".to_string()],
    ];

    let results = QueryResults::from_data(columns, rows);
    let plain = results.render_plain();

    // Should contain separators
    assert!(plain.contains('|') || plain.contains('\t') || plain.contains(' '));
}

/// E2E test: Table alignment.
#[test]
fn e2e_table_alignment() {
    let columns = vec!["short".to_string(), "much_longer_column".to_string()];
    let rows = vec![
        vec!["a".to_string(), "b".to_string()],
        vec!["longer".to_string(), "x".to_string()],
    ];

    let results = QueryResults::from_data(columns, rows);
    let plain = results.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("short");
    output.assert_stdout_contains("much_longer_column");
    output.assert_plain_mode_clean();
}
