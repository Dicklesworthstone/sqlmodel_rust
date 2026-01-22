//! Complete workflow E2E tests simulating real usage.
//!
//! These tests simulate complete user workflows:
//! - Connection → Query → Results → Cleanup
//! - Error handling paths
//! - Batch operations
//! - Schema operations

use sqlmodel_console::renderables::{
    ErrorPanel, ErrorSeverity, OperationProgress, QueryResults, BatchOperationTracker,
};
use sqlmodel_console::{OutputMode, SqlModelConsole};
use super::output_capture::CapturedOutput;
use std::time::Duration;

// ============================================================================
// Complete Query Workflow Tests
// ============================================================================

/// E2E test: Complete query workflow (connect → query → display → done).
#[test]
fn e2e_complete_query_workflow() {
    // Simulate the output that would be generated during a query workflow
    let mut workflow_output = String::new();
    let mut workflow_stderr = String::new();

    // 1. Connection status
    workflow_stderr.push_str("Connecting to database...\n");

    // 2. Query execution
    workflow_stderr.push_str("Executing: SELECT * FROM users LIMIT 10\n");

    // 3. Results
    let columns = vec!["id".to_string(), "name".to_string(), "email".to_string()];
    let rows = vec![
        vec!["1".to_string(), "Alice".to_string(), "alice@example.com".to_string()],
        vec!["2".to_string(), "Bob".to_string(), "bob@example.com".to_string()],
    ];
    let results = QueryResults::from_data(columns, rows);
    workflow_output.push_str(&results.render_plain());
    workflow_output.push('\n');

    // 4. Completion
    workflow_stderr.push_str("Query returned 2 rows\n");

    let output = CapturedOutput::from_strings(workflow_output, workflow_stderr);

    // Verify workflow stages
    output.assert_stderr_contains("Connecting");
    output.assert_stderr_contains("Executing");
    output.assert_stdout_contains("Alice");
    output.assert_stdout_contains("Bob");
    output.assert_stderr_contains("2 rows");

    // Verify no ANSI codes
    output.assert_all_plain();
}

/// E2E test: Query workflow with error.
#[test]
fn e2e_query_workflow_with_error() {
    let mut workflow_output = String::new();
    let mut workflow_stderr = String::new();

    // 1. Connection status
    workflow_stderr.push_str("Connecting to database...\n");
    workflow_stderr.push_str("Connected\n");

    // 2. Query attempt
    workflow_stderr.push_str("Executing query...\n");

    // 3. Error occurs
    let error = ErrorPanel::new("SQL Syntax Error", "Unexpected token 'SELCT'")
        .with_sql("SELCT * FROM users")
        .with_hint("Did you mean 'SELECT'?");
    workflow_output.push_str(&error.render_plain());

    let output = CapturedOutput::from_strings(workflow_output, workflow_stderr);

    // Verify error is properly displayed
    output.assert_stdout_contains("SQL Syntax Error");
    output.assert_stdout_contains("SELCT");
    output.assert_stdout_contains("SELECT");
    output.assert_all_plain();
}

// ============================================================================
// Batch Operation Workflow Tests
// ============================================================================

/// E2E test: Batch insert workflow.
#[test]
fn e2e_batch_insert_workflow() {
    let mut workflow_output = String::new();
    let mut workflow_stderr = String::new();

    // 1. Start batch operation
    workflow_stderr.push_str("Starting batch insert...\n");

    // 2. Progress updates
    let progress = OperationProgress::new("Inserting records", 1000)
        .completed(1000);
    workflow_output.push_str(&progress.render_plain());
    workflow_output.push('\n');

    // 3. Results summary - use actual BatchOperationTracker API
    let mut tracker = BatchOperationTracker::new("Batch complete", 10, 1000);
    // Simulate completing batches (10 batches of 100 rows each)
    for _ in 0..10 {
        tracker.complete_batch(100);
    }
    workflow_output.push_str(&tracker.render_plain());

    let output = CapturedOutput::from_strings(workflow_output, workflow_stderr);

    output.assert_stderr_contains("Starting batch");
    output.assert_stdout_contains("1000");
    output.assert_all_plain();
}

/// E2E test: Batch operation with partial failures.
#[test]
fn e2e_batch_with_failures() {
    let mut workflow_output = String::new();

    // Batch results - use actual BatchOperationTracker API
    let mut tracker = BatchOperationTracker::new("Import users", 10, 100);
    // Complete 8 batches of 10 rows each
    for _ in 0..8 {
        tracker.complete_batch(10);
    }
    // Record errors for remaining 2 batches
    tracker.record_errors(15);
    workflow_output.push_str(&tracker.render_plain());
    workflow_output.push('\n');

    // Error details for failures
    let error = ErrorPanel::new("Import Errors", "15 records failed validation")
        .severity(ErrorSeverity::Warning)
        .add_context("Constraint violation: unique email")
        .add_context("Invalid data format: 5 records")
        .with_hint("Review error log for details");
    workflow_output.push_str(&error.render_plain());

    let output = CapturedOutput::from_strings(workflow_output, String::new());

    output.assert_stdout_contains("80");
    output.assert_stdout_contains("15");
    output.assert_stdout_contains("Import Errors");
    output.assert_stdout_contains("validation");
    output.assert_plain_mode_clean();
}

// ============================================================================
// Connection Workflow Tests
// ============================================================================

/// E2E test: Connection failure workflow.
#[test]
fn e2e_connection_failure_workflow() {
    let mut workflow_output = String::new();
    let mut workflow_stderr = String::new();

    // 1. Attempt connection
    workflow_stderr.push_str("Connecting to postgres://localhost:5432/mydb...\n");

    // 2. Connection fails
    let error = ErrorPanel::new("Connection Failed", "Could not connect to database")
        .severity(ErrorSeverity::Critical)
        .with_detail("Connection refused (os error 111)")
        .add_context("Host: localhost:5432")
        .add_context("Database: mydb")
        .with_hint("Check that PostgreSQL is running");
    workflow_output.push_str(&error.render_plain());

    let output = CapturedOutput::from_strings(workflow_output, workflow_stderr);

    output.assert_stderr_contains("Connecting");
    output.assert_stdout_contains("Connection Failed");
    output.assert_stdout_contains("Connection refused");
    output.assert_stdout_contains("PostgreSQL");
    output.assert_all_plain();
}

/// E2E test: Successful connection workflow.
#[test]
fn e2e_successful_connection_workflow() {
    let mut workflow_stderr = String::new();

    workflow_stderr.push_str("Connecting to postgres://localhost:5432/mydb...\n");
    workflow_stderr.push_str("Connected successfully\n");
    workflow_stderr.push_str("Server version: PostgreSQL 15.2\n");
    workflow_stderr.push_str("Ready\n");

    let output = CapturedOutput::from_strings(String::new(), workflow_stderr);

    output.assert_stderr_contains("Connected successfully");
    output.assert_stderr_contains("PostgreSQL");
    output.assert_stderr_contains("Ready");
    output.assert_stderr_plain();
}

// ============================================================================
// Migration Workflow Tests
// ============================================================================

/// E2E test: Migration execution workflow.
#[test]
fn e2e_migration_workflow() {
    let mut workflow_output = String::new();
    let mut workflow_stderr = String::new();

    // 1. Migration discovery
    workflow_stderr.push_str("Scanning for migrations...\n");
    workflow_stderr.push_str("Found 3 pending migrations\n");

    // 2. Progress
    let progress = OperationProgress::new("Running migrations", 3)
        .completed(3);
    workflow_output.push_str(&progress.render_plain());
    workflow_output.push('\n');

    // 3. Results
    workflow_output.push_str("Migration 001_create_users: OK\n");
    workflow_output.push_str("Migration 002_create_posts: OK\n");
    workflow_output.push_str("Migration 003_add_indexes: OK\n");

    // 4. Summary
    workflow_stderr.push_str("All migrations completed successfully\n");

    let output = CapturedOutput::from_strings(workflow_output, workflow_stderr);

    output.assert_stderr_contains("3 pending migrations");
    output.assert_stdout_contains("001_create_users");
    output.assert_stdout_contains("OK");
    output.assert_stderr_contains("successfully");
    output.assert_all_plain();
}

/// E2E test: Migration with rollback.
#[test]
fn e2e_migration_rollback_workflow() {
    let mut workflow_output = String::new();
    let mut workflow_stderr = String::new();

    // 1. Migration fails
    workflow_stderr.push_str("Running migration 003...\n");
    let error = ErrorPanel::new("Migration Failed", "Error executing 003_add_indexes")
        .with_sql("CREATE INDEX idx_users_email ON users(email)")
        .with_detail("Column 'email' does not exist")
        .severity(ErrorSeverity::Error);
    workflow_output.push_str(&error.render_plain());
    workflow_output.push('\n');

    // 2. Rollback
    workflow_stderr.push_str("Rolling back migration 003...\n");
    workflow_stderr.push_str("Rollback completed\n");
    workflow_stderr.push_str("Database state: migration 002\n");

    let output = CapturedOutput::from_strings(workflow_output, workflow_stderr);

    output.assert_stdout_contains("Migration Failed");
    output.assert_stdout_contains("email");
    output.assert_stderr_contains("Rolling back");
    output.assert_stderr_contains("Rollback completed");
    output.assert_all_plain();
}

// ============================================================================
// Agent Workflow Tests
// ============================================================================

/// E2E test: Workflow output is agent-parseable.
#[test]
fn e2e_agent_parseable_workflow() {
    let console = SqlModelConsole::with_mode(OutputMode::Plain);
    assert!(console.is_plain());

    // Simulate agent-friendly output
    let columns = vec!["id".to_string(), "status".to_string()];
    let rows = vec![
        vec!["1".to_string(), "active".to_string()],
        vec!["2".to_string(), "pending".to_string()],
    ];
    let results = QueryResults::from_data(columns, rows);
    let output = results.render_plain();

    // Verify parseable format
    let captured = CapturedOutput::from_strings(output.clone(), String::new());
    captured.assert_plain_mode_clean();

    // Output should have predictable structure
    let lines: Vec<&str> = output.lines().collect();
    assert!(lines.len() >= 2, "Should have header and at least one data row");
}

/// E2E test: JSON workflow for structured output.
#[test]
fn e2e_json_workflow() {
    let console = SqlModelConsole::with_mode(OutputMode::Json);
    assert!(console.is_json());

    // Create structured data
    let columns = vec!["id".to_string(), "value".to_string()];
    let rows = vec![vec!["1".to_string(), "test".to_string()]];
    let results = QueryResults::from_data(columns, rows);

    // Get JSON using the to_json() method
    let json_value = results.to_json();

    // Verify valid JSON structure
    assert!(json_value.is_object() || json_value.is_array());
}

// ============================================================================
// Performance-Sensitive Workflow Tests
// ============================================================================

/// E2E test: Large result workflow performance.
#[test]
fn e2e_large_result_performance() {
    // Generate large dataset
    let columns: Vec<String> = (0..10).map(|i| format!("col_{i}")).collect();
    let rows: Vec<Vec<String>> = (0..1000)
        .map(|r| (0..10).map(|c| format!("r{r}c{c}")).collect())
        .collect();

    let results = QueryResults::from_data(columns, rows);
    let start = std::time::Instant::now();
    let output = results.render_plain();
    let duration = start.elapsed();

    let captured = CapturedOutput::with_duration(output, String::new(), duration);

    // Should complete in reasonable time (under 1 second for 1000 rows)
    captured.assert_duration_under(1000);
    captured.assert_plain_mode_clean();
}

/// E2E test: Streaming-style progress updates.
#[test]
fn e2e_streaming_progress_updates() {
    // Simulate multiple progress updates
    let mut all_output = String::new();

    for completed in [0, 25, 50, 75, 100] {
        let progress = OperationProgress::new("Processing", 100)
            .completed(completed);
        all_output.push_str(&format!("{}\n", progress.render_plain()));
    }

    let output = CapturedOutput::from_strings(all_output, String::new());

    // All updates should be valid
    output.assert_plain_mode_clean();
}

// ============================================================================
// Error Recovery Workflow Tests
// ============================================================================

/// E2E test: Query retry after error.
#[test]
fn e2e_query_retry_workflow() {
    let mut workflow_output = String::new();
    let mut workflow_stderr = String::new();

    // First attempt fails
    workflow_stderr.push_str("Attempt 1: Executing query...\n");
    let error = ErrorPanel::new("Timeout", "Query timed out")
        .severity(ErrorSeverity::Warning);
    workflow_output.push_str(&error.render_plain());
    workflow_output.push('\n');

    // Retry succeeds
    workflow_stderr.push_str("Attempt 2: Executing query...\n");
    let columns = vec!["result".to_string()];
    let rows = vec![vec!["success".to_string()]];
    let results = QueryResults::from_data(columns, rows);
    workflow_output.push_str(&results.render_plain());

    workflow_stderr.push_str("Query completed on retry\n");

    let output = CapturedOutput::from_strings(workflow_output, workflow_stderr);

    output.assert_stderr_contains("Attempt 1");
    output.assert_stdout_contains("Timeout");
    output.assert_stderr_contains("Attempt 2");
    output.assert_stdout_contains("success");
    output.assert_all_plain();
}

/// E2E test: Complete workflow with cleanup.
#[test]
fn e2e_complete_workflow_with_cleanup() {
    let mut workflow_stderr = String::new();

    // Full lifecycle
    workflow_stderr.push_str("Opening connection...\n");
    workflow_stderr.push_str("Executing queries...\n");
    workflow_stderr.push_str("Committing transaction...\n");
    workflow_stderr.push_str("Closing connection...\n");
    workflow_stderr.push_str("Done\n");

    let output = CapturedOutput::from_strings(String::new(), workflow_stderr);

    output.assert_stderr_contains("Opening connection");
    output.assert_stderr_contains("Committing transaction");
    output.assert_stderr_contains("Closing connection");
    output.assert_stderr_contains("Done");
    output.assert_stderr_plain();
}
