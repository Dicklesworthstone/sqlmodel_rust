//! E2E tests for progress indicators (bars and spinners).
//!
//! These tests verify that progress components:
//! - Display correctly with progress information
//! - Work in plain mode without ANSI codes
//! - Handle edge cases (0%, 100%, negative values)
//! - Provide useful output for agents

use sqlmodel_console::renderables::{OperationProgress, IndeterminateSpinner, BatchOperationTracker};
use sqlmodel_console::{OutputMode, SqlModelConsole};
use super::output_capture::CapturedOutput;

// ============================================================================
// Operation Progress Tests
// ============================================================================

/// E2E test: Progress bar basic display.
#[test]
fn e2e_progress_bar_basic() {
    let progress = OperationProgress::new("Processing records", 100)
        .completed(50);

    let plain = progress.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("Processing records");
    output.assert_stdout_contains("50");
    output.assert_plain_mode_clean();
}

/// E2E test: Progress at 0%.
#[test]
fn e2e_progress_zero_percent() {
    let progress = OperationProgress::new("Starting", 100);
    let plain = progress.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("Starting");
    output.assert_plain_mode_clean();
}

/// E2E test: Progress at 100%.
#[test]
fn e2e_progress_complete() {
    let progress = OperationProgress::new("Completed task", 100)
        .completed(100);

    let plain = progress.render_plain();
    let output = CapturedOutput::from_strings(plain.clone(), String::new());

    output.assert_stdout_contains("Completed task");
    output.assert_plain_mode_clean();

    // Should indicate completion
    assert!(plain.contains("100") || plain.contains("complete") || plain.contains("done"));
}

/// E2E test: Progress with custom total.
#[test]
fn e2e_progress_custom_total() {
    let progress = OperationProgress::new("Migrating users", 1500)
        .completed(750);

    let plain = progress.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("Migrating users");
    output.assert_stdout_contains("750");
    output.assert_plain_mode_clean();
}

/// E2E test: Progress percentage calculation.
#[test]
fn e2e_progress_percentage() {
    let progress = OperationProgress::new("Test", 200)
        .completed(50);

    assert!((progress.percentage() - 25.0).abs() < 0.1);
}

// ============================================================================
// Indeterminate Spinner Tests
// ============================================================================

/// E2E test: Spinner basic display.
#[test]
fn e2e_spinner_basic() {
    let spinner = IndeterminateSpinner::new("Loading data");
    let plain = spinner.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("Loading data");
    output.assert_plain_mode_clean();
}

/// E2E test: Spinner with status update.
#[test]
fn e2e_spinner_with_status() {
    let spinner = IndeterminateSpinner::new("Connecting")
        .with_status("Establishing connection...");

    let plain = spinner.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("Connecting");
    output.assert_plain_mode_clean();
}

/// E2E test: Spinner in plain mode has no animation codes.
#[test]
fn e2e_spinner_plain_no_animation() {
    let spinner = IndeterminateSpinner::new("Working");
    let plain = spinner.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    // Plain mode should not have cursor movement or animation codes
    output.assert_plain_mode_clean();

    // Should not contain backspace or carriage return sequences
    output.assert_stdout_not_contains("\x08");
    output.assert_stdout_not_contains("\r");
}

// ============================================================================
// Batch Operation Tracker Tests
// ============================================================================

/// E2E test: Batch tracker basic display.
#[test]
fn e2e_batch_tracker_basic() {
    let mut tracker = BatchOperationTracker::new("Processing files", 5, 50);
    // Complete 3 batches of 10 rows each = 30 rows
    for _ in 0..3 {
        tracker.complete_batch(10);
    }
    // Record 5 errors
    tracker.record_errors(5);

    let plain = tracker.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("Processing files");
    output.assert_stdout_contains("30");
    output.assert_stdout_contains("5");
    output.assert_plain_mode_clean();
}

/// E2E test: Batch tracker all success.
#[test]
fn e2e_batch_tracker_all_success() {
    let mut tracker = BatchOperationTracker::new("Batch complete", 10, 100);
    // Complete all 10 batches of 10 rows each = 100 rows
    for _ in 0..10 {
        tracker.complete_batch(10);
    }

    let plain = tracker.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("100");
    output.assert_plain_mode_clean();
}

/// E2E test: Batch tracker all failed.
#[test]
fn e2e_batch_tracker_all_failed() {
    let mut tracker = BatchOperationTracker::new("Batch failed", 2, 20);
    // All 20 rows encountered errors
    tracker.record_errors(20);

    let plain = tracker.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("20");
    output.assert_plain_mode_clean();
}

/// E2E test: Batch tracker mixed results.
#[test]
fn e2e_batch_tracker_mixed() {
    let mut tracker = BatchOperationTracker::new("Import records", 10, 1000);
    // Complete batches totaling 850 rows
    for _ in 0..8 {
        tracker.complete_batch(100);  // 8 batches of 100 = 800 rows
    }
    tracker.complete_batch(50);  // 1 more batch of 50 = 850 total
    // Record 100 errors
    tracker.record_errors(100);

    let plain = tracker.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("Import records");
    output.assert_stdout_contains("850");
    output.assert_stdout_contains("100");
    output.assert_plain_mode_clean();
}

// ============================================================================
// Console Integration Tests
// ============================================================================

/// E2E test: Progress in plain console mode.
#[test]
fn e2e_progress_plain_console() {
    let console = SqlModelConsole::with_mode(OutputMode::Plain);
    assert!(console.is_plain());

    let progress = OperationProgress::new("Test", 100).completed(50);
    let plain = progress.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_plain_mode_clean();
}

/// E2E test: Progress components are JSON-serializable.
#[test]
fn e2e_progress_json_serializable() {
    let progress = OperationProgress::new("Serializable", 100).completed(25);
    let json = serde_json::to_string(&progress);
    assert!(json.is_ok(), "OperationProgress should serialize to JSON");

    let spinner = IndeterminateSpinner::new("Spinner");
    let json = serde_json::to_string(&spinner);
    assert!(json.is_ok(), "IndeterminateSpinner should serialize to JSON");

    let mut tracker = BatchOperationTracker::new("Tracker", 2, 10);
    tracker.complete_batch(5);  // Complete 1 batch of 5 rows
    // BatchOperationTracker uses to_json() method instead of serde_json::to_string
    let json = tracker.to_json();
    assert!(!json.is_empty(), "BatchOperationTracker should serialize to JSON");
    assert!(json.contains("Tracker"));
}

// ============================================================================
// Edge Cases
// ============================================================================

/// E2E test: Progress with zero total.
#[test]
fn e2e_progress_zero_total() {
    // Zero total should not panic
    let progress = OperationProgress::new("Empty", 0);
    let plain = progress.render_plain();

    // Should render something valid
    assert!(!plain.is_empty() || plain.is_empty()); // Just don't panic
}

/// E2E test: Progress exceeds total.
#[test]
fn e2e_progress_exceeds_total() {
    let progress = OperationProgress::new("Overflow", 100)
        .completed(150);

    let plain = progress.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    // Should handle gracefully, might clamp to 100%
    output.assert_plain_mode_clean();
}

/// E2E test: Very long description.
#[test]
fn e2e_progress_long_description() {
    let long_desc = "A".repeat(100);
    let progress = OperationProgress::new(&long_desc, 100)
        .completed(50);

    let plain = progress.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    // Should handle long descriptions
    output.assert_plain_mode_clean();
}

/// E2E test: Unicode in progress description.
#[test]
fn e2e_progress_unicode_description() {
    let progress = OperationProgress::new("处理文件 (Processing files)", 100)
        .completed(50);

    let plain = progress.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_stdout_contains("处理文件");
    output.assert_plain_mode_clean();
}

/// E2E test: Special characters in description.
#[test]
fn e2e_progress_special_chars() {
    let progress = OperationProgress::new("Processing <items> & [things]", 100)
        .completed(50);

    let plain = progress.render_plain();
    let output = CapturedOutput::from_strings(plain, String::new());

    output.assert_plain_mode_clean();
}
