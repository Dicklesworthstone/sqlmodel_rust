//! Batch operation tracker for bulk database operations.
//!
//! `BatchOperationTracker` provides specialized tracking for batch inserts,
//! updates, and migrations with batch-level progress, row counts, error
//! tracking, and smoothed rate calculation.
//!
//! # Example
//!
//! ```rust
//! use sqlmodel_console::renderables::BatchOperationTracker;
//!
//! let mut tracker = BatchOperationTracker::new("Batch insert", 20, 10000);
//!
//! // Complete a batch of 500 rows
//! tracker.complete_batch(500);
//!
//! // Plain text: "Batch insert: 5% (1/20 batches), 500/10000 rows, 0 errors"
//! println!("{}", tracker.render_plain());
//! ```

use std::time::{Duration, Instant};

use serde::Serialize;

use crate::theme::Theme;

/// State for batch tracker styling.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BatchState {
    /// Normal progress (default)
    #[default]
    Normal,
    /// Completed successfully
    Complete,
    /// Has errors but below threshold
    Warning,
    /// Errors exceed threshold
    Error,
}

/// A tracker for bulk database operations with batch-level progress.
///
/// Tracks:
/// - Batch-level progress (batch X of Y)
/// - Row-level progress (rows processed / total)
/// - Error counting with configurable threshold
/// - Smoothed rate calculation using recent batch times
///
/// # Rendering Modes
///
/// - **Rich mode**: Two-line display with progress bar, row count, rate, errors
/// - **Plain mode**: Single line for agents
/// - **JSON mode**: Structured data for programmatic consumption
///
/// # Example
///
/// ```rust
/// use sqlmodel_console::renderables::BatchOperationTracker;
///
/// let mut tracker = BatchOperationTracker::new("Migrating users", 10, 1000);
/// tracker.complete_batch(100);
/// tracker.complete_batch(100);
///
/// assert_eq!(tracker.completed_batches(), 2);
/// assert_eq!(tracker.processed_rows(), 200);
/// ```
#[derive(Debug, Clone)]
pub struct BatchOperationTracker {
    /// Name of the operation being tracked
    operation_name: String,
    /// Total number of batches
    total_batches: u64,
    /// Number of batches completed
    completed_batches: u64,
    /// Total number of rows expected
    total_rows: u64,
    /// Number of rows processed so far
    processed_rows: u64,
    /// Number of errors encountered
    error_count: u64,
    /// Error threshold for warning state
    error_threshold: u64,
    /// When the operation started
    started_at: Instant,
    /// Recent batch durations for rate smoothing
    batch_times: Vec<Duration>,
    /// Rows processed in recent batches (parallel to batch_times)
    batch_rows: Vec<u64>,
    /// Maximum number of batches to track for smoothing
    smoothing_window: usize,
    /// Last batch start time
    last_batch_start: Instant,
    /// Current state for styling
    state: BatchState,
    /// Optional theme for styling
    theme: Option<Theme>,
    /// Optional fixed width for rendering
    width: Option<usize>,
}

impl BatchOperationTracker {
    /// Create a new batch operation tracker.
    ///
    /// # Arguments
    /// - `operation_name`: Human-readable name for the operation
    /// - `total_batches`: Total number of batches to process
    /// - `total_rows`: Total number of rows expected across all batches
    #[must_use]
    pub fn new(operation_name: impl Into<String>, total_batches: u64, total_rows: u64) -> Self {
        let now = Instant::now();
        Self {
            operation_name: operation_name.into(),
            total_batches,
            completed_batches: 0,
            total_rows,
            processed_rows: 0,
            error_count: 0,
            error_threshold: 10,
            started_at: now,
            batch_times: Vec::with_capacity(10),
            batch_rows: Vec::with_capacity(10),
            smoothing_window: 5,
            last_batch_start: now,
            state: BatchState::Normal,
            theme: None,
            width: None,
        }
    }

    /// Set the theme for styled output.
    #[must_use]
    pub fn theme(mut self, theme: Theme) -> Self {
        self.theme = Some(theme);
        self
    }

    /// Set the error threshold for warning state.
    ///
    /// When error_count exceeds this threshold, the tracker shows error styling.
    #[must_use]
    pub fn error_threshold(mut self, threshold: u64) -> Self {
        self.error_threshold = threshold;
        self
    }

    /// Set the rendering width.
    #[must_use]
    pub fn width(mut self, width: usize) -> Self {
        self.width = Some(width);
        self
    }

    /// Set the smoothing window size for rate calculation.
    #[must_use]
    pub fn smoothing_window(mut self, size: usize) -> Self {
        self.smoothing_window = size.max(1);
        self
    }

    /// Record completion of a batch.
    ///
    /// # Arguments
    /// - `rows_in_batch`: Number of rows processed in this batch
    pub fn complete_batch(&mut self, rows_in_batch: u64) {
        let now = Instant::now();
        let duration = now.duration_since(self.last_batch_start);

        // Track batch time and rows for rate smoothing
        self.batch_times.push(duration);
        self.batch_rows.push(rows_in_batch);

        // Keep only recent batches for smoothing
        while self.batch_times.len() > self.smoothing_window {
            self.batch_times.remove(0);
            self.batch_rows.remove(0);
        }

        self.completed_batches += 1;
        self.processed_rows += rows_in_batch;
        self.last_batch_start = now;

        self.update_state();
    }

    /// Record an error.
    pub fn record_error(&mut self) {
        self.error_count += 1;
        self.update_state();
    }

    /// Record multiple errors.
    pub fn record_errors(&mut self, count: u64) {
        self.error_count += count;
        self.update_state();
    }

    /// Get the operation name.
    #[must_use]
    pub fn operation_name(&self) -> &str {
        &self.operation_name
    }

    /// Get the number of completed batches.
    #[must_use]
    pub fn completed_batches(&self) -> u64 {
        self.completed_batches
    }

    /// Get the total number of batches.
    #[must_use]
    pub fn total_batches(&self) -> u64 {
        self.total_batches
    }

    /// Get the number of processed rows.
    #[must_use]
    pub fn processed_rows(&self) -> u64 {
        self.processed_rows
    }

    /// Get the total number of rows.
    #[must_use]
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Get the error count.
    #[must_use]
    pub fn error_count(&self) -> u64 {
        self.error_count
    }

    /// Get the current state.
    #[must_use]
    pub fn current_state(&self) -> BatchState {
        self.state
    }

    /// Check if the operation is complete.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.completed_batches >= self.total_batches
    }

    /// Calculate the batch completion percentage.
    #[must_use]
    pub fn batch_percentage(&self) -> f64 {
        if self.total_batches == 0 {
            return 100.0;
        }
        (self.completed_batches as f64 / self.total_batches as f64) * 100.0
    }

    /// Calculate the row completion percentage.
    #[must_use]
    pub fn row_percentage(&self) -> f64 {
        if self.total_rows == 0 {
            return 100.0;
        }
        (self.processed_rows as f64 / self.total_rows as f64) * 100.0
    }

    /// Calculate the elapsed time in seconds.
    #[must_use]
    pub fn elapsed_secs(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }

    /// Calculate the smoothed throughput (rows per second).
    ///
    /// Uses recent batch times for a more stable rate.
    #[must_use]
    pub fn throughput(&self) -> f64 {
        if self.batch_times.is_empty() {
            // Fall back to overall rate
            let elapsed = self.elapsed_secs();
            if elapsed < 0.001 {
                return 0.0;
            }
            return self.processed_rows as f64 / elapsed;
        }

        let total_duration: Duration = self.batch_times.iter().sum();
        let total_rows: u64 = self.batch_rows.iter().sum();

        let secs = total_duration.as_secs_f64();
        if secs < 0.001 {
            return 0.0;
        }

        total_rows as f64 / secs
    }

    /// Calculate the success rate percentage.
    #[must_use]
    pub fn success_rate(&self) -> f64 {
        let total = self.processed_rows + self.error_count;
        if total == 0 {
            return 100.0;
        }
        (self.processed_rows as f64 / total as f64) * 100.0
    }

    /// Update state based on current progress and errors.
    fn update_state(&mut self) {
        if self.completed_batches >= self.total_batches {
            self.state = BatchState::Complete;
        } else if self.error_count > self.error_threshold {
            self.state = BatchState::Error;
        } else if self.error_count > 0 {
            self.state = BatchState::Warning;
        } else {
            self.state = BatchState::Normal;
        }
    }

    /// Render as plain text for agents.
    ///
    /// Format: `Name: 50% (10/20 batches), 5000/10000 rows, 523 rows/s, 0 errors`
    #[must_use]
    pub fn render_plain(&self) -> String {
        let pct = self.batch_percentage();
        let rate = self.throughput();

        let mut parts = vec![format!(
            "{}: {:.0}% ({}/{} batches), {}/{} rows",
            self.operation_name,
            pct,
            self.completed_batches,
            self.total_batches,
            self.processed_rows,
            self.total_rows
        )];

        if self.processed_rows > 0 {
            parts.push(format!("{rate:.0} rows/s"));
        }

        parts.push(format!("{} errors", self.error_count));

        parts.join(", ")
    }

    /// Render with ANSI styling.
    ///
    /// Two-line display with progress bar, row count, rate, and errors.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // bar_width is bounded
    pub fn render_styled(&self) -> String {
        let bar_width = self.width.unwrap_or(30);
        let pct = self.batch_percentage();
        let filled = ((pct / 100.0) * bar_width as f64).round() as usize;
        let empty = bar_width.saturating_sub(filled);

        let theme = self.theme.clone().unwrap_or_default();

        let (bar_color, text_color) = match self.state {
            BatchState::Normal => (theme.info.color_code(), theme.info.color_code()),
            BatchState::Complete => (theme.success.color_code(), theme.success.color_code()),
            BatchState::Warning => (theme.warning.color_code(), theme.warning.color_code()),
            BatchState::Error => (theme.error.color_code(), theme.error.color_code()),
        };
        let reset = "\x1b[0m";

        // Line 1: Progress bar
        let bar = format!(
            "{bar_color}[{filled}{empty}]{reset}",
            filled = "=".repeat(filled.saturating_sub(1)) + if filled > 0 { ">" } else { "" },
            empty = " ".repeat(empty),
        );

        let line1 = format!(
            "{text_color}{}{reset} {bar} {pct:.0}% ({}/{} batches)",
            self.operation_name, self.completed_batches, self.total_batches
        );

        // Line 2: Row stats
        let rate = self.throughput();
        let error_str = if self.error_count == 0 {
            format!("{}{} errors{reset}", theme.success.color_code(), self.error_count)
        } else if self.error_count > self.error_threshold {
            format!(
                "{}{} errors (threshold exceeded!){reset}",
                theme.error.color_code(),
                self.error_count
            )
        } else {
            format!(
                "{}{} errors{reset}",
                theme.warning.color_code(),
                self.error_count
            )
        };

        let line2 = format!(
            "  Rows: {}/{} | Rate: {:.0} rows/s | {}",
            self.processed_rows, self.total_rows, rate, error_str
        );

        format!("{line1}\n{line2}")
    }

    /// Render a completion summary.
    ///
    /// Shows total time, rows, average rate, error count, and success rate.
    #[must_use]
    pub fn render_summary(&self) -> String {
        let elapsed = self.elapsed_secs();
        let avg_rate = if elapsed > 0.001 {
            self.processed_rows as f64 / elapsed
        } else {
            0.0
        };

        format!(
            "Summary for '{}':\n\
             - Total time: {}\n\
             - Total rows: {}\n\
             - Average rate: {:.0} rows/s\n\
             - Errors: {}\n\
             - Success rate: {:.1}%",
            self.operation_name,
            format_duration(elapsed),
            self.processed_rows,
            avg_rate,
            self.error_count,
            self.success_rate()
        )
    }

    /// Render as JSON for structured output.
    #[must_use]
    pub fn to_json(&self) -> String {
        #[derive(Serialize)]
        struct BatchJson<'a> {
            operation: &'a str,
            completed_batches: u64,
            total_batches: u64,
            processed_rows: u64,
            total_rows: u64,
            batch_percentage: f64,
            row_percentage: f64,
            throughput: f64,
            error_count: u64,
            error_threshold: u64,
            elapsed_secs: f64,
            is_complete: bool,
            success_rate: f64,
            state: &'a str,
        }

        let state_str = match self.state {
            BatchState::Normal => "normal",
            BatchState::Complete => "complete",
            BatchState::Warning => "warning",
            BatchState::Error => "error",
        };

        let json = BatchJson {
            operation: &self.operation_name,
            completed_batches: self.completed_batches,
            total_batches: self.total_batches,
            processed_rows: self.processed_rows,
            total_rows: self.total_rows,
            batch_percentage: self.batch_percentage(),
            row_percentage: self.row_percentage(),
            throughput: self.throughput(),
            error_count: self.error_count,
            error_threshold: self.error_threshold,
            elapsed_secs: self.elapsed_secs(),
            is_complete: self.is_complete(),
            success_rate: self.success_rate(),
            state: state_str,
        };

        serde_json::to_string(&json).unwrap_or_else(|_| "{}".to_string())
    }
}

/// Format a duration in seconds to human-readable form.
fn format_duration(secs: f64) -> String {
    if secs < 1.0 {
        return format!("{:.0}ms", secs * 1000.0);
    }
    if secs < 60.0 {
        return format!("{secs:.1}s");
    }
    if secs < 3600.0 {
        let mins = (secs / 60.0).floor();
        let remaining = secs % 60.0;
        return format!("{mins:.0}m{remaining:.0}s");
    }
    let hours = (secs / 3600.0).floor();
    let remaining_mins = ((secs % 3600.0) / 60.0).floor();
    format!("{hours:.0}h{remaining_mins:.0}m")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_tracker_creation() {
        let tracker = BatchOperationTracker::new("Test", 10, 1000);
        assert_eq!(tracker.operation_name(), "Test");
        assert_eq!(tracker.total_batches(), 10);
        assert_eq!(tracker.total_rows(), 1000);
        assert_eq!(tracker.completed_batches(), 0);
        assert_eq!(tracker.processed_rows(), 0);
        assert_eq!(tracker.error_count(), 0);
        assert_eq!(tracker.current_state(), BatchState::Normal);
    }

    #[test]
    fn test_batch_complete() {
        let mut tracker = BatchOperationTracker::new("Test", 10, 1000);
        assert_eq!(tracker.completed_batches(), 0);

        tracker.complete_batch(100);
        assert_eq!(tracker.completed_batches(), 1);

        tracker.complete_batch(100);
        assert_eq!(tracker.completed_batches(), 2);
    }

    #[test]
    fn test_batch_rows_tracking() {
        let mut tracker = BatchOperationTracker::new("Test", 10, 1000);

        tracker.complete_batch(100);
        assert_eq!(tracker.processed_rows(), 100);

        tracker.complete_batch(150);
        assert_eq!(tracker.processed_rows(), 250);

        tracker.complete_batch(50);
        assert_eq!(tracker.processed_rows(), 300);
    }

    #[test]
    fn test_batch_rate_calculation() {
        let mut tracker = BatchOperationTracker::new("Test", 10, 1000);

        // With no completed batches, rate should be 0 or very low
        assert!(tracker.throughput() >= 0.0);

        // Complete a batch
        tracker.complete_batch(100);

        // Rate should now be calculable
        assert!(tracker.throughput() >= 0.0);
    }

    #[test]
    fn test_batch_error_recording() {
        let mut tracker = BatchOperationTracker::new("Test", 10, 1000);
        assert_eq!(tracker.error_count(), 0);

        tracker.record_error();
        assert_eq!(tracker.error_count(), 1);

        tracker.record_errors(5);
        assert_eq!(tracker.error_count(), 6);
    }

    #[test]
    fn test_batch_error_threshold() {
        let mut tracker = BatchOperationTracker::new("Test", 10, 1000).error_threshold(5);
        tracker.complete_batch(100);

        // No errors - normal state
        assert_eq!(tracker.current_state(), BatchState::Normal);

        // Some errors but below threshold - warning
        tracker.record_errors(3);
        assert_eq!(tracker.current_state(), BatchState::Warning);

        // Exceed threshold - error state
        tracker.record_errors(5);
        assert_eq!(tracker.current_state(), BatchState::Error);
    }

    #[test]
    fn test_batch_render_plain() {
        let mut tracker = BatchOperationTracker::new("Batch insert", 20, 10000);
        tracker.complete_batch(500);

        let plain = tracker.render_plain();
        assert!(plain.contains("Batch insert:"));
        assert!(plain.contains("5%"));
        assert!(plain.contains("(1/20 batches)"));
        assert!(plain.contains("500/10000 rows"));
        assert!(plain.contains("0 errors"));
    }

    #[test]
    fn test_batch_render_plain_with_errors() {
        let mut tracker = BatchOperationTracker::new("Test", 10, 1000);
        tracker.complete_batch(100);
        tracker.record_errors(3);

        let plain = tracker.render_plain();
        assert!(plain.contains("3 errors"));
    }

    #[test]
    fn test_batch_summary() {
        let mut tracker = BatchOperationTracker::new("Migration", 5, 500);
        tracker.complete_batch(100);
        tracker.complete_batch(100);
        tracker.complete_batch(100);
        tracker.complete_batch(100);
        tracker.complete_batch(100);

        let summary = tracker.render_summary();
        assert!(summary.contains("Migration"));
        assert!(summary.contains("Total rows: 500"));
        assert!(summary.contains("Errors: 0"));
        assert!(summary.contains("Success rate:"));
    }

    #[test]
    fn test_batch_single_batch() {
        let mut tracker = BatchOperationTracker::new("Single", 1, 100);
        tracker.complete_batch(100);

        assert!(tracker.is_complete());
        assert_eq!(tracker.batch_percentage(), 100.0);
        assert_eq!(tracker.current_state(), BatchState::Complete);
    }

    #[test]
    fn test_batch_many_batches() {
        let mut tracker = BatchOperationTracker::new("Large", 100, 10000);

        for _ in 0..100 {
            tracker.complete_batch(100);
        }

        assert!(tracker.is_complete());
        assert_eq!(tracker.processed_rows(), 10000);
        assert_eq!(tracker.completed_batches(), 100);
    }

    #[test]
    fn test_batch_percentage_calculation() {
        let mut tracker = BatchOperationTracker::new("Test", 10, 1000);

        assert!((tracker.batch_percentage() - 0.0).abs() < f64::EPSILON);

        tracker.complete_batch(100);
        assert!((tracker.batch_percentage() - 10.0).abs() < f64::EPSILON);

        tracker.complete_batch(100);
        tracker.complete_batch(100);
        tracker.complete_batch(100);
        tracker.complete_batch(100);
        assert!((tracker.batch_percentage() - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_row_percentage_calculation() {
        let mut tracker = BatchOperationTracker::new("Test", 10, 1000);

        assert!((tracker.row_percentage() - 0.0).abs() < f64::EPSILON);

        tracker.complete_batch(250);
        assert!((tracker.row_percentage() - 25.0).abs() < f64::EPSILON);

        tracker.complete_batch(250);
        assert!((tracker.row_percentage() - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_batch_zero_total() {
        let tracker = BatchOperationTracker::new("Test", 0, 0);
        assert!((tracker.batch_percentage() - 100.0).abs() < f64::EPSILON);
        assert!((tracker.row_percentage() - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_batch_is_complete() {
        let mut tracker = BatchOperationTracker::new("Test", 3, 300);
        assert!(!tracker.is_complete());

        tracker.complete_batch(100);
        assert!(!tracker.is_complete());

        tracker.complete_batch(100);
        assert!(!tracker.is_complete());

        tracker.complete_batch(100);
        assert!(tracker.is_complete());
    }

    #[test]
    fn test_batch_success_rate() {
        let mut tracker = BatchOperationTracker::new("Test", 10, 1000);
        tracker.complete_batch(100);

        // No errors - 100% success
        assert!((tracker.success_rate() - 100.0).abs() < 0.1);

        // Some errors
        tracker.record_error();
        // 100 rows, 1 error: 100/101 ≈ 99.01%
        assert!(tracker.success_rate() > 99.0 && tracker.success_rate() < 100.0);
    }

    #[test]
    fn test_batch_success_rate_no_data() {
        let tracker = BatchOperationTracker::new("Test", 10, 1000);
        // No processed rows and no errors - default to 100%
        assert!((tracker.success_rate() - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_batch_json_output() {
        let mut tracker = BatchOperationTracker::new("Test", 10, 1000);
        tracker.complete_batch(100);
        tracker.record_error();

        let json = tracker.to_json();
        assert!(json.contains("\"operation\":\"Test\""));
        assert!(json.contains("\"completed_batches\":1"));
        assert!(json.contains("\"total_batches\":10"));
        assert!(json.contains("\"processed_rows\":100"));
        assert!(json.contains("\"error_count\":1"));
        assert!(json.contains("\"state\":\"warning\""));
    }

    #[test]
    fn test_batch_json_complete() {
        let mut tracker = BatchOperationTracker::new("Test", 1, 100);
        tracker.complete_batch(100);

        let json = tracker.to_json();
        assert!(json.contains("\"is_complete\":true"));
        assert!(json.contains("\"state\":\"complete\""));
    }

    #[test]
    fn test_batch_styled_contains_progress_bar() {
        let mut tracker = BatchOperationTracker::new("Test", 10, 1000).width(20);
        tracker.complete_batch(500);

        let styled = tracker.render_styled();
        assert!(styled.contains('['));
        assert!(styled.contains(']'));
        assert!(styled.contains("Rows:"));
    }

    #[test]
    fn test_batch_styled_error_warning() {
        let mut tracker = BatchOperationTracker::new("Test", 10, 1000)
            .error_threshold(5)
            .width(20);
        tracker.complete_batch(100);
        tracker.record_errors(10);

        let styled = tracker.render_styled();
        assert!(styled.contains("threshold exceeded"));
    }

    #[test]
    fn test_batch_builder_chain() {
        let tracker = BatchOperationTracker::new("Test", 10, 1000)
            .theme(Theme::default())
            .width(40)
            .error_threshold(20)
            .smoothing_window(10);

        assert_eq!(tracker.total_batches(), 10);
    }

    #[test]
    fn test_format_duration_ms() {
        let result = format_duration(0.5);
        assert!(result.contains("ms"));
    }

    #[test]
    fn test_format_duration_seconds() {
        let result = format_duration(30.0);
        assert!(result.contains('s'));
        assert!(!result.contains('m'));
    }

    #[test]
    fn test_format_duration_minutes() {
        let result = format_duration(125.0);
        assert!(result.contains('m'));
    }

    #[test]
    fn test_format_duration_hours() {
        let result = format_duration(7300.0);
        assert!(result.contains('h'));
    }
}
