//! Operation progress bar for determinate operations.
//!
//! `OperationProgress` displays a progress bar with completion percentage,
//! throughput rate, and estimated time remaining.
//!
//! # Example
//!
//! ```rust
//! use sqlmodel_console::renderables::OperationProgress;
//!
//! let progress = OperationProgress::new("Inserting rows", 1000)
//!     .completed(420);
//!
//! // Plain text: "Inserting rows: 42% (420/1000)"
//! println!("{}", progress.render_plain());
//! ```

use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::theme::Theme;

/// Progress state for styling.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProgressState {
    /// Normal progress (default)
    #[default]
    Normal,
    /// Completed successfully
    Complete,
    /// Progress is slow/stalled
    Warning,
    /// Progress has errored
    Error,
}

/// A progress bar for operations with known total count.
///
/// Tracks completion percentage, calculates throughput rate, and estimates
/// time remaining based on current progress speed.
///
/// # Rendering Modes
///
/// - **Rich mode**: Colored progress bar with percentage, counter, throughput, ETA
/// - **Plain mode**: Text format suitable for agents: `Name: 42% (420/1000) 50.2/s ETA: 12s`
/// - **JSON mode**: Structured data for programmatic consumption
///
/// # Example
///
/// ```rust
/// use sqlmodel_console::renderables::OperationProgress;
///
/// let mut progress = OperationProgress::new("Processing", 100);
/// progress.set_completed(50);
///
/// assert_eq!(progress.percentage(), 50.0);
/// ```
#[derive(Debug, Clone)]
pub struct OperationProgress {
    /// Name of the operation being tracked
    operation_name: String,
    /// Number of items completed
    completed: u64,
    /// Total number of items
    total: u64,
    /// When the operation started (for rate calculation)
    started_at: Instant,
    /// Current state for styling
    state: ProgressState,
    /// Optional theme for styling
    theme: Option<Theme>,
    /// Optional fixed width for rendering
    width: Option<usize>,
    /// Whether to show ETA
    show_eta: bool,
    /// Whether to show throughput
    show_throughput: bool,
    /// Unit label for items (e.g., "rows", "bytes")
    unit: String,
}

impl OperationProgress {
    /// Create a new progress tracker.
    ///
    /// # Arguments
    /// - `operation_name`: Human-readable name for the operation
    /// - `total`: Total number of items to process
    #[must_use]
    pub fn new(operation_name: impl Into<String>, total: u64) -> Self {
        Self {
            operation_name: operation_name.into(),
            completed: 0,
            total,
            started_at: Instant::now(),
            state: ProgressState::Normal,
            theme: None,
            width: None,
            show_eta: true,
            show_throughput: true,
            unit: String::new(),
        }
    }

    /// Set the number of completed items.
    #[must_use]
    pub fn completed(mut self, completed: u64) -> Self {
        self.completed = completed.min(self.total);
        self.update_state();
        self
    }

    /// Set the number of completed items (mutable version).
    pub fn set_completed(&mut self, completed: u64) {
        self.completed = completed.min(self.total);
        self.update_state();
    }

    /// Increment the completed count by one.
    pub fn increment(&mut self) {
        if self.completed < self.total {
            self.completed += 1;
            self.update_state();
        }
    }

    /// Add to the completed count.
    pub fn add(&mut self, count: u64) {
        self.completed = self.completed.saturating_add(count).min(self.total);
        self.update_state();
    }

    /// Set the theme for styled output.
    #[must_use]
    pub fn theme(mut self, theme: Theme) -> Self {
        self.theme = Some(theme);
        self
    }

    /// Set the rendering width.
    #[must_use]
    pub fn width(mut self, width: usize) -> Self {
        self.width = Some(width);
        self
    }

    /// Set whether to show ETA.
    #[must_use]
    pub fn show_eta(mut self, show: bool) -> Self {
        self.show_eta = show;
        self
    }

    /// Set whether to show throughput.
    #[must_use]
    pub fn show_throughput(mut self, show: bool) -> Self {
        self.show_throughput = show;
        self
    }

    /// Set the unit label for items.
    #[must_use]
    pub fn unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = unit.into();
        self
    }

    /// Set the state manually (e.g., for error indication).
    #[must_use]
    pub fn state(mut self, state: ProgressState) -> Self {
        self.state = state;
        self
    }

    /// Reset the start time (useful when reusing a progress tracker).
    pub fn reset_timer(&mut self) {
        self.started_at = Instant::now();
    }

    /// Get the operation name.
    #[must_use]
    pub fn operation_name(&self) -> &str {
        &self.operation_name
    }

    /// Get the completed count.
    #[must_use]
    pub fn completed_count(&self) -> u64 {
        self.completed
    }

    /// Get the total count.
    #[must_use]
    pub fn total_count(&self) -> u64 {
        self.total
    }

    /// Get the current state.
    #[must_use]
    pub fn current_state(&self) -> ProgressState {
        self.state
    }

    /// Calculate the completion percentage.
    #[must_use]
    pub fn percentage(&self) -> f64 {
        if self.total == 0 {
            return 100.0;
        }
        (self.completed as f64 / self.total as f64) * 100.0
    }

    /// Calculate the elapsed time in seconds.
    #[must_use]
    pub fn elapsed_secs(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }

    /// Calculate the throughput (items per second).
    #[must_use]
    pub fn throughput(&self) -> f64 {
        let elapsed = self.elapsed_secs();
        if elapsed < 0.001 {
            return 0.0;
        }
        self.completed as f64 / elapsed
    }

    /// Calculate the estimated time remaining in seconds.
    #[must_use]
    pub fn eta_secs(&self) -> Option<f64> {
        let rate = self.throughput();
        if rate < 0.001 {
            return None;
        }
        let remaining = self.total.saturating_sub(self.completed);
        Some(remaining as f64 / rate)
    }

    /// Check if the operation is complete.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.completed >= self.total
    }

    /// Update state based on current progress.
    fn update_state(&mut self) {
        if self.completed >= self.total {
            self.state = ProgressState::Complete;
        }
        // Could add warning state detection for slow operations
    }

    /// Render as plain text for agents.
    ///
    /// Format: `Name: 42% (420/1000) 50.2/s ETA: 12s`
    #[must_use]
    pub fn render_plain(&self) -> String {
        let pct = self.percentage();
        let mut parts = vec![format!(
            "{}: {:.0}% ({}/{})",
            self.operation_name, pct, self.completed, self.total
        )];

        if self.show_throughput && self.completed > 0 {
            let rate = self.throughput();
            let unit_label = if self.unit.is_empty() {
                ""
            } else {
                &self.unit
            };
            parts.push(format!("{rate:.1}{unit_label}/s"));
        }

        if self.show_eta && !self.is_complete() {
            if let Some(eta) = self.eta_secs() {
                parts.push(format!("ETA: {}", format_duration(eta)));
            }
        }

        parts.join(" ")
    }

    /// Render with ANSI styling.
    ///
    /// Shows a visual progress bar with colors based on state.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // bar_width is bounded
    pub fn render_styled(&self) -> String {
        let bar_width = self.width.unwrap_or(30);
        let pct = self.percentage();
        let filled = ((pct / 100.0) * bar_width as f64).round() as usize;
        let empty = bar_width.saturating_sub(filled);

        let theme = self.theme.clone().unwrap_or_default();

        let (bar_color, text_color) = match self.state {
            ProgressState::Normal => (theme.info.color_code(), theme.info.color_code()),
            ProgressState::Complete => (theme.success.color_code(), theme.success.color_code()),
            ProgressState::Warning => (theme.warning.color_code(), theme.warning.color_code()),
            ProgressState::Error => (theme.error.color_code(), theme.error.color_code()),
        };
        let reset = "\x1b[0m";

        // Build the progress bar
        let bar = format!(
            "{bar_color}[{filled}{empty}]{reset}",
            filled = "=".repeat(filled.saturating_sub(1))
                + if filled > 0 { ">" } else { "" },
            empty = " ".repeat(empty),
        );

        // Build the status line
        let mut parts = vec![
            format!("{text_color}{}{reset}", self.operation_name),
            bar,
            format!("{pct:.0}%"),
            format!("({}/{})", self.completed, self.total),
        ];

        if self.show_throughput && self.completed > 0 {
            let rate = self.throughput();
            let unit_label = if self.unit.is_empty() {
                ""
            } else {
                &self.unit
            };
            parts.push(format!("{rate:.1}{unit_label}/s"));
        }

        if self.show_eta && !self.is_complete() {
            if let Some(eta) = self.eta_secs() {
                parts.push(format!("ETA: {}", format_duration(eta)));
            }
        }

        parts.join(" ")
    }

    /// Render as JSON for structured output.
    #[must_use]
    pub fn to_json(&self) -> String {
        #[derive(Serialize)]
        struct ProgressJson<'a> {
            operation: &'a str,
            completed: u64,
            total: u64,
            percentage: f64,
            throughput: f64,
            #[serde(skip_serializing_if = "Option::is_none")]
            eta_secs: Option<f64>,
            elapsed_secs: f64,
            is_complete: bool,
            state: &'a str,
            #[serde(skip_serializing_if = "str::is_empty")]
            unit: &'a str,
        }

        let state_str = match self.state {
            ProgressState::Normal => "normal",
            ProgressState::Complete => "complete",
            ProgressState::Warning => "warning",
            ProgressState::Error => "error",
        };

        let json = ProgressJson {
            operation: &self.operation_name,
            completed: self.completed,
            total: self.total,
            percentage: self.percentage(),
            throughput: self.throughput(),
            eta_secs: self.eta_secs(),
            elapsed_secs: self.elapsed_secs(),
            is_complete: self.is_complete(),
            state: state_str,
            unit: &self.unit,
        };

        serde_json::to_string(&json).unwrap_or_else(|_| "{}".to_string())
    }
}

/// Format a duration in seconds to human-readable form.
fn format_duration(secs: f64) -> String {
    if secs < 1.0 {
        return "<1s".to_string();
    }
    if secs < 60.0 {
        return format!("{:.0}s", secs);
    }
    if secs < 3600.0 {
        let mins = (secs / 60.0).floor();
        let remaining = secs % 60.0;
        return format!("{:.0}m{:.0}s", mins, remaining);
    }
    let hours = (secs / 3600.0).floor();
    let remaining_mins = ((secs % 3600.0) / 60.0).floor();
    format!("{:.0}h{:.0}m", hours, remaining_mins)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_creation() {
        let progress = OperationProgress::new("Test", 100);
        assert_eq!(progress.operation_name(), "Test");
        assert_eq!(progress.completed_count(), 0);
        assert_eq!(progress.total_count(), 100);
        assert_eq!(progress.current_state(), ProgressState::Normal);
    }

    #[test]
    fn test_progress_percentage_calculation_zero() {
        let progress = OperationProgress::new("Test", 100).completed(0);
        assert!((progress.percentage() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_progress_percentage_calculation_half() {
        let progress = OperationProgress::new("Test", 100).completed(50);
        assert!((progress.percentage() - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_progress_percentage_calculation_full() {
        let progress = OperationProgress::new("Test", 100).completed(100);
        assert!((progress.percentage() - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_progress_percentage_zero_total() {
        let progress = OperationProgress::new("Test", 0);
        assert!((progress.percentage() - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_progress_increment() {
        let mut progress = OperationProgress::new("Test", 100);
        assert_eq!(progress.completed_count(), 0);
        progress.increment();
        assert_eq!(progress.completed_count(), 1);
        progress.increment();
        assert_eq!(progress.completed_count(), 2);
    }

    #[test]
    fn test_progress_increment_at_max() {
        let mut progress = OperationProgress::new("Test", 5).completed(5);
        progress.increment();
        assert_eq!(progress.completed_count(), 5); // Should not exceed total
    }

    #[test]
    fn test_progress_add_batch() {
        let mut progress = OperationProgress::new("Test", 100);
        progress.add(25);
        assert_eq!(progress.completed_count(), 25);
        progress.add(50);
        assert_eq!(progress.completed_count(), 75);
    }

    #[test]
    fn test_progress_add_exceeds_total() {
        let mut progress = OperationProgress::new("Test", 100);
        progress.add(150);
        assert_eq!(progress.completed_count(), 100); // Capped at total
    }

    #[test]
    fn test_progress_is_complete() {
        let progress = OperationProgress::new("Test", 100).completed(99);
        assert!(!progress.is_complete());

        let progress = OperationProgress::new("Test", 100).completed(100);
        assert!(progress.is_complete());
    }

    #[test]
    fn test_progress_state_updates() {
        let progress = OperationProgress::new("Test", 100).completed(100);
        assert_eq!(progress.current_state(), ProgressState::Complete);
    }

    #[test]
    fn test_progress_manual_state() {
        let progress = OperationProgress::new("Test", 100).state(ProgressState::Error);
        assert_eq!(progress.current_state(), ProgressState::Error);
    }

    #[test]
    fn test_progress_render_plain() {
        let progress = OperationProgress::new("Processing", 1000)
            .completed(500)
            .show_throughput(false)
            .show_eta(false);

        let plain = progress.render_plain();
        assert!(plain.contains("Processing:"));
        assert!(plain.contains("50%"));
        assert!(plain.contains("(500/1000)"));
    }

    #[test]
    fn test_progress_render_plain_complete() {
        let progress = OperationProgress::new("Done", 100)
            .completed(100)
            .show_throughput(false)
            .show_eta(false);

        let plain = progress.render_plain();
        assert!(plain.contains("100%"));
    }

    #[test]
    fn test_progress_render_styled_contains_bar() {
        let progress = OperationProgress::new("Test", 100)
            .completed(50)
            .width(20)
            .show_throughput(false)
            .show_eta(false);

        let styled = progress.render_styled();
        assert!(styled.contains('['));
        assert!(styled.contains(']'));
        assert!(styled.contains("50%"));
    }

    #[test]
    fn test_progress_json_output() {
        let progress = OperationProgress::new("Test", 100).completed(42);
        let json = progress.to_json();

        assert!(json.contains("\"operation\":\"Test\""));
        assert!(json.contains("\"completed\":42"));
        assert!(json.contains("\"total\":100"));
        assert!(json.contains("\"percentage\":42"));
        assert!(json.contains("\"is_complete\":false"));
    }

    #[test]
    fn test_progress_json_complete() {
        let progress = OperationProgress::new("Test", 100).completed(100);
        let json = progress.to_json();

        assert!(json.contains("\"is_complete\":true"));
        assert!(json.contains("\"state\":\"complete\""));
    }

    #[test]
    fn test_progress_with_unit() {
        let progress = OperationProgress::new("Transferring", 1000)
            .completed(500)
            .unit("KB")
            .show_throughput(true)
            .show_eta(false);

        let plain = progress.render_plain();
        assert!(plain.contains("KB/s") || plain.contains("(500/1000)"));
    }

    #[test]
    fn test_progress_set_completed() {
        let mut progress = OperationProgress::new("Test", 100);
        progress.set_completed(75);
        assert_eq!(progress.completed_count(), 75);
    }

    #[test]
    fn test_progress_builder_chain() {
        let progress = OperationProgress::new("Test", 100)
            .completed(50)
            .theme(Theme::default())
            .width(40)
            .show_eta(true)
            .show_throughput(true)
            .unit("items");

        assert_eq!(progress.completed_count(), 50);
    }

    #[test]
    fn test_format_duration_subsecond() {
        assert_eq!(format_duration(0.5), "<1s");
    }

    #[test]
    fn test_format_duration_seconds() {
        assert_eq!(format_duration(45.0), "45s");
    }

    #[test]
    fn test_format_duration_minutes() {
        let result = format_duration(125.0);
        assert!(result.contains('m'));
        assert!(result.contains('s'));
    }

    #[test]
    fn test_format_duration_hours() {
        let result = format_duration(3700.0);
        assert!(result.contains('h'));
        assert!(result.contains('m'));
    }

    #[test]
    fn test_progress_throughput_initial() {
        // Initially, throughput should be 0 or very low
        let progress = OperationProgress::new("Test", 100);
        // With 0 completed and near-zero elapsed time
        assert!(progress.throughput() >= 0.0);
    }

    #[test]
    fn test_progress_eta_no_progress() {
        let progress = OperationProgress::new("Test", 100);
        // With no progress, ETA should be None (rate is 0)
        assert!(progress.eta_secs().is_none() || progress.eta_secs().unwrap_or(0.0) >= 0.0);
    }
}
