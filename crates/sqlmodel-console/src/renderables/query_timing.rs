//! Query timing display for execution performance visualization.
//!
//! Provides visual breakdown of query execution timing.
//!
//! # Example
//!
//! ```rust
//! use sqlmodel_console::renderables::QueryTiming;
//! use std::time::Duration;
//!
//! let timing = QueryTiming::new()
//!     .total(Duration::from_millis(12))
//!     .parse(Duration::from_micros(1200))
//!     .plan(Duration::from_micros(3400))
//!     .execute(Duration::from_micros(7700))
//!     .rows(3);
//!
//! println!("{}", timing.render_plain());
//! println!("{}", timing.render_styled());
//! ```

use crate::theme::Theme;
use std::time::Duration;

/// A timing phase with label and duration.
#[derive(Debug, Clone)]
pub struct TimingPhase {
    /// Phase name (e.g., "Parse", "Plan", "Execute")
    pub name: String,
    /// Duration of this phase
    pub duration: Duration,
}

impl TimingPhase {
    /// Create a new timing phase.
    #[must_use]
    pub fn new(name: impl Into<String>, duration: Duration) -> Self {
        Self {
            name: name.into(),
            duration,
        }
    }
}

/// Query timing display for execution performance visualization.
///
/// Shows a breakdown of query execution time with optional phase details.
#[derive(Debug, Clone)]
pub struct QueryTiming {
    /// Total execution time
    total_time: Option<Duration>,
    /// Number of rows affected/returned
    row_count: Option<u64>,
    /// Individual timing phases
    phases: Vec<TimingPhase>,
    /// Theme for styled output
    theme: Option<Theme>,
    /// Width of timing bars
    bar_width: usize,
}

impl QueryTiming {
    /// Create a new query timing display.
    #[must_use]
    pub fn new() -> Self {
        Self {
            total_time: None,
            row_count: None,
            phases: Vec::new(),
            theme: None,
            bar_width: 20,
        }
    }

    /// Set the total execution time.
    #[must_use]
    pub fn total(mut self, duration: Duration) -> Self {
        self.total_time = Some(duration);
        self
    }

    /// Set the total execution time in milliseconds.
    #[must_use]
    pub fn total_ms(mut self, ms: f64) -> Self {
        self.total_time = Some(Duration::from_secs_f64(ms / 1000.0));
        self
    }

    /// Set the row count.
    #[must_use]
    pub fn rows(mut self, count: u64) -> Self {
        self.row_count = Some(count);
        self
    }

    /// Add a timing phase.
    #[must_use]
    pub fn phase(mut self, name: impl Into<String>, duration: Duration) -> Self {
        self.phases.push(TimingPhase::new(name, duration));
        self
    }

    /// Add parse phase timing.
    #[must_use]
    pub fn parse(self, duration: Duration) -> Self {
        self.phase("Parse", duration)
    }

    /// Add plan phase timing.
    #[must_use]
    pub fn plan(self, duration: Duration) -> Self {
        self.phase("Plan", duration)
    }

    /// Add execute phase timing.
    #[must_use]
    pub fn execute(self, duration: Duration) -> Self {
        self.phase("Execute", duration)
    }

    /// Add fetch phase timing.
    #[must_use]
    pub fn fetch(self, duration: Duration) -> Self {
        self.phase("Fetch", duration)
    }

    /// Set the theme for styled output.
    #[must_use]
    pub fn theme(mut self, theme: Theme) -> Self {
        self.theme = Some(theme);
        self
    }

    /// Set the width of timing bars.
    #[must_use]
    pub fn bar_width(mut self, width: usize) -> Self {
        self.bar_width = width;
        self
    }

    /// Format a duration for display.
    fn format_duration(duration: Duration) -> String {
        let micros = duration.as_micros();
        if micros < 1000 {
            format!("{}µs", micros)
        } else if micros < 1_000_000 {
            format!("{:.2}ms", micros as f64 / 1000.0)
        } else {
            format!("{:.2}s", duration.as_secs_f64())
        }
    }

    /// Calculate the total from phases if not set.
    fn effective_total(&self) -> Duration {
        self.total_time.unwrap_or_else(|| {
            self.phases.iter().map(|p| p.duration).sum()
        })
    }

    /// Render as plain text.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn render_plain(&self) -> String {
        let mut lines = Vec::new();
        let total = self.effective_total();

        // Header line
        let row_info = self.row_count.map_or(String::new(), |r| format!(" ({} rows)", r));
        lines.push(format!(
            "Query completed in {}{}",
            Self::format_duration(total),
            row_info
        ));

        // Phase breakdown
        if !self.phases.is_empty() {
            for phase in &self.phases {
                let pct = if total.as_nanos() > 0 {
                    (phase.duration.as_nanos() as f64 / total.as_nanos() as f64 * 100.0) as u32
                } else {
                    0
                };
                lines.push(format!(
                    "  {}: {} ({}%)",
                    phase.name,
                    Self::format_duration(phase.duration),
                    pct
                ));
            }
        }

        lines.join("\n")
    }

    /// Render as styled text with ANSI colors and bar charts.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn render_styled(&self) -> String {
        let theme = self.theme.clone().unwrap_or_default();
        let total = self.effective_total();
        let reset = "\x1b[0m";
        let success_color = theme.success.color_code();
        let dim = theme.dim.color_code();
        let info_color = theme.info.color_code();

        let mut lines = Vec::new();

        // Header line
        let row_info = self.row_count.map_or(String::new(), |r| format!(" ({} rows)", r));
        lines.push(format!(
            "{success_color}Query completed in {}{row_info}{reset}",
            Self::format_duration(total),
        ));

        // Phase breakdown with bars
        if !self.phases.is_empty() {
            let max_name_len = self.phases.iter().map(|p| p.name.len()).max().unwrap_or(0);
            let max_time_len = self
                .phases
                .iter()
                .map(|p| Self::format_duration(p.duration).len())
                .max()
                .unwrap_or(0);

            for phase in &self.phases {
                let pct = if total.as_nanos() > 0 {
                    phase.duration.as_nanos() as f64 / total.as_nanos() as f64
                } else {
                    0.0
                };
                let filled = (pct * self.bar_width as f64).round() as usize;
                let empty = self.bar_width.saturating_sub(filled);

                let bar = format!(
                    "{info_color}{}{dim}{}{reset}",
                    "█".repeat(filled),
                    "░".repeat(empty)
                );

                lines.push(format!(
                    "  {:width$}  {} {:>time_width$}",
                    phase.name,
                    bar,
                    Self::format_duration(phase.duration),
                    width = max_name_len,
                    time_width = max_time_len
                ));
            }
        }

        lines.join("\n")
    }

    /// Render as JSON-serializable structure.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        let total = self.effective_total();

        let phases: Vec<serde_json::Value> = self
            .phases
            .iter()
            .map(|p| {
                serde_json::json!({
                    "name": p.name,
                    "duration_us": p.duration.as_micros(),
                    "duration_ms": p.duration.as_secs_f64() * 1000.0,
                })
            })
            .collect();

        serde_json::json!({
            "total_us": total.as_micros(),
            "total_ms": total.as_secs_f64() * 1000.0,
            "row_count": self.row_count,
            "phases": phases,
        })
    }
}

impl Default for QueryTiming {
    fn default() -> Self {
        Self::new()
    }
}

/// Compact timing display for inline use.
///
/// Shows timing in a single line format suitable for headers or footers.
#[derive(Debug, Clone)]
pub struct CompactTiming {
    /// Execution time
    duration: Duration,
    /// Row count
    rows: Option<u64>,
}

impl CompactTiming {
    /// Create a new compact timing display.
    #[must_use]
    pub fn new(duration: Duration) -> Self {
        Self {
            duration,
            rows: None,
        }
    }

    /// Create from milliseconds.
    #[must_use]
    pub fn from_ms(ms: f64) -> Self {
        Self::new(Duration::from_secs_f64(ms / 1000.0))
    }

    /// Set the row count.
    #[must_use]
    pub fn rows(mut self, count: u64) -> Self {
        self.rows = Some(count);
        self
    }

    /// Render as plain text.
    #[must_use]
    pub fn render(&self) -> String {
        let time_str = QueryTiming::format_duration(self.duration);
        match self.rows {
            Some(r) => format!("{} rows in {}", r, time_str),
            None => time_str,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_timing_new() {
        let timing = QueryTiming::new();
        assert!(timing.total_time.is_none());
        assert!(timing.row_count.is_none());
    }

    #[test]
    fn test_query_timing_total() {
        let timing = QueryTiming::new().total(Duration::from_millis(100));
        assert_eq!(timing.total_time, Some(Duration::from_millis(100)));
    }

    #[test]
    fn test_query_timing_total_ms() {
        let timing = QueryTiming::new().total_ms(50.0);
        let expected = Duration::from_secs_f64(0.05);
        assert!((timing.total_time.unwrap().as_secs_f64() - expected.as_secs_f64()).abs() < 0.001);
    }

    #[test]
    fn test_query_timing_rows() {
        let timing = QueryTiming::new().rows(42);
        assert_eq!(timing.row_count, Some(42));
    }

    #[test]
    fn test_query_timing_phases() {
        let timing = QueryTiming::new()
            .parse(Duration::from_micros(100))
            .plan(Duration::from_micros(200))
            .execute(Duration::from_micros(300));

        assert_eq!(timing.phases.len(), 3);
        assert_eq!(timing.phases[0].name, "Parse");
        assert_eq!(timing.phases[1].name, "Plan");
        assert_eq!(timing.phases[2].name, "Execute");
    }

    #[test]
    fn test_format_duration_micros() {
        let s = QueryTiming::format_duration(Duration::from_micros(500));
        assert!(s.contains("µs"));
    }

    #[test]
    fn test_format_duration_millis() {
        let s = QueryTiming::format_duration(Duration::from_millis(50));
        assert!(s.contains("ms"));
    }

    #[test]
    fn test_format_duration_seconds() {
        let s = QueryTiming::format_duration(Duration::from_secs(2));
        assert!(s.contains('s'));
    }

    #[test]
    fn test_render_plain_basic() {
        let timing = QueryTiming::new()
            .total(Duration::from_millis(12))
            .rows(3);

        let output = timing.render_plain();
        assert!(output.contains("Query completed"));
        assert!(output.contains("3 rows"));
    }

    #[test]
    fn test_render_plain_with_phases() {
        let timing = QueryTiming::new()
            .total(Duration::from_millis(10))
            .parse(Duration::from_micros(1000))
            .plan(Duration::from_micros(2000))
            .execute(Duration::from_micros(7000));

        let output = timing.render_plain();
        assert!(output.contains("Parse"));
        assert!(output.contains("Plan"));
        assert!(output.contains("Execute"));
    }

    #[test]
    fn test_render_styled_contains_ansi() {
        let timing = QueryTiming::new()
            .total(Duration::from_millis(10))
            .parse(Duration::from_micros(5000))
            .execute(Duration::from_micros(5000));

        let styled = timing.render_styled();
        assert!(styled.contains('\x1b'));
    }

    #[test]
    fn test_render_styled_contains_bars() {
        let timing = QueryTiming::new()
            .total(Duration::from_millis(10))
            .parse(Duration::from_micros(5000))
            .execute(Duration::from_micros(5000));

        let styled = timing.render_styled();
        assert!(styled.contains('█') || styled.contains('░'));
    }

    #[test]
    fn test_to_json() {
        let timing = QueryTiming::new()
            .total(Duration::from_millis(10))
            .rows(5)
            .parse(Duration::from_micros(3000));

        let json = timing.to_json();
        assert_eq!(json["row_count"], 5);
        assert!(json["total_us"].as_u64().unwrap() > 0);
        assert!(json["phases"].is_array());
    }

    #[test]
    fn test_effective_total_from_phases() {
        let timing = QueryTiming::new()
            .parse(Duration::from_micros(1000))
            .execute(Duration::from_micros(2000));

        // No explicit total set, should sum phases
        let total = timing.effective_total();
        assert_eq!(total, Duration::from_micros(3000));
    }

    #[test]
    fn test_timing_phase_new() {
        let phase = TimingPhase::new("Test", Duration::from_millis(5));
        assert_eq!(phase.name, "Test");
        assert_eq!(phase.duration, Duration::from_millis(5));
    }

    #[test]
    fn test_compact_timing_new() {
        let compact = CompactTiming::new(Duration::from_millis(10));
        assert_eq!(compact.duration, Duration::from_millis(10));
    }

    #[test]
    fn test_compact_timing_from_ms() {
        let compact = CompactTiming::from_ms(25.0);
        let rendered = compact.render();
        assert!(rendered.contains("ms"));
    }

    #[test]
    fn test_compact_timing_with_rows() {
        let compact = CompactTiming::new(Duration::from_millis(10)).rows(42);
        let rendered = compact.render();
        assert!(rendered.contains("42 rows"));
    }

    #[test]
    fn test_default() {
        let timing = QueryTiming::default();
        assert!(timing.total_time.is_none());
    }

    #[test]
    fn test_bar_width() {
        let timing = QueryTiming::new()
            .bar_width(30)
            .parse(Duration::from_micros(500))
            .execute(Duration::from_micros(500));

        assert_eq!(timing.bar_width, 30);
    }

    #[test]
    fn test_fetch_phase() {
        let timing = QueryTiming::new()
            .fetch(Duration::from_micros(1000));

        assert_eq!(timing.phases.len(), 1);
        assert_eq!(timing.phases[0].name, "Fetch");
    }

    #[test]
    fn test_custom_phase() {
        let timing = QueryTiming::new()
            .phase("Custom", Duration::from_micros(1000));

        assert_eq!(timing.phases.len(), 1);
        assert_eq!(timing.phases[0].name, "Custom");
    }
}
