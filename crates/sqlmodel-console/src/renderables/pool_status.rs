//! Connection pool status display renderable.
//!
//! Provides a visual dashboard for connection pool status, showing utilization,
//! health, and queue information at a glance.
//!
//! # Example
//!
//! ```rust
//! use sqlmodel_console::renderables::PoolStatusDisplay;
//!
//! // Create display with pool stats: active=8, idle=2, max=20, pending=0, timeouts=3
//! let display = PoolStatusDisplay::new(8, 2, 20, 0, 3);
//!
//! // Rich mode: Styled panel with progress bar
//! // Plain mode: Simple text output for agents
//! println!("{}", display.render_plain());
//! ```

use crate::theme::Theme;
use std::time::Duration;

/// Pool health status based on utilization and queue depth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolHealth {
    /// Pool is healthy: low utilization, no waiting requests
    Healthy,
    /// Pool is busy: high utilization (>80%) but no waiting requests
    Busy,
    /// Pool is degraded: some requests are waiting
    Degraded,
    /// Pool is exhausted: at capacity with significant waiting queue
    Exhausted,
}

impl PoolHealth {
    /// Get a human-readable status string.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "HEALTHY",
            Self::Busy => "BUSY",
            Self::Degraded => "DEGRADED",
            Self::Exhausted => "EXHAUSTED",
        }
    }

    /// Get the ANSI color code for this health status.
    #[must_use]
    pub fn color_code(&self) -> &'static str {
        match self {
            Self::Healthy => "\x1b[32m",        // Green
            Self::Busy => "\x1b[33m",           // Yellow
            Self::Degraded => "\x1b[38;5;208m", // Orange (256-color)
            Self::Exhausted => "\x1b[31m",      // Red
        }
    }
}

impl std::fmt::Display for PoolHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Statistics snapshot for pool status display.
///
/// This trait allows PoolStatusDisplay to work with any type that provides
/// pool statistics, including `sqlmodel_pool::PoolStats`.
pub trait PoolStatsProvider {
    /// Number of connections currently in use.
    fn active_connections(&self) -> usize;
    /// Number of idle connections available.
    fn idle_connections(&self) -> usize;
    /// Maximum number of connections allowed.
    fn max_connections(&self) -> usize;
    /// Minimum number of connections to maintain.
    fn min_connections(&self) -> usize;
    /// Number of requests waiting for a connection.
    fn pending_requests(&self) -> usize;
    /// Total connections ever created.
    fn connections_created(&self) -> u64;
    /// Total connections closed.
    fn connections_closed(&self) -> u64;
    /// Total successful acquires.
    fn total_acquires(&self) -> u64;
    /// Total acquire timeouts.
    fn total_timeouts(&self) -> u64;
}

/// Display options for pool status.
#[derive(Debug, Clone)]
pub struct PoolStatusDisplay {
    /// Active connections
    active: usize,
    /// Idle connections
    idle: usize,
    /// Maximum connections
    max: usize,
    /// Minimum connections
    min: usize,
    /// Pending requests
    pending: usize,
    /// Total connections created
    created: u64,
    /// Total connections closed
    closed: u64,
    /// Total acquires
    acquires: u64,
    /// Total timeouts
    timeouts: u64,
    /// Theme for styled output
    theme: Theme,
    /// Optional width constraint
    width: Option<usize>,
    /// Pool uptime
    uptime: Option<Duration>,
    /// Pool name/label
    name: Option<String>,
}

impl PoolStatusDisplay {
    /// Create a new pool status display from statistics.
    #[must_use]
    pub fn from_stats<S: PoolStatsProvider>(stats: &S) -> Self {
        Self {
            active: stats.active_connections(),
            idle: stats.idle_connections(),
            max: stats.max_connections(),
            min: stats.min_connections(),
            pending: stats.pending_requests(),
            created: stats.connections_created(),
            closed: stats.connections_closed(),
            acquires: stats.total_acquires(),
            timeouts: stats.total_timeouts(),
            theme: Theme::default(),
            width: None,
            uptime: None,
            name: None,
        }
    }

    /// Create a pool status display with explicit values.
    #[must_use]
    pub fn new(active: usize, idle: usize, max: usize, min: usize, pending: usize) -> Self {
        Self {
            active,
            idle,
            max,
            min,
            pending,
            created: 0,
            closed: 0,
            acquires: 0,
            timeouts: 0,
            theme: Theme::default(),
            width: None,
            uptime: None,
            name: None,
        }
    }

    /// Set the theme for styled output.
    #[must_use]
    pub fn theme(mut self, theme: Theme) -> Self {
        self.theme = theme;
        self
    }

    /// Set the display width.
    #[must_use]
    pub fn width(mut self, width: usize) -> Self {
        self.width = Some(width);
        self
    }

    /// Set the pool uptime.
    #[must_use]
    pub fn uptime(mut self, uptime: Duration) -> Self {
        self.uptime = Some(uptime);
        self
    }

    /// Set the pool name/label.
    #[must_use]
    pub fn name<S: Into<String>>(mut self, name: S) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set acquisition statistics.
    #[must_use]
    pub fn with_acquisition_stats(mut self, acquires: u64, timeouts: u64) -> Self {
        self.acquires = acquires;
        self.timeouts = timeouts;
        self
    }

    /// Set lifetime statistics.
    #[must_use]
    pub fn with_lifetime_stats(mut self, created: u64, closed: u64) -> Self {
        self.created = created;
        self.closed = closed;
        self
    }

    /// Get the total number of connections.
    #[must_use]
    pub fn total(&self) -> usize {
        self.active + self.idle
    }

    /// Calculate pool utilization as a percentage.
    #[must_use]
    pub fn utilization(&self) -> f64 {
        if self.max == 0 {
            0.0
        } else {
            (self.active as f64 / self.max as f64) * 100.0
        }
    }

    /// Determine pool health status.
    #[must_use]
    pub fn health(&self) -> PoolHealth {
        let utilization = self.utilization();

        if self.pending > 0 {
            if self.pending >= self.max || self.active >= self.max {
                PoolHealth::Exhausted
            } else {
                PoolHealth::Degraded
            }
        } else if utilization >= 80.0 {
            PoolHealth::Busy
        } else {
            PoolHealth::Healthy
        }
    }

    /// Format uptime duration as human-readable string.
    fn format_uptime(duration: Duration) -> String {
        let secs = duration.as_secs();
        if secs < 60 {
            format!("{}s", secs)
        } else if secs < 3600 {
            let mins = secs / 60;
            let secs = secs % 60;
            if secs == 0 {
                format!("{}m", mins)
            } else {
                format!("{}m {}s", mins, secs)
            }
        } else if secs < 86400 {
            let hours = secs / 3600;
            let mins = (secs % 3600) / 60;
            if mins == 0 {
                format!("{}h", hours)
            } else {
                format!("{}h {}m", hours, mins)
            }
        } else {
            let days = secs / 86400;
            let hours = (secs % 86400) / 3600;
            if hours == 0 {
                format!("{}d", days)
            } else {
                format!("{}d {}h", days, hours)
            }
        }
    }

    /// Render as plain text for agent consumption.
    #[must_use]
    pub fn render_plain(&self) -> String {
        let health = self.health();
        let utilization = self.utilization();
        let total = self.total();

        let mut lines = Vec::new();

        // Main status line
        let name_prefix = self
            .name
            .as_ref()
            .map(|n| format!("{}: ", n))
            .unwrap_or_default();

        lines.push(format!(
            "{}Pool: {}/{} active ({:.0}%), {} waiting, {}",
            name_prefix, self.active, self.max, utilization, self.pending, health
        ));

        // Detail line
        lines.push(format!(
            "  Active: {}, Idle: {}, Total: {}, Max: {}, Min: {}",
            self.active, self.idle, total, self.max, self.min
        ));

        // Statistics line (if available)
        if self.acquires > 0 || self.timeouts > 0 || self.created > 0 {
            let mut stats_parts = Vec::new();

            if self.acquires > 0 {
                stats_parts.push(format!("Acquires: {}", self.acquires));
            }
            if self.timeouts > 0 {
                stats_parts.push(format!("Timeouts: {}", self.timeouts));
            }
            if self.created > 0 {
                stats_parts.push(format!("Created: {}", self.created));
            }
            if self.closed > 0 {
                stats_parts.push(format!("Closed: {}", self.closed));
            }

            if !stats_parts.is_empty() {
                lines.push(format!("  {}", stats_parts.join(", ")));
            }
        }

        // Uptime line (if available)
        if let Some(uptime) = self.uptime {
            lines.push(format!("  Uptime: {}", Self::format_uptime(uptime)));
        }

        lines.join("\n")
    }

    /// Render with ANSI colors for terminal display.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn render_styled(&self) -> String {
        let health = self.health();
        let utilization = self.utilization();
        let width = self.width.unwrap_or(60);

        let mut lines = Vec::new();

        // Header with pool name
        let title = self.name.as_ref().map_or_else(
            || "Connection Pool Status".to_string(),
            |n| format!("Connection Pool: {}", n),
        );

        // Box drawing
        let top_border = format!("┌{}┐", "─".repeat(width - 2));
        let bottom_border = format!("└{}┘", "─".repeat(width - 2));

        lines.push(top_border);
        lines.push(format!("│ {:<width$}│", title, width = width - 3));
        lines.push(format!("├{}┤", "─".repeat(width - 2)));

        // Utilization bar
        let bar_width = width - 20;
        // Intentional truncation: utilization is 0-100%, bar_width is small
        let filled = ((utilization / 100.0) * bar_width as f64) as usize;
        let empty = bar_width.saturating_sub(filled);
        let bar = format!("{}{}", "█".repeat(filled), "░".repeat(empty));

        // Progress bar with percentage and status
        lines.push(format!(
            "│ [{}] {:.0}% {:<width$}│",
            bar,
            utilization,
            health.as_str(),
            width = width - bar_width - 12
        ));

        // Connection counts
        lines.push(
            format!(
                "│ Active: {:>4}  │  Idle: {:>4}  │  Max: {:>4}   │",
                self.active, self.idle, self.max
            )
            .chars()
            .take(width - 1)
            .collect::<String>()
                + "│",
        );

        // Waiting requests (with color if any)
        if self.pending > 0 {
            lines.push(format!(
                "│ ⚠ Waiting requests: {:<width$}│",
                self.pending,
                width = width - 24
            ));
        }

        // Statistics
        if self.acquires > 0 || self.timeouts > 0 {
            let timeout_rate = if self.acquires > 0 {
                (self.timeouts as f64 / self.acquires as f64) * 100.0
            } else {
                0.0
            };
            lines.push(format!(
                "│ Acquires: {} | Timeouts: {} ({:.1}%){:>width$}│",
                self.acquires,
                self.timeouts,
                timeout_rate,
                "",
                width = width.saturating_sub(40)
            ));
        }

        // Uptime
        if let Some(uptime) = self.uptime {
            lines.push(format!(
                "│ Uptime: {:<width$}│",
                Self::format_uptime(uptime),
                width = width - 12
            ));
        }

        lines.push(bottom_border);

        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock implementation of PoolStatsProvider for testing.
    #[derive(Debug, Clone)]
    struct MockPoolStats {
        active: usize,
        idle: usize,
        max: usize,
        min: usize,
        pending: usize,
        created: u64,
        closed: u64,
        acquires: u64,
        timeouts: u64,
    }

    impl MockPoolStats {
        fn healthy() -> Self {
            Self {
                active: 3,
                idle: 2,
                max: 20,
                min: 2,
                pending: 0,
                created: 100,
                closed: 95,
                acquires: 1000,
                timeouts: 5,
            }
        }

        fn busy() -> Self {
            Self {
                active: 17,
                idle: 1,
                max: 20,
                min: 2,
                pending: 0,
                created: 200,
                closed: 182,
                acquires: 5000,
                timeouts: 10,
            }
        }

        fn degraded() -> Self {
            // Degraded: some requests waiting but not at full capacity
            Self {
                active: 15,
                idle: 0,
                max: 20,
                min: 2,
                pending: 3,
                created: 300,
                closed: 280,
                acquires: 8000,
                timeouts: 50,
            }
        }

        fn exhausted() -> Self {
            Self {
                active: 20,
                idle: 0,
                max: 20,
                min: 2,
                pending: 25,
                created: 500,
                closed: 480,
                acquires: 10000,
                timeouts: 200,
            }
        }
    }

    impl PoolStatsProvider for MockPoolStats {
        fn active_connections(&self) -> usize {
            self.active
        }
        fn idle_connections(&self) -> usize {
            self.idle
        }
        fn max_connections(&self) -> usize {
            self.max
        }
        fn min_connections(&self) -> usize {
            self.min
        }
        fn pending_requests(&self) -> usize {
            self.pending
        }
        fn connections_created(&self) -> u64 {
            self.created
        }
        fn connections_closed(&self) -> u64 {
            self.closed
        }
        fn total_acquires(&self) -> u64 {
            self.acquires
        }
        fn total_timeouts(&self) -> u64 {
            self.timeouts
        }
    }

    #[test]
    fn test_pool_health_healthy() {
        let stats = MockPoolStats::healthy();
        let display = PoolStatusDisplay::from_stats(&stats);
        assert_eq!(display.health(), PoolHealth::Healthy);
    }

    #[test]
    fn test_pool_health_busy() {
        let stats = MockPoolStats::busy();
        let display = PoolStatusDisplay::from_stats(&stats);
        assert_eq!(display.health(), PoolHealth::Busy);
    }

    #[test]
    fn test_pool_health_degraded() {
        let stats = MockPoolStats::degraded();
        let display = PoolStatusDisplay::from_stats(&stats);
        assert_eq!(display.health(), PoolHealth::Degraded);
    }

    #[test]
    fn test_pool_health_exhausted() {
        let stats = MockPoolStats::exhausted();
        let display = PoolStatusDisplay::from_stats(&stats);
        assert_eq!(display.health(), PoolHealth::Exhausted);
    }

    #[test]
    fn test_pool_health_as_str() {
        assert_eq!(PoolHealth::Healthy.as_str(), "HEALTHY");
        assert_eq!(PoolHealth::Busy.as_str(), "BUSY");
        assert_eq!(PoolHealth::Degraded.as_str(), "DEGRADED");
        assert_eq!(PoolHealth::Exhausted.as_str(), "EXHAUSTED");
    }

    #[test]
    fn test_pool_health_display() {
        assert_eq!(format!("{}", PoolHealth::Healthy), "HEALTHY");
        assert_eq!(format!("{}", PoolHealth::Exhausted), "EXHAUSTED");
    }

    #[test]
    fn test_utilization_calculation() {
        let display = PoolStatusDisplay::new(10, 5, 20, 2, 0);
        assert!((display.utilization() - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_utilization_zero_max() {
        let display = PoolStatusDisplay::new(0, 0, 0, 0, 0);
        assert!(display.utilization().abs() < f64::EPSILON);
    }

    #[test]
    fn test_total_connections() {
        let display = PoolStatusDisplay::new(8, 4, 20, 2, 0);
        assert_eq!(display.total(), 12);
    }

    #[test]
    fn test_render_plain_healthy() {
        let stats = MockPoolStats::healthy();
        let display = PoolStatusDisplay::from_stats(&stats);
        let output = display.render_plain();

        assert!(output.contains("Pool:"));
        assert!(output.contains("HEALTHY"));
        assert!(output.contains("Active: 3"));
        assert!(output.contains("Idle: 2"));
    }

    #[test]
    fn test_render_plain_with_name() {
        let display = PoolStatusDisplay::new(5, 3, 20, 2, 0).name("PostgreSQL Main");
        let output = display.render_plain();

        assert!(output.contains("PostgreSQL Main:"));
    }

    #[test]
    fn test_render_plain_with_uptime() {
        let display = PoolStatusDisplay::new(5, 3, 20, 2, 0).uptime(Duration::from_secs(3725)); // 1h 2m 5s
        let output = display.render_plain();

        assert!(output.contains("Uptime:"));
        assert!(output.contains("1h 2m"));
    }

    #[test]
    fn test_render_plain_with_waiting() {
        let display = PoolStatusDisplay::new(20, 0, 20, 2, 5);
        let output = display.render_plain();

        assert!(output.contains("5 waiting"));
        assert!(output.contains("DEGRADED") || output.contains("EXHAUSTED"));
    }

    #[test]
    fn test_format_uptime_seconds() {
        assert_eq!(
            PoolStatusDisplay::format_uptime(Duration::from_secs(45)),
            "45s"
        );
    }

    #[test]
    fn test_format_uptime_minutes() {
        assert_eq!(
            PoolStatusDisplay::format_uptime(Duration::from_secs(125)),
            "2m 5s"
        );
        assert_eq!(
            PoolStatusDisplay::format_uptime(Duration::from_secs(120)),
            "2m"
        );
    }

    #[test]
    fn test_format_uptime_hours() {
        assert_eq!(
            PoolStatusDisplay::format_uptime(Duration::from_secs(3725)),
            "1h 2m"
        );
        assert_eq!(
            PoolStatusDisplay::format_uptime(Duration::from_secs(3600)),
            "1h"
        );
    }

    #[test]
    fn test_format_uptime_days() {
        assert_eq!(
            PoolStatusDisplay::format_uptime(Duration::from_secs(90000)),
            "1d 1h"
        );
        assert_eq!(
            PoolStatusDisplay::format_uptime(Duration::from_secs(86400)),
            "1d"
        );
    }

    #[test]
    fn test_new_with_explicit_values() {
        let display = PoolStatusDisplay::new(10, 5, 30, 3, 2);

        assert_eq!(display.active, 10);
        assert_eq!(display.idle, 5);
        assert_eq!(display.max, 30);
        assert_eq!(display.min, 3);
        assert_eq!(display.pending, 2);
    }

    #[test]
    fn test_builder_pattern() {
        let display = PoolStatusDisplay::new(5, 3, 20, 2, 0)
            .theme(Theme::light())
            .width(80)
            .name("TestPool")
            .uptime(Duration::from_secs(60))
            .with_acquisition_stats(100, 5)
            .with_lifetime_stats(50, 45);

        assert_eq!(display.width, Some(80));
        assert_eq!(display.name, Some("TestPool".to_string()));
        assert!(display.uptime.is_some());
        assert_eq!(display.acquires, 100);
        assert_eq!(display.timeouts, 5);
        assert_eq!(display.created, 50);
        assert_eq!(display.closed, 45);
    }

    #[test]
    fn test_health_color_codes() {
        assert!(PoolHealth::Healthy.color_code().contains("32")); // Green
        assert!(PoolHealth::Busy.color_code().contains("33")); // Yellow
        assert!(PoolHealth::Exhausted.color_code().contains("31")); // Red
    }

    #[test]
    fn test_render_styled_contains_box_drawing() {
        let display = PoolStatusDisplay::new(5, 3, 20, 2, 0).width(60);
        let output = display.render_styled();

        assert!(output.contains("┌"));
        assert!(output.contains("┐"));
        assert!(output.contains("└"));
        assert!(output.contains("┘"));
        assert!(output.contains("│"));
    }

    #[test]
    fn test_render_styled_contains_progress_bar() {
        let display = PoolStatusDisplay::new(10, 5, 20, 2, 0).width(60);
        let output = display.render_styled();

        // Should contain bar characters
        assert!(output.contains("█") || output.contains("░"));
        assert!(output.contains("50%")); // 10/20 = 50%
    }

    #[test]
    fn test_from_stats_captures_all_values() {
        let stats = MockPoolStats {
            active: 7,
            idle: 3,
            max: 25,
            min: 5,
            pending: 1,
            created: 150,
            closed: 140,
            acquires: 2000,
            timeouts: 15,
        };

        let display = PoolStatusDisplay::from_stats(&stats);

        assert_eq!(display.active, 7);
        assert_eq!(display.idle, 3);
        assert_eq!(display.max, 25);
        assert_eq!(display.min, 5);
        assert_eq!(display.pending, 1);
        assert_eq!(display.created, 150);
        assert_eq!(display.closed, 140);
        assert_eq!(display.acquires, 2000);
        assert_eq!(display.timeouts, 15);
    }
}
