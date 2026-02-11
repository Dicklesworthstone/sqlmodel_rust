//! Migration status renderable for tracking database migrations.
//!
//! Provides a visual display of migration status, showing applied vs pending
//! migrations with timestamps, checksums, and visual indicators.
//!
//! # Example
//!
//! ```rust
//! use sqlmodel_console::renderables::{MigrationStatus, MigrationRecord, MigrationState};
//!
//! let status = MigrationStatus::new(vec![
//!     MigrationRecord::new("001", "create_users")
//!         .state(MigrationState::Applied)
//!         .applied_at(Some("2024-01-15T10:30:00Z".to_string()))
//!         .duration_ms(Some(45)),
//!     MigrationRecord::new("002", "add_email_index")
//!         .state(MigrationState::Applied)
//!         .applied_at(Some("2024-01-15T10:30:01Z".to_string()))
//!         .duration_ms(Some(12)),
//!     MigrationRecord::new("003", "add_posts_table")
//!         .state(MigrationState::Pending),
//! ]);
//!
//! // Plain mode output for agents
//! println!("{}", status.render_plain());
//! ```

use crate::theme::Theme;

/// Migration state enum indicating the status of a migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MigrationState {
    /// Migration has been successfully applied to the database.
    Applied,
    /// Migration is pending and has not yet been applied.
    #[default]
    Pending,
    /// Migration failed during execution.
    Failed,
    /// Migration was skipped (e.g., manually marked as complete).
    Skipped,
}

impl MigrationState {
    /// Get a human-readable status string.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Applied => "APPLIED",
            Self::Pending => "PENDING",
            Self::Failed => "FAILED",
            Self::Skipped => "SKIPPED",
        }
    }

    /// Get the short status indicator for plain mode.
    #[must_use]
    pub fn indicator(&self) -> &'static str {
        match self {
            Self::Applied => "[OK]",
            Self::Pending => "[PENDING]",
            Self::Failed => "[FAILED]",
            Self::Skipped => "[SKIPPED]",
        }
    }

    /// Get the status icon for rich mode.
    #[must_use]
    pub fn icon(&self) -> &'static str {
        match self {
            Self::Applied => "✓",
            Self::Pending => "○",
            Self::Failed => "✗",
            Self::Skipped => "⊘",
        }
    }

    /// Get the ANSI color code for this migration state.
    #[must_use]
    pub fn color_code(&self) -> &'static str {
        match self {
            Self::Applied => "\x1b[32m", // Green
            Self::Pending => "\x1b[33m", // Yellow
            Self::Failed => "\x1b[31m",  // Red
            Self::Skipped => "\x1b[90m", // Gray
        }
    }

    /// Get the ANSI reset code.
    #[must_use]
    pub fn reset_code() -> &'static str {
        "\x1b[0m"
    }
}

impl std::fmt::Display for MigrationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A single migration record with metadata.
#[derive(Debug, Clone, Default)]
pub struct MigrationRecord {
    /// Version identifier (e.g., "001", "20240115_1030").
    pub version: String,
    /// Human-readable migration name.
    pub name: String,
    /// Current state of this migration.
    pub state: MigrationState,
    /// ISO-8601 timestamp when migration was applied.
    pub applied_at: Option<String>,
    /// Checksum for migration file verification.
    pub checksum: Option<String>,
    /// Execution duration in milliseconds.
    pub duration_ms: Option<u64>,
    /// Error message if migration failed.
    pub error_message: Option<String>,
    /// Up SQL preview (for pending migrations).
    pub up_sql: Option<String>,
    /// Down SQL preview (for applied migrations).
    pub down_sql: Option<String>,
}

impl MigrationRecord {
    /// Create a new migration record with version and name.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlmodel_console::renderables::MigrationRecord;
    ///
    /// let record = MigrationRecord::new("001", "create_users_table");
    /// ```
    #[must_use]
    pub fn new(version: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            version: version.into(),
            name: name.into(),
            state: MigrationState::default(),
            applied_at: None,
            checksum: None,
            duration_ms: None,
            error_message: None,
            up_sql: None,
            down_sql: None,
        }
    }

    /// Set the migration state.
    #[must_use]
    pub fn state(mut self, state: MigrationState) -> Self {
        self.state = state;
        self
    }

    /// Set the applied timestamp.
    #[must_use]
    pub fn applied_at(mut self, timestamp: Option<String>) -> Self {
        self.applied_at = timestamp;
        self
    }

    /// Set the checksum.
    #[must_use]
    pub fn checksum(mut self, checksum: Option<String>) -> Self {
        self.checksum = checksum;
        self
    }

    /// Set the execution duration in milliseconds.
    #[must_use]
    pub fn duration_ms(mut self, duration: Option<u64>) -> Self {
        self.duration_ms = duration;
        self
    }

    /// Set an error message (for failed migrations).
    #[must_use]
    pub fn error_message(mut self, message: Option<String>) -> Self {
        self.error_message = message;
        self
    }

    /// Set the up SQL preview.
    #[must_use]
    pub fn up_sql(mut self, sql: Option<String>) -> Self {
        self.up_sql = sql;
        self
    }

    /// Set the down SQL preview.
    #[must_use]
    pub fn down_sql(mut self, sql: Option<String>) -> Self {
        self.down_sql = sql;
        self
    }

    /// Format the duration for display.
    fn format_duration(&self) -> Option<String> {
        self.duration_ms.map(|ms| {
            if ms < 1000 {
                format!("{}ms", ms)
            } else if ms < 60_000 {
                let secs = ms as f64 / 1000.0;
                format!("{:.1}s", secs)
            } else {
                let mins = ms / 60_000;
                let secs = (ms % 60_000) / 1000;
                format!("{}m {}s", mins, secs)
            }
        })
    }

    /// Format the timestamp for display (simplified).
    fn format_timestamp(&self) -> Option<String> {
        self.applied_at.as_ref().map(|ts| {
            // Try to extract just the date and time portion
            // Input: "2024-01-15T10:30:00Z" or "2024-01-15 10:30:00"
            // Output: "2024-01-15 10:30:00"
            ts.replace('T', " ")
                .trim_end_matches('Z')
                .trim_end_matches("+00:00")
                .to_string()
        })
    }
}

/// Display options for migration status.
///
/// Shows a list of migrations with their states, timestamps, and durations.
#[derive(Debug, Clone)]
pub struct MigrationStatus {
    /// List of migration records to display.
    records: Vec<MigrationRecord>,
    /// Theme for styled output.
    theme: Theme,
    /// Whether to show checksums.
    show_checksums: bool,
    /// Whether to show durations.
    show_duration: bool,
    /// Whether to show SQL previews.
    show_sql: bool,
    /// Optional width constraint.
    width: Option<usize>,
    /// Title for the status display.
    title: Option<String>,
}

impl MigrationStatus {
    /// Create a new migration status display from a list of records.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlmodel_console::renderables::{MigrationStatus, MigrationRecord, MigrationState};
    ///
    /// let status = MigrationStatus::new(vec![
    ///     MigrationRecord::new("001", "create_users").state(MigrationState::Applied),
    ///     MigrationRecord::new("002", "add_posts").state(MigrationState::Pending),
    /// ]);
    /// ```
    #[must_use]
    pub fn new(records: Vec<MigrationRecord>) -> Self {
        Self {
            records,
            theme: Theme::default(),
            show_checksums: false,
            show_duration: true,
            show_sql: false,
            width: None,
            title: None,
        }
    }

    /// Set the theme for styled output.
    #[must_use]
    pub fn theme(mut self, theme: Theme) -> Self {
        self.theme = theme;
        self
    }

    /// Set whether to show checksums.
    #[must_use]
    pub fn show_checksums(mut self, show: bool) -> Self {
        self.show_checksums = show;
        self
    }

    /// Set whether to show durations.
    #[must_use]
    pub fn show_duration(mut self, show: bool) -> Self {
        self.show_duration = show;
        self
    }

    /// Set whether to show SQL previews.
    #[must_use]
    pub fn show_sql(mut self, show: bool) -> Self {
        self.show_sql = show;
        self
    }

    /// Set the display width.
    #[must_use]
    pub fn width(mut self, width: usize) -> Self {
        self.width = Some(width);
        self
    }

    /// Set a custom title.
    #[must_use]
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Get the count of applied migrations.
    #[must_use]
    pub fn applied_count(&self) -> usize {
        self.records
            .iter()
            .filter(|r| r.state == MigrationState::Applied)
            .count()
    }

    /// Get the count of pending migrations.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.records
            .iter()
            .filter(|r| r.state == MigrationState::Pending)
            .count()
    }

    /// Get the count of failed migrations.
    #[must_use]
    pub fn failed_count(&self) -> usize {
        self.records
            .iter()
            .filter(|r| r.state == MigrationState::Failed)
            .count()
    }

    /// Get the count of skipped migrations.
    #[must_use]
    pub fn skipped_count(&self) -> usize {
        self.records
            .iter()
            .filter(|r| r.state == MigrationState::Skipped)
            .count()
    }

    /// Get the total count of migrations.
    #[must_use]
    pub fn total_count(&self) -> usize {
        self.records.len()
    }

    /// Check if all migrations are applied.
    #[must_use]
    pub fn is_up_to_date(&self) -> bool {
        self.pending_count() == 0 && self.failed_count() == 0
    }

    /// Render as plain text for agent consumption.
    ///
    /// Returns a structured plain text representation suitable for
    /// non-TTY environments or agent parsing.
    #[must_use]
    pub fn render_plain(&self) -> String {
        let mut lines = Vec::new();

        // Header
        let title = self.title.as_deref().unwrap_or("MIGRATION STATUS");
        lines.push(title.to_string());
        lines.push("=".repeat(title.len()));

        // Summary line
        lines.push(format!(
            "Applied: {}, Pending: {}, Failed: {}, Total: {}",
            self.applied_count(),
            self.pending_count(),
            self.failed_count(),
            self.total_count()
        ));
        lines.push(String::new());

        if self.records.is_empty() {
            lines.push("No migrations found.".to_string());
            return lines.join("\n");
        }

        // Migration records
        for record in &self.records {
            let mut parts = vec![
                record.state.indicator().to_string(),
                format!("{}_{}", record.version, record.name),
            ];

            // Add timestamp for applied migrations
            if let Some(ts) = record.format_timestamp() {
                parts.push(format!("- Applied {}", ts));
            }

            // Add duration if showing and available
            if self.show_duration {
                if let Some(dur) = record.format_duration() {
                    parts.push(format!("({})", dur));
                }
            }

            lines.push(parts.join(" "));

            // Add error message for failed migrations
            if record.state == MigrationState::Failed {
                if let Some(ref err) = record.error_message {
                    lines.push(format!("    Error: {}", err));
                }
            }

            // Add checksum if showing
            if self.show_checksums {
                if let Some(ref checksum) = record.checksum {
                    lines.push(format!("    Checksum: {}", checksum));
                }
            }

            // Add SQL preview if showing
            if self.show_sql {
                if record.state == MigrationState::Pending {
                    if let Some(ref sql) = record.up_sql {
                        lines.push("    Up SQL:".to_string());
                        for sql_line in sql.lines().take(3) {
                            lines.push(format!("      {}", sql_line));
                        }
                    }
                } else if record.state == MigrationState::Applied {
                    if let Some(ref sql) = record.down_sql {
                        lines.push("    Down SQL:".to_string());
                        for sql_line in sql.lines().take(3) {
                            lines.push(format!("      {}", sql_line));
                        }
                    }
                }
            }
        }

        lines.join("\n")
    }

    /// Render with ANSI colors for terminal display.
    ///
    /// Returns a rich panel representation with colored status indicators
    /// and formatted content.
    #[must_use]
    pub fn render_styled(&self) -> String {
        let width = self.width.unwrap_or(80).max(6);
        let reset = MigrationState::reset_code();
        let dim = "\x1b[2m";

        let mut lines = Vec::new();

        // Title
        let title = self.title.as_deref().unwrap_or("Migration Status");

        // Top border with title
        let max_title_chars = width.saturating_sub(4);
        let title_text = self.truncate_plain_to_width(title, max_title_chars);
        let title_display = format!(" {title_text} ");
        let title_len = title_display.chars().count();
        let border_space = width.saturating_sub(2);
        let total_pad = border_space.saturating_sub(title_len);
        let left_pad = total_pad / 2;
        let right_pad = total_pad.saturating_sub(left_pad);

        lines.push(format!(
            "{}╭{}{}{}╮{}",
            self.border_color(),
            "─".repeat(left_pad),
            title_display,
            "─".repeat(right_pad),
            reset
        ));

        // Summary line
        let summary = format!(
            " Applied: {}{}{}  Pending: {}{}{}  Failed: {}{}{}",
            self.theme.success.color_code(),
            self.applied_count(),
            reset,
            self.theme.warning.color_code(),
            self.pending_count(),
            reset,
            self.theme.error.color_code(),
            self.failed_count(),
            reset,
        );
        lines.push(self.wrap_line(&summary, width));

        // Separator
        lines.push(format!(
            "{}├{}┤{}",
            self.border_color(),
            "─".repeat(width.saturating_sub(2)),
            reset
        ));

        if self.records.is_empty() {
            let empty_msg = format!(" {}No migrations found.{}", dim, reset);
            lines.push(self.wrap_line(&empty_msg, width));
        } else {
            // Column headers
            let header = format!(
                " {dim}Status   Version   Name{:width$}Applied At          Duration{reset}",
                "",
                width = width.saturating_sub(70),
                dim = dim,
                reset = reset
            );
            lines.push(self.wrap_line(&header, width));
            lines.push(format!(
                "{}│{}{}│{}",
                self.border_color(),
                dim,
                "─".repeat(width.saturating_sub(2)),
                reset
            ));

            // Migration rows
            for record in &self.records {
                let state_color = record.state.color_code();
                let icon = record.state.icon();

                // Format version and name (truncate if needed)
                let version_name = format!("{}_{}", record.version, record.name);
                let version_name_display = self.truncate_plain_to_width(&version_name, 30);

                // Format timestamp
                let timestamp = record.format_timestamp().unwrap_or_else(|| "-".to_string());

                // Format duration
                let duration = if self.show_duration {
                    record.format_duration().unwrap_or_else(|| "-".to_string())
                } else {
                    String::new()
                };

                let row = format!(
                    " {}{} {:7}{} {:30} {:19} {:>8}",
                    state_color,
                    icon,
                    record.state.as_str(),
                    reset,
                    version_name_display,
                    timestamp,
                    duration,
                );
                lines.push(self.wrap_line(&row, width));

                // Show error for failed migrations
                if record.state == MigrationState::Failed {
                    if let Some(ref err) = record.error_message {
                        let err_line = format!(
                            "   {}Error: {}{}",
                            self.theme.error.color_code(),
                            err,
                            reset
                        );
                        lines.push(self.wrap_line(&err_line, width));
                    }
                }

                // Show checksum if enabled
                if self.show_checksums {
                    if let Some(ref checksum) = record.checksum {
                        let checksum_line = format!("   {}Checksum: {}{}", dim, checksum, reset);
                        lines.push(self.wrap_line(&checksum_line, width));
                    }
                }
            }
        }

        // Bottom border
        lines.push(format!(
            "{}╰{}╯{}",
            self.border_color(),
            "─".repeat(width.saturating_sub(2)),
            reset
        ));

        lines.join("\n")
    }

    /// Render as JSON-serializable structure.
    ///
    /// Returns a JSON value suitable for structured logging or API responses.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        let records: Vec<serde_json::Value> = self
            .records
            .iter()
            .map(|r| {
                serde_json::json!({
                    "version": r.version,
                    "name": r.name,
                    "state": r.state.as_str(),
                    "applied_at": r.applied_at,
                    "checksum": r.checksum,
                    "duration_ms": r.duration_ms,
                    "error_message": r.error_message,
                })
            })
            .collect();

        serde_json::json!({
            "title": self.title,
            "summary": {
                "applied": self.applied_count(),
                "pending": self.pending_count(),
                "failed": self.failed_count(),
                "skipped": self.skipped_count(),
                "total": self.total_count(),
                "up_to_date": self.is_up_to_date(),
            },
            "migrations": records,
        })
    }

    /// Get the border color code.
    fn border_color(&self) -> String {
        self.theme.border.color_code()
    }

    /// Wrap a line to fit within the panel width.
    fn wrap_line(&self, content: &str, width: usize) -> String {
        let visible_len = self.visible_length(content);
        let padding = width.saturating_sub(2).saturating_sub(visible_len);
        let reset = MigrationState::reset_code();

        format!(
            "{}│{}{content}{:padding$}{}│{}",
            self.border_color(),
            reset,
            "",
            self.border_color(),
            reset,
            padding = padding
        )
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
                len += 1;
            }
        }
        len
    }
}

impl Default for MigrationStatus {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // === Test 1: test_migration_status_creation ===
    #[test]
    fn test_migration_status_creation() {
        let status = MigrationStatus::new(vec![
            MigrationRecord::new("001", "create_users"),
            MigrationRecord::new("002", "add_posts"),
        ]);

        assert_eq!(status.total_count(), 2);
        assert_eq!(status.records.len(), 2);
    }

    // === Test 2: test_migration_state_applied ===
    #[test]
    fn test_migration_state_applied() {
        let state = MigrationState::Applied;

        assert_eq!(state.as_str(), "APPLIED");
        assert_eq!(state.indicator(), "[OK]");
        assert_eq!(state.icon(), "✓");
        assert!(state.color_code().contains("32")); // Green
    }

    // === Test 3: test_migration_state_pending ===
    #[test]
    fn test_migration_state_pending() {
        let state = MigrationState::Pending;

        assert_eq!(state.as_str(), "PENDING");
        assert_eq!(state.indicator(), "[PENDING]");
        assert_eq!(state.icon(), "○");
        assert!(state.color_code().contains("33")); // Yellow
    }

    // === Test 4: test_migration_state_failed ===
    #[test]
    fn test_migration_state_failed() {
        let state = MigrationState::Failed;

        assert_eq!(state.as_str(), "FAILED");
        assert_eq!(state.indicator(), "[FAILED]");
        assert_eq!(state.icon(), "✗");
        assert!(state.color_code().contains("31")); // Red
    }

    // === Test 5: test_migration_render_plain ===
    #[test]
    fn test_migration_render_plain() {
        let status = MigrationStatus::new(vec![
            MigrationRecord::new("001", "create_users")
                .state(MigrationState::Applied)
                .applied_at(Some("2024-01-15T10:30:00Z".to_string()))
                .duration_ms(Some(45)),
            MigrationRecord::new("002", "add_posts").state(MigrationState::Pending),
        ]);

        let plain = status.render_plain();

        // Should contain header
        assert!(plain.contains("MIGRATION STATUS"));

        // Should contain summary
        assert!(plain.contains("Applied: 1"));
        assert!(plain.contains("Pending: 1"));

        // Should contain records
        assert!(plain.contains("[OK] 001_create_users"));
        assert!(plain.contains("[PENDING] 002_add_posts"));

        // Should contain timestamp
        assert!(plain.contains("2024-01-15"));

        // Should contain duration
        assert!(plain.contains("45ms"));
    }

    // === Test 6: test_migration_render_rich ===
    #[test]
    fn test_migration_render_rich() {
        let status = MigrationStatus::new(vec![
            MigrationRecord::new("001", "create_users").state(MigrationState::Applied),
        ])
        .width(80);

        let styled = status.render_styled();

        // Should contain box drawing characters
        assert!(styled.contains("╭"));
        assert!(styled.contains("╯"));
        assert!(styled.contains("│"));

        // Should contain status icon
        assert!(styled.contains("✓"));
    }

    #[test]
    fn test_migration_render_styled_tiny_width_does_not_panic() {
        let status = MigrationStatus::new(vec![
            MigrationRecord::new("001", "create_users").state(MigrationState::Applied),
        ])
        .width(1);

        let styled = status.render_styled();

        assert!(!styled.is_empty());
        assert!(styled.contains('╭'));
        assert!(styled.contains('╯'));
    }

    #[test]
    fn test_migration_render_styled_unicode_name_truncation() {
        let status = MigrationStatus::new(vec![
            MigrationRecord::new(
                "001",
                "🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥",
            )
            .state(MigrationState::Applied),
        ])
        .width(80);

        let styled = status.render_styled();

        assert!(styled.contains("..."));
        assert!(styled.contains("001_"));
    }

    // === Test 7: test_migration_timestamps ===
    #[test]
    fn test_migration_timestamps() {
        let record = MigrationRecord::new("001", "test")
            .applied_at(Some("2024-01-15T10:30:00Z".to_string()));

        let formatted = record.format_timestamp();

        assert!(formatted.is_some());
        let ts = formatted.unwrap();
        assert!(ts.contains("2024-01-15"));
        assert!(ts.contains("10:30:00"));
        assert!(!ts.contains('T')); // Should be replaced with space
        assert!(!ts.contains('Z')); // Should be stripped
    }

    // === Test 8: test_migration_checksums ===
    #[test]
    fn test_migration_checksums() {
        let status = MigrationStatus::new(vec![
            MigrationRecord::new("001", "test")
                .state(MigrationState::Applied)
                .checksum(Some("abc123def456".to_string())),
        ])
        .show_checksums(true);

        let plain = status.render_plain();

        assert!(plain.contains("Checksum: abc123def456"));
    }

    // === Test 9: test_migration_duration ===
    #[test]
    fn test_migration_duration() {
        // Test milliseconds
        let record_ms = MigrationRecord::new("001", "test").duration_ms(Some(45));
        assert_eq!(record_ms.format_duration(), Some("45ms".to_string()));

        // Test seconds
        let record_sec = MigrationRecord::new("002", "test").duration_ms(Some(2500));
        assert_eq!(record_sec.format_duration(), Some("2.5s".to_string()));

        // Test minutes
        let record_m = MigrationRecord::new("003", "test").duration_ms(Some(125_000));
        assert_eq!(record_m.format_duration(), Some("2m 5s".to_string()));
    }

    // === Test 10: test_migration_empty_list ===
    #[test]
    fn test_migration_empty_list() {
        let status = MigrationStatus::new(vec![]);

        assert_eq!(status.total_count(), 0);
        assert_eq!(status.applied_count(), 0);
        assert_eq!(status.pending_count(), 0);
        assert!(status.is_up_to_date());

        let plain = status.render_plain();
        assert!(plain.contains("No migrations found"));
    }

    // === Additional tests ===

    #[test]
    fn test_migration_state_display() {
        assert_eq!(format!("{}", MigrationState::Applied), "APPLIED");
        assert_eq!(format!("{}", MigrationState::Pending), "PENDING");
        assert_eq!(format!("{}", MigrationState::Failed), "FAILED");
        assert_eq!(format!("{}", MigrationState::Skipped), "SKIPPED");
    }

    #[test]
    fn test_migration_state_skipped() {
        let state = MigrationState::Skipped;

        assert_eq!(state.as_str(), "SKIPPED");
        assert_eq!(state.indicator(), "[SKIPPED]");
        assert_eq!(state.icon(), "⊘");
        assert!(state.color_code().contains("90")); // Gray
    }

    #[test]
    fn test_migration_record_builder() {
        let record = MigrationRecord::new("001", "create_users")
            .state(MigrationState::Applied)
            .applied_at(Some("2024-01-15T10:30:00Z".to_string()))
            .checksum(Some("abc123".to_string()))
            .duration_ms(Some(100))
            .error_message(None)
            .up_sql(Some("CREATE TABLE users".to_string()))
            .down_sql(Some("DROP TABLE users".to_string()));

        assert_eq!(record.version, "001");
        assert_eq!(record.name, "create_users");
        assert_eq!(record.state, MigrationState::Applied);
        assert!(record.applied_at.is_some());
        assert!(record.checksum.is_some());
        assert_eq!(record.duration_ms, Some(100));
        assert!(record.up_sql.is_some());
        assert!(record.down_sql.is_some());
    }

    #[test]
    fn test_migration_status_counts() {
        let status = MigrationStatus::new(vec![
            MigrationRecord::new("001", "a").state(MigrationState::Applied),
            MigrationRecord::new("002", "b").state(MigrationState::Applied),
            MigrationRecord::new("003", "c").state(MigrationState::Pending),
            MigrationRecord::new("004", "d").state(MigrationState::Failed),
            MigrationRecord::new("005", "e").state(MigrationState::Skipped),
        ]);

        assert_eq!(status.applied_count(), 2);
        assert_eq!(status.pending_count(), 1);
        assert_eq!(status.failed_count(), 1);
        assert_eq!(status.skipped_count(), 1);
        assert_eq!(status.total_count(), 5);
        assert!(!status.is_up_to_date()); // Has pending and failed
    }

    #[test]
    fn test_migration_is_up_to_date() {
        // All applied
        let status1 = MigrationStatus::new(vec![
            MigrationRecord::new("001", "a").state(MigrationState::Applied),
            MigrationRecord::new("002", "b").state(MigrationState::Applied),
        ]);
        assert!(status1.is_up_to_date());

        // Has pending
        let status2 = MigrationStatus::new(vec![
            MigrationRecord::new("001", "a").state(MigrationState::Applied),
            MigrationRecord::new("002", "b").state(MigrationState::Pending),
        ]);
        assert!(!status2.is_up_to_date());

        // Has failed
        let status3 = MigrationStatus::new(vec![
            MigrationRecord::new("001", "a").state(MigrationState::Failed),
        ]);
        assert!(!status3.is_up_to_date());
    }

    #[test]
    fn test_migration_status_builder_pattern() {
        let status = MigrationStatus::new(vec![])
            .theme(Theme::light())
            .show_checksums(true)
            .show_duration(false)
            .show_sql(true)
            .width(100)
            .title("Custom Title");

        assert!(status.show_checksums);
        assert!(!status.show_duration);
        assert!(status.show_sql);
        assert_eq!(status.width, Some(100));
        assert_eq!(status.title, Some("Custom Title".to_string()));
    }

    #[test]
    fn test_migration_to_json() {
        let status = MigrationStatus::new(vec![
            MigrationRecord::new("001", "create_users")
                .state(MigrationState::Applied)
                .applied_at(Some("2024-01-15T10:30:00Z".to_string()))
                .duration_ms(Some(45)),
            MigrationRecord::new("002", "add_posts").state(MigrationState::Pending),
        ]);

        let json = status.to_json();

        // Check summary
        assert_eq!(json["summary"]["applied"], 1);
        assert_eq!(json["summary"]["pending"], 1);
        assert_eq!(json["summary"]["total"], 2);
        assert!(!json["summary"]["up_to_date"].as_bool().unwrap());

        // Check migrations array
        let migrations = json["migrations"].as_array().unwrap();
        assert_eq!(migrations.len(), 2);
        assert_eq!(migrations[0]["state"], "APPLIED");
        assert_eq!(migrations[1]["state"], "PENDING");
    }

    #[test]
    fn test_migration_failed_with_error() {
        let status = MigrationStatus::new(vec![
            MigrationRecord::new("001", "broken")
                .state(MigrationState::Failed)
                .error_message(Some("Duplicate column 'id'".to_string())),
        ]);

        let plain = status.render_plain();

        assert!(plain.contains("[FAILED]"));
        assert!(plain.contains("Error: Duplicate column 'id'"));
    }

    #[test]
    fn test_migration_render_plain_with_sql() {
        let status = MigrationStatus::new(vec![
            MigrationRecord::new("001", "create_users")
                .state(MigrationState::Pending)
                .up_sql(Some(
                    "CREATE TABLE users (\n  id SERIAL,\n  name TEXT\n);".to_string(),
                )),
        ])
        .show_sql(true);

        let plain = status.render_plain();

        assert!(plain.contains("Up SQL:"));
        assert!(plain.contains("CREATE TABLE users"));
    }

    #[test]
    fn test_migration_default() {
        let status = MigrationStatus::default();
        assert_eq!(status.total_count(), 0);
        assert!(status.records.is_empty());
    }

    #[test]
    fn test_migration_record_default() {
        let record = MigrationRecord::default();
        assert_eq!(record.version, "");
        assert_eq!(record.name, "");
        assert_eq!(record.state, MigrationState::Pending);
    }

    #[test]
    fn test_migration_state_default() {
        let state = MigrationState::default();
        assert_eq!(state, MigrationState::Pending);
    }
}
