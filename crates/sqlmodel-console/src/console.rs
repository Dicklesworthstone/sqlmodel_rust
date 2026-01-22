//! SqlModelConsole - Main coordinator for console output.
//!
//! This module provides the central `SqlModelConsole` struct that coordinates
//! all output rendering. It automatically adapts to the detected output mode
//! and provides a consistent API for all console operations.
//!
//! # Stream Separation
//!
//! - `print()` → stdout (semantic data for agents to parse)
//! - `status()`, `success()`, `error()`, etc. → stderr (human feedback)
//!
//! # Markup Syntax
//!
//! In rich mode, text can use markup syntax: `[bold red]text[/]`
//! In plain mode, markup is automatically stripped.
//!
//! # Example
//!
//! ```rust
//! use sqlmodel_console::{SqlModelConsole, OutputMode};
//!
//! let console = SqlModelConsole::new();
//!
//! // Mode-aware output
//! console.print("Regular output");
//! console.success("Operation completed");
//! console.error("Something went wrong");
//! ```

use crate::mode::OutputMode;
use crate::theme::Theme;

/// Main coordinator for all SQLModel console output.
///
/// `SqlModelConsole` provides a unified API for rendering output that
/// automatically adapts to the detected output mode (Plain, Rich, or Json).
///
/// # Example
///
/// ```rust
/// use sqlmodel_console::{SqlModelConsole, OutputMode};
///
/// let console = SqlModelConsole::new();
/// console.print("Hello, world!");
/// console.status("Processing...");
/// console.success("Done!");
/// ```
#[derive(Debug, Clone)]
pub struct SqlModelConsole {
    /// Current output mode.
    mode: OutputMode,
    /// Color theme.
    theme: Theme,
    /// Default width for plain mode rules and formatting.
    plain_width: usize,
    // Note: We intentionally don't store rich_rust::Console here because it contains
    // Cell/RefCell types that are not Sync. Instead, rich output is created on-demand
    // in methods that need it. This allows SqlModelConsole to be Send+Sync for use
    // in global statics and cross-thread sharing.
}

impl SqlModelConsole {
    /// Create a new console with auto-detected mode and default theme.
    ///
    /// This is the recommended way to create a console. It will:
    /// 1. Check environment variables for explicit mode
    /// 2. Detect AI agent environments
    /// 3. Check terminal capabilities
    /// 4. Choose appropriate mode
    #[must_use]
    pub fn new() -> Self {
        Self {
            mode: OutputMode::detect(),
            theme: Theme::default(),
            plain_width: 80,
        }
    }

    /// Create a console with a specific output mode.
    ///
    /// Use this when you need to force a specific mode regardless of environment.
    #[must_use]
    pub fn with_mode(mode: OutputMode) -> Self {
        Self {
            mode,
            theme: Theme::default(),
            plain_width: 80,
        }
    }

    /// Create a console with a specific theme.
    #[must_use]
    pub fn with_theme(theme: Theme) -> Self {
        Self {
            mode: OutputMode::detect(),
            theme,
            plain_width: 80,
        }
    }

    /// Builder method to set the theme.
    #[must_use]
    pub fn theme(mut self, theme: Theme) -> Self {
        self.theme = theme;
        self
    }

    /// Builder method to set the plain mode width.
    #[must_use]
    pub fn plain_width(mut self, width: usize) -> Self {
        self.plain_width = width;
        self
    }

    /// Get the current output mode.
    #[must_use]
    pub const fn mode(&self) -> OutputMode {
        self.mode
    }

    /// Get the current theme.
    #[must_use]
    pub const fn get_theme(&self) -> &Theme {
        &self.theme
    }

    /// Get the plain mode width.
    #[must_use]
    pub const fn get_plain_width(&self) -> usize {
        self.plain_width
    }

    /// Set the output mode.
    pub fn set_mode(&mut self, mode: OutputMode) {
        self.mode = mode;
    }

    /// Set the theme.
    pub fn set_theme(&mut self, theme: Theme) {
        self.theme = theme;
    }

    /// Check if rich output is active.
    #[must_use]
    pub fn is_rich(&self) -> bool {
        self.mode == OutputMode::Rich
    }

    /// Check if plain output is active.
    #[must_use]
    pub fn is_plain(&self) -> bool {
        self.mode == OutputMode::Plain
    }

    /// Check if JSON output is active.
    #[must_use]
    pub fn is_json(&self) -> bool {
        self.mode == OutputMode::Json
    }

    // =========================================================================
    // Basic Output Methods
    // =========================================================================

    /// Print a message to stdout.
    ///
    /// In rich mode, supports markup syntax: `[bold red]text[/]`
    /// In plain mode, prints without formatting (markup stripped).
    /// In JSON mode, regular prints go to stderr to keep stdout clean.
    pub fn print(&self, message: &str) {
        match self.mode {
            OutputMode::Rich => {
                // TODO: Use rich_rust console when available
                println!("{}", strip_markup(message));
            }
            OutputMode::Plain => {
                println!("{}", strip_markup(message));
            }
            OutputMode::Json => {
                // In JSON mode, regular prints go to stderr to keep stdout for JSON
                eprintln!("{}", strip_markup(message));
            }
        }
    }

    /// Print to stdout without any markup processing.
    ///
    /// Use this when you need raw output without markup stripping.
    pub fn print_raw(&self, message: &str) {
        println!("{message}");
    }

    /// Print a message followed by a newline to stderr.
    ///
    /// Status messages are always sent to stderr because:
    /// - Agents typically only parse stdout
    /// - Status messages are transient/informational
    /// - Separating streams helps with output redirection
    pub fn status(&self, message: &str) {
        match self.mode {
            OutputMode::Rich => {
                // TODO: Use dim styling when rich_rust available
                eprintln!("{}", strip_markup(message));
            }
            OutputMode::Plain | OutputMode::Json => {
                eprintln!("{}", strip_markup(message));
            }
        }
    }

    /// Print a success message (green with checkmark).
    pub fn success(&self, message: &str) {
        self.print_styled_status(message, "green", "\u{2713}"); // ✓
    }

    /// Print an error message (red with X).
    pub fn error(&self, message: &str) {
        self.print_styled_status(message, "red bold", "\u{2717}"); // ✗
    }

    /// Print a warning message (yellow with warning sign).
    pub fn warning(&self, message: &str) {
        self.print_styled_status(message, "yellow", "\u{26A0}"); // ⚠
    }

    /// Print an info message (cyan with info symbol).
    pub fn info(&self, message: &str) {
        self.print_styled_status(message, "cyan", "\u{2139}"); // ℹ
    }

    fn print_styled_status(&self, message: &str, _style: &str, icon: &str) {
        match self.mode {
            OutputMode::Rich => {
                // TODO: Use rich_rust styling when available
                eprintln!("{icon} {message}");
            }
            OutputMode::Plain => {
                // Plain mode: no icons, just the message
                eprintln!("{message}");
            }
            OutputMode::Json => {
                // JSON mode: include icon for context
                eprintln!("{icon} {message}");
            }
        }
    }

    // =========================================================================
    // Horizontal Rules
    // =========================================================================

    /// Print a horizontal rule/divider.
    ///
    /// Optionally includes a title centered in the rule.
    pub fn rule(&self, title: Option<&str>) {
        match self.mode {
            OutputMode::Rich => {
                // TODO: Use rich_rust rule when available
                self.plain_rule(title);
            }
            OutputMode::Plain | OutputMode::Json => {
                self.plain_rule(title);
            }
        }
    }

    fn plain_rule(&self, title: Option<&str>) {
        let width = self.plain_width;
        match title {
            Some(t) => {
                let title_len = t.chars().count();
                if title_len + 4 >= width {
                    // Title too long, just print it
                    eprintln!("-- {t} --");
                } else {
                    let padding = (width - title_len - 2) / 2;
                    let left = "-".repeat(padding);
                    let right_padding = width - padding - title_len - 2;
                    let right = "-".repeat(right_padding);
                    eprintln!("{left} {t} {right}");
                }
            }
            None => {
                eprintln!("{}", "-".repeat(width));
            }
        }
    }

    // =========================================================================
    // JSON Output
    // =========================================================================

    /// Output JSON to stdout (compact format for parseability).
    ///
    /// Returns an error if serialization fails.
    pub fn print_json<T: serde::Serialize>(&self, value: &T) -> Result<(), serde_json::Error> {
        let json = serde_json::to_string(value)?;
        println!("{json}");
        Ok(())
    }

    /// Output pretty-printed JSON to stdout.
    ///
    /// In rich mode, could include syntax highlighting (not yet implemented).
    pub fn print_json_pretty<T: serde::Serialize>(
        &self,
        value: &T,
    ) -> Result<(), serde_json::Error> {
        let json = serde_json::to_string_pretty(value)?;
        match self.mode {
            OutputMode::Rich => {
                #[cfg(feature = "rich")]
                {
                    // TODO: JSON syntax highlighting when rich_rust available
                    println!("{json}");
                    return Ok(());
                }
                #[cfg(not(feature = "rich"))]
                println!("{json}");
            }
            OutputMode::Plain | OutputMode::Json => {
                println!("{json}");
            }
        }
        Ok(())
    }

    // =========================================================================
    // Line/Newline Helpers
    // =========================================================================

    /// Print an empty line to stdout.
    pub fn newline(&self) {
        println!();
    }

    /// Print an empty line to stderr.
    pub fn newline_stderr(&self) {
        eprintln!();
    }
}

impl Default for SqlModelConsole {
    fn default() -> Self {
        Self::new()
    }
}

// =========================================================================
// Helper Functions
// =========================================================================

/// Strip markup tags from a string for plain output.
///
/// Removes `[tag]...[/]` patterns commonly used in rich markup syntax.
/// Handles nested tags and preserves literal bracket characters when
/// they're not part of markup patterns.
///
/// A tag is considered markup if:
/// - It starts with `/` (closing tags: `[/]`, `[/bold]`)
/// - It contains a space (compound styles: `[red on white]`)
/// - It has 2+ alphabetic characters (style names: `[bold]`, `[red]`)
///
/// This preserves array indices like `[0]`, `[i]`, `[idx]` which are typically
/// short identifiers without spaces.
///
/// # Example
///
/// ```rust
/// use sqlmodel_console::console::strip_markup;
///
/// assert_eq!(strip_markup("[bold]text[/]"), "text");
/// assert_eq!(strip_markup("[red on white]hello[/]"), "hello");
/// assert_eq!(strip_markup("no markup"), "no markup");
/// assert_eq!(strip_markup("array[0]"), "array[0]");
/// ```
#[must_use]
pub fn strip_markup(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        if c == '[' {
            // Look ahead to find the closing ]
            let mut j = i + 1;
            let mut found_close = false;
            let mut close_idx = 0;

            while j < chars.len() {
                if chars[j] == ']' {
                    found_close = true;
                    close_idx = j;
                    break;
                }
                if chars[j] == '[' {
                    // Nested open bracket before close - not a tag
                    break;
                }
                j += 1;
            }

            if found_close {
                // Extract the tag content
                let tag_content: String = chars[i + 1..close_idx].iter().collect();

                // Check if this looks like markup:
                // 1. Closing tags: starts with '/' (e.g., "/", "/bold")
                // 2. Compound styles: contains a space (e.g., "red on white")
                // 3. Style names: has 2+ alphabetic chars (e.g., "bold", "red")
                let letter_count = tag_content.chars().filter(|c| c.is_alphabetic()).count();
                let is_markup =
                    tag_content.starts_with('/') || tag_content.contains(' ') || letter_count >= 2;

                if is_markup {
                    // Skip the entire tag
                    i = close_idx + 1;
                    continue;
                }
            }

            // Not a markup tag, keep the bracket
            result.push(c);
        } else {
            result.push(c);
        }

        i += 1;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_markup_basic() {
        assert_eq!(strip_markup("[bold]text[/]"), "text");
        assert_eq!(strip_markup("[red]hello[/]"), "hello");
    }

    #[test]
    fn test_strip_markup_with_style() {
        assert_eq!(strip_markup("[red on white]hello[/]"), "hello");
        assert_eq!(strip_markup("[bold italic]styled[/]"), "styled");
    }

    #[test]
    fn test_strip_markup_no_markup() {
        assert_eq!(strip_markup("no markup"), "no markup");
        assert_eq!(strip_markup("plain text"), "plain text");
    }

    #[test]
    fn test_strip_markup_nested() {
        assert_eq!(strip_markup("[bold][italic]nested[/][/]"), "nested");
        // Realistic nested tags use style names, not single letters
        assert_eq!(strip_markup("[red][bold][dim]deep[/][/][/]"), "deep");
    }

    #[test]
    fn test_strip_markup_multiple() {
        assert_eq!(
            strip_markup("[bold]hello[/] [italic]world[/]"),
            "hello world"
        );
    }

    #[test]
    fn test_strip_markup_preserves_brackets() {
        // Unclosed brackets should be preserved
        assert_eq!(strip_markup("array[0]"), "array[0]");
        assert_eq!(strip_markup("func(a[i])"), "func(a[i])");
    }

    #[test]
    fn test_strip_markup_empty() {
        assert_eq!(strip_markup(""), "");
        assert_eq!(strip_markup("[bold][/]"), "");
    }

    #[test]
    fn test_console_creation() {
        let console = SqlModelConsole::new();
        // Mode depends on environment, so just check it's valid
        assert!(matches!(
            console.mode(),
            OutputMode::Plain | OutputMode::Rich | OutputMode::Json
        ));
    }

    #[test]
    fn test_with_mode() {
        let console = SqlModelConsole::with_mode(OutputMode::Plain);
        assert!(console.is_plain());
        assert!(!console.is_rich());
        assert!(!console.is_json());

        let console = SqlModelConsole::with_mode(OutputMode::Rich);
        assert!(console.is_rich());
        assert!(!console.is_plain());

        let console = SqlModelConsole::with_mode(OutputMode::Json);
        assert!(console.is_json());
    }

    #[test]
    fn test_with_theme() {
        let light_theme = Theme::light();
        let console = SqlModelConsole::with_theme(light_theme.clone());
        assert_eq!(console.get_theme().success.rgb(), light_theme.success.rgb());
    }

    #[test]
    fn test_builder_methods() {
        let console = SqlModelConsole::new().plain_width(120);
        assert_eq!(console.get_plain_width(), 120);
    }

    #[test]
    fn test_set_mode() {
        let mut console = SqlModelConsole::new();
        console.set_mode(OutputMode::Json);
        assert!(console.is_json());
    }

    #[test]
    fn test_default() {
        let console1 = SqlModelConsole::default();
        let console2 = SqlModelConsole::new();
        assert_eq!(console1.mode(), console2.mode());
    }

    #[test]
    fn test_json_output() {
        use serde::Serialize;

        #[derive(Serialize)]
        struct TestData {
            name: String,
            value: i32,
        }

        let console = SqlModelConsole::with_mode(OutputMode::Json);
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        // Just verify it doesn't panic - actual output goes to stdout
        let result = console.print_json(&data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_json_pretty_output() {
        use serde::Serialize;

        #[derive(Serialize)]
        struct TestData {
            items: Vec<i32>,
        }

        let console = SqlModelConsole::with_mode(OutputMode::Plain);
        let data = TestData {
            items: vec![1, 2, 3],
        };

        let result = console.print_json_pretty(&data);
        assert!(result.is_ok());
    }
}
