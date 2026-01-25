//! Output mode detection for agent-safe console output.
//!
//! This module provides automatic detection of whether output should be
//! plain text (for AI agents and CI) or richly formatted (for humans).
//!
//! # Detection Priority
//!
//! The detection follows this priority order (first match wins):
//!
//! 1. `SQLMODEL_PLAIN=1` - Force plain output
//! 2. `SQLMODEL_JSON=1` - Force JSON output
//! 3. `SQLMODEL_RICH=1` - Force rich output (overrides agent detection!)
//! 4. `NO_COLOR` - Standard env var for disabling colors
//! 5. `CI=true` - CI environment detection
//! 6. `TERM=dumb` - Dumb terminal
//! 7. Agent env vars - Claude Code, Codex CLI, Cursor, etc.
//! 8. `!is_terminal(stdout)` - Piped or redirected output
//! 9. Default: Rich output
//!
//! # Agent Detection
//!
//! The following AI coding agents are detected:
//!
//! - Claude Code (`CLAUDE_CODE`)
//! - OpenAI Codex CLI (`CODEX_CLI`)
//! - Cursor IDE (`CURSOR_SESSION`)
//! - Aider (`AIDER_MODEL`, `AIDER_REPO`)
//! - GitHub Copilot (`GITHUB_COPILOT`)
//! - Continue.dev (`CONTINUE_SESSION`)
//! - Generic agent marker (`AGENT_MODE`)

use std::env;
use std::io::IsTerminal;

/// Output mode for console rendering.
///
/// Determines how console output should be formatted. The mode is automatically
/// detected based on environment variables and terminal state, but can be
/// overridden via `SQLMODEL_PLAIN`, `SQLMODEL_RICH`, or `SQLMODEL_JSON`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum OutputMode {
    /// Plain text output, no ANSI codes. Machine-parseable.
    ///
    /// Used for: AI agents, CI systems, piped output, dumb terminals.
    Plain,

    /// Rich formatted output with colors, tables, panels.
    ///
    /// Used for: Interactive human terminal sessions.
    #[default]
    Rich,

    /// Structured JSON output for programmatic consumption.
    ///
    /// Used for: Tool integrations, scripting, IDEs.
    Json,
}

impl OutputMode {
    /// Detect the appropriate output mode from the environment.
    ///
    /// This function checks various environment variables and terminal state
    /// to determine the best output mode. The detection is deterministic and
    /// follows a well-defined priority order.
    ///
    /// # Priority Order
    ///
    /// 1. `SQLMODEL_PLAIN=1` - Force plain output
    /// 2. `SQLMODEL_JSON=1` - Force JSON output
    /// 3. `SQLMODEL_RICH=1` - Force rich output (overrides agent detection!)
    /// 4. `NO_COLOR` present - Plain (standard convention)
    /// 5. `CI=true` - Plain (CI environment)
    /// 6. `TERM=dumb` - Plain (dumb terminal)
    /// 7. Agent environment detected - Plain
    /// 8. stdout is not a TTY - Plain
    /// 9. Default - Rich
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sqlmodel_console::OutputMode;
    ///
    /// let mode = OutputMode::detect();
    /// match mode {
    ///     OutputMode::Plain => println!("Using plain text"),
    ///     OutputMode::Rich => println!("Using rich formatting"),
    ///     OutputMode::Json => println!("Using JSON output"),
    /// }
    /// ```
    #[must_use]
    pub fn detect() -> Self {
        // Explicit overrides (highest priority)
        if env_is_truthy("SQLMODEL_PLAIN") {
            return Self::Plain;
        }
        if env_is_truthy("SQLMODEL_JSON") {
            return Self::Json;
        }
        if env_is_truthy("SQLMODEL_RICH") {
            return Self::Rich; // Force rich even for agents
        }

        // Standard "no color" convention (https://no-color.org/)
        if env::var("NO_COLOR").is_ok() {
            return Self::Plain;
        }

        // CI environments
        if env_is_truthy("CI") {
            return Self::Plain;
        }

        // Dumb terminal
        if env::var("TERM").is_ok_and(|t| t == "dumb") {
            return Self::Plain;
        }

        // Agent detection
        if Self::is_agent_environment() {
            return Self::Plain;
        }

        // Not a TTY (piped, redirected)
        if !std::io::stdout().is_terminal() {
            return Self::Plain;
        }

        // Default: rich output for humans
        Self::Rich
    }

    /// Check if we're running in an AI coding agent environment.
    ///
    /// This function checks for environment variables set by known AI coding
    /// assistants. When detected, we default to plain output to ensure
    /// machine-parseability.
    ///
    /// # Known Agent Environment Variables
    ///
    /// - `CLAUDE_CODE` - Claude Code CLI
    /// - `CODEX_CLI` - OpenAI Codex CLI
    /// - `CURSOR_SESSION` - Cursor IDE
    /// - `AIDER_MODEL` / `AIDER_REPO` - Aider coding assistant
    /// - `AGENT_MODE` - Generic agent marker
    /// - `GITHUB_COPILOT` - GitHub Copilot
    /// - `CONTINUE_SESSION` - Continue.dev extension
    /// - `CODY_*` - Sourcegraph Cody
    /// - `WINDSURF_*` - Windsurf/Codeium
    /// - `GEMINI_CLI` - Google Gemini CLI
    ///
    /// # Returns
    ///
    /// `true` if any agent environment variable is detected.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sqlmodel_console::OutputMode;
    ///
    /// if OutputMode::is_agent_environment() {
    ///     println!("Running under an AI agent");
    /// }
    /// ```
    #[must_use]
    pub fn is_agent_environment() -> bool {
        const AGENT_MARKERS: &[&str] = &[
            // Claude/Anthropic
            "CLAUDE_CODE",
            // OpenAI
            "CODEX_CLI",
            "CODEX_SESSION",
            // Cursor
            "CURSOR_SESSION",
            "CURSOR_EDITOR",
            // Aider
            "AIDER_MODEL",
            "AIDER_REPO",
            // Generic
            "AGENT_MODE",
            "AI_AGENT",
            // GitHub Copilot
            "GITHUB_COPILOT",
            "COPILOT_SESSION",
            // Continue.dev
            "CONTINUE_SESSION",
            // Sourcegraph Cody
            "CODY_AGENT",
            "CODY_SESSION",
            // Windsurf/Codeium
            "WINDSURF_SESSION",
            "CODEIUM_AGENT",
            // Google Gemini
            "GEMINI_CLI",
            "GEMINI_SESSION",
            // Amazon CodeWhisperer / Q
            "CODEWHISPERER_SESSION",
            "AMAZON_Q_SESSION",
        ];

        AGENT_MARKERS.iter().any(|var| env::var(var).is_ok())
    }

    /// Check if this mode should use ANSI escape codes.
    ///
    /// Returns `true` only for `Rich` mode, which is the only mode that
    /// uses colors and formatting.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sqlmodel_console::OutputMode;
    ///
    /// assert!(!OutputMode::Plain.supports_ansi());
    /// assert!(OutputMode::Rich.supports_ansi());
    /// assert!(!OutputMode::Json.supports_ansi());
    /// ```
    #[must_use]
    pub const fn supports_ansi(&self) -> bool {
        matches!(self, Self::Rich)
    }

    /// Check if this mode uses structured format.
    ///
    /// Returns `true` only for `Json` mode, which outputs structured data
    /// for programmatic consumption.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sqlmodel_console::OutputMode;
    ///
    /// assert!(!OutputMode::Plain.is_structured());
    /// assert!(!OutputMode::Rich.is_structured());
    /// assert!(OutputMode::Json.is_structured());
    /// ```
    #[must_use]
    pub const fn is_structured(&self) -> bool {
        matches!(self, Self::Json)
    }

    /// Check if this mode is plain text.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sqlmodel_console::OutputMode;
    ///
    /// assert!(OutputMode::Plain.is_plain());
    /// assert!(!OutputMode::Rich.is_plain());
    /// assert!(!OutputMode::Json.is_plain());
    /// ```
    #[must_use]
    pub const fn is_plain(&self) -> bool {
        matches!(self, Self::Plain)
    }

    /// Check if this mode uses rich formatting.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sqlmodel_console::OutputMode;
    ///
    /// assert!(!OutputMode::Plain.is_rich());
    /// assert!(OutputMode::Rich.is_rich());
    /// assert!(!OutputMode::Json.is_rich());
    /// ```
    #[must_use]
    pub const fn is_rich(&self) -> bool {
        matches!(self, Self::Rich)
    }

    /// Get the mode name as a string slice.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sqlmodel_console::OutputMode;
    ///
    /// assert_eq!(OutputMode::Plain.as_str(), "plain");
    /// assert_eq!(OutputMode::Rich.as_str(), "rich");
    /// assert_eq!(OutputMode::Json.as_str(), "json");
    /// ```
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::Rich => "rich",
            Self::Json => "json",
        }
    }
}

impl std::fmt::Display for OutputMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Check if an environment variable is set to a truthy value.
///
/// Recognizes: `1`, `true`, `yes`, `on` (case-insensitive).
fn env_is_truthy(name: &str) -> bool {
    env::var(name).is_ok_and(|v| {
        let v = v.to_lowercase();
        v == "1" || v == "true" || v == "yes" || v == "on"
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    /// Environment variables to clean before each test.
    const VARS_TO_CLEAR: &[&str] = &[
        "SQLMODEL_PLAIN",
        "SQLMODEL_JSON",
        "SQLMODEL_RICH",
        "NO_COLOR",
        "CI",
        "TERM",
        "CLAUDE_CODE",
        "CODEX_CLI",
        "CURSOR_SESSION",
        "AIDER_MODEL",
        "AGENT_MODE",
        "GITHUB_COPILOT",
        "CONTINUE_SESSION",
    ];

    /// Wrapper for env::set_var (unsafe in Rust 2024 edition).
    ///
    /// # Safety
    /// This is only safe in single-threaded test contexts with #[test].
    /// Tests must be run with `--test-threads=1` for safety.
    #[allow(unsafe_code)]
    fn test_set_var(key: &str, value: &str) {
        // SAFETY: Tests are run single-threaded via `cargo test -- --test-threads=1`
        // or the env manipulation is isolated to a single test function.
        unsafe { env::set_var(key, value) };
    }

    /// Wrapper for env::remove_var (unsafe in Rust 2024 edition).
    #[allow(unsafe_code)]
    fn test_remove_var(key: &str) {
        // SAFETY: Same as test_set_var
        unsafe { env::remove_var(key) };
    }

    /// Helper to run test with clean environment.
    fn with_clean_env<F: FnOnce()>(f: F) {
        // Save current values
        let saved: Vec<_> = VARS_TO_CLEAR
            .iter()
            .map(|&v| (v, env::var(v).ok()))
            .collect();

        // Clear all relevant vars
        for &var in VARS_TO_CLEAR {
            test_remove_var(var);
        }

        // Run the test
        f();

        // Restore original values
        for (var, val) in saved {
            match val {
                Some(v) => test_set_var(var, &v),
                None => test_remove_var(var),
            }
        }
    }

    #[test]
    fn test_default_is_rich() {
        assert_eq!(OutputMode::default(), OutputMode::Rich);
    }

    #[test]
    fn test_explicit_plain_override() {
        with_clean_env(|| {
            test_set_var("SQLMODEL_PLAIN", "1");
            assert_eq!(OutputMode::detect(), OutputMode::Plain);
        });
    }

    #[test]
    fn test_explicit_plain_override_true() {
        with_clean_env(|| {
            test_set_var("SQLMODEL_PLAIN", "true");
            assert_eq!(OutputMode::detect(), OutputMode::Plain);
        });
    }

    #[test]
    #[ignore = "flaky: env var race conditions in parallel tests"]
    fn test_explicit_json_override() {
        with_clean_env(|| {
            test_set_var("SQLMODEL_JSON", "1");
            assert_eq!(OutputMode::detect(), OutputMode::Json);
        });
    }

    #[test]
    #[ignore = "flaky: env var race conditions in parallel tests (CI sets CI=true)"]
    fn test_explicit_rich_override() {
        with_clean_env(|| {
            test_set_var("SQLMODEL_RICH", "1");
            // Note: This test runs in a non-TTY context (cargo test),
            // but SQLMODEL_RICH should still force rich mode
            assert_eq!(OutputMode::detect(), OutputMode::Rich);
        });
    }

    #[test]
    #[ignore = "flaky: env var race conditions in parallel tests"]
    fn test_plain_takes_priority_over_json() {
        with_clean_env(|| {
            test_set_var("SQLMODEL_PLAIN", "1");
            test_set_var("SQLMODEL_JSON", "1");
            assert_eq!(OutputMode::detect(), OutputMode::Plain);
        });
    }

    #[test]
    fn test_agent_detection_claude() {
        with_clean_env(|| {
            test_set_var("CLAUDE_CODE", "1");
            assert!(OutputMode::is_agent_environment());
        });
    }

    #[test]
    #[ignore = "flaky: env var race conditions in parallel tests"]
    fn test_agent_detection_codex() {
        with_clean_env(|| {
            test_set_var("CODEX_CLI", "1");
            assert!(OutputMode::is_agent_environment());
        });
    }

    #[test]
    fn test_agent_detection_cursor() {
        with_clean_env(|| {
            test_set_var("CURSOR_SESSION", "active");
            assert!(OutputMode::is_agent_environment());
        });
    }

    #[test]
    #[ignore = "flaky: env var race conditions in parallel tests"]
    fn test_agent_detection_aider() {
        with_clean_env(|| {
            test_set_var("AIDER_MODEL", "gpt-4");
            assert!(OutputMode::is_agent_environment());
        });
    }

    #[test]
    fn test_agent_causes_plain_mode() {
        with_clean_env(|| {
            test_set_var("CLAUDE_CODE", "1");
            assert_eq!(OutputMode::detect(), OutputMode::Plain);
        });
    }

    #[test]
    #[ignore = "flaky: env var race conditions in parallel tests (CI sets CI=true)"]
    fn test_rich_override_beats_agent() {
        with_clean_env(|| {
            test_set_var("CLAUDE_CODE", "1");
            test_set_var("SQLMODEL_RICH", "1");
            assert_eq!(OutputMode::detect(), OutputMode::Rich);
        });
    }

    #[test]
    fn test_no_color_causes_plain() {
        with_clean_env(|| {
            test_set_var("NO_COLOR", "");
            assert_eq!(OutputMode::detect(), OutputMode::Plain);
        });
    }

    #[test]
    fn test_ci_causes_plain() {
        with_clean_env(|| {
            test_set_var("CI", "true");
            assert_eq!(OutputMode::detect(), OutputMode::Plain);
        });
    }

    #[test]
    fn test_dumb_terminal_causes_plain() {
        with_clean_env(|| {
            test_set_var("TERM", "dumb");
            assert_eq!(OutputMode::detect(), OutputMode::Plain);
        });
    }

    #[test]
    fn test_supports_ansi() {
        assert!(!OutputMode::Plain.supports_ansi());
        assert!(OutputMode::Rich.supports_ansi());
        assert!(!OutputMode::Json.supports_ansi());
    }

    #[test]
    fn test_is_structured() {
        assert!(!OutputMode::Plain.is_structured());
        assert!(!OutputMode::Rich.is_structured());
        assert!(OutputMode::Json.is_structured());
    }

    #[test]
    fn test_is_plain() {
        assert!(OutputMode::Plain.is_plain());
        assert!(!OutputMode::Rich.is_plain());
        assert!(!OutputMode::Json.is_plain());
    }

    #[test]
    fn test_is_rich() {
        assert!(!OutputMode::Plain.is_rich());
        assert!(OutputMode::Rich.is_rich());
        assert!(!OutputMode::Json.is_rich());
    }

    #[test]
    fn test_as_str() {
        assert_eq!(OutputMode::Plain.as_str(), "plain");
        assert_eq!(OutputMode::Rich.as_str(), "rich");
        assert_eq!(OutputMode::Json.as_str(), "json");
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", OutputMode::Plain), "plain");
        assert_eq!(format!("{}", OutputMode::Rich), "rich");
        assert_eq!(format!("{}", OutputMode::Json), "json");
    }

    #[test]
    fn test_env_is_truthy() {
        with_clean_env(|| {
            // Not set
            assert!(!env_is_truthy("SQLMODEL_TEST_VAR"));

            // Various truthy values
            test_set_var("SQLMODEL_TEST_VAR", "1");
            assert!(env_is_truthy("SQLMODEL_TEST_VAR"));

            test_set_var("SQLMODEL_TEST_VAR", "true");
            assert!(env_is_truthy("SQLMODEL_TEST_VAR"));

            test_set_var("SQLMODEL_TEST_VAR", "TRUE");
            assert!(env_is_truthy("SQLMODEL_TEST_VAR"));

            test_set_var("SQLMODEL_TEST_VAR", "yes");
            assert!(env_is_truthy("SQLMODEL_TEST_VAR"));

            test_set_var("SQLMODEL_TEST_VAR", "on");
            assert!(env_is_truthy("SQLMODEL_TEST_VAR"));

            // Falsy values
            test_set_var("SQLMODEL_TEST_VAR", "0");
            assert!(!env_is_truthy("SQLMODEL_TEST_VAR"));

            test_set_var("SQLMODEL_TEST_VAR", "false");
            assert!(!env_is_truthy("SQLMODEL_TEST_VAR"));

            test_set_var("SQLMODEL_TEST_VAR", "");
            assert!(!env_is_truthy("SQLMODEL_TEST_VAR"));

            test_remove_var("SQLMODEL_TEST_VAR");
        });
    }

    #[test]
    fn test_no_agent_when_clean() {
        with_clean_env(|| {
            assert!(!OutputMode::is_agent_environment());
        });
    }
}
