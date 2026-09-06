//! E2E tests for output mode detection and switching.
//!
//! These tests verify that the console correctly:
//! - Detects agent environments
//! - Switches between Plain/Rich/JSON modes
//! - Respects environment variable overrides
//! - Produces appropriate output for each mode
//!
//! # Important
//!
//! These tests manipulate environment variables and should be run
//! with `--test-threads=1` for safety.

use super::output_capture::CapturedOutput;
use sqlmodel_console::{OutputMode, SqlModelConsole};
use std::collections::HashMap;

// ============================================================================
// Environment Variable Mock Helper
// ============================================================================

#[derive(Default, Clone)]
struct MockEnv {
    vars: HashMap<&'static str, &'static str>,
}

impl MockEnv {
    fn new() -> Self {
        Self::default()
    }

    fn with(key: &'static str, value: &'static str) -> Self {
        let mut env = Self::new();
        env.set(key, value);
        env
    }

    fn set(&mut self, key: &'static str, value: &'static str) -> &mut Self {
        self.vars.insert(key, value);
        self
    }

    fn lookup(&self, key: &str) -> Option<String> {
        self.vars.get(key).map(|v| (*v).to_string())
    }

    fn console(&self) -> SqlModelConsole {
        SqlModelConsole::with_env(|k| self.lookup(k), true)
    }

    fn is_agent(&self) -> bool {
        OutputMode::is_agent_environment_with(|k| self.lookup(k))
    }
}

// ============================================================================
// E2E Mode Detection Tests
// ============================================================================

/// E2E test: Plain mode produces no ANSI escape codes.
#[test]
fn e2e_plain_mode_produces_no_ansi() {
    let env = MockEnv::with("SQLMODEL_PLAIN", "1");
    let console = env.console();
    assert!(console.is_plain(), "Console should be in plain mode");
    assert!(
        !console.mode().supports_ansi(),
        "Plain mode should not support ANSI"
    );

    // Simulate output that would be captured
    let output = CapturedOutput::from_strings(
        "Query completed: 5 rows returned".to_string(),
        "Processing...".to_string(),
    );

    output.assert_plain_mode_clean();
    output.assert_stderr_plain();
}

/// E2E test: Agent detection triggers plain mode.
#[test]
fn e2e_agent_detection_triggers_plain_mode() {
    let agents = [
        ("CLAUDE_CODE", "1"),
        ("CODEX_CLI", "1"),
        ("CURSOR_SESSION", "test-session"),
        ("AIDER_MODEL", "gpt-4"),
        ("GITHUB_COPILOT", "1"),
        ("GEMINI_CLI", "1"),
    ];

    for (var, value) in agents {
        let env = MockEnv::with(var, value);
        let console = env.console();
        assert!(
            console.is_plain(),
            "{var}={value} should trigger plain mode, got {:?}",
            console.mode()
        );
    }
}

/// E2E test: Force rich mode even in agent environment.
#[test]
fn e2e_force_rich_overrides_agent() {
    let mut env = MockEnv::new();
    env.set("CLAUDE_CODE", "1");
    env.set("SQLMODEL_RICH", "1");

    let console = env.console();
    assert!(
        console.is_rich(),
        "SQLMODEL_RICH should override agent detection"
    );
}

/// E2E test: JSON mode for structured output.
#[test]
#[allow(clippy::items_after_statements)]
fn e2e_json_mode_for_structured_output() {
    let env = MockEnv::with("SQLMODEL_JSON", "1");
    let console = env.console();
    assert!(console.is_json(), "Console should be in JSON mode");
    assert!(
        console.mode().is_structured(),
        "JSON mode should be structured"
    );

    // Verify JSON output works
    #[derive(serde::Serialize)]
    struct TestData {
        query: String,
        rows: usize,
    }

    let data = TestData {
        query: "SELECT * FROM users".to_string(),
        rows: 42,
    };

    let result = console.print_json(&data);
    assert!(result.is_ok(), "JSON serialization should succeed");
}

/// E2E test: CI environment triggers plain mode.
#[test]
fn e2e_ci_environment_triggers_plain() {
    let env = MockEnv::with("CI", "true");
    let console = env.console();
    assert!(console.is_plain(), "CI=true should trigger plain mode");
}

/// E2E test: NO_COLOR standard convention.
#[test]
fn e2e_no_color_triggers_plain() {
    let env = MockEnv::with("NO_COLOR", "");
    let console = env.console();
    assert!(
        console.is_plain(),
        "NO_COLOR presence should trigger plain mode"
    );
}

/// E2E test: Dumb terminal triggers plain mode.
#[test]
fn e2e_dumb_terminal_triggers_plain() {
    let env = MockEnv::with("TERM", "dumb");
    let console = env.console();
    assert!(console.is_plain(), "TERM=dumb should trigger plain mode");
}

// ============================================================================
// Mode Switching Tests
// ============================================================================

/// E2E test: Dynamic mode switching.
#[test]
fn e2e_dynamic_mode_switching() {
    let mut console = SqlModelConsole::with_mode(OutputMode::Plain);
    assert!(console.is_plain());

    // Switch to Rich
    console.set_mode(OutputMode::Rich);
    assert!(console.is_rich());
    assert!(console.mode().supports_ansi());

    // Switch to JSON
    console.set_mode(OutputMode::Json);
    assert!(console.is_json());
    assert!(!console.mode().supports_ansi());

    // Back to Plain
    console.set_mode(OutputMode::Plain);
    assert!(console.is_plain());
}

/// E2E test: Mode predicates are mutually exclusive.
#[test]
fn e2e_mode_predicates_mutually_exclusive() {
    for mode in [OutputMode::Plain, OutputMode::Rich, OutputMode::Json] {
        let console = SqlModelConsole::with_mode(mode);

        let predicates = [console.is_plain(), console.is_rich(), console.is_json()];
        let true_count = predicates.iter().filter(|&&p| p).count();

        assert_eq!(
            true_count, 1,
            "Exactly one predicate should be true for mode {mode:?}"
        );
    }
}

// ============================================================================
// Output Format Tests
// ============================================================================

/// E2E test: Plain mode output is parseable.
#[test]
fn e2e_plain_output_parseable() {
    let console = SqlModelConsole::with_mode(OutputMode::Plain);

    // Mode string is simple ASCII
    assert!(console.mode().as_str().is_ascii());
    assert_eq!(console.mode().as_str(), "plain");
}

/// E2E test: Rich mode string is correct.
#[test]
fn e2e_rich_mode_string() {
    let console = SqlModelConsole::with_mode(OutputMode::Rich);

    assert_eq!(console.mode().as_str(), "rich");
    assert!(console.mode().supports_ansi());
}

/// E2E test: JSON mode string is correct.
#[test]
fn e2e_json_mode_string() {
    let console = SqlModelConsole::with_mode(OutputMode::Json);

    assert_eq!(console.mode().as_str(), "json");
    assert!(console.mode().is_structured());
}

// ============================================================================
// Builder Pattern Tests
// ============================================================================

/// E2E test: Console builder methods work correctly.
#[test]
fn e2e_console_builder_methods() {
    let console = SqlModelConsole::new().plain_width(120);
    assert_eq!(console.get_plain_width(), 120);

    let console = SqlModelConsole::with_mode(OutputMode::Rich).plain_width(100);
    assert!(console.is_rich());
    assert_eq!(console.get_plain_width(), 100);
}

/// E2E test: Console default equals new.
#[test]
fn e2e_console_default_equals_new() {
    let c1 = SqlModelConsole::default();
    let c2 = SqlModelConsole::new();

    assert_eq!(c1.mode(), c2.mode());
    assert_eq!(c1.get_plain_width(), c2.get_plain_width());
}

// ============================================================================
// Combined Scenarios
// ============================================================================

/// E2E test: Full workflow with mode detection.
#[test]
fn e2e_full_mode_detection_workflow() {
    let mut env = MockEnv::new();

    // Start clean - no agent vars
    assert!(!env.is_agent());

    // Simulate agent environment
    env.set("CLAUDE_CODE", "1");
    assert!(env.is_agent());

    // Create console - should detect agent
    let console = env.console();
    assert!(console.is_plain());

    // Force override to rich
    env.set("SQLMODEL_RICH", "1");
    let console2 = env.console();
    assert!(console2.is_rich());

    // Plain override takes precedence
    env.set("SQLMODEL_PLAIN", "1");
    let console3 = env.console();
    assert!(console3.is_plain());
}

/// E2E test: Multiple agent markers don't conflict.
#[test]
fn e2e_multiple_agent_markers() {
    let mut env = MockEnv::new();

    env.set("CLAUDE_CODE", "1");
    env.set("CODEX_CLI", "1");
    env.set("CURSOR_SESSION", "test");
    env.set("GEMINI_CLI", "1");

    // Should still detect as agent environment
    assert!(env.is_agent());

    // Should use plain mode
    let console = env.console();
    assert!(console.is_plain());
}
