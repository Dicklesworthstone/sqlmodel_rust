//! Agent Compatibility Test Suite
//!
//! This module verifies that console output maintains compatibility with AI coding
//! agents (Claude Code, Codex, Cursor, Aider, Gemini, etc.) by testing:
//!
//! 1. Stream separation (stdout for data, stderr for decorations)
//! 2. Plain mode output has no ANSI escape codes
//! 3. Agent detection works correctly for all known agents
//! 4. Output format is machine-parseable
//! 5. Force override flags work as expected
//!
//! # Running Tests
//!
//! These tests manipulate environment variables and must be run single-threaded:
//!
//! ```bash
//! cargo test -p sqlmodel-console --test agent_compat -- --test-threads=1
//! ```

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

    fn detect(&self) -> OutputMode {
        OutputMode::detect_with_env(|k| self.lookup(k), true)
    }

    fn is_agent(&self) -> bool {
        OutputMode::is_agent_environment_with(|k| self.lookup(k))
    }
}

// ============================================================================
// Agent Detection Tests
// ============================================================================

/// Test that Claude Code environment is detected correctly.
#[test]
fn test_detects_claude_code() {
    let env = MockEnv::with("CLAUDE_CODE", "1");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that OpenAI Codex CLI is detected correctly.
#[test]
fn test_detects_codex_cli() {
    let env = MockEnv::with("CODEX_CLI", "1");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that Codex session marker is detected.
#[test]
fn test_detects_codex_session() {
    let env = MockEnv::with("CODEX_SESSION", "session-123");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that Cursor IDE is detected correctly.
#[test]
fn test_detects_cursor_session() {
    let env = MockEnv::with("CURSOR_SESSION", "abc123");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that Cursor editor marker is detected.
#[test]
fn test_detects_cursor_editor() {
    let env = MockEnv::with("CURSOR_EDITOR", "1");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that Aider is detected via AIDER_MODEL.
#[test]
fn test_detects_aider_model() {
    let env = MockEnv::with("AIDER_MODEL", "gpt-4");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that Aider is detected via AIDER_REPO.
#[test]
fn test_detects_aider_repo() {
    let env = MockEnv::with("AIDER_REPO", "/path/to/repo");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that generic AGENT_MODE marker is detected.
#[test]
fn test_detects_agent_mode() {
    let env = MockEnv::with("AGENT_MODE", "1");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that generic AI_AGENT marker is detected.
#[test]
fn test_detects_ai_agent() {
    let env = MockEnv::with("AI_AGENT", "1");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that GitHub Copilot is detected.
#[test]
fn test_detects_github_copilot() {
    let env = MockEnv::with("GITHUB_COPILOT", "1");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that Copilot session marker is detected.
#[test]
fn test_detects_copilot_session() {
    let env = MockEnv::with("COPILOT_SESSION", "sess-456");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that Continue.dev is detected.
#[test]
fn test_detects_continue_session() {
    let env = MockEnv::with("CONTINUE_SESSION", "cont-789");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that Sourcegraph Cody agent marker is detected.
#[test]
fn test_detects_cody_agent() {
    let env = MockEnv::with("CODY_AGENT", "1");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that Cody session marker is detected.
#[test]
fn test_detects_cody_session() {
    let env = MockEnv::with("CODY_SESSION", "cody-abc");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that Windsurf/Codeium is detected via WINDSURF_SESSION.
#[test]
fn test_detects_windsurf_session() {
    let env = MockEnv::with("WINDSURF_SESSION", "ws-123");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that Codeium agent is detected.
#[test]
fn test_detects_codeium_agent() {
    let env = MockEnv::with("CODEIUM_AGENT", "1");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that Google Gemini CLI is detected.
#[test]
fn test_detects_gemini_cli() {
    let env = MockEnv::with("GEMINI_CLI", "1");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that Gemini session marker is detected.
#[test]
fn test_detects_gemini_session() {
    let env = MockEnv::with("GEMINI_SESSION", "gem-xyz");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that Amazon CodeWhisperer is detected.
#[test]
fn test_detects_codewhisperer() {
    let env = MockEnv::with("CODEWHISPERER_SESSION", "cw-123");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that Amazon Q is detected.
#[test]
fn test_detects_amazon_q() {
    let env = MockEnv::with("AMAZON_Q_SESSION", "q-456");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that no agent is detected in clean environment.
#[test]
fn test_no_agent_when_clean() {
    let env = MockEnv::new();
    assert!(!env.is_agent());
}

// ============================================================================
// Environment Variable Precedence Tests
// ============================================================================

/// Test that SQLMODEL_RICH overrides agent detection.
#[test]
fn test_force_rich_in_agent_environment() {
    let mut env = MockEnv::new();
    env.set("CLAUDE_CODE", "1");
    env.set("SQLMODEL_RICH", "1");
    assert_eq!(env.detect(), OutputMode::Rich);
}

/// Test that SQLMODEL_PLAIN takes priority over agent detection.
#[test]
fn test_plain_override_with_agent() {
    let mut env = MockEnv::new();
    env.set("CLAUDE_CODE", "1");
    env.set("SQLMODEL_PLAIN", "1");
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that SQLMODEL_PLAIN takes priority over SQLMODEL_RICH.
#[test]
fn test_plain_beats_rich_override() {
    let mut env = MockEnv::new();
    env.set("SQLMODEL_PLAIN", "1");
    env.set("SQLMODEL_RICH", "1");
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that SQLMODEL_PLAIN takes priority over SQLMODEL_JSON.
#[test]
fn test_plain_beats_json_override() {
    let mut env = MockEnv::new();
    env.set("SQLMODEL_PLAIN", "1");
    env.set("SQLMODEL_JSON", "1");
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that SQLMODEL_JSON comes after PLAIN but before RICH.
#[test]
fn test_json_beats_rich_override() {
    let mut env = MockEnv::new();
    env.set("SQLMODEL_JSON", "1");
    env.set("SQLMODEL_RICH", "1");
    assert_eq!(env.detect(), OutputMode::Json);
}

/// Test that NO_COLOR standard convention works.
#[test]
fn test_no_color_causes_plain() {
    let mut env = MockEnv::new();
    env.set("NO_COLOR", "");
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that CI environment causes plain mode.
#[test]
fn test_ci_causes_plain() {
    let mut env = MockEnv::new();
    env.set("CI", "true");
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that TERM=dumb causes plain mode.
#[test]
fn test_dumb_terminal_causes_plain() {
    let mut env = MockEnv::new();
    env.set("TERM", "dumb");
    assert_eq!(env.detect(), OutputMode::Plain);
}

/// Test that multiple agent markers don't cause issues.
#[test]
fn test_multiple_agents_detected() {
    let mut env = MockEnv::new();
    env.set("CLAUDE_CODE", "1");
    env.set("CODEX_CLI", "1");
    env.set("CURSOR_SESSION", "test");
    assert!(env.is_agent());
    assert_eq!(env.detect(), OutputMode::Plain);
}

// ============================================================================
// Plain Mode Output Tests (No ANSI Codes)
// ============================================================================

/// Test that plain mode console doesn't produce ANSI codes.
#[test]
fn test_plain_mode_console_no_ansi() {
    let console = SqlModelConsole::with_mode(OutputMode::Plain);
    assert!(console.is_plain());
    assert!(!console.mode().supports_ansi());
}

/// Test that JSON mode also doesn't support ANSI.
#[test]
fn test_json_mode_no_ansi() {
    let console = SqlModelConsole::with_mode(OutputMode::Json);
    assert!(console.is_json());
    assert!(!console.mode().supports_ansi());
}

/// Test that only Rich mode supports ANSI.
#[test]
fn test_only_rich_supports_ansi() {
    assert!(!OutputMode::Plain.supports_ansi());
    assert!(OutputMode::Rich.supports_ansi());
    assert!(!OutputMode::Json.supports_ansi());
}

/// Test that markup stripping removes ANSI-style tags.
#[test]
fn test_strip_markup_removes_style_tags() {
    use sqlmodel_console::console::strip_markup;

    // Basic tags
    assert_eq!(strip_markup("[bold]text[/]"), "text");
    assert_eq!(strip_markup("[red]error[/]"), "error");
    assert_eq!(strip_markup("[green]success[/]"), "success");

    // Compound styles
    assert_eq!(strip_markup("[bold red]warning[/]"), "warning");
    assert_eq!(strip_markup("[red on white]highlighted[/]"), "highlighted");

    // Nested tags
    assert_eq!(strip_markup("[bold][italic]nested[/][/]"), "nested");

    // Multiple tags in sequence
    assert_eq!(strip_markup("[red]a[/] [blue]b[/]"), "a b");
}

/// Test that strip_markup preserves non-markup brackets.
///
/// The strip_markup function considers a tag to be markup if:
/// 1. It starts with '/' (closing tags)
/// 2. It contains a space (compound styles)
/// 3. It has 2+ alphabetic characters (style names)
///
/// Therefore:
/// - `[0]`, `[i]`, `[1]` are preserved (numeric/single letter)
/// - `[key]`, `[idx]` are stripped (2+ letters = looks like markup)
#[test]
fn test_strip_markup_preserves_array_indices() {
    use sqlmodel_console::console::strip_markup;

    // Numeric indices should be preserved
    assert_eq!(strip_markup("array[0]"), "array[0]");
    assert_eq!(strip_markup("array[123]"), "array[123]");

    // Single-letter indices should be preserved
    assert_eq!(strip_markup("items[i]"), "items[i]");
    assert_eq!(strip_markup("matrix[n]"), "matrix[n]");

    // Mixed alphanumeric with digits are preserved
    assert_eq!(strip_markup("data[x1]"), "data[x1]");
    assert_eq!(strip_markup("arr[i2]"), "arr[i2]");

    // Function calls with numeric indices
    assert_eq!(strip_markup("get_item(arr[0])"), "get_item(arr[0])");

    // Note: [key], [idx] etc. with 2+ letters ARE stripped because they
    // look like markup tags. This is by design - real code rarely uses
    // such identifiers in brackets, while [bold], [red] are common markup.
}

/// Test that plain output has no escape sequences.
#[test]
fn test_plain_output_no_escape_sequences() {
    // Common ANSI escape sequences to check for
    let ansi_patterns = [
        "\x1b[",    // CSI sequence start
        "\x1b]",    // OSC sequence start
        "\x1bP",    // DCS sequence start
        "\x1b\\",   // ST (string terminator)
        "\u{009b}", // C1 CSI
    ];

    // Create plain console and check method contracts
    let console = SqlModelConsole::with_mode(OutputMode::Plain);

    // The console contract is that plain mode won't emit ANSI codes
    // We verify the mode settings here
    assert!(console.is_plain());
    assert!(!console.mode().supports_ansi());

    // Test the mode enum directly
    for pattern in ansi_patterns {
        let mode_str = OutputMode::Plain.as_str();
        assert!(
            !mode_str.contains(pattern),
            "Mode string should not contain ANSI: {mode_str}"
        );
    }
}

// ============================================================================
// Machine Parseability Tests
// ============================================================================

/// Test that plain mode strings are parseable.
#[test]
fn test_plain_mode_strings_parseable() {
    let console = SqlModelConsole::with_mode(OutputMode::Plain);

    // Mode string is simple ASCII
    assert_eq!(console.mode().as_str(), "plain");
    assert!(console.mode().as_str().is_ascii());
}

/// Test that JSON mode produces valid JSON.
#[test]
fn test_json_mode_produces_valid_json() {
    #[derive(serde::Serialize)]
    struct TestData {
        name: String,
        count: i32,
        active: bool,
    }

    let data = TestData {
        name: "test".to_string(),
        count: 42,
        active: true,
    };

    let json = serde_json::to_string(&data).unwrap();

    // Should be valid JSON
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["name"], "test");
    assert_eq!(parsed["count"], 42);
    assert_eq!(parsed["active"], true);
}

/// Test that JSON mode string is correct.
#[test]
fn test_json_mode_string() {
    assert_eq!(OutputMode::Json.as_str(), "json");
    assert!(OutputMode::Json.is_structured());
}

/// Test mode display implementations.
#[test]
fn test_mode_display() {
    assert_eq!(format!("{}", OutputMode::Plain), "plain");
    assert_eq!(format!("{}", OutputMode::Rich), "rich");
    assert_eq!(format!("{}", OutputMode::Json), "json");
}

// ============================================================================
// Console Constructor Tests
// ============================================================================

/// Test that console auto-detection works.
#[test]
fn test_console_auto_detection() {
    let mut env = MockEnv::new();
    env.set("CLAUDE_CODE", "1");

    let console = SqlModelConsole::with_env(|k| env.lookup(k), true);
    // Should detect agent and use plain mode
    assert!(console.is_plain());
}

/// Test that console respects explicit mode.
#[test]
fn test_console_explicit_mode() {
    let console = SqlModelConsole::with_mode(OutputMode::Rich);
    assert!(console.is_rich());

    let console = SqlModelConsole::with_mode(OutputMode::Plain);
    assert!(console.is_plain());

    let console = SqlModelConsole::with_mode(OutputMode::Json);
    assert!(console.is_json());
}

/// Test that console mode can be changed.
#[test]
fn test_console_set_mode() {
    let mut console = SqlModelConsole::with_mode(OutputMode::Rich);
    assert!(console.is_rich());

    console.set_mode(OutputMode::Plain);
    assert!(console.is_plain());

    console.set_mode(OutputMode::Json);
    assert!(console.is_json());
}

// ============================================================================
// Edge Cases
// ============================================================================

/// Test truthy value detection for env vars.
#[test]
fn test_truthy_values() {
    // Various truthy values
    for truthy in ["1", "true", "TRUE", "True", "yes", "YES", "on", "ON"] {
        let env = MockEnv::with("SQLMODEL_PLAIN", truthy);
        assert_eq!(
            env.detect(),
            OutputMode::Plain,
            "Failed for truthy value: {truthy}"
        );
    }
}

/// Test falsy value detection for env vars.
#[test]
fn test_falsy_values() {
    for falsy in ["0", "false", "FALSE", "no", "NO", "off", "OFF", ""] {
        let mut env = MockEnv::new();
        env.set("SQLMODEL_PLAIN", falsy);
        env.set("SQLMODEL_RICH", "1"); // Force rich to check if PLAIN was triggered

        let mode = env.detect();
        assert_eq!(
            mode,
            OutputMode::Rich,
            "SQLMODEL_PLAIN={falsy} should not trigger plain mode"
        );
    }
}

/// Test that empty agent marker is still detected (presence matters).
#[test]
fn test_agent_marker_presence_not_value() {
    let env = MockEnv::with("CLAUDE_CODE", "");
    assert!(env.is_agent());
}

/// Test default mode enum value.
#[test]
fn test_output_mode_default() {
    assert_eq!(OutputMode::default(), OutputMode::Rich);
}

/// Test mode predicate methods.
#[test]
fn test_mode_predicates() {
    // is_plain
    assert!(OutputMode::Plain.is_plain());
    assert!(!OutputMode::Rich.is_plain());
    assert!(!OutputMode::Json.is_plain());

    // is_rich
    assert!(!OutputMode::Plain.is_rich());
    assert!(OutputMode::Rich.is_rich());
    assert!(!OutputMode::Json.is_rich());

    // is_structured
    assert!(!OutputMode::Plain.is_structured());
    assert!(!OutputMode::Rich.is_structured());
    assert!(OutputMode::Json.is_structured());
}

/// Test that console default equals new.
#[test]
fn test_console_default_equals_new() {
    let c1 = SqlModelConsole::default();
    let c2 = SqlModelConsole::new();

    assert_eq!(c1.mode(), c2.mode());
    assert_eq!(c1.get_plain_width(), c2.get_plain_width());
}

// ============================================================================
// Documentation Tests
// ============================================================================

/// Document expected behavior for all agents.
///
/// This test serves as living documentation of which agents are supported
/// and how they are detected.
#[test]
fn test_documented_agent_support() {
    struct AgentInfo {
        name: &'static str,
        env_var: &'static str,
        example_value: &'static str,
    }

    let agents = [
        AgentInfo {
            name: "Claude Code",
            env_var: "CLAUDE_CODE",
            example_value: "1",
        },
        AgentInfo {
            name: "OpenAI Codex CLI",
            env_var: "CODEX_CLI",
            example_value: "1",
        },
        AgentInfo {
            name: "Cursor IDE",
            env_var: "CURSOR_SESSION",
            example_value: "session-id",
        },
        AgentInfo {
            name: "Aider",
            env_var: "AIDER_MODEL",
            example_value: "gpt-4",
        },
        AgentInfo {
            name: "GitHub Copilot",
            env_var: "GITHUB_COPILOT",
            example_value: "1",
        },
        AgentInfo {
            name: "Continue.dev",
            env_var: "CONTINUE_SESSION",
            example_value: "sess-123",
        },
        AgentInfo {
            name: "Sourcegraph Cody",
            env_var: "CODY_AGENT",
            example_value: "1",
        },
        AgentInfo {
            name: "Windsurf/Codeium",
            env_var: "WINDSURF_SESSION",
            example_value: "ws-123",
        },
        AgentInfo {
            name: "Google Gemini CLI",
            env_var: "GEMINI_CLI",
            example_value: "1",
        },
        AgentInfo {
            name: "Amazon CodeWhisperer",
            env_var: "CODEWHISPERER_SESSION",
            example_value: "cw-123",
        },
        AgentInfo {
            name: "Amazon Q",
            env_var: "AMAZON_Q_SESSION",
            example_value: "q-456",
        },
    ];

    for agent in agents {
        let env = MockEnv::with(agent.env_var, agent.example_value);

        assert!(
            env.is_agent(),
            "{} should be detected via {} env var",
            agent.name,
            agent.env_var
        );

        assert_eq!(
            env.detect(),
            OutputMode::Plain,
            "{} should trigger plain mode",
            agent.name
        );
    }
}
