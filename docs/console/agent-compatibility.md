# Agent Compatibility Guide

This guide explains how SQLModel Rust's console system maintains compatibility with AI coding agents (Claude Code, Codex CLI, Cursor, Aider, and others) while providing rich output for human users.

---

## Why Agent Compatibility Matters

AI coding agents are primary users of SQLModel Rust. When agents parse your program's output:

- **Rich formatting breaks parsing**: ANSI escape codes, box-drawing characters, and color codes appear as garbage in agent context
- **Stream mixing confuses agents**: Agents expect structured data on stdout, not mixed with progress indicators
- **Detection prevents issues**: Automatic agent detection ensures clean output without manual configuration

SQLModel Rust solves this with a **dual-mode output system** that automatically adapts to the environment.

---

## How Auto-Detection Works

The `OutputMode::detect()` function checks the environment in this priority order (first match wins):

### Priority 1: Explicit Overrides

| Environment Variable | Effect |
|---------------------|--------|
| `SQLMODEL_PLAIN=1` | Force plain text mode |
| `SQLMODEL_JSON=1` | Force JSON output mode |
| `SQLMODEL_RICH=1` | Force rich mode (even for agents!) |

### Priority 2: Standard Conventions

| Condition | Result |
|-----------|--------|
| `NO_COLOR` present | Plain mode ([no-color.org](https://no-color.org/)) |
| `CI=true` | Plain mode |
| `TERM=dumb` | Plain mode |

### Priority 3: Agent Environment Detection

The following AI coding agents are automatically detected:

| Agent | Environment Variable(s) |
|-------|------------------------|
| Claude Code | `CLAUDE_CODE` |
| OpenAI Codex CLI | `CODEX_CLI`, `CODEX_SESSION` |
| Cursor IDE | `CURSOR_SESSION`, `CURSOR_EDITOR` |
| Aider | `AIDER_MODEL`, `AIDER_REPO` |
| GitHub Copilot | `GITHUB_COPILOT`, `COPILOT_SESSION` |
| Continue.dev | `CONTINUE_SESSION` |
| Sourcegraph Cody | `CODY_AGENT`, `CODY_SESSION` |
| Windsurf/Codeium | `WINDSURF_SESSION`, `CODEIUM_AGENT` |
| Google Gemini | `GEMINI_CLI`, `GEMINI_SESSION` |
| Amazon Q/CodeWhisperer | `AMAZON_Q_SESSION`, `CODEWHISPERER_SESSION` |
| Generic Marker | `AGENT_MODE`, `AI_AGENT` |

If any of these variables exist (regardless of value), plain mode is enabled.

### Priority 4: Terminal Detection

| Condition | Result |
|-----------|--------|
| stdout is not a TTY (piped/redirected) | Plain mode |
| Otherwise | **Rich mode** (default for humans) |

---

## Stream Separation Contract

SQLModel Rust separates output into two streams for maximum compatibility:

### stdout (Semantic Data)

Use stdout for data that agents need to parse:

- Query results (rows, columns, values)
- Success/failure status codes
- Structured data (JSON when requested)
- Return values from operations

```rust
console.print("id|name|email");           // stdout
console.print("1|Alice|alice@example.com"); // stdout
console.print_json(&results)?;             // stdout
```

### stderr (Decorative/Informational)

Use stderr for human feedback that agents should ignore:

- Progress indicators
- Connection status messages
- Timing information
- Formatted error displays (visual version)
- Status messages

```rust
console.status("Connecting...");  // stderr
console.success("Done!");         // stderr
console.error("Failed!");         // stderr
console.rule(Some("Query"));      // stderr
```

This separation means agents can safely redirect stdout to capture data while ignoring stderr decoration.

---

## Plain Text Output Format

When in plain mode, output follows predictable, parseable formats.

### Query Results

```
# 3 rows in 12.34ms
id|name|email
1|Alice|alice@example.com
2|Bob|bob@example.com
3|Carol|carol@example.com
```

Parse pattern: First line optionally starts with `#` (metadata), second line is header, remaining lines are data.

### Errors

```
ERROR: Connection timeout (SQLMODEL-E001)
  Host: localhost:5432
  Timeout: 30s
```

Parse pattern: Line starting with `ERROR:` followed by message and optional indented details.

### Progress (stderr only)

```
Inserting rows: 50% (500/1000) 123 rows/s
```

---

## Environment Variables Reference

### Output Mode Control

| Variable | Values | Effect |
|----------|--------|--------|
| `SQLMODEL_PLAIN` | `1`, `true`, `yes`, `on` | Force plain text mode |
| `SQLMODEL_RICH` | `1`, `true`, `yes`, `on` | Force rich mode (overrides agent detection!) |
| `SQLMODEL_JSON` | `1`, `true`, `yes`, `on` | Force JSON output mode |
| `NO_COLOR` | (any value) | Disable colors (standard convention) |

### CI/Terminal Detection

| Variable | Values | Effect |
|----------|--------|--------|
| `CI` | `true` | Treated as CI environment → plain mode |
| `TERM` | `dumb` | Dumb terminal → plain mode |

### Agent Markers (Auto-Detected)

Any of these will trigger plain mode:

```
CLAUDE_CODE, CODEX_CLI, CODEX_SESSION, CURSOR_SESSION, CURSOR_EDITOR,
AIDER_MODEL, AIDER_REPO, AGENT_MODE, AI_AGENT, GITHUB_COPILOT,
COPILOT_SESSION, CONTINUE_SESSION, CODY_AGENT, CODY_SESSION,
WINDSURF_SESSION, CODEIUM_AGENT, GEMINI_CLI, GEMINI_SESSION,
AMAZON_Q_SESSION, CODEWHISPERER_SESSION
```

---

## Testing Agent Compatibility

### Simulate Agent Mode

```bash
# Run as if under Claude Code
CLAUDE_CODE=1 cargo run --example query_results

# Run in plain mode explicitly
SQLMODEL_PLAIN=1 cargo run --example console_demo

# Verify no ANSI escape codes in output
SQLMODEL_PLAIN=1 cargo run --example query_results 2>&1 | cat -v
# Output should contain no ^[ sequences
```

### Verify Stream Separation

```bash
# Capture only stdout (data)
SQLMODEL_PLAIN=1 cargo run --example query_results > output.txt

# Check stderr separately
SQLMODEL_PLAIN=1 cargo run --example query_results 2> status.txt
```

### Programmatic Mode Detection

```rust
use sqlmodel_console::OutputMode;

fn main() {
    // Check if we're in an agent environment
    if OutputMode::is_agent_environment() {
        println!("Running under an AI agent");
    }

    // Get detected mode
    let mode = OutputMode::detect();
    println!("Mode: {}", mode); // "plain", "rich", or "json"

    // Check mode properties
    println!("Supports ANSI: {}", mode.supports_ansi());
    println!("Is structured: {}", mode.is_structured());
}
```

---

## For Agent Authors

If you're building an AI coding agent that uses SQLModel Rust:

### Recommended Detection Setup

1. **Set a unique environment variable** when your agent spawns processes:
   ```rust
   std::env::set_var("MY_AGENT_SESSION", "active");
   ```

2. **Register your marker with us** by opening an issue at the repository. We'll add it to the detection list.

3. **Document for your users** that they can force modes via `SQLMODEL_PLAIN=1` or `SQLMODEL_RICH=1`.

### Parsing Recommendations

1. **Read stdout for data** - This contains query results, JSON output, and return values
2. **Optionally display stderr to users** - Contains progress and status for human observation
3. **Handle both modes gracefully** - Your agent might run in environments where detection doesn't work
4. **Use JSON mode for structured parsing**:
   ```bash
   SQLMODEL_JSON=1 your-command
   ```

### Output Format Parsing

For pipe-delimited output (default plain format):

```
header1|header2|header3
value1|value2|value3
value4|value5|value6
```

For CSV output:

```rust
let table = QueryResultTable::new()
    .columns(vec!["id", "name"])
    .row(vec!["1", "Alice"])
    .plain_format(PlainFormat::Csv);
```

For JSON output:

```rust
let table = QueryResultTable::new()
    .columns(vec!["id", "name"])
    .row(vec!["1", "Alice"])
    .plain_format(PlainFormat::JsonArray);
```

---

## For Contributors

If you're contributing to SQLModel Rust, follow these rules to maintain agent compatibility:

### Golden Rules

1. **Never put semantic data in stderr** - Only decorative/status information
2. **Plain mode must have zero ANSI codes** - Test with `cat -v`
3. **All new features need plain output** - Every renderable must implement `render_plain()`
4. **Test in CI environment** - Our CI runs in non-TTY context

### Testing Checklist

```bash
# Run unit tests (includes agent detection tests)
cargo test -p sqlmodel-console -- --test-threads=1

# Verify no ANSI in plain mode
SQLMODEL_PLAIN=1 cargo run -p sqlmodel-console --example console_demo 2>&1 | grep -P '\x1b\[' && echo "FAIL: ANSI codes found" || echo "PASS: No ANSI codes"

# Test specific agent detection
CLAUDE_CODE=1 cargo test -p sqlmodel-console -- output_mode --nocapture
```

### Adding New Agent Detection

To add detection for a new AI coding agent:

1. Add the environment variable to `AGENT_MARKERS` in `crates/sqlmodel-console/src/mode.rs`:
   ```rust
   const AGENT_MARKERS: &[&str] = &[
       // ... existing markers
       "MY_NEW_AGENT",
   ];
   ```

2. Add a test case:
   ```rust
   #[test]
   fn test_agent_detection_my_agent() {
       with_clean_env(|| {
           test_set_var("MY_NEW_AGENT", "1");
           assert!(OutputMode::is_agent_environment());
       });
   }
   ```

3. Update this documentation with the new agent.

---

## Troubleshooting

### "Output has garbage characters"

You're seeing ANSI escape codes. The agent environment wasn't detected.

**Fix**: Set `SQLMODEL_PLAIN=1` in your environment.

### "Agent isn't parsing output correctly"

Your agent might be reading stderr mixed with stdout.

**Fix**: Redirect stderr separately:
```bash
your-command 2>/dev/null  # Discard stderr
your-command 2>status.txt  # Capture separately
```

### "I want rich output but I'm running under an agent"

Use the override:
```bash
SQLMODEL_RICH=1 your-command
```

### "My custom agent isn't detected"

1. Check that your agent sets an environment variable
2. Use `AGENT_MODE=1` as a generic marker
3. Or set `SQLMODEL_PLAIN=1` explicitly
4. Consider opening an issue to add your agent's marker

---

## API Reference

### `OutputMode` enum

```rust
pub enum OutputMode {
    Plain,  // No ANSI codes, machine-parseable
    Rich,   // Colors, tables, panels (default for humans)
    Json,   // Structured JSON output
}

impl OutputMode {
    pub fn detect() -> Self;
    pub fn is_agent_environment() -> bool;
    pub fn supports_ansi(&self) -> bool;
    pub fn is_structured(&self) -> bool;
    pub fn is_plain(&self) -> bool;
    pub fn is_rich(&self) -> bool;
    pub fn as_str(&self) -> &'static str;
}
```

### `SqlModelConsole` output methods

```rust
impl SqlModelConsole {
    // stdout methods (for data)
    pub fn print(&self, message: &str);
    pub fn print_raw(&self, message: &str);
    pub fn print_json<T: Serialize>(&self, value: &T) -> Result<...>;
    pub fn print_json_pretty<T: Serialize>(&self, value: &T) -> Result<...>;

    // stderr methods (for status)
    pub fn status(&self, message: &str);
    pub fn success(&self, message: &str);
    pub fn error(&self, message: &str);
    pub fn warning(&self, message: &str);
    pub fn info(&self, message: &str);
    pub fn rule(&self, title: Option<&str>);
}
```
