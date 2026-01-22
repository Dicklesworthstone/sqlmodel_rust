# SQLModel Console User Guide

Beautiful terminal output for database operations, with automatic adaptation between rich formatting for humans and plain text for AI coding agents.

---

## Table of Contents

- [Introduction](#introduction)
- [Quick Start](#quick-start)
- [Output Modes](#output-modes)
- [Theme Configuration](#theme-configuration)
- [Renderables Catalog](#renderables-catalog)
- [Integration Patterns](#integration-patterns)
- [Agent Compatibility](#agent-compatibility)
- [Troubleshooting](#troubleshooting)
- [API Reference](#api-reference)

---

## Introduction

### What the Console Provides

SQLModel Console transforms database operation output from plain text into beautiful, informative displays:

- **Query results** as formatted tables with type-based coloring
- **Error messages** as detailed panels with context and suggestions
- **Progress indicators** for long-running operations
- **Schema visualization** as trees and tables
- **SQL syntax highlighting** for query display
- **Connection pool dashboards** with health metrics

### When to Use It

**Use the console when:**
- Humans are watching database operations (development, demos)
- You want rich error messages with full context
- Debugging complex queries with visual output
- Monitoring connection pool health

**Don't use the console when:**
- Running in CI/CD pipelines (auto-detected → plain mode)
- AI agents are parsing output (auto-detected → plain mode)
- You need minimal dependencies
- Output is being piped to files or other programs

### Quick Comparison

**Without console:**
```
id|name|email
1|Alice|alice@example.com
2|Bob|bob@example.com
```

**With console (rich mode):**
```
╭────────────── Query Results ── 2 rows in 1.23ms ──────────────╮
│ id │ name  │ email              │
├────┼───────┼────────────────────┤
│  1 │ Alice │ alice@example.com  │
│  2 │ Bob   │ bob@example.com    │
╰───────────────────────────────────────────────────────────────╯
```

---

## Quick Start

### Add the Dependency

In your `Cargo.toml`:

```toml
[dependencies]
sqlmodel-console = { path = "../crates/sqlmodel-console" }

# Or from crates.io (when published)
# sqlmodel-console = "0.1"
```

### Create a Console

```rust
use sqlmodel_console::{SqlModelConsole, OutputMode};

fn main() {
    // Auto-detect mode (recommended)
    let console = SqlModelConsole::new();

    // Or force a specific mode
    let plain_console = SqlModelConsole::with_mode(OutputMode::Plain);
    let rich_console = SqlModelConsole::with_mode(OutputMode::Rich);
}
```

### Display Query Results

```rust
use sqlmodel_console::SqlModelConsole;
use sqlmodel_console::renderables::QueryResultTable;

fn main() {
    let console = SqlModelConsole::new();

    // Create query results
    let table = QueryResultTable::new()
        .title("Users")
        .columns(vec!["id", "name", "email"])
        .row(vec!["1", "Alice", "alice@example.com"])
        .row(vec!["2", "Bob", "bob@example.com"])
        .timing_ms(1.23);

    // Display in the detected mode
    if console.is_rich() {
        println!("{}", table.render_styled());
    } else {
        println!("{}", table.render_plain());
    }
}
```

### Display Errors

```rust
use sqlmodel_console::SqlModelConsole;
use sqlmodel_console::renderables::{ErrorPanel, ErrorSeverity};

fn main() {
    let console = SqlModelConsole::new();

    let error = ErrorPanel::new(
        "Connection timeout",
        ErrorSeverity::Error
    )
    .detail("Host", "localhost:5432")
    .detail("Timeout", "30 seconds")
    .suggestion("Check if the database server is running")
    .sql("SELECT * FROM users WHERE id = $1");

    // Render based on mode
    if console.is_rich() {
        eprintln!("{}", error.render_styled());
    } else {
        eprintln!("{}", error.render_plain());
    }
}
```

---

## Output Modes

### Three Output Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Rich** | Colors, tables, panels, box-drawing | Interactive human terminals |
| **Plain** | No ANSI codes, machine-parseable | AI agents, CI, piped output |
| **Json** | Structured JSON output | Tool integrations, scripting |

### Auto-Detection

The console automatically detects the appropriate mode:

```rust
use sqlmodel_console::OutputMode;

let mode = OutputMode::detect();

match mode {
    OutputMode::Rich => println!("Interactive terminal detected"),
    OutputMode::Plain => println!("Agent or non-TTY detected"),
    OutputMode::Json => println!("JSON mode requested"),
}
```

### Detection Priority

The detection follows this order (first match wins):

1. `SQLMODEL_PLAIN=1` → Plain mode
2. `SQLMODEL_JSON=1` → JSON mode
3. `SQLMODEL_RICH=1` → Rich mode (overrides agent detection)
4. `NO_COLOR` present → Plain mode
5. `CI=true` → Plain mode
6. `TERM=dumb` → Plain mode
7. AI agent environment detected → Plain mode
8. stdout is not a TTY → Plain mode
9. **Default** → Rich mode

### Environment Variables

| Variable | Values | Effect |
|----------|--------|--------|
| `SQLMODEL_PLAIN` | `1`, `true`, `yes`, `on` | Force plain mode |
| `SQLMODEL_RICH` | `1`, `true`, `yes`, `on` | Force rich mode |
| `SQLMODEL_JSON` | `1`, `true`, `yes`, `on` | Force JSON mode |
| `NO_COLOR` | (any value) | Disable colors |

### Force a Specific Mode

```rust
use sqlmodel_console::{SqlModelConsole, OutputMode};

// Force plain mode (useful for testing)
let console = SqlModelConsole::with_mode(OutputMode::Plain);
assert!(console.is_plain());

// Force rich mode (for demos)
let console = SqlModelConsole::with_mode(OutputMode::Rich);
assert!(console.is_rich());

// Force JSON mode
let console = SqlModelConsole::with_mode(OutputMode::Json);
assert!(console.is_json());
```

### Mode Properties

```rust
use sqlmodel_console::OutputMode;

let mode = OutputMode::detect();

// Check capabilities
if mode.supports_ansi() {
    // Use ANSI color codes
}

if mode.is_structured() {
    // Output JSON
}

// Get mode as string
println!("Mode: {}", mode.as_str()); // "plain", "rich", or "json"
```

---

## Theme Configuration

### Default Themes

SQLModel Console includes two built-in themes:

```rust
use sqlmodel_console::Theme;

// Dark theme (default) - Dracula-inspired colors
let dark = Theme::dark();

// Light theme - Optimized for light backgrounds
let light = Theme::light();
```

### Theme Colors

The dark theme uses the Dracula color palette:

| Color | Purpose | RGB |
|-------|---------|-----|
| Green | Success, strings | `(80, 250, 123)` |
| Red | Errors, operators | `(255, 85, 85)` |
| Yellow | Warnings, booleans | `(241, 250, 140)` |
| Cyan | Info, numbers | `(139, 233, 253)` |
| Magenta | Dates, SQL keywords | `(255, 121, 198)` |
| Purple | JSON, SQL numbers | `(189, 147, 249)` |
| Gray | Borders, comments, dim | `(98, 114, 164)` |

### Custom Themes

Customize any color in the theme:

```rust
use sqlmodel_console::Theme;
use sqlmodel_console::theme::ThemeColor;

// Start from a preset
let mut theme = Theme::dark();

// Customize specific colors
theme.success = ThemeColor::new((0, 255, 0), 46);    // Brighter green
theme.error = ThemeColor::new((255, 0, 0), 196);     // Pure red
theme.header = ThemeColor::new((255, 255, 0), 226);  // Yellow headers
```

### ThemeColor Structure

Each color has RGB and ANSI-256 fallback:

```rust
use sqlmodel_console::theme::ThemeColor;

// Basic color
let red = ThemeColor::new((255, 0, 0), 196);

// Color with plain-mode marker (for NULL values, etc.)
let null_color = ThemeColor::with_marker(
    (128, 128, 128),  // RGB for rich mode
    244,               // ANSI-256 fallback
    "NULL"             // Plain mode marker
);
```

### Using Themes

```rust
use sqlmodel_console::{SqlModelConsole, Theme};
use sqlmodel_console::renderables::QueryResultTable;

// Console with custom theme
let console = SqlModelConsole::with_theme(Theme::light());

// Renderables can also use themes directly
let table = QueryResultTable::new()
    .columns(vec!["name"])
    .row(vec!["Alice"])
    .theme(Theme::dark());
```

### Theme Color Categories

```rust
use sqlmodel_console::Theme;

let theme = Theme::dark();

// Status colors
let _ = theme.success;   // Success messages
let _ = theme.error;     // Error messages
let _ = theme.warning;   // Warnings
let _ = theme.info;      // Informational

// SQL value type colors
let _ = theme.null_value;    // NULL
let _ = theme.bool_value;    // true/false
let _ = theme.number_value;  // 42, 3.14
let _ = theme.string_value;  // "text"
let _ = theme.date_value;    // 2024-01-15
let _ = theme.binary_value;  // [blob]
let _ = theme.json_value;    // {"key": "value"}
let _ = theme.uuid_value;    // 550e8400-...

// SQL syntax colors
let _ = theme.sql_keyword;    // SELECT, FROM
let _ = theme.sql_string;     // 'value'
let _ = theme.sql_number;     // 42
let _ = theme.sql_comment;    // -- comment
let _ = theme.sql_operator;   // =, AND
let _ = theme.sql_identifier; // table_name

// UI element colors
let _ = theme.border;     // Box borders
let _ = theme.header;     // Table headers
let _ = theme.dim;        // Secondary text
let _ = theme.highlight;  // Emphasized text
```

---

## Renderables Catalog

### QueryResultTable

Display query results as formatted tables.

```rust
use sqlmodel_console::renderables::{QueryResultTable, PlainFormat};

let table = QueryResultTable::new()
    .title("Query Results")
    .columns(vec!["id", "name", "email"])
    .row(vec!["1", "Alice", "alice@example.com"])
    .row(vec!["2", "Bob", "bob@example.com"])
    .timing_ms(12.34)
    .max_width(80)
    .max_rows(100)
    .with_row_numbers();

// Styled output (rich mode)
println!("{}", table.render_styled());

// Plain output (default format: pipe-delimited)
println!("{}", table.render_plain());

// Alternative plain formats
println!("{}", table.render_plain_format(PlainFormat::Csv));
println!("{}", table.render_plain_format(PlainFormat::JsonLines));
println!("{}", table.render_plain_format(PlainFormat::JsonArray));
```

**Plain Output Formats:**

| Format | Example |
|--------|---------|
| Pipe (default) | `id\|name\|email` |
| CSV | `id,name,email` |
| JSON Lines | `{"id":1,"name":"Alice"}` |
| JSON Array | `[{"id":1,"name":"Alice"}]` |

### ErrorPanel

Display errors with context and suggestions.

```rust
use sqlmodel_console::renderables::{ErrorPanel, ErrorSeverity};

let error = ErrorPanel::new("Connection failed", ErrorSeverity::Error)
    .detail("Host", "localhost:5432")
    .detail("Database", "myapp")
    .detail("User", "postgres")
    .suggestion("Verify database credentials")
    .suggestion("Check if PostgreSQL is running")
    .sql("SELECT * FROM users WHERE id = $1");

// Styled output
eprintln!("{}", error.render_styled());

// Plain output
eprintln!("{}", error.render_plain());
```

**Error Severities:**

| Severity | Icon | Usage |
|----------|------|-------|
| `Error` | X | Unrecoverable errors |
| `Warning` | ! | Potential issues |
| `Info` | i | Informational messages |

### OperationProgress

Show progress for long-running operations.

```rust
use sqlmodel_console::renderables::OperationProgress;

// Create progress bar
let mut progress = OperationProgress::new("Inserting rows", 1000);

// Update progress
for i in 0..1000 {
    progress.update(i + 1);
    println!("\r{}", progress.render_styled());
}

// Mark complete
progress.complete();
println!("\n{}", progress.render_styled());
```

### IndeterminateSpinner

Show activity for operations with unknown duration.

```rust
use sqlmodel_console::renderables::{IndeterminateSpinner, SpinnerStyle};

let spinner = IndeterminateSpinner::new("Connecting to database")
    .style(SpinnerStyle::Dots);

// Advance animation
for _ in 0..10 {
    let next = spinner.next();
    println!("\r{}", next.render_styled());
    std::thread::sleep(std::time::Duration::from_millis(100));
}
```

**Spinner Styles:**

| Style | Frames |
|-------|--------|
| `Dots` | ⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ |
| `Line` | \|/-\\ |
| `Simple` | ◐◓◑◒ |

### BatchOperationTracker

Track bulk operations (inserts, updates, deletes).

```rust
use sqlmodel_console::renderables::BatchOperationTracker;

let mut tracker = BatchOperationTracker::new("Bulk insert", 10000);

// Update with batch completions
for batch in 0..100 {
    tracker.complete_batch(100);  // 100 items per batch
    println!("\r{}", tracker.render_styled());
}

tracker.finish();
```

### SqlHighlighter

Syntax highlight SQL queries.

```rust
use sqlmodel_console::renderables::SqlHighlighter;
use sqlmodel_console::Theme;

let highlighter = SqlHighlighter::with_theme(Theme::dark());

let sql = "SELECT u.name, COUNT(*) FROM users u WHERE u.active = true GROUP BY u.name";

// Highlighted (rich mode)
println!("{}", highlighter.highlight(sql));

// Plain (no colors)
println!("{}", highlighter.plain(sql));

// Formatted with indentation
println!("{}", highlighter.format(sql));
```

### QueryTiming

Display query execution time.

```rust
use sqlmodel_console::renderables::QueryTiming;
use std::time::Duration;

let timing = QueryTiming::new(Duration::from_millis(123));

println!("{}", timing.render_styled());  // "123.00ms"
println!("{}", timing.render_plain());   // "123.00ms"
```

### PoolStatusDisplay

Show connection pool health.

```rust
use sqlmodel_console::renderables::{PoolStatusDisplay, PoolStatsProvider, PoolHealth};

// Implement PoolStatsProvider for your pool type
struct MyPoolStats {
    total: usize,
    active: usize,
    idle: usize,
}

impl PoolStatsProvider for MyPoolStats {
    fn total_connections(&self) -> usize { self.total }
    fn active_connections(&self) -> usize { self.active }
    fn idle_connections(&self) -> usize { self.idle }
    fn health(&self) -> PoolHealth {
        if self.idle > 0 { PoolHealth::Healthy }
        else if self.active < self.total { PoolHealth::Degraded }
        else { PoolHealth::Critical }
    }
}

let stats = MyPoolStats { total: 10, active: 3, idle: 7 };
let display = PoolStatusDisplay::from_provider(&stats);

println!("{}", display.render_styled());
```

---

## Integration Patterns

### Per-Session Console

Attach a console to each database session:

```rust
use sqlmodel_console::{SqlModelConsole, Theme};

struct Session {
    console: SqlModelConsole,
    // ... connection fields
}

impl Session {
    pub fn new() -> Self {
        Self {
            console: SqlModelConsole::new(),
        }
    }

    pub fn new_with_theme(theme: Theme) -> Self {
        Self {
            console: SqlModelConsole::with_theme(theme),
        }
    }
}
```

### Global Console

Share a single console instance:

```rust
use sqlmodel_console::SqlModelConsole;
use std::sync::LazyLock;

static CONSOLE: LazyLock<SqlModelConsole> = LazyLock::new(SqlModelConsole::new);

fn main() {
    CONSOLE.print("Hello from global console");
    CONSOLE.success("Operation completed");
}
```

### Mode-Aware Output

Write functions that adapt to the output mode:

```rust
use sqlmodel_console::{SqlModelConsole, OutputMode};
use sqlmodel_console::renderables::QueryResultTable;

fn display_results(console: &SqlModelConsole, table: &QueryResultTable) {
    match console.mode() {
        OutputMode::Rich => {
            println!("{}", table.render_styled());
        }
        OutputMode::Plain => {
            println!("{}", table.render_plain());
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string(&table.to_json()).unwrap());
        }
    }
}
```

### Stream Separation

Keep semantic data on stdout, status on stderr:

```rust
use sqlmodel_console::SqlModelConsole;

let console = SqlModelConsole::new();

// Status messages → stderr (for humans)
console.status("Connecting to database...");
console.success("Connected!");

// Data → stdout (for agents to parse)
console.print("id|name|email");
console.print("1|Alice|alice@example.com");

// Errors → stderr
console.error("Query failed");
```

---

## Agent Compatibility

For detailed information about AI agent compatibility, see the [Agent Compatibility Guide](./agent-compatibility.md).

### Quick Summary

- **Auto-detection**: Agents are detected via environment variables
- **Stream separation**: stdout = data, stderr = status
- **Plain mode**: No ANSI codes, parseable formats
- **Override**: Use `SQLMODEL_RICH=1` to force rich mode

### Check Agent Mode

```rust
use sqlmodel_console::OutputMode;

if OutputMode::is_agent_environment() {
    println!("Running under an AI coding agent");
}
```

### Detected Agents

- Claude Code (`CLAUDE_CODE`)
- Codex CLI (`CODEX_CLI`)
- Cursor IDE (`CURSOR_SESSION`)
- Aider (`AIDER_MODEL`)
- GitHub Copilot (`GITHUB_COPILOT`)
- Continue.dev (`CONTINUE_SESSION`)
- And more...

---

## Troubleshooting

### Colors Not Showing

**Symptoms:** Output appears as plain text even in terminal.

**Causes:**
1. Running under detected AI agent
2. Output is piped/redirected
3. `NO_COLOR` environment variable is set
4. `TERM=dumb`

**Solutions:**
```bash
# Force rich mode
SQLMODEL_RICH=1 cargo run

# Check what mode is detected
cargo run --example mode_check
```

### ANSI Codes in Output

**Symptoms:** See escape sequences like `^[[32m` in output.

**Causes:**
1. Agent environment not detected
2. Rich mode forced in non-terminal context

**Solutions:**
```bash
# Force plain mode
SQLMODEL_PLAIN=1 cargo run

# Or set generic agent marker
AGENT_MODE=1 cargo run
```

### Output in Wrong Stream

**Symptoms:** Status messages mixed with data.

**Cause:** Using `print()` for status messages.

**Solution:** Use the appropriate method:
```rust
// Data → stdout
console.print("id|name");

// Status → stderr
console.status("Processing...");
console.success("Done!");
console.error("Failed!");
```

### Performance Issues

**Symptoms:** Slow output, especially with large result sets.

**Solutions:**
1. Use `max_rows()` to limit displayed rows
2. Use plain mode for bulk data
3. Render once instead of per-row

```rust
let table = QueryResultTable::new()
    .columns(cols)
    .rows(all_rows)  // Add all at once
    .max_rows(100);  // Limit display
```

---

## API Reference

### Core Types

```rust
// Console management
pub struct SqlModelConsole { ... }
pub enum OutputMode { Plain, Rich, Json }
pub struct Theme { ... }
pub struct ThemeColor { ... }

// Traits
pub trait ConsoleAware { ... }
pub trait PoolStatsProvider { ... }
```

### SqlModelConsole Methods

```rust
impl SqlModelConsole {
    // Creation
    pub fn new() -> Self;
    pub fn with_mode(mode: OutputMode) -> Self;
    pub fn with_theme(theme: Theme) -> Self;

    // Mode queries
    pub fn mode(&self) -> OutputMode;
    pub fn is_rich(&self) -> bool;
    pub fn is_plain(&self) -> bool;
    pub fn is_json(&self) -> bool;

    // stdout output
    pub fn print(&self, message: &str);
    pub fn print_raw(&self, message: &str);
    pub fn print_json<T: Serialize>(&self, value: &T) -> Result<...>;

    // stderr output
    pub fn status(&self, message: &str);
    pub fn success(&self, message: &str);
    pub fn error(&self, message: &str);
    pub fn warning(&self, message: &str);
    pub fn info(&self, message: &str);
    pub fn rule(&self, title: Option<&str>);
}
```

### OutputMode Methods

```rust
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

### Renderable Methods (Common Pattern)

All renderables follow this pattern:

```rust
impl Renderable {
    pub fn new(...) -> Self;           // Create
    pub fn render_styled(&self) -> String;  // Rich output
    pub fn render_plain(&self) -> String;   // Plain output
}
```

---

## Next Steps

- Read the [Agent Compatibility Guide](./agent-compatibility.md) for AI agent details
- Explore the `examples/` directory for more code samples
- Check the API documentation with `cargo doc --open`
