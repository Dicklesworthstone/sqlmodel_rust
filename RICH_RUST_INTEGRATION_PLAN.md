# Rich Rust Integration Plan for SQLModel Rust

> **Goal:** Integrate rich_rust throughout the sqlmodel_rust codebase to provide stunning, professional console output for humans watching agents work, while ensuring zero interference with AI coding agents parsing output.

---

## Executive Summary

This plan details how to integrate `rich_rust` into `sqlmodel_rust` to achieve:

1. **Premium Visual Experience** - Beautiful tables, panels, syntax highlighting, progress bars
2. **Agent Safety** - Output that AI agents can still parse without confusion
3. **Zero Performance Impact** - Optional feature, lazy initialization, minimal overhead
4. **Comprehensive Coverage** - Every crate gets appropriate styled output

---

## Table of Contents

1. [Core Architecture: Agent-Safe Output System](#1-core-architecture-agent-safe-output-system)
2. [New Crate: sqlmodel-console](#2-new-crate-sqlmodel-console)
3. [Integration Points by Crate](#3-integration-points-by-crate)
4. [Feature Catalog](#4-feature-catalog)
5. [Implementation Phases](#5-implementation-phases)
6. [API Design](#6-api-design)
7. [Testing Strategy](#7-testing-strategy)
8. [Risk Mitigation](#8-risk-mitigation)

---

## 1. Core Architecture: Agent-Safe Output System

### The Problem

AI coding agents (Claude Code, Codex, Cursor, etc.) parse stdout/stderr to understand program output. If we add ANSI escape codes and fancy formatting everywhere, we risk:

- Agents misinterpreting styled output as errors
- JSON/structured output becoming unparseable
- Context pollution with box-drawing characters
- Confusion between decorative and semantic content

### The Solution: Dual-Mode Output

```
┌─────────────────────────────────────────────────────────────────┐
│                    Output Mode Detection                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Environment Check ─┬─► SQLMODEL_PLAIN=1      → Plain Mode      │
│                     ├─► NO_COLOR=1            → Plain Mode      │
│                     ├─► CI=true               → Plain Mode      │
│                     ├─► TERM=dumb             → Plain Mode      │
│                     ├─► !is_tty(stdout)       → Plain Mode      │
│                     └─► Otherwise             → Rich Mode       │
│                                                                  │
│  Agent Detection ───┬─► CLAUDE_CODE=1         → Plain Mode      │
│                     ├─► CODEX_CLI=1           → Plain Mode      │
│                     ├─► CURSOR_SESSION=1      → Plain Mode      │
│                     └─► AGENT_MODE=1          → Plain Mode      │
│                                                                  │
│  Override ──────────┴─► SQLMODEL_RICH=1       → Rich Mode       │
│                         (forces rich even in agent mode)        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Output Mode Enum

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    /// Plain text, no ANSI codes, machine-parseable
    Plain,
    /// Rich formatting with colors, tables, boxes
    Rich,
    /// JSON output for structured data (agents can parse)
    Json,
}
```

### Key Principle: Semantic Separation

```
┌─────────────────────────────────────────────────────────────────┐
│                    Output Streams                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  STDOUT (Semantic Data)                                         │
│  ├─ Query results                                               │
│  ├─ Schema information                                          │
│  ├─ JSON output                                                 │
│  └─ Machine-parseable content                                   │
│                                                                  │
│  STDERR (Human Feedback)                                        │
│  ├─ Progress bars and spinners                                  │
│  ├─ Status messages                                             │
│  ├─ Decorative panels and tables                                │
│  ├─ Warnings and errors (styled)                                │
│  └─ Visual diagnostics                                          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Why this works for agents:**
- Agents typically capture stdout for data
- stderr is for human consumption
- Agents can ignore stderr or filter it
- Semantic content remains clean and parseable

---

## 2. New Crate: sqlmodel-console

### Purpose

A thin wrapper around `rich_rust` that provides:
- Automatic output mode detection
- SQLModel-specific renderables (query tables, schema trees, etc.)
- Consistent styling theme across the project
- Zero-cost when disabled

### Crate Structure

```
crates/sqlmodel-console/
├── Cargo.toml
└── src/
    ├── lib.rs              # Public API, prelude
    ├── mode.rs             # OutputMode detection
    ├── console.rs          # SqlModelConsole wrapper
    ├── theme.rs            # Color theme definitions
    ├── renderables/
    │   ├── mod.rs
    │   ├── query.rs        # Query result tables
    │   ├── schema.rs       # Schema visualization
    │   ├── error.rs        # Error panels
    │   ├── migration.rs    # Migration status
    │   ├── pool.rs         # Pool statistics
    │   └── explain.rs      # Query explain plans
    └── widgets/
        ├── mod.rs
        ├── progress.rs     # DB operation progress
        ├── spinner.rs      # Connection spinners
        └── status.rs       # Status indicators
```

### Cargo.toml

```toml
[package]
name = "sqlmodel-console"
version = "0.1.0"
edition = "2024"

[features]
default = []
rich = ["rich_rust"]
syntax = ["rich", "rich_rust/syntax"]
full = ["rich", "syntax"]

[dependencies]
# Optional: only included with "rich" feature
rich_rust = { path = "../../../rich_rust", optional = true }

# Always included (minimal)
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

### Key Types

```rust
/// Main console for SQLModel output
pub struct SqlModelConsole {
    mode: OutputMode,
    #[cfg(feature = "rich")]
    rich_console: Option<rich_rust::Console>,
    theme: Theme,
}

impl SqlModelConsole {
    /// Create with automatic mode detection
    pub fn new() -> Self;

    /// Force a specific mode
    pub fn with_mode(mode: OutputMode) -> Self;

    /// Check current mode
    pub fn mode(&self) -> OutputMode;

    /// Print with automatic mode handling
    pub fn print(&self, content: impl Display);

    /// Print a query result (table in rich, JSON/CSV in plain)
    pub fn print_query_result(&self, rows: &[Row], columns: &[String]);

    /// Print an error with context
    pub fn print_error(&self, error: &Error);

    /// Print schema information
    pub fn print_schema(&self, schema: &SchemaInfo);

    /// Create a progress bar for operations
    pub fn progress(&self, total: u64, label: &str) -> ProgressHandle;

    /// Print to stderr (for human feedback)
    pub fn status(&self, message: &str);
}
```

---

## 3. Integration Points by Crate

### 3.1 sqlmodel-core

**Current State:** Core types, no console output
**Integration:** Add optional Display implementations with rich formatting

#### Error Display Enhancement

```rust
// In error.rs - Enhanced error display

impl Error {
    /// Render as rich panel (when feature enabled)
    #[cfg(feature = "console")]
    pub fn to_panel(&self) -> Panel {
        let title = match self {
            Error::Connection(_) => "Connection Error",
            Error::Query(q) => match q.kind {
                QueryErrorKind::Syntax => "SQL Syntax Error",
                QueryErrorKind::Constraint => "Constraint Violation",
                // ...
            },
            Error::Type(_) => "Type Conversion Error",
            // ...
        };

        Panel::from_text(&self.to_string())
            .title(title)
            .title_style(Style::new().bold().color(Color::Red))
            .border_style(Style::new().color(Color::Red))
    }
}
```

#### Row Display Enhancement

```rust
// Pretty-print Row data
impl Row {
    #[cfg(feature = "console")]
    pub fn to_table_row(&self) -> Vec<Cell> {
        self.values().map(|v| {
            let style = match v {
                Value::Null => Style::new().dim().italic(),
                Value::Bool(_) => Style::new().color(Color::Yellow),
                Value::BigInt(_) | Value::Int(_) => Style::new().color(Color::Cyan),
                Value::Text(_) => Style::new().color(Color::Green),
                Value::Timestamp(_) => Style::new().color(Color::Magenta),
                _ => Style::new(),
            };
            Cell::new(v.to_string()).style(style)
        }).collect()
    }
}
```

### 3.2 sqlmodel-query

**Current State:** Query builders, SQL generation
**Integration:** SQL syntax highlighting, query explanation visualization

#### SQL Syntax Highlighting

```rust
// In select.rs, builder.rs, etc.

impl<M: Model> Select<M> {
    /// Generate SQL with syntax highlighting
    #[cfg(feature = "console")]
    pub fn to_highlighted_sql(&self, dialect: Dialect) -> Syntax {
        let sql = self.to_sql(dialect);
        Syntax::new(&sql, "sql")
            .line_numbers(false)
            .theme("base16-ocean.dark")
    }

    /// Print query with explanation
    #[cfg(feature = "console")]
    pub fn explain(&self, console: &SqlModelConsole) {
        let panel = Panel::from_rich_text(self.to_highlighted_sql(Dialect::Postgres))
            .title("Generated SQL")
            .subtitle(format!("Table: {}", M::TABLE_NAME));
        console.print_panel(&panel);
    }
}
```

#### Query Builder Visualization

```rust
// Visual representation of query structure
impl<M: Model> Select<M> {
    #[cfg(feature = "console")]
    pub fn to_tree(&self) -> Tree {
        let mut root = TreeNode::new(format!("SELECT from {}", M::TABLE_NAME));

        if !self.columns.is_empty() {
            let mut cols = TreeNode::new("Columns");
            for col in &self.columns {
                cols.add_child(TreeNode::new(col));
            }
            root.add_child(cols);
        }

        if let Some(ref where_clause) = self.where_clause {
            root.add_child(TreeNode::new(format!("WHERE {}", where_clause)));
        }

        if !self.joins.is_empty() {
            let mut joins = TreeNode::new("Joins");
            for join in &self.joins {
                joins.add_child(TreeNode::new(format!("{:?} {}", join.kind, join.table)));
            }
            root.add_child(joins);
        }

        // ... order_by, limit, offset

        Tree::new(root).guide_style(Style::new().color(Color::Cyan))
    }
}
```

### 3.3 sqlmodel-schema

**Current State:** DDL generation, migrations
**Integration:** Schema visualization, migration status displays

#### Schema Tree Visualization

```rust
// Visual schema representation
pub struct SchemaTree {
    tables: Vec<TableInfo>,
}

impl SchemaTree {
    #[cfg(feature = "console")]
    pub fn render(&self) -> Tree {
        let mut root = TreeNode::new("Database Schema")
            .icon("🗄️");

        for table in &self.tables {
            let mut table_node = TreeNode::new(&table.name)
                .icon("📋");

            // Primary key columns
            let pk_cols: Vec<_> = table.columns.iter()
                .filter(|c| c.primary_key)
                .collect();
            if !pk_cols.is_empty() {
                let mut pk_node = TreeNode::new("Primary Key")
                    .icon("🔑");
                for col in pk_cols {
                    pk_node.add_child(TreeNode::new(format!(
                        "{} ({})", col.name, col.sql_type
                    )));
                }
                table_node.add_child(pk_node);
            }

            // Regular columns
            let mut cols_node = TreeNode::new("Columns")
                .icon("📝");
            for col in &table.columns {
                let nullable = if col.nullable { "NULL" } else { "NOT NULL" };
                cols_node.add_child(TreeNode::new(format!(
                    "{}: {} {}", col.name, col.sql_type, nullable
                )));
            }
            table_node.add_child(cols_node);

            // Foreign keys
            if !table.foreign_keys.is_empty() {
                let mut fk_node = TreeNode::new("Foreign Keys")
                    .icon("🔗");
                for fk in &table.foreign_keys {
                    fk_node.add_child(TreeNode::new(format!(
                        "{} → {}.{}", fk.column, fk.ref_table, fk.ref_column
                    )));
                }
                table_node.add_child(fk_node);
            }

            root.add_child(table_node);
        }

        Tree::new(root)
    }
}
```

#### Migration Status Display

```rust
// Migration status panel
pub struct MigrationStatus {
    applied: Vec<MigrationInfo>,
    pending: Vec<MigrationInfo>,
}

impl MigrationStatus {
    #[cfg(feature = "console")]
    pub fn render(&self) -> Panel {
        let mut table = Table::new()
            .title("Migration Status")
            .with_column(Column::new("Version").style(Style::new().bold()))
            .with_column(Column::new("Name"))
            .with_column(Column::new("Status").justify(JustifyMethod::Center))
            .with_column(Column::new("Applied At"));

        for mig in &self.applied {
            table.add_row(Row::new()
                .cell(Cell::new(&mig.version))
                .cell(Cell::new(&mig.name))
                .cell(Cell::new("✅ Applied").style(Style::new().color(Color::Green)))
                .cell(Cell::new(mig.applied_at.format("%Y-%m-%d %H:%M"))));
        }

        for mig in &self.pending {
            table.add_row(Row::new()
                .cell(Cell::new(&mig.version))
                .cell(Cell::new(&mig.name))
                .cell(Cell::new("⏳ Pending").style(Style::new().color(Color::Yellow)))
                .cell(Cell::new("—")));
        }

        Panel::new(table)
            .title("Migrations")
            .border_style(Style::new().color(Color::Cyan))
    }
}
```

### 3.4 sqlmodel-pool

**Current State:** Connection pooling with stats
**Integration:** Real-time pool status, connection health visualization

#### Pool Status Dashboard

```rust
// Pool statistics visualization
impl PoolStats {
    #[cfg(feature = "console")]
    pub fn render(&self) -> Panel {
        let usage_pct = (self.active_connections as f64 /
                        self.total_connections as f64 * 100.0) as u64;

        let bar = ProgressBar::new()
            .completed(usage_pct)
            .total(100)
            .width(30)
            .style(if usage_pct > 80 {
                BarStyle::Gradient // Red when high
            } else {
                BarStyle::Block
            });

        let mut content = Text::new("");
        content.append_line(format!("Pool Utilization: {}%", usage_pct));
        content.append_line("");
        content.append_segments(bar.render(30));
        content.append_line("");
        content.append_line(format!(
            "  Active: {} │ Idle: {} │ Total: {}",
            self.active_connections,
            self.idle_connections,
            self.total_connections
        ));
        content.append_line(format!(
            "  Pending: {} │ Created: {} │ Timeouts: {}",
            self.pending_requests,
            self.connections_created,
            self.timeouts
        ));

        Panel::from_rich_text(content)
            .title("Connection Pool")
            .title_style(Style::new().bold().color(Color::Cyan))
    }
}
```

#### Connection Health Indicator

```rust
// Individual connection status
pub struct ConnectionHealth {
    state: ConnectionState,
    age: Duration,
    last_query: Option<Duration>,
}

impl ConnectionHealth {
    #[cfg(feature = "console")]
    pub fn status_icon(&self) -> &'static str {
        match self.state {
            ConnectionState::Ready => "🟢",
            ConnectionState::InQuery => "🔵",
            ConnectionState::InTransaction => "🟡",
            ConnectionState::Error => "🔴",
            ConnectionState::Closed => "⚫",
            _ => "⚪",
        }
    }
}
```

### 3.5 sqlmodel-postgres / sqlmodel-sqlite / sqlmodel-mysql

**Current State:** Database drivers
**Integration:** Connection progress, query timing, protocol visualization

#### Connection Progress Spinner

```rust
// Connection establishment feedback
impl PgConnection {
    pub async fn connect_with_progress(
        cx: &Cx,
        config: &PgConfig,
        console: &SqlModelConsole,
    ) -> Outcome<Self, Error> {
        let spinner = console.spinner("Connecting to PostgreSQL...");

        spinner.update("Resolving host...");
        // ... DNS resolution

        spinner.update("Establishing TCP connection...");
        // ... TCP connect

        spinner.update("Authenticating...");
        // ... SCRAM authentication

        spinner.update("Negotiating parameters...");
        // ... startup message exchange

        spinner.finish_with("Connected ✓");

        Ok(conn)
    }
}
```

#### Query Timing Display

```rust
// Query execution with timing
pub struct QueryTiming {
    parse_time: Duration,
    plan_time: Duration,
    execute_time: Duration,
    rows_returned: usize,
}

impl QueryTiming {
    #[cfg(feature = "console")]
    pub fn render(&self) -> Text {
        let total = self.parse_time + self.plan_time + self.execute_time;

        let mut text = Text::new("");
        text.append(format!("Query completed in "), Style::new().dim());
        text.append(format!("{:.2}ms", total.as_secs_f64() * 1000.0),
                   Style::new().bold().color(Color::Green));
        text.append(format!(" ({} rows)\n", self.rows_returned), Style::new().dim());

        // Timing breakdown bar
        let total_ms = total.as_millis() as f64;
        let parse_pct = (self.parse_time.as_millis() as f64 / total_ms * 100.0) as usize;
        let plan_pct = (self.plan_time.as_millis() as f64 / total_ms * 100.0) as usize;
        let exec_pct = 100 - parse_pct - plan_pct;

        text.append("  Parse: ", Style::new().dim());
        text.append("█".repeat(parse_pct / 5), Style::new().color(Color::Blue));
        text.append(format!(" {:.1}ms\n", self.parse_time.as_secs_f64() * 1000.0),
                   Style::new().dim());

        text.append("  Plan:  ", Style::new().dim());
        text.append("█".repeat(plan_pct / 5), Style::new().color(Color::Yellow));
        text.append(format!(" {:.1}ms\n", self.plan_time.as_secs_f64() * 1000.0),
                   Style::new().dim());

        text.append("  Exec:  ", Style::new().dim());
        text.append("█".repeat(exec_pct / 5), Style::new().color(Color::Green));
        text.append(format!(" {:.1}ms", self.execute_time.as_secs_f64() * 1000.0),
                   Style::new().dim());

        text
    }
}
```

### 3.6 sqlmodel-macros

**Current State:** Procedural macros
**Integration:** Compile-time diagnostics (limited, macros run at compile time)

Note: Proc macros have limited console output capability. Integration here is primarily through better error messages that the compiler displays.

```rust
// Enhanced error messages in macro expansion
fn emit_error(span: Span, message: &str, hint: Option<&str>) {
    let mut diag = Diagnostic::spanned(span, Level::Error, message);
    if let Some(h) = hint {
        diag = diag.help(h);
    }
    diag.emit();
}

// Usage in derive macro:
if field.primary_key && field.nullable {
    emit_error(
        field.span(),
        "Primary key fields cannot be nullable",
        Some("Remove #[sqlmodel(nullable)] or #[sqlmodel(primary_key)]")
    );
}
```

### 3.7 sqlmodel (Facade Crate)

**Current State:** Re-exports all crates
**Integration:** Unified console export, global configuration

```rust
// In lib.rs - Console re-export
#[cfg(feature = "console")]
pub use sqlmodel_console::{
    SqlModelConsole, OutputMode, Theme,
    // Renderables
    QueryResultTable, SchemaTree, MigrationPanel,
    // Widgets
    ProgressBar, Spinner, StatusIndicator,
};

#[cfg(feature = "console")]
pub mod console {
    //! Console output utilities for beautiful terminal displays.
    //!
    //! This module provides styled output that automatically adapts
    //! to the terminal environment. When running under an AI coding
    //! agent, output is plain text. When running interactively,
    //! output is richly formatted.
    //!
    //! # Example
    //!
    //! ```rust
    //! use sqlmodel::prelude::*;
    //! use sqlmodel::console::SqlModelConsole;
    //!
    //! let console = SqlModelConsole::new();
    //!
    //! // Automatically uses rich or plain based on environment
    //! console.print_query_result(&rows, &columns);
    //! ```

    pub use sqlmodel_console::*;
}
```

---

## 4. Feature Catalog

### 4.1 Rich Text & Markup

| Feature | Where Used | Description |
|---------|------------|-------------|
| Bold/Italic | Error messages, headers | Emphasis |
| Colors | Values by type, status | Semantic coloring |
| Markup syntax | All user-facing strings | `[bold red]Error[/]` |

### 4.2 Tables

| Feature | Where Used | Description |
|---------|------------|-------------|
| Auto-sizing columns | Query results | Fit content to terminal |
| Column alignment | Numeric data right-aligned | Professional appearance |
| Header styling | Table headers | Bold, distinct |
| Border styles | All tables | Rounded by default |
| Overflow handling | Long text cells | Ellipsis or wrap |

### 4.3 Panels

| Feature | Where Used | Description |
|---------|------------|-------------|
| Error panels | All error displays | Red border, clear title |
| Info panels | Connection info | Cyan border |
| Warning panels | Deprecation notices | Yellow border |
| Success panels | Completion messages | Green border |

### 4.4 Trees

| Feature | Where Used | Description |
|---------|------------|-------------|
| Schema trees | Database introspection | Tables → columns → constraints |
| Query trees | Query builder visualization | SELECT → WHERE → JOIN |
| Dependency trees | Migration dependencies | Visual dependency graph |

### 4.5 Progress Indicators

| Feature | Where Used | Description |
|---------|------------|-------------|
| Progress bars | Batch operations | Insert/update many rows |
| Spinners | Connection, long queries | Async feedback |
| Percentage | Migration progress | X of Y migrations |

### 4.6 Syntax Highlighting

| Feature | Where Used | Description |
|---------|------------|-------------|
| SQL highlighting | Generated queries | Keywords, strings, numbers |
| Rust highlighting | Error context | Code snippets in errors |
| JSON highlighting | JSON columns | Pretty-print JSON values |

### 4.7 Rules & Dividers

| Feature | Where Used | Description |
|---------|------------|-------------|
| Section dividers | Between output sections | Visual separation |
| Titled rules | "Query Results", "Schema" | Labeled sections |

---

## 5. Implementation Phases

### Phase 1: Foundation (Week 1)

1. **Create sqlmodel-console crate**
   - [ ] Cargo.toml with optional rich_rust dependency
   - [ ] OutputMode detection logic
   - [ ] SqlModelConsole basic implementation
   - [ ] Theme definitions

2. **Basic integration**
   - [ ] Add `console` feature to sqlmodel facade
   - [ ] Wire up re-exports
   - [ ] Add to workspace Cargo.toml

### Phase 2: Error Display (Week 2)

1. **Enhanced errors**
   - [ ] Error::to_panel() implementation
   - [ ] QueryError visualization with SQL context
   - [ ] Connection error details

2. **Testing**
   - [ ] Unit tests for plain mode output
   - [ ] Visual tests for rich mode (examples/)

### Phase 3: Query Output (Week 3)

1. **Query results**
   - [ ] QueryResultTable renderable
   - [ ] Type-based cell coloring
   - [ ] Null value styling
   - [ ] Row count footer

2. **SQL display**
   - [ ] Syntax highlighting integration
   - [ ] Query builder .explain() method
   - [ ] Query timing visualization

### Phase 4: Schema Visualization (Week 4)

1. **Schema display**
   - [ ] SchemaTree renderable
   - [ ] Table/column/constraint hierarchy
   - [ ] Foreign key relationship lines

2. **Migration status**
   - [ ] MigrationPanel renderable
   - [ ] Applied/pending status
   - [ ] Dependency visualization

### Phase 5: Pool & Operations (Week 5)

1. **Pool status**
   - [ ] PoolDashboard renderable
   - [ ] Connection health indicators
   - [ ] Utilization bar

2. **Operation progress**
   - [ ] Batch insert progress
   - [ ] Migration runner progress
   - [ ] Connection spinner

### Phase 6: Polish & Documentation (Week 6)

1. **Refinement**
   - [ ] Theme tuning
   - [ ] Edge case handling
   - [ ] Performance optimization

2. **Documentation**
   - [ ] README updates
   - [ ] Example programs
   - [ ] API documentation

---

## 6. API Design

### 6.1 Console Creation

```rust
// Automatic mode detection (recommended)
let console = SqlModelConsole::new();

// Force specific mode
let console = SqlModelConsole::with_mode(OutputMode::Rich);
let console = SqlModelConsole::with_mode(OutputMode::Plain);
let console = SqlModelConsole::with_mode(OutputMode::Json);

// With custom theme
let console = SqlModelConsole::new()
    .with_theme(Theme::dark());
```

### 6.2 Output Methods

```rust
// Basic printing (mode-aware)
console.print("Hello, world!");
console.print("[bold green]Success![/]");

// Status messages (always to stderr)
console.status("Connecting...");
console.status_success("Connected");
console.status_warning("Slow query detected");
console.status_error("Connection failed");

// Structured output
console.print_query_result(&rows, &columns);
console.print_error(&error);
console.print_schema(&schema_info);
console.print_migration_status(&status);
console.print_pool_stats(&stats);

// Progress tracking
let progress = console.progress(100, "Inserting rows");
for i in 0..100 {
    // ... insert row
    progress.advance(1);
}
progress.finish();

// Spinner for async operations
let spinner = console.spinner("Connecting...");
// ... async work
spinner.finish_with("Connected ✓");
```

### 6.3 Conditional Rendering

```rust
// Only render in rich mode
if console.is_rich() {
    console.print_panel(&detailed_panel);
} else {
    console.print(&summary);
}

// Different output per mode
match console.mode() {
    OutputMode::Rich => console.print_renderable(&fancy_table),
    OutputMode::Plain => println!("{}", rows.to_csv()),
    OutputMode::Json => println!("{}", serde_json::to_string(&rows)?),
}
```

### 6.4 Extension Point for Custom Renderables

```rust
// Users can create custom renderables
pub trait SqlModelRenderable {
    fn render_rich(&self) -> Box<dyn Renderable>;
    fn render_plain(&self) -> String;
    fn render_json(&self) -> serde_json::Value;
}

impl<T: SqlModelRenderable> SqlModelConsole {
    pub fn print_custom(&self, item: &T) {
        match self.mode {
            OutputMode::Rich => self.print_renderable(&*item.render_rich()),
            OutputMode::Plain => println!("{}", item.render_plain()),
            OutputMode::Json => println!("{}", item.render_json()),
        }
    }
}
```

---

## 7. Testing Strategy

### 7.1 Unit Tests

```rust
#[test]
fn test_output_mode_detection() {
    // Test environment variable detection
    std::env::set_var("SQLMODEL_PLAIN", "1");
    assert_eq!(detect_output_mode(), OutputMode::Plain);
    std::env::remove_var("SQLMODEL_PLAIN");

    std::env::set_var("CLAUDE_CODE", "1");
    assert_eq!(detect_output_mode(), OutputMode::Plain);
    std::env::remove_var("CLAUDE_CODE");
}

#[test]
fn test_plain_mode_no_ansi() {
    let console = SqlModelConsole::with_mode(OutputMode::Plain);
    let output = capture_stdout(|| {
        console.print("[bold red]Error[/]");
    });

    // Should not contain ANSI codes
    assert!(!output.contains("\x1b["));
    assert!(output.contains("Error"));
}

#[test]
fn test_query_result_table() {
    let console = SqlModelConsole::with_mode(OutputMode::Plain);
    let rows = vec![/* test data */];
    let columns = vec!["id", "name"];

    let output = capture_stdout(|| {
        console.print_query_result(&rows, &columns);
    });

    // Verify structure without ANSI codes
    assert!(output.contains("id"));
    assert!(output.contains("name"));
}
```

### 7.2 Visual Examples

```rust
// examples/rich_demo.rs
use sqlmodel::prelude::*;
use sqlmodel::console::*;

fn main() {
    let console = SqlModelConsole::new();

    // Force rich mode for demo
    std::env::set_var("SQLMODEL_RICH", "1");

    console.rule(Some("Query Results Demo"));

    // Demo query results table
    let rows = create_demo_rows();
    console.print_query_result(&rows, &["id", "name", "email", "created_at"]);

    console.rule(Some("Error Display Demo"));

    // Demo error panel
    let error = Error::Query(QueryError {
        kind: QueryErrorKind::Syntax,
        message: "Unexpected token 'SELCT' near position 1".into(),
        sql: Some("SELCT * FROM users".into()),
        // ...
    });
    console.print_error(&error);

    console.rule(Some("Schema Tree Demo"));

    // Demo schema tree
    let schema = create_demo_schema();
    console.print_schema(&schema);
}
```

### 7.3 Agent Compatibility Tests

```rust
#[test]
fn test_agent_can_parse_plain_output() {
    std::env::set_var("CLAUDE_CODE", "1");
    let console = SqlModelConsole::new();

    let output = capture_stdout(|| {
        console.print_query_result(&rows, &columns);
    });

    // Verify agents can parse the output
    // Should be valid CSV or simple table
    let parsed: Vec<Vec<&str>> = output
        .lines()
        .map(|line| line.split('|').collect())
        .collect();

    assert!(!parsed.is_empty());
}

#[test]
fn test_json_mode_valid_json() {
    let console = SqlModelConsole::with_mode(OutputMode::Json);

    let output = capture_stdout(|| {
        console.print_query_result(&rows, &columns);
    });

    // Must be valid JSON
    let _: serde_json::Value = serde_json::from_str(&output)
        .expect("Output should be valid JSON");
}
```

---

## 8. Risk Mitigation

### 8.1 Risk: Breaking Agent Workflows

**Mitigation:**
- Default to plain mode when any agent environment variable detected
- All semantic output goes to stdout (parseable)
- All decorative output goes to stderr (ignorable)
- Extensive testing with agent environment simulation
- `SQLMODEL_PLAIN=1` always forces plain mode

### 8.2 Risk: Performance Overhead

**Mitigation:**
- Console feature is optional (not in default features)
- Lazy initialization of rich_rust Console
- LRU caching for repeated renders
- Minimal work in plain mode (just print)

### 8.3 Risk: Terminal Compatibility

**Mitigation:**
- rich_rust handles color downgrading automatically
- ASCII fallback for box characters
- Test on common terminals (iTerm2, Windows Terminal, VS Code)
- `NO_COLOR` environment variable respected

### 8.4 Risk: Dependency Bloat

**Mitigation:**
- rich_rust only included with `console` feature
- Syntax highlighting only with `console-syntax` feature
- Core functionality has zero additional dependencies

---

## 9. Color Theme

### SQLModel Dark Theme (Default)

```rust
pub struct Theme {
    // Semantic colors
    pub success: Color,      // #50fa7b (green)
    pub error: Color,        // #ff5555 (red)
    pub warning: Color,      // #f1fa8c (yellow)
    pub info: Color,         // #8be9fd (cyan)

    // Value type colors
    pub null_value: Color,   // #6272a4 (gray, dim)
    pub bool_value: Color,   // #f1fa8c (yellow)
    pub number_value: Color, // #8be9fd (cyan)
    pub string_value: Color, // #50fa7b (green)
    pub date_value: Color,   // #ff79c6 (magenta)
    pub binary_value: Color, // #ffb86c (orange)

    // SQL syntax colors
    pub sql_keyword: Color,  // #ff79c6 (magenta)
    pub sql_string: Color,   // #50fa7b (green)
    pub sql_number: Color,   // #bd93f9 (purple)
    pub sql_comment: Color,  // #6272a4 (gray)
    pub sql_operator: Color, // #ff5555 (red)

    // UI element colors
    pub border: Color,       // #6272a4 (gray)
    pub header: Color,       // #f8f8f2 (white)
    pub dim: Color,          // #6272a4 (gray)
}

impl Theme {
    pub fn dark() -> Self {
        Self {
            success: Color::from_rgb(80, 250, 123),
            error: Color::from_rgb(255, 85, 85),
            warning: Color::from_rgb(241, 250, 140),
            info: Color::from_rgb(139, 233, 253),
            // ... etc
        }
    }

    pub fn light() -> Self {
        // Light theme variant
    }
}
```

---

## 10. Example Output

### Query Results (Rich Mode)

```
┌──────────────────────────── Query Results ────────────────────────────┐
│ Query completed in 12.34ms (3 rows)                                   │
├───────┬─────────────┬──────────────────────┬─────────────────────────┤
│    id │ name        │ email                │ created_at              │
├───────┼─────────────┼──────────────────────┼─────────────────────────┤
│     1 │ Alice       │ alice@example.com    │ 2024-01-15 10:30:00    │
│     2 │ Bob         │ bob@example.com      │ 2024-01-16 14:22:00    │
│     3 │ Charlie     │ NULL                 │ 2024-01-17 09:15:00    │
└───────┴─────────────┴──────────────────────┴─────────────────────────┘
```

### Query Results (Plain Mode - Agent Safe)

```
id|name|email|created_at
1|Alice|alice@example.com|2024-01-15 10:30:00
2|Bob|bob@example.com|2024-01-16 14:22:00
3|Charlie|NULL|2024-01-17 09:15:00
```

### Error Display (Rich Mode)

```
╭─────────────────────── SQL Syntax Error ───────────────────────╮
│                                                                 │
│  Unexpected token 'SELCT' near position 1                      │
│                                                                 │
│  ┌─ Query ──────────────────────────────────────────────────┐  │
│  │ SELCT * FROM users                                       │  │
│  │ ^^^^^                                                    │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  💡 Hint: Did you mean 'SELECT'?                               │
│                                                                 │
│  SQLSTATE: 42601                                               │
│                                                                 │
╰─────────────────────────────────────────────────────────────────╯
```

### Schema Tree (Rich Mode)

```
🗄️ Database Schema
├── 📋 users
│   ├── 🔑 Primary Key
│   │   └── id (BIGINT)
│   ├── 📝 Columns
│   │   ├── id: BIGINT NOT NULL
│   │   ├── name: VARCHAR(255) NOT NULL
│   │   ├── email: VARCHAR(255) NOT NULL
│   │   └── created_at: TIMESTAMP NOT NULL
│   └── 🔗 Foreign Keys
│       └── team_id → teams.id
├── 📋 teams
│   ├── 🔑 Primary Key
│   │   └── id (BIGINT)
│   └── 📝 Columns
│       ├── id: BIGINT NOT NULL
│       └── name: VARCHAR(255) NOT NULL
```

### Pool Status (Rich Mode)

```
╭───────────────────── Connection Pool ─────────────────────╮
│                                                           │
│  Pool Utilization: 45%                                    │
│                                                           │
│  ████████████░░░░░░░░░░░░░░░░░░                          │
│                                                           │
│    Active: 9 │ Idle: 11 │ Total: 20                      │
│    Pending: 0 │ Created: 47 │ Timeouts: 2                │
│                                                           │
│  Health: 🟢 Healthy                                       │
│                                                           │
╰───────────────────────────────────────────────────────────╯
```

---

## Appendix A: Environment Variables

| Variable | Effect |
|----------|--------|
| `SQLMODEL_PLAIN=1` | Force plain text mode |
| `SQLMODEL_RICH=1` | Force rich mode (overrides agent detection) |
| `SQLMODEL_JSON=1` | Force JSON output mode |
| `NO_COLOR=1` | Disable all colors (standard) |
| `FORCE_COLOR=1` | Enable colors even if not TTY |
| `CLAUDE_CODE=1` | Detected as agent → plain mode |
| `CODEX_CLI=1` | Detected as agent → plain mode |
| `CURSOR_SESSION=1` | Detected as agent → plain mode |
| `AGENT_MODE=1` | Generic agent detection → plain mode |
| `CI=true` | CI environment → plain mode |
| `TERM=dumb` | Dumb terminal → plain mode |

---

## Appendix B: Feature Flags

```toml
[features]
default = []

# Basic console support (no syntax highlighting)
console = ["sqlmodel-console"]

# Console with SQL syntax highlighting
console-syntax = ["console", "sqlmodel-console/syntax"]

# Full console features
console-full = ["console", "sqlmodel-console/full"]
```

---

## Appendix C: Dependencies Added

| Crate | Version | Purpose | When Included |
|-------|---------|---------|---------------|
| `rich_rust` | path | Terminal rendering | `console` feature |
| (via rich_rust) `crossterm` | 0.29 | Terminal detection | `console` feature |
| (via rich_rust) `unicode-width` | 0.2.2 | Text width | `console` feature |
| (via rich_rust) `syntect` | 5.3 | Syntax highlighting | `console-syntax` feature |

---

*This plan provides a comprehensive roadmap for integrating rich_rust into sqlmodel_rust while maintaining full compatibility with AI coding agents.*
