# SQLModel Rust

<div align="center">
  <img src="sqlmodel_rust_illustration.webp" alt="SQLModel Rust - SQL databases in Rust, designed to be intuitive and type-safe">
</div>

<div align="center">

**SQL databases in Rust, designed to be intuitive and type-safe.**

[![CI](https://github.com/Dicklesworthstone/sqlmodel_rust/actions/workflows/ci.yml/badge.svg)](https://github.com/Dicklesworthstone/sqlmodel_rust/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust: Nightly](https://img.shields.io/badge/Rust-nightly-orange.svg)](https://www.rust-lang.org/)

*A Rust port of [tiangolo/sqlmodel](https://github.com/tiangolo/sqlmodel) (Python), extended with [asupersync](https://github.com/Dicklesworthstone/asupersync) for structured concurrency and cancel-correct async database operations.*

</div>

---

## TL;DR

**The Problem**: Existing Rust ORMs are either too low-level (raw SQL strings), too magical (runtime reflection), or force you to learn complex DSLs. You shouldn't need a PhD in database theory to insert a row.

**The Solution**: SQLModel Rust provides Python SQLModel's developer experience with Rust's compile-time safety. Define your models with derive macros, query with type-safe builders, and let the compiler catch your mistakes.

### Why SQLModel Rust?

| Feature | What It Does |
|---------|--------------|
| **Zero-cost derive macros** | `#[derive(Model)]` generates efficient code at compile time—no runtime reflection |
| **Type-safe query builder** | Compile-time validation of SQL expressions, columns, and joins |
| **Cancel-correct async** | Built on [asupersync](https://github.com/Dicklesworthstone/asupersync) for structured concurrency |
| **Multi-dialect support** | Single codebase generates Postgres, SQLite, or MySQL SQL |
| **Lean dependencies** | No tokio/sqlx/diesel/sea-orm; core stays small, drivers/validation use focused crypto/regex deps |

---

## Quick Example

```rust
use sqlmodel::prelude::*;

#[derive(Model, Debug)]
#[sqlmodel(table = "heroes")]
struct Hero {
    #[sqlmodel(primary_key, auto_increment)]
    id: Option<i64>,

    #[sqlmodel(unique)]
    name: String,

    secret_name: String,

    #[sqlmodel(nullable)]
    age: Option<i32>,

    #[sqlmodel(foreign_key = "teams.id")]
    team_id: Option<i64>,
}

// Type-safe query building
let query = select!(Hero)
    .filter(Expr::col("age").gt(18))
    .order_by(Expr::col("name").asc())
    .limit(10);

// Generates: SELECT * FROM "heroes" WHERE "age" > $1 ORDER BY "name" ASC LIMIT 10
println!("{}", query.to_sql(Dialect::Postgres));

// Execute against a connection
let heroes: Vec<Hero> = query.all(cx, &conn).await?;
```

---

## Design Philosophy

### 1. First-Principles, Not Translation

We extracted the *behavior specification* from Python SQLModel/SQLAlchemy/Pydantic, then implemented fresh in Rust. No line-by-line translation. Rust has compile-time types and macros—we use them instead of runtime validation.

### 2. Zero-Cost Abstractions

All `Model` implementations are generated at compile time via proc macros. No runtime reflection, no vtables, no hidden allocations. The generated code is as fast as hand-written implementations.

### 3. Structured Concurrency

Every async operation takes `&Cx` (capability context) and returns `Outcome<T, E>` instead of `Result`. This enables:
- Cancel-correct operations (no leaked resources)
- Budget/timeout enforcement
- Proper panic boundaries

### 4. Type Safety Over Convenience

The query builder validates at compile time when possible, and provides clear error messages when runtime checks are needed. We'd rather fail at compile time than corrupt your database.

### 5. Minimal Dependencies

Core crates keep dependencies tight:
- `asupersync` - Async runtime with structured concurrency
- `serde` / `serde_json` - Serialization
- `proc-macro2` / `quote` / `syn` - Macro support

Drivers and validation add focused dependencies where required (e.g., TLS/auth crypto, regex validation), but we still avoid heavyweight ORM/database stacks.

No tokio, no sqlx, no diesel, no sea-orm. We build what we need.

---

## How SQLModel Rust Compares

| Feature | SQLModel Rust | Diesel | SeaORM | sqlx |
|---------|---------------|--------|--------|------|
| Compile-time safety | ✅ Full | ✅ Full | ⚠️ Partial | ⚠️ Partial |
| Derive macros | ✅ Simple | ⚠️ Complex | ✅ Simple | ❌ None |
| Structured concurrency | ✅ Native | ❌ None | ❌ None | ❌ None |
| Multi-dialect | ✅ Postgres/SQLite/MySQL | ⚠️ Separate features | ✅ Yes | ✅ Yes |
| Dependencies | ✅ Minimal | 🐢 Heavy | 🐢 Heavy | ⚠️ Moderate |
| Learning curve | ✅ Low | ❌ Steep | ⚠️ Moderate | ✅ Low |

**When to use SQLModel Rust:**
- You want Python SQLModel's ergonomics in Rust
- You need cancel-correct async with structured concurrency
- You prefer compile-time errors over runtime surprises
- You're building from scratch and want minimal dependencies

**When SQLModel Rust might not fit:**
- You need an established ecosystem with extensive documentation
- You require immediate production readiness (we're in active development)
- You need implicit relationship traversal/lazy loading without explicit load calls (we provide `Lazy<T>` + `Session::load_lazy/load_many`, but we avoid hidden N+1 behavior)

---

## Installation

### From crates.io (recommended)

```toml
# Cargo.toml
[dependencies]
sqlmodel = "0.1.1"

# Choose a driver (pick one or more)
sqlmodel-postgres = "0.1.1"
# sqlmodel-mysql = "0.1.1"
# sqlmodel-sqlite = "0.1.1"

# Optional rich console output
sqlmodel-console = { version = "0.1.1", features = ["rich"] }
```

You do **not** need to add `asupersync` directly; the `Cx` and `Outcome` types are
re-exported from `sqlmodel` and `sqlmodel-core`.

### From Source

```bash
git clone https://github.com/sqlmodel/sqlmodel-rust.git
cd sqlmodel-rust

# Build the workspace
cargo build --workspace

# Run tests
cargo test --workspace
```

---

## Quick Start

### 1. Define Your Model

```rust
use sqlmodel::prelude::*;

#[derive(Model, Debug, Clone)]
struct User {
    #[sqlmodel(primary_key, auto_increment)]
    id: Option<i64>,

    #[sqlmodel(unique)]
    email: String,

    name: String,

    #[sqlmodel(default = "false")]
    is_active: bool,
}
```

### 2. Generate Schema

```rust
use sqlmodel_schema::SchemaBuilder;

let schema = SchemaBuilder::new()
    .create_table::<User>()
    .build();

// Generates:
// CREATE TABLE IF NOT EXISTS "users" (
//   "id" BIGINT AUTOINCREMENT,
//   "email" VARCHAR(255) NOT NULL,
//   "name" TEXT NOT NULL,
//   "is_active" BOOLEAN NOT NULL DEFAULT false,
//   PRIMARY KEY ("id"),
//   CONSTRAINT "uk_email" UNIQUE ("email")
// )
```

### 3. Build Queries

```rust
// SELECT
let users = select!(User)
    .filter(Expr::col("is_active").eq(true))
    .order_by(Expr::col("name").asc())
    .all(cx, &conn)
    .await?;

// INSERT
let new_user = User {
    id: None,
    email: "alice@example.com".into(),
    name: "Alice".into(),
    is_active: true,
};
let id = insert!(new_user).execute(cx, &conn).await?;

// UPDATE
let updated = update!(user)
    .filter(Expr::col("id").eq(1))
    .execute(cx, &conn)
    .await?;

// DELETE
let deleted = delete!(User)
    .filter(Expr::col("is_active").eq(false))
    .execute(cx, &conn)
    .await?;
```

---

## Console Output

SQLModel Rust includes an optional rich console output system for beautiful terminal feedback.

### Features

- **Styled error messages** with context, SQL highlighting, and suggestions
- **Formatted query result tables** with type-based coloring
- **Schema visualization** as interactive trees
- **Progress bars** for bulk operations
- **Agent-safe**: auto-detects AI coding tools (Claude Code, Codex, Cursor, Aider, etc.)

### Quick Setup

Add the console feature to your dependency:

```toml
[dependencies]
sqlmodel-console = { version = "0.1.1", features = ["rich"] }
```

Create and use a console:

```rust
use sqlmodel_console::{SqlModelConsole, OutputMode};
use sqlmodel_console::renderables::QueryResultTable;

// Auto-detect mode (rich for humans, plain for agents)
let console = SqlModelConsole::new();

// Display query results
let table = QueryResultTable::new()
    .columns(vec!["id", "name", "email"])
    .row(vec!["1", "Alice", "alice@example.com"])
    .timing_ms(12.34);

console.print_table(&table);
```

### Output Modes

| Mode | When Used | Output |
|------|-----------|--------|
| **Rich** | Human on TTY | Colors, tables, panels |
| **Plain** | Agent detected / piped | Parseable text |
| **JSON** | `SQLMODEL_JSON=1` | Structured JSON |

### Agent Compatibility

Console output is **agent-safe by default**. When running under Claude Code, Codex CLI, Cursor, or other AI coding tools, output automatically switches to plain text that agents can parse.

Environment variables for control:
- `SQLMODEL_PLAIN=1` - Force plain text mode
- `SQLMODEL_RICH=1` - Force rich mode (even for agents)
- `SQLMODEL_JSON=1` - Force JSON output

### Documentation

- [Console User Guide](docs/console/user-guide.md) - Complete feature guide
- [Agent Compatibility Guide](docs/console/agent-compatibility.md) - For agent authors

### Visual Examples

Run the example programs to preview both rich and plain output:

```bash
cargo run -p sqlmodel-console --example console_demo
cargo run -p sqlmodel-console --example error_showcase
cargo run -p sqlmodel-console --example query_results
cargo run -p sqlmodel-console --example progress_demo
cargo run -p sqlmodel-console --example schema_visualization
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        sqlmodel (facade)                         │
│            Re-exports all crates for easy import                 │
└─────────────────────────────────────────────────────────────────┘
                               │
       ┌───────────────┬───────────────┬───────────────┬───────────────┬───────────────┐
       ▼               ▼               ▼               ▼               ▼
┌─────────────┐  ┌───────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│sqlmodel-core│  │ sqlmodel-macros   │  │ sqlmodel-query  │  │ sqlmodel-schema │  │ sqlmodel-session│
│ Model trait │  │ #[derive(Model)]  │  │ Query builder  │  │ DDL + migration │  │ Unit of work    │
└─────────────┘  └───────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘
                               │
                 ┌─────────────┴─────────────┐
                 ▼                           ▼
          ┌─────────────┐           ┌─────────────────┐
          │sqlmodel-pool│           │sqlmodel-console │ (optional)
          │Conn pooling │           │Rich output      │
          └─────────────┘           └─────────────────┘
                 │
       ┌─────────┼─────────┬─────────┐
       ▼         ▼         ▼         ▼
sqlmodel-postgres sqlmodel-mysql sqlmodel-sqlite (drivers)
```

### Crate Responsibilities

| Crate | Purpose |
|-------|---------|
| `sqlmodel` | Facade crate—re-exports everything for `use sqlmodel::prelude::*` |
| `sqlmodel-core` | Core traits (`Model`, `Connection`), types (`Value`, `Row`, `Error`) |
| `sqlmodel-macros` | `#[derive(Model)]` proc macro with attribute parsing and code gen |
| `sqlmodel-query` | Type-safe query builder with multi-dialect support |
| `sqlmodel-schema` | DDL generation, schema builder, migration support |
| `sqlmodel-session` | Unit of work + identity map |
| `sqlmodel-pool` | Connection pooling with asupersync channels |
| `sqlmodel-postgres` | PostgreSQL wire protocol implementation |
| `sqlmodel-mysql` | MySQL wire protocol implementation |
| `sqlmodel-sqlite` | SQLite driver (FFI) |
| `sqlmodel-console` | Optional rich console output for humans and agents |

---

## Model Attributes Reference

```rust
#[derive(Model)]
#[sqlmodel(table = "custom_table_name")]  // Override table name
struct MyModel {
    #[sqlmodel(primary_key)]              // Part of primary key
    #[sqlmodel(auto_increment)]           // Auto-increment (usually with primary_key)
    #[sqlmodel(unique)]                   // UNIQUE constraint
    #[sqlmodel(nullable)]                 // Allow NULL values
    #[sqlmodel(column = "db_column")]     // Override column name
    #[sqlmodel(sql_type = "VARCHAR(100)")]// Override SQL type
    #[sqlmodel(default = "value")]        // DEFAULT clause
    #[sqlmodel(foreign_key = "table.col")]// FOREIGN KEY constraint
    #[sqlmodel(index)]                    // Create index on column
    #[sqlmodel(skip)]                     // Exclude from all DB operations
    field: Type,
}
```

### Automatic Type Mapping

| Rust Type | SQL Type |
|-----------|----------|
| `i8` | `TINYINT` |
| `i16` | `SMALLINT` |
| `i32` | `INTEGER` |
| `i64` | `BIGINT` |
| `f32` | `REAL` |
| `f64` | `DOUBLE PRECISION` |
| `bool` | `BOOLEAN` |
| `String` | `TEXT` |
| `char` | `CHAR(1)` |
| `Option<T>` | Nullable version of T |
| `Vec<u8>` | `BYTEA` / `BLOB` |
| `chrono::NaiveDate` | `DATE` |
| `chrono::NaiveDateTime` | `TIMESTAMP` |
| `uuid::Uuid` | `UUID` |

---

## Expression Builder Reference

```rust
use sqlmodel_query::Expr;

// Column references
Expr::col("name")                      // "name"
Expr::qualified("users", "name")       // "users"."name"

// Comparisons
Expr::col("age").eq(18)                // "age" = $1
Expr::col("age").ne(18)                // "age" != $1
Expr::col("age").gt(18)                // "age" > $1
Expr::col("age").ge(18)                // "age" >= $1
Expr::col("age").lt(18)                // "age" < $1
Expr::col("age").le(18)                // "age" <= $1

// Null checks
Expr::col("deleted").is_null()         // "deleted" IS NULL
Expr::col("name").is_not_null()        // "name" IS NOT NULL

// Pattern matching
Expr::col("name").like("%john%")       // "name" LIKE $1
Expr::col("email").ilike("%@GMAIL%")   // "email" ILIKE $1 (Postgres)

// Lists and ranges
Expr::col("status").in_list([1, 2, 3]) // "status" IN ($1, $2, $3)
Expr::col("age").between(18, 65)       // "age" BETWEEN $1 AND $2

// Logical operators
expr1.and(expr2)                       // (expr1) AND (expr2)
expr1.or(expr2)                        // (expr1) OR (expr2)
Expr::not(expr)                        // NOT (expr)

// Aggregates
Expr::count_star()                     // COUNT(*)
Expr::col("id").count()                // COUNT("id")
Expr::col("amount").sum()              // SUM("amount")
Expr::col("price").avg()               // AVG("price")
Expr::col("age").min()                 // MIN("age")
Expr::col("age").max()                 // MAX("age")

// CASE expressions
Expr::case()
    .when(Expr::col("status").eq("active"), "Yes")
    .when(Expr::col("status").eq("pending"), "Maybe")
    .otherwise("No")
```

---

## Limitations

### Implementation Status

| Capability | Status | Notes |
|------------|--------|-------|
| Query execution | ✅ Complete | Full SELECT/INSERT/UPDATE/DELETE with eager loading |
| Connection pooling | ✅ Complete | Generic pool with timeouts, health checks, metrics |
| Transactions | ✅ Complete | BEGIN/COMMIT/ROLLBACK with savepoint support |
| SQLite driver | ✅ Complete | Full Connection trait with transactions |
| MySQL driver | ✅ Complete | Wire protocol + SharedMySqlConnection |
| PostgreSQL driver | ✅ Complete | Wire protocol + SharedPgConnection with SCRAM auth |
| Runtime migrations | ✅ Complete | Schema diffing, migration runner, version tracking |
| Lazy loading | ✅ Explicit | `Lazy<T>` + `Session::load_lazy/load_many` (batch-friendly; no implicit N+1) |

### Known Limitations

- **Nightly Rust required**: We use Edition 2024 features
- **No stable release yet**: API may change
- **Limited documentation**: We're working on it
- **asupersync dependency**: Pulled via git for now (requires git access during builds)

---

## Troubleshooting

### "Failed to fetch git dependency `asupersync`"

```bash
# Ensure git can reach GitHub and retry
export CARGO_NET_GIT_FETCH_WITH_CLI=true
cargo update -p asupersync
cargo build
```

### "error[E0658]: edition 2024 is unstable"

```bash
# Ensure you're on nightly
rustup default nightly
rustup update nightly
```

### Clippy warnings about `unsafe_code`

The workspace has `unsafe_code = "warn"` by default. If you need unsafe code (e.g., for FFI), use `#[allow(unsafe_code)]` locally.

### Build takes forever

```bash
# Use sccache for faster rebuilds
cargo install sccache
export RUSTC_WRAPPER=sccache
cargo build
```

---

## FAQ

### Why "SQLModel Rust" and not just use Diesel/SeaORM?

We wanted Python SQLModel's simplicity with Rust's safety. Diesel is powerful but has a steep learning curve. SeaORM is good but uses runtime async. We built SQLModel Rust for structured concurrency with asupersync from the ground up.

### Why build your own PostgreSQL driver?

Control. We need deep integration with asupersync's capability context for cancel-correct operations. Existing drivers don't support our concurrency model.

### Is this production-ready?

Nearly. Core functionality is complete: query execution, connection pooling, transactions, and drivers for PostgreSQL, MySQL, and SQLite all work. However, the API may still change before 1.0, and test coverage for edge cases is ongoing.

### Does it work with tokio?

No. We use asupersync exclusively. Tokio's model doesn't support structured concurrency the way we need.

### Can I use this without async?

Not currently. The entire design assumes async operations with capability contexts.

---

## About Contributions

Please don't take this the wrong way, but I do not accept outside contributions for any of my projects. I simply don't have the mental bandwidth to review anything, and it's my name on the thing, so I'm responsible for any problems it causes; thus, the risk-reward is highly asymmetric from my perspective. I'd also have to worry about other "stakeholders," which seems unwise for tools I mostly make for myself for free. Feel free to submit issues, and even PRs if you want to illustrate a proposed fix, but know I won't merge them directly. Instead, I'll have Claude or Codex review submissions via `gh` and independently decide whether and how to address them. Bug reports in particular are welcome. Sorry if this offends, but I want to avoid wasted time and hurt feelings. I understand this isn't in sync with the prevailing open-source ethos that seeks community contributions, but it's the only way I can move at this velocity and keep my sanity.

---

## License

MIT License. See [LICENSE](LICENSE).
