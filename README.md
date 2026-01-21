# SQLModel Rust

<div align="center">

**SQL databases in Rust, designed to be intuitive and type-safe.**

[![CI](https://github.com/sqlmodel/sqlmodel-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/sqlmodel/sqlmodel-rust/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)
[![Rust: Nightly](https://img.shields.io/badge/Rust-nightly-orange.svg)](https://www.rust-lang.org/)

A first-principles Rust port of Python's [SQLModel](https://sqlmodel.tiangolo.com/) library.

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
| **Minimal dependencies** | Only serde + asupersync—no tokio, no sqlx, no diesel |

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

Only these dependencies are allowed:
- `asupersync` - Async runtime with structured concurrency
- `serde` / `serde_json` - Serialization
- `proc-macro2` / `quote` / `syn` - Macro support

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
- You need advanced ORM features like lazy loading or complex relationship traversal

---

## Installation

### From Source (Currently the only option)

```bash
git clone https://github.com/sqlmodel/sqlmodel-rust.git
cd sqlmodel-rust

# Build the workspace
cargo build --workspace

# Run tests
cargo test --workspace
```

### Add to Your Project

```toml
# Cargo.toml
[dependencies]
sqlmodel = { git = "https://github.com/sqlmodel/sqlmodel-rust.git" }

# You'll also need asupersync
asupersync = { git = "https://github.com/Dicklesworthstone/asupersync.git" }
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

## Console Examples

SQLModel Rust ships a `sqlmodel-console` crate with visual renderables. You can run
example programs to preview both rich and plain output:

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
       ┌───────────────────────┼───────────────────────┐
       │                       │                       │
       ▼                       ▼                       ▼
┌─────────────┐    ┌───────────────────┐    ┌─────────────────┐
│sqlmodel-core│    │ sqlmodel-macros   │    │ sqlmodel-query  │
│             │    │                   │    │                 │
│ Model trait │◄───│ #[derive(Model)]  │    │ Type-safe SQL   │
│ Value/Row   │    │ Attribute parsing │    │ SELECT/INSERT   │
│ Error types │    │ SQL type infer    │    │ UPDATE/DELETE   │
│ Connection  │    │ Code generation   │    │ Expr builder    │
└─────────────┘    └───────────────────┘    └─────────────────┘
       │                                            │
       │           ┌───────────────────┐           │
       │           │  sqlmodel-schema  │           │
       └──────────►│                   │◄──────────┘
                   │ CREATE TABLE gen  │
                   │ Migration runner  │
                   │ Schema builder    │
                   └───────────────────┘
                            │
       ┌────────────────────┼────────────────────┐
       │                    │                    │
       ▼                    ▼                    ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────────┐
│sqlmodel-pool│    │sqlmodel-    │    │ Future drivers  │
│             │    │postgres     │    │                 │
│ Conn pooling│    │             │    │ SQLite, MySQL   │
│ Health check│    │ Wire proto  │    │                 │
│ Budget-aware│    │ SCRAM auth  │    │                 │
└─────────────┘    └─────────────┘    └─────────────────┘
```

### Crate Responsibilities

| Crate | Purpose |
|-------|---------|
| `sqlmodel` | Facade crate—re-exports everything for `use sqlmodel::prelude::*` |
| `sqlmodel-core` | Core traits (`Model`, `Connection`), types (`Value`, `Row`, `Error`) |
| `sqlmodel-macros` | `#[derive(Model)]` proc macro with attribute parsing and code gen |
| `sqlmodel-query` | Type-safe query builder with multi-dialect support |
| `sqlmodel-schema` | DDL generation, schema builder, migration support |
| `sqlmodel-pool` | Connection pooling with asupersync channels |
| `sqlmodel-postgres` | PostgreSQL wire protocol implementation |

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

### What SQLModel Rust Doesn't Do (Yet)

| Capability | Status | Notes |
|------------|--------|-------|
| Query execution | 🚧 In Progress | Protocol implemented, connection pending |
| Connection pooling | 🚧 Planned | Waiting for asupersync channels |
| Transactions | 🚧 Planned | Part of connection layer |
| SQLite driver | 📋 Planned | After Postgres is complete |
| MySQL driver | 📋 Planned | After SQLite |
| Lazy loading | ❌ Not Planned | Explicit joins only—by design |
| Runtime migrations | 🚧 Planned | Schema diffing in progress |

### Known Limitations

- **Nightly Rust required**: We use Edition 2024 features
- **No stable release yet**: API may change
- **Limited documentation**: We're working on it
- **asupersync dependency**: Must be cloned as sibling directory

---

## Troubleshooting

### "Can't find crate `asupersync`"

```bash
# Clone asupersync as a sibling directory
cd ..
git clone https://github.com/Dicklesworthstone/asupersync.git asupersync
cd sqlmodel_rust
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

Not yet. We're in active development (Phase 1). The query builder and schema generation are solid, but query execution is still being connected.

### Does it work with tokio?

No. We use asupersync exclusively. Tokio's model doesn't support structured concurrency the way we need.

### Can I use this without async?

Not currently. The entire design assumes async operations with capability contexts.

---

## About Contributions

Please don't take this the wrong way, but I do not accept outside contributions for any of my projects. I simply don't have the mental bandwidth to review anything, and it's my name on the thing, so I'm responsible for any problems it causes; thus, the risk-reward is highly asymmetric from my perspective. I'd also have to worry about other "stakeholders," which seems unwise for tools I mostly make for myself for free. Feel free to submit issues, and even PRs if you want to illustrate a proposed fix, but know I won't merge them directly. Instead, I'll have Claude or Codex review submissions via `gh` and independently decide whether and how to address them. Bug reports in particular are welcome. Sorry if this offends, but I want to avoid wasted time and hurt feelings. I understand this isn't in sync with the prevailing open-source ethos that seeks community contributions, but it's the only way I can move at this velocity and keep my sanity.

---

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
