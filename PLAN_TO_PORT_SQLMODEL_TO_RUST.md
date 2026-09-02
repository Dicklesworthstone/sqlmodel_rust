# Plan to Port SQLModel to Rust

## Executive Summary

SQLModel is a Python library that combines Pydantic (data validation) and SQLAlchemy (SQL toolkit/ORM) to provide intuitive, type-safe database operations. This document outlines the strategy for porting SQLModel to Rust while preserving its developer experience and enhancing performance.

## Why We Don't Port Pydantic/SQLAlchemy Separately

**Critical insight:** In Python, SQLModel depends on Pydantic and SQLAlchemy because Python lacks:
- Compile-time type checking (Pydantic provides runtime validation)
- Zero-cost abstractions (SQLAlchemy abstracts away database differences)
- Powerful macro system (everything done via metaclasses/decorators)

**In Rust, we have all of these natively:**

| Python Library | What It Provides | Rust Native Equivalent |
|----------------|------------------|------------------------|
| **Pydantic** | Runtime type validation | Rust's type system (compile-time) |
| **Pydantic** | JSON serialization | `serde` + `serde_json` |
| **Pydantic** | Field metadata | Proc macro attributes |
| **SQLAlchemy Core** | Connection management | Our `sqlmodel-core` |
| **SQLAlchemy Core** | Query building | Our `sqlmodel-query` |
| **SQLAlchemy ORM** | Model→Table mapping | Our `#[derive(Model)]` macro |
| **SQLAlchemy ORM** | Session/UoW | Explicit transactions (simpler!) |
| **SQLAlchemy** | Migrations | Our `sqlmodel-schema` |
| **SQLAlchemy** | Connection pooling | Our `sqlmodel-pool` |

**The legacy repos are REFERENCE ONLY** - we study them to understand:
1. What SQL each operation should generate
2. What edge cases exist
3. What the user-facing API should feel like

We do NOT translate their code. We implement the *essence* directly in idiomatic Rust.

## Value Propositions to Preserve

1. **Intuitive API** - Define models as simple structs with derive macros
2. **Type Safety** - Compile-time checks for queries and data access
3. **Single Definition** - One struct for validation, serialization, AND database mapping
4. **Minimal Boilerplate** - Derive macros generate all the glue code
5. **Flexible Queries** - Both ORM-style and raw SQL supported

## Architecture Overview

### Crate Structure

```
sqlmodel (facade)
├── sqlmodel-core (types, traits)
├── sqlmodel-macros (derive macros)
├── sqlmodel-query (query builder)
├── sqlmodel-schema (DDL, migrations)
└── sqlmodel-pool (connection pooling)
```

### Dependency Stack

**Required:**
- `asupersync` - Async runtime with structured concurrency
- `serde` / `serde_json` - Serialization
- `proc-macro2` / `quote` / `syn` - Macro support

**NOT using:**
- `tokio` - Replaced by asupersync
- `sqlx` - Building custom for zero-copy and asupersync integration
- `diesel` - Different design philosophy
- `sea-orm` - Too much runtime overhead

## Scope And Parity

Earlier drafts of this document scoped out a number of Python SQLModel/Pydantic/SQLAlchemy behaviors.
The current project goal is **feature-for-feature parity** with the legacy Python SQLModel library.

Treat the items below as historical notes, not policy: if something is missing, it should be implemented
or explicitly justified and tracked (see `FEATURE_PARITY.md` and Beads issues).

### 1. Python Runtime Introspection
- **Python**: Uses `__annotations__`, `get_type_hints()`, runtime type inspection
- **Rust**: Use proc macros for compile-time code generation

### 2. Pydantic Integration
- **Python**: SQLModel inherits from both `BaseModel` and SQLAlchemy
- **Rust**: Separate `Model` and `Validate` derive macros using serde

### 3. SQLAlchemy Session
- **Python**: Complex session/unit-of-work pattern
- **Rust**: Direct connection operations, explicit transactions

### 4. Backward Compatibility
- No need to support legacy APIs or deprecated features
- Design for Rust idioms from the start

### 5. Relationship Lazy Loading
- **Python**: Automatic lazy loading of relationships
- **Rust**: Explicit eager loading with joins (no magic)

### 6. Dynamic Query Construction
- **Python**: Supports building queries at runtime from strings
- **Rust**: Type-safe query builder only (raw SQL available for escape hatch)

### 7. Multiple Database Dialects (Initially)
- Phase 1: SQLite only
- Phase 2: PostgreSQL
- Phase 3: MySQL

### 8. Alembic-style Migrations
- Simpler migration system without auto-generation
- Explicit up/down SQL scripts

### 9. Async Session Management
- **Python**: Complex async session context managers
- **Rust**: Explicit connection passing with asupersync Cx

### 10. Field Aliases
- **Python**: `Field(alias="...")` for different JSON/DB names
- **Rust**: Use serde's `#[serde(rename = "...")]` separately

### 11. Validators/Serializers
- **Python**: `@field_validator`, `@model_validator`, `@field_serializer`
- **Rust**: Separate validation trait with explicit methods

### 12. Computed Fields
- **Python**: `@computed_field` for derived values
- **Rust**: Use regular methods on the struct

### 13. Generic Models
- **Python**: `SQLModel[T]` generic support
- **Rust**: Use concrete types or trait bounds

### 14. JSON Schema Generation
- Defer to separate crate or manual implementation
- Not core to database operations

### 15. Discriminated Unions for Inheritance
- **Python**: Complex inheritance patterns
- **Rust**: Use enums or composition instead

## Co-development with asupersync

SQLModel Rust depends on asupersync for:

| Feature | asupersync Component |
|---------|---------------------|
| Async operations | `Cx` capability context |
| Cancellation | `cx.checkpoint()`, `Outcome::Cancelled` |
| Timeouts | `Budget` |
| Connection pool | Channels (when available) |
| Testing | `LabRuntime` |

### Current asupersync Status

asupersync 0.4.x (workspace requirement `^0.4.9`, lock at 0.4.10 as of 2026-09-02) provides everything this plan depended on:

| Component | Status | Impact on SQLModel |
|-----------|--------|-------------------|
| Scheduler | ✅ Done | Runs all async ops |
| Cx context | ✅ Done | Threaded through every async API |
| Channels | ✅ Done | Pool implementation |
| TCP/IO | ✅ Done | Postgres and MySQL wire drivers ship on it |
| LabRuntime (virtual time, DPOR) | ✅ Available | Not yet used by this workspace's tests; adopting it is bd-x6jl |

## Phased Implementation

All six phases below are implemented (crate in parentheses). What remains is proof and hardening, tracked in Beads under `bd-162`.

### Phase 0: Foundation (COMPLETE)
- Workspace setup
- Core type definitions
- Query builder skeleton
- asupersync integration patterns

### Phase 1: Core Operations (COMPLETE — `sqlmodel-macros`, `sqlmodel-query`, `sqlmodel-core`)
1. Model derive macro (full implementation)
2. SELECT with type conversion
3. INSERT/UPDATE/DELETE
4. Transaction support

### Phase 2: Schema (COMPLETE — `sqlmodel-schema`; end-to-end runner proof is bd-slot.7)
1. CREATE TABLE generation
2. Migration tracking table
3. Migration execution
4. Database introspection

### Phase 3: Pooling (COMPLETE — `sqlmodel-pool`)
1. Connection pool using asupersync channels
2. Health checks
3. Connection recycling

### Phase 4: Validation (COMPLETE — `#[derive(Validate)]`)
1. Validate derive macro
2. Constraint checking
3. Error message generation

### Phase 5: SQLite Driver (COMPLETE — `sqlmodel-sqlite` (C) and `sqlmodel-frankensqlite` (pure Rust))
1. SQLite protocol implementation
2. Type mapping
3. Zero-copy optimizations

### Phase 6: PostgreSQL Driver (COMPLETE — `sqlmodel-postgres`; MySQL followed as `sqlmodel-mysql`)
1. PostgreSQL protocol
2. Binary format support
3. Array types

## Success Criteria

None of the quantitative targets has been measured yet; measuring them is bd-4ttf (benches, CI size/startup job). Until then the table records targets only.

| Metric | Target | Measured |
|--------|--------|----------|
| Code size | 10-20x smaller than Python SQLModel | Not measured (the workspace is ~93k lines of Rust; the claim is likely false and will be recorded honestly) |
| Binary size | < 5MB with LTO | Not measured (bd-4ttf.2) |
| Startup time | < 10ms | Not measured (bd-4ttf.2) |
| Query latency | Competitive with native drivers | Not measured (bd-4ttf.1) |
| Type safety | 100% compile-time checked | Holds by construction for models and builders; raw SQL escape hatches are runtime-checked |

## Example API (Target)

```rust
use sqlmodel::prelude::*;

#[derive(Model, Debug, Clone)]
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

async fn example(cx: &Cx, conn: &impl Connection) -> Outcome<(), Error> {
    // Create table
    conn.execute(cx, &create_table::<Hero>().build(), &[]).await?;

    // Insert
    let hero = Hero {
        id: None,
        name: "Spider-Man".into(),
        secret_name: "Peter Parker".into(),
        age: Some(25),
        team_id: None,
    };
    let id = insert!(hero).execute(cx, conn).await?;

    // Query
    let heroes = select!(Hero)
        .filter(Expr::col("age").gt(18))
        .order_by(OrderBy::asc("name"))
        .limit(10)
        .all(cx, conn)
        .await?;

    // Transaction
    let tx = conn.begin(cx).await?;
    tx.execute(cx, "UPDATE heroes SET age = age + 1", &[]).await?;
    tx.commit(cx).await?;

    Outcome::Ok(())
}
```

## Open Questions

1. **Relationship handling** - Should we support `Vec<Related>` fields?
2. **Connection string parsing** - Build custom or use existing crate?
3. **SSL/TLS** - Use rustls or native-tls?
4. **Prepared statements** - Cache at connection or pool level?

## Next Steps

1. Complete Model derive macro implementation
2. Extract full spec from legacy Python code
3. Implement SELECT query execution
4. Write comprehensive tests using LabRuntime
