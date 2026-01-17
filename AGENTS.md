# AGENTS.md - Guidelines for AI Coding Agents

## Project Overview

**SQLModel Rust** is a first-principles Rust port of Python's SQLModel library. The goal is to provide the same developer experience (intuitive, type-safe SQL operations) while leveraging Rust's performance and safety guarantees.

### Key Principles

1. **First-principles implementation** - Extract behavior spec from legacy code, then implement fresh in Rust
2. **Never translate line-by-line** - Python idioms don't map to Rust; design for Rust from the start
3. **Minimal dependencies** - Only asupersync + serde (+ proc-macro crates for macros)
4. **Zero-cost abstractions** - Compile-time code generation, no runtime reflection
5. **Structured concurrency** - All async operations go through asupersync's `Cx` context

## Rule #1: NO FILE DELETION

**ABSOLUTELY FORBIDDEN** without explicit user permission:
- `rm` on any project file
- `git reset --hard`
- `git clean -fd`
- Any operation that permanently destroys work

If you need to remove a file, ASK FIRST.

## Project Structure

```
sqlmodel_rust/
├── Cargo.toml                    # Workspace root
├── rust-toolchain.toml           # Nightly requirement
├── AGENTS.md                     # This file
├── PLAN_TO_PORT_SQLMODEL_TO_RUST.md  # Porting strategy
├── EXISTING_SQLMODEL_STRUCTURE.md    # Extracted spec (TODO)
├── PROPOSED_RUST_ARCHITECTURE.md     # Rust design (TODO)
├── legacy_sqlmodel/              # Python SQLModel reference
├── legacy_pydantic/              # Python Pydantic reference
├── legacy_sqlalchemy/            # Python SQLAlchemy reference
└── crates/
    ├── sqlmodel/                 # Main facade crate
    ├── sqlmodel-core/            # Core types and traits
    ├── sqlmodel-macros/          # Proc macros (Model, Validate)
    ├── sqlmodel-query/           # Query builder
    ├── sqlmodel-schema/          # Schema/migration support
    └── sqlmodel-pool/            # Connection pooling
```

## Legacy Code Reference

The `legacy_*` directories contain cloned Python repositories for SPECIFICATION EXTRACTION ONLY:

- `legacy_sqlmodel/` - Main SQLModel library (built on Pydantic + SQLAlchemy)
- `legacy_pydantic/` - Data validation library
- `legacy_sqlalchemy/` - SQL toolkit and ORM

**Use these to extract BEHAVIORS, not to translate code.**

### Why We DON'T Port Pydantic/SQLAlchemy as Separate Crates

In Python, SQLModel depends on Pydantic + SQLAlchemy because Python lacks compile-time types and macros. **Rust has these natively:**

| Python Needs Library For | Rust Has Built-In |
|--------------------------|-------------------|
| Runtime type validation (Pydantic) | Compile-time type system |
| JSON ser/de (Pydantic) | `serde` ecosystem |
| Field metadata (Pydantic) | Proc macro attributes |
| Connection abstraction (SQLAlchemy) | Traits + generics |
| Query building (SQLAlchemy) | Type-safe builders |
| ORM mapping (SQLAlchemy) | `#[derive(Model)]` macro |

**We implement the COMBINED functionality directly in sqlmodel-rust crates.**

The legacy repos help us understand:
1. What SQL to generate for each operation
2. What edge cases exist (null handling, type coercion)
3. What the user-facing API should feel like

We do NOT create separate `pydantic-rust` or `sqlalchemy-rust` crates.

## Porting Methodology

### The Three Documents

1. **PLAN_TO_PORT_SQLMODEL_TO_RUST.md** - Scope, exclusions, phases
2. **EXISTING_SQLMODEL_STRUCTURE.md** - Complete behavior specification (THE SPEC)
3. **PROPOSED_RUST_ARCHITECTURE.md** - Rust design decisions

### Extraction Process

1. Read legacy code to understand BEHAVIOR
2. Document exact:
   - Data structures with ALL fields
   - Validation rules with exact conditions
   - SQL generation logic
   - Error cases and messages
3. Write to EXISTING_SQLMODEL_STRUCTURE.md
4. **After spec is complete, implement from spec, NOT legacy code**

## Toolchain Requirements

- **Rust**: Nightly (required for Edition 2024)
- **Edition**: 2024
- **Minimum Rust version**: 1.85+

### Build Commands (MANDATORY)

Before committing ANY code changes, run:

```bash
# Check for compiler errors
cargo check --all-targets

# Lint with Clippy
cargo clippy --all-targets -- -D warnings

# Verify formatting
cargo fmt --check
```

## Minimal Dependency Stack

**ONLY these dependencies are allowed:**

| Crate | Purpose |
|-------|---------|
| `asupersync` | Async runtime with structured concurrency |
| `serde` | Serialization/deserialization |
| `serde_json` | JSON support |
| `proc-macro2` | Proc macro support (macros crate only) |
| `quote` | Proc macro code generation |
| `syn` | Proc macro parsing |

**NOT allowed:**
- `tokio` (use asupersync)
- `sqlx` (build custom)
- `diesel` (build custom)
- `sea-orm` (build custom)
- Any ORM/database crate

## asupersync Integration

All database operations must:

1. Take `&Cx` as first parameter
2. Return `Outcome<T, E>` (not `Result`)
3. Support cancellation via `cx.checkpoint()`
4. Respect budget/timeout via `cx.budget()`

Example:
```rust
pub async fn query(
    &self,
    cx: &Cx,
    sql: &str,
    params: &[Value],
) -> Outcome<Vec<Row>, Error> {
    cx.checkpoint()?;  // Early cancellation check
    // ... implementation
}
```

## Code Quality Guidelines

### Do

- Use `const fn` where possible
- Prefer zero-copy operations
- Use compile-time validation (proc macros)
- Write clear error messages
- Test with asupersync's `LabRuntime`

### Don't

- Use runtime reflection
- Allocate unnecessarily
- Ignore cancellation signals
- Use `unwrap()` in library code
- Translate Python patterns to Rust

## Session Completion Checklist

Before ending a session:

1. [ ] All changes compile: `cargo check --all-targets`
2. [ ] Clippy passes: `cargo clippy --all-targets -- -D warnings`
3. [ ] Code formatted: `cargo fmt`
4. [ ] Commit changes with descriptive message
5. [ ] Update relevant documentation
6. [ ] Note any incomplete work in commit message

## Implementation Phases

### Phase 0: Foundation (CURRENT)
- [x] Workspace structure
- [x] Core types (Value, Row, Error)
- [x] Model trait definition
- [x] Query builder skeleton
- [x] asupersync integration

### Phase 1: Core Query Operations
- [ ] Model derive macro implementation
- [ ] SELECT query execution
- [ ] INSERT/UPDATE/DELETE operations
- [ ] Basic type conversions

### Phase 2: Schema & Migrations
- [ ] CREATE TABLE generation
- [ ] Migration runner
- [ ] Database introspection

### Phase 3: Connection Pooling
- [ ] Pool implementation with asupersync channels
- [ ] Connection lifecycle management
- [ ] Health checks

### Phase 4: Advanced Features
- [ ] Relationships (foreign keys)
- [ ] Transactions
- [ ] Validation derive macro

### Phase 5: Database Drivers
- [ ] SQLite driver
- [ ] PostgreSQL driver
- [ ] MySQL driver

## Questions?

If unsure about:
- **Architecture decisions** → Check PROPOSED_RUST_ARCHITECTURE.md
- **Python behavior** → Check EXISTING_SQLMODEL_STRUCTURE.md
- **What to exclude** → Check PLAN_TO_PORT_SQLMODEL_TO_RUST.md
- **asupersync usage** → Check /data/projects/asupersync

When in doubt, ASK before implementing.
