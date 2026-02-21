# AGENTS.md — sqlmodel_rust

> Guidelines for AI coding agents working in this Rust codebase.

---

## RULE 0 - THE FUNDAMENTAL OVERRIDE PREROGATIVE

If I tell you to do something, even if it goes against what follows below, YOU MUST LISTEN TO ME. I AM IN CHARGE, NOT YOU.

---

## RULE NUMBER 1: NO FILE DELETION

**YOU ARE NEVER ALLOWED TO DELETE A FILE WITHOUT EXPRESS PERMISSION.** Even a new file that you yourself created, such as a test code file. You have a horrible track record of deleting critically important files or otherwise throwing away tons of expensive work. As a result, you have permanently lost any and all rights to determine that a file or folder should be deleted.

**YOU MUST ALWAYS ASK AND RECEIVE CLEAR, WRITTEN PERMISSION BEFORE EVER DELETING A FILE OR FOLDER OF ANY KIND.**

---

## Irreversible Git & Filesystem Actions — DO NOT EVER BREAK GLASS

1. **Absolutely forbidden commands:** `git reset --hard`, `git clean -fd`, `rm -rf`, or any command that can delete or overwrite code/data must never be run unless the user explicitly provides the exact command and states, in the same message, that they understand and want the irreversible consequences.
2. **No guessing:** If there is any uncertainty about what a command might delete or overwrite, stop immediately and ask the user for specific approval. "I think it's safe" is never acceptable.
3. **Safer alternatives first:** When cleanup or rollbacks are needed, request permission to use non-destructive options (`git status`, `git diff`, `git stash`, copying to backups) before ever considering a destructive command.
4. **Mandatory explicit plan:** Even after explicit user authorization, restate the command verbatim, list exactly what will be affected, and wait for a confirmation that your understanding is correct. Only then may you execute it—if anything remains ambiguous, refuse and escalate.
5. **Document the confirmation:** When running any approved destructive command, record (in the session notes / final response) the exact user text that authorized it, the command actually run, and the execution time. If that record is absent, the operation did not happen.

---

## Git Branch: ONLY Use `main`, NEVER `master`

**The default branch is `main`. The `master` branch exists only for legacy URL compatibility.**

- **All work happens on `main`** — commits, PRs, feature branches all merge to `main`
- **Never reference `master` in code or docs** — if you see `master` anywhere, it's a bug that needs fixing
- **The `master` branch must stay synchronized with `main`** — after pushing to `main`, also push to `master`:
  ```bash
  git push origin main:master
  ```

**If you see `master` referenced anywhere:**
1. Update it to `main`
2. Ensure `master` is synchronized: `git push origin main:master`

---

## Toolchain: Rust & Cargo

We only use **Cargo** in this project, NEVER any other package manager.

- **Edition:** Rust 2024 (nightly required — see `rust-toolchain.toml`)
- **Dependency versions:** Explicit versions for stability
- **Configuration:** Cargo.toml workspace with `workspace = true` pattern
- **Unsafe code:** Warned (`#![warn(unsafe_code)]` via workspace lints)

### Async Runtime: asupersync (MANDATORY — NO TOKIO)

**This project uses [asupersync](/dp/asupersync) exclusively for all async/concurrent operations. Tokio and the entire tokio ecosystem are FORBIDDEN.**

- **Structured concurrency**: `Cx`, `Scope`, `region()` — no orphan tasks
- **Cancel-correct channels**: Two-phase `reserve()/send()` — no data loss on cancellation
- **Sync primitives**: `asupersync::sync::Mutex`, `RwLock`, `OnceCell`, `Pool` — cancel-aware
- **Deterministic testing**: `LabRuntime` with virtual time, DPOR, oracles
- **Native HTTP**: `asupersync::http::h1` for network operations (replaces reqwest)

**Forbidden crates**: `tokio`, `hyper`, `reqwest`, `axum`, `tower` (tokio adapter), `async-std`, `smol`, or any crate that transitively depends on tokio.

**Pattern**: All async functions take `&Cx` as first parameter. All database operations return `Outcome<T, E>` (not `Result`). The `Cx` flows down from the consumer's runtime — sqlmodel does NOT create its own runtime.

### Key Dependencies

| Crate | Purpose |
|-------|---------|
| `asupersync` | Structured async runtime (channels, sync, regions, HTTP, testing) |
| `serde` + `serde_json` | Serialization |
| `proc-macro2` + `quote` + `syn` | Proc macro code generation (sqlmodel-macros only) |
| `regex` | Compile-time and runtime validation patterns |
| `tracing` | Structured logging and diagnostics |
| `sha1` + `sha2` + `hmac` + `pbkdf2` | Database authentication (PostgreSQL, MySQL) |
| `rand` | Auth nonce generation |
| `md5` | PostgreSQL MD5 authentication |
| `rsa` | MySQL RSA authentication (`caching_sha2_password`/`sha256_password`) |
| `rustls` + `webpki-roots` | Optional TLS for PostgreSQL and MySQL |
| `rich_rust` | Optional rich terminal output (sqlmodel-console) |
| `fsqlite` + `fsqlite-core` + `fsqlite-types` | FrankenSQLite pure-Rust SQLite driver |
| `thiserror` | Ergonomic error type derivation |

**NOT allowed**: `tokio`, `sqlx`, `diesel`, `sea-orm`, or any ORM/database crate.

### Release Profile

The release build optimizes for binary size (libraries used in embedded/CLI contexts):

```toml
[profile.release]
opt-level = "z"     # Optimize for size
lto = true          # Link-time optimization
codegen-units = 1   # Single codegen unit for better optimization
panic = "abort"     # Abort on panic (no unwinding)
strip = true        # Remove debug symbols
```

---

## Code Editing Discipline

### No Script-Based Changes

**NEVER** run a script that processes/changes code files in this repo. Brittle regex-based transformations create far more problems than they solve.

- **Always make code changes manually**, even when there are many instances
- For many simple changes: use parallel subagents
- For subtle/complex changes: do them methodically yourself

### No File Proliferation

If you want to change something or add a feature, **revise existing code files in place**.

**NEVER** create variations like:
- `mainV2.rs`
- `main_improved.rs`
- `main_enhanced.rs`

New files are reserved for **genuinely new functionality** that makes zero sense to include in any existing file. The bar for creating new files is **incredibly high**.

---

## Backwards Compatibility

We do not care about backwards compatibility—we're in early development with no users. We want to do things the **RIGHT** way with **NO TECH DEBT**.

- Never create "compatibility shims"
- Never create wrapper functions for deprecated APIs
- Just fix the code directly

---

## Compiler Checks (CRITICAL)

**After any substantive code changes, you MUST verify no errors were introduced:**

```bash
# Check for compiler errors and warnings (workspace-wide)
cargo check --workspace --all-targets

# Check for clippy lints (pedantic + nursery are enabled)
cargo clippy --workspace --all-targets -- -D warnings

# Verify formatting
cargo fmt --check
```

If you see errors, **carefully understand and resolve each issue**. Read sufficient context to fix them the RIGHT way.

---

## Testing

### Testing Policy

Every component crate includes inline `#[cfg(test)]` unit tests alongside the implementation. Tests must cover:
- Happy path
- Edge cases (empty input, max values, boundary conditions)
- Error conditions

Cross-component integration tests live in the workspace `tests/` directory.

### Unit Tests

```bash
# Run all tests across the workspace
cargo test --workspace

# Run with output
cargo test --workspace -- --nocapture

# Run tests for a specific crate
cargo test -p sqlmodel-core
cargo test -p sqlmodel-macros
cargo test -p sqlmodel-query
cargo test -p sqlmodel-schema
cargo test -p sqlmodel-session
cargo test -p sqlmodel-pool
cargo test -p sqlmodel-sqlite
cargo test -p sqlmodel-postgres
cargo test -p sqlmodel-mysql
cargo test -p sqlmodel-console
cargo test -p sqlmodel-frankensqlite

# Run tests with all features enabled
cargo test --workspace --all-features
```

### Test Categories

| Crate | Focus Areas |
|-------|-------------|
| `sqlmodel-core` | Value types, Row operations, Error types, Model trait contracts, Field metadata, Validation rules, SqlType mappings, Dialect-specific behavior |
| `sqlmodel-macros` | `#[derive(Model)]` code generation, `#[derive(Validate)]` expansion, attribute parsing, compile-time validation |
| `sqlmodel-query` | Query builder DSL, SELECT/INSERT/UPDATE/DELETE generation, parameterized queries, dialect-specific SQL |
| `sqlmodel-schema` | CREATE TABLE generation, migration runner, database introspection, foreign key DDL |
| `sqlmodel-session` | Unit-of-work lifecycle, Session CRUD operations, transaction management, identity map |
| `sqlmodel-pool` | Connection pooling, lifecycle management, health checks, pool sizing |
| `sqlmodel-sqlite` | SQLite wire protocol, C SQLite FFI, prepared statements, type coercion |
| `sqlmodel-postgres` | PostgreSQL wire protocol, authentication (MD5, SCRAM-SHA-256), type OIDs, TLS |
| `sqlmodel-mysql` | MySQL wire protocol, authentication (native, caching_sha2, sha256), RSA auth, TLS |
| `sqlmodel-frankensqlite` | Pure-Rust SQLite adapter, MVCC concurrency, behavioral differences from C SQLite |
| `sqlmodel-console` | Rich terminal output formatting, agent vs human detection, SQL syntax highlighting |

---

## Third-Party Library Usage

If you aren't 100% sure how to use a third-party library, **SEARCH ONLINE** to find the latest documentation and current best practices.

---

## sqlmodel_rust — This Project

**This is the project you're working on.** sqlmodel_rust is a first-principles Rust port of Python's SQLModel library. It provides the same developer experience (intuitive, type-safe SQL operations) while leveraging Rust's performance and safety guarantees.

### What It Does

Provides a type-safe ORM and query builder for Rust with derive macros (`#[derive(Model)]`), async database operations via asupersync's structured concurrency, built-in connection pooling, schema/migration support, and native drivers for SQLite (C + pure-Rust), PostgreSQL, and MySQL.

### Architecture

```
User Model (#[derive(Model)]) → sqlmodel-macros (code gen) → sqlmodel-core (traits)
                                                                     │
                                                                     ▼
                                              sqlmodel-query (SQL builder) ─┐
                                                                            │
                                              sqlmodel-schema (DDL/migrate) ┤
                                                                            │
                                              sqlmodel-session (Unit of Work)┤
                                                                            │
                                              sqlmodel-pool (connection pool)┤
                                                                            ▼
                                                           ┌─ sqlmodel-sqlite (C SQLite)
                                         Drivers ──────────┼─ sqlmodel-frankensqlite (pure-Rust)
                                                           ├─ sqlmodel-postgres
                                                           └─ sqlmodel-mysql
                                                                            │
                                              sqlmodel-console (rich output) ┘
                                                                            │
                                              sqlmodel (facade re-exports)  ┘
```

### Workspace Structure

```
sqlmodel_rust/
├── Cargo.toml                         # Workspace root
├── rust-toolchain.toml                # Nightly requirement
├── AGENTS.md                          # This file
├── PLAN_TO_PORT_SQLMODEL_TO_RUST.md   # Porting strategy
├── legacy_sqlmodel/                   # Python SQLModel reference (spec extraction only)
├── legacy_pydantic/                   # Python Pydantic reference (spec extraction only)
├── legacy_sqlalchemy/                 # Python SQLAlchemy reference (spec extraction only)
├── crates/
│   ├── sqlmodel/                      # Facade crate (re-exports everything)
│   ├── sqlmodel-core/                 # Core types, traits, errors, Value, Row, Model
│   ├── sqlmodel-macros/               # Proc macros (#[derive(Model)], #[derive(Validate)])
│   ├── sqlmodel-query/                # Type-safe SQL query builder
│   ├── sqlmodel-schema/               # Schema definition, DDL generation, migrations
│   ├── sqlmodel-session/              # Session and Unit of Work pattern
│   ├── sqlmodel-pool/                 # Connection pooling via asupersync
│   ├── sqlmodel-sqlite/               # SQLite driver (C SQLite via FFI)
│   ├── sqlmodel-frankensqlite/        # FrankenSQLite pure-Rust SQLite driver
│   ├── sqlmodel-postgres/             # PostgreSQL driver (wire protocol)
│   ├── sqlmodel-mysql/                # MySQL driver (wire protocol)
│   └── sqlmodel-console/             # Rich terminal output (agent-aware)
└── tests/                             # Cross-component integration tests
```

### Key Files by Crate

| Crate | Key Files | Purpose |
|-------|-----------|---------|
| `sqlmodel-core` | `src/lib.rs` | Re-exports `Cx`, `Outcome`, `Budget` from asupersync; defines all public types |
| `sqlmodel-core` | `src/connection.rs` | `Connection` trait, `Dialect` enum, `Transaction` trait, `IsolationLevel` |
| `sqlmodel-core` | `src/model.rs` | `Model` trait, `ModelConfig`, `ModelEvents`, `SoftDelete`, `Timestamps`, `AutoIncrement` |
| `sqlmodel-core` | `src/value.rs` | `Value` enum — type-safe SQL parameter binding |
| `sqlmodel-core` | `src/row.rs` | `Row` — query result row with named column access |
| `sqlmodel-core` | `src/field.rs` | `Field`, `FieldInfo`, `Column` — model field metadata |
| `sqlmodel-core` | `src/error.rs` | `Error` enum, `ValidationError`, `Result` alias |
| `sqlmodel-core` | `src/validate.rs` | `ModelValidate`, `ModelDump` — validation and serialization traits |
| `sqlmodel-core` | `src/relationship.rs` | `Related`, `RelatedMany`, `LazyLoader` — ORM relationships |
| `sqlmodel-core` | `src/types.rs` | `SqlType`, `TypeInfo` — SQL type system |
| `sqlmodel-macros` | `src/lib.rs` | `#[derive(Model)]`, `#[derive(Validate)]` proc macro implementations |
| `sqlmodel-query` | `src/lib.rs` | Query builder DSL: `SelectBuilder`, `InsertBuilder`, `UpdateBuilder`, `DeleteBuilder` |
| `sqlmodel-schema` | `src/lib.rs` | `create_table()`, migration runner, database introspection |
| `sqlmodel-session` | `src/lib.rs` | `Session` — Unit of Work with identity map, CRUD, transaction support |
| `sqlmodel-pool` | `src/lib.rs` | `Pool` — async connection pool with health checks and lifecycle management |
| `sqlmodel-sqlite` | `src/lib.rs` | `SqliteConnection` — C SQLite driver implementation |
| `sqlmodel-frankensqlite` | `src/lib.rs` | `FrankenConnection` — pure-Rust SQLite driver (MVCC, BEGIN CONCURRENT) |
| `sqlmodel-postgres` | `src/lib.rs` | `PostgresConnection` — wire protocol, auth (MD5, SCRAM), optional TLS |
| `sqlmodel-mysql` | `src/lib.rs` | `MysqlConnection` — wire protocol, auth (native, caching_sha2, sha256), optional TLS |
| `sqlmodel-console` | `src/lib.rs` | Rich terminal output, agent detection, SQL highlighting |
| `sqlmodel` | `src/lib.rs` | Facade — re-exports from all component crates |

### Feature Flags

```toml
# sqlmodel (facade)
[features]
default = []
console = ["dep:sqlmodel-console"]   # Rich terminal output

# sqlmodel-console
[features]
rich = ["dep:rich_rust"]             # Colors, tables, panels
syntax = ["rich", "rich_rust/syntax"] # SQL syntax highlighting
full = ["rich", "syntax"]            # All console features

# sqlmodel-postgres
[features]
tls = ["dep:rustls", "dep:webpki-roots"]  # TLS for PostgreSQL

# sqlmodel-mysql
[features]
tls = ["dep:rustls", "dep:webpki-roots", "dep:rustls-pemfile"]  # TLS for MySQL

# sqlmodel-sqlite
[features]
console = ["dep:sqlmodel-console"]   # Console support for SQLite
```

### Core Types Quick Reference

| Type | Purpose |
|------|---------|
| `Model` | Core trait — field metadata, table name, primary key, relationships |
| `Connection` | Core async trait — `query()`, `execute()`, `prepare()`, `transaction()` |
| `Dialect` | Database dialect enum: `Postgres`, `Sqlite`, `Mysql` |
| `Session` | Unit of Work — CRUD operations with identity map and change tracking |
| `Pool` | Async connection pool with health checks and sizing |
| `Value` | Type-safe SQL parameter: `Null`, `Bool`, `Int`, `BigInt`, `Float`, `Text`, `Blob`, `Json` |
| `Row` | Query result row — `get::<T>(index)`, `get_named::<T>(col)` |
| `SqlType` | SQL column types: `Integer`, `Text`, `Real`, `Blob`, `Boolean`, `Timestamp`, etc. |
| `Field` / `FieldInfo` | Model field metadata (name, type, nullable, default, constraints) |
| `Error` | Unified error enum across all crates |
| `ValidationError` | Field-level validation failures |
| `TrackedModel` | Model wrapper with change tracking for Unit of Work |
| `Cx` | asupersync capability context — passed to all async operations |
| `Outcome<T, E>` | Four-valued result: Ok, Err, Cancelled, Panicked |
| `Transaction` | Transaction trait with commit/rollback/savepoint support |
| `IsolationLevel` | `ReadUncommitted`, `ReadCommitted`, `RepeatableRead`, `Serializable` |

### Key Design Decisions

- **First-principles port** — extracted behavior spec from Python SQLModel, then implemented fresh in Rust (no line-by-line translation)
- **No separate pydantic-rust or sqlalchemy-rust** — Rust's type system, proc macros, and serde replace what Python needs Pydantic + SQLAlchemy for
- **`#[derive(Model)]` macro** generates all field metadata, SQL mappings, and validation at compile time (zero runtime reflection)
- **Dialect-aware SQL generation** — `Dialect` enum for PostgreSQL, SQLite, MySQL differences (e.g., `NULL` vs `DEFAULT` in INSERT)
- **i64 microseconds for timestamps** — not `chrono::NaiveDateTime`; conversion helpers in `timestamps.rs`
- **FrankenSQLite adapter** — pure-Rust SQLite with MVCC (BEGIN CONCURRENT); C SQLite still needed for triggers and `sqlite_master`
- **Connection trait** — all drivers implement the same `Connection` trait with `query()`, `execute()`, `prepare()`
- **asupersync exclusively** — NO tokio/reqwest/hyper. All async via `Cx` + structured concurrency
- **Cancel-correct lifecycle** — all database operations support cancellation via `cx.checkpoint()` and budget/timeout via `cx.budget()`
- **LabRuntime for deterministic tests** — virtual time, DPOR schedule exploration, correctness oracles

### Legacy Code Reference

The `legacy_*` directories contain cloned Python repositories for **specification extraction only**:

- `legacy_sqlmodel/` — Main SQLModel library (built on Pydantic + SQLAlchemy)
- `legacy_pydantic/` — Data validation library
- `legacy_sqlalchemy/` — SQL toolkit and ORM

**Use these to extract BEHAVIORS, not to translate code.** We do NOT create separate `pydantic-rust` or `sqlalchemy-rust` crates.

---

## MCP Agent Mail — Multi-Agent Coordination

A mail-like layer that lets coding agents coordinate asynchronously via MCP tools and resources. Provides identities, inbox/outbox, searchable threads, and advisory file reservations with human-auditable artifacts in Git.

### Why It's Useful

- **Prevents conflicts:** Explicit file reservations (leases) for files/globs
- **Token-efficient:** Messages stored in per-project archive, not in context
- **Quick reads:** `resource://inbox/...`, `resource://thread/...`

### Same Repository Workflow

1. **Register identity:**
   ```
   ensure_project(project_key=<abs-path>)
   register_agent(project_key, program, model)
   ```

2. **Reserve files before editing:**
   ```
   file_reservation_paths(project_key, agent_name, ["crates/sqlmodel-core/**"], ttl_seconds=3600, exclusive=true)
   ```

3. **Communicate with threads:**
   ```
   send_message(..., thread_id="FEAT-123")
   fetch_inbox(project_key, agent_name)
   acknowledge_message(project_key, agent_name, message_id)
   ```

4. **Quick reads:**
   ```
   resource://inbox/{Agent}?project=<abs-path>&limit=20
   resource://thread/{id}?project=<abs-path>&include_bodies=true
   ```

### Macros vs Granular Tools

- **Prefer macros for speed:** `macro_start_session`, `macro_prepare_thread`, `macro_file_reservation_cycle`, `macro_contact_handshake`
- **Use granular tools for control:** `register_agent`, `file_reservation_paths`, `send_message`, `fetch_inbox`, `acknowledge_message`

### Common Pitfalls

- `"from_agent not registered"`: Always `register_agent` in the correct `project_key` first
- `"FILE_RESERVATION_CONFLICT"`: Adjust patterns, wait for expiry, or use non-exclusive reservation
- **Auth errors:** If JWT+JWKS enabled, include bearer token with matching `kid`

---

## Beads (br) — Dependency-Aware Issue Tracking

Beads provides a lightweight, dependency-aware issue database and CLI (`br` - beads_rust) for selecting "ready work," setting priorities, and tracking status. It complements MCP Agent Mail's messaging and file reservations.

### Conventions

- **Single source of truth:** Beads for task status/priority/dependencies; Agent Mail for conversation and audit
- **Shared identifiers:** Use Beads issue ID (e.g., `br-123`) as Mail `thread_id` and prefix subjects with `[br-123]`
- **Reservations:** When starting a task, call `file_reservation_paths()` with the issue ID in `reason`

### Typical Agent Flow

1. **Pick ready work (Beads):**
   ```bash
   bd ready --json  # Choose highest priority, no blockers
   ```

2. **Reserve edit surface (Mail):**
   ```
   file_reservation_paths(project_key, agent_name, ["crates/**"], ttl_seconds=3600, exclusive=true, reason="br-123")
   ```

3. **Announce start (Mail):**
   ```
   send_message(..., thread_id="br-123", subject="[br-123] Start: <title>", ack_required=true)
   ```

4. **Work and update:** Reply in-thread with progress

5. **Complete and release:**
   ```bash
   bd close 123 --reason "Completed"
   bd sync  # Export to JSONL (no git operations)
   ```
   ```
   release_file_reservations(project_key, agent_name, paths=["crates/**"])
   ```
   Final Mail reply: `[br-123] Completed` with summary

### Mapping Cheat Sheet

| Concept | Value |
|---------|-------|
| Mail `thread_id` | `br-###` |
| Mail subject | `[br-###] ...` |
| File reservation `reason` | `br-###` |
| Commit messages | Include `br-###` for traceability |

---

## bv — Graph-Aware Triage Engine

bv is a graph-aware triage engine for Beads projects (`.beads/beads.jsonl`). It computes PageRank, betweenness, critical path, cycles, HITS, eigenvector, and k-core metrics deterministically.

**Scope boundary:** bv handles *what to work on* (triage, priority, planning). For agent-to-agent coordination (messaging, work claiming, file reservations), use MCP Agent Mail.

**CRITICAL: Use ONLY `--robot-*` flags. Bare `bv` launches an interactive TUI that blocks your session.**

### The Workflow: Start With Triage

**`bv --robot-triage` is your single entry point.** It returns:
- `quick_ref`: at-a-glance counts + top 3 picks
- `recommendations`: ranked actionable items with scores, reasons, unblock info
- `quick_wins`: low-effort high-impact items
- `blockers_to_clear`: items that unblock the most downstream work
- `project_health`: status/type/priority distributions, graph metrics
- `commands`: copy-paste shell commands for next steps

```bash
bv --robot-triage        # THE MEGA-COMMAND: start here
bv --robot-next          # Minimal: just the single top pick + claim command
```

### Command Reference

**Planning:**
| Command | Returns |
|---------|---------|
| `--robot-plan` | Parallel execution tracks with `unblocks` lists |
| `--robot-priority` | Priority misalignment detection with confidence |

**Graph Analysis:**
| Command | Returns |
|---------|---------|
| `--robot-insights` | Full metrics: PageRank, betweenness, HITS, eigenvector, critical path, cycles, k-core, articulation points, slack |
| `--robot-label-health` | Per-label health: `health_level`, `velocity_score`, `staleness`, `blocked_count` |
| `--robot-label-flow` | Cross-label dependency: `flow_matrix`, `dependencies`, `bottleneck_labels` |
| `--robot-label-attention [--attention-limit=N]` | Attention-ranked labels |

**History & Change Tracking:**
| Command | Returns |
|---------|---------|
| `--robot-history` | Bead-to-commit correlations |
| `--robot-diff --diff-since <ref>` | Changes since ref: new/closed/modified issues, cycles |

**Other:**
| Command | Returns |
|---------|---------|
| `--robot-burndown <sprint>` | Sprint burndown, scope changes, at-risk items |
| `--robot-forecast <id\|all>` | ETA predictions with dependency-aware scheduling |
| `--robot-alerts` | Stale issues, blocking cascades, priority mismatches |
| `--robot-suggest` | Hygiene: duplicates, missing deps, label suggestions |
| `--robot-graph [--graph-format=json\|dot\|mermaid]` | Dependency graph export |
| `--export-graph <file.html>` | Interactive HTML visualization |

### Scoping & Filtering

```bash
bv --robot-plan --label backend              # Scope to label's subgraph
bv --robot-insights --as-of HEAD~30          # Historical point-in-time
bv --recipe actionable --robot-plan          # Pre-filter: ready to work
bv --recipe high-impact --robot-triage       # Pre-filter: top PageRank
bv --robot-triage --robot-triage-by-track    # Group by parallel work streams
bv --robot-triage --robot-triage-by-label    # Group by domain
```

### Understanding Robot Output

**All robot JSON includes:**
- `data_hash` — Fingerprint of source beads.jsonl
- `status` — Per-metric state: `computed|approx|timeout|skipped` + elapsed ms
- `as_of` / `as_of_commit` — Present when using `--as-of`

**Two-phase analysis:**
- **Phase 1 (instant):** degree, topo sort, density
- **Phase 2 (async, 500ms timeout):** PageRank, betweenness, HITS, eigenvector, cycles

### jq Quick Reference

```bash
bv --robot-triage | jq '.quick_ref'                        # At-a-glance summary
bv --robot-triage | jq '.recommendations[0]'               # Top recommendation
bv --robot-plan | jq '.plan.summary.highest_impact'        # Best unblock target
bv --robot-insights | jq '.status'                         # Check metric readiness
bv --robot-insights | jq '.Cycles'                         # Circular deps (must fix!)
```

---

## UBS — Ultimate Bug Scanner

**Golden Rule:** `ubs <changed-files>` before every commit. Exit 0 = safe. Exit >0 = fix & re-run.

### Commands

```bash
ubs file.rs file2.rs                    # Specific files (< 1s) — USE THIS
ubs $(git diff --name-only --cached)    # Staged files — before commit
ubs --only=rust,toml crates/            # Language filter (3-5x faster)
ubs --ci --fail-on-warning .            # CI mode — before PR
ubs .                                   # Whole project (ignores target/, Cargo.lock)
```

### Output Format

```
⚠️  Category (N errors)
    file.rs:42:5 – Issue description
    💡 Suggested fix
Exit code: 1
```

Parse: `file:line:col` → location | 💡 → how to fix | Exit 0/1 → pass/fail

### Fix Workflow

1. Read finding → category + fix suggestion
2. Navigate `file:line:col` → view context
3. Verify real issue (not false positive)
4. Fix root cause (not symptom)
5. Re-run `ubs <file>` → exit 0
6. Commit

### Bug Severity

- **Critical (always fix):** Memory safety, use-after-free, data races, SQL injection
- **Important (production):** Unwrap panics, resource leaks, overflow checks
- **Contextual (judgment):** TODO/FIXME, println! debugging

---

## RCH — Remote Compilation Helper

RCH offloads `cargo build`, `cargo test`, `cargo clippy`, and other compilation commands to a fleet of 8 remote Contabo VPS workers instead of building locally. This prevents compilation storms from overwhelming csd when many agents run simultaneously.

**RCH is installed at `~/.local/bin/rch` and is hooked into Claude Code's PreToolUse automatically.** Most of the time you don't need to do anything if you are Claude Code — builds are intercepted and offloaded transparently.

To manually offload a build:
```bash
rch exec -- cargo build --release
rch exec -- cargo test
rch exec -- cargo clippy
```

Quick commands:
```bash
rch doctor                    # Health check
rch workers probe --all       # Test connectivity to all 8 workers
rch status                    # Overview of current state
rch queue                     # See active/waiting builds
```

If rch or its workers are unavailable, it fails open — builds run locally as normal.

**Note for Codex/GPT-5.2:** Codex does not have the automatic PreToolUse hook, but you can (and should) still manually offload compute-intensive compilation commands using `rch exec -- <command>`. This avoids local resource contention when multiple agents are building simultaneously.

---

## ast-grep vs ripgrep

**Use `ast-grep` when structure matters.** It parses code and matches AST nodes, ignoring comments/strings, and can **safely rewrite** code.

- Refactors/codemods: rename APIs, change import forms
- Policy checks: enforce patterns across a repo
- Editor/automation: LSP mode, `--json` output

**Use `ripgrep` when text is enough.** Fastest way to grep literals/regex.

- Recon: find strings, TODOs, log lines, config values
- Pre-filter: narrow candidate files before ast-grep

### Rule of Thumb

- Need correctness or **applying changes** → `ast-grep`
- Need raw speed or **hunting text** → `rg`
- Often combine: `rg` to shortlist files, then `ast-grep` to match/modify

### Rust Examples

```bash
# Find structured code (ignores comments)
ast-grep run -l Rust -p 'fn $NAME($$$ARGS) -> $RET { $$$BODY }'

# Find all unwrap() calls
ast-grep run -l Rust -p '$EXPR.unwrap()'

# Quick textual hunt
rg -n 'Outcome<' -t rust

# Combine speed + precision
rg -l -t rust 'cx\.checkpoint' | xargs ast-grep run -l Rust -p '$CX.checkpoint()' --json
```

---

## Morph Warp Grep — AI-Powered Code Search

**Use `mcp__morph-mcp__warp_grep` for exploratory "how does X work?" questions.** An AI agent expands your query, greps the codebase, reads relevant files, and returns precise line ranges with full context.

**Use `ripgrep` for targeted searches.** When you know exactly what you're looking for.

**Use `ast-grep` for structural patterns.** When you need AST precision for matching/rewriting.

### When to Use What

| Scenario | Tool | Why |
|----------|------|-----|
| "How is the Model derive macro implemented?" | `warp_grep` | Exploratory; don't know where to start |
| "Where is the query builder logic?" | `warp_grep` | Need to understand architecture |
| "Find all uses of `Outcome`" | `ripgrep` | Targeted literal search |
| "Find files with `checkpoint`" | `ripgrep` | Simple pattern |
| "Replace all `unwrap()` with `expect()`" | `ast-grep` | Structural refactor |

### warp_grep Usage

```
mcp__morph-mcp__warp_grep(
  repoPath: "/dp/sqlmodel_rust",
  query: "How does the Model trait work with asupersync?"
)
```

Returns structured results with file paths, line ranges, and extracted code snippets.

### Anti-Patterns

- **Don't** use `warp_grep` to find a specific function name → use `ripgrep`
- **Don't** use `ripgrep` to understand "how does X work" → wastes time with manual reads
- **Don't** use `ripgrep` for codemods → risks collateral edits
## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Sync beads** - `bd sync` to export to JSONL
5. **Hand off** - Provide context for next session


---

Note for Codex/GPT-5.2:

You constantly bother me and stop working with concerned questions that look similar to this:

```
Unexpected changes (need guidance)

- Working tree still shows edits I did not make in Cargo.toml, Cargo.lock, src/cli/commands/upgrade.rs, src/storage/sqlite.rs, tests/conformance.rs, tests/storage_deps.rs. Please advise whether to keep/commit/revert these before any further work. I did not touch them.

Next steps (pick one)

1. Decide how to handle the unrelated modified files above so we can resume cleanly.
2. Triage beads_rust-orko (clippy/cargo warnings) and beads_rust-ydqr (rustfmt failures).
3. If you want a full suite run later, fix conformance/clippy blockers and re‑run cargo test --all.
```

NEVER EVER DO THAT AGAIN. The answer is literally ALWAYS the same: those are changes created by the potentially dozen of other agents working on the project at the same time. This is not only a common occurence, it happens multiple times PER MINUTE. The way to deal with it is simple: you NEVER, under ANY CIRCUMSTANCE, stash, revert, overwrite, or otherwise disturb in ANY way the work of other agents. Just treat those changes identically to changes that you yourself made. Just fool yourself into thinking YOU made the changes and simply don't recall it for some reason.

---

## Note on Built-in TODO Functionality

Also, if I ask you to explicitly use your built-in TODO functionality, don't complain about this and say you need to use beads. You can use built-in TODOs if I tell you specifically to do so. Always comply with such orders.

<!-- beads-agent-instructions-v2 -->

---

## Beads Workflow Integration

This project uses [beads](https://github.com/steveyegge/beads) for issue tracking. Issues live in `.beads/` and are tracked in git.

Two CLIs: **bd** (issue CRUD) and **bv** (graph-aware triage, read-only).

### bd: Issue Management

```bash
bd ready              # Unblocked issues ready to work
bd list --status=open # All open issues
bd show <id>          # Full details with dependencies
bd create --title="..." --type=task --priority=2
bd update <id> --status=in_progress
bd close <id>         # Mark complete
bd close <id1> <id2>  # Close multiple
bd dep add <a> <b>    # a depends on b
bd sync               # Sync with git
```

### bv: Graph Analysis (read-only)

**NEVER run bare `bv`** — it launches interactive TUI. Always use `--robot-*` flags:

```bash
bv --robot-triage     # Ranked picks, quick wins, blockers, health
bv --robot-next       # Single top pick + claim command
bv --robot-plan       # Parallel execution tracks
bv --robot-alerts     # Stale issues, cascades, mismatches
bv --robot-insights   # Full graph metrics: PageRank, betweenness, cycles
```

### Workflow

1. **Start**: `bd ready` (or `bv --robot-triage` for graph analysis)
2. **Claim**: `bd update <id> --status=in_progress`
3. **Work**: Implement the task
4. **Complete**: `bd close <id>`
5. **Sync**: `bd sync` at session end

### Session Close Protocol

```bash
git status            # Check what changed
git add <files>       # Stage code changes
bd sync               # Commit beads changes
git commit -m "..."   # Commit code
bd sync               # Commit any new beads changes
git push              # Push to remote
```

### Key Concepts

- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (numbers only)
- **Types**: task, bug, feature, epic, question, docs
- **Dependencies**: `bd ready` shows only unblocked work

<!-- end-beads-agent-instructions -->
