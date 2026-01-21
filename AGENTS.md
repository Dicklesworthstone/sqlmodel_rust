# AGENTS.md - Guidelines for AI Coding Agents

## Project Overview

**SQLModel Rust** is a first-principles Rust port of Python's SQLModel library. The goal is to provide the same developer experience (intuitive, type-safe SQL operations) while leveraging Rust's performance and safety guarantees.

### Key Principles

1. **First-principles implementation** - Extract behavior spec from legacy code, then implement fresh in Rust
2. **Never translate line-by-line** - Python idioms don't map to Rust; design for Rust from the start
3. **Minimal dependencies** - Only asupersync + serde (+ proc-macro crates for macros)
4. **Zero-cost abstractions** - Compile-time code generation, no runtime reflection
5. **Structured concurrency** - All async operations go through asupersync's `Cx` context

## RULE 0 - THE FUNDAMENTAL OVERRIDE PEROGATIVE

If I tell you to do something, even if it goes against what follows below, YOU MUST LISTEN TO ME. I AM IN CHARGE, NOT YOU.

---

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

### Macros vs Granular Tools

- **Prefer macros for speed:** `macro_start_session`, `macro_prepare_thread`, `macro_file_reservation_cycle`, `macro_contact_handshake`
- **Use granular tools for control:** `register_agent`, `file_reservation_paths`, `send_message`, `fetch_inbox`, `acknowledge_message`

### Common Pitfalls

- `"from_agent not registered"`: Always `register_agent` in the correct `project_key` first
- `"FILE_RESERVATION_CONFLICT"`: Adjust patterns, wait for expiry, or use non-exclusive reservation

---

## Beads (br) — Dependency-Aware Issue Tracking

Beads provides a lightweight, dependency-aware issue database and CLI (`br`) for selecting "ready work," setting priorities, and tracking status.

**Note:** br is non-invasive and never executes git commands. You must manually run git add/commit/push after `br sync --flush-only`.

### Typical Agent Flow

1. **Pick ready work (Beads):**
   ```bash
   br ready --json  # Choose highest priority, no blockers
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
   br close br-123 --reason "Completed"
   ```
   ```
   release_file_reservations(project_key, agent_name, paths=["crates/**"])
   ```

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
| `--robot-insights` | Full metrics: PageRank, betweenness, HITS, eigenvector, critical path, cycles |
| `--robot-diff --diff-since <ref>` | Changes since ref: new/closed/modified issues, cycles |

### jq Quick Reference

```bash
bv --robot-triage | jq '.quick_ref'                        # At-a-glance summary
bv --robot-triage | jq '.recommendations[0]'               # Top recommendation
bv --robot-plan | jq '.plan.summary.highest_impact'        # Best unblock target
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
Warning: Category (N errors)
    file.rs:42:5 – Issue description
    Suggested fix
Exit code: 1
```

Parse: `file:line:col` -> location | Suggested fix -> how to fix | Exit 0/1 -> pass/fail

### Fix Workflow

1. Read finding -> category + fix suggestion
2. Navigate `file:line:col` -> view context
3. Verify real issue (not false positive)
4. Fix root cause (not symptom)
5. Re-run `ubs <file>` -> exit 0
6. Commit

---

## ast-grep vs ripgrep

**Use `ast-grep` when structure matters.** It parses code and matches AST nodes, ignoring comments/strings, and can **safely rewrite** code.

**Use `ripgrep` when text is enough.** Fastest way to grep literals/regex.

### Rule of Thumb

- Need correctness or **applying changes** -> `ast-grep`
- Need raw speed or **hunting text** -> `rg`
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
  repoPath: "/data/projects/sqlmodel_rust",
  query: "How does the Model trait work with asupersync?"
)
```

Returns structured results with file paths, line ranges, and extracted code snippets.

### Anti-Patterns

- **Don't** use `warp_grep` to find a specific function name -> use `ripgrep`
- **Don't** use `ripgrep` to understand "how does X work" -> wastes time with manual reads
- **Don't** use `ripgrep` for codemods -> risks collateral edits

---

## cass — Cross-Agent Session Search

`cass` indexes prior agent conversations (Claude Code, Codex, Cursor, Gemini, ChatGPT, Aider, etc.) into a unified, searchable index so you can reuse solved problems.

**NEVER run bare `cass`** — it launches an interactive TUI. Always use `--robot` or `--json`.

### Quick Start

```bash
# Check if index is healthy (exit 0=ok, 1=run index first)
cass health

# Search across all agent histories
cass search "sqlmodel proc macro" --robot --limit 5

# View a specific result (from search output)
cass view /path/to/session.jsonl -n 42 --json

# Expand context around a line
cass expand /path/to/session.jsonl -n 42 -C 3 --json

# Learn the full API
cass capabilities --json      # Feature discovery
cass robot-docs guide         # LLM-optimized docs
```

### Key Flags

| Flag | Purpose |
|------|---------|
| `--robot` / `--json` | Machine-readable JSON output (required!) |
| `--fields minimal` | Reduce payload: `source_path`, `line_number`, `agent` only |
| `--limit N` | Cap result count |
| `--agent NAME` | Filter to specific agent (claude, codex, cursor, etc.) |
| `--days N` | Limit to recent N days |

**stdout = data only, stderr = diagnostics. Exit 0 = success.**

### Pre-Flight Health Check

```bash
cass health --json
```

Returns in <50ms:
- **Exit 0:** Healthy—proceed with queries
- **Exit 1:** Unhealthy—run `cass index --full` first

### Exit Codes

| Code | Meaning | Retryable |
|------|---------|-----------|
| 0 | Success | N/A |
| 1 | Health check failed | Yes—run `cass index --full` |
| 2 | Usage/parsing error | No—fix syntax |
| 3 | Index/DB missing | Yes—run `cass index --full` |

Treat cass as a way to avoid re-solving problems other agents already handled.

<!-- bv-agent-instructions-v1 -->

---

## Beads Workflow Integration

This project uses Beads for issue tracking. Issues are stored in `.beads/` and tracked in git.

**Note:** br is non-invasive and never executes git commands. You must manually run `git add .beads/` and `git commit` after `br sync --flush-only`.

### Essential Commands

```bash
# CLI commands for agents
br ready              # Show issues ready to work (no blockers)
br list --status=open # All open issues
br show <id>          # Full issue details with dependencies
br create --title="..." --type=task --priority=2
br update <id> --status=in_progress
br close <id> --reason="Completed"
br close <id1> <id2>  # Close multiple issues at once
br sync --flush-only  # Export to JSONL
git add .beads/
git commit -m "sync beads"
```

### Workflow Pattern

1. **Start**: Run `br ready` to find actionable work
2. **Claim**: Use `br update <id> --status=in_progress`
3. **Work**: Implement the task
4. **Complete**: Use `br close <id>`
5. **Sync**: Always run `br sync --flush-only` at session end, then `git add .beads/ && git commit`

### Key Concepts

- **Dependencies**: Issues can block other issues. `br ready` shows only unblocked work.
- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (use numbers, not words)
- **Types**: task, bug, feature, epic, question, docs
- **Blocking**: `br dep add <issue> <depends-on>` to add dependencies

<!-- end-bv-agent-instructions -->

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

NEVER EVER DO THAT AGAIN. The answer is literally ALWAYS the same: those are changes created by the potentially dozen of other agents working on the project at the same time. This is not only a common occurence, it happens multiple times PER MINUTE. The way to deal with it is simple: you NEVER, under ANY CIRCUMSTANCE, stash, revert, overwrite, or otherwise disturb in ANY way the work of other agents. Just treat those changes identically to changes that you yourself made. Just fool yourself into think YOU made the changes and simply don't recall it for some reason.

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   br sync --flush-only
   git add .beads/
   git commit -m "sync beads"
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

---

## Note on Built-in TODO Functionality

Also, if I ask you to explicitly use your built-in TODO functionality, don't complain about this and say you need to use beads. You can use built-in TODOs if I tell you specifically to do so. Always comply with such orders.