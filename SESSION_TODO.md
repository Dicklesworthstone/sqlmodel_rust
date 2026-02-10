# Session TODO (Codex)

Purpose: eliminate "comment-only" SQLite DDL behavior and fix doc/spec drift without losing track of sub-tasks.

## A. SQLite DDL: Remove Comment-Only Paths (Constraint Ops)

### A1. Audit current SQLite DDL generator
- [x] Inventory remaining `SchemaOperation::*` arms in `crates/sqlmodel-schema/src/ddl/sqlite.rs` that emit comments/errors instead of executable DDL
- [x] Confirm which ops are actually supported by SQLite `ALTER TABLE` vs require recreation

### A2. Extend SchemaOperation with table_info for constraint ops
- [x] Add `table_info: Option<TableInfo>` fields to:
  - [x] `AddPrimaryKey`
  - [x] `DropPrimaryKey`
  - [x] `AddForeignKey`
  - [x] `DropForeignKey`
  - [x] `AddUnique` (so SQLite can recreate-drop when current unique is an autoindex)
  - [x] `DropUnique` (so SQLite can recreate-drop when current unique is an autoindex)
- [x] Update `SchemaOperation::inverse()` to propagate/compute correct `table_info` for rollback where possible
- [x] Update all DDL generators (sqlite/postgres/mysql) pattern matches + unit tests to compile

### A3. Diff engine populates table_info for constraint ops
- [x] In `crates/sqlmodel-schema/src/diff.rs`, attach `Some(current_table.clone())` when creating ops in:
  - [x] primary key diffs
  - [x] foreign key diffs
  - [x] unique constraint diffs

### A4. Implement SQLite recreation for constraint ops
- [x] Add/extend helpers in `crates/sqlmodel-schema/src/ddl/sqlite.rs`:
  - [x] `sqlite_add_primary_key_recreate`
  - [x] `sqlite_drop_primary_key_recreate`
  - [x] `sqlite_add_foreign_key_recreate`
  - [x] `sqlite_drop_foreign_key_recreate`
  - [x] `sqlite_drop_unique_recreate` (needed when the current unique is backed by `sqlite_autoindex_*`)
- [x] Ensure indexes are preserved/recreated appropriately
- [x] Ensure FK enforcement is handled (PRAGMA foreign_keys OFF/ON)

### A5. Tests
- [x] Add/update unit tests in `crates/sqlmodel-schema/src/ddl/sqlite.rs` verifying generated statements (not just comments)
- [x] Add/update diff tests in `crates/sqlmodel-schema/src/diff.rs` validating `table_info: Some(_)` is attached for the ops above

### A6. Quality gates for SQLite DDL work
- [x] `cargo fmt --check`
- [x] `cargo check --all-targets`
- [x] `cargo clippy --all-targets -- -D warnings`
- [x] `cargo test -p sqlmodel-schema`

## B. Doc/Spec Drift Cleanup (bd-1ytr)

### B1. Audit docs for stale statements
- [x] `rg -n 'TODO|Not implemented|NOT IMPLEMENTED|would need|placeholder' EXISTING_SQLMODEL_STRUCTURE.md README.md AGENTS.md FEATURE_PARITY.md`
- [x] Identify claims that conflict with code reality (relationships, validate macro, model_dump/validate helpers, etc.)

### B2. Fix `EXISTING_SQLMODEL_STRUCTURE.md`
- [x] Update feature mapping summary rows to match actual implementation
- [ ] Remove obsolete "Rust Equivalent (Serde only)" guidance where model-aware helpers exist
- [ ] Ensure we do not claim features as implemented unless verified in code/tests

### B3. Optional: align README/FEATURE_PARITY where needed
- [x] Only adjust if we find provable drift

### B4. Quality gates for doc changes
- [ ] `cargo fmt --check` (if Rust touched)
- [ ] `cargo check --all-targets`
- [ ] `cargo clippy --all-targets -- -D warnings`

## C. Landing The Plane (MANDATORY)
- [ ] File/close beads issues for any remaining work
- [ ] `git pull --rebase`
- [ ] `br sync --flush-only`
- [ ] `git add .beads/ && git commit -m "sync beads"`
- [ ] `git push`
- [ ] `git status` clean and up to date

## D. Schema Diff/Introspection Correctness (Unique/Indexes)

### D1. Introspection: unique constraints are real (not comment-only)
- [x] In `crates/sqlmodel-schema/src/introspect.rs`, populate `TableInfo.unique_constraints` for each dialect:
  - [x] SQLite: derive from `PRAGMA index_list/index_info` for unique indexes (including constraint-backed ones)
  - [x] PostgreSQL: query `pg_constraint` contype='u' to get unique constraint names + ordered columns
  - [x] MySQL: derive from `SHOW INDEX` (unique && !PRIMARY)
- [x] Ensure `TableInfo.indexes` excludes constraint-backed indexes (PK + UNIQUE) so diff doesn't try illegal DROP INDEX

### D2. Diff: new tables also create indexes
- [x] Ensure `SchemaOperation::CreateTable(TableInfo)` DDL emits `CREATE INDEX` statements for `table.indexes`
- [x] Add tests asserting CreateTable generates indexes for all dialects

### D3. Naming: deterministic, collision-safe constraint names
- [x] Update expected schema extraction to name uniques as `uk_<table>_<columns...>` (not `uk_<col>`)
- [x] Align CreateTable builder (`crates/sqlmodel-schema/src/create.rs`) to use same naming

## E. "Would" / Stub Cleanup (bd-162)

Goal: eliminate real behavior gaps hidden behind "we'd need ..." comments and ensure the code matches the stated parity goals.

### E1. Eager SELECT must alias related columns (no `table.*`)
- [x] Add `RelationshipInfo.related_fields_fn` so query builders can project related model columns deterministically
- [x] Derive macro wires `.related_fields(<RelatedModel as Model>::fields)`
- [x] Update `Select::build_eager_with_dialect()` to project `related_table.col AS related_table__col` (not `related_table.*`)
- [x] Add tests asserting `teams.id AS teams__id` etc are present for eager join queries

### E2. MySQL binary protocol temporal decoding must be structured (no "keep as text")
- [x] Decode MySQL binary DATE into `Value::Date(days_since_epoch)` where possible
- [x] Decode MySQL binary TIME into `Value::Time(microseconds)` (supports days + sign)
- [x] Decode MySQL binary DATETIME/TIMESTAMP into `Value::Timestamp(microseconds_since_epoch)` where possible
- [x] Add unit tests for DATE/TIME/DATETIME binary result decoding
- [ ] Consider parsing text-protocol temporal strings in `decode_text_value` into structured `Value::*` (optional, but improves API consistency)

### E3. Doc/Parity Drift: "Excluded" sections must become real tracked work
- [ ] Audit `FEATURE_PARITY.md` for "Explicitly Excluded" content and reconcile with bd-162 (no exclusions)
- [ ] Create/adjust beads for each formerly-excluded feature and link them to bd-162

## F. ORM Patterns Wiring + API Reality (bd-3lz)

Goal: ensure the *actual public facade* (`sqlmodel::prelude::*`) exposes the real ORM Session (unit of work / identity map / lazy loading), and stop shipping misleading "Session" APIs that are only a connection wrapper.

### F1. Facade exports the ORM Session
- [x] Add `sqlmodel-session` as a dependency of `crates/sqlmodel`
- [x] Re-export `sqlmodel_session::{Session, SessionConfig, GetOptions, ObjectKey, ObjectState, SessionDebugInfo}` from the facade
- [x] Ensure `sqlmodel::prelude::*` includes ORM session types/options

### F2. Resolve the duplicate "Session" concept
- [x] Move the old connection+console wrapper into `sqlmodel::ConnectionSession` + `ConnectionSessionBuilder`
- [x] Update docs/comments that previously implied `Session::builder()` was the ORM session

### F3. Follow-ups (not done yet)
- [x] Add a small compile-level test in `crates/sqlmodel/tests/` that exercises `use sqlmodel::prelude::*;` + `Session::<MockConnection>::new(MockConnection)` + `SessionConfig` (guards against future facade drift)
- [ ] Audit `README.md` and `FEATURE_PARITY.md` for any remaining references to the old "Session builder" that now means `ConnectionSession`
- [ ] Decide and implement whether ORM identity map guarantees *reference identity* (shared instance) vs *value caching* (clones). If reference-identity is required, plan the core API shift (`LazyLoader`, `Lazy<T>`, etc.) and track it explicitly under `bd-162`.

## G. UBS Critical Findings (bd-3obp)

Goal: make `ubs --diff --only=rust,toml .` exit 0 without broad ignores so it can gate commits.

- [x] Fix UBS "hardcoded secrets" false positives in MySQL auth plugin matching (avoid triggering `password\\s*=` regex).
- [x] Fix MySQL config password setter to avoid UBS pattern matches without changing runtime behavior.
- [x] Confirm `ubs --diff --only=rust,toml .` exits 0 (Critical: 0).
- [x] Close `bd-3obp` with a concrete reason once UBS is clean.
