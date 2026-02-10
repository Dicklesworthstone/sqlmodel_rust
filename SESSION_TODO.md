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
