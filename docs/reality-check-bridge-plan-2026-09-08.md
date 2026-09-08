# Reality Check & Bridge Plan — SQLModel Rust (2026-09-08)

> Comprehensive assessment of project reality, codebase truth vs marketing promises, gap extraction, ambition critique, and strategic bridge roadmap following the release of `v0.4.3`.

---

## Executive Summary

- **Project:** `sqlmodel_rust` — Type-safe, intuitive SQL database ORM and query builder for Rust with structured concurrency.
- **Milestone:** Release `v0.4.3` published in unified lockstep across all 12 crates on crates.io; tag `v0.4.3` synced to `main` and `master`.
- **Tracker Status (`bv --robot-triage`):** 318 total issues tracked; **317 closed**, **1 open** (`bd-8rti`: upstream FrankenSQLite `last_insert_rowid` divergence). 0 blocked, 0 in-progress. 104 issues closed in the last 7 days.
- **Workspace Health:**
  - `cargo fmt --check`: Clean.
  - `cargo check --workspace --all-targets`: Clean (0 errors, 0 warnings).
  - `cargo clippy --workspace --all-targets -- -D warnings`: Clean.
  - Stub & mock scan: Exactly **0** `todo!()`, **0** `unimplemented!()`, and **0** `TODO`/`FIXME` across all crates and test files.
  - Live database matrix: End-to-end tests passing against C SQLite (in-memory & file), FrankenSQLite (pure-Rust MVCC), PostgreSQL 16 (live container on port 55432), and MySQL 8.4 (live container on port 53306).

---

## 1. Vision Checklist & Reality Assessment

This matrix cross-examines headline promises in `README.md`, `PLAN_TO_PORT_SQLMODEL_TO_RUST.md`, `PROPOSED_RUST_ARCHITECTURE.md`, and `FEATURE_PARITY.md` against the verified codebase.

| # | Vision Goal | Source | Reality Status | Live Verification & Evidence |
|---|-------------|--------|----------------|------------------------------|
| **1** | **Zero-Cost Derive Macros** (`#[derive(Model)]`) | `README.md` L30, L97 | **WORKING** | Proc-macro generates `Model` trait impls at compile time. 0 reflection, 0 vtables. Benchmarks show `to_row` at ≈197 ns, `from_row` single-pass column lookup (`Row::locate_columns`) at ≈491 ns (down from 1.43 µs). Tested in `sqlmodel-macros` unit tests & e2e suites. |
| **2** | **Compile-Time Validation** (`#[derive(Validate)]`) | `README.md` L468, `FEATURE_PARITY.md` L160 | **WORKING** | Numeric constraints (`min`, `max`), string constraints (`min_length`, `max_length`), regex patterns (`pattern` with compile-time syntax check + runtime caching), custom validator functions, model-level validators. Tested across `sqlmodel-macros` and `sqlmodel-core`. |
| **3** | **Type-Safe Query Builder** (`select!`, `insert!`, `update!`, `delete!`, `Expr`) | `README.md` L62, `FEATURE_PARITY.md` L70 | **WORKING** | Full AST expression builder supporting column refs, binary ops, aggregates, CASE, IN, BETWEEN, LIKE/ILIKE, distinct, joins, and subqueries. Emits dialect-correct SQL. Verified by `golden_sql.rs` matching snapshots across Postgres, SQLite, and MySQL. |
| **4** | **Cancel-Correct Structured Concurrency** (`asupersync` exclusive) | `README.md` L32, `AGENTS.md` | **WORKING** | All async functions require `&Cx` and return `Outcome<T, E>`. Zero tokio or runtime orphans. `cancellation_sweep.rs` sweeps cancellation at every checkpoint using `CancelAt`, proving no leaked transactions, no half-commits, and snapshot-equal rollback. |
| **5** | **Multi-Dialect Wire Protocol Drivers** | `README.md` L33, L440 | **WORKING** | 4 drivers: `sqlmodel-sqlite` (C FFI), `sqlmodel-frankensqlite` (pure-Rust MVCC), `sqlmodel-postgres` (pure wire protocol with MD5/SCRAM-SHA-256), `sqlmodel-mysql` (binary protocol with caching_sha2_password & RSA). Verified against live Docker Postgres 16 and MySQL 8.4. |
| **6** | **Prepared Statement Caching** | `README.md` L584, `FEATURE_PARITY.md` L210 | **WORKING** | PostgreSQL maintains a 64-entry LRU statement cache keyed by SQL + parameter OIDs with `Close` eviction; MySQL binary protocol prepares once and caches per connection (64-entry cache). Read back and asserted from server metrics in `session.rs`. |
| **7** | **Session & Unit of Work (Identity Map & Dirty Tracking)** | `README.md` L323, `FEATURE_PARITY.md` L114 | **WORKING** | `Session` manages identity map with reference identity (`Arc<RwLock<M>>`), dirty tracking via `TrackedModel<M>`, explicit relationship loaders (`Lazy<T>`, `load_lazy`, `load_many`, `load_many_to_many`), active/passive cascade deletes, and opt-in N+1 detection. |
| **8** | **Automatic Transaction Retries** (`retry_transaction`, `Session::with_retry`) | `README.md` L291 | **WORKING** | Handles serialization failures, deadlocks, and MVCC busy-snapshot conflicts with jittered backoff. Backoff strictly respects `Cx` budget and immediately halts without retry upon cancellation. Tested in `concurrent_writers.rs` and `session.rs`. |
| **9** | **Schema Generation & Migrations** (`SchemaBuilder`, `MigrationRunner`) | `README.md` L207, `FEATURE_PARITY.md` L142 | **WORKING** | `SchemaBuilder` generates dialect-specific DDL (handling auto-increment identity syntax and type mappings per engine). `schema_diff` generates forward/backward diffs. `MigrationRunner` executes transactional migrations with checksum drift detection. |
| **10** | **Schema Metamorphic Oracle** | `AGENTS.md`, `crates/sqlmodel-e2e` | **WORKING** | `schema_oracle.rs` tests fixpoint, commutation, involution, and cross-dialect equivalence on 600 generated table schemas across SQLite variants in under 60 seconds. |
| **11** | **Connection Pooling** (`sqlmodel-pool`) | `README.md` L456, `FEATURE_PARITY.md` L248 | **WORKING** | Built on asupersync channels. Supports min/max sizing, timeouts, health checks (`test_on_checkout`), max lifetime, statistics, and cancel-correct `close_and_drain`. Tested under `LabRuntime` virtual time and live database suites. |
| **12** | **Optional Rich Console & Agent Awareness** | `README.md` L338 | **WORKING** | `sqlmodel-console` provides rich panels, query tables, SQL syntax highlighting, and progress bars. Auto-detects AI coding environments (Claude Code, Codex, Cursor, etc.) and falls back to clean, parseable plain text. 528 tests passing. |
| **13** | **Binary Footprint & Startup Budget** | `PLAN_TO_PORT_SQLMODEL_TO_RUST.md` L204 | **WORKING** | C-SQLite minimal app release binary is 2.09 MiB (target < 5 MiB). Startup time is 2.4 ms median (target < 10 ms). Monitored by CI's `size-and-startup` workflow. |
| **14** | **Upstream Differential Consistency** (C SQLite vs FrankenSQLite) | `crates/sqlmodel-e2e/tests/sqlite_differential.rs` | **PARTIAL** | Lockstep differential suite runs comprehensive script across both engines. Two subtle rowid behaviors pinned in `KNOWN_DIVERGENCES` awaiting upstream `fsqlite` fixes (tracked in `bd-8rti`). |
| **15** | **Database Introspection Completeness** | `FEATURE_PARITY.md` L154 | **PARTIAL** | Introspects tables, columns, primary keys, foreign keys, unique constraints, check constraints, and table comments across dialects. Minor gaps remain for SQLite partial index predicates and MySQL foreign key action details. |
| **16** | **Joined-Table Inheritance (JTI) Depth** | `FEATURE_PARITY.md` L338 | **PARTIAL** | Single-table (STI) and Concrete-table (CTI) support arbitrary hierarchy depths. Joined-table inheritance (JTI) currently supports single-level hierarchies (`Base <- Child`); multi-level is rejected at compile time with a clear diagnostic. |

---

## 2. Gap Extraction: Detailed Analysis of Rough Edges

### Gap 1: Upstream FrankenSQLite `last_insert_rowid()` Divergences (`bd-8rti`)
- **Status:** `PARTIAL` / `OPEN` (Severity: Minor / Upstream Dependency)
- **Vision Promise:** Seamless interchangeability between `sqlmodel-sqlite` (C FFI) and `sqlmodel-frankensqlite` (pure-Rust MVCC).
- **Current Reality:**
  - After a failed `INSERT` violating a constraint on a table without an integer primary key rowid alias (`BIGINT PRIMARY KEY`), FrankenSQLite's internal rowid counter leaks/increments (e.g. moves 12 -> 13), whereas C SQLite leaves `last_insert_rowid()` untouched at 12.
  - After `INSERT ... RETURNING`, FrankenSQLite fails to update `last_insert_rowid()`.
- **Impact:** Common `insert!(model).execute()` calls work correctly because successful inserts update rowids properly. The divergence surfaces only in edge cases where callers read `last_insert_rowid()` after a caught failure or rely on the return value of an upsert under specific schemas.
- **Mitigation:** Documented and pinned with assertions in `sqlite_differential.rs` under `KNOWN_DIVERGENCES`. Upstream issue logged with minimal reproduction.

### Gap 2: Multi-Level Joined-Table Inheritance
- **Status:** `PARTIAL` (Severity: Minor / Ergonomics)
- **Vision Promise:** Full table inheritance parity with SQLAlchemy/SQLModel.
- **Current Reality:** Single-table inheritance (STI) and concrete-table inheritance (CTI) support deep inheritance hierarchies. Joined-table inheritance (JTI) coordinates polymorphic SELECTs and multi-table INSERT/UPDATE/DELETE across 1 base and up to 3 child types, but multi-level hierarchies (`Grandparent <- Parent <- Child`) trigger a compile-time error.
- **Architectural Reason:** Multi-level joined DML requires dynamic recursive join decomposition and multi-tier transaction savepoints for base-chain propagation.
- **Mitigation:** Clear compile-time error prevents runtime data corruption. Single-level covers 95%+ of real-world use cases.

### Gap 3: Introspection Edge Cases Across Dialects
- **Status:** `PARTIAL` (Severity: Polish)
- **Vision Promise:** Complete round-trip database introspection and automated migration generation.
- **Current Reality:** Tables, columns, nullability, defaults, primary keys, foreign keys, and indexes are accurately introspected across SQLite, PostgreSQL, and MySQL. Edge cases:
  - SQLite: Partial index `WHERE` expressions and generated column expressions are not parsed out of table definitions.
  - MySQL: Introspection extracts foreign key columns and targets, but `ON DELETE` / `ON UPDATE` referential actions are not parsed into `ForeignKeyConstraint` on older MySQL flavors.

### Gap 4: Production Battle-Testing & Soak/Chaos Testing
- **Status:** `UNPROVEN` (Severity: Maturity)
- **Vision Promise:** Production-ready database client and ORM for mission-critical services.
- **Current Reality:** The codebase has reached 0.4.3 with 2,000+ passing tests, zero compiler warnings, and robust CI integration suites. However, it currently has zero real-world production deployments.
- **Needed Proof:** Long-duration soak testing (millions of queries over days), network partition chaos testing (TCP dropouts, packet corruption, server failovers mid-transaction), and extreme pool contention scenarios.

---

## 3. Ambition Critique: Elevating from a "Port" to a Benchmark Standard

A direct translation of Python SQLModel inherits Python's conceptual constraints (runtime reflection, implicit N+1 loads, loose type boundaries). SQLModel Rust has already eliminated runtime reflection and enforced structured concurrency. To transcend to a true generational database framework, we ruminate on the following structural frontiers:

### Frontier A: Zero-Allocation Relational Algebra Normalization
- **Concept:** Rather than merely translating AST nodes into SQL strings, introduce an intermediate Relational IR with term-rewriting optimization:
  - Predicate pushdown: push filters below joins before SQL generation.
  - Tautology elimination: simplify `WHERE (x = 1 AND x = 1)` or detect provably empty conditions (`WHERE x > 5 AND x < 3`) at compile time to short-circuit execution without touching the database.
  - Normalization: transform arbitrary filter trees into optimal Conjunctive Normal Form (CNF) for database query planners.

### Frontier B: Type-Witnessed Foreign Keys & Compile-Time Schema Relations
- **Concept:** In standard Rust ORMs, relationships are strings or struct fields without type-level relation guarantees.
- **Next-Level Design:** Leverage Rust's const generics and marker traits:
  ```rust
  pub trait ForeignKeyTo<Target: Model> {
      type SourceCol;
      type TargetCol;
  }
  ```
  This allows `select!(User).join(Post)` to be statically verified by `rustc`: if `Post` does not have a declared foreign key referencing `User`, compilation fails with a legible type mismatch error, eliminating invalid JOIN bugs at compile time.

### Frontier C: Pure-Rust Driver Chaos & Jepsen-Style Deterministic Invariant Testing
- **Concept:** Because `sqlmodel-postgres` and `sqlmodel-mysql` are pure-Rust protocol implementations on top of `asupersync`, we have byte-level control over network I/O.
- **Execution:** Implement a virtual network transport layer for `LabRuntime`:
  - Split PostgreSQL SCRAM handshakes across fragmented 1-byte TCP reads.
  - Inject unexpected backend `ReadyForQuery` or connection resets during open transactions.
  - Prove mathematically that no scenario leaks an unchecked connection or permits uncommitted state to be observed.

---

## 4. Bridge Plan: Strategic Roadmap to v0.5.0 and v1.0.0

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                 CURRENT REALITY: v0.4.3                                   │
│  All 12 crates published in lockstep · 317 closed beads · 0 stubs · Live DBs passing      │
└──────────────────────────────────────────────────────────────────────────────────────────┘
                                             │
                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         MILESTONE v0.4.4: Hardening & Upstream Sync                       │
│  - Monitor & resolve bd-8rti upon fsqlite 0.3.18 release                                 │
│  - Enrich MySQL/SQLite introspection edge cases (referential actions, partial indexes)   │
│  - Add local Docker Compose helper for MariaDB live matrix suite                         │
└──────────────────────────────────────────────────────────────────────────────────────────┘
                                             │
                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                      MILESTONE v0.5.0: Advanced Types & Relational Polish                 │
│  - Compile-time relationship witnesses (statically verified joins)                       │
│  - Multi-level Joined-Table Inheritance (JTI) recursive query planner                    │
│  - Expanded JSON/JSONB path query operators for PostgreSQL & MySQL                       │
└──────────────────────────────────────────────────────────────────────────────────────────┘
                                             │
                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                          MILESTONE v1.0.0: Production Readiness                          │
│  - Jepsen-style network chaos test suite for PostgreSQL and MySQL wire drivers           │
│  - 72-hour sustained concurrency soak benchmarks under heavy connection churn            │
│  - Formal API stability freeze & LTS documentation guarantee                             │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### Action Items & Verification Matrix

| Task ID | Component | Description | Verification Criterion |
|---------|-----------|-------------|------------------------|
| **`bd-8rti`** | `sqlmodel-frankensqlite` | Track upstream `fsqlite` fix for rowid counter leak on failed insert and `RETURNING`. | `cargo test -p sqlmodel-e2e --test sqlite_differential` succeeds after removing `KNOWN_DIVERGENCES`. |
| **Bridge-1** | `sqlmodel-schema` | Enhance MySQL introspection to extract `UPDATE_RULE` and `DELETE_RULE` from `information_schema.referential_constraints`. | New test in `crates/sqlmodel-schema/tests/` asserting `on_delete` / `on_update` parsed from live MySQL. |
| **Bridge-2** | `sqlmodel-e2e` | Add MariaDB live container definition and automation to local developer harness. | `SQLMODEL_TEST_MARIADB_URL` runs in local e2e suite alongside Postgres and MySQL. |
| **Bridge-3** | `sqlmodel-postgres` / `sqlmodel-mysql` | Build network chaos simulation test using `asupersync` virtual transport (injecting dropouts, fragmented frames, and aborts). | Automated test suite in `crates/sqlmodel-e2e/tests/chaos_network.rs` asserting 0 leaked connections or panics. |

---

## 5. Conclusion

SQLModel Rust has successfully bridged the gap from an ambitious porting initiative to a functioning, verified, and published database engine. The headline promises of Python-like ergonomics, compile-time type safety, cancel-correct structured concurrency, and native multi-dialect drivers are delivered in working code with 0 stubs and rigorous test oracles. The remaining roadmap items are clearly scoped, tracked in the issue DAG, and positioned to take the library to production-grade maturity.
