# PROPOSED_RUST_ARCHITECTURE.md

This document captures the intended architecture for SQLModel Rust as a first-principles Rust implementation. It complements:

- [PLAN_TO_PORT_SQLMODEL_TO_RUST.md](PLAN_TO_PORT_SQLMODEL_TO_RUST.md)
- [EXISTING_SQLMODEL_STRUCTURE.md](EXISTING_SQLMODEL_STRUCTURE.md)
- [FEATURE_PARITY.md](FEATURE_PARITY.md)

Use this document for design decisions and invariants. Use `EXISTING_SQLMODEL_STRUCTURE.md` as behavior spec.

## 1. Architectural Goals

1. Keep the user-facing experience close to Python SQLModel while staying idiomatic in Rust.
2. Prefer compile-time guarantees (derive macros, static metadata, type checking).
3. Keep runtime simple and predictable (no reflection-based ORM behavior).
4. Enforce cancel-correct async and structured concurrency via asupersync.
5. Keep dependencies minimal and focused.

## 2. Workspace Layering

The workspace is intentionally split into small crates with clear boundaries.

- `sqlmodel`:
  Facade crate; re-exports the public API (`Model`, `Connection`, query builders, macros, schema/session/pool APIs).

- `sqlmodel-core`:
  Core types and traits. Owns `Model`, `Connection`, `TransactionOps`, `Value`, `Row`, `FieldInfo`, and shared error types.

- `sqlmodel-macros`:
  Proc macros (`derive(Model)`, validation derives). Generates static model metadata and conversion logic at compile time.

- `sqlmodel-query`:
  Query DSL, builders, expression system, select/polymorphic loading, and dialect SQL generation.

- `sqlmodel-schema`:
  DDL generation and migration execution primitives.

- `sqlmodel-session`:
  Unit-of-work and identity map semantics.

- `sqlmodel-pool`:
  Connection pooling primitives, health, and routing support.

- Driver crates:
  `sqlmodel-sqlite`, `sqlmodel-postgres`, `sqlmodel-mysql` provide wire/driver implementations behind the `Connection` trait.

### Dependency Direction

- `sqlmodel-core` is the base.
- `sqlmodel-query`, `sqlmodel-schema`, `sqlmodel-session`, `sqlmodel-pool`, drivers depend on `sqlmodel-core`.
- `sqlmodel` depends on all public sub-crates and re-exports them.
- `sqlmodel-macros` depends on `sqlmodel-core` metadata types for generated code shape.

## 3. Core Trait Contracts

## 3.1 `Model`

`Model` is static metadata + row conversion contract:

- Static table metadata:
  `TABLE_NAME`, `PRIMARY_KEY`, `fields()`, `inheritance()`.
- Runtime conversions:
  `to_row()`, `from_row()`, `primary_key_value()`, `is_new()`.
- Joined-inheritance support:
  `joined_parent_row()` for joined child models with `#[sqlmodel(parent)]` embedded base.

Design intent:

- No runtime schema introspection is required for normal model operations.
- All per-model metadata is generated at compile time.

## 3.2 `Connection` and `TransactionOps`

`Connection` abstracts query/execute/transaction lifecycle. Drivers own protocol details.

`TransactionOps` provides transactional query/execute and commit/rollback/savepoint behavior.

Design intent:

- Query builders produce SQL + params; drivers execute them.
- Session and builders compose against trait contracts, not concrete drivers.

## 3.3 asupersync `Cx` + `Outcome`

All async DB operations are designed to:

- accept `&Cx`
- return `Outcome<T, Error>`
- remain cancel-correct and budget-aware

This is an architectural invariant across query/session/driver layers.

## 4. Query Builder Architecture

## 4.1 API Shape

- Macro entry points: `select!`, `insert!`, `insert_many!`, `update!`, `delete!`
- Fluent builders for filters, joins, ordering, limits, conflict behavior, and returning.
- Builders are typed by model (`Select<M>`, `InsertBuilder<M>`, etc.).

## 4.2 SQL Dialect Handling

`Dialect` is carried through query generation. Placeholder conventions are invariant:

- PostgreSQL: `$1`, `$2`, ...
- SQLite: `?1`, `?2`, ...
- MySQL: `?`

Builders produce dialect-specific SQL with deterministic parameter ordering.

## 4.3 Projection and Aliasing Invariant

When a query must hydrate multiple logical model shapes from one row (e.g. eager loading, joined inheritance polymorphism), columns are projected with alias format:

- `table__column`

Example:

- `people.id AS people__id`
- `students.grade AS students__grade`

This `table__col` aliasing convention is required for stable prefix-based hydration.

## 5. Inheritance Strategy

The architecture supports three mapping strategies represented in `InheritanceInfo`.

## 5.1 Single-Table Inheritance (STI)

- One physical table for base + children.
- Base defines discriminator column.
- Children define discriminator values.
- Child queries apply implicit discriminator filter.

Polymorphic strategy:

- Base reads discriminator and hydrates the matching Rust type.
- No additional table join is required.

## 5.2 Joined-Table Inheritance

- Base and each child have separate physical tables.
- Child PK is also FK to base PK.
- Child models embed base via `#[sqlmodel(parent)]`.

Insert/update/delete strategy:

- Operations touching a child may need coordinated base+child statements in one transaction.
- For inserts with generated PKs, base insertion occurs first, PK is propagated to child row.

Polymorphic query strategy:

- Base SELECT with explicit `LEFT JOIN`(s) to child table(s).
- Full prefixed projection for base and children (`table__col`).
- Hydration chooses variant based on non-null child prefixes.
- Ambiguity (more than one child prefix non-null) is treated as an error.

## 5.3 Concrete-Table Inheritance

- Each type owns independent table mapping.
- No DB-level parent-child FK coupling required.

## 6. Schema and Migration Architecture

`sqlmodel-schema` is responsible for generated DDL and migration execution.

Key behavior:

- joined inheritance child DDL includes FK from child PK columns to parent PK columns
- single-table inheritance children do not create independent physical tables
- schema builders remain model-metadata driven, not reflection-driven

## 7. Session and Identity Map Architecture

`sqlmodel-session` provides a unit-of-work boundary:

- tracks object states (new/dirty/deleted)
- flushes into database operations
- keeps identity map consistency by PK
- coordinates transactions for grouped operations

Design intent:

- explicit, predictable lifecycle
- no hidden lazy global state
- model-level operations funnel into query builders + connection contracts

## 8. Performance and Safety Principles

1. Compile-time metadata generation rather than runtime reflection.
2. Minimal allocations where practical (reuse row/value structures, deterministic SQL emit).
3. Explicit transactional composition for multi-table inheritance operations.
4. Clear structured errors over implicit fallback behavior.

## 9. Current Known Gaps / Follow-ups

At the time of writing, remaining architectural follow-ups are tracked in beads.

- `bd-3bmd`:
  Joined-table inheritance DML semantics for explicit `WHERE`/`SET` builder usage and ON CONFLICT edge behavior.

- `bd-162`:
  Epic umbrella for full SQLModel parity tracking.

Keep this section synchronized with `FEATURE_PARITY.md` and open beads issues.

## 10. Architectural Invariants Summary

These invariants should remain true unless explicitly revised:

1. All async database APIs accept `&Cx` and return `Outcome<_, Error>`.
2. Query generation is dialect-aware with stable placeholder behavior.
3. Multi-model hydration uses `table__col` aliasing.
4. Joined inheritance child writes are coordinated across base+child tables.
5. `sqlmodel` remains a thin facade over focused sub-crates.
6. Core remains dependency-lean and avoids heavyweight ORM/database dependencies.
