# SQLModel Rust ORM User Guide

Welcome to the comprehensive user guide for **SQLModel Rust**.

SQLModel Rust is a first-principles Rust port of Python's SQLModel library. It provides an intuitive, type-safe developer experience for interacting with SQL databases while leveraging Rust's compile-time safety, zero-cost abstractions, and high performance.

All async operations in SQLModel Rust are powered by [asupersync](https://crates.io/crates/asupersync), ensuring strict cancel-correctness and capability-based structured concurrency without reliance on Tokio.

---

## Guide Overview

The user guide is organized into eleven focused chapters:

1. [**Models & Attributes**](models.md)
   Defining database models with `#[derive(Model)]`, configuring table names, primary keys, auto-incrementing sequences, column constraints, defaults, indexes, timestamps, and soft deletion.

2. [**Query Building**](queries.md)
   Constructing type-safe SQL queries with `select!`, `insert!`, `insert_many!`, `update!`, and `delete!`. Filtering with `Expr`, sorting, pagination, joins, subqueries, Common Table Expressions (CTEs), and upserts (`ON CONFLICT`).

3. [**Session & Unit of Work**](sessions.md)
   Managing unit-of-work lifecycles with `Session`. Understanding the identity map for pointer-equal model sharing, automatic dirty checking, flush dependency ordering, transaction savepoints, and lifecycle hooks.

4. [**Relationships & Loading**](relationships.md)
   Modeling one-to-one, one-to-many, and many-to-many relationships using `Related<T>`, `Lazy<T>`, and `RelatedMany<T>`. Eager loading with `EagerLoader`, batch loaders, and opt-in N+1 query detection.

5. [**Model Inheritance**](inheritance.md)
   Implementing polymorphic model inheritance using Single Table Inheritance (STI), Joined Table Inheritance (JTI), and Concrete Table Inheritance (CTI). Executing polymorphic queries and hydrating subclass hierarchies.

6. [**Schema & Migrations**](migrations.md)
   Generating DDL with `SchemaBuilder`, computing declarative schema diffs, running versioned database migrations with `MigrationRunner`, checksum drift detection, and transactional migration rollbacks.

7. [**Database Drivers**](drivers.md)
   Connecting to supported database engines: C-SQLite (FFI), FrankenSQLite (pure-Rust with MVCC and `BEGIN CONCURRENT`), PostgreSQL (native wire protocol with SCRAM auth and prepared statement caching), and MySQL (native binary protocol). Configuring TLS via `SslMode`.

8. [**Connection Pooling**](pooling.md)
   Configuring connection pools with `Pool`. Setting pool sizing (`min_connections`, `max_connections`), connection timeouts, idle retirement, health checking, read replica routing with `ReplicaPool`, and graceful shutdowns.

9. [**Error Handling & Retries**](errors.md)
   Understanding the four-valued `Outcome<T, E>` (Ok, Err, Cancelled, Panicked), categorizing `Error` variants, inspecting retryable errors with `Error::is_retryable()`, and wrapping operations in `retry_transaction` or `Session::with_retry`.

10. [**Cancellation & Concurrency**](cancellation.md)
    Designing cancel-safe async applications using `asupersync`'s `Cx` context. How cooperative cancellation checkpoints prevent database corruption, transaction leakage, and connection exhaustion.

11. [**Testing Strategies**](testing.md)
    Testing applications with in-memory SQLite, isolated test transactions, deterministic concurrency testing with `asupersync::lab::LabRuntime`, and fault-injection cancellation sweeps with `CancelAt`.

---

## Quick Example

Here is a minimal, self-contained example demonstrating model definition, DDL generation, and query building:

```rust
use sqlmodel::prelude::*;
use sqlmodel::{Dialect, SchemaBuilder};

#[derive(Model, Debug, Clone, PartialEq)]
#[sqlmodel(table = "items")]
pub struct Item {
    #[sqlmodel(primary_key, auto_increment)]
    pub id: Option<i64>,

    #[sqlmodel(index = "items_sku_idx", unique)]
    pub sku: String,

    pub name: String,

    #[sqlmodel(default = "0")]
    pub price_cents: i64,

    #[sqlmodel(nullable)]
    pub description: Option<String>,
}

fn main() {
    // Generate DDL for SQLite
    let statements = SchemaBuilder::new()
        .dialect(Dialect::Sqlite)
        .create_table::<Item>()
        .build();

    assert_eq!(statements.len(), 2); // CREATE TABLE + CREATE INDEX
    assert!(statements[0].contains("CREATE TABLE IF NOT EXISTS \"items\""));
    assert!(statements[1].contains("CREATE INDEX IF NOT EXISTS \"items_sku_idx\""));

    // Build a type-safe query
    let query = select!(Item)
        .filter(Expr::col("price_cents").gt(1000i64))
        .order_by(Expr::col("price_cents").desc())
        .limit(10);

    let (sql, params) = query.build_with_dialect(Dialect::Sqlite);
    assert_eq!(
        sql,
        "SELECT * FROM \"items\" WHERE \"price_cents\" > ?1 ORDER BY \"price_cents\" DESC LIMIT 10"
    );
    assert_eq!(params.len(), 1);
    assert_eq!(params[0], Value::from(1000i64));
}
```

---

## Architectural Philosophy

- **Zero-Cost Abstractions**: All field metadata, column lists, and row converters are evaluated at compile time by procedural macros (`#[derive(Model)]`). There is no runtime reflection.
- **Dialect Awareness**: The query and schema builders understand database engine dialect specifics—such as identifier quoting (`"` vs `` ` ``), parameter placeholders (`?`, `$1`, `?1`), auto-incrementing primary key idioms, and upsert clauses.
- **Cancel-Correctness**: Every database operation takes `&Cx` as its first parameter and returns `Outcome<T, E>`. When an async operation is cancelled, connections and transactions are safely rolled back and cleaned up without leaking server-side state.
- **Explicit Over Implicit**: Relationship loading is explicit (`Lazy<T>` with `load_lazy`), eliminating hidden N+1 performance pitfalls while retaining ergonomics.

For detailed API references, consult the [docs.rs/sqlmodel](https://docs.rs/sqlmodel) documentation.
