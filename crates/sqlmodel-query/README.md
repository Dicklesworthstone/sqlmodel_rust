# sqlmodel-query

Type-safe SQL query builder and expression DSL.

## Role in the SQLModel Rust System
- Provides select!/insert!/update!/delete! macros.
- Builds SQL + params across Postgres/MySQL/SQLite dialects.
- Executes via sqlmodel-core::Connection implementations.

## Usage
Most users should depend on `sqlmodel` and import from `sqlmodel::prelude::*`.
Use this crate directly if you are extending internals or building tooling around the core APIs.

## Diagnostics

`sqlmodel-query` emits structured `tracing` events for inheritance mapping, polymorphic execution, and joined DML transactions:

- **`sqlmodel_query::inheritance`**:
  - `DEBUG`: mapping resolution on builder initialization (`model`, `strategy`, `parent`, `discriminator`).
  - `DEBUG`: joined DML multi-statement transactions (`table`, `op`, `pk_values`) for observable inserts, updates, and deletes without dumping entire rows.
  - `WARN`: ambiguous rows during polymorphic joined hydration when multiple child table prefixes are non-NULL.
- **`sqlmodel_query::polymorphic`**:
  - `TRACE`: generated polymorphic SQL query string, parameters, dialect, and child table prefixes.
- **Error Context**:
  - Parent hydration errors during joined inheritance row hydration are wrapped in `sqlmodel_core::Error::Type` containing the parent table name and column prefix context.

## Links
- Repository: https://github.com/Dicklesworthstone/sqlmodel_rust
- Documentation: https://docs.rs/sqlmodel-query

