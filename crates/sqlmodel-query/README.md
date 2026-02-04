# sqlmodel-query

Type-safe SQL query builder and expression DSL.

## Role in the SQLModel Rust System
- Provides select!/insert!/update!/delete! macros.
- Builds SQL + params across Postgres/MySQL/SQLite dialects.
- Executes via sqlmodel-core::Connection implementations.

## Usage
Most users should depend on `sqlmodel` and import from `sqlmodel::prelude::*`.
Use this crate directly if you are extending internals or building tooling around the core APIs.

## Links
- Repository: https://github.com/sqlmodel/sqlmodel-rust
- Documentation: https://docs.rs/sqlmodel-query
