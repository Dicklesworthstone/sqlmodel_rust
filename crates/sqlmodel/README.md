# sqlmodel

Facade crate for SQLModel Rust; re-exports the full ORM stack behind a single dependency.

## Role in the SQLModel Rust System
- Primary user-facing entry point (prelude, macros, query builders, schema tools).
- Glue layer over sqlmodel-core, sqlmodel-macros, sqlmodel-query, sqlmodel-schema, sqlmodel-session, sqlmodel-pool.
- Optional console integration via the `console` feature.

## Usage
Most users should depend on `sqlmodel` and import from `sqlmodel::prelude::*`.
Use this crate directly if you are extending internals or building tooling around the core APIs.

## Links
- Repository: https://github.com/sqlmodel/sqlmodel-rust
- Documentation: https://docs.rs/sqlmodel
