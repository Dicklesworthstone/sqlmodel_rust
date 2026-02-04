# sqlmodel-schema

Schema generation, DDL, and migrations for SQLModel Rust.

## Role in the SQLModel Rust System
- Derives expected schema from Model metadata.
- Generates dialect-specific CREATE/ALTER SQL.
- Provides diffing and migration runner utilities.

## Usage
Most users should depend on `sqlmodel` and import from `sqlmodel::prelude::*`.
Use this crate directly if you are extending internals or building tooling around the core APIs.

## Links
- Repository: https://github.com/sqlmodel/sqlmodel-rust
- Documentation: https://docs.rs/sqlmodel-schema
