# sqlmodel-core

Core traits and types used across the SQLModel Rust ecosystem.

## Role in the SQLModel Rust System
- Defines Model and Connection contracts plus Row/Value/Error types.
- Re-exports Cx and Outcome for cancel-correct operations.
- Consumed by query, schema, session, and driver crates.

## Usage
Most users should depend on `sqlmodel` and import from `sqlmodel::prelude::*`.
Use this crate directly if you are extending internals or building tooling around the core APIs.

## Links
- Repository: https://github.com/sqlmodel/sqlmodel-rust
- Documentation: https://docs.rs/sqlmodel-core
