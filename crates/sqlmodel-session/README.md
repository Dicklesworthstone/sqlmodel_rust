# sqlmodel-session

Unit-of-work and identity map layer for SQLModel Rust.

## Role in the SQLModel Rust System
- Tracks object identity and pending changes before flush.
- Coordinates transactional commit/rollback flows.
- Runs on top of sqlmodel-core::Connection and query builders.

## Usage
Most users should depend on `sqlmodel` and import from `sqlmodel::prelude::*`.
Use this crate directly if you are extending internals or building tooling around the core APIs.

## Links
- Repository: https://github.com/sqlmodel/sqlmodel-rust
- Documentation: https://docs.rs/sqlmodel-session
