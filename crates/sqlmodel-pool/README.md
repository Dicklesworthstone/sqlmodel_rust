# sqlmodel-pool

Structured-concurrency-aware connection pooling.

## Role in the SQLModel Rust System
- Budget-aware acquisition via Cx timeouts and cancellation.
- Health checks and lifecycle management for connections.
- Works with any sqlmodel-core::Connection implementation.

## Usage
Most users should depend on `sqlmodel` and import from `sqlmodel::prelude::*`.
Use this crate directly if you are extending internals or building tooling around the core APIs.

## Links
- Repository: https://github.com/sqlmodel/sqlmodel-rust
- Documentation: https://docs.rs/sqlmodel-pool
