# sqlmodel-console

Optional rich console output for SQLModel Rust.

## Role in the SQLModel Rust System
- Renders errors, tables, and schema visuals with rich formatting.
- Auto-detects AI agents/CI and falls back to plain/JSON output.
- Enabled via the `console` feature in sqlmodel or driver crates.

## Usage
Most users should depend on `sqlmodel` and import from `sqlmodel::prelude::*`.
Use this crate directly if you are extending internals or building tooling around the core APIs.

## Links
- Repository: https://github.com/sqlmodel/sqlmodel-rust
- Documentation: https://docs.rs/sqlmodel-console
