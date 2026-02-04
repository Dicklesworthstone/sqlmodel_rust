# sqlmodel-sqlite

SQLite driver implementing the SQLModel Connection trait.

## Role in the SQLModel Rust System
- FFI-backed SQLite execution and type conversion.
- Lightweight backend for local/dev/test usage.
- Used by sqlmodel-query and sqlmodel-session at runtime.

## Usage
Most users should depend on `sqlmodel` and import from `sqlmodel::prelude::*`.
Use this crate directly if you are extending internals or building tooling around the core APIs.

## Links
- Repository: https://github.com/sqlmodel/sqlmodel-rust
- Documentation: https://docs.rs/sqlmodel-sqlite
