# sqlmodel-macros

Proc-macros that generate Model metadata and validation code.

## Role in the SQLModel Rust System
- Derive macros for Model, Validate, and JsonSchema.
- Generates compile-time metadata used by query and schema layers.
- Used indirectly through the sqlmodel facade.

## Usage
Most users should depend on `sqlmodel` and import from `sqlmodel::prelude::*`.
Use this crate directly if you are extending internals or building tooling around the core APIs.

## Links
- Repository: https://github.com/sqlmodel/sqlmodel-rust
- Documentation: https://docs.rs/sqlmodel-macros
