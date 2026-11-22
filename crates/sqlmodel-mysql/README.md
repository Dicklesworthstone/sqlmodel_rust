# sqlmodel-mysql

MySQL driver implementing the SQLModel Connection trait.

## Role in the SQLModel Rust System
- Implements the MySQL wire protocol with asupersync I/O.
- Provides authentication, query, and type conversion support.
- Used by sqlmodel-query and sqlmodel-session at runtime.

## Usage
Most users should depend on `sqlmodel` and import from `sqlmodel::prelude::*`.
Use this crate directly if you are extending internals or building tooling around the core APIs.

## Security notes

### RSA password exchange
This driver never holds an RSA private key and never decrypts or signs: it uses `rsa` 0.10
(which resolves historical RUSTSEC-2023-0071) in one place (`src/auth.rs`, `sha256_password_rsa`)
to **encrypt** the scrambled password with the MySQL server's **public** key during
`caching_sha2_password` / `sha256_password` full authentication on a connection that is not
protected by TLS. A unit test in `auth.rs` asserts that no private-key API of `rsa` is referenced.

### Prefer TLS
With `SslMode::Required` (or `VerifyCa` / `VerifyIdentity`) the password is sent inside the
TLS channel and the RSA exchange is never used. TLS-only deployments can drop the `rsa`
dependency with `default-features = false, features = ["tls"]` (omitting `rsa-auth`).

## Links
- Repository: https://github.com/Dicklesworthstone/sqlmodel_rust
- Documentation: https://docs.rs/sqlmodel-mysql
