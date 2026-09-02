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

### RSA password exchange and RUSTSEC-2023-0071
`cargo audit` reports RUSTSEC-2023-0071 ("Marvin Attack") against the `rsa` crate this driver
depends on. That advisory concerns recovering a **private** key through timing differences in
PKCS#1 v1.5 **decryption**. This driver never holds an RSA private key and never decrypts or
signs: it uses `rsa` in one place (`src/auth.rs`, `sha256_password_rsa`) to **encrypt** the
scrambled password with the MySQL server's **public** key during `caching_sha2_password` /
`sha256_password` full authentication on a connection that is not protected by TLS. The
decrypting party is the MySQL server (OpenSSL), not this crate. A unit test in `auth.rs`
asserts that no private-key API of `rsa` is referenced, so this argument cannot silently
stop being true. The advisory is therefore ignored in `.cargo/audit.toml` with a review date.

### Prefer TLS
With `SslMode::Required` (or `VerifyCa` / `VerifyIdentity`) the password is sent inside the
TLS channel and the RSA exchange is never used. TLS-only deployments can additionally drop the
`rsa` dependency once the `rsa-auth` feature gate lands (tracked as bd-j7wt.2).

## Links
- Repository: https://github.com/Dicklesworthstone/sqlmodel_rust
- Documentation: https://docs.rs/sqlmodel-mysql
