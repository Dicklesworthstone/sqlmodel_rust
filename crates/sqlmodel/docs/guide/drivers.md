# Chapter 7: Database Drivers

**SQLModel Rust** decouples the ORM and query builder from underlying storage engines through the `Connection` trait. It provides native, asynchronous drivers for SQLite, PostgreSQL, and MySQL.

---

## Driver Overview

| Driver | Crate | Implementation | Concurrency Features |
|--------|-------|----------------|----------------------|
| **C-SQLite** | `sqlmodel-sqlite` | C FFI (`libsqlite3-sys`) | File-locking transactions (`DEFERRED`, `IMMEDIATE`, `EXCLUSIVE`) |
| **FrankenSQLite** | `sqlmodel-frankensqlite` | 100% Pure Rust | MVCC page-level concurrency (`BEGIN CONCURRENT`) |
| **PostgreSQL** | `sqlmodel-postgres` | Async wire protocol | SCRAM-SHA-256 auth, 64-entry LRU prepared statement cache, TLS |
| **MySQL / MariaDB** | `sqlmodel-mysql` | Async wire protocol | Binary protocol, caching_sha2 / RSA auth, 64-entry LRU cache, TLS |

---

## SQLite Drivers: C-SQLite vs FrankenSQLite

SQLModel offers two SQLite implementations depending on your concurrency and deployment needs:

### 1. C-SQLite (`sqlmodel-sqlite`)
The C-SQLite driver links against the standard C SQLite engine (using the bundled amalgamation). It is the ideal choice for in-memory testing and embedded single-writer applications:

```rust,ignore
use sqlmodel::prelude::*;
use sqlmodel_sqlite::SqliteConnection;

async fn connect_sqlite(cx: &Cx) -> Outcome<SqliteConnection, Error> {
    // In-memory or file-backed database
    SqliteConnection::open_in_memory(cx).await
}
```

### 2. FrankenSQLite (`sqlmodel-frankensqlite`)
The FrankenSQLite driver is a pure-Rust reimplementation of SQLite featuring **page-level MVCC (Multi-Version Concurrency Control)**. It allows concurrent writer transactions without blocking readers or other disjoint writers via `TransactionMode::Concurrent`:

```rust,ignore
use sqlmodel::prelude::*;
use sqlmodel_frankensqlite::FrankenConnection;

async fn connect_franken(cx: &Cx) -> Outcome<FrankenConnection, Error> {
    FrankenConnection::open_in_memory(cx).await
}
```

---

## PostgreSQL Driver (`sqlmodel-postgres`)

The PostgreSQL driver implements the PostgreSQL v3 wire protocol in pure async Rust without any dependence on `libpq`:

```rust,ignore
use sqlmodel::prelude::*;
use sqlmodel_postgres::{PgConfig, SharedPgConnection, SslMode};
use std::time::Duration;

async fn connect_postgres(cx: &Cx) -> Outcome<SharedPgConnection, Error> {
    let config = PgConfig::new("127.0.0.1", "postgres", "my_database")
        .port(5432)
        .password("secret")
        .application_name("my_app")
        .connect_timeout(Duration::from_secs(10))
        .ssl_mode(SslMode::Prefer);

    SharedPgConnection::connect(cx, config).await
}
```

### Features
- **Authentication**: Supports Cleartext, MD5, and `SCRAM-SHA-256` password authentication.
- **Statement Cache**: Parameterized queries automatically use named prepared statements (`sqlmodel_sN`) managed by a bounded 64-entry LRU cache (`STATEMENT_CACHE_CAPACITY`). On cache hits, the `Parse` step is skipped completely. When the cache is full, the oldest statement is closed via `Close(Statement)`.
- **TLS Support**: Configured via `SslMode` (`Disable`, `Prefer`, `Require`, `VerifyCa`, `VerifyFull`) backed by `rustls`.

---

## MySQL & MariaDB Driver (`sqlmodel-mysql`)

The MySQL driver connects directly over the network without requiring `libmysqlclient`:

```rust,ignore
use sqlmodel::prelude::*;
use sqlmodel_mysql::{MySqlConfig, SharedMySqlConnection, SslMode};
use std::time::Duration;

async fn connect_mysql(cx: &Cx) -> Outcome<SharedMySqlConnection, Error> {
    let config = MySqlConfig::new()
        .host("127.0.0.1")
        .port(3306)
        .user("root")
        .password("secret")
        .database("my_database")
        .connect_timeout(Duration::from_secs(10))
        .ssl_mode(SslMode::Preferred);

    SharedMySqlConnection::connect(cx, config).await
}
```

### Features
- **Binary Protocol**: Parameterized queries utilize MySQL's binary protocol (`COM_STMT_PREPARE` and `COM_STMT_EXECUTE`) for high-performance typed data transmission.
- **Statement Cache**: 64-entry LRU statement cache eliminates repeated `COM_STMT_PREPARE` round-trips for identical SQL strings.
- **Authentication**: Supports `mysql_native_password`, `caching_sha2_password`, and `sha256_password`. Includes RSA public-key encryption for non-TLS full authentication.

---

## Differences from Python SQLModel

- **Zero C Library Dependencies for Network DBs**: Python drivers like `psycopg2` or `mysqlclient` rely on C dynamic libraries (`libpq`, `libmysqlclient`). SQLModel Rust drivers are pure async Rust protocol implementations.
- **Pure-Rust MVCC SQLite**: Python is constrained to the standard C `sqlite3` GIL-bound single-writer model. SQLModel Rust offers `sqlmodel-frankensqlite` for non-blocking concurrent writes.
- **Automatic Server-Side Statement Caching**: Both MySQL and PostgreSQL drivers provide built-in LRU prepared statement caching on every query without requiring manual driver configuration.
