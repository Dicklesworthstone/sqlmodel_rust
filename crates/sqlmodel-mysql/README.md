# sqlmodel-mysql

MySQL wire protocol driver for SQLModel Rust.

## Status

**Current State**: Async text-protocol connection works via `MySqlAsyncConnection`. Use
`SharedMySqlConnection` (async mutex wrapper) for a correct `sqlmodel_core::Connection`
implementation.

### What Works

- TCP connection establishment
- Protocol handshake (v10)
- Authentication (mysql_native_password, caching_sha2_password)
- Text protocol queries with parameter binding
- Error handling with MySQL error codes
- Connection state machine
- Type encoding/decoding for common types
- Async connection (`MySqlAsyncConnection`) + `Connection` via `SharedMySqlConnection`
- 58 unit tests passing

### What's Missing

- [ ] **Prepared statements** - Binary protocol (COM_STMT_PREPARE, COM_STMT_EXECUTE)
- [ ] **SSL/TLS** - Encrypted connections
- [ ] **Connection trait for MySqlAsyncConnection** - Remove the current stub (requires interior mutability or trait changes)

## Integration Tests

The integration suite in `crates/sqlmodel-mysql/tests/mysql_integration.rs` is **env-gated**:
it will skip unless `SQLMODEL_TEST_MYSQL_URL` is set.

Example:

```bash
export SQLMODEL_TEST_MYSQL_URL='mysql://root:secret@127.0.0.1:3306/sqlmodel_test'
cargo test -p sqlmodel-mysql --test mysql_integration -- --nocapture
```

Note: Some MySQL 8 configurations using `caching_sha2_password` may require TLS for full auth;
until `bd-38q` is implemented, use a test user configured for `mysql_native_password` (or
otherwise compatible auth settings).

## Current API (Synchronous)

```rust
use sqlmodel_mysql::{MySqlConfig, MySqlConnection};
use sqlmodel_core::Value;

// Connect
let config = MySqlConfig::new()
    .host("localhost")
    .port(3306)
    .user("root")
    .password("secret")
    .database("mydb");

let mut conn = MySqlConnection::connect(config)?;

// Query with parameters
let rows = conn.query_sync(
    "SELECT * FROM users WHERE id = ?",
    &[Value::Int(1)]
)?;

// Execute statement
let affected = conn.execute_sync(
    "UPDATE users SET name = ? WHERE id = ?",
    &[Value::Text("Alice".into()), Value::Int(1)]
)?;

// Insert and get last ID
let id = conn.insert_sync(
    "INSERT INTO users (name) VALUES (?)",
    &[Value::Text("Bob".into())]
)?;

// Ping to check connection
conn.ping()?;

// Close gracefully
conn.close()?;
```

## Migration to Async

The async migration requires:

1. **Replace TcpStream**: Use `asupersync::net::TcpStream` instead of `std::net::TcpStream`
2. **Async I/O**: Convert `read_packet`/`write_packet` to async with `.await`
3. **Add Cx context**: All async methods take `&Cx` for cancellation support
4. **Return Outcome**: Use `Outcome<T, E>` instead of `Result<T, E>`
5. **Implement Connection trait**: Match the `sqlmodel_core::Connection` interface

Example target API:

```rust
use sqlmodel_core::{Connection, Value};
use asupersync::{Cx, Outcome};

// With async Connection trait
async fn example(cx: &Cx, conn: &impl Connection) -> Outcome<(), sqlmodel_core::Error> {
    let rows = conn.query(cx, "SELECT * FROM users WHERE id = ?", &[Value::Int(1)]).await?;
    let tx = conn.begin(cx).await?;
    tx.execute(cx, "INSERT INTO logs (msg) VALUES (?)", &[Value::Text("action".into())]).await?;
    tx.commit(cx).await?;
    Outcome::Ok(())
}
```

## Type Mapping

| MySQL Type | Value Variant | Rust Type |
|------------|---------------|-----------|
| TINYINT | `Value::TinyInt` | `i8` |
| SMALLINT | `Value::SmallInt` | `i16` |
| INT | `Value::Int` | `i32` |
| BIGINT | `Value::BigInt` | `i64` |
| FLOAT | `Value::Float` | `f32` |
| DOUBLE | `Value::Double` | `f64` |
| DECIMAL | `Value::Decimal` | `String` (arbitrary precision) |
| VARCHAR, TEXT | `Value::Text` | `String` |
| BLOB, BINARY | `Value::Bytes` | `Vec<u8>` |
| DATE | `Value::Date` | `i32` (days since epoch) |
| TIME | `Value::Time` | `i64` (microseconds since midnight) |
| DATETIME | `Value::Timestamp` | `i64` (microseconds since epoch) |
| TIMESTAMP | `Value::TimestampTz` | `i64` (microseconds since epoch, UTC) |
| JSON | `Value::Json` | `serde_json::Value` |

## References

- [MySQL Protocol Documentation](https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html)
- [MariaDB Protocol](https://mariadb.com/kb/en/clientserver-protocol/)
