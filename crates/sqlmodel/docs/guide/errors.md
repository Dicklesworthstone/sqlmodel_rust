# Chapter 9: Error Handling & Retries

Robust database systems must handle errors gracefully, distinguishing between permanent constraint violations, transient concurrency conflicts, and deliberate request cancellations. **SQLModel Rust** achieves this using a unified `Error` taxonomy and the four-valued `Outcome<T, E>` type.

---

## The Four-Valued `Outcome<T, E>`

Unlike standard Rust `Result<T, E>`—which only represents binary success or failure—all asynchronous database calls in SQLModel Rust return `Outcome<T, E>` from [asupersync](https://crates.io/crates/asupersync):

```rust,ignore
pub enum Outcome<T, E> {
    /// The operation succeeded with a value.
    Ok(T),
    /// The operation failed with a domain error.
    Err(E),
    /// The operation was cancelled cooperatively via the `Cx` context.
    Cancelled(asupersync::outcome::CancelReason),
    /// The operation panicked, caught at a task boundary.
    Panicked(asupersync::outcome::PanicPayload),
}
```

### Why Four Values?
In Python and standard async Rust, cancellation is often represented as an exception (`asyncio.CancelledError`) or dropped future. This easily leads to:
1. **Accidental swallowing**: A catch-all `catch_unwind` or `except Exception:` block captures cancellation and treats it as a business logic error.
2. **Resource leakage**: Open transactions remain uncommitted on the database server.

With `Outcome`, cancellation is an explicit variant that flows up the call stack, ensuring transactions are cleanly aborted and pooled connections are reclaimed.

### The `nightly-try` Feature
When compiling on Rust nightly with the `nightly-try` feature enabled, `?` works directly on `Outcome`:
```toml
[dependencies]
sqlmodel = { version = "0.4.3", features = ["nightly-try"] }
```
This enables seamless error propagation across async functions.

---

## The `Error` Taxonomy

All crate errors are consolidated into `sqlmodel_core::Error`:

```rust
use sqlmodel::Error;
use sqlmodel_core::error::{QueryError, QueryErrorKind};

# fn main() {
let err = Error::Query(QueryError {
    kind: QueryErrorKind::Syntax,
    sql: None,
    sqlstate: None,
    message: "syntax error near WHERE".into(),
    detail: None,
    hint: None,
    position: None,
    source: None,
});
match &err {
    Error::Connection(e) => println!("Network or authentication failure: {e}"),
    Error::Query(e) => match e.kind {
        QueryErrorKind::Constraint => println!("Constraint violated: {e}"),
        QueryErrorKind::Syntax => println!("Invalid SQL syntax: {e}"),
        _ => println!("Other query error: {e}"),
    },
    Error::Schema(e) => println!("Schema migration error: {e}"),
    Error::Validation(e) => println!("Data validation failed: {e}"),
    Error::Transaction(e) => println!("Transaction failed: {e}"),
    Error::Pool(e) => println!("Connection pool exhausted: {e}"),
    _ => println!("Other error: {err}"),
}
# }
```

---

## Retrying Transient Errors with `retry_transaction`

Database transactions under high concurrency frequently encounter transient write conflicts (e.g. PostgreSQL `40001` serialization failures, deadlocks, or FrankenSQLite MVCC conflicts).

SQLModel provides `Error::is_retryable()` to detect these conditions, paired with `retry_transaction` and `Session::with_retry`:

```rust,ignore
use sqlmodel::prelude::*;
use sqlmodel::{RetryPolicy, TransactionOptions, retry_transaction};

async fn transfer_funds<'c, C: Connection>(
    cx: &Cx,
    conn: &'c C,
    from_id: i64,
    to_id: i64,
    amount: i64,
) -> Outcome<(), Error> {
    let policy = RetryPolicy::new().max_attempts(5);

    retry_transaction(cx, conn, TransactionOptions::default(), &policy, |cx, tx| async move {
        // Run transactional queries here...
        // If a retryable conflict occurs, the transaction rolls back
        // and the closure executes again after backoff.
        Outcome::Ok(())
    }).await
}
```

### Guarantees of `retry_transaction`:
- Respects the `Cx` deadline: It will never sleep past the cancellation budget.
- Clean rollback: Failed attempts are rolled back before retrying.
- Non-retryable errors (e.g. UNIQUE constraint violations) fail immediately on the first attempt.

---

## Differences from Python SQLModel

- **Four-Valued Outcome vs Exception Bubbling**: Python relies on exception throwing, where cancellation can be swallowed by generic `except Exception` blocks. Rust models cancellation as a first-class outcome variant.
- **Categorized Error Kinds**: Rather than inspecting raw database error strings or regexes, SQLModel Rust provides structured error enumerations (`ConnectionErrorKind`, `QueryErrorKind`).
- **Standardized Retry Mechanics**: `is_retryable()` and `retry_transaction` remove the need for external libraries like `tenacity` while honoring structured cancellation budgets.
