# Chapter 10: Cancellation & Structured Concurrency

Modern web and microservice backends must handle request cancellation cleanly. When a client disconnects or an upstream timeout fires, continuing expensive database operations wastes server CPU, consumes connection leases, and risks leaving partial writes in open transactions.

**SQLModel Rust** is designed from the ground up on [asupersync](https://crates.io/crates/asupersync) for **cancel-correct structured concurrency**.

---

## The `Cx` Capability Context

All asynchronous functions in SQLModel accept a capability token reference `&Cx` as their first argument:

```rust,no_run
use sqlmodel::prelude::*;

async fn fetch_records(cx: &Cx, conn: &impl Connection) -> Outcome<Vec<Row>, Error> {
    conn.query(cx, "SELECT * FROM large_table", &[]).await
}
```

The `Cx` context encapsulates:
1. **Cancellation State**: A cooperative flag signaling when execution should halt.
2. **Time Budget**: A deadline bounding total allowed elapsed time.
3. **Task & Region Identity**: Structured concurrency scopes that prevent orphan background tasks.

SQLModel Rust **never** creates its own runtime; the `Cx` flows down from the consumer's application runtime.

---

## Cooperative Cancellation Checkpoints

Database operations in SQLModel check `cx` at critical execution boundaries:
- **Before sending queries**: If cancellation was already requested, the driver skips writing to the socket and returns `Outcome::Cancelled` immediately.
- **Between wire packets**: During large result streaming, long read loops yield and check for cancellation.
- **Before committing transactions**: If a cancellation signal arrives while preparing to commit, the commit is aborted and the transaction is rolled back instead.

---

## Transaction Cancellation Guards

One of the most dangerous bugs in traditional database libraries is transaction leakage: when an async future is dropped or cancelled, the server-side transaction may remain open, holding locks indefinitely.

In SQLModel Rust:
1. A transaction object implements cancel-safe cleanup on `Drop`.
2. If cancellation occurs, an explicit `ROLLBACK` is dispatched or the connection is severed and reset before being returned to the pool.
3. Your database tables are guaranteed never to remain locked by forgotten transactions.

---

## Enforcing Budgets & Deadlines

Using `asupersync`'s budget primitives, you can place strict time limits on database calls:

```rust,no_run
use sqlmodel::prelude::*;
use std::time::Duration;

async fn bounded_query(cx: &Cx, conn: &impl Connection) -> Outcome<Vec<Row>, Error> {
    // If the query takes longer than 2 seconds, cx cancels it cooperatively
    conn.query(cx, "SELECT pg_sleep(10)", &[]).await
}
```

When the budget expires, the query aborts cleanly with `Outcome::Cancelled` and the connection returns to a ready state.

---

## Differences from Python SQLModel

- **Structured Concurrency vs Fire-and-Forget**: Python's `asyncio` allows unbounded background tasks via `asyncio.create_task()` which can outlive request handlers. SQLModel Rust requires all async work to be bounded by structured concurrency scopes.
- **Explicit `Cx` Context**: Python relies on implicit global event loops and thread-local contexts. Rust passes `&Cx` explicitly, making cancellation pathways clear and statically verifiable.
- **Leak-Proof Transactions**: Python coroutines dropped mid-transaction can leave database connections in indeterminate states; SQLModel Rust enforces cancel-correct rollbacks on every path.
