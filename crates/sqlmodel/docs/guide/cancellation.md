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
- **Before committing transactions**: A cancellation already observed before the commit is sent prevents that write. A commit sent before cancellation can have an unknown outcome.

On repository `main`, PostgreSQL also registers cancellation and deadline
wakers while wire I/O is pending. An interrupted exchange closes its socket
before releasing the shared connection lock. Other clones then reject new
queries, and a pool with checkout validation retires the connection. This
prevents a later query from receiving rows belonging to an abandoned query.
Cancellation before the first write leaves a clean connection reusable, as
does a server error whose complete response has been consumed.

---

## Transaction Cancellation Guards

One of the most dangerous bugs in traditional database libraries is transaction leakage: when an async future is dropped or cancelled, the server-side transaction may remain open, holding locks indefinitely.

On repository `main`, `SharedPgTransaction` exclusively owns its PostgreSQL
connection until finalization. Other shared handles wait for that ownership
to end. A waiting handle's cancellation or deadline removes that waiter
without cancelling the transaction owner.

Dropping an unfinished transaction closes its socket before releasing the
connection lock. This also covers holder panic and dropped or cancelled
commit/rollback futures. A pool with checkout validation retires the closed
connection on its next acquisition, even if another shared handle survives.
The server aborts an open transaction when it observes the disconnect.

Successful explicit commit and rollback release a healthy idle connection for
reuse before reporting completion. A server `ROLLBACK` acknowledgement in
response to `COMMIT` is reported as `AlreadyRolledBack`, not as a successful
commit. These ownership protections are unreleased and are not part of
published SQLModel 0.5.1.

If a `COMMIT` was sent and its response was lost, closing the socket does not
establish whether the commit succeeded. Treat that result as unknown rather
than assuming the transaction rolled back.

---

## Enforcing Budgets & Deadlines

Using `asupersync`'s budget primitives, you can place strict time limits on database calls:

```rust,no_run
use sqlmodel::prelude::*;
async fn bounded_query(cx: &Cx, conn: &impl Connection) -> Outcome<Vec<Row>, Error> {
    // The caller must have created cx with a two-second deadline.
    conn.query(cx, "SELECT pg_sleep(10)", &[]).await
}
```

The incoming `Cx` must carry a deadline; the SQL statement alone does not set
one. On repository `main`, PostgreSQL returns `Outcome::Cancelled` when that
deadline expires, even while a socket read is pending. If a wire exchange has
started, the connection is closed and must be replaced. These changes are not
part of the published SQLModel 0.5.1 release.

Closing the client socket is not a PostgreSQL `CancelRequest` and does not
prove that the server stopped a statement before applying its effects. A
timeout or cancellation must not be treated as evidence that retrying a
non-idempotent write is safe.

---

## Differences from Python SQLModel

- **Structured Concurrency vs Fire-and-Forget**: Python's `asyncio` allows unbounded background tasks via `asyncio.create_task()` which can outlive request handlers. SQLModel Rust requires all async work to be bounded by structured concurrency scopes.
- **Explicit `Cx` Context**: Python relies on implicit global event loops and thread-local contexts. Rust passes `&Cx` explicitly, making cancellation pathways clear and statically verifiable.
- **Explicit PostgreSQL transaction ownership**: Transaction operations retain exclusive access until finalization. Abandoned transactions retire their connection, while acknowledged commit and rollback preserve healthy reuse.
