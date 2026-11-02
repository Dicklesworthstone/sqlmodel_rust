# Chapter 3: Session & Unit of Work

The `Session` in **SQLModel Rust** implements the **Unit of Work** and **Identity Map** patterns. Instead of issuing immediate, isolated database writes for every change, a `Session` tracks entity states in memory, automatically detects modifications, and flushes operations to the database in dependency-ordered batches within transactions.

---

## The Unit of Work Lifecycle

A `Session` wraps an underlying database connection:

```rust,no_run
use sqlmodel::prelude::*;
use sqlmodel::Session;

#[derive(Model, Debug, Clone, serde::Serialize, serde::Deserialize)]
#[sqlmodel(table = "accounts")]
pub struct Account {
    #[sqlmodel(primary_key)]
    pub id: i64,
    pub balance: i64,
}

async fn transfer(
    cx: &Cx,
    conn: impl Connection + 'static,
    from_id: i64,
    to_id: i64,
    amount: i64,
) -> Outcome<(), Error> {
    let mut session = Session::new(conn);

    // Fetch accounts into the session identity map
    let mut from_acc: Account = match session.get(cx, from_id).await {
        Outcome::Ok(Some(a)) => a,
        Outcome::Ok(None) => return Outcome::Err(Error::Custom("Source account not found".into())),
        Outcome::Err(e) => return Outcome::Err(e),
        Outcome::Cancelled(r) => return Outcome::Cancelled(r),
        Outcome::Panicked(p) => return Outcome::Panicked(p),
    };

    let mut to_acc: Account = match session.get(cx, to_id).await {
        Outcome::Ok(Some(a)) => a,
        Outcome::Ok(None) => return Outcome::Err(Error::Custom("Target account not found".into())),
        Outcome::Err(e) => return Outcome::Err(e),
        Outcome::Cancelled(r) => return Outcome::Cancelled(r),
        Outcome::Panicked(p) => return Outcome::Panicked(p),
    };

    from_acc.balance -= amount;
    to_acc.balance += amount;

    // Register modifications
    session.add(&from_acc);
    session.add(&to_acc);

    // Commit automatically flushes dirty models inside a transaction
    session.commit(cx).await
}
```

---

## Identity Map & Reference Identity

The **Identity Map** ensures that within a single `Session`, any primary key maps to exactly one in-memory object:

1. Calling `session.get(cx, id)` for an already loaded ID returns the cached instance without querying the database again.
2. Changes made to an entity in one part of your code are immediately visible to any other part accessing that entity through the session.
3. It prevents duplicate memory allocations and prevents conflicting update statements from overwriting each other.

---

## Automatic Dirty Tracking

When models are added to a session or loaded via `session.get`, they are tracked by `TrackedModel<M>`:
- The session snapshots the initial column values upon load.
- When `session.flush(cx)` is called, the session compares current values against the initial snapshot.
- Only columns that actually changed are included in the generated `UPDATE ... SET ...` statement. If no columns were modified, no query is sent.

---

## Core Session Operations

| Method | Behavior |
|--------|----------|
| `session.add(model)` | Registers a new or updated model in the session's unit of work. |
| `session.get(cx, pk)` | Looks up a model by primary key, checking the identity map first. |
| `session.delete(model)` | Marks an entity for deletion upon the next flush. |
| `session.flush(cx)` | Executes pending INSERTs, UPDATEs, and DELETEs to the database. |
| `session.commit(cx)` | Flushes pending changes and commits the transaction. |
| `session.rollback(cx)` | Discards uncommitted changes and resets the session identity map. |
| `session.refresh(cx, &mut model)` | Re-reads current column values from the database into the model. |
| `session.merge(cx, model)` | Incorporates the state of a detached model into the session. |

### Transaction acknowledgements and lifecycle events

PostgreSQL can respond to `COMMIT` with a `ROLLBACK` acknowledgement when a
previous statement aborted the transaction. `Session::commit` returns
`Error::Transaction` with `TransactionErrorKind::AlreadyRolledBack` in that
case. It marks the transaction inactive, removes tracked inserts whose rows
were rolled back, expires other affected persistent objects, and fires
`AfterRollback` exactly once. It does not fire `AfterCommit`. A failing
`AfterRollback` callback is logged without replacing the database's rollback
result.

A normal acknowledged commit still fires `AfterCommit`; explicit rollback
fires `AfterRollback`. PostgreSQL connections remain usable after these
completed exchanges. Other errors or cancellation do not establish a
definitive transaction outcome: the Session retains its conservative state
until rollback succeeds, and an interrupted PostgreSQL wire exchange closes
the socket to prevent unsafe reuse.

Connection adapters must forward `Connection::commit_transaction` and
`Connection::rollback_transaction` to their inner driver. Using the default
raw SQL implementation in a wrapper would discard a PostgreSQL driver's
command-tag checks. These hooks are for owners such as Session that issue
`BEGIN` directly; typed transactions returned by `Connection::begin` continue
to use their own `TransactionOps::commit` and `TransactionOps::rollback`.

---

## Resilient Transactions with `with_retry`

Transient database errors—such as serialization failures, deadlock aborts, or optimistic concurrency write-skew conflicts—can be automatically retried using `Session::with_retry`:

```rust,ignore
use sqlmodel::prelude::*;
use sqlmodel::{RetryPolicy, Session};

# #[derive(Model, Debug, Clone, serde::Serialize, serde::Deserialize)]
# #[sqlmodel(table = "counters")]
# pub struct Counter {
#     #[sqlmodel(primary_key)]
#     pub id: i64,
#     pub count: i64,
# }
async fn increment_counter(
    cx: &Cx,
    conn: impl Connection + 'static,
) -> Outcome<i64, Error> {
    let mut session = Session::new(conn);
    let policy = RetryPolicy::new().max_attempts(3);

    session.with_retry(cx, &policy, |cx, s| async move {
        let counter_opt: Option<Counter> = match s.get(cx, 1i64).await {
            Outcome::Ok(c) => c,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };
        let mut counter: Counter = counter_opt.expect("counter exists");
        counter.count += 1;
        let new_val = counter.count;
        s.add(&counter);
        Outcome::Ok(new_val)
    }).await
}
```

Between retry attempts:

- The database must acknowledge rollback of the failed attempt.
- The identity map and pending queues are cleared after that acknowledgement.
- Backoff respects the `Cx` budget, and every new attempt checks the deadline
  before issuing `BEGIN` or entering the work closure. Immediate retry policies
  obey the same attempt boundary.

### Interrupted cleanup requires explicit recovery

If rollback returns an error, cancellation, or panic outcome, `with_retry`
stops without running another attempt. It retains the transaction flag,
tracked objects, pending operations, and inserted-object ownership. Dropping
the retry future during work or rollback also preserves that ownership and
marks the Session as requiring rollback.

Subsequent `begin`, `flush`, `commit`, `with_retry`, bulk writes, and relationship
flushes return `TransactionErrorKind::RollbackRequired` before issuing SQL.
Rejected relationship flushes leave the caller's pending links and unlinks
intact. Call `session.rollback` with a live context to recover. Only an
acknowledged rollback clears this state; PostgreSQL may instead have closed
an interrupted wire exchange, in which case the connection needs replacement.

When failed work returned an ordinary error, a failed cleanup's outcome is
returned immediately. If the original work was cancelled or panicked, that
original terminal outcome is preserved and the cleanup failure is logged.
Neither case permits another attempt before recovery.

### A committed attempt is never replayed automatically

An `AfterCommit` callback runs after the database has committed. Its error
does not undo those writes. `with_retry` returns the callback error directly,
preserves committed tracking, and never invokes the retry policy for that
attempt, even if the callback error is normally retryable.

The same rule applies if the work closure calls `Session::commit` itself and
then fails. If it starts another transaction after that commit, cleanup rolls
back the later transaction while retaining the earlier durable objects in the
identity map. Work is still not replayed. A failed cleanup retains the recovery
state described above.

These guarantees track Session-managed commits. Do not issue raw transaction
control through `session.connection()` or commit through an independent alias
inside retry work: those operations bypass the Session's commit tracking.
External effects such as sending a message also remain the application's
responsibility when designing retryable work.

---

## Differences from Python SQLModel

- **Explicit Concurrency & Bounds**: In Python SQLAlchemy/SQLModel, sessions rely on thread-local contexts or asyncio task contexts with hidden event loops. In Rust, `Session` takes explicit `&Cx` contexts and executes within structured concurrency regions.
- **Cancellation and ownership**: Interrupted retry work retains its transaction state for explicit rollback with a live context. PostgreSQL closes incomplete wire exchanges, and pool checkout rejects unfinished Session transactions before lending a connection to another borrower.
- **Typed Reference Identity**: Identity mapping uses `Arc<RwLock<M>>` internally for safe interior mutability and dirty checking without requiring Python's dynamic `__dict__` instrumentation.
- **Built-in Resilient Retries**: `Session::with_retry` handles clean identity map clearing, transaction rollbacks, and budget-aware backoff natively.
