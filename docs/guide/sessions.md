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
- The open transaction is safely rolled back.
- The identity map is cleared to eliminate stale state.
- Exponential backoff jitter is applied, strictly bounded by the `Cx` budget.

---

## Differences from Python SQLModel

- **Explicit Concurrency & Bounds**: In Python SQLAlchemy/SQLModel, sessions rely on thread-local contexts or asyncio task contexts with hidden event loops. In Rust, `Session` takes explicit `&Cx` contexts and executes within structured concurrency regions.
- **Cancel-Safety**: Python sessions can leave dangling transactions if a coroutine is cancelled mid-execution. SQLModel Rust guarantees that a dropped or cancelled session safely rolls back and releases pooled connections.
- **Typed Reference Identity**: Identity mapping uses `Arc<RwLock<M>>` internally for safe interior mutability and dirty checking without requiring Python's dynamic `__dict__` instrumentation.
- **Built-in Resilient Retries**: `Session::with_retry` handles clean identity map clearing, transaction rollbacks, and budget-aware backoff natively.
