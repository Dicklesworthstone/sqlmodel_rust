# sqlmodel-frankensqlite

Pure-Rust SQLite driver for SQLModel Rust, backed by [FrankenSQLite](https://github.com/Dicklesworthstone/frankensqlite) (`fsqlite`): a ground-up SQLite reimplementation with page-level MVCC and Serializable Snapshot Isolation.

## Role in the SQLModel Rust System
- Implements the `sqlmodel-core` `Connection` trait without any C dependency; a drop-in alternative to `sqlmodel-sqlite` for the same models, queries, sessions, and pools.
- Adds what C SQLite cannot offer: concurrent writers on one database file via `BEGIN CONCURRENT`.
- Keeps the same sync helper family as `sqlmodel-sqlite` (`execute_raw`, `query_sync`, `execute_sync`, `begin_sync`/`commit_sync`/`rollback_sync`, `changes`, `last_insert_rowid`, `close_sync`) so existing call sites move over unchanged.

## Requirements
- **Nightly Rust.** `fsqlite-pager` enables `core_intrinsics` on x86_64, so this crate is nightly-only while the rest of the workspace compiles on stable 1.95+. The repository pins `nightly-2026-08-25` in `rust-toolchain.toml`.
- **fsqlite 0.3.14** (the version the workspace tests against; the lockstep `fsqlite-core`, `fsqlite-types`, and `fsqlite-error` crates are pinned to the same version). FrankenSQLite releases weekly; this crate tracks it in the workspace's dependency refreshes.

## Opening a connection
| Constructor | Use |
|---|---|
| `FrankenConnection::open(path)` | General-purpose file database (created if missing). |
| `open_memory()` | In-memory database for tests. |
| `open_file(path)` / `open_file_with_page_size(path, size)` | Explicit file open, optionally with a page size. |
| `open_existing(path)` | Strictly query-only callers: fails if the file does not exist, never creates one. |
| `open_file_read_only(path)` | Read-only handle. |
| `open_schema_only(path)` | Schema inspection without loading data pages. |
| `open_strict_durable_control_plane_file(path)` | Control-plane profile: verifies file identity, installs and checks the strict-durability pragmas, and refuses to proceed if they cannot be enforced. Pair with `with_exclusive_transaction[_result]` for exclusive, typed-failure transactions. |

## Transactions and concurrent writers
`Connection::begin_with(cx, IsolationLevel)` maps `Serializable` to `BEGIN EXCLUSIVE`, `RepeatableRead`/`ReadCommitted` to `BEGIN IMMEDIATE`, and `ReadUncommitted` to `BEGIN DEFERRED`.

Concurrent MVCC writers use `TransactionMode::Concurrent`, which issues `BEGIN CONCURRENT`:

```rust
use sqlmodel_core::{Connection, TransactionOps, TransactionOptions, RetryPolicy, retry_transaction, Value};

// One transaction, started concurrently:
let tx = conn.begin_with_options(&cx, TransactionOptions::concurrent()).await.into_result()?;
tx.execute(&cx, "UPDATE accounts SET balance = balance - 10 WHERE id = ?1", &[Value::BigInt(1)]).await.into_result()?;
tx.commit(&cx).await.into_result()?;

// The same, retried automatically when two writers conflict:
retry_transaction(&cx, &conn, TransactionOptions::concurrent(), &RetryPolicy::default(), async |cx, tx| {
    tx.execute(cx, "UPDATE accounts SET balance = balance - 10 WHERE id = ?1", &[Value::BigInt(1)]).await
}).await;
```

`Session` users set `SessionConfig::default().with_transaction_mode(TransactionMode::Concurrent)`. Two connections writing disjoint rows both commit; writers that touch the same page fail with a serialization error for which `Error::is_retryable()` is true. `retry_transaction` re-runs the whole transaction with jittered exponential backoff, never past the `Cx` budget deadline, and never after a cancellation. The sync helper family has `begin_concurrent_sync()` for the same purpose. C SQLite (`sqlmodel-sqlite`) rejects `TransactionMode::Concurrent` with `TransactionErrorKind::UnsupportedMode` rather than silently downgrading.

## Differences from C SQLite
- Triggers and direct `sqlite_master` queries still require `sqlmodel-sqlite` (C SQLite).
- `last_insert_rowid` is tracked by the adapter.
- Error messages differ in wording; match on `Error` kinds, not text.
An ORM-level differential test against `sqlmodel-sqlite` (bd-slot.9) maintains the authoritative list.

## Concurrency model
`fsqlite`'s engine is single-threaded (`Rc`/`RefCell`) and its futures are not `Send`. The adapter keeps a private current-thread asupersync runtime per connection and drives every fsqlite future to completion inside one blocking call while holding the connection mutex, so the engine never crosses a thread. That is why `FrankenConnection` is `Send + Sync` and why the `Connection` trait's futures stay `Send`. Concurrency comes from opening several connections (for example through `sqlmodel-pool`), not from sharing one. Pool retirement can skip the final WAL checkpoint via `close_without_checkpoint_sync` for contention-safe teardown.

## Usage
Most users should depend on `sqlmodel` plus this crate and import from `sqlmodel::prelude::*`:

```toml
[dependencies]
sqlmodel = "0.4"
sqlmodel-frankensqlite = "0.4"
```

Use this crate directly if you are extending internals or building tooling around the core APIs.

## Links
- Repository: https://github.com/Dicklesworthstone/sqlmodel_rust
- Documentation: https://docs.rs/sqlmodel-frankensqlite
- FrankenSQLite: https://github.com/Dicklesworthstone/frankensqlite
