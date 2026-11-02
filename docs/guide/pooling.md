# Chapter 8: Connection Pooling

In high-throughput services, establishing a new TCP/TLS database connection for every incoming request introduces severe latency and server overhead. **SQLModel Rust** provides `Pool`, a generic, asynchronous, cancel-aware connection pool designed for structured concurrency.

---

## Configuring and Creating a Pool

A connection pool is configured via `PoolConfig` and instantiated with a factory closure:

```rust,ignore
use sqlmodel::prelude::*;
use sqlmodel_pool::{Pool, PoolConfig};
use sqlmodel_postgres::{PgConfig, SharedPgConnection};
use std::time::Duration;

async fn create_pg_pool(cx: &Cx) -> Outcome<Pool<SharedPgConnection>, Error> {
    let pg_cfg = PgConfig::new("127.0.0.1", "postgres", "app_db")
        .password("secret");

    let pool_cfg = PoolConfig::default()
        .min_connections(5)
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Duration::from_secs(300))
        .max_lifetime(Duration::from_secs(3600));

    Pool::new(pool_cfg, move |cx| {
        let cfg = pg_cfg.clone();
        async move { SharedPgConnection::connect(cx, cfg).await }
    })
}
```

---

## Acquiring and Using Connections

Acquiring a pooled connection returns a smart-pointer lease (`PooledConnection<C>`):

```rust,ignore
use sqlmodel::prelude::*;
use sqlmodel_pool::Pool;

# #[derive(Model, Debug, Clone)]
# #[sqlmodel(table = "users")]
# pub struct User { #[sqlmodel(primary_key)] pub id: i64 }
async fn get_user_count(cx: &Cx, pool: &Pool<impl Connection>) -> Outcome<i64, Error> {
    // Acquire a connection from the pool
    let conn = match pool.acquire(cx).await {
        Outcome::Ok(c) => c,
        Outcome::Err(e) => return Outcome::Err(e),
        Outcome::Cancelled(r) => return Outcome::Cancelled(r),
        Outcome::Panicked(p) => return Outcome::Panicked(p),
    };

    // Use the pooled connection like any other Connection
    let rows = match conn.query(cx, "SELECT count(*) FROM users", &[]).await {
        Outcome::Ok(r) => r,
        Outcome::Err(e) => return Outcome::Err(e),
        Outcome::Cancelled(r) => return Outcome::Cancelled(r),
        Outcome::Panicked(p) => return Outcome::Panicked(p),
    };

    Outcome::Ok(rows[0].get_as::<i64>(0).unwrap_or(0))
    // Connection is automatically returned to the pool when `conn` drops
}
```

---

## Key Resilience Features

### Cancel-Correct Acquisition
If a task waiting for an available connection is cancelled (for example, due to an HTTP client disconnect or timeout), the reservation is safely revoked. No connection is wasted or orphaned.

### One Deadline for the Entire Checkout

`PoolConfig::acquire_timeout(milliseconds)` bounds waiting for capacity, creating
a connection, validating an idle connection, and retiring an unsuitable one.
The caller's `Cx` budget can shorten this deadline. Waiting or retrying a failed
validation does not start a fresh timeout.

A pending checkout returns `Error::Pool` with `PoolErrorKind::Timeout` when the
deadline expires. Cancellation returns `Outcome::Cancelled`; cancellation or a
panic reported by the validation operation is propagated without a factory
retry. A connection whose validation was interrupted is discarded, never put
back into the idle queue. Pool ownership and capacity are released even when
the acquisition future is dropped or unwinds. Only a successfully returned
lease increments `acquires`; a timeout increments `timeouts` once.

These are cooperative async deadlines: the runtime must be driven, and a
driver's `poll` must yield. They cannot preempt synchronous work that blocks the
runtime thread.

### Panic Safety & Automatic Return
`PooledConnection` implements `Drop`. If a task panics during request processing, the unwind logic returns the connection to the pool without corrupting internal pool counters.

### Health Checks & Idle Retirement
- Connections that exceed `max_lifetime` or `idle_timeout` are retired and replaced automatically.
- Optional test-on-borrow validation ensures broken server connections are discarded before being handed to application logic.

Every reused connection also passes the driver's `validate_for_pool` hook.
Disabling `test_on_checkout` skips the network health probe while retaining
local reuse safeguards. On repository `main`, PostgreSQL physically retires
an open or failed transaction left by raw SQL or an abandoned ORM `Session`,
including holder panic. The next borrower receives a fresh connection and
cannot observe or commit the abandoned work. A surviving shared handle cannot
keep the rejected backend alive. Explicit commit and rollback preserve normal
reuse of the original backend. Custom connection wrappers must forward this
hook to preserve their driver's behavior.

This check does not reset arbitrary session state such as `SET ROLE`,
`search_path`, temporary tables, or session advisory locks. Applications must
restore session settings and release such locks before returning a lease.

### Graceful Drain (`close_and_drain`)
During application shutdown, calling `pool.close_and_drain(cx)` closes all idle connections and waits for all active leases to be returned before terminating:

```rust,no_run
use sqlmodel::prelude::*;
use sqlmodel_pool::Pool;

async fn shutdown_pool(cx: &Cx, pool: Pool<impl Connection>) -> Outcome<(), Error> {
    pool.close_and_drain(cx).await
}
```

---

## PostgreSQL Runtime Handoff and the Asupersync Compatibility Port

[SQLModel issue #43](https://github.com/Dicklesworthstone/sqlmodel_rust/issues/43)
and [Asupersync issue #85](https://github.com/Dicklesworthstone/asupersync/issues/85)
describe an Asupersync 0.5.0 socket registration left on an undriven reactor
after its original current-thread runtime exits. This can stall a reused
PostgreSQL connection's checkout ping after either normal holder return or
holder panic. The acquisition deadline turns that stall into a bounded failure
and retires the unusable connection. It does not repair socket migration and
must not be reported as successful reuse.

On the published 0.5 runtime, keep pooled PostgreSQL connections on the same
driven runtime, or use the explicitly qualified
`RuntimeBuilder::new().worker_threads(2)` configuration for this handoff
scenario. Disabling checkout validation does not establish that a connection
can make progress under its new runtime.

The production-context regressions in `crates/sqlmodel-e2e/tests/pool.rs` use
`Runtime::request_cx_with_budget(Budget::INFINITE)`, real PostgreSQL queries,
and an independent watchdog. The original all-driver success contract remains
in place. The bounded-failure test checks the timeout, retirement accounting,
the old backend's disappearance from `pg_stat_activity`, and a usable fresh
replacement. Success tests require the original `pg_backend_pid()`, a working
query, no replacement connection, and a completed drain. Set
`SQLMODEL_TEST_REQUIRE_SERVICES=1` alongside `SQLMODEL_TEST_POSTGRES_URL` when
running these regressions as a service qualification gate, so an absent server
cannot be mistaken for an executed test.

The standalone qualification in
`crates/sqlmodel-e2e/runtime-handoff/README.md` also isolates the production
dependency graph from all dev-dependency features. Run
`python3 scripts/qualify_postgres_runtime_handoff.py --source workspace` to
require successful reuse in both runtime configurations, including normal
holder exit and intentional panic. Two additional TCP cases force pending
reads before and after handoff to exercise reactor migration deterministically.
Each case runs in its own process with an external deadline. A timeout,
replacement session, missing service, or absent completion marker fails
qualification; the runner retains the dependency graph, lockfile, source
provenance, and individual logs.

### Published Dependency Boundary (verified 2026-11-02 UTC)

The upstream runtime fix is now available in the registry:
[Asupersync 0.6.0](https://crates.io/crates/asupersync/0.6.0) was published at
13:41 UTC and [0.6.1](https://crates.io/crates/asupersync/0.6.1) at 20:03 UTC on
November 1. The 0.6.1 package records source revision
`4d0ef0e79a6efa36023f32fcd81615122c809d3d`; its source includes the weak
current-thread root waker and migration to the polling socket reactor. Neither
release satisfies SQLModel's existing `asupersync = "0.5.0"` requirement, and
there is still no published compatible 0.5 patch carrying the fix.

A complete SQLModel 0.6 port must also align the FrankenSQLite dependency
family. Even [fsqlite-core 0.4.13](https://crates.io/crates/fsqlite-core/0.4.13/dependencies)
and [fsqlite-types 0.4.13](https://crates.io/crates/fsqlite-types/0.4.13/dependencies)
still select Asupersync `^0.5.0`. Updating only SQLModel's workspace dependency
to 0.6.1 produces both runtime versions in the full graph, violating the
existing downstream single-Asupersync gate in `publish-set.yml` and
`release.yml` (tracked by `bd-jeof.3`). The production dependency and these
gates therefore remain unchanged; the port needs an aligned published engine
family and a repeat of the native success matrix, not a local dependency patch.

The same runner accepts `--stage-asupersync-version 0.6.1` to evaluate the
PostgreSQL subset against the published runtime in a copied workspace. It
rejects multiple runtimes, a non-registry runtime without an explicitly
requested source override, and the `test-internals` feature. A successful
staged run is evidence for that PostgreSQL integration, while the all-driver
port and qualification of a published SQLModel release remain separate gates.

### Native Qualification Results (2026-11-02 UTC)

The repository checkout protection was exercised against PostgreSQL 16.15 on
native x86_64 Linux, using the published Asupersync 0.5.0 dependency:

| Scenario | Observed result |
|---|---|
| Current-thread runtime handoff, normal holder exit and holder panic | Checkout reached its configured two-second timeout; the old backend was retired and a fresh usable connection could be acquired |
| Two-worker runtime handoff, normal holder exit and holder panic | Checkout reused the original backend PID, completed a query, and drained cleanly |
| Reuse within one current-thread runtime | Successful reuse of the original backend PID |
| Expired or server-terminated connections | Retirement and replacement succeeded with the existing accounting and drain checks |
| Dropped PostgreSQL query future | The interrupted socket was closed; checkout replaced it and returned the new query's own result |
| Abandoned transaction or transaction holder panic, with a surviving shared handle | First checkout replaced the backend; the old server session disappeared, abandoned writes were absent, and draining succeeded |
| Raw transaction `COMMIT` followed by holder panic | The committed write remained, while the poisoned connection was physically retired and replaced |
| Abandoned, panicked, or failed ORM `Session`, with checkout ping enabled or disabled | First checkout retired the dirty backend, including surviving shared handles, and returned no abandoned writes; explicit commit and rollback controls reused the original backend |

The 104 native pool tests also passed, including stalled validation,
cancellation, and future drain deadlines on both runtime configurations.
The separate strict current-thread handoff success
contract still fails on 0.5.0 with a pool timeout: bounding that failure does
not establish successful migration of the old connection.

The PostgreSQL suite passed 61 driver unit tests and 16 live integration tests,
including prepared-statement caching, transaction ownership and isolation,
pending-I/O and lock-wait cancellation, commit error preservation, and
introspection. All eight production PostgreSQL pool tests and all four
cancellation sweep tests passed.
Workspace checking, formatting, and Clippy with warnings denied also passed.
The native builds used the pinned nightly LLVM toolchain, one Cargo job,
disabled debug information, and 256 codegen units for the large runtime and
engine dependencies to fit the build environment; dependency sources were
unchanged.

The latest staged 0.6.1 evaluation used clean SQLModel revision
[`bef4100dff319d9387163c009461a15e87295a05`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/bef4100dff319d9387163c009461a15e87295a05).
It resolved exactly one registry Asupersync version, with `test-internals`
disabled and no Tokio in the native dependency tree. The runtime source files
matched the published archive. Native LLVM compilation, including the
dependency-only `-Zhint-mostly-unused` build option, ended with `SIGKILL` in
the eight-GiB build environment. None of the six strict cases executed.
Dependency resolution and source integrity are verified; SQLModel API
compatibility and successful runtime handoff remain unqualified until the
native build and complete matrix pass. The compatible 0.5 patch and full
single-runtime FrankenSQLite port requirements above remain unresolved.

---

## Read Replica Routing (`ReplicaPool`)

For read-heavy workloads, `ReplicaPool` manages routing between a primary writer pool and one or more read replica pools using round-robin or random distribution strategies:

```rust,no_run
use sqlmodel::prelude::*;
use sqlmodel_pool::{Pool, ReplicaPool};

async fn build_replica_cluster<C: Connection>(
    primary: Pool<C>,
    replicas: Vec<Pool<C>>,
) -> ReplicaPool<C> {
    ReplicaPool::new(primary, replicas)
}
```

---

## Differences from Python SQLModel

- **Structured Concurrency Integration**: Python SQLAlchemy's `QueuePool` relies on Python threading locks or asyncio queues. SQLModel Rust's pool integrates directly with `asupersync`'s structured concurrency.
- **Cancel-Safety**: Python connection pools can leak checked-out connections if an `asyncio.Task` is cancelled mid-acquisition. In SQLModel Rust, cancellation strictly restores pool queue integrity.
- **Explicit Graceful Draining**: `close_and_drain` provides a deterministic, cancel-safe shutdown sequence that cleanly waits for in-flight transactions.
