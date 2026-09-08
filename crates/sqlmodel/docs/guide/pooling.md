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

### Panic Safety & Automatic Return
`PooledConnection` implements `Drop`. If a task panics during request processing, the unwind logic returns the connection to the pool without corrupting internal pool counters.

### Health Checks & Idle Retirement
- Connections that exceed `max_lifetime` or `idle_timeout` are retired and replaced automatically.
- Optional test-on-borrow validation ensures broken server connections are discarded before being handed to application logic.

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
