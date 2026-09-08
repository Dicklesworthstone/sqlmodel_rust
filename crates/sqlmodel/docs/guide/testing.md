# Chapter 11: Testing Strategies

Testing database code is often fraught with flakiness, slow execution times, and complex environment setups. **SQLModel Rust** provides first-class testing utilities for lightning-fast in-memory unit tests, transactional isolation, deterministic concurrency testing, and exhaustive cancellation sweeps.

---

## In-Memory SQLite Unit Tests

For fast, self-contained unit tests that require no external database servers, use in-memory SQLite:

```rust,no_run
use sqlmodel::prelude::*;
use sqlmodel::create_table;

#[derive(Model, Debug, Clone, PartialEq)]
#[sqlmodel(table = "users")]
pub struct User {
    #[sqlmodel(primary_key, auto_increment)]
    pub id: Option<i64>,
    pub name: String,
}

async fn test_user_creation(cx: &Cx, conn: &impl Connection) -> Outcome<(), Error> {
    // Create table schema
    let sql = create_table::<User>().dialect(conn.dialect()).build();
    match conn.execute(cx, &sql, &[]).await {
        Outcome::Ok(_) => {}
        Outcome::Err(e) => return Outcome::Err(e),
        Outcome::Cancelled(r) => return Outcome::Cancelled(r),
        Outcome::Panicked(p) => return Outcome::Panicked(p),
    }

    // Insert user
    let user = User { id: None, name: "Alice".into() };
    let id = match insert!(&user).execute(cx, conn).await {
        Outcome::Ok(i) => i,
        Outcome::Err(e) => return Outcome::Err(e),
        Outcome::Cancelled(r) => return Outcome::Cancelled(r),
        Outcome::Panicked(p) => return Outcome::Panicked(p),
    };
    assert_eq!(id, 1);

    // Query user
    let found: Option<User> = match select!(User)
        .filter(Expr::col("id").eq(id))
        .first(cx, conn)
        .await
    {
        Outcome::Ok(u) => u,
        Outcome::Err(e) => return Outcome::Err(e),
        Outcome::Cancelled(r) => return Outcome::Cancelled(r),
        Outcome::Panicked(p) => return Outcome::Panicked(p),
    };

    assert_eq!(found.map(|u| u.name), Some("Alice".into()));
    Outcome::Ok(())
}
```

---

## Transactional Test Isolation

When running integration tests against a shared staging PostgreSQL or MySQL database, you can isolate tests by wrapping each test body in a transaction and rolling it back upon completion:

```rust,no_run
use sqlmodel::prelude::*;

async fn run_isolated_test<'c, C, F, Fut>(cx: &Cx, conn: &'c C, test_fn: F) -> Outcome<(), Error>
where
    C: Connection,
    F: FnOnce(&C::Tx<'c>) -> Fut,
    Fut: std::future::Future<Output = Outcome<(), Error>>,
{
    let tx = match conn.begin(cx).await {
        Outcome::Ok(t) => t,
        Outcome::Err(e) => return Outcome::Err(e),
        Outcome::Cancelled(r) => return Outcome::Cancelled(r),
        Outcome::Panicked(p) => return Outcome::Panicked(p),
    };
    let result = test_fn(&tx).await;
    // Discard all writes by rolling back the transaction
    let _ = tx.rollback(cx).await;
    result
}
```

---

## Deterministic Concurrency Testing (`LabRuntime`)

Testing timing, race conditions, and connection pool timeouts with `thread::sleep` creates slow, flaky CI suites.

SQLModel Rust is tested using `asupersync::lab::LabRuntime`:
- **Virtual Time**: Time only advances when tasks are waiting for timers; multi-hour timeouts can be tested in microseconds.
- **Deterministic Scheduling**: Seeded task schedulers reproduce race conditions reliably.
- **Oracles & Invariants**: Concurrency properties can be formally verified on every run without flakiness.

---

## Fault Injection: Cancellation Sweeps (`CancelAt`)

To prove that multi-step database workflows are cancel-safe, SQLModel provides the `CancelAt` test wrapper (available under the `test-support` feature on `sqlmodel-core`):

```rust,ignore
use sqlmodel::prelude::*;
use sqlmodel_core::test_support::CancelAt;

async fn run_sweep<C: Connection>(cx: &Cx, base_conn: C) {
    // 1. First pass: count total database checkpoints in the operation
    let probe = CancelAt::new(base_conn, 0);
    // run operation to discover k_max ...

    // 2. Iterate through every checkpoint k = 1..=k_max
    // proving that cancelling at the k-th step leaves zero corrupted state
    // and returns Outcome::Cancelled.
}
```

This guarantees that an abrupt cancellation mid-workflow never leaves half-written rows, uncommitted locks, or broken state invariants.

---

## Differences from Python SQLModel

- **Virtual-Time Concurrency**: Python testing relies on `unittest.mock` or real sleeps, which either miss concurrency bugs or slow down CI. `asupersync::lab` enables instantaneous virtual-time tests.
- **Exhaustive Cancellation Proofs**: Python offers no native mechanism to prove cancel-safety across every checkpoint; SQLModel Rust's `CancelAt` sweep provides mathematical verification.
- **Zero-Setup In-Memory Speed**: Both pure-Rust FrankenSQLite and C-SQLite run thousands of in-memory ORM test cases per second with zero external dependencies.
