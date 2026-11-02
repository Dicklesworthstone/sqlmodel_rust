//! Connection pooling for SQLModel Rust using asupersync.
//!
//! `sqlmodel-pool` is the **connection lifecycle layer**. It provides a generic,
//! budget-aware pool that integrates with structured concurrency and can wrap any
//! `Connection` implementation.
//!
//! # Role In The Architecture
//!
//! - **Shared connection management**: reuse connections across tasks safely.
//! - **Budget-aware acquisition**: respects `Cx` timeouts and cancellation.
//! - **Health checks**: validates connections before handing them out.
//! - **Metrics**: exposes stats for pool sizing and tuning.
//!
//! # Health-check rule
//!
//! Every idle candidate passes [`Connection::validate_for_pool`], allowing a
//! driver to reject state that cannot safely cross borrower boundaries.
//! With `test_on_checkout` (the default) every idle connection is pinged before
//! it is handed out; one that fails is closed and replaced transparently, so a
//! server-side kill of an idle connection costs one reconnect when the acquire
//! budget permits. Waiting, connecting, validation, and retirement share the
//! earlier of `acquire_timeout` and the caller's budget deadline. A stalled
//! ping or close cannot extend it. An interrupted validation discards its
//! connection before releasing the pool slot; it is never returned idle.
//! Disabling the ping retains driver-specific local safeguards. PostgreSQL
//! rejects an unfinished transaction instead of exposing its uncommitted work
//! to another borrower. This does not reset arbitrary session configuration.
//! Returning a connection runs no check (a return is a synchronous `Drop`), so
//! a lease whose statement failed with a connection error should be
//! [`PooledConnection::detach`]ed rather than dropped back into the pool;
//! without a health probe, a driver may still hand an undetected dead socket
//! out and fail its next statement. The e2e pool scenario asserts server-side
//! termination with both probe settings against PostgreSQL and MySQL.
//!
//! A lease holder that panics returns its connection during unwinding (the
//! lease's `Drop` runs); the pool's accounting stays consistent, it keeps
//! serving, and `close_and_drain` still completes. Only a panic *inside* the
//! pool's own lock poisons it, and every lock site recovers from that.
//!
//! # Features
//!
//! - Generic over any `Connection` type
//! - RAII-based connection return (connections returned on drop)
//! - Timeout support via `Cx` context
//! - Connection health validation
//! - Idle and max lifetime tracking
//! - Pool statistics
//!
//! # Example
//!
//! ```rust,ignore
//! use sqlmodel_pool::{Pool, PoolConfig};
//!
//! // Create a pool
//! let config = PoolConfig::new(10)
//!     .min_connections(2)
//!     .acquire_timeout(5000);
//!
//! let pool: Pool<PgConnection> = Pool::new(config);
//!
//! // Acquire a connection; the factory opens a new one when the pool has no
//! // idle connection and is below its maximum.
//! let conn = pool
//!     .acquire(&cx, || async { PgConnection::connect(&cx, pg_config.clone()).await })
//!     .await?;
//!
//! // Use the connection (automatically returned to pool on drop)
//! conn.query(&cx, "SELECT 1", &[]).await?;
//! ```

pub mod replica;
pub use replica::{ReplicaPool, ReplicaStrategy};

pub mod sharding;
pub use sharding::{ModuloShardChooser, QueryHints, ShardChooser, ShardedPool, ShardedPoolStats};

use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use asupersync::{
    Budget, CancelReason, Cx, Outcome, Time,
    combinator::{Either, Select},
    runtime::RuntimeBuilder,
    sync::{Notify, OnceCell},
    time::TimerDriverHandle,
};
use sqlmodel_core::error::{PoolError, PoolErrorKind};
use sqlmodel_core::{Connection, Error};

/// Connection pool configuration.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Minimum number of connections to maintain
    pub min_connections: usize,
    /// Maximum number of connections allowed
    pub max_connections: usize,
    /// Connection idle timeout in milliseconds
    pub idle_timeout_ms: u64,
    /// Maximum acquisition time in milliseconds, including connection creation,
    /// checkout validation, and retirement of unusable connections. Also bounds
    /// the graceful retirement work of each synchronous close or lease return.
    pub acquire_timeout_ms: u64,
    /// Maximum lifetime of a connection in milliseconds
    pub max_lifetime_ms: u64,
    /// Ping connections before giving them out; a failed ping closes the
    /// connection and the acquire moves on to another (or a new) one.
    pub test_on_checkout: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 10,
            idle_timeout_ms: 600_000,   // 10 minutes
            acquire_timeout_ms: 30_000, // 30 seconds
            max_lifetime_ms: 1_800_000, // 30 minutes
            test_on_checkout: true,
        }
    }
}

impl PoolConfig {
    /// Create a new pool configuration with the given max connections.
    #[must_use]
    pub fn new(max_connections: usize) -> Self {
        Self {
            max_connections,
            ..Default::default()
        }
    }

    /// Set minimum connections.
    #[must_use]
    pub fn min_connections(mut self, n: usize) -> Self {
        self.min_connections = n;
        self
    }

    /// Set idle timeout in milliseconds.
    #[must_use]
    pub fn idle_timeout(mut self, ms: u64) -> Self {
        self.idle_timeout_ms = ms;
        self
    }

    /// Set the acquisition timeout and synchronous retirement grace in milliseconds.
    #[must_use]
    pub fn acquire_timeout(mut self, ms: u64) -> Self {
        self.acquire_timeout_ms = ms;
        self
    }

    /// Set max lifetime in milliseconds.
    #[must_use]
    pub fn max_lifetime(mut self, ms: u64) -> Self {
        self.max_lifetime_ms = ms;
        self
    }

    /// Enable/disable the ping before a connection is handed out (see the
    /// crate-level "Health-check rule"). Driver-specific local reuse checks
    /// still run when disabled. There is no test on return: a return is a
    /// synchronous `Drop`; detach a lease you know to be dead instead.
    #[must_use]
    pub fn test_on_checkout(mut self, enabled: bool) -> Self {
        self.test_on_checkout = enabled;
        self
    }
}

/// Pool statistics.
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Total number of connections (active + idle)
    pub total_connections: usize,
    /// Number of idle connections
    pub idle_connections: usize,
    /// Number of active connections (currently in use)
    pub active_connections: usize,
    /// Number of pending acquire requests
    pub pending_requests: usize,
    /// Total number of connections created
    pub connections_created: u64,
    /// Total number of connections closed
    pub connections_closed: u64,
    /// Total number of successful acquires
    pub acquires: u64,
    /// Total number of acquire timeouts
    pub timeouts: u64,
}

/// Metadata about a pooled connection.
///
/// Timestamps use asupersync's `Time` read through a [`TimerDriverHandle`]
/// instead of `std::time::Instant`, so every pool timing decision (idle
/// timeout, max lifetime, acquire deadline) observes the same clock as the
/// runtime: the wall clock in production and a [`asupersync::time::VirtualClock`]
/// under the lab runtime used by deterministic tests.
struct ConnectionMeta<C> {
    /// The actual connection
    conn: C,
    /// When this connection was created
    created_at: Time,
    /// When this connection was last used
    last_used: Time,
    /// Clock shared with the owning pool
    clock: TimerDriverHandle,
}

impl<C> ConnectionMeta<C> {
    fn new(conn: C, clock: TimerDriverHandle) -> Self {
        let now = clock.now();
        Self {
            conn,
            created_at: now,
            last_used: now,
            clock,
        }
    }

    fn touch(&mut self) {
        self.last_used = self.clock.now();
    }

    fn age(&self) -> Duration {
        Duration::from_nanos(self.clock.now().duration_since(self.created_at))
    }

    fn idle_time(&self) -> Duration {
        Duration::from_nanos(self.clock.now().duration_since(self.last_used))
    }
}

/// Internal pool state shared between pool and connections.
struct PoolInner<C: Connection> {
    /// Pool configuration
    config: PoolConfig,
    /// Idle connections available for use
    idle: VecDeque<ConnectionMeta<C>>,
    /// Number of connections currently checked out
    active_count: usize,
    /// Total number of connections (idle + active)
    total_count: usize,
    /// Number of waiters in the queue
    waiter_count: usize,
    /// Whether the pool has been closed
    closed: bool,
}

impl<C: Connection> PoolInner<C> {
    fn new(config: PoolConfig) -> Self {
        Self {
            config,
            idle: VecDeque::new(),
            active_count: 0,
            total_count: 0,
            waiter_count: 0,
            closed: false,
        }
    }

    fn can_create_new(&self) -> bool {
        !self.closed && self.total_count < self.config.max_connections
    }

    fn stats(&self) -> PoolStats {
        PoolStats {
            total_connections: self.total_count,
            idle_connections: self.idle.len(),
            active_connections: self.active_count,
            pending_requests: self.waiter_count,
            ..Default::default()
        }
    }
}

/// Shared state wrapper with condition variable for notification.
struct PoolShared<C: Connection> {
    /// Protected pool state
    inner: Mutex<PoolInner<C>>,
    /// Notifies waiters when connections become available
    /// Woken on every return and on close. Async on purpose: a waiter must
    /// not block the runtime thread, or the tasks that would release a
    /// lease never run (a `Condvar` here deadlocked single-threaded runtimes
    /// until every waiter timed out; found by the e2e fan-out on PostgreSQL).
    conn_available: Notify,
    /// One-shot latch initialized when the irreversible pool drain completes.
    active_drained: OnceCell<()>,
    /// Stable first teardown failure, retained so every current or later
    /// drainer observes the same fail-closed lifecycle state.
    retirement_failure: Mutex<Option<Arc<str>>>,
    /// Statistics counters (atomic for lock-free reads)
    connections_created: AtomicU64,
    connections_closed: AtomicU64,
    acquires: AtomicU64,
    timeouts: AtomicU64,
    /// Clock every pool timestamp and deadline is read from. Production pools
    /// share asupersync's wall-clock timer driver; tests install a virtual
    /// clock so timing behavior is deterministic.
    clock: TimerDriverHandle,
}

impl<C: Connection> PoolShared<C> {
    fn new(config: PoolConfig, clock: TimerDriverHandle) -> Self {
        Self {
            inner: Mutex::new(PoolInner::new(config)),
            conn_available: Notify::new(),
            active_drained: OnceCell::new(),
            retirement_failure: Mutex::new(None),
            connections_created: AtomicU64::new(0),
            connections_closed: AtomicU64::new(0),
            acquires: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            clock,
        }
    }

    /// Lock the inner mutex, recovering from poisoning for read-only access.
    ///
    /// A poisoned mutex occurs when a thread panicked while holding the lock.
    /// The data inside is still valid for reading, so we recover by logging
    /// and using `into_inner()` to get the guard.
    ///
    /// This should only be used for read-only operations where the data is
    /// always valid regardless of whether a previous operation completed.
    fn lock_or_recover(&self) -> std::sync::MutexGuard<'_, PoolInner<C>> {
        self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::error!(
                "Pool mutex poisoned; recovering for read-only access. \
                 A thread panicked while holding the lock."
            );
            poisoned.into_inner()
        })
    }

    /// Lock the inner mutex, returning an error if poisoned.
    ///
    /// Use this for mutation operations where the pool state may be inconsistent
    /// after a panic. Unlike `lock_or_recover()`, this propagates the error
    /// to the caller.
    #[allow(clippy::result_large_err)] // Error type is large by design for rich diagnostics
    fn lock_or_error(
        &self,
        operation: &'static str,
    ) -> Result<std::sync::MutexGuard<'_, PoolInner<C>>, Error> {
        self.inner
            .lock()
            .map_err(|_| Error::Pool(PoolError::poisoned(operation)))
    }

    /// Release one active slot that is leaving the pool permanently.
    ///
    /// The caller must not invoke this until any required driver close has
    /// completed or its owned close future and resource have been destroyed.
    /// Any retirement failure must be recorded before releasing the slot.
    /// Keeping the slot active through disposal is what makes
    /// `Pool::close_and_drain` a resource-quiescence boundary instead of only
    /// a bookkeeping boundary.
    fn release_active_slot(&self, operation: &'static str) {
        let mut accounting_underflow = false;
        let (drained, notify_open_waiter) = match self.inner.lock() {
            Ok(mut inner) => {
                if inner.active_count == 0 || inner.total_count == 0 {
                    tracing::error!(
                        operation,
                        active_count = inner.active_count,
                        total_count = inner.total_count,
                        "attempted to release an unaccounted pool slot"
                    );
                    accounting_underflow = true;
                }
                inner.active_count = inner.active_count.saturating_sub(1);
                inner.total_count = inner.total_count.saturating_sub(1);
                if accounting_underflow && inner.closed && inner.active_count == 0 {
                    inner.total_count = 0;
                }
                (inner.closed && inner.active_count == 0, !inner.closed)
            }
            Err(poisoned) => {
                tracing::error!(
                    operation,
                    "Pool mutex poisoned while releasing an active slot; \
                     recovering to prevent stranded drain accounting"
                );
                let error = Error::Pool(PoolError::poisoned(operation));
                self.record_retirement_failure(operation, &error);
                let mut inner = poisoned.into_inner();
                if inner.active_count == 0 || inner.total_count == 0 {
                    accounting_underflow = true;
                }
                inner.active_count = inner.active_count.saturating_sub(1);
                inner.total_count = inner.total_count.saturating_sub(1);
                if accounting_underflow && inner.closed && inner.active_count == 0 {
                    inner.total_count = 0;
                }
                (inner.closed && inner.active_count == 0, !inner.closed)
            }
        };

        if accounting_underflow {
            let error = Error::Custom(format!(
                "pool accounting underflow while releasing active slot during {operation}"
            ));
            self.record_retirement_failure(operation, &error);
        }
        if notify_open_waiter {
            self.conn_available.notify_one();
        }
        if drained {
            // Pool closure is irreversible, so a one-shot latch exactly models
            // the single transition to fully drained and wakes every waiter.
            let _ = self.active_drained.set(());
        }
    }

    fn record_retirement_failure(&self, context: &'static str, error: &Error) {
        let message: Arc<str> = format!("{context}: {error}").into();
        let mut failure = self.retirement_failure.lock().unwrap_or_else(|poisoned| {
            tracing::error!(
                context,
                "Pool retirement-failure mutex poisoned; recovering"
            );
            poisoned.into_inner()
        });
        if failure.is_none() {
            *failure = Some(message);
        }
    }

    fn retirement_failure_error(&self) -> Option<Error> {
        let failure = self
            .retirement_failure
            .lock()
            .unwrap_or_else(|poisoned| {
                tracing::error!("Pool retirement-failure mutex poisoned; recovering");
                poisoned.into_inner()
            })
            .clone();
        failure.map(|message| Error::Custom(format!("pool retirement failed: {message}")))
    }
}

fn retirement_timeout() -> Error {
    Error::Custom("synchronous pool retirement grace period expired".to_string())
}

/// Enforce one absolute grace independently of driver cooperation. Failure is
/// published while the owned operation still exists: dropping it may release
/// the final accounting slot and synchronously wake a concurrent drainer.
#[allow(clippy::result_large_err)] // Preserve the driver's existing rich Error type.
async fn supervise_retirement<F>(
    cx: &Cx,
    deadline: Time,
    operation: F,
    record_failure: impl Fn(&Error),
) -> Result<(), Error>
where
    F: Future<Output = Result<(), Error>>,
{
    let mut operation = Box::pin(operation);
    let mut timer = std::pin::pin!(asupersync::time::sleep_until(deadline));
    let result = std::future::poll_fn(|task_cx| {
        let outcome = if cx.now() >= deadline {
            std::task::Poll::Ready(Err(retirement_timeout()))
        } else if timer.as_mut().poll(task_cx).is_ready() {
            let error = if cx.now() >= deadline {
                retirement_timeout()
            } else {
                // Sleep can also finish because a distinct ambient context
                // was cancelled. Never repoll its completed future.
                Error::Custom("synchronous pool retirement interrupted".to_string())
            };
            std::task::Poll::Ready(Err(error))
        } else {
            let outcome = operation.as_mut().poll(task_cx);
            if outcome.is_pending() && cx.now() >= deadline {
                std::task::Poll::Ready(Err(retirement_timeout()))
            } else {
                // A completed guarded close already classified its deadline
                // before releasing the slot. Do not invent a later failure
                // after a successful final release has woken drainers.
                outcome
            }
        };
        if let std::task::Poll::Ready(Err(error)) = &outcome {
            record_failure(error);
        }
        outcome
    })
    .await;
    drop(operation);
    result
}

#[allow(clippy::result_large_err)] // Error is the crate's rich public error type.
fn run_retirement_blocking<F, Fut>(
    deadline: Time,
    context: &'static str,
    operation: F,
    record_failure: impl Fn(&Error),
) -> Result<(), Error>
where
    F: FnOnce(Cx) -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    let unavailable = if std::thread::panicking() {
        // A user is already unwinding. Invoking another driver hook here could
        // panic a second time and abort instead of releasing owned resources.
        Some(Error::Custom(
            "synchronous pool retirement interrupted by unwinding".to_string(),
        ))
    } else if asupersync::time::wall_now() >= deadline {
        Some(retirement_timeout())
    } else {
        None
    };
    if let Some(error) = unavailable {
        record_failure(&error);
        drop(operation);
        return Err(error);
    }
    let runtime = match RuntimeBuilder::current_thread().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            let error = Error::Custom(format!(
                "failed to build runtime while retiring pooled connection: {error}"
            ));
            record_failure(&error);
            drop(operation);
            return Err(error);
        }
    };
    // The finite context lets cooperative drivers stop themselves. A request
    // context's budget alone cannot wake a permanently pending driver, so the
    // supervisor also registers this runtime's independent deadline timer.
    let cx = runtime.request_cx_with_budget(Budget::INFINITE.with_deadline(deadline));
    let result = runtime.block_on(supervise_retirement(
        &cx,
        deadline,
        operation(cx.clone()),
        record_failure,
    ));
    if let Err(error) = &result {
        tracing::warn!(context, error = %error, "synchronous pool retirement failed");
    }
    result
}

#[allow(clippy::result_large_err)] // Error is the crate's rich public error type.
fn close_connection_blocking<C: Connection>(
    conn: C,
    grace: Duration,
    context: &'static str,
) -> Result<(), Error> {
    let deadline = asupersync::time::wall_now() + grace;
    run_retirement_blocking(
        deadline,
        context,
        move |cx| async move {
            let result = conn.close_for_pool(&cx).await;
            if cx.now() >= deadline {
                Err(retirement_timeout())
            } else {
                result
            }
        },
        |error| {
            tracing::warn!(context, error = %error, "unowned-pool connection retirement failed");
        },
    )
}

/// Owns an active/total pool slot while an asynchronous connection factory is
/// in flight.
///
/// Dropping an `acquire` future must not strand its reserved slot forever:
/// `close_and_drain` may already be waiting for that slot to retire.
struct ActiveSlotGuard<C: Connection> {
    pool: Arc<PoolShared<C>>,
    armed: bool,
}

impl<C: Connection> ActiveSlotGuard<C> {
    fn new(pool: Arc<PoolShared<C>>) -> Self {
        Self { pool, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    fn release(&mut self, operation: &'static str) {
        if self.armed {
            self.armed = false;
            self.pool.release_active_slot(operation);
        }
    }
}

impl<C: Connection> Drop for ActiveSlotGuard<C> {
    fn drop(&mut self) {
        self.release("connection factory future drop");
    }
}

/// Owns the pending-request count across the wait's cancellation points.
struct WaiterGuard<C: Connection> {
    pool: Arc<PoolShared<C>>,
}

impl<C: Connection> Drop for WaiterGuard<C> {
    fn drop(&mut self) {
        let mut inner = self.pool.lock_or_recover();
        inner.waiter_count = inner.waiter_count.saturating_sub(1);
    }
}

#[derive(Clone, Copy)]
struct AcquireDeadline {
    at: Time,
    budget_limited: bool,
}

/// Owns an active connection while checkout validation is in flight.
///
/// On hard cancellation the guard drops the connection resource before it
/// releases the active slot, preserving the drain boundary without requiring
/// asynchronous work from `Drop`.
struct ActiveConnectionGuard<C: Connection> {
    pool: Arc<PoolShared<C>>,
    meta: Option<ConnectionMeta<C>>,
    armed: bool,
    /// Synchronous retirement cannot return cancellation to its caller. Any
    /// unfinished close, including an unwind, must become a sticky failure
    /// before this guard can publish the final drain notification.
    synchronous_context: Option<&'static str>,
}

enum RetirementOutcome {
    Closed,
    Cancelled(CancelReason),
    Failed(Error),
}

impl<C: Connection> ActiveConnectionGuard<C> {
    fn new(pool: Arc<PoolShared<C>>, meta: ConnectionMeta<C>) -> Self {
        Self {
            pool,
            meta: Some(meta),
            armed: true,
            synchronous_context: None,
        }
    }

    fn connection(&self) -> &C {
        &self
            .meta
            .as_ref()
            .expect("active connection guard already consumed")
            .conn
    }

    fn into_meta(mut self) -> ConnectionMeta<C> {
        self.armed = false;
        self.meta
            .take()
            .expect("active connection guard already consumed")
    }

    fn release(&mut self, operation: &'static str) {
        if self.armed {
            self.armed = false;
            self.pool.connections_closed.fetch_add(1, Ordering::Relaxed);
            self.pool.release_active_slot(operation);
        }
    }

    #[allow(clippy::result_large_err)] // Preserve the driver's existing rich Error type.
    async fn close_synchronously(
        mut self,
        cx: &Cx,
        deadline: Time,
        context: &'static str,
    ) -> Result<(), Error> {
        self.synchronous_context = Some(context);
        let meta = self
            .meta
            .take()
            .expect("active connection guard already consumed");
        // Keep the consuming future younger than the accounting owner. Its
        // resource must be destroyed first on completion, timeout, or panic.
        let mut close = Box::pin(meta.conn.close_for_pool(cx));
        let result = close.as_mut().await;
        drop(close);
        let result = if cx.now() >= deadline {
            Err(retirement_timeout())
        } else {
            result
        };
        if let Err(error) = &result {
            self.pool.record_retirement_failure(context, error);
        }
        // Unlike the async close path, do not let a cancellation latch release
        // this owner before the synchronous supervisor classifies its failure.
        self.release(context);
        result
    }

    async fn close(mut self, cx: &Cx, context: &'static str) -> RetirementOutcome {
        // `close_for_pool` consumes the connection. Keep `self` (the armed
        // accounting guard) alive first, then declare the consuming future.
        // Rust drops locals in reverse declaration order, so hard-dropping this
        // async function drops the later close future and its owned connection
        // before `self` releases the active/total slot.
        let meta = self
            .meta
            .take()
            .expect("active connection guard already consumed");
        let close_future = Box::pin(meta.conn.close_for_pool(cx));
        let cancellation_latch = OnceCell::<()>::new();
        let cancellation_future = Box::pin(cancellation_latch.wait(cx));

        // The close hook receives `cx`, but a driver may fail to observe it.
        // Race it against a cancel-aware one-shot wait so cancellation always
        // drops the owned close future and resource. This is an intentional
        // loser drop: there is no task or obligation to drain after the
        // connection-owning future itself has been destroyed.
        let selected = Select::new(close_future, cancellation_future).await;
        let outcome = match selected {
            Ok(Either::Left(result)) => match result {
                Ok(()) => RetirementOutcome::Closed,
                Err(error) => {
                    tracing::warn!(
                        context,
                        error = %error,
                        "failed to close pooled connection explicitly"
                    );
                    self.pool.record_retirement_failure(context, &error);
                    RetirementOutcome::Failed(error)
                }
            },
            Ok(Either::Right(_)) => RetirementOutcome::Cancelled(
                cx.cancel_reason()
                    .unwrap_or_else(|| CancelReason::user("pool retirement cancelled")),
            ),
            Err(error) => {
                tracing::error!(
                    context,
                    error = %error,
                    "fresh pool retirement select completed inconsistently"
                );
                let error = Error::Custom(format!(
                    "pool retirement select completed inconsistently: {error}"
                ));
                self.pool.record_retirement_failure(context, &error);
                RetirementOutcome::Failed(error)
            }
        };
        // Record any failure above before releasing the potentially-final slot:
        // the latch publication is the synchronization point for all drainers.
        self.release(context);
        outcome
    }
}

impl<C: Connection> Drop for ActiveConnectionGuard<C> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(context) = self.synchronous_context {
            let error = Error::Custom(
                "synchronous pool retirement interrupted before close completed".to_string(),
            );
            self.pool.record_retirement_failure(context, &error);
        }
        // Async Drop is impossible. Dropping the resource itself is the
        // hard-cancellation fallback; do that before releasing its pool slot so
        // a concurrent drainer cannot observe quiescence too early.
        drop(self.meta.take());
        self.release(
            self.synchronous_context
                .unwrap_or("checkout validation future drop"),
        );
    }
}

/// A connection pool for database connections.
///
/// The pool manages a collection of connections, reusing them across
/// requests to avoid the overhead of establishing new connections.
///
/// # Type Parameters
///
/// - `C`: The connection type, must implement `Connection`
///
/// # Cancellation
///
/// Pool operations respect cancellation via the `Cx` context:
/// - `acquire` will return early if cancellation is requested
/// - Connections are properly cleaned up on cancellation
pub struct Pool<C: Connection> {
    shared: Arc<PoolShared<C>>,
}

impl<C: Connection> Pool<C> {
    /// Create a new connection pool with the given configuration.
    ///
    /// Timestamps and deadlines are read from asupersync's wall-clock timer
    /// driver. Use [`Pool::with_timer_driver`] to supply a different clock
    /// (for example a virtual clock under the lab runtime in tests).
    #[must_use]
    pub fn new(config: PoolConfig) -> Self {
        Self::with_timer_driver(config, TimerDriverHandle::with_wall_clock())
    }

    /// Create a new connection pool whose clock is the given timer driver.
    ///
    /// Every pool timestamp (connection age, idle time) and every deadline
    /// (acquire timeout) reads `clock.now()`, so sharing this handle with the
    /// runtime's timer driver keeps the pool consistent with `cx.now()` —
    /// including a lab runtime's virtual clock.
    #[must_use]
    pub fn with_timer_driver(config: PoolConfig, clock: TimerDriverHandle) -> Self {
        Self {
            shared: Arc::new(PoolShared::new(config, clock)),
        }
    }

    /// Get the pool configuration.
    #[must_use]
    pub fn config(&self) -> PoolConfig {
        let inner = self.shared.lock_or_recover();
        inner.config.clone()
    }

    /// Get the current pool statistics.
    #[must_use]
    pub fn stats(&self) -> PoolStats {
        let inner = self.shared.lock_or_recover();
        let mut stats = inner.stats();
        stats.connections_created = self.shared.connections_created.load(Ordering::Relaxed);
        stats.connections_closed = self.shared.connections_closed.load(Ordering::Relaxed);
        stats.acquires = self.shared.acquires.load(Ordering::Relaxed);
        stats.timeouts = self.shared.timeouts.load(Ordering::Relaxed);
        stats
    }

    /// Check if the pool is at capacity.
    #[must_use]
    pub fn at_capacity(&self) -> bool {
        let inner = self.shared.lock_or_recover();
        inner.total_count >= inner.config.max_connections
    }

    /// Check if the pool has been closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        let inner = self.shared.lock_or_recover();
        inner.closed
    }

    /// Acquire a connection from the pool.
    ///
    /// Idle connections are validated before reuse; a new connection is
    /// created when capacity permits, otherwise the caller waits for a return.
    /// One deadline covers waiting, the factory, checkout validation, and
    /// retiring rejected connections. The earlier of `acquire_timeout_ms`
    /// and `cx.budget().deadline` wins.
    ///
    /// On timeout or cancellation, in-flight futures are dropped. A connection
    /// interrupted during validation is discarded before its slot is released;
    /// it is never returned to idle inventory.
    /// As with other cooperative async deadlines, a driver must yield from its
    /// `poll` implementation; synchronous blocking work cannot be preempted.
    ///
    /// # Errors
    ///
    /// Returns an error if the pool is closed, acquisition times out, or
    /// connection creation or retirement fails. Cancellation and panic outcomes
    /// from validation are propagated without retrying. An ordinary failed
    /// ping closes that connection and retries while acquisition time remains.
    pub async fn acquire<F, Fut>(&self, cx: &Cx, factory: F) -> Outcome<PooledConnection<C>, Error>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Outcome<C, Error>>,
    {
        let config = self.config();
        let mut deadline = AcquireDeadline {
            at: self.shared.clock.now() + Duration::from_millis(config.acquire_timeout_ms),
            budget_limited: false,
        };
        if let Some(budget_deadline) = cx.budget().deadline
            && budget_deadline < deadline.at
        {
            deadline.at = budget_deadline;
            deadline.budget_limited = true;
        }

        // Register wake sources independently of driver cooperation. A ping
        // parked on a stale reactor may never wake itself; this runtime's timer
        // and the caller's cancellation waker still wake the acquisition.
        // Ordered polling makes interruption win over a simultaneously ready
        // connection. Each publication site also checks after its final await.
        let cancellation_latch = OnceCell::<()>::new();
        let mut cancellation = std::pin::pin!(cancellation_latch.wait(cx));
        let mut timeout = std::pin::pin!(asupersync::time::sleep_until(deadline.at));
        let mut acquisition = std::pin::pin!(self.acquire_until(cx, factory, config, deadline));
        std::future::poll_fn(|task_cx| {
            if let Some(outcome) = self.acquire_interruption(cx, deadline) {
                return std::task::Poll::Ready(outcome);
            }
            let cancelled = cancellation.as_mut().poll(task_cx).is_ready();
            let timer_ready = timeout.as_mut().poll(task_cx).is_ready();
            if let Some(outcome) = self.acquire_interruption(cx, deadline) {
                return std::task::Poll::Ready(outcome);
            }
            if cancelled || timer_ready {
                // Sleep can finish early when the ambient runtime context is
                // cancelled, even if the supplied request context is distinct.
                // Never repoll a completed timer or misclassify that wake as
                // a timeout. OnceCell also observes non-time budget exhaustion.
                let reason = cx
                    .cancel_reason()
                    .or_else(|| Cx::current().and_then(|ambient| ambient.cancel_reason()))
                    .unwrap_or_else(|| CancelReason::user("pool acquire interrupted"));
                return std::task::Poll::Ready(Outcome::Cancelled(reason));
            }
            let outcome = acquisition.as_mut().poll(task_cx);
            if outcome.is_pending()
                && let Some(interruption) = self.acquire_interruption(cx, deadline)
            {
                return std::task::Poll::Ready(interruption);
            }
            outcome
        })
        .await
        // The owned acquisition future drops before this outcome reaches the
        // caller. Its guards discard I/O before releasing slots and unregister
        // queued waiters, including when the outer future is itself dropped.
    }

    fn acquire_interruption<T>(
        &self,
        cx: &Cx,
        deadline: AcquireDeadline,
    ) -> Option<Outcome<T, Error>> {
        // Deadline wins at the boundary, including when a runtime budget
        // timer has already requested cancellation for the same deadline.
        if self.shared.clock.now() >= deadline.at {
            self.shared.timeouts.fetch_add(1, Ordering::Relaxed);
            let message = if deadline.budget_limited {
                "acquire timeout: budget deadline reached before a connection was available"
            } else {
                "acquire timeout: no connections available"
            };
            return Some(Outcome::Err(Error::Pool(PoolError {
                kind: PoolErrorKind::Timeout,
                message: message.to_string(),
                source: None,
            })));
        }
        if cx.is_cancel_requested() {
            return Some(Outcome::Cancelled(
                cx.cancel_reason()
                    .unwrap_or_else(|| CancelReason::user("pool acquire cancelled")),
            ));
        }
        None
    }

    async fn acquire_until<F, Fut>(
        &self,
        cx: &Cx,
        factory: F,
        config: PoolConfig,
        deadline: AcquireDeadline,
    ) -> Outcome<PooledConnection<C>, Error>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Outcome<C, Error>>,
    {
        let clock = self.shared.clock.clone();
        let max_lifetime = Duration::from_millis(config.max_lifetime_ms);
        let idle_timeout = Duration::from_millis(config.idle_timeout_ms);

        loop {
            if let Some(outcome) = self.acquire_interruption(cx, deadline) {
                return outcome;
            }

            let (action, retired) = {
                let mut inner = match self.shared.lock_or_error("acquire") {
                    Ok(guard) => guard,
                    Err(error) => return Outcome::Err(error),
                };
                // Every removed entry immediately has an armed guard, so hard
                // cancellation cannot strand later entries during retirement.
                let mut retired = Vec::with_capacity(inner.idle.len());
                let action = if inner.closed {
                    AcquireAction::PoolClosed
                } else {
                    let mut found_conn = None;
                    while let Some(mut meta) = inner.idle.pop_front() {
                        if meta.age() > max_lifetime || meta.idle_time() > idle_timeout {
                            inner.active_count += 1;
                            retired
                                .push(ActiveConnectionGuard::new(Arc::clone(&self.shared), meta));
                            continue;
                        }
                        if !retired.is_empty() {
                            // Finish retirement before reserving another slot.
                            inner.idle.push_front(meta);
                            break;
                        }
                        meta.touch();
                        inner.active_count += 1;
                        found_conn = Some(meta);
                        break;
                    }

                    if !retired.is_empty() {
                        AcquireAction::RetireAndRetry
                    } else if let Some(meta) = found_conn {
                        AcquireAction::ValidateExisting(meta)
                    } else if inner.can_create_new() {
                        inner.total_count += 1;
                        inner.active_count += 1;
                        AcquireAction::CreateNew
                    } else {
                        inner.waiter_count += 1;
                        AcquireAction::Wait(WaiterGuard {
                            pool: Arc::clone(&self.shared),
                        })
                    }
                };
                (action, retired)
            };

            // Teardown runs outside the mutex and inside the same deadline as
            // validation and creation. Dropping the acquisition drops every
            // remaining guard, including a close hook that never completes.
            for guard in retired {
                if let Some(outcome) = self.acquire_interruption(cx, deadline) {
                    return outcome;
                }
                match guard
                    .close(cx, "expired pooled connection retirement")
                    .await
                {
                    RetirementOutcome::Closed => {}
                    RetirementOutcome::Cancelled(reason) => {
                        return Outcome::Cancelled(reason);
                    }
                    RetirementOutcome::Failed(error) => return Outcome::Err(error),
                }
            }

            match action {
                AcquireAction::RetireAndRetry => continue,
                AcquireAction::PoolClosed => {
                    return Outcome::Err(Error::Pool(PoolError {
                        kind: PoolErrorKind::Closed,
                        message: "pool has been closed".to_string(),
                        source: None,
                    }));
                }
                AcquireAction::ValidateExisting(meta) => {
                    match self
                        .validate_and_wrap(cx, meta, config.test_on_checkout, deadline)
                        .await
                    {
                        Outcome::Ok(Some(pooled)) => return Outcome::Ok(pooled),
                        Outcome::Ok(None) => {
                            tracing::warn!(
                                "pooled connection failed its checkout ping; closed and replaced"
                            );
                        }
                        Outcome::Err(error) => return Outcome::Err(error),
                        Outcome::Cancelled(reason) => return Outcome::Cancelled(reason),
                        Outcome::Panicked(info) => return Outcome::Panicked(info),
                    }
                }
                AcquireAction::CreateNew => {
                    let mut slot_guard = ActiveSlotGuard::new(Arc::clone(&self.shared));
                    match factory().await {
                        Outcome::Ok(conn) => {
                            self.shared
                                .connections_created
                                .fetch_add(1, Ordering::Relaxed);
                            let guard = ActiveConnectionGuard::new(
                                Arc::clone(&self.shared),
                                ConnectionMeta::new(conn, clock.clone()),
                            );
                            slot_guard.disarm();
                            if let Some(outcome) = self.acquire_interruption(cx, deadline) {
                                return outcome;
                            }
                            let publish = self.publish_acquired(cx, guard, deadline);
                            return match publish {
                                AcquirePublish::Published(pooled) => Outcome::Ok(pooled),
                                AcquirePublish::Interrupted(outcome) => outcome,
                                AcquirePublish::Retire { guard, error } => {
                                    if let Some(outcome) = self.acquire_interruption(cx, deadline) {
                                        return outcome;
                                    }
                                    match guard.close(cx, "acquire publication rejected").await {
                                        RetirementOutcome::Closed => Outcome::Err(error),
                                        RetirementOutcome::Cancelled(reason) => {
                                            Outcome::Cancelled(reason)
                                        }
                                        RetirementOutcome::Failed(error) => Outcome::Err(error),
                                    }
                                }
                            };
                        }
                        Outcome::Err(error) => {
                            slot_guard.release("connection factory error");
                            return Outcome::Err(error);
                        }
                        Outcome::Cancelled(reason) => {
                            slot_guard.release("connection factory cancellation");
                            return Outcome::Cancelled(reason);
                        }
                        Outcome::Panicked(info) => {
                            slot_guard.release("connection factory panic");
                            return Outcome::Panicked(info);
                        }
                    }
                }
                AcquireAction::Wait(_waiter_guard) => {
                    // Notify stores return permits. Retain a short slice to
                    // observe a pool-close broadcast that raced registration;
                    // the outer future independently bounds all other awaits.
                    let remaining = Duration::from_nanos(
                        deadline
                            .at
                            .as_nanos()
                            .saturating_sub(clock.now().as_nanos()),
                    );
                    let wait_time = remaining.min(Duration::from_millis(100));
                    let mut notified = std::pin::pin!(self.shared.conn_available.notified());
                    let mut slice = std::pin::pin!(asupersync::time::sleep(cx.now(), wait_time));
                    let _ = Select::new(notified.as_mut(), slice.as_mut()).await;
                    // WaiterGuard unregisters on wake, timeout, cancellation,
                    // and hard drop of the acquisition.
                }
            }
        }
    }

    /// Fence admission against closure, cancellation, and deadline expiry.
    /// The guard predates the lock, so interruption drops the lock before
    /// destroying the connection and releasing its accounted slot.
    fn publish_acquired(
        &self,
        cx: &Cx,
        guard: ActiveConnectionGuard<C>,
        deadline: AcquireDeadline,
    ) -> AcquirePublish<C> {
        let inner = match self.shared.lock_or_error("acquire_publish") {
            Ok(inner) => inner,
            Err(error) => return AcquirePublish::Retire { guard, error },
        };
        if let Some(outcome) = self.acquire_interruption(cx, deadline) {
            drop(inner);
            drop(guard);
            return AcquirePublish::Interrupted(outcome);
        }
        if inner.closed {
            return AcquirePublish::Retire {
                guard,
                error: Error::Pool(PoolError {
                    kind: PoolErrorKind::Closed,
                    message: "pool has been closed".to_string(),
                    source: None,
                }),
            };
        }
        self.shared.acquires.fetch_add(1, Ordering::Relaxed);
        AcquirePublish::Published(PooledConnection::new(
            guard.into_meta(),
            Arc::downgrade(&self.shared),
            Duration::from_millis(inner.config.acquire_timeout_ms),
        ))
    }

    /// Validate an idle connection. Ok(None) requests a retry after an
    /// ordinary unhealthy result; cancellation and panic are terminal.
    async fn validate_and_wrap(
        &self,
        cx: &Cx,
        meta: ConnectionMeta<C>,
        test_on_checkout: bool,
        deadline: AcquireDeadline,
    ) -> Outcome<Option<PooledConnection<C>>, Error> {
        let guard = ActiveConnectionGuard::new(Arc::clone(&self.shared), meta);
        // The flag controls the optional health probe, not driver safeguards
        // against handing another borrower an unfinished transaction.
        match guard
            .connection()
            .validate_for_pool(cx, test_on_checkout)
            .await
        {
            Outcome::Ok(()) => {}
            Outcome::Cancelled(reason) => return Outcome::Cancelled(reason),
            Outcome::Panicked(info) => return Outcome::Panicked(info),
            Outcome::Err(_) => {
                // A driver may consume the remaining budget within one
                // poll. Do not begin another I/O operation after expiry.
                if let Some(outcome) = self.acquire_interruption(cx, deadline) {
                    return outcome;
                }
                match guard
                    .close(cx, "pooled connection checkout validation failure")
                    .await
                {
                    RetirementOutcome::Closed => return Outcome::Ok(None),
                    RetirementOutcome::Cancelled(reason) => {
                        return Outcome::Cancelled(reason);
                    }
                    RetirementOutcome::Failed(error) => return Outcome::Err(error),
                }
            }
        }

        match self.publish_acquired(cx, guard, deadline) {
            AcquirePublish::Published(pooled) => Outcome::Ok(Some(pooled)),
            AcquirePublish::Interrupted(outcome) => outcome.map(Some),
            AcquirePublish::Retire { guard, error } => {
                if let Some(outcome) = self.acquire_interruption(cx, deadline) {
                    return outcome;
                }
                match guard.close(cx, "checkout publication rejected").await {
                    RetirementOutcome::Closed => Outcome::Err(error),
                    RetirementOutcome::Cancelled(reason) => Outcome::Cancelled(reason),
                    RetirementOutcome::Failed(error) => Outcome::Err(error),
                }
            }
        }
    }

    /// Remove every idle connection from admission and transfer it to active
    /// retirement accounting.
    ///
    /// `total_count` deliberately remains unchanged until each retirement
    /// guard has finished or been hard-dropped. This lets `close_and_drain`
    /// treat driver teardown as part of the quiescence boundary.
    fn begin_idle_retirement(&self) -> Vec<ActiveConnectionGuard<C>> {
        match self.shared.inner.lock() {
            Ok(mut inner) => {
                let mut retired = Vec::with_capacity(inner.idle.len());
                while let Some(meta) = inner.idle.pop_front() {
                    inner.active_count += 1;
                    retired.push(ActiveConnectionGuard::new(Arc::clone(&self.shared), meta));
                }
                retired
            }
            Err(_poisoned) => {
                tracing::error!(
                    "Pool mutex poisoned during idle retirement; \
                     idle connections cannot be retired safely"
                );
                Vec::new()
            }
        }
    }

    /// Atomically close admission and transfer all idle inventory to active
    /// retirement accounting.
    fn begin_close(&self) -> Vec<ActiveConnectionGuard<C>> {
        let retired = match self.shared.inner.lock() {
            Ok(mut inner) => {
                inner.closed = true;
                let mut retired = Vec::with_capacity(inner.idle.len());
                while let Some(meta) = inner.idle.pop_front() {
                    inner.active_count += 1;
                    retired.push(ActiveConnectionGuard::new(Arc::clone(&self.shared), meta));
                }
                retired
            }
            Err(poisoned) => {
                // Recover from poisoning - we still want to mark the pool as
                // closed and wake waiters even if counts may be inconsistent.
                tracing::error!(
                    "Pool mutex poisoned during close; attempting recovery. \
                     Pool state may be inconsistent."
                );
                let mut inner = poisoned.into_inner();
                inner.closed = true;
                let mut retired = Vec::with_capacity(inner.idle.len());
                while let Some(meta) = inner.idle.pop_front() {
                    inner.active_count += 1;
                    retired.push(ActiveConnectionGuard::new(Arc::clone(&self.shared), meta));
                }
                retired
            }
        };

        // Wake all waiters so they see the pool is closed
        self.shared.conn_available.notify_waiters();
        retired
    }

    fn close_retirements_blocking(
        mut retired: Vec<ActiveConnectionGuard<C>>,
        deadline: Time,
        context: &'static str,
    ) {
        let Some(first) = retired.first() else {
            return;
        };
        let shared = Arc::clone(&first.pool);
        for guard in &mut retired {
            guard.synchronous_context = Some(context);
        }
        let work_shared = Arc::clone(&shared);
        let _ = run_retirement_blocking(
            deadline,
            context,
            move |cx| async move {
                let mut first_failure = None;
                for guard in retired {
                    if cx.now() >= deadline {
                        let error = retirement_timeout();
                        work_shared.record_retirement_failure(context, &error);
                        return Err(error);
                    }
                    if let Err(error) = guard.close_synchronously(&cx, deadline, context).await {
                        first_failure.get_or_insert(error);
                    }
                }
                first_failure.map_or(Ok(()), Err)
            },
            |error| shared.record_retirement_failure(context, error),
        );
    }

    /// Close all currently idle connections.
    ///
    /// The entire idle batch shares one `acquire_timeout_ms` grace period.
    /// Unfinished owned resources are dropped on expiry and later drainers
    /// observe a sticky retirement failure. No background cleanup is detached.
    ///
    /// If the pool mutex is poisoned, this logs an error and leaves the idle
    /// inventory untouched because its accounting cannot be mutated safely.
    pub fn clear_idle(&self) {
        let deadline =
            asupersync::time::wall_now() + Duration::from_millis(self.config().acquire_timeout_ms);
        let retired = self.begin_idle_retirement();
        Self::close_retirements_blocking(retired, deadline, "pool clear_idle");
    }

    /// Close the pool, preventing new connections and closing all idle connections.
    ///
    /// Graceful idle retirement shares one `acquire_timeout_ms` wall-clock
    /// grace period. Expiry or unwinding drops the remaining pool-owned work
    /// and records a failure before waking drainers. Independently held shared
    /// connection aliases remain outside the pool's ownership.
    ///
    /// If the pool mutex is poisoned, this logs an error but still wakes waiters.
    pub fn close(&self) {
        let deadline =
            asupersync::time::wall_now() + Duration::from_millis(self.config().acquire_timeout_ms);
        let retired = self.begin_close();
        Self::close_retirements_blocking(retired, deadline, "pool close");

        // Closing an already-empty pool is itself the one and only drain
        // transition. Accounted retirements set the latch when their final
        // guard releases.
        if self.shared.lock_or_recover().active_count == 0 {
            let _ = self.shared.active_drained.set(());
        }
    }

    /// Close the pool and wait for every pool-owned active connection to retire.
    ///
    /// Closing admission and removing idle inventory happen synchronously
    /// before this method first yields. Blocked acquirers are woken and observe
    /// [`PoolErrorKind::Closed`]. Connections already checked out are closed
    /// when returned. Each slot remains active until its `close_for_pool` hook
    /// finishes or its owned close future and resource have been destroyed.
    /// Incomplete synchronous retirement leaves a sticky failure, reported
    /// after the active count reaches zero.
    ///
    /// The wait is cancellation- and deadline-aware through `cx`. Cancellation
    /// returns [`Outcome::Cancelled`] without reopening the pool: `closed`
    /// remains a one-way lifecycle transition, and a later caller may resume
    /// draining. Dropping this future has the same persistent-close property.
    /// One timer covers both idle retirement and waiting for borrowed leases.
    /// Deadline expiry drops this drain's in-flight retirement resources before
    /// returning; leases still held by callers retain their ownership and remain
    /// accounted until they return or detach.
    ///
    /// Multiple concurrent drainers are supported by the pool's one-shot drain
    /// latch. Since pool closure is irreversible, there is no reopen generation
    /// whose notification could be confused with this drain cycle.
    ///
    /// Driver teardown failures are sticky and fail closed: the pool still
    /// retires every accounted resource, but this and every later drainer
    /// returns [`Outcome::Err`] after the active count reaches zero.
    ///
    /// A connection removed with [`PooledConnection::detach`] is caller-owned
    /// and is no longer part of pool accounting, so it is outside this drain
    /// guarantee.
    pub async fn close_and_drain(&self, cx: &Cx) -> Outcome<(), Error> {
        // The irreversible lifecycle transition and waiter wake happen before
        // any cancellation point. Idle inventory remains accounted as active
        // retirement work until each close hook or hard drop releases it.
        let retired = self.begin_close();
        let mut drain = std::pin::pin!(self.drain_retirements_and_leases(cx, retired));
        let Some(deadline) = cx.budget().deadline else {
            return drain.as_mut().await;
        };
        let mut elapsed = std::pin::pin!(asupersync::time::sleep_until(deadline));

        // A request Cx need not belong to a scheduled task. Its cancellation
        // latch detects an elapsed budget when polled, but cannot itself wake
        // a drain whose driver or borrower never produces another event.
        // Keep one independently registered deadline across every await.
        std::future::poll_fn(|task_cx| {
            if cx.now() >= deadline {
                return std::task::Poll::Ready(Outcome::Cancelled(
                    cx.cancel_reason().unwrap_or_else(CancelReason::deadline),
                ));
            }
            if elapsed.as_mut().poll(task_cx).is_ready() {
                // Sleep may also finish when a distinct ambient context is
                // cancelled. Preserve that cause and never repoll the timer.
                if cx.now() >= deadline {
                    return std::task::Poll::Ready(Outcome::Cancelled(
                        cx.cancel_reason().unwrap_or_else(CancelReason::deadline),
                    ));
                }
                let reason = cx
                    .cancel_reason()
                    .or_else(|| Cx::current().and_then(|ambient| ambient.cancel_reason()))
                    .unwrap_or_else(|| CancelReason::user("pool drain interrupted"));
                return std::task::Poll::Ready(Outcome::Cancelled(reason));
            }
            let outcome = drain.as_mut().poll(task_cx);
            if outcome.is_pending() && cx.now() >= deadline {
                return std::task::Poll::Ready(Outcome::Cancelled(
                    cx.cancel_reason().unwrap_or_else(CancelReason::deadline),
                ));
            }
            outcome
        })
        .await
        // Drop the owned drain future (including unstarted retirement guards)
        // before exposing the result, even if the caller retains this future.
    }

    async fn drain_retirements_and_leases(
        &self,
        cx: &Cx,
        retired: Vec<ActiveConnectionGuard<C>>,
    ) -> Outcome<(), Error> {
        let mut direct_failure = None;
        for guard in retired {
            match guard.close(cx, "pool close-and-drain").await {
                RetirementOutcome::Closed => {}
                RetirementOutcome::Cancelled(reason) => {
                    return Outcome::Cancelled(reason);
                }
                RetirementOutcome::Failed(error) => {
                    direct_failure.get_or_insert(error);
                }
            }
        }

        let poisoned_failure = match self.shared.inner.lock() {
            Ok(inner) => {
                drop(inner);
                None
            }
            Err(poisoned) => {
                let error = Error::Pool(PoolError::poisoned("close_and_drain"));
                self.shared
                    .record_retirement_failure("close_and_drain", &error);
                drop(poisoned.into_inner());
                Some(error)
            }
        };

        if self.shared.lock_or_recover().active_count == 0 {
            let _ = self.shared.active_drained.set(());
        }

        if self.shared.active_drained.wait(cx).await.is_ok() {
            if let Some(error) = direct_failure {
                Outcome::Err(error)
            } else if let Some(error) = poisoned_failure {
                Outcome::Err(error)
            } else if let Some(error) = self.shared.retirement_failure_error() {
                Outcome::Err(error)
            } else {
                Outcome::Ok(())
            }
        } else {
            let reason = cx
                .cancel_reason()
                .unwrap_or_else(|| CancelReason::user("pool drain cancelled"));
            Outcome::Cancelled(reason)
        }
    }

    /// Get the number of idle connections.
    #[must_use]
    pub fn idle_count(&self) -> usize {
        let inner = self.shared.lock_or_recover();
        inner.idle.len()
    }

    /// Get the number of active connections.
    #[must_use]
    pub fn active_count(&self) -> usize {
        let inner = self.shared.lock_or_recover();
        inner.active_count
    }

    /// Get the total number of connections.
    #[must_use]
    pub fn total_count(&self) -> usize {
        let inner = self.shared.lock_or_recover();
        inner.total_count
    }
}

impl<C: Connection> Drop for Pool<C> {
    fn drop(&mut self) {
        self.close();
    }
}

/// Action to take when acquiring a connection.
enum AcquireAction<C: Connection> {
    /// Expired idle connections were removed and must retire before retrying.
    RetireAndRetry,
    /// Pool is closed
    PoolClosed,
    /// Found an existing connection to validate
    ValidateExisting(ConnectionMeta<C>),
    /// Create a new connection
    CreateNew,
    /// Wait for a connection to become available
    Wait(WaiterGuard<C>),
}

enum AcquirePublish<C: Connection> {
    Published(PooledConnection<C>),
    Interrupted(Outcome<PooledConnection<C>, Error>),
    Retire {
        guard: ActiveConnectionGuard<C>,
        error: Error,
    },
}

/// A connection borrowed from the pool.
///
/// When dropped, the connection is automatically returned to the pool, also
/// while a panic unwinds the holder. The connection can be used via `Deref`
/// and `DerefMut`.
/// Retirement after closure or maximum age uses the pool's configured
/// acquisition timeout as its graceful-close bound, even if the pool is gone.
pub struct PooledConnection<C: Connection> {
    /// The connection metadata (Some while held, None after return)
    meta: Option<ConnectionMeta<C>>,
    /// Weak reference to pool for returning
    pool: Weak<PoolShared<C>>,
    /// Retained independently of the weak pool reference for late returns.
    retirement_grace: Duration,
}

impl<C: Connection> PooledConnection<C> {
    fn new(meta: ConnectionMeta<C>, pool: Weak<PoolShared<C>>, retirement_grace: Duration) -> Self {
        Self {
            meta: Some(meta),
            pool,
            retirement_grace,
        }
    }

    fn retire(&self, pool: &Arc<PoolShared<C>>, meta: ConnectionMeta<C>, context: &'static str) {
        let deadline = asupersync::time::wall_now() + self.retirement_grace;
        let guard = ActiveConnectionGuard::new(Arc::clone(pool), meta);
        Pool::close_retirements_blocking(vec![guard], deadline, context);
    }

    /// Detach this connection from the pool.
    ///
    /// The connection will not be returned to the pool when dropped.
    /// This is useful when you need to close a connection explicitly.
    pub fn detach(mut self) -> C {
        let conn = self.meta.take().expect("connection already detached").conn;
        if let Some(pool) = self.pool.upgrade() {
            pool.connections_closed.fetch_add(1, Ordering::Relaxed);
            pool.release_active_slot("pooled connection detach");
        }
        conn
    }

    /// Get the age of this connection (time since creation).
    #[must_use]
    pub fn age(&self) -> Duration {
        self.meta.as_ref().map_or(Duration::ZERO, |m| m.age())
    }

    /// Get the idle time of this connection (time since last use).
    #[must_use]
    pub fn idle_time(&self) -> Duration {
        self.meta.as_ref().map_or(Duration::ZERO, |m| m.idle_time())
    }
}

impl<C: Connection> std::ops::Deref for PooledConnection<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self
            .meta
            .as_ref()
            .expect("connection already returned to pool")
            .conn
    }
}

impl<C: Connection> std::ops::DerefMut for PooledConnection<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self
            .meta
            .as_mut()
            .expect("connection already returned to pool")
            .conn
    }
}

impl<C: Connection> Drop for PooledConnection<C> {
    fn drop(&mut self) {
        if let Some(mut meta) = self.meta.take() {
            meta.touch(); // Update last used time
            if let Some(pool) = self.pool.upgrade() {
                // Return to pool - but if mutex is poisoned, do not panic in
                // Drop. Close the resource, then recover only enough accounting
                // to prevent a drain waiter from being stranded.
                let mut inner = match pool.inner.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => {
                        // Release the poisoned guard before the close hook and
                        // poison-aware accounting transition reacquire state.
                        drop(poisoned);
                        tracing::error!(
                            "Pool mutex poisoned during connection return; \
                             connection will be closed instead of returned. A thread panicked while holding the lock."
                        );
                        let context = "pooled connection drop poisoned";
                        self.retire(&pool, meta, context);
                        return;
                    }
                };

                if inner.closed {
                    drop(inner);
                    let context = "pooled connection drop closed pool";
                    self.retire(&pool, meta, context);
                    return;
                }

                // Check max lifetime
                let max_lifetime = Duration::from_millis(inner.config.max_lifetime_ms);
                if meta.age() > max_lifetime {
                    drop(inner);
                    let context = "pooled connection drop max lifetime";
                    self.retire(&pool, meta, context);
                    return;
                }

                inner.active_count -= 1;
                inner.idle.push_back(meta);

                drop(inner);
                pool.conn_available.notify_one();
            } else {
                let _ = close_connection_blocking(
                    meta.conn,
                    self.retirement_grace,
                    "pooled connection drop missing pool",
                );
            }
        }
    }
}

impl<C: Connection + std::fmt::Debug> std::fmt::Debug for PooledConnection<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledConnection")
            .field("conn", &self.meta.as_ref().map(|m| &m.conn))
            .field("age", &self.age())
            .field("idle_time", &self.idle_time())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asupersync::lab::explorer::{DporExplorer, ExplorerConfig};
    use asupersync::lab::{LabConfig, LabRuntime};
    use asupersync::time::VirtualClock;
    use asupersync::types::RegionId;
    use asupersync::{Budget, Time};
    use sqlmodel_core::connection::{
        Dialect, IsolationLevel, PreparedStatement, TransactionMode, TransactionOps,
    };
    use sqlmodel_core::error::{
        ConnectionError, ConnectionErrorKind, QueryError, QueryErrorKind, TransactionErrorKind,
    };
    use sqlmodel_core::{RetryPolicy, TransactionOptions, retry_transaction};
    use sqlmodel_core::{Row, Value};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::task::{Context, Poll, Wake, Waker};

    /// A fresh wall-clock timer driver for tests that do not care about
    /// controlling time (all wall-clock handles read the same OS clock).
    fn test_clock() -> TimerDriverHandle {
        TimerDriverHandle::with_wall_clock()
    }

    /// A virtual clock and the driver handle reading it. Advancing the clock
    /// moves every `clock.now()` / `meta.age()` / pool deadline at once.
    fn virtual_clock() -> (Arc<VirtualClock>, TimerDriverHandle) {
        let clock = Arc::new(VirtualClock::new());
        let driver = TimerDriverHandle::with_virtual_clock(Arc::clone(&clock));
        (clock, driver)
    }

    /// Backdates a connection's creation instant, as if it had been sitting
    /// in the pool for `ago` before the current virtual or wall instant.
    fn backdate<C>(meta: &mut ConnectionMeta<C>, ago: Duration) {
        meta.created_at = meta
            .clock
            .now()
            .saturating_sub_nanos(u64::try_from(ago.as_nanos()).expect("duration fits u64 nanos"));
    }

    #[derive(Debug)]
    enum MockPing {
        Ready,
        Pending,
        Gated(Arc<AtomicBool>),
        Delay(Duration),
        Cancelled,
        Panicked,
        Unwind,
        CancelOnReady,
        AdvanceClock(Arc<VirtualClock>, u64),
    }

    #[derive(Clone, Copy, Debug)]
    enum MockPoolValidation {
        Default,
        Reject,
        Pending,
        Cancelled,
    }

    /// An externally releasable close hook. Its independent context deliberately
    /// ignores the retirement deadline, requiring the pool to enforce the bound.
    /// Tests release it before joining their worker even when the old pool code
    /// waits forever, so a failed regression cannot strand a thread.
    #[derive(Debug)]
    struct PoolCloseGate {
        released: OnceCell<()>,
        started: std::sync::mpsc::Sender<()>,
        completions: AtomicUsize,
    }

    impl PoolCloseGate {
        fn new() -> (Arc<Self>, std::sync::mpsc::Receiver<()>) {
            let (started, receiver) = std::sync::mpsc::channel();
            (
                Arc::new(Self {
                    released: OnceCell::new(),
                    started,
                    completions: AtomicUsize::new(0),
                }),
                receiver,
            )
        }

        async fn wait(&self) {
            let independent = Cx::for_testing();
            let mut wait = std::pin::pin!(self.released.wait(&independent));
            let mut announced = false;
            std::future::poll_fn(|task_cx| {
                let outcome = wait.as_mut().poll(task_cx);
                if outcome.is_pending() && !announced {
                    announced = true;
                    let _ = self.started.send(());
                }
                outcome
            })
            .await
            .expect("the test gate is released, never cancelled");
            self.completions.fetch_add(1, Ordering::Relaxed);
        }

        fn release(&self) {
            let _ = self.released.set(());
        }
    }

    #[derive(Clone, Copy)]
    enum GateRelease {
        AfterStart,
        AfterWatchdog,
    }

    /// Observe synchronous teardown without letting a RED leave it blocked.
    /// Both the gate release and worker join happen before any assertion.
    fn run_gated_retirement(
        gate: &PoolCloseGate,
        started: &std::sync::mpsc::Receiver<()>,
        release: GateRelease,
        retire: impl FnOnce() + Send + 'static,
    ) -> (bool, Duration) {
        let (completed_tx, completed_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let started_at = std::time::Instant::now();
            retire();
            let elapsed = started_at.elapsed();
            let _ = completed_tx.send(());
            elapsed
        });
        let hook_started = started.recv_timeout(Duration::from_secs(2));
        let completed_before_release = match release {
            GateRelease::AfterStart => completed_rx.try_recv().is_ok(),
            GateRelease::AfterWatchdog => completed_rx.recv_timeout(Duration::from_secs(1)).is_ok(),
        };
        gate.release();
        let elapsed = worker.join().expect("retirement worker must not panic");
        assert!(hook_started.is_ok(), "the close hook must be attempted");
        (completed_before_release, elapsed)
    }

    /// Observes resource destruction itself, before the pool may release its
    /// active slot. A missing inner snapshot also detects a held pool mutex.
    #[derive(Debug)]
    struct ConnectionDropProbe {
        pool: Weak<PoolShared<MockConnection>>,
        observed: Arc<OnceCell<Option<PoolStats>>>,
    }

    impl Drop for ConnectionDropProbe {
        fn drop(&mut self) {
            let snapshot = self
                .pool
                .upgrade()
                .and_then(|shared| shared.inner.try_lock().ok().map(|inner| inner.stats()));
            let _ = self.observed.set(snapshot);
        }
    }

    /// A mock connection for testing pool behavior.
    #[derive(Debug)]
    struct MockConnection {
        id: u32,
        ping_should_fail: Arc<AtomicBool>,
        ping: MockPing,
        pool_validation: MockPoolValidation,
        validation_checks: Arc<Mutex<Vec<bool>>>,
        _drop_probe: Option<ConnectionDropProbe>,
        /// Incremented each time the pool retires this connection via
        /// `close_for_pool` (as opposed to a caller-owned `close`).
        pool_close_calls: Arc<AtomicUsize>,
        /// When true, `close_for_pool` remains pending until its future is
        /// dropped. Used to prove slot guards across close await points.
        pool_close_pending: bool,
        /// When true, pool retirement returns a deterministic driver error.
        pool_close_should_fail: bool,
        /// When true, the close hook unwinds after taking ownership.
        pool_close_panics: bool,
        pool_close_gate: Option<Arc<PoolCloseGate>>,
        pool_close_delay: Option<Duration>,
        /// Optional probe used to prove the pool mutex is not held while the
        /// retirement hook runs.
        pool_shared: Option<Weak<PoolShared<MockConnection>>>,
        pool_lock_was_free: Option<Arc<AtomicBool>>,
    }

    impl MockConnection {
        fn new(id: u32) -> Self {
            Self {
                id,
                ping_should_fail: Arc::new(AtomicBool::new(false)),
                ping: MockPing::Ready,
                pool_validation: MockPoolValidation::Default,
                validation_checks: Arc::new(Mutex::new(Vec::new())),
                _drop_probe: None,
                pool_close_calls: Arc::new(AtomicUsize::new(0)),
                pool_close_pending: false,
                pool_close_should_fail: false,
                pool_close_panics: false,
                pool_close_gate: None,
                pool_close_delay: None,
                pool_shared: None,
                pool_lock_was_free: None,
            }
        }

        #[allow(dead_code)]
        fn with_ping_behavior(id: u32, should_fail: Arc<AtomicBool>) -> Self {
            Self {
                ping_should_fail: should_fail,
                ..Self::new(id)
            }
        }

        fn with_pool_close_counter(id: u32, pool_close_calls: Arc<AtomicUsize>) -> Self {
            Self {
                pool_close_calls,
                ..Self::new(id)
            }
        }

        fn with_pool_close_probe(
            id: u32,
            pool_close_calls: Arc<AtomicUsize>,
            pool_shared: Weak<PoolShared<MockConnection>>,
            pool_lock_was_free: Arc<AtomicBool>,
        ) -> Self {
            Self {
                pool_close_calls,
                pool_shared: Some(pool_shared),
                pool_lock_was_free: Some(pool_lock_was_free),
                ..Self::new(id)
            }
        }

        fn with_pending_pool_close(id: u32, pool_close_calls: Arc<AtomicUsize>) -> Self {
            Self {
                pool_close_calls,
                pool_close_pending: true,
                ..Self::new(id)
            }
        }

        fn with_failing_pool_close(id: u32) -> Self {
            Self {
                pool_close_should_fail: true,
                ..Self::new(id)
            }
        }

        fn with_ping(id: u32, ping: MockPing) -> Self {
            Self {
                ping,
                ..Self::new(id)
            }
        }

        fn with_pool_validation(id: u32, pool_validation: MockPoolValidation) -> Self {
            Self {
                pool_validation,
                ..Self::new(id)
            }
        }

        fn observe_drop(
            self,
            pool: &Pool<Self>,
            observed: Arc<OnceCell<Option<PoolStats>>>,
        ) -> Self {
            Self {
                _drop_probe: Some(ConnectionDropProbe {
                    pool: Arc::downgrade(&pool.shared),
                    observed,
                }),
                ..self
            }
        }
    }

    /// Manually released connection factory used to put `acquire` exactly
    /// across the pool-close publication fence without timing sleeps.
    struct GatedFactory {
        ready: Arc<AtomicBool>,
        conn: Option<MockConnection>,
    }

    #[derive(Default)]
    struct WakeCounter {
        wakes: AtomicUsize,
    }

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::Relaxed);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Observe the publication boundary itself, not a later poll after the
    /// retiring thread has had another opportunity to record its failure.
    struct DrainFailureWake {
        pool: Weak<PoolShared<MockConnection>>,
        observed: OnceCell<bool>,
        advance_after_wake: Option<(Arc<VirtualClock>, u64)>,
    }

    impl DrainFailureWake {
        fn new(pool: &Pool<MockConnection>) -> Arc<Self> {
            Arc::new(Self {
                pool: Arc::downgrade(&pool.shared),
                observed: OnceCell::new(),
                advance_after_wake: None,
            })
        }

        fn record(&self) {
            let failed = self
                .pool
                .upgrade()
                .is_some_and(|pool| pool.retirement_failure_error().is_some());
            if self.observed.set(failed).is_ok()
                && let Some((clock, nanos)) = &self.advance_after_wake
            {
                clock.advance(*nanos);
            }
        }
    }

    impl Wake for DrainFailureWake {
        fn wake(self: Arc<Self>) {
            self.record();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.record();
        }
    }

    impl Future for GatedFactory {
        type Output = Outcome<MockConnection, Error>;

        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.ready.load(Ordering::Acquire) {
                Poll::Ready(Outcome::Ok(
                    self.conn
                        .take()
                        .expect("gated factory polled after completion"),
                ))
            } else {
                Poll::Pending
            }
        }
    }

    /// Mock transaction for MockConnection.
    struct MockTx;

    // These test doubles deliberately mirror the trait's async spelling; the
    // bodies are immediate because no real driver I/O occurs.
    #[allow(clippy::unused_async_trait_impl)]
    impl TransactionOps for MockTx {
        async fn query(&self, _cx: &Cx, _sql: &str, _params: &[Value]) -> Outcome<Vec<Row>, Error> {
            Outcome::Ok(vec![])
        }

        async fn query_one(
            &self,
            _cx: &Cx,
            _sql: &str,
            _params: &[Value],
        ) -> Outcome<Option<Row>, Error> {
            Outcome::Ok(None)
        }

        async fn execute(&self, _cx: &Cx, _sql: &str, _params: &[Value]) -> Outcome<u64, Error> {
            Outcome::Ok(0)
        }

        async fn savepoint(&self, _cx: &Cx, _name: &str) -> Outcome<(), Error> {
            Outcome::Ok(())
        }

        async fn rollback_to(&self, _cx: &Cx, _name: &str) -> Outcome<(), Error> {
            Outcome::Ok(())
        }

        async fn release(&self, _cx: &Cx, _name: &str) -> Outcome<(), Error> {
            Outcome::Ok(())
        }

        async fn commit(self, _cx: &Cx) -> Outcome<(), Error> {
            Outcome::Ok(())
        }

        async fn rollback(self, _cx: &Cx) -> Outcome<(), Error> {
            Outcome::Ok(())
        }
    }

    #[allow(clippy::unused_async_trait_impl)]
    impl Connection for MockConnection {
        type Tx<'conn> = MockTx;

        async fn query(&self, _cx: &Cx, _sql: &str, _params: &[Value]) -> Outcome<Vec<Row>, Error> {
            Outcome::Ok(vec![])
        }

        async fn query_one(
            &self,
            _cx: &Cx,
            _sql: &str,
            _params: &[Value],
        ) -> Outcome<Option<Row>, Error> {
            Outcome::Ok(None)
        }

        async fn execute(&self, _cx: &Cx, _sql: &str, _params: &[Value]) -> Outcome<u64, Error> {
            Outcome::Ok(0)
        }

        async fn insert(&self, _cx: &Cx, _sql: &str, _params: &[Value]) -> Outcome<i64, Error> {
            Outcome::Ok(0)
        }

        async fn batch(
            &self,
            _cx: &Cx,
            _statements: &[(String, Vec<Value>)],
        ) -> Outcome<Vec<u64>, Error> {
            Outcome::Ok(vec![])
        }

        async fn begin(&self, _cx: &Cx) -> Outcome<Self::Tx<'_>, Error> {
            Outcome::Ok(MockTx)
        }

        async fn begin_with(
            &self,
            _cx: &Cx,
            _isolation: IsolationLevel,
        ) -> Outcome<Self::Tx<'_>, Error> {
            Outcome::Ok(MockTx)
        }

        async fn prepare(&self, _cx: &Cx, _sql: &str) -> Outcome<PreparedStatement, Error> {
            Outcome::Ok(PreparedStatement::new(1, String::new(), 0))
        }

        async fn query_prepared(
            &self,
            _cx: &Cx,
            _stmt: &PreparedStatement,
            _params: &[Value],
        ) -> Outcome<Vec<Row>, Error> {
            Outcome::Ok(vec![])
        }

        async fn execute_prepared(
            &self,
            _cx: &Cx,
            _stmt: &PreparedStatement,
            _params: &[Value],
        ) -> Outcome<u64, Error> {
            Outcome::Ok(0)
        }

        async fn ping(&self, cx: &Cx) -> Outcome<(), Error> {
            match &self.ping {
                MockPing::Ready => {}
                MockPing::Pending => std::future::pending::<()>().await,
                MockPing::Gated(ready) => {
                    std::future::poll_fn(|_| {
                        if ready.load(Ordering::Acquire) {
                            Poll::Ready(())
                        } else {
                            Poll::Pending
                        }
                    })
                    .await;
                }
                MockPing::Delay(delay) => asupersync::time::sleep(cx.now(), *delay).await,
                MockPing::Cancelled => {
                    return Outcome::Cancelled(CancelReason::user("mock ping cancelled"));
                }
                MockPing::Panicked => {
                    return Outcome::Panicked(asupersync::types::PanicPayload::new(
                        "mock ping panicked",
                    ));
                }
                MockPing::Unwind => panic!("mock ping unwound"),
                MockPing::CancelOnReady => cx.set_cancel_requested(true),
                MockPing::AdvanceClock(clock, nanos) => clock.advance(*nanos),
            }
            if self.ping_should_fail.load(Ordering::Relaxed) {
                Outcome::Err(Error::Connection(ConnectionError {
                    kind: ConnectionErrorKind::Disconnected,
                    message: "mock ping failed".to_string(),
                    source: None,
                }))
            } else {
                Outcome::Ok(())
            }
        }

        async fn validate_for_pool(&self, cx: &Cx, check_connection: bool) -> Outcome<(), Error> {
            self.validation_checks
                .lock()
                .unwrap()
                .push(check_connection);
            if let Some(reason) = sqlmodel_core::cancel_requested(cx) {
                return Outcome::Cancelled(reason);
            }
            match self.pool_validation {
                MockPoolValidation::Default if check_connection => self.ping(cx).await,
                MockPoolValidation::Default => Outcome::Ok(()),
                MockPoolValidation::Reject => Outcome::Err(Error::Custom(
                    "mock connection is unsuitable for reuse".into(),
                )),
                MockPoolValidation::Pending => std::future::pending().await,
                MockPoolValidation::Cancelled => {
                    Outcome::Cancelled(CancelReason::user("mock pool validation cancelled"))
                }
            }
        }

        async fn close(self, _cx: &Cx) -> Result<(), Error> {
            Ok(())
        }

        async fn close_for_pool(self, cx: &Cx) -> Result<(), Error> {
            if let (Some(pool_shared), Some(pool_lock_was_free)) =
                (self.pool_shared.as_ref(), self.pool_lock_was_free.as_ref())
            {
                let mutex_is_available = pool_shared
                    .upgrade()
                    .is_none_or(|shared| shared.inner.try_lock().is_ok());
                pool_lock_was_free.store(mutex_is_available, Ordering::Relaxed);
            }
            self.pool_close_calls.fetch_add(1, Ordering::Relaxed);
            if let Some(gate) = &self.pool_close_gate {
                gate.wait().await;
            }
            assert!(!self.pool_close_panics, "mock pool close panicked");
            if let Some(delay) = self.pool_close_delay {
                asupersync::time::sleep(cx.now(), delay).await;
            }
            if self.pool_close_pending {
                std::future::pending::<()>().await;
            }
            if self.pool_close_should_fail {
                return Err(Error::Custom("mock pool close failure".to_string()));
            }
            Ok(())
        }
    }

    fn acquire_mock(
        pool: &Pool<MockConnection>,
        conn: MockConnection,
    ) -> PooledConnection<MockConnection> {
        let cx = Cx::for_testing();
        let conn = Mutex::new(Some(conn));
        let mut acquire = Box::pin(pool.acquire(&cx, || {
            std::future::ready(Outcome::Ok(conn.lock().unwrap().take().unwrap()))
        }));
        let mut task_cx = Context::from_waker(Waker::noop());
        match acquire.as_mut().poll(&mut task_cx) {
            Poll::Ready(Outcome::Ok(lease)) => lease,
            other => panic!("immediate factory did not create the fixture: {other:?}"),
        }
    }

    fn seed_idle(pool: &Pool<MockConnection>, conn: MockConnection) {
        drop(acquire_mock(pool, conn));
        assert_eq!(pool.stats().idle_connections, 1);
    }

    fn assert_sticky_retirement_failure(pool: &Pool<MockConnection>) {
        let cx = Cx::for_testing();
        let mut task_cx = Context::from_waker(Waker::noop());
        let mut messages = Vec::new();
        for _ in 0..2 {
            let mut drain = Box::pin(pool.close_and_drain(&cx));
            match drain.as_mut().poll(&mut task_cx) {
                Poll::Ready(Outcome::Err(error)) => messages.push(error.to_string()),
                other => panic!("incomplete synchronous retirement must fail drain: {other:?}"),
            }
        }
        assert_eq!(messages[0], messages[1]);
        assert_no_occupied_slots(pool);
        assert_eq!(pool.stats().timeouts, 0, "retirement is not acquisition");
    }

    fn assert_no_occupied_slots(pool: &Pool<MockConnection>) {
        let stats = pool.stats();
        assert_eq!(stats.total_connections, 0, "{stats:?}");
        assert_eq!(stats.active_connections, 0, "{stats:?}");
        assert_eq!(stats.idle_connections, 0, "{stats:?}");
        assert_eq!(stats.pending_requests, 0, "{stats:?}");
    }

    fn assert_drop_precedes_slot_release(observed: &OnceCell<Option<PoolStats>>) {
        let at_drop = observed
            .get()
            .expect("connection resource was destroyed before acquire returned")
            .as_ref()
            .expect("resource was destroyed without holding the pool mutex");
        assert_eq!(at_drop.active_connections, 1);
        assert_eq!(at_drop.total_connections, 1);
        assert_eq!(at_drop.idle_connections, 0);
    }

    #[test]
    fn pool_validation_rejects_unsuitable_connections_without_health_ping() {
        let pool = Pool::new(PoolConfig::new(1).test_on_checkout(false));
        let dropped = Arc::new(OnceCell::new());
        let conn = MockConnection::with_pool_validation(1, MockPoolValidation::Reject);
        let checks = Arc::clone(&conn.validation_checks);
        let closes = Arc::clone(&conn.pool_close_calls);
        seed_idle(&pool, conn.observe_drop(&pool, Arc::clone(&dropped)));
        let replacement_checks = Arc::new(Mutex::new(Vec::new()));
        let factories = AtomicUsize::new(0);
        let factory = || async {
            factories.fetch_add(1, Ordering::Relaxed);
            // The healthy replacement can be reused without a network ping.
            // Accidentally probing it would return a terminal panic outcome.
            let mut conn = MockConnection::with_ping(2, MockPing::Panicked);
            conn.validation_checks = Arc::clone(&replacement_checks);
            Outcome::Ok(conn)
        };
        let cx = Cx::for_testing();
        let mut task_cx = Context::from_waker(Waker::noop());
        let mut acquire = Box::pin(pool.acquire(&cx, &factory));
        let lease = match acquire.as_mut().poll(&mut task_cx) {
            Poll::Ready(Outcome::Ok(lease)) => lease,
            other => panic!("rejected connection must be replaced immediately: {other:?}"),
        };
        assert_eq!(
            lease.id, 2,
            "local reuse safeguards run with health checks disabled"
        );
        assert_eq!(*checks.lock().unwrap(), vec![false]);
        assert_eq!(closes.load(Ordering::Relaxed), 1);
        assert_drop_precedes_slot_release(&dropped);
        drop(lease);

        let mut reuse = Box::pin(pool.acquire(&cx, &factory));
        let lease = match reuse.as_mut().poll(&mut task_cx) {
            Poll::Ready(Outcome::Ok(lease)) => lease,
            other => panic!("disabled health check must not call ping: {other:?}"),
        };
        assert_eq!(lease.id, 2);
        assert_eq!(*replacement_checks.lock().unwrap(), vec![false]);
        assert_eq!(factories.load(Ordering::Relaxed), 1);
        drop(lease);
        let stats = pool.stats();
        assert_eq!(stats.connections_created, 2);
        assert_eq!(stats.connections_closed, 1);
        assert_eq!(stats.acquires, 3);
        assert_eq!(stats.timeouts, 0);
        assert_eq!(stats.total_connections, 1);
        assert_eq!(stats.idle_connections, 1);
        assert_eq!(stats.active_connections, 0);
        assert_eq!(stats.pending_requests, 0);
        let mut drain = Box::pin(pool.close_and_drain(&cx));
        assert!(matches!(
            drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Ok(()))
        ));
        assert_no_occupied_slots(&pool);
    }

    #[test]
    fn pool_validation_cancellation_is_terminal_without_health_ping() {
        for validation in [MockPoolValidation::Pending, MockPoolValidation::Cancelled] {
            let pool = Pool::new(PoolConfig::new(1).test_on_checkout(false));
            let dropped = Arc::new(OnceCell::new());
            let conn = MockConnection::with_pool_validation(1, validation);
            let checks = Arc::clone(&conn.validation_checks);
            let closes = Arc::clone(&conn.pool_close_calls);
            seed_idle(&pool, conn.observe_drop(&pool, Arc::clone(&dropped)));
            let cx = Cx::for_testing();
            let factories = AtomicUsize::new(0);
            let mut acquire = Box::pin(pool.acquire(&cx, || async {
                factories.fetch_add(1, Ordering::Relaxed);
                Outcome::Ok(MockConnection::new(2))
            }));
            let notifications = Arc::new(WakeCounter::default());
            let waker = Waker::from(Arc::clone(&notifications));
            let mut task_cx = Context::from_waker(&waker);
            let expected = if matches!(validation, MockPoolValidation::Pending) {
                assert!(acquire.as_mut().poll(&mut task_cx).is_pending());
                let before = notifications.wakes.load(Ordering::Relaxed);
                cx.cancel_with(
                    asupersync::CancelKind::User,
                    Some("interrupt pending pool validation"),
                );
                assert!(notifications.wakes.load(Ordering::Relaxed) > before);
                cx.cancel_reason().expect("caller cancellation reason")
            } else {
                CancelReason::user("mock pool validation cancelled")
            };
            match acquire.as_mut().poll(&mut task_cx) {
                Poll::Ready(Outcome::Cancelled(reason)) => assert_eq!(reason, expected),
                other => panic!("pool hook cancellation must propagate unchanged: {other:?}"),
            }
            assert_eq!(*checks.lock().unwrap(), vec![false]);
            assert_eq!(factories.load(Ordering::Relaxed), 0);
            assert_eq!(closes.load(Ordering::Relaxed), 0);
            assert_drop_precedes_slot_release(&dropped);
            assert_no_occupied_slots(&pool);
            assert_eq!(pool.stats().connections_created, 1);
            assert_eq!(pool.stats().connections_closed, 1);
            assert_eq!(pool.stats().acquires, 1);
            assert_eq!(pool.stats().timeouts, 0);
        }
    }

    #[test]
    fn checkout_cancellation_wakes_a_ping_that_never_wakes() {
        let pool = Pool::new(PoolConfig::new(1));
        let dropped = Arc::new(OnceCell::new());
        seed_idle(
            &pool,
            MockConnection::with_ping(1, MockPing::Pending)
                .observe_drop(&pool, Arc::clone(&dropped)),
        );
        let cx = Cx::for_testing();
        let calls = AtomicUsize::new(0);
        let mut acquire = Box::pin(pool.acquire(&cx, || async {
            calls.fetch_add(1, Ordering::Relaxed);
            Outcome::Ok(MockConnection::new(2))
        }));
        let notifications = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&notifications));
        let mut task_cx = Context::from_waker(&waker);
        assert!(acquire.as_mut().poll(&mut task_cx).is_pending());
        assert_eq!(pool.stats().active_connections, 1);
        assert!(dropped.get().is_none());

        let before = notifications.wakes.load(Ordering::Relaxed);
        cx.set_cancel_requested(true);
        assert!(notifications.wakes.load(Ordering::Relaxed) > before);
        assert!(matches!(
            acquire.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Cancelled(_))
        ));

        // Deliberately retain the completed future: cleanup must precede
        // returning Ready, rather than relying on its caller to drop it.
        assert_drop_precedes_slot_release(&dropped);
        assert_no_occupied_slots(&pool);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(pool.stats().connections_closed, 1);
        assert_eq!(pool.stats().timeouts, 0);
        let drain_cx = Cx::for_testing();
        let mut drain = Box::pin(pool.close_and_drain(&drain_cx));
        assert!(matches!(
            drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Ok(()))
        ));
    }

    #[test]
    fn dropping_checkout_validation_discards_resource_and_wakes_waiter() {
        let pool = Pool::new(PoolConfig::new(1));
        let dropped = Arc::new(OnceCell::new());
        seed_idle(
            &pool,
            MockConnection::with_ping(1, MockPing::Pending)
                .observe_drop(&pool, Arc::clone(&dropped)),
        );
        let validating_cx = Cx::for_testing();
        let waiting_cx = Cx::for_testing();
        let mut validation = Box::pin(pool.acquire(&validating_cx, || async {
            Outcome::Ok(MockConnection::new(99))
        }));
        let mut waiter = Box::pin(pool.acquire(&waiting_cx, || async {
            Outcome::Ok(MockConnection::new(2))
        }));
        let notifications = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&notifications));
        let mut task_cx = Context::from_waker(&waker);
        assert!(validation.as_mut().poll(&mut task_cx).is_pending());
        assert!(waiter.as_mut().poll(&mut task_cx).is_pending());
        assert_eq!(pool.stats().pending_requests, 1);
        let before = notifications.wakes.load(Ordering::Relaxed);

        drop(validation);

        assert_drop_precedes_slot_release(&dropped);
        assert!(notifications.wakes.load(Ordering::Relaxed) > before);
        assert_eq!(pool.stats().total_connections, 0);
        assert_eq!(pool.stats().pending_requests, 1);
        let lease = match waiter.as_mut().poll(&mut task_cx) {
            Poll::Ready(Outcome::Ok(lease)) => lease,
            other => panic!("freed slot did not let the waiter recover: {other:?}"),
        };
        assert_eq!(lease.id, 2);
        assert_eq!(pool.stats().pending_requests, 0);
        assert_eq!(pool.stats().connections_created, 2);
        assert_eq!(pool.stats().connections_closed, 1);
        drop(lease);
        let drain_cx = Cx::for_testing();
        let mut drain = Box::pin(pool.close_and_drain(&drain_cx));
        assert!(matches!(
            drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Ok(()))
        ));
        assert_no_occupied_slots(&pool);
    }

    #[test]
    fn dropping_queued_acquire_unregisters_only_its_own_waiter() {
        let pool = Pool::new(PoolConfig::new(1));
        let holder_cx = Cx::for_testing();
        let mut holder =
            Box::pin(pool.acquire(&holder_cx, || async { Outcome::Ok(MockConnection::new(1)) }));
        let mut task_cx = Context::from_waker(Waker::noop());
        let lease = match holder.as_mut().poll(&mut task_cx) {
            Poll::Ready(Outcome::Ok(lease)) => lease,
            other => panic!("holder did not acquire: {other:?}"),
        };
        let waiting_cx = Cx::for_testing();
        let mut first = Box::pin(pool.acquire(&waiting_cx, || async {
            Outcome::Ok(MockConnection::new(2))
        }));
        let mut second = Box::pin(pool.acquire(&waiting_cx, || async {
            Outcome::Ok(MockConnection::new(3))
        }));
        assert!(first.as_mut().poll(&mut task_cx).is_pending());
        assert!(second.as_mut().poll(&mut task_cx).is_pending());
        assert_eq!(pool.stats().pending_requests, 2);
        drop(first);
        assert_eq!(pool.stats().pending_requests, 1);
        assert_eq!(pool.stats().active_connections, 1);
        drop(second);
        assert_eq!(pool.stats().pending_requests, 0);
        drop(lease);
        assert_eq!(pool.stats().idle_connections, 1);
    }

    #[test]
    fn checkout_driver_cancel_and_panic_outcomes_are_terminal() {
        for (ping, panicked) in [(MockPing::Cancelled, false), (MockPing::Panicked, true)] {
            let pool = Pool::new(PoolConfig::new(1));
            let dropped = Arc::new(OnceCell::new());
            seed_idle(
                &pool,
                MockConnection::with_ping(1, ping).observe_drop(&pool, Arc::clone(&dropped)),
            );
            let cx = Cx::for_testing();
            let calls = AtomicUsize::new(0);
            let mut acquire = Box::pin(pool.acquire(&cx, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                Outcome::Ok(MockConnection::new(2))
            }));
            let mut task_cx = Context::from_waker(Waker::noop());
            match acquire.as_mut().poll(&mut task_cx) {
                Poll::Ready(Outcome::Cancelled(reason)) if !panicked => {
                    assert_eq!(reason, CancelReason::user("mock ping cancelled"));
                }
                Poll::Ready(Outcome::Panicked(payload)) if panicked => {
                    assert_eq!(payload.message(), "mock ping panicked");
                }
                other => panic!("validation outcome must propagate unchanged: {other:?}"),
            }
            assert!(
                !cx.is_cancel_requested(),
                "the driver outcome is independent of the caller's flag"
            );
            assert_eq!(
                calls.load(Ordering::Relaxed),
                0,
                "terminal outcomes must never retry"
            );
            assert_drop_precedes_slot_release(&dropped);
            assert_no_occupied_slots(&pool);
            assert_eq!(pool.stats().connections_closed, 1);
            assert_eq!(pool.stats().acquires, 1);
            assert_eq!(pool.stats().timeouts, 0);
        }
    }

    #[test]
    fn checkout_unwinding_drops_the_resource_and_pool_recovers() {
        let pool = Pool::new(PoolConfig::new(1));
        let dropped = Arc::new(OnceCell::new());
        seed_idle(
            &pool,
            MockConnection::with_ping(1, MockPing::Unwind)
                .observe_drop(&pool, Arc::clone(&dropped)),
        );
        let cx = Cx::for_testing();
        let mut task_cx = Context::from_waker(Waker::noop());
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Owning the future inside this boundary exercises actual unwind
            // destruction, as opposed to merely returning Outcome::Panicked.
            let mut acquire =
                Box::pin(pool.acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) }));
            let _ = acquire.as_mut().poll(&mut task_cx);
        }));
        assert!(panic.is_err());
        assert_drop_precedes_slot_release(&dropped);
        assert_no_occupied_slots(&pool);
        assert!(!pool.shared.inner.is_poisoned());
        seed_idle(&pool, MockConnection::new(2));
        let mut drain = Box::pin(pool.close_and_drain(&cx));
        assert!(matches!(
            drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Ok(()))
        ));
        assert_no_occupied_slots(&pool);
    }

    #[test]
    fn checkout_success_that_requests_cancellation_is_not_published() {
        let pool = Pool::new(PoolConfig::new(1));
        let dropped = Arc::new(OnceCell::new());
        seed_idle(
            &pool,
            MockConnection::with_ping(1, MockPing::CancelOnReady)
                .observe_drop(&pool, Arc::clone(&dropped)),
        );
        let cx = Cx::for_testing();
        let mut acquire =
            Box::pin(pool.acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) }));
        let mut task_cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            acquire.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Cancelled(_))
        ));
        assert!(cx.is_cancel_requested());
        assert_drop_precedes_slot_release(&dropped);
        assert_no_occupied_slots(&pool);
        assert_eq!(pool.stats().acquires, 1);
    }

    #[test]
    fn checkout_validation_cannot_publish_after_concurrent_pool_close() {
        let pool = Pool::new(PoolConfig::new(1));
        let ready = Arc::new(AtomicBool::new(false));
        let closes = Arc::new(AtomicUsize::new(0));
        let mut conn = MockConnection::with_pool_close_counter(1, Arc::clone(&closes));
        conn.ping = MockPing::Gated(Arc::clone(&ready));
        seed_idle(&pool, conn);
        let cx = Cx::for_testing();
        let mut acquire =
            Box::pin(pool.acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) }));
        let mut task_cx = Context::from_waker(Waker::noop());
        assert!(acquire.as_mut().poll(&mut task_cx).is_pending());
        pool.close();
        ready.store(true, Ordering::Release);
        assert!(matches!(
            acquire.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Err(Error::Pool(PoolError {
                kind: PoolErrorKind::Closed,
                ..
            })))
        ));
        assert_eq!(closes.load(Ordering::Relaxed), 1);
        assert_eq!(pool.stats().acquires, 1);
        assert_no_occupied_slots(&pool);
        let mut drain = Box::pin(pool.close_and_drain(&cx));
        assert!(matches!(
            drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Ok(()))
        ));
    }

    #[test]
    fn checkout_ambient_cancellation_finishes_without_repolling_completed_timer() {
        let pool = Pool::new(PoolConfig::new(1));
        seed_idle(&pool, MockConnection::with_ping(1, MockPing::Pending));
        let request = Cx::for_testing();
        let ambient = Cx::for_testing();
        let _ambient_guard = Cx::set_current(Some(ambient.clone()));
        let mut acquire =
            Box::pin(pool.acquire(&request, || async { Outcome::Ok(MockConnection::new(2)) }));
        let mut task_cx = Context::from_waker(Waker::noop());
        assert!(acquire.as_mut().poll(&mut task_cx).is_pending());
        ambient.set_cancel_requested(true);
        assert!(!request.is_cancel_requested());
        assert!(matches!(
            acquire.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Cancelled(_))
        ));
        assert_no_occupied_slots(&pool);
        assert_eq!(pool.stats().timeouts, 0);
    }

    #[test]
    fn production_context_stalled_validation_is_bounded_on_supported_runtimes() {
        const ACQUIRE_TIMEOUT_MS: u64 = 50;
        for current_thread in [true, false] {
            let builder = if current_thread {
                RuntimeBuilder::current_thread()
            } else {
                RuntimeBuilder::new().worker_threads(2)
            };
            let runtime = builder.build().expect("production runtime");
            let cx = runtime.request_cx_with_budget(Budget::INFINITE);
            let pool = Pool::new(PoolConfig::new(1).acquire_timeout(ACQUIRE_TIMEOUT_MS));
            let dropped = Arc::new(OnceCell::new());
            seed_idle(
                &pool,
                MockConnection::with_ping(1, MockPing::Pending)
                    .observe_drop(&pool, Arc::clone(&dropped)),
            );
            let factories = AtomicUsize::new(0);
            let started = std::time::Instant::now();
            let guarded = runtime.block_on(asupersync::time::timeout(
                asupersync::time::wall_now(),
                Duration::from_secs(2),
                pool.acquire(&cx, || async {
                    factories.fetch_add(1, Ordering::Relaxed);
                    Outcome::Ok(MockConnection::new(2))
                }),
            ));
            // A watchdog timeout is a failure. The pool's own finite deadline
            // must produce the inner Timeout and release its resource first.
            let outcome = guarded.expect("outer watchdog won: pool validation was not bounded");
            assert!(matches!(
                outcome,
                Outcome::Err(Error::Pool(PoolError {
                    kind: PoolErrorKind::Timeout,
                    ..
                }))
            ));
            assert!(started.elapsed() >= Duration::from_millis(ACQUIRE_TIMEOUT_MS));
            assert_eq!(factories.load(Ordering::Relaxed), 0);
            assert_drop_precedes_slot_release(&dropped);
            assert_no_occupied_slots(&pool);
            let stats = pool.stats();
            assert_eq!(stats.connections_created, 1);
            assert_eq!(stats.connections_closed, 1);
            assert_eq!(stats.acquires, 1);
            assert_eq!(stats.timeouts, 1);
        }
    }

    #[test]
    fn test_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.min_connections, 1);
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.idle_timeout_ms, 600_000);
        assert_eq!(config.acquire_timeout_ms, 30_000);
        assert_eq!(config.max_lifetime_ms, 1_800_000);
        assert!(config.test_on_checkout);
    }

    #[test]
    fn test_config_builder() {
        let config = PoolConfig::new(20)
            .min_connections(5)
            .idle_timeout(60_000)
            .acquire_timeout(5_000)
            .max_lifetime(300_000)
            .test_on_checkout(false);

        assert_eq!(config.min_connections, 5);
        assert_eq!(config.max_connections, 20);
        assert_eq!(config.idle_timeout_ms, 60_000);
        assert_eq!(config.acquire_timeout_ms, 5_000);
        assert_eq!(config.max_lifetime_ms, 300_000);
        assert!(!config.test_on_checkout);
    }

    #[test]
    fn test_config_clone() {
        let config = PoolConfig::new(15).min_connections(3);
        let cloned = config.clone();
        assert_eq!(config.max_connections, cloned.max_connections);
        assert_eq!(config.min_connections, cloned.min_connections);
    }

    #[test]
    fn test_stats_default() {
        let stats = PoolStats::default();
        assert_eq!(stats.total_connections, 0);
        assert_eq!(stats.idle_connections, 0);
        assert_eq!(stats.active_connections, 0);
        assert_eq!(stats.pending_requests, 0);
        assert_eq!(stats.connections_created, 0);
        assert_eq!(stats.connections_closed, 0);
        assert_eq!(stats.acquires, 0);
        assert_eq!(stats.timeouts, 0);
    }

    #[test]
    fn test_stats_clone() {
        let stats = PoolStats {
            total_connections: 5,
            acquires: 100,
            ..Default::default()
        };
        let cloned = stats.clone();
        assert_eq!(stats.total_connections, cloned.total_connections);
        assert_eq!(stats.acquires, cloned.acquires);
    }

    #[test]
    fn test_connection_meta_timing() {
        // Create a dummy type for testing
        struct DummyConn;

        let (clock, driver) = virtual_clock();
        let meta = ConnectionMeta::new(DummyConn, driver);
        let initial_age = meta.age();
        assert_eq!(initial_age, Duration::ZERO);

        // Advance virtual time instead of sleeping
        clock.advance(10_000_000); // 10ms

        // Age should reflect the advanced virtual time exactly
        assert!(meta.age() >= Duration::from_millis(10));
        assert!(meta.idle_time() >= Duration::from_millis(10));
    }

    #[test]
    fn test_connection_meta_touch() {
        struct DummyConn;

        let (clock, driver) = virtual_clock();
        let mut meta = ConnectionMeta::new(DummyConn, driver);

        // Build up some idle time on the virtual clock
        clock.advance(10_000_000); // 10ms
        let idle_before_touch = meta.idle_time();
        assert!(idle_before_touch >= Duration::from_millis(10));

        // Touch should reset idle time to exactly zero on the same clock
        meta.touch();
        let idle_after_touch = meta.idle_time();
        assert_eq!(idle_after_touch, Duration::ZERO);
        assert!(idle_after_touch < idle_before_touch);

        // Age is measured from creation and survives the touch
        assert!(meta.age() >= Duration::from_millis(10));
    }

    #[test]
    fn test_pool_new() {
        let config = PoolConfig::new(5);
        let pool: Pool<MockConnection> = Pool::new(config);

        // New pool should be empty
        assert_eq!(pool.idle_count(), 0);
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.total_count(), 0);
        assert!(!pool.is_closed());
        assert!(!pool.at_capacity());
    }

    #[test]
    fn test_pool_config() {
        let config = PoolConfig::new(7).min_connections(2);
        let pool: Pool<MockConnection> = Pool::new(config);

        let retrieved_config = pool.config();
        assert_eq!(retrieved_config.max_connections, 7);
        assert_eq!(retrieved_config.min_connections, 2);
    }

    #[test]
    fn test_pool_stats_initial() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));

        let stats = pool.stats();
        assert_eq!(stats.total_connections, 0);
        assert_eq!(stats.idle_connections, 0);
        assert_eq!(stats.active_connections, 0);
        assert_eq!(stats.pending_requests, 0);
        assert_eq!(stats.connections_created, 0);
        assert_eq!(stats.connections_closed, 0);
        assert_eq!(stats.acquires, 0);
        assert_eq!(stats.timeouts, 0);
    }

    #[test]
    fn test_pool_close() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));

        assert!(!pool.is_closed());
        pool.close();
        assert!(pool.is_closed());
    }

    #[test]
    fn test_close_and_drain_zero_active_completes_immediately() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .expect("build test runtime");
        let cx = Cx::for_testing();

        let outcome = runtime.block_on(pool.close_and_drain(&cx));

        assert!(matches!(outcome, Outcome::Ok(())));
        assert!(pool.is_closed());
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.total_count(), 0);
    }

    #[test]
    fn test_close_and_drain_surfaces_exact_idle_retirement_error() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(1));
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.idle.push_back(ConnectionMeta::new(
                MockConnection::with_failing_pool_close(1),
                test_clock(),
            ));
        }
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .expect("build test runtime");
        let cx = Cx::for_testing();

        let outcome = runtime.block_on(pool.close_and_drain(&cx));

        match outcome {
            Outcome::Err(Error::Custom(message)) => {
                assert_eq!(message, "mock pool close failure");
            }
            other => panic!("expected exact driver close error, got {other:?}"),
        }
        assert!(pool.is_closed());
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.total_count(), 0);

        let later_cx = Cx::for_testing();
        let later = runtime.block_on(pool.close_and_drain(&later_cx));
        match later {
            Outcome::Err(Error::Custom(message)) => {
                assert_eq!(
                    message,
                    "pool retirement failed: pool close-and-drain: mock pool close failure"
                );
            }
            other => panic!("expected persistent retirement error, got {other:?}"),
        }
    }

    #[test]
    fn test_checked_out_retirement_error_reaches_every_drainer() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(1));
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }
        let pooled = PooledConnection::new(
            ConnectionMeta::new(MockConnection::with_failing_pool_close(1), test_clock()),
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );
        let first_cx = Cx::for_testing();
        let second_cx = Cx::for_testing();
        let mut first_drain = Box::pin(pool.close_and_drain(&first_cx));
        let mut second_drain = Box::pin(pool.close_and_drain(&second_cx));
        let mut task_cx = Context::from_waker(Waker::noop());

        assert!(matches!(
            first_drain.as_mut().poll(&mut task_cx),
            Poll::Pending
        ));
        assert!(matches!(
            second_drain.as_mut().poll(&mut task_cx),
            Poll::Pending
        ));

        drop(pooled);

        let first_message = match first_drain.as_mut().poll(&mut task_cx) {
            Poll::Ready(Outcome::Err(Error::Custom(message))) => message,
            other => panic!("first drainer did not fail closed: {other:?}"),
        };
        let second_message = match second_drain.as_mut().poll(&mut task_cx) {
            Poll::Ready(Outcome::Err(Error::Custom(message))) => message,
            other => panic!("second drainer did not fail closed: {other:?}"),
        };
        assert_eq!(first_message, second_message);
        assert_eq!(
            first_message,
            "pool retirement failed: pooled connection drop closed pool: \
             mock pool close failure"
        );
        assert!(pool.is_closed());
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.total_count(), 0);
    }

    #[test]
    fn test_close_and_drain_waits_for_active_return_and_explicit_close() {
        let pool_close_calls = Arc::new(AtomicUsize::new(0));
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(1));
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }
        let pooled = PooledConnection::new(
            ConnectionMeta::new(
                MockConnection::with_pool_close_counter(1, Arc::clone(&pool_close_calls)),
                test_clock(),
            ),
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );
        let cx = Cx::for_testing();
        let mut drain = Box::pin(pool.close_and_drain(&cx));
        let mut task_cx = Context::from_waker(Waker::noop());

        assert!(matches!(drain.as_mut().poll(&mut task_cx), Poll::Pending));
        assert!(pool.is_closed());
        assert_eq!(pool.active_count(), 1);
        assert_eq!(pool_close_calls.load(Ordering::Relaxed), 0);

        drop(pooled);

        assert_eq!(pool_close_calls.load(Ordering::Relaxed), 1);
        assert!(matches!(
            drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Ok(()))
        ));
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.total_count(), 0);
    }

    #[test]
    fn test_close_and_drain_multiple_handles_and_drainers_share_final_wake() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(2));
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 2;
            inner.active_count = 2;
        }
        let first = PooledConnection::new(
            ConnectionMeta::new(MockConnection::new(1), test_clock()),
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );
        let second = PooledConnection::new(
            ConnectionMeta::new(MockConnection::new(2), test_clock()),
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );
        let first_cx = Cx::for_testing();
        let second_cx = Cx::for_testing();
        let mut first_drain = Box::pin(pool.close_and_drain(&first_cx));
        let mut second_drain = Box::pin(pool.close_and_drain(&second_cx));
        let mut task_cx = Context::from_waker(Waker::noop());

        assert!(matches!(
            first_drain.as_mut().poll(&mut task_cx),
            Poll::Pending
        ));
        assert!(matches!(
            second_drain.as_mut().poll(&mut task_cx),
            Poll::Pending
        ));

        drop(first);
        assert_eq!(pool.active_count(), 1);
        assert!(matches!(
            first_drain.as_mut().poll(&mut task_cx),
            Poll::Pending
        ));
        assert!(matches!(
            second_drain.as_mut().poll(&mut task_cx),
            Poll::Pending
        ));

        drop(second);
        assert_eq!(pool.active_count(), 0);
        assert!(matches!(
            first_drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Ok(()))
        ));
        assert!(matches!(
            second_drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Ok(()))
        ));
    }

    #[test]
    fn test_close_and_drain_cancellation_keeps_pool_closed() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(1));
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }
        let pooled = PooledConnection::new(
            ConnectionMeta::new(MockConnection::new(1), test_clock()),
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );
        let cx = Cx::for_testing();
        let mut drain = Box::pin(pool.close_and_drain(&cx));
        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&wake_counter));
        let mut task_cx = Context::from_waker(&waker);

        assert!(matches!(drain.as_mut().poll(&mut task_cx), Poll::Pending));
        cx.set_cancel_requested(true);
        assert!(
            wake_counter.wakes.load(Ordering::Relaxed) > 0,
            "Cx cancellation must wake the registered close-and-drain waiter"
        );
        assert!(matches!(
            drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Cancelled(_))
        ));
        assert!(pool.is_closed());
        assert_eq!(pool.active_count(), 1);

        drop(drain);
        let resume_cx = Cx::for_testing();
        let mut resumed_drain = Box::pin(pool.close_and_drain(&resume_cx));
        let mut resumed_task_cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            resumed_drain.as_mut().poll(&mut resumed_task_cx),
            Poll::Pending
        ));
        assert!(pool.is_closed());

        drop(pooled);
        assert!(matches!(
            resumed_drain.as_mut().poll(&mut resumed_task_cx),
            Poll::Ready(Outcome::Ok(()))
        ));
        assert!(pool.is_closed());
        assert_eq!(pool.active_count(), 0);
    }

    #[test]
    fn test_close_and_drain_expired_deadline_keeps_pool_closed() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(1));
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }
        let pooled = PooledConnection::new(
            ConnectionMeta::new(MockConnection::new(1), test_clock()),
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );
        let cx = Cx::for_testing_with_budget(Budget::new().with_deadline(Time::ZERO));
        let mut drain = Box::pin(pool.close_and_drain(&cx));
        let mut task_cx = Context::from_waker(Waker::noop());

        assert!(matches!(
            drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Cancelled(_))
        ));
        assert!(pool.is_closed());
        assert_eq!(pool.active_count(), 1);

        drop(drain);
        drop(pooled);
        assert_eq!(pool.active_count(), 0);
    }

    #[test]
    fn production_drain_deadline_wakes_without_reclaiming_a_held_lease() {
        const DRAIN_TIMEOUT_MS: u64 = 50;
        for current_thread in [true, false] {
            let builder = if current_thread {
                RuntimeBuilder::current_thread()
            } else {
                RuntimeBuilder::new().worker_threads(2)
            };
            let runtime = builder.build().expect("production runtime");
            let pool = Pool::new(PoolConfig::new(1));
            let holder_cx = runtime.request_cx_with_budget(Budget::INFINITE);
            let dropped = Arc::new(OnceCell::new());
            let lease = runtime
                .block_on(pool.acquire(&holder_cx, || async {
                    Outcome::Ok(MockConnection::new(1).observe_drop(&pool, Arc::clone(&dropped)))
                }))
                .expect("borrower acquires before shutdown");
            let started = std::time::Instant::now();
            let deadline = asupersync::time::wall_now() + Duration::from_millis(DRAIN_TIMEOUT_MS);
            let drain_cx = runtime.request_cx_with_budget(Budget::new().with_deadline(deadline));
            let mut drain = Box::pin(pool.close_and_drain(&drain_cx));
            let guarded = runtime.block_on(asupersync::time::timeout(
                asupersync::time::wall_now(),
                Duration::from_secs(2),
                drain.as_mut(),
            ));
            let outcome = guarded.expect("watchdog won: drain deadline did not wake the caller");
            match outcome {
                Outcome::Cancelled(reason) => {
                    assert_eq!(reason.kind, asupersync::CancelKind::Deadline);
                }
                other => panic!("expected deadline cancellation, got {other:?}"),
            }
            let elapsed = started.elapsed();
            assert!(elapsed >= Duration::from_millis(DRAIN_TIMEOUT_MS));
            assert!(
                elapsed < Duration::from_secs(1),
                "drain only noticed the deadline after an unrelated wake: {elapsed:?}"
            );
            assert!(pool.is_closed());
            assert!(dropped.get().is_none(), "borrower still owns the resource");
            assert!(!holder_cx.is_cancel_requested());
            let stats = pool.stats();
            assert_eq!(stats.active_connections, 1);
            assert_eq!(stats.total_connections, 1);
            assert_eq!(stats.idle_connections, 0);
            assert_eq!(stats.pending_requests, 0);
            assert_eq!(stats.connections_created, 1);
            assert_eq!(stats.connections_closed, 0);
            assert_eq!(stats.acquires, 1);
            assert_eq!(stats.timeouts, 0, "drain is not an acquire timeout");

            drop(lease);
            assert_drop_precedes_slot_release(&dropped);
            let later_cx = runtime.request_cx_with_budget(Budget::INFINITE);
            runtime
                .block_on(pool.close_and_drain(&later_cx))
                .expect("later caller resumes draining after borrower returns");
            assert_no_occupied_slots(&pool);
            assert_eq!(pool.stats().connections_closed, 1);
            assert_eq!(pool.stats().timeouts, 0);
        }
    }

    #[test]
    fn production_drain_deadline_drops_stalled_close_and_all_remaining_idle_resources() {
        const DRAIN_TIMEOUT_MS: u64 = 50;
        for current_thread in [true, false] {
            let builder = if current_thread {
                RuntimeBuilder::current_thread()
            } else {
                RuntimeBuilder::new().worker_threads(2)
            };
            let runtime = builder.build().expect("production runtime");
            let pool = Pool::new(PoolConfig::new(3));
            let holder_cx = runtime.request_cx_with_budget(Budget::INFINITE);
            let closes = Arc::new(AtomicUsize::new(0));
            let mut dropped = Vec::new();
            let mut leases = Vec::new();
            for id in 1..=3 {
                let observed = Arc::new(OnceCell::new());
                let lease = runtime
                    .block_on(pool.acquire(&holder_cx, || async {
                        Outcome::Ok(
                            MockConnection::with_pending_pool_close(id, Arc::clone(&closes))
                                .observe_drop(&pool, Arc::clone(&observed)),
                        )
                    }))
                    .expect("create distinct connection before shutdown");
                leases.push(lease);
                dropped.push(observed);
            }
            drop(leases);
            assert_eq!(pool.stats().idle_connections, 3);
            let started = std::time::Instant::now();
            let deadline = asupersync::time::wall_now() + Duration::from_millis(DRAIN_TIMEOUT_MS);
            let drain_cx = runtime.request_cx_with_budget(Budget::new().with_deadline(deadline));
            let mut drain = Box::pin(pool.close_and_drain(&drain_cx));
            let guarded = runtime.block_on(asupersync::time::timeout(
                asupersync::time::wall_now(),
                Duration::from_secs(2),
                drain.as_mut(),
            ));
            let outcome = guarded.expect("watchdog won: pending retirement ignored drain deadline");
            match outcome {
                Outcome::Cancelled(reason) => {
                    assert_eq!(reason.kind, asupersync::CancelKind::Deadline);
                }
                other => panic!("expected deadline cancellation, got {other:?}"),
            }
            let elapsed = started.elapsed();
            assert!(elapsed >= Duration::from_millis(DRAIN_TIMEOUT_MS));
            assert!(
                elapsed < Duration::from_secs(1),
                "retirement only noticed the deadline after an unrelated wake: {elapsed:?}"
            );
            // Keep the completed future alive: Ready itself must release
            // every owned resource, including guards not yet being closed.
            let mut active_at_drop = Vec::new();
            for observed in dropped {
                let stats = observed
                    .get()
                    .expect("every idle resource was destroyed before drain returned")
                    .as_ref()
                    .expect("resource destruction must not hold the pool mutex");
                assert_eq!(stats.total_connections, stats.active_connections);
                assert_eq!(stats.idle_connections, 0);
                active_at_drop.push(stats.active_connections);
            }
            active_at_drop.sort_unstable();
            assert_eq!(active_at_drop, vec![1, 2, 3]);
            assert_eq!(closes.load(Ordering::Relaxed), 1);
            assert_no_occupied_slots(&pool);
            assert!(pool.is_closed());
            let stats = pool.stats();
            assert_eq!(stats.connections_created, 3);
            assert_eq!(stats.connections_closed, 3);
            assert_eq!(stats.acquires, 3);
            assert_eq!(stats.timeouts, 0, "drain is not an acquire timeout");
            let later_cx = runtime.request_cx_with_budget(Budget::INFINITE);
            runtime
                .block_on(pool.close_and_drain(&later_cx))
                .expect("later caller observes complete resource cleanup");
            assert_no_occupied_slots(&pool);
            assert_eq!(pool.stats().timeouts, 0);
        }
    }

    #[test]
    fn test_dropped_in_flight_factory_releases_reserved_slot() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(1));
        let cx = Cx::for_testing();
        let mut acquire = Box::pin(pool.acquire(&cx, || {
            std::future::pending::<Outcome<MockConnection, Error>>()
        }));
        let mut task_cx = Context::from_waker(Waker::noop());

        assert!(matches!(acquire.as_mut().poll(&mut task_cx), Poll::Pending));
        assert_eq!(pool.active_count(), 1);
        assert_eq!(pool.total_count(), 1);

        drop(acquire);

        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.total_count(), 0);
    }

    #[test]
    fn test_dropped_multi_expired_idle_close_releases_all_retirement_slots() {
        let pool_close_calls = Arc::new(AtomicUsize::new(0));
        let pool: Pool<MockConnection> =
            Pool::new(PoolConfig::new(3).max_lifetime(1).test_on_checkout(false));
        let mut first_expired = ConnectionMeta::new(
            MockConnection::with_pending_pool_close(1, Arc::clone(&pool_close_calls)),
            pool.shared.clock.clone(),
        );
        backdate(&mut first_expired, Duration::from_secs(1));
        let mut second_expired = ConnectionMeta::new(
            MockConnection::with_pool_close_counter(2, Arc::clone(&pool_close_calls)),
            pool.shared.clock.clone(),
        );
        backdate(&mut second_expired, Duration::from_secs(1));
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 2;
            inner.idle.push_back(first_expired);
            inner.idle.push_back(second_expired);
        }
        let cx = Cx::for_testing();
        let mut acquire =
            Box::pin(pool.acquire(&cx, || async { Outcome::Ok(MockConnection::new(3)) }));
        let mut task_cx = Context::from_waker(Waker::noop());

        assert!(matches!(acquire.as_mut().poll(&mut task_cx), Poll::Pending));
        assert_eq!(pool_close_calls.load(Ordering::Relaxed), 1);
        assert_eq!(pool.active_count(), 2);
        assert_eq!(pool.total_count(), 2);

        let drain_cx = Cx::for_testing();
        let mut drain = Box::pin(pool.close_and_drain(&drain_cx));
        assert!(matches!(drain.as_mut().poll(&mut task_cx), Poll::Pending));

        drop(acquire);
        assert!(matches!(
            drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Ok(()))
        ));
        assert!(pool.is_closed());
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.total_count(), 0);
    }

    #[test]
    fn test_dropped_pending_idle_drain_releases_resource_for_other_drainer() {
        let pool_close_calls = Arc::new(AtomicUsize::new(0));
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(1));
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.idle.push_back(ConnectionMeta::new(
                MockConnection::with_pending_pool_close(1, Arc::clone(&pool_close_calls)),
                test_clock(),
            ));
        }
        let first_cx = Cx::for_testing();
        let second_cx = Cx::for_testing();
        let mut first_drain = Box::pin(pool.close_and_drain(&first_cx));
        let mut second_drain = Box::pin(pool.close_and_drain(&second_cx));
        let mut task_cx = Context::from_waker(Waker::noop());

        assert!(matches!(
            first_drain.as_mut().poll(&mut task_cx),
            Poll::Pending
        ));
        assert!(pool.is_closed());
        assert_eq!(pool_close_calls.load(Ordering::Relaxed), 1);
        assert_eq!(pool.active_count(), 1);
        assert_eq!(pool.total_count(), 1);

        assert!(matches!(
            second_drain.as_mut().poll(&mut task_cx),
            Poll::Pending
        ));

        first_cx.set_cancel_requested(true);
        assert!(matches!(
            first_drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Cancelled(_))
        ));
        drop(first_drain);

        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.total_count(), 0);
        assert!(matches!(
            second_drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Ok(()))
        ));
        assert!(pool.is_closed());
    }

    #[test]
    fn test_dropped_validation_close_releases_armed_active_slot() {
        let pool_close_calls = Arc::new(AtomicUsize::new(0));
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(1).test_on_checkout(true));
        let failed = MockConnection::with_pending_pool_close(1, Arc::clone(&pool_close_calls));
        failed.ping_should_fail.store(true, Ordering::Relaxed);
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner
                .idle
                .push_back(ConnectionMeta::new(failed, test_clock()));
        }
        let cx = Cx::for_testing();
        let mut acquire =
            Box::pin(pool.acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) }));
        let mut task_cx = Context::from_waker(Waker::noop());

        assert!(matches!(acquire.as_mut().poll(&mut task_cx), Poll::Pending));
        assert_eq!(pool_close_calls.load(Ordering::Relaxed), 1);
        assert_eq!(pool.active_count(), 1);
        assert_eq!(pool.total_count(), 1);

        pool.close();
        drop(acquire);

        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.total_count(), 0);
        let drain_cx = Cx::for_testing();
        let mut drain = Box::pin(pool.close_and_drain(&drain_cx));
        assert!(matches!(
            drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Ok(()))
        ));
    }

    #[test]
    fn test_in_flight_factory_cannot_publish_after_close() {
        let pool_close_calls = Arc::new(AtomicUsize::new(0));
        let factory_ready = Arc::new(AtomicBool::new(false));
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(1));
        let cx = Cx::for_testing();
        let mut acquire = Box::pin(pool.acquire(&cx, || GatedFactory {
            ready: Arc::clone(&factory_ready),
            conn: Some(MockConnection::with_pool_close_counter(
                1,
                Arc::clone(&pool_close_calls),
            )),
        }));
        let mut task_cx = Context::from_waker(Waker::noop());

        assert!(matches!(acquire.as_mut().poll(&mut task_cx), Poll::Pending));
        assert_eq!(pool.active_count(), 1);

        pool.close();
        factory_ready.store(true, Ordering::Release);

        assert!(matches!(
            acquire.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Err(Error::Pool(PoolError {
                kind: PoolErrorKind::Closed,
                ..
            })))
        ));
        assert_eq!(pool_close_calls.load(Ordering::Relaxed), 1);
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.total_count(), 0);
    }

    #[test]
    fn test_close_wakes_blocked_acquirer_to_observe_closed_state() {
        use std::sync::mpsc;
        use std::thread;

        let pool = Arc::new(Pool::<MockConnection>::new(PoolConfig::new(1)));
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }
        let pooled = PooledConnection::new(
            ConnectionMeta::new(MockConnection::new(1), test_clock()),
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );
        let waiting_pool = Arc::clone(&pool);
        let (started_tx, started_rx) = mpsc::sync_channel(0);
        let waiter = thread::spawn(move || {
            let runtime = RuntimeBuilder::current_thread()
                .build()
                .expect("build waiter runtime");
            let cx = Cx::for_testing();
            started_tx.send(()).expect("signal waiter start");
            matches!(
                runtime.block_on(
                    waiting_pool.acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) })
                ),
                Outcome::Err(Error::Pool(PoolError {
                    kind: PoolErrorKind::Closed,
                    ..
                }))
            )
        });
        started_rx.recv().expect("waiter thread should start");

        let mut observed_waiter = false;
        for _ in 0..100_000 {
            if pool.stats().pending_requests == 1 {
                observed_waiter = true;
                break;
            }
            thread::yield_now();
        }

        pool.close();
        let observed_closed = waiter.join().expect("waiter thread should not panic");
        drop(pooled);

        assert!(
            observed_waiter,
            "acquirer never registered as a pool waiter"
        );
        assert!(
            observed_closed,
            "blocked acquirer did not observe pool close"
        );
    }

    #[test]
    fn test_pool_close_routes_through_close_for_pool() {
        let pool_close_calls = Arc::new(AtomicUsize::new(0));
        let pool_lock_was_free = Arc::new(AtomicBool::new(false));
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(2));

        // Seed one idle connection whose `close_for_pool` override records
        // the call, proving pool teardown uses the driver's pool-close path
        // rather than the ordinary `close`.
        {
            let mut inner = pool
                .shared
                .inner
                .lock()
                .expect("pool mutex should not be poisoned");
            inner.total_count = 1;
            inner.idle.push_back(ConnectionMeta::new(
                MockConnection::with_pool_close_probe(
                    1,
                    Arc::clone(&pool_close_calls),
                    Arc::downgrade(&pool.shared),
                    Arc::clone(&pool_lock_was_free),
                ),
                test_clock(),
            ));
        }

        pool.close();

        assert!(pool.is_closed());
        assert_eq!(pool_close_calls.load(Ordering::Relaxed), 1);
        assert!(pool_lock_was_free.load(Ordering::Relaxed));
    }

    #[test]
    fn synchronous_retirement_bounds_closed_lease_return() {
        const GRACE_MS: u64 = 50;
        let pool = Pool::new(PoolConfig::new(1).acquire_timeout(GRACE_MS));
        let (gate, started) = PoolCloseGate::new();
        let closes = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(OnceCell::new());
        let mut conn = MockConnection::with_pool_close_counter(1, Arc::clone(&closes));
        conn.pool_close_gate = Some(Arc::clone(&gate));
        let lease = acquire_mock(&pool, conn.observe_drop(&pool, Arc::clone(&dropped)));
        pool.close();
        let drain_cx = Cx::for_testing();
        let wake = DrainFailureWake::new(&pool);
        let waker = Waker::from(Arc::clone(&wake));
        let mut task_cx = Context::from_waker(&waker);
        let mut drain = Box::pin(pool.close_and_drain(&drain_cx));
        let waited = drain.as_mut().poll(&mut task_cx).is_pending();

        let (bounded, elapsed) =
            run_gated_retirement(&gate, &started, GateRelease::AfterWatchdog, move || {
                drop(lease);
            });

        assert!(
            bounded,
            "lease return ignored its configured grace and needed gate release: {elapsed:?}"
        );
        assert!(elapsed >= Duration::from_millis(GRACE_MS));
        assert_eq!(closes.load(Ordering::Relaxed), 1);
        assert_eq!(gate.completions.load(Ordering::Relaxed), 0);
        assert_drop_precedes_slot_release(&dropped);
        assert_no_occupied_slots(&pool);
        let stats = pool.stats();
        assert_eq!(stats.connections_created, 1);
        assert_eq!(stats.connections_closed, 1);
        assert_eq!(stats.acquires, 1);
        assert_eq!(stats.timeouts, 0);
        assert!(waited);
        assert_eq!(
            wake.observed.get(),
            Some(&true),
            "the final-slot wake must already expose retirement failure"
        );
        assert!(matches!(
            drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Err(_))
        ));
        assert_sticky_retirement_failure(&pool);
    }

    #[test]
    fn synchronous_retirement_shares_one_grace_across_idle_batch() {
        const GRACE_MS: u64 = 50;
        let pool = Arc::new(Pool::new(PoolConfig::new(3).acquire_timeout(GRACE_MS)));
        let (gate, started) = PoolCloseGate::new();
        let closes = Arc::new(AtomicUsize::new(0));
        let mut dropped = Vec::new();
        let mut leases = Vec::new();
        for id in 1..=3 {
            let observed = Arc::new(OnceCell::new());
            let mut conn = MockConnection::with_pool_close_counter(id, Arc::clone(&closes));
            conn.pool_close_gate = Some(Arc::clone(&gate));
            leases.push(acquire_mock(
                &pool,
                conn.observe_drop(&pool, Arc::clone(&observed)),
            ));
            dropped.push(observed);
        }
        drop(leases);
        let retiring_pool = Arc::clone(&pool);
        let (bounded, elapsed) =
            run_gated_retirement(&gate, &started, GateRelease::AfterWatchdog, move || {
                retiring_pool.clear_idle();
            });

        assert!(
            bounded,
            "idle retirement needed the watchdog's gate release: {elapsed:?}"
        );
        assert!(elapsed >= Duration::from_millis(GRACE_MS));
        assert_eq!(
            closes.load(Ordering::Relaxed),
            1,
            "expiry must stop the batch before another close hook starts"
        );
        assert_eq!(gate.completions.load(Ordering::Relaxed), 0);
        let mut active_at_drop = Vec::new();
        for observed in dropped {
            let stats = observed
                .get()
                .expect("every retired resource must be destroyed before return")
                .as_ref()
                .expect("resource destruction must not hold the pool mutex");
            assert_eq!(stats.total_connections, stats.active_connections);
            assert_eq!(stats.idle_connections, 0);
            active_at_drop.push(stats.active_connections);
        }
        active_at_drop.sort_unstable();
        assert_eq!(active_at_drop, vec![1, 2, 3]);
        assert_no_occupied_slots(&pool);
        assert!(!pool.is_closed(), "clear_idle preserves admission");
        let stats = pool.stats();
        assert_eq!(stats.connections_created, 3);
        assert_eq!(stats.connections_closed, 3);
        assert_eq!(stats.acquires, 3);
        assert_eq!(stats.timeouts, 0);

        let replacement = acquire_mock(&pool, MockConnection::new(4));
        assert_eq!(replacement.id, 4, "retirement must restore usable capacity");
        drop(replacement);
        assert_sticky_retirement_failure(&pool);
        assert_eq!(pool.stats().connections_created, 4);
        assert_eq!(pool.stats().connections_closed, 4);
    }

    #[test]
    fn synchronous_retirement_bounds_expired_lease_on_a_virtual_pool_clock() {
        const GRACE_MS: u64 = 50;
        let (mut runtime, pool, region) =
            lab_pool(PoolConfig::new(1).max_lifetime(1).acquire_timeout(GRACE_MS));
        let (gate, started) = PoolCloseGate::new();
        let closes = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(OnceCell::new());
        let held = Arc::new(Mutex::new(None));
        let record = Arc::clone(&held);
        let task_pool = Arc::clone(&pool);
        let task_gate = Arc::clone(&gate);
        let task_closes = Arc::clone(&closes);
        let task_dropped = Arc::clone(&dropped);
        let (task, _) = runtime
            .state
            .create_task(region, Budget::INFINITE, async move {
                let cx = Cx::current().expect("lab task has a context");
                let lease = task_pool
                    .acquire(&cx, || async {
                        let mut conn =
                            MockConnection::with_pool_close_counter(1, Arc::clone(&task_closes));
                        conn.pool_close_gate = Some(Arc::clone(&task_gate));
                        Outcome::Ok(conn.observe_drop(&task_pool, Arc::clone(&task_dropped)))
                    })
                    .await
                    .expect("create lease on the lab clock");
                *record.lock().unwrap() = Some(lease);
            })
            .expect("spawn lease creation");
        runtime.scheduler.lock().schedule(task, 0);
        runtime.run_with_auto_advance();
        runtime.advance_time(2_000_000);
        let lease = held.lock().unwrap().take().expect("lab task created lease");
        let age = lease.age();
        let (bounded, elapsed) =
            run_gated_retirement(&gate, &started, GateRelease::AfterWatchdog, move || {
                drop(lease);
            });

        assert!(
            bounded,
            "expired return must use a live wall-clock grace: {elapsed:?}"
        );
        assert_eq!(age, Duration::from_millis(2));
        assert!(elapsed >= Duration::from_millis(GRACE_MS));
        assert_eq!(closes.load(Ordering::Relaxed), 1);
        assert_eq!(gate.completions.load(Ordering::Relaxed), 0);
        assert_drop_precedes_slot_release(&dropped);
        assert_no_occupied_slots(&pool);
        assert!(!pool.is_closed());
        assert_eq!(pool.stats().connections_closed, 1);
        assert_sticky_retirement_failure(&pool);
    }

    #[test]
    fn synchronous_retirement_remembers_grace_after_pool_is_gone() {
        const GRACE_MS: u64 = 50;
        let pool = Pool::new(PoolConfig::new(1).acquire_timeout(GRACE_MS));
        let (gate, started) = PoolCloseGate::new();
        let closes = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(OnceCell::new());
        let mut conn = MockConnection::with_pool_close_counter(1, Arc::clone(&closes));
        conn.pool_close_gate = Some(Arc::clone(&gate));
        let lease = acquire_mock(&pool, conn.observe_drop(&pool, Arc::clone(&dropped)));
        let weak_pool = Arc::downgrade(&pool.shared);
        drop(pool);
        let pool_was_gone = weak_pool.upgrade().is_none();
        let (bounded, elapsed) =
            run_gated_retirement(&gate, &started, GateRelease::AfterWatchdog, move || {
                drop(lease);
            });

        assert!(
            bounded,
            "a surviving lease lost its configured retirement grace: {elapsed:?}"
        );
        assert!(pool_was_gone, "fixture must outlive the entire shared pool");
        assert!(elapsed >= Duration::from_millis(GRACE_MS));
        assert_eq!(closes.load(Ordering::Relaxed), 1);
        assert_eq!(gate.completions.load(Ordering::Relaxed), 0);
        assert!(
            matches!(dropped.get(), Some(None)),
            "the connection must be destroyed even without accounting state"
        );
    }

    #[test]
    fn synchronous_retirement_preserves_graceful_close_after_pending() {
        let pool = Arc::new(Pool::new(PoolConfig::new(1).acquire_timeout(2_000)));
        let (gate, started) = PoolCloseGate::new();
        let closes = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(OnceCell::new());
        let mut conn = MockConnection::with_pool_close_counter(1, Arc::clone(&closes));
        conn.pool_close_gate = Some(Arc::clone(&gate));
        seed_idle(&pool, conn.observe_drop(&pool, Arc::clone(&dropped)));
        let retiring_pool = Arc::clone(&pool);
        let (completed_before_release, _) =
            run_gated_retirement(&gate, &started, GateRelease::AfterStart, move || {
                retiring_pool.close();
            });

        assert!(!completed_before_release);
        assert_eq!(closes.load(Ordering::Relaxed), 1);
        assert_eq!(gate.completions.load(Ordering::Relaxed), 1);
        assert_drop_precedes_slot_release(&dropped);
        assert_no_occupied_slots(&pool);
        assert!(pool.shared.retirement_failure_error().is_none());
        let cx = Cx::for_testing();
        let mut drain = Box::pin(pool.close_and_drain(&cx));
        let mut task_cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Ok(()))
        ));
        assert_eq!(pool.stats().connections_closed, 1);
        assert_eq!(pool.stats().timeouts, 0);
    }

    #[test]
    fn synchronous_retirement_panic_does_not_strand_lease_accounting() {
        let pool = Pool::new(PoolConfig::new(1).acquire_timeout(50));
        let closes = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(OnceCell::new());
        let mut conn = MockConnection::with_pool_close_counter(1, Arc::clone(&closes));
        conn.pool_close_panics = true;
        let lease = acquire_mock(&pool, conn.observe_drop(&pool, Arc::clone(&dropped)));
        pool.close();
        let drain_cx = Cx::for_testing();
        let wake = DrainFailureWake::new(&pool);
        let waker = Waker::from(Arc::clone(&wake));
        let mut task_cx = Context::from_waker(&waker);
        let mut drain = Box::pin(pool.close_and_drain(&drain_cx));
        let waited = drain.as_mut().poll(&mut task_cx).is_pending();
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(lease)));

        assert!(
            unwound.is_err(),
            "fixture must actually unwind the close hook"
        );
        assert_eq!(closes.load(Ordering::Relaxed), 1);
        assert_drop_precedes_slot_release(&dropped);
        assert_no_occupied_slots(&pool);
        assert_eq!(pool.stats().connections_closed, 1);
        assert_eq!(pool.stats().timeouts, 0);
        assert!(pool.shared.active_drained.get().is_some());
        assert!(!pool.shared.inner.is_poisoned());
        assert!(waited);
        assert_eq!(
            wake.observed.get(),
            Some(&true),
            "unwind must record failure before waking the blocked drainer"
        );
        assert!(matches!(
            drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Err(_))
        ));
        assert_sticky_retirement_failure(&pool);
    }

    #[test]
    fn synchronous_retirement_during_holder_unwind_skips_driver_hook() {
        let pool = Pool::new(PoolConfig::new(1).acquire_timeout(50));
        let closes = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(OnceCell::new());
        let conn = MockConnection::with_pool_close_counter(1, Arc::clone(&closes));
        let lease = acquire_mock(&pool, conn.observe_drop(&pool, Arc::clone(&dropped)));
        pool.close();
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _lease = lease;
            panic!("holder panic before retirement");
        }));

        let payload = unwound.expect_err("the original holder panic must propagate");
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"holder panic before retirement")
        );
        assert_eq!(
            closes.load(Ordering::Relaxed),
            0,
            "do not invoke a potentially panicking driver hook during unwind"
        );
        assert_drop_precedes_slot_release(&dropped);
        assert_no_occupied_slots(&pool);
        assert_eq!(pool.stats().connections_closed, 1);
        assert_sticky_retirement_failure(&pool);
    }

    #[test]
    fn test_expired_idle_connection_routes_through_close_for_pool() {
        let pool_close_calls = Arc::new(AtomicUsize::new(0));
        let pool: Pool<MockConnection> =
            Pool::new(PoolConfig::new(2).max_lifetime(1).test_on_checkout(false));
        let mut expired = ConnectionMeta::new(
            MockConnection::with_pool_close_counter(1, Arc::clone(&pool_close_calls)),
            pool.shared.clock.clone(),
        );
        backdate(&mut expired, Duration::from_secs(1));
        {
            let mut inner = pool
                .shared
                .inner
                .lock()
                .expect("pool mutex should not be poisoned");
            inner.total_count = 1;
            inner.idle.push_back(expired);
        }

        let runtime = RuntimeBuilder::current_thread()
            .build()
            .expect("build test runtime");
        let cx = Cx::for_testing();
        let acquired =
            runtime.block_on(pool.acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) }));

        assert!(matches!(acquired, Outcome::Ok(_)));
        assert_eq!(pool_close_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_failed_validation_routes_through_close_for_pool() {
        let pool_close_calls = Arc::new(AtomicUsize::new(0));
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(2).test_on_checkout(true));
        let failed = MockConnection::with_pool_close_counter(1, Arc::clone(&pool_close_calls));
        failed.ping_should_fail.store(true, Ordering::Relaxed);
        {
            let mut inner = pool
                .shared
                .inner
                .lock()
                .expect("pool mutex should not be poisoned");
            inner.total_count = 1;
            inner
                .idle
                .push_back(ConnectionMeta::new(failed, test_clock()));
        }

        let runtime = RuntimeBuilder::current_thread()
            .build()
            .expect("build test runtime");
        let cx = Cx::for_testing();
        let acquired =
            runtime.block_on(pool.acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) }));

        // The dead idle connection is closed and the acquire moves on to a
        // fresh one from the factory instead of failing (until 2026-09 it
        // returned an error and left the caller to retry).
        assert!(
            matches!(acquired, Outcome::Ok(_)),
            "acquire replaces the dead idle connection"
        );
        assert_eq!(pool_close_calls.load(Ordering::Relaxed), 1);
        let stats = pool.stats();
        assert_eq!(stats.connections_closed, 1);
        assert_eq!(stats.connections_created, 1);
        assert_eq!(stats.total_connections, 1);
        drop(acquired);
        assert_eq!(pool.stats().idle_connections, 1);
    }

    #[test]
    fn waiters_yield_instead_of_blocking_a_single_threaded_runtime() {
        // Four tasks contend on a pool of one under one current-thread
        // runtime: a waiter must yield so the holder can run and return its
        // lease. With the old blocking `Condvar` wait the holder never ran
        // and every waiter timed out (found by the e2e fan-out on PostgreSQL).
        let pool: Arc<Pool<MockConnection>> = Arc::new(Pool::new(
            PoolConfig::new(1)
                .min_connections(0)
                .acquire_timeout(2_000)
                .test_on_checkout(false),
        ));
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .expect("build test runtime");
        let handle = runtime.handle();
        let tasks: Vec<_> = (0..4)
            .map(|i| {
                let pool = Arc::clone(&pool);
                handle.spawn(async move {
                    let cx = Cx::for_testing();
                    match pool
                        .acquire(&cx, || async { Outcome::Ok(MockConnection::new(1)) })
                        .await
                    {
                        Outcome::Ok(lease) => {
                            // Hold the lease across a yield so the others queue.
                            asupersync::time::sleep(cx.now(), Duration::from_millis(20)).await;
                            drop(lease);
                            Ok(i)
                        }
                        Outcome::Err(e) => Err(e.to_string()),
                        Outcome::Cancelled(_) | Outcome::Panicked(_) => Err("cancelled".into()),
                    }
                })
            })
            .collect();
        let results = runtime.block_on(async {
            let mut results = Vec::new();
            for task in tasks {
                results.push(task.await);
            }
            results
        });
        assert!(results.iter().all(Result::is_ok), "{results:?}");
        let stats = pool.stats();
        assert_eq!(stats.timeouts, 0, "{stats:?}");
        assert_eq!(stats.connections_created, 1, "{stats:?}");
        assert_eq!(stats.acquires, 4, "{stats:?}");
        assert_eq!(stats.active_connections, 0, "{stats:?}");
    }

    #[test]
    fn test_pool_inner_can_create_new() {
        let mut inner = PoolInner::<MockConnection>::new(PoolConfig::new(3));

        // Initially can create new
        assert!(inner.can_create_new());

        // At capacity
        inner.total_count = 3;
        assert!(!inner.can_create_new());

        // Below capacity again
        inner.total_count = 2;
        assert!(inner.can_create_new());

        // Closed pool
        inner.closed = true;
        assert!(!inner.can_create_new());
    }

    #[test]
    fn test_pool_inner_stats() {
        let mut inner = PoolInner::<MockConnection>::new(PoolConfig::new(10));

        inner.total_count = 5;
        inner.active_count = 3;
        inner.waiter_count = 2;
        inner
            .idle
            .push_back(ConnectionMeta::new(MockConnection::new(1), test_clock()));
        inner
            .idle
            .push_back(ConnectionMeta::new(MockConnection::new(2), test_clock()));

        let stats = inner.stats();
        assert_eq!(stats.total_connections, 5);
        assert_eq!(stats.idle_connections, 2);
        assert_eq!(stats.active_connections, 3);
        assert_eq!(stats.pending_requests, 2);
    }

    #[test]
    fn test_pooled_connection_age_and_idle_time() {
        let (clock, driver) = virtual_clock();
        let pool: Pool<MockConnection> =
            Pool::with_timer_driver(PoolConfig::new(5).test_on_checkout(false), driver);

        // Properly initialize pool state as if acquire happened
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }

        let meta = ConnectionMeta::new(MockConnection::new(1), pool.shared.clock.clone());
        let pooled = PooledConnection::new(
            meta,
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );

        // Age starts at exactly zero on the virtual clock
        assert_eq!(pooled.age(), Duration::ZERO);
        assert_eq!(pooled.idle_time(), Duration::ZERO);

        clock.advance(5_000_000); // 5ms
        assert!(pooled.age() >= Duration::from_millis(5));
        assert!(pooled.idle_time() >= Duration::from_millis(5));
    }

    #[test]
    fn test_pooled_connection_detach() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));

        // Manually add a connection to simulate acquire
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }

        let meta = ConnectionMeta::new(MockConnection::new(42), test_clock());
        let pooled = PooledConnection::new(
            meta,
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );

        // Verify counts before detach
        assert_eq!(pool.total_count(), 1);
        assert_eq!(pool.active_count(), 1);

        // Detach returns the connection
        let conn = pooled.detach();
        assert_eq!(conn.id, 42);

        // After detach, counts should be decremented
        assert_eq!(pool.total_count(), 0);
        assert_eq!(pool.active_count(), 0);

        // connections_closed should be incremented
        let stats = pool.stats();
        assert_eq!(stats.connections_closed, 1);
    }

    #[test]
    fn test_pooled_connection_drop_returns_to_pool() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));

        // Manually set up pool state as if we acquired a connection
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }

        let meta = ConnectionMeta::new(MockConnection::new(1), test_clock());
        let pooled = PooledConnection::new(
            meta,
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );

        // While held, active=1, idle=0
        assert_eq!(pool.active_count(), 1);
        assert_eq!(pool.idle_count(), 0);

        // Drop the connection
        drop(pooled);

        // After drop, active=0, idle=1 (returned to pool)
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.idle_count(), 1);
        assert_eq!(pool.total_count(), 1); // Total unchanged
    }

    #[test]
    fn test_pooled_connection_drop_when_pool_closed() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));

        // Set up pool state
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }

        let meta = ConnectionMeta::new(MockConnection::new(1), test_clock());
        let pooled = PooledConnection::new(
            meta,
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );

        // Close the pool while connection is out
        pool.close();

        // Drop the connection
        drop(pooled);

        // Connection should not be returned to idle (pool is closed)
        assert_eq!(pool.idle_count(), 0);
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.total_count(), 0);

        // Connection was closed
        assert_eq!(pool.stats().connections_closed, 1);
    }

    #[test]
    fn test_pooled_connection_deref() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));

        // Properly initialize pool state as if acquire happened
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }

        let meta = ConnectionMeta::new(MockConnection::new(99), test_clock());
        let pooled = PooledConnection::new(
            meta,
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );

        // Deref should give access to the connection's id
        assert_eq!(pooled.id, 99);
    }

    #[test]
    fn test_pooled_connection_deref_mut() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));

        // Properly initialize pool state as if acquire happened
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }

        let meta = ConnectionMeta::new(MockConnection::new(1), test_clock());
        let mut pooled = PooledConnection::new(
            meta,
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );

        // DerefMut should allow mutation
        pooled.id = 50;
        assert_eq!(pooled.id, 50);
    }

    #[test]
    fn test_pooled_connection_debug() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));

        // Properly initialize pool state as if acquire happened
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }

        let meta = ConnectionMeta::new(MockConnection::new(1), test_clock());
        let pooled = PooledConnection::new(
            meta,
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );

        let debug_str = format!("{:?}", pooled);
        assert!(debug_str.contains("PooledConnection"));
        assert!(debug_str.contains("age"));
    }

    #[test]
    fn test_pool_at_capacity() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(2));

        assert!(!pool.at_capacity());

        // Simulate connections being created
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
        }
        assert!(!pool.at_capacity());

        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 2;
        }
        assert!(pool.at_capacity());
    }

    #[test]
    fn test_acquire_action_enum() {
        // Verify the enum variants exist and can be pattern-matched
        let retire: AcquireAction<MockConnection> = AcquireAction::RetireAndRetry;
        assert!(matches!(retire, AcquireAction::RetireAndRetry));

        let closed: AcquireAction<MockConnection> = AcquireAction::PoolClosed;
        assert!(matches!(closed, AcquireAction::PoolClosed));

        let create: AcquireAction<MockConnection> = AcquireAction::CreateNew;
        assert!(matches!(create, AcquireAction::CreateNew));

        let pool = Pool::<MockConnection>::new(PoolConfig::new(1));
        pool.shared.lock_or_recover().waiter_count = 1;
        let wait = AcquireAction::Wait(WaiterGuard {
            pool: Arc::clone(&pool.shared),
        });
        assert!(matches!(&wait, AcquireAction::Wait(_)));
        drop(wait);
        assert_eq!(pool.stats().pending_requests, 0);

        let meta = ConnectionMeta::new(MockConnection::new(1), test_clock());
        let validate: AcquireAction<MockConnection> = AcquireAction::ValidateExisting(meta);
        assert!(matches!(validate, AcquireAction::ValidateExisting(_)));
    }

    #[test]
    fn test_pool_shared_atomic_counters() {
        let shared = PoolShared::<MockConnection>::new(PoolConfig::new(5), test_clock());

        // Initial values should be 0
        assert_eq!(shared.connections_created.load(Ordering::Relaxed), 0);
        assert_eq!(shared.connections_closed.load(Ordering::Relaxed), 0);
        assert_eq!(shared.acquires.load(Ordering::Relaxed), 0);
        assert_eq!(shared.timeouts.load(Ordering::Relaxed), 0);

        // Test incrementing
        shared.connections_created.fetch_add(1, Ordering::Relaxed);
        shared.connections_closed.fetch_add(2, Ordering::Relaxed);
        shared.acquires.fetch_add(10, Ordering::Relaxed);
        shared.timeouts.fetch_add(3, Ordering::Relaxed);

        assert_eq!(shared.connections_created.load(Ordering::Relaxed), 1);
        assert_eq!(shared.connections_closed.load(Ordering::Relaxed), 2);
        assert_eq!(shared.acquires.load(Ordering::Relaxed), 10);
        assert_eq!(shared.timeouts.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_pool_close_clears_idle() {
        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));

        // Add some idle connections
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 3;
            inner
                .idle
                .push_back(ConnectionMeta::new(MockConnection::new(1), test_clock()));
            inner
                .idle
                .push_back(ConnectionMeta::new(MockConnection::new(2), test_clock()));
            inner
                .idle
                .push_back(ConnectionMeta::new(MockConnection::new(3), test_clock()));
        }

        assert_eq!(pool.idle_count(), 3);
        assert_eq!(pool.total_count(), 3);

        pool.close();

        // After close, idle connections should be cleared
        assert_eq!(pool.idle_count(), 0);
        assert_eq!(pool.total_count(), 0);
        assert!(pool.is_closed());

        // connections_closed should reflect the 3 idle connections
        assert_eq!(pool.stats().connections_closed, 3);
    }

    // ==================== Lock Poisoning Safety Tests ====================
    //
    // These tests verify that the pool correctly handles mutex poisoning,
    // which occurs when a thread panics while holding the lock.
    //
    // Tier 1 (mutations): Return Error if poisoned
    // Tier 2 (read-only): Recover and return valid data
    // Tier 3 (Drop): Log, close, and recover drain accounting (don't panic)

    /// Helper to poison a pool's mutex by panicking while holding the lock.
    ///
    /// Returns the pool with a poisoned mutex.
    fn poison_pool_mutex() -> Pool<MockConnection> {
        use std::panic;
        use std::thread;

        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));

        // Set up some valid state before poisoning
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 2;
            inner.active_count = 1;
            inner
                .idle
                .push_back(ConnectionMeta::new(MockConnection::new(1), test_clock()));
        }

        // Spawn a thread that will panic while holding the lock
        let shared_clone = Arc::clone(&pool.shared);
        let handle = thread::spawn(move || {
            let _guard = shared_clone.inner.lock().unwrap();
            // Panic while holding the lock - this poisons the mutex
            panic!("intentional panic to poison mutex");
        });

        // Wait for the thread to panic (ignore the panic result)
        let _ = handle.join();

        // Verify the mutex is now poisoned
        assert!(pool.shared.inner.lock().is_err());

        pool
    }

    // -------------------- Tier 2: Read-Only Methods --------------------

    #[test]
    fn test_config_after_poisoning_returns_valid_data() {
        let pool = poison_pool_mutex();

        // config() should recover and return the configuration
        let config = pool.config();
        assert_eq!(config.max_connections, 5);
    }

    #[test]
    fn test_stats_after_poisoning_returns_valid_data() {
        let pool = poison_pool_mutex();

        // stats() should recover and return valid statistics
        let stats = pool.stats();
        // The state before poisoning was: total=2, active=1, idle=1
        assert_eq!(stats.total_connections, 2);
        assert_eq!(stats.active_connections, 1);
        assert_eq!(stats.idle_connections, 1);
    }

    #[test]
    fn test_at_capacity_after_poisoning() {
        let pool = poison_pool_mutex();

        // at_capacity() should recover and return correct value
        // Pool has 2 connections, max is 5, so not at capacity
        assert!(!pool.at_capacity());
    }

    #[test]
    fn test_is_closed_after_poisoning() {
        let pool = poison_pool_mutex();

        // is_closed() should recover and return correct value
        assert!(!pool.is_closed());
    }

    #[test]
    fn test_idle_count_after_poisoning() {
        let pool = poison_pool_mutex();

        // idle_count() should recover and return correct value
        assert_eq!(pool.idle_count(), 1);
    }

    #[test]
    fn test_active_count_after_poisoning() {
        let pool = poison_pool_mutex();

        // active_count() should recover and return correct value
        assert_eq!(pool.active_count(), 1);
    }

    #[test]
    fn test_total_count_after_poisoning() {
        let pool = poison_pool_mutex();

        // total_count() should recover and return correct value
        assert_eq!(pool.total_count(), 2);
    }

    // -------------------- Tier 1: Mutation Methods --------------------

    #[test]
    fn test_lock_or_error_returns_error_when_poisoned() {
        use std::thread;

        let shared = Arc::new(PoolShared::<MockConnection>::new(
            PoolConfig::new(5),
            test_clock(),
        ));

        // Poison the mutex
        let shared_clone = Arc::clone(&shared);
        let handle = thread::spawn(move || {
            let _guard = shared_clone.inner.lock().unwrap();
            panic!("intentional panic to poison mutex");
        });
        let _ = handle.join();

        // lock_or_error should return an error
        let result = shared.lock_or_error("test_operation");

        // Verify it's a pool poisoning error
        match result {
            Err(Error::Pool(pool_err)) => {
                assert!(matches!(pool_err.kind, PoolErrorKind::Poisoned));
                assert!(pool_err.message.contains("poisoned"));
            }
            Err(other) => panic!("Expected Pool error, got: {:?}", other),
            Ok(_) => panic!("Expected error, got Ok"),
        }
    }

    #[test]
    fn test_lock_or_recover_succeeds_when_poisoned() {
        use std::thread;

        let shared = Arc::new(PoolShared::<MockConnection>::new(
            PoolConfig::new(5),
            test_clock(),
        ));

        // Set up some state
        {
            let mut inner = shared.inner.lock().unwrap();
            inner.total_count = 42;
        }

        // Poison the mutex
        let shared_clone = Arc::clone(&shared);
        let handle = thread::spawn(move || {
            let _guard = shared_clone.inner.lock().unwrap();
            panic!("intentional panic to poison mutex");
        });
        let _ = handle.join();

        // Verify mutex is poisoned
        assert!(shared.inner.lock().is_err());

        // lock_or_recover should still succeed and provide access to data
        let inner = shared.lock_or_recover();
        assert_eq!(inner.total_count, 42);
    }

    #[test]
    fn test_close_after_poisoning_recovers_and_closes() {
        let pool = poison_pool_mutex();

        // close() should recover from poisoning and still close the pool
        pool.close();

        // After close, the pool should be marked as closed
        assert!(pool.is_closed());

        // Idle connections should be cleared
        assert_eq!(pool.idle_count(), 0);
    }

    #[test]
    fn test_poisoned_pool_return_completes_drain_accounting() {
        use std::thread;

        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(1));
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }
        let pooled = PooledConnection::new(
            ConnectionMeta::new(MockConnection::new(1), test_clock()),
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );

        let shared = Arc::clone(&pool.shared);
        let poisoner = thread::spawn(move || {
            let _guard = shared.inner.lock().unwrap();
            panic!("intentional panic to poison drain accounting");
        });
        let _ = poisoner.join();

        let cx = Cx::for_testing();
        let mut drain = Box::pin(pool.close_and_drain(&cx));
        let mut task_cx = Context::from_waker(Waker::noop());

        assert!(matches!(drain.as_mut().poll(&mut task_cx), Poll::Pending));
        assert!(pool.is_closed());
        assert_eq!(pool.active_count(), 1);

        drop(pooled);

        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.total_count(), 0);
        assert!(
            pool.shared.active_drained.get().is_some(),
            "poison-aware final release must publish the drain latch"
        );
        assert!(matches!(
            drain.as_mut().poll(&mut task_cx),
            Poll::Ready(Outcome::Err(Error::Pool(PoolError {
                kind: PoolErrorKind::Poisoned,
                ..
            })))
        ));
    }

    // -------------------- Tier 3: Drop Safety --------------------

    #[test]
    fn test_drop_pooled_connection_after_poisoning_does_not_panic() {
        use std::panic;
        use std::thread;

        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));

        // Set up a connection that's "checked out"
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }

        // Create a pooled connection
        let meta = ConnectionMeta::new(MockConnection::new(1), test_clock());
        let pooled = PooledConnection::new(
            meta,
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );

        // Poison the mutex by panicking in another thread
        let shared_clone = Arc::clone(&pool.shared);
        let handle = thread::spawn(move || {
            let _guard = shared_clone.inner.lock().unwrap();
            panic!("intentional panic to poison mutex");
        });
        let _ = handle.join();

        // Verify mutex is poisoned
        assert!(pool.shared.inner.lock().is_err());

        // Drop the pooled connection - should NOT panic
        // The connection will be leaked, but that's the correct behavior
        let drop_result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            drop(pooled);
        }));

        // Dropping should not panic
        assert!(
            drop_result.is_ok(),
            "Dropping PooledConnection after mutex poisoning should not panic"
        );
    }

    #[test]
    fn test_detach_after_poisoning_does_not_panic() {
        use std::panic;
        use std::thread;

        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));

        // Set up a connection that's "checked out"
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }

        // Create a pooled connection
        let meta = ConnectionMeta::new(MockConnection::new(42), test_clock());
        let pooled = PooledConnection::new(
            meta,
            Arc::downgrade(&pool.shared),
            Duration::from_millis(pool.config().acquire_timeout_ms),
        );

        // Poison the mutex
        let shared_clone = Arc::clone(&pool.shared);
        let handle = thread::spawn(move || {
            let _guard = shared_clone.inner.lock().unwrap();
            panic!("intentional panic to poison mutex");
        });
        let _ = handle.join();

        // Verify mutex is poisoned
        assert!(pool.shared.inner.lock().is_err());

        // Detach should not panic, even though it can't update counters
        let detach_result = panic::catch_unwind(panic::AssertUnwindSafe(|| pooled.detach()));

        assert!(
            detach_result.is_ok(),
            "detach() after mutex poisoning should not panic"
        );

        // Should still get the connection back
        let conn = detach_result.unwrap();
        assert_eq!(conn.id, 42);
    }

    // -------------------- Integration: Pool Survives Thread Panic --------------------

    #[test]
    fn test_pool_survives_thread_panic_during_acquire() {
        use std::thread;

        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));
        let pool_arc = Arc::new(pool);

        // Simulate a thread that acquires, does work, then panics
        // The connection should be leaked but pool should remain usable for reads
        let pool_clone = Arc::clone(&pool_arc);
        let handle = thread::spawn(move || {
            // Manually simulate having acquired a connection
            {
                let mut inner = pool_clone.shared.inner.lock().unwrap();
                inner.total_count = 1;
                inner.active_count = 1;
            }

            // Panic while holding the pool's internal mutex to simulate a poisoned lock.
            // This models an internal panic in pool bookkeeping, not user code.
            let _guard = pool_clone.shared.inner.lock().unwrap();
            panic!("simulated panic during database operation");
        });

        // Wait for thread to panic
        let _ = handle.join();

        // Pool's mutex is now poisoned, but read-only methods should still work
        assert_eq!(pool_arc.total_count(), 1);
        assert_eq!(pool_arc.config().max_connections, 5);

        // Stats should be recoverable
        let stats = pool_arc.stats();
        assert_eq!(stats.total_connections, 1);
    }

    #[test]
    fn test_pool_close_after_thread_panic() {
        use std::thread;

        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));

        // Add some idle connections
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 2;
            inner
                .idle
                .push_back(ConnectionMeta::new(MockConnection::new(1), test_clock()));
            inner
                .idle
                .push_back(ConnectionMeta::new(MockConnection::new(2), test_clock()));
        }

        // Poison the mutex
        let shared_clone = Arc::clone(&pool.shared);
        let handle = thread::spawn(move || {
            let _guard = shared_clone.inner.lock().unwrap();
            panic!("intentional panic");
        });
        let _ = handle.join();

        // close() should recover and still work
        pool.close();

        // Pool should be closed and idle connections cleared
        assert!(pool.is_closed());
        assert_eq!(pool.idle_count(), 0);
    }

    #[test]
    fn test_multiple_reads_after_poisoning() {
        let pool = poison_pool_mutex();

        // Multiple read operations should all succeed
        for _ in 0..10 {
            let _ = pool.config();
            let _ = pool.stats();
            let _ = pool.at_capacity();
            let _ = pool.is_closed();
            let _ = pool.idle_count();
            let _ = pool.active_count();
            let _ = pool.total_count();
        }

        // All reads should have recovered successfully
        assert_eq!(pool.total_count(), 2);
    }

    #[test]
    fn test_waiters_count_after_poisoning() {
        use std::thread;

        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));

        // Set up waiter count
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.waiter_count = 3;
        }

        // Poison the mutex
        let shared_clone = Arc::clone(&pool.shared);
        let handle = thread::spawn(move || {
            let _guard = shared_clone.inner.lock().unwrap();
            panic!("intentional panic");
        });
        let _ = handle.join();

        // stats() should recover and show correct waiter count
        let stats = pool.stats();
        assert_eq!(stats.pending_requests, 3);
    }

    // ------------------------------------------------------------------
    // Timing under LabRuntime virtual time
    //
    // Every timing feature of the pool (idle timeout, max lifetime, acquire
    // timeout, checkout health, drain) is exercised on a `LabRuntime` whose
    // virtual clock is shared with the pool via `Pool::with_timer_driver`.
    // Virtual time is advanced explicitly (or auto-advanced by the lab
    // scheduler), so each test asserts the exact virtual instant of the
    // effect with zero wall-clock waiting.
    // ------------------------------------------------------------------

    /// Creates a lab runtime, a pool sharing the lab's virtual clock, and
    /// the root region tasks are spawned into.
    fn lab_pool(config: PoolConfig) -> (LabRuntime, Arc<Pool<MockConnection>>, RegionId) {
        let mut runtime = LabRuntime::new(LabConfig::new(0x50f1).max_steps(500_000));
        let driver = runtime
            .state
            .timer_driver_handle()
            .expect("lab runtime installs a virtual-clock timer driver");
        let pool = Arc::new(Pool::with_timer_driver(config, driver));
        let region = runtime.state.create_root_region(Budget::INFINITE);
        (runtime, pool, region)
    }

    fn run_acquire_lab<F, Fut>(config: PoolConfig, budget: Budget, test: F)
    where
        F: FnOnce(Arc<Pool<MockConnection>>, Cx) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (mut runtime, pool, region) = lab_pool(config);
        let completed = Arc::new(OnceCell::new());
        let record = Arc::clone(&completed);
        let (task, _) = runtime
            .state
            .create_task(region, budget, async move {
                let cx = Cx::current().expect("lab task has a context");
                test(pool, cx).await;
                let _ = record.set(());
            })
            .expect("spawn acquisition regression");
        runtime.scheduler.lock().schedule(task, 0);
        runtime.run_with_auto_advance();
        assert!(
            completed.get().is_some(),
            "acquisition regression must finish every assertion"
        );
    }

    #[test]
    fn lab_synchronous_retirement_supervisor_expires_at_exact_deadline() {
        for grace_ms in [0, 50] {
            run_acquire_lab(
                PoolConfig::new(1).acquire_timeout(50),
                Budget::INFINITE,
                move |pool, cx| async move {
                    let dropped = Arc::new(OnceCell::new());
                    let closes = Arc::new(AtomicUsize::new(0));
                    let mut lease = pool
                        .acquire(&cx, || async {
                            Outcome::Ok(
                                MockConnection::with_pending_pool_close(1, Arc::clone(&closes))
                                    .observe_drop(&pool, Arc::clone(&dropped)),
                            )
                        })
                        .await
                        .expect("create a lease before retirement");
                    let guard = ActiveConnectionGuard::new(
                        Arc::clone(&pool.shared),
                        lease.meta.take().expect("transfer lease ownership"),
                    );
                    drop(lease);
                    pool.close();
                    let deadline = cx.now() + Duration::from_millis(grace_ms);
                    let context = "lab synchronous retirement";
                    let mut retiring = Box::pin(supervise_retirement(
                        &cx,
                        deadline,
                        guard.close_synchronously(&cx, deadline, context),
                        |error| pool.shared.record_retirement_failure(context, error),
                    ));

                    assert!(retiring.as_mut().await.is_err());
                    assert_eq!(cx.now(), Time::from_millis(grace_ms));
                    // Retain the completed supervisor: its Ready result, not its
                    // later destruction, must release the owned close future.
                    assert_eq!(closes.load(Ordering::Relaxed), usize::from(grace_ms > 0));
                    assert_drop_precedes_slot_release(&dropped);
                    assert_no_occupied_slots(&pool);
                    assert_eq!(pool.stats().connections_closed, 1);
                    assert_eq!(pool.stats().timeouts, 0);
                    assert_sticky_retirement_failure(&pool);
                },
            );
        }
    }

    #[test]
    fn lab_synchronous_retirement_does_not_fail_after_successful_final_release() {
        let (clock, driver) = virtual_clock();
        let mut runtime = LabRuntime::new(LabConfig::new(0x43_0003));
        runtime.state.set_timer_driver(driver.clone());
        let pool = Arc::new(Pool::with_timer_driver(
            PoolConfig::new(1).acquire_timeout(50),
            driver,
        ));
        let region = runtime.state.create_root_region(Budget::INFINITE);
        let completed = Arc::new(OnceCell::new());
        let record = Arc::clone(&completed);
        let (task, _) = runtime
            .state
            .create_task(region, Budget::INFINITE, async move {
                let cx = Cx::current().expect("lab task context");
                let dropped = Arc::new(OnceCell::new());
                let mut lease = pool
                    .acquire(&cx, || async {
                        Outcome::Ok(
                            MockConnection::new(1).observe_drop(&pool, Arc::clone(&dropped)),
                        )
                    })
                    .await
                    .expect("create lease before retirement");
                let guard = ActiveConnectionGuard::new(
                    Arc::clone(&pool.shared),
                    lease.meta.take().expect("transfer lease ownership"),
                );
                drop(lease);
                pool.close();
                let wake = Arc::new(DrainFailureWake {
                    pool: Arc::downgrade(&pool.shared),
                    observed: OnceCell::new(),
                    advance_after_wake: Some((clock, 50_000_000)),
                });
                let waker = Waker::from(Arc::clone(&wake));
                let mut drain = Box::pin(pool.close_and_drain(&cx));
                {
                    let mut task_cx = Context::from_waker(&waker);
                    assert!(drain.as_mut().poll(&mut task_cx).is_pending());
                }
                let deadline = Time::from_millis(50);
                let context = "lab ready retirement boundary";
                supervise_retirement(
                    &cx,
                    deadline,
                    guard.close_synchronously(&cx, deadline, context),
                    |error| pool.shared.record_retirement_failure(context, error),
                )
                .await
                .expect("the close completed before the final-slot wake advanced time");

                assert_eq!(cx.now(), deadline);
                assert_eq!(wake.observed.get(), Some(&false));
                assert!(pool.shared.retirement_failure_error().is_none());
                assert_drop_precedes_slot_release(&dropped);
                assert_no_occupied_slots(&pool);
                drain
                    .await
                    .expect("successful retirement remains successful");
                let _ = record.set(());
            })
            .expect("spawn final-release boundary regression");
        runtime.scheduler.lock().schedule(task, 0);
        runtime.run_with_auto_advance();
        assert!(completed.get().is_some());
    }

    #[test]
    fn lab_pool_validation_without_health_ping_obeys_acquisition_deadline() {
        run_acquire_lab(
            PoolConfig::new(1)
                .test_on_checkout(false)
                .acquire_timeout(200),
            Budget::INFINITE,
            |pool, cx| async move {
                let dropped = Arc::new(OnceCell::new());
                let checks = Arc::new(Mutex::new(Vec::new()));
                let first = pool
                    .acquire(&cx, || async {
                        let mut conn =
                            MockConnection::with_pool_validation(1, MockPoolValidation::Pending);
                        conn.validation_checks = Arc::clone(&checks);
                        Outcome::Ok(conn.observe_drop(&pool, Arc::clone(&dropped)))
                    })
                    .await
                    .expect("create connection before reuse validation");
                drop(first);
                let factories = AtomicUsize::new(0);
                let mut acquire = Box::pin(pool.acquire(&cx, || async {
                    factories.fetch_add(1, Ordering::Relaxed);
                    Outcome::Ok(MockConnection::new(99))
                }));
                assert!(matches!(
                    acquire.as_mut().await,
                    Outcome::Err(Error::Pool(PoolError {
                        kind: PoolErrorKind::Timeout,
                        ..
                    }))
                ));
                assert_eq!(cx.now(), Time::from_millis(200));
                assert_eq!(*checks.lock().unwrap(), vec![false]);
                assert_eq!(factories.load(Ordering::Relaxed), 0);
                assert_drop_precedes_slot_release(&dropped);
                assert_no_occupied_slots(&pool);
                assert_eq!(pool.stats().connections_created, 1);
                assert_eq!(pool.stats().connections_closed, 1);
                assert_eq!(pool.stats().acquires, 1);
                assert_eq!(pool.stats().timeouts, 1);
                let replacement = pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) })
                    .await
                    .expect("timed-out pool hook releases capacity");
                assert_eq!(replacement.id, 2);
                drop(replacement);
                pool.close_and_drain(&cx)
                    .await
                    .expect("replacement drains successfully");
                assert_no_occupied_slots(&pool);
            },
        );
    }

    #[test]
    fn lab_checkout_validation_deadline_discards_connection_and_pool_recovers() {
        run_acquire_lab(
            PoolConfig::new(1).acquire_timeout(200),
            Budget::INFINITE,
            |pool, cx| async move {
                let dropped = Arc::new(OnceCell::new());
                let first = pool
                    .acquire(&cx, || async {
                        Outcome::Ok(
                            MockConnection::with_ping(1, MockPing::Pending)
                                .observe_drop(&pool, Arc::clone(&dropped)),
                        )
                    })
                    .await
                    .expect("create first connection");
                drop(first);
                let calls = AtomicUsize::new(0);
                let mut acquire = Box::pin(pool.acquire(&cx, || async {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Outcome::Ok(MockConnection::new(99))
                }));
                assert!(matches!(
                    acquire.as_mut().await,
                    Outcome::Err(Error::Pool(PoolError {
                        kind: PoolErrorKind::Timeout,
                        ..
                    }))
                ));
                assert_eq!(cx.now(), Time::from_millis(200));
                assert_eq!(calls.load(Ordering::Relaxed), 0);
                assert_drop_precedes_slot_release(&dropped);
                assert_no_occupied_slots(&pool);
                assert_eq!(pool.stats().connections_created, 1);
                assert_eq!(pool.stats().connections_closed, 1);
                assert_eq!(pool.stats().timeouts, 1);
                assert_eq!(pool.stats().acquires, 1);

                let replacement = pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) })
                    .await
                    .expect("pool recovers after stalled validation");
                assert_eq!(replacement.id, 2);
                drop(replacement);
                pool.close_and_drain(&cx)
                    .await
                    .expect("recovered pool drains");
                assert_no_occupied_slots(&pool);
                assert_eq!(pool.stats().connections_closed, 2);
            },
        );
    }

    #[test]
    fn lab_checkout_validation_uses_the_shorter_caller_budget() {
        let budget = Budget::new().with_deadline(Time::from_millis(75));
        run_acquire_lab(
            PoolConfig::new(1).acquire_timeout(200),
            budget,
            |pool, cx| async move {
                let first = pool
                    .acquire(&cx, || async {
                        Outcome::Ok(MockConnection::with_ping(1, MockPing::Pending))
                    })
                    .await
                    .expect("create first connection");
                drop(first);
                let mut acquire =
                    Box::pin(pool.acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) }));
                match acquire.as_mut().await {
                    Outcome::Err(Error::Pool(PoolError {
                        kind: PoolErrorKind::Timeout,
                        message,
                        ..
                    })) => {
                        assert!(message.contains("budget deadline"));
                    }
                    other => panic!("budget expiry must remain a timeout, got {other:?}"),
                }
                assert_eq!(cx.now(), Time::from_millis(75));
                assert_no_occupied_slots(&pool);
                assert_eq!(pool.stats().timeouts, 1);
                assert_eq!(pool.stats().connections_closed, 1);
            },
        );
    }

    #[test]
    fn lab_pending_factory_is_bounded_and_drops_its_resource_before_slot_release() {
        run_acquire_lab(
            PoolConfig::new(1).acquire_timeout(200),
            Budget::INFINITE,
            |pool, cx| async move {
                let dropped = Arc::new(OnceCell::new());
                let ready = Arc::new(AtomicBool::new(false));
                let mut acquire = Box::pin(pool.acquire(&cx, || GatedFactory {
                    ready: Arc::clone(&ready),
                    conn: Some(MockConnection::new(1).observe_drop(&pool, Arc::clone(&dropped))),
                }));
                assert!(matches!(
                    acquire.as_mut().await,
                    Outcome::Err(Error::Pool(PoolError {
                        kind: PoolErrorKind::Timeout,
                        ..
                    }))
                ));
                assert_eq!(cx.now(), Time::from_millis(200));
                assert_drop_precedes_slot_release(&dropped);
                assert_no_occupied_slots(&pool);
                assert_eq!(pool.stats().connections_created, 0);
                assert_eq!(pool.stats().connections_closed, 0);
                assert_eq!(pool.stats().timeouts, 1);
                let replacement = pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) })
                    .await
                    .expect("timed-out factory releases its reservation");
                drop(replacement);
                pool.close_and_drain(&cx)
                    .await
                    .expect("drain after replacement");
                assert_no_occupied_slots(&pool);
            },
        );
    }

    #[test]
    fn lab_queue_wait_and_checkout_validation_share_one_deadline() {
        run_acquire_lab(
            PoolConfig::new(1).acquire_timeout(200),
            Budget::INFINITE,
            |pool, cx| async move {
                let holder = pool
                    .acquire(&cx, || async {
                        Outcome::Ok(MockConnection::with_ping(1, MockPing::Pending))
                    })
                    .await
                    .expect("holder creates connection");
                let mut waiter =
                    Box::pin(pool.acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) }));
                std::future::poll_fn(|task_cx| {
                    assert!(waiter.as_mut().poll(task_cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                assert_eq!(pool.stats().pending_requests, 1);
                asupersync::time::sleep(cx.now(), Duration::from_millis(70)).await;
                drop(holder);
                assert!(matches!(
                    waiter.as_mut().await,
                    Outcome::Err(Error::Pool(PoolError {
                        kind: PoolErrorKind::Timeout,
                        ..
                    }))
                ));
                assert_eq!(
                    cx.now(),
                    Time::from_millis(200),
                    "validation cannot restart the queue's budget"
                );
                assert_no_occupied_slots(&pool);
                assert_eq!(pool.stats().timeouts, 1);
                assert_eq!(pool.stats().connections_closed, 1);
            },
        );
    }

    #[test]
    fn lab_failed_ping_retirement_cannot_extend_the_acquisition_deadline() {
        run_acquire_lab(
            PoolConfig::new(1).acquire_timeout(200),
            Budget::INFINITE,
            |pool, cx| async move {
                let dropped = Arc::new(OnceCell::new());
                let closes = Arc::new(AtomicUsize::new(0));
                let first = pool
                    .acquire(&cx, || async {
                        let mut conn =
                            MockConnection::with_pending_pool_close(1, Arc::clone(&closes))
                                .observe_drop(&pool, Arc::clone(&dropped));
                        conn.ping = MockPing::Delay(Duration::from_millis(50));
                        conn.ping_should_fail.store(true, Ordering::Relaxed);
                        Outcome::Ok(conn)
                    })
                    .await
                    .expect("create failing connection");
                drop(first);
                let mut acquire =
                    Box::pin(pool.acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) }));
                assert!(matches!(
                    acquire.as_mut().await,
                    Outcome::Err(Error::Pool(PoolError {
                        kind: PoolErrorKind::Timeout,
                        ..
                    }))
                ));
                assert_eq!(cx.now(), Time::from_millis(200));
                assert_eq!(closes.load(Ordering::Relaxed), 1);
                assert_drop_precedes_slot_release(&dropped);
                assert_no_occupied_slots(&pool);
                assert_eq!(pool.stats().connections_closed, 1);
                pool.close_and_drain(&cx)
                    .await
                    .expect("timed-out retirement is already drained");
            },
        );
    }

    #[test]
    fn lab_expired_idle_retirement_cannot_extend_the_acquisition_deadline() {
        run_acquire_lab(
            PoolConfig::new(1).max_lifetime(1).acquire_timeout(200),
            Budget::INFINITE,
            |pool, cx| async move {
                let dropped = Arc::new(OnceCell::new());
                let closes = Arc::new(AtomicUsize::new(0));
                let first = pool
                    .acquire(&cx, || async {
                        Outcome::Ok(
                            MockConnection::with_pending_pool_close(1, Arc::clone(&closes))
                                .observe_drop(&pool, Arc::clone(&dropped)),
                        )
                    })
                    .await
                    .expect("create connection before its lifetime expires");
                drop(first);
                asupersync::time::sleep(cx.now(), Duration::from_millis(2)).await;
                let factories = AtomicUsize::new(0);
                let mut acquire = Box::pin(pool.acquire(&cx, || async {
                    factories.fetch_add(1, Ordering::Relaxed);
                    Outcome::Ok(MockConnection::new(99))
                }));
                assert!(matches!(
                    acquire.as_mut().await,
                    Outcome::Err(Error::Pool(PoolError {
                        kind: PoolErrorKind::Timeout,
                        ..
                    }))
                ));
                assert_eq!(cx.now(), Time::from_millis(202));
                assert_eq!(closes.load(Ordering::Relaxed), 1);
                assert_eq!(factories.load(Ordering::Relaxed), 0);
                assert_drop_precedes_slot_release(&dropped);
                assert_no_occupied_slots(&pool);
                assert_eq!(pool.stats().connections_created, 1);
                assert_eq!(pool.stats().connections_closed, 1);
                assert_eq!(pool.stats().acquires, 1);
                assert_eq!(pool.stats().timeouts, 1);

                let replacement = pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) })
                    .await
                    .expect("timed-out idle retirement releases its capacity");
                assert_eq!(replacement.id, 2);
                drop(replacement);
                pool.close_and_drain(&cx)
                    .await
                    .expect("replacement drains successfully");
                assert_no_occupied_slots(&pool);
            },
        );
    }

    #[test]
    fn lab_successful_checkout_reuses_connection_and_unregisters_timeout() {
        let (mut runtime, pool, region) = lab_pool(PoolConfig::new(1).acquire_timeout(200));
        let completed = Arc::new(OnceCell::new());
        let record = Arc::clone(&completed);
        let task_pool = Arc::clone(&pool);
        let (task, _) = runtime
            .state
            .create_task(region, Budget::INFINITE, async move {
                let cx = Cx::current().expect("lab task context");
                let first = task_pool
                    .acquire(&cx, || async {
                        Outcome::Ok(MockConnection::with_ping(
                            1,
                            MockPing::Delay(Duration::from_millis(50)),
                        ))
                    })
                    .await
                    .expect("create first connection");
                drop(first);
                let factories = AtomicUsize::new(0);
                let lease = task_pool
                    .acquire(&cx, || async {
                        factories.fetch_add(1, Ordering::Relaxed);
                        Outcome::Ok(MockConnection::new(2))
                    })
                    .await
                    .expect("validation completes before the deadline");
                assert_eq!(lease.id, 1, "checkout reuses the original connection");
                assert_eq!(factories.load(Ordering::Relaxed), 0);
                assert_eq!(cx.now(), Time::from_millis(50));
                drop(lease);
                let _ = record.set(());
            })
            .expect("spawn successful validation");
        runtime.scheduler.lock().schedule(task, 0);
        runtime.run_with_auto_advance();
        assert!(completed.get().is_some(), "successful reuse must complete");
        let stats = pool.stats();
        assert_eq!(stats.connections_created, 1);
        assert_eq!(stats.connections_closed, 0);
        assert_eq!(stats.active_connections, 0);
        assert_eq!(stats.idle_connections, 1);
        assert_eq!(stats.total_connections, 1);
        assert_eq!(stats.pending_requests, 0);
        assert_eq!(stats.acquires, 2);
        assert_eq!(stats.timeouts, 0);
        assert_eq!(
            runtime.now(),
            Time::from_millis(50),
            "successful checkout removes its 200ms timeout from the timer driver"
        );
    }

    #[test]
    fn lab_validation_retries_and_close_hooks_share_the_original_deadline() {
        run_acquire_lab(
            PoolConfig::new(2).acquire_timeout(200),
            Budget::INFINITE,
            |pool, cx| async move {
                let first = pool
                    .acquire(&cx, || async {
                        let mut conn = MockConnection::with_ping(
                            1,
                            MockPing::Delay(Duration::from_millis(80)),
                        );
                        conn.ping_should_fail.store(true, Ordering::Relaxed);
                        conn.pool_close_delay = Some(Duration::from_millis(50));
                        Outcome::Ok(conn)
                    })
                    .await
                    .expect("create first connection");
                let second = pool
                    .acquire(&cx, || async {
                        Outcome::Ok(MockConnection::with_ping(2, MockPing::Pending))
                    })
                    .await
                    .expect("create second connection");
                drop(first);
                drop(second);
                let factories = AtomicUsize::new(0);
                let mut acquire = Box::pin(pool.acquire(&cx, || async {
                    factories.fetch_add(1, Ordering::Relaxed);
                    Outcome::Ok(MockConnection::new(3))
                }));
                assert!(matches!(
                    acquire.as_mut().await,
                    Outcome::Err(Error::Pool(PoolError {
                        kind: PoolErrorKind::Timeout,
                        ..
                    }))
                ));
                assert_eq!(
                    cx.now(),
                    Time::from_millis(200),
                    "retry must inherit the 130ms already spent"
                );
                assert_eq!(factories.load(Ordering::Relaxed), 0);
                assert_eq!(pool.stats().connections_closed, 2);
                assert_eq!(pool.stats().connections_created, 2);
                assert_eq!(pool.stats().timeouts, 1);
                assert_no_occupied_slots(&pool);
            },
        );
    }

    #[test]
    fn lab_successful_factory_and_ping_recheck_deadline_after_the_final_poll() {
        for validate_existing in [false, true] {
            let (clock, driver) = virtual_clock();
            let mut runtime = LabRuntime::new(LabConfig::new(0x43_0001));
            // The same controllable clock drives the pool and the task. The
            // injected advance represents work performed inside one poll,
            // before the runtime gets another chance to poll the timeout.
            runtime.state.set_timer_driver(driver.clone());
            let pool = Arc::new(Pool::with_timer_driver(
                PoolConfig::new(1).acquire_timeout(200),
                driver,
            ));
            let region = runtime.state.create_root_region(Budget::INFINITE);
            let completed = Arc::new(OnceCell::new());
            let record = Arc::clone(&completed);
            let (task, _) = runtime
                .state
                .create_task(region, Budget::INFINITE, async move {
                    let cx = Cx::current().expect("lab task context");
                    let dropped = Arc::new(OnceCell::new());
                    if validate_existing {
                        let first = pool
                            .acquire(&cx, || async {
                                Outcome::Ok(
                                    MockConnection::with_ping(
                                        1,
                                        MockPing::AdvanceClock(Arc::clone(&clock), 200_000_000),
                                    )
                                    .observe_drop(&pool, Arc::clone(&dropped)),
                                )
                            })
                            .await
                            .expect("create connection before deadline");
                        drop(first);
                    }
                    let mut acquire = Box::pin(pool.acquire(&cx, || async {
                        assert!(!validate_existing, "existing connection must be validated");
                        clock.advance(200_000_000);
                        Outcome::Ok(
                            MockConnection::new(2).observe_drop(&pool, Arc::clone(&dropped)),
                        )
                    }));
                    assert!(matches!(
                        acquire.as_mut().await,
                        Outcome::Err(Error::Pool(PoolError {
                            kind: PoolErrorKind::Timeout,
                            ..
                        }))
                    ));
                    assert_eq!(cx.now(), Time::from_millis(200));
                    assert_drop_precedes_slot_release(&dropped);
                    assert_no_occupied_slots(&pool);
                    assert_eq!(pool.stats().acquires, u64::from(validate_existing));
                    assert_eq!(pool.stats().connections_closed, 1);
                    assert_eq!(pool.stats().timeouts, 1);
                    let _ = record.set(());
                })
                .expect("spawn final-poll boundary regression");
            runtime.scheduler.lock().schedule(task, 0);
            runtime.run_with_auto_advance();
            assert!(
                completed.get().is_some(),
                "both publication sites must recheck time"
            );
        }
    }

    #[test]
    fn lab_failed_ping_at_deadline_does_not_start_retirement() {
        let (clock, driver) = virtual_clock();
        let mut runtime = LabRuntime::new(LabConfig::new(0x43_0002));
        runtime.state.set_timer_driver(driver.clone());
        let pool = Arc::new(Pool::with_timer_driver(
            PoolConfig::new(1).acquire_timeout(200),
            driver,
        ));
        let region = runtime.state.create_root_region(Budget::INFINITE);
        let completed = Arc::new(OnceCell::new());
        let record = Arc::clone(&completed);
        let (task, _) = runtime
            .state
            .create_task(region, Budget::INFINITE, async move {
                let cx = Cx::current().expect("lab task context");
                let dropped = Arc::new(OnceCell::new());
                let closes = Arc::new(AtomicUsize::new(0));
                let first = pool
                    .acquire(&cx, || async {
                        let mut conn =
                            MockConnection::with_pool_close_counter(1, Arc::clone(&closes))
                                .observe_drop(&pool, Arc::clone(&dropped));
                        conn.ping = MockPing::AdvanceClock(Arc::clone(&clock), 200_000_000);
                        conn.ping_should_fail.store(true, Ordering::Relaxed);
                        Outcome::Ok(conn)
                    })
                    .await
                    .expect("create connection before deadline");
                drop(first);
                let mut acquire =
                    Box::pin(pool.acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) }));
                assert!(matches!(
                    acquire.as_mut().await,
                    Outcome::Err(Error::Pool(PoolError {
                        kind: PoolErrorKind::Timeout,
                        ..
                    }))
                ));
                assert_eq!(cx.now(), Time::from_millis(200));
                assert_eq!(
                    closes.load(Ordering::Relaxed),
                    0,
                    "failed ping must not start more driver work after the deadline"
                );
                assert_drop_precedes_slot_release(&dropped);
                assert_no_occupied_slots(&pool);
                assert_eq!(pool.stats().connections_created, 1);
                assert_eq!(pool.stats().connections_closed, 1);
                assert_eq!(pool.stats().acquires, 1);
                assert_eq!(pool.stats().timeouts, 1);
                let _ = record.set(());
            })
            .expect("spawn expired-ping retirement regression");
        runtime.scheduler.lock().schedule(task, 0);
        runtime.run_with_auto_advance();
        assert!(
            completed.get().is_some(),
            "expired ping must finish cleanup"
        );
    }

    #[test]
    fn lab_idle_timeout_retires_exactly_the_expired_idle_connections() {
        let (mut runtime, pool, region) = lab_pool(
            PoolConfig::new(3)
                .idle_timeout(100)
                .test_on_checkout(false)
                .acquire_timeout(5_000),
        );
        let stats_after = Arc::new(OnceCell::<PoolStats>::new());
        let stats_recorder = Arc::clone(&stats_after);
        let acquired_id = Arc::new(OnceCell::<u32>::new());
        let id_recorder = Arc::clone(&acquired_id);

        let (task, _) = runtime
            .state
            .create_task(region, Budget::INFINITE, async move {
                let cx = Cx::current().expect("lab task has a cx");
                // Three concurrent leases => three distinct connections.
                let a = pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(1)) })
                    .await
                    .expect("acquire a");
                let b = pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) })
                    .await
                    .expect("acquire b");
                let c = pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(3)) })
                    .await
                    .expect("acquire c");
                drop(c);
                drop(b);
                drop(a);
                // Everything idle at once; 250ms virtual passes the 100ms
                // idle timeout for all three.
                asupersync::time::sleep(cx.now(), Duration::from_millis(250)).await;
                let d = pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(4)) })
                    .await
                    .expect("acquire d");
                let _ = id_recorder.set(d.id);
                let _ = stats_recorder.set(pool.stats());
            })
            .expect("spawn lab task");
        runtime.scheduler.lock().schedule(task, 0);

        // Run until parked, then jump virtual time to each timer deadline.
        runtime.run_with_auto_advance();

        assert_eq!(acquired_id.get().copied(), Some(4));
        let stats = stats_after.get().expect("task ran to completion");
        // Exactly the three idle connections were retired and replaced.
        assert_eq!(stats.connections_created, 4);
        assert_eq!(stats.connections_closed, 3);
        assert_eq!(stats.acquires, 4);
        assert_eq!(stats.idle_connections, 0);
        assert_eq!(stats.active_connections, 1);
        assert_eq!(stats.total_connections, 1);
    }

    #[test]
    fn lab_max_lifetime_replaces_a_connection_exactly_when_it_passes() {
        let (mut runtime, pool, region) = lab_pool(
            PoolConfig::new(2)
                .max_lifetime(500)
                .idle_timeout(100_000)
                .test_on_checkout(false)
                .acquire_timeout(5_000),
        );
        let first_id = Arc::new(OnceCell::<u32>::new());
        let first_recorder = Arc::clone(&first_id);
        let second = Arc::new(OnceCell::<(u32, u64, u64)>::new());
        let second_recorder = Arc::clone(&second);

        let (task, _) = runtime
            .state
            .create_task(region, Budget::INFINITE, async move {
                let cx = Cx::current().expect("lab task has a cx");
                let a = pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(1)) })
                    .await
                    .expect("acquire a");
                drop(a);
                // At 400ms of age the connection is still inside its 500ms
                // lifetime: the next checkout must hand back the SAME one.
                asupersync::time::sleep(cx.now(), Duration::from_millis(400)).await;
                let b = pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) })
                    .await
                    .expect("acquire b");
                let _ = first_recorder.set(b.id);
                drop(b);
                // 101ms later the lifetime has passed (501ms > 500ms): the
                // next checkout must retire it and create a fresh one.
                asupersync::time::sleep(cx.now(), Duration::from_millis(101)).await;
                let c = pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(3)) })
                    .await
                    .expect("acquire c");
                let stats = pool.stats();
                let _ = second_recorder.set((
                    c.id,
                    stats.connections_created,
                    stats.connections_closed,
                ));
            })
            .expect("spawn lab task");
        runtime.scheduler.lock().schedule(task, 0);

        runtime.run_with_auto_advance();

        assert_eq!(
            first_id.get().copied(),
            Some(1),
            "not retired before its lifetime passes"
        );
        assert_eq!(
            second.get().copied(),
            Some((3, 2, 1)),
            "replaced exactly once the lifetime has passed"
        );
    }

    #[test]
    fn lab_acquire_timeout_fires_at_the_exact_virtual_deadline() {
        let (mut runtime, pool, region) = lab_pool(
            PoolConfig::new(1)
                .acquire_timeout(200)
                .test_on_checkout(false),
        );
        let holder_result = Arc::new(OnceCell::<u64>::new());
        let holder_recorder = Arc::clone(&holder_result);
        let waiter_result = Arc::new(OnceCell::<(u64, bool)>::new());
        let waiter_recorder = Arc::clone(&waiter_result);

        let holder_pool = Arc::clone(&pool);
        let (holder, _) = runtime
            .state
            .create_task(region, Budget::INFINITE, async move {
                let cx = Cx::current().expect("lab task has a cx");
                let lease = holder_pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(1)) })
                    .await
                    .expect("holder acquires the only lease");
                // Hold the lease well past the waiter's deadline.
                asupersync::time::sleep(cx.now(), Duration::from_millis(10_000)).await;
                drop(lease);
                let _ = holder_recorder.set(cx.now().as_millis());
            })
            .expect("spawn holder");
        runtime.scheduler.lock().schedule(holder, 0);
        let waiter_pool = Arc::clone(&pool);
        let (waiter, _) = runtime
            .state
            .create_task(region, Budget::INFINITE, async move {
                let cx = Cx::current().expect("lab task has a cx");
                let outcome = waiter_pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) })
                    .await;
                let instant_ms = cx.now().as_millis();
                let timed_out = matches!(
                    &outcome,
                    Outcome::Err(Error::Pool(PoolError {
                        kind: PoolErrorKind::Timeout,
                        ..
                    }))
                );
                let _ = waiter_recorder.set((instant_ms, timed_out));
            })
            .expect("spawn waiter");
        runtime.scheduler.lock().schedule(waiter, 0);

        runtime.run_with_auto_advance();

        // The waiter failed at exactly T+200ms: woken only by its own
        // deadline slices, never early.
        assert_eq!(
            waiter_result.get().copied(),
            Some((200, true)),
            "acquire timeout fires at the exact virtual deadline"
        );
        assert_eq!(pool.stats().timeouts, 1);
        // The holder ran to its own (much later) wake-up and released.
        assert_eq!(holder_result.get().copied(), Some(10_000));
    }

    #[test]
    fn lab_unhealthy_connection_is_replaced_on_the_first_checkout_after_failure() {
        let (mut runtime, pool, region) = lab_pool(
            PoolConfig::new(2)
                .test_on_checkout(true)
                .idle_timeout(100_000)
                .acquire_timeout(5_000),
        );
        let flag = Arc::new(AtomicBool::new(false));
        let checkout = Arc::new(OnceCell::<(u32, u64, u64)>::new());
        let checkout_recorder = Arc::clone(&checkout);
        let ping_flag = Arc::clone(&flag);

        let (task, _) = runtime
            .state
            .create_task(region, Budget::INFINITE, async move {
                let cx = Cx::current().expect("lab task has a cx");
                let factory_flag = Arc::clone(&ping_flag);
                let a = pool
                        .acquire(&cx, move || {
                            let flag_for_conn = Arc::clone(&factory_flag);
                            async move {
                                Outcome::Ok(MockConnection::with_ping_behavior(1, flag_for_conn))
                            }
                        })
                        .await
                        .expect("acquire healthy a");
                drop(a);
                // From this virtual instant on, the pooled connection pings
                // unhealthy.
                ping_flag.store(true, Ordering::Relaxed);
                asupersync::time::sleep(cx.now(), Duration::from_millis(1)).await;
                let b = pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) })
                    .await
                    .expect("acquire after failure");
                let stats = pool.stats();
                let _ = checkout_recorder.set((
                    b.id,
                    stats.connections_created,
                    stats.connections_closed,
                ));
            })
            .expect("spawn lab task");
        runtime.scheduler.lock().schedule(task, 0);

        runtime.run_with_auto_advance();

        // The dead connection was closed at checkout and a fresh one
        // created; the acquire itself still succeeded.
        assert_eq!(
            checkout.get().copied(),
            Some((2, 2, 1)),
            "first checkout after the health flip gets a replacement"
        );
    }

    #[test]
    fn lab_close_and_drain_wakes_waiters_immediately_and_completes_at_last_release() {
        let (mut runtime, pool, region) = lab_pool(
            PoolConfig::new(2)
                .test_on_checkout(false)
                .acquire_timeout(5_000),
        );
        let holder_done = Arc::new(OnceCell::<u64>::new());
        let holder_recorder = Arc::clone(&holder_done);
        let drainer_done = Arc::new(OnceCell::<(u64, bool)>::new());
        let drainer_recorder = Arc::clone(&drainer_done);
        let waiter_done = Arc::new(OnceCell::<(u64, bool)>::new());
        let waiter_recorder = Arc::clone(&waiter_done);

        let holder_pool = Arc::clone(&pool);
        let (holder, _) = runtime
            .state
            .create_task(region, Budget::INFINITE, async move {
                let cx = Cx::current().expect("lab task has a cx");
                let lease = holder_pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(1)) })
                    .await
                    .expect("holder acquires");
                asupersync::time::sleep(cx.now(), Duration::from_millis(10_000)).await;
                drop(lease);
                let _ = holder_recorder.set(cx.now().as_millis());
            })
            .expect("spawn holder");
        runtime.scheduler.lock().schedule(holder, 0);
        let drainer_pool = Arc::clone(&pool);
        let (drainer, _) = runtime
            .state
            .create_task(region, Budget::INFINITE, async move {
                let cx = Cx::current().expect("lab task has a cx");
                let outcome = drainer_pool.close_and_drain(&cx).await;
                let _ = drainer_recorder.set((cx.now().as_millis(), outcome.is_ok()));
            })
            .expect("spawn drainer");
        runtime.scheduler.lock().schedule(drainer, 0);
        let waiter_pool = Arc::clone(&pool);
        let (waiter, _) = runtime
            .state
            .create_task(region, Budget::INFINITE, async move {
                let cx = Cx::current().expect("lab task has a cx");
                let outcome = waiter_pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) })
                    .await;
                let instant_ms = cx.now().as_millis();
                let closed = matches!(
                    &outcome,
                    Outcome::Err(Error::Pool(PoolError {
                        kind: PoolErrorKind::Closed,
                        ..
                    }))
                );
                let _ = waiter_recorder.set((instant_ms, closed));
            })
            .expect("spawn waiter");
        // The waiter is only scheduled AFTER the close: its first (and only)
        // acquire attempt must observe the closed pool - no timer, no wake.

        // Deterministic phase 1: holder and drainer run at t=0 until parked.
        // The holder parks on its sleep; the drainer marks the pool closed
        // and parks on the drain latch while the holder's lease is active.
        runtime.run_until_idle();
        assert!(waiter_done.get().is_none(), "waiter has not run yet");
        assert!(
            drainer_done.get().is_none(),
            "drain waits for the active lease"
        );

        // Deterministic phase 2: the waiter runs at t=0 against the closed
        // pool and must fail immediately with `Closed`.
        runtime.scheduler.lock().schedule(waiter, 0);
        runtime.run_until_idle();

        assert_eq!(
            waiter_done.get().copied(),
            Some((0, true)),
            "waiter observes Closed immediately at close"
        );

        // Deterministic phase 3: jump exactly to the holder's wake-up. The
        // release retires the lease and the drain completes at that instant.
        runtime.advance_to_next_timer();
        runtime.run_until_idle();

        assert_eq!(holder_done.get().copied(), Some(10_000));
        assert_eq!(
            drainer_done.get().copied(),
            Some((10_000, true)),
            "drain completes exactly at the last release"
        );
    }

    // ------------------------------------------------------------------
    // Bounded schedule exploration under asupersync's lab runtime
    // (`bd-x6jl.3`).
    //
    // asupersync 0.4.10 exposes no complete DPOR enumerator: what it ships
    // is a deterministic lab runtime whose scheduler permutes task
    // interleavings by seed, a race-informed seed-sweep explorer
    // (`DporExplorer::explore`), per-run invariant checks, and
    // forced-schedule artifacts for exact replay of a failing seed. These
    // programs use exactly that: every explored run checks the pool's
    // interleaving oracles, violations are collected (never panicked inside
    // a run) and asserted after the sweep with the offending seeds printed.
    // `SQLMODEL_DPOR_SEED=<n>` re-runs a single seed for reproduction.
    //
    // Session identity-map note (per the bead): every state-mutating
    // `Session` operation takes `&mut self`, so two tasks cannot share one
    // Session through the public API at all — Rust's aliasing rules make
    // the identity-map interleaving problem unrepresentable without an
    // external futures-aware mutex, which the crate's design forbids in
    // favor of one Session per task over pooled connections. The pool
    // programs below therefore cover the realistic shared-state surface.
    // ------------------------------------------------------------------

    /// Collects oracle violations across explored runs; a run that records
    /// a violation still lets the sweep finish so the report can name every
    /// offending seed.
    #[derive(Default)]
    struct Violations {
        list: std::sync::Mutex<Vec<String>>,
    }

    impl Violations {
        fn lock(&self) -> std::sync::MutexGuard<'_, Vec<String>> {
            self.list
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }

        fn record(&self, message: String) {
            self.lock().push(message);
        }

        fn take(&self, label: &str, seeds: &[u64]) {
            let violations = self.lock();
            assert!(
                violations.is_empty(),
                "{label}: {} oracle violation(s) across explored schedules (seeds {seeds:?}):\n{}",
                violations.len(),
                violations.join("\n")
            );
        }
    }

    /// Single-seed replay hook: `SQLMODEL_DPOR_SEED=<n>` runs exactly one
    /// seed instead of the sweep.
    fn exploration_seed(base: u64, runs: usize) -> (u64, usize) {
        match std::env::var("SQLMODEL_DPOR_SEED") {
            Ok(seed) => {
                let seed = seed.parse::<u64>().unwrap_or_else(|error| {
                    panic!("SQLMODEL_DPOR_SEED must be an integer: {error}")
                });
                (seed, 1)
            }
            Err(_) => (base, runs),
        }
    }

    #[test]
    fn dpor_pool_close_and_drain_never_loses_wakeups_or_resurrects() {
        let (base_seed, runs) = exploration_seed(0x50F1_0001, 48);
        let mut explorer = DporExplorer::new(
            ExplorerConfig::new(base_seed, runs)
                .worker_count(1)
                .max_steps(2_000),
        );
        let violations = Arc::new(Violations::default());
        let report = explorer.explore(|runtime| {
            let pool = Arc::new(Pool::with_timer_driver(
                PoolConfig::new(2)
                    .test_on_checkout(false)
                    .acquire_timeout(60_000),
                runtime
                    .state
                    .timer_driver_handle()
                    .expect("lab runtime timer driver"),
            ));
            let region = runtime.state.create_root_region(Budget::INFINITE);
            let violations = Arc::new(Violations::default());
            // Outcome flags: u8 codes (0 pending, 1 ok, 2 closed, 3 other).
            let waiter_outcome = Arc::new(AtomicU64::new(0));
            let ok_after_close = Arc::new(AtomicU64::new(0));
            let holders_done = Arc::new(AtomicU64::new(0));
            let waiter_done = Arc::new(AtomicBool::new(false));
            let drain_done = Arc::new(AtomicBool::new(false));

            // Two holders: take both leases and release them after a short
            // virtual delay, so the drain always races the retirement.
            for holder in 0..2u64 {
                let (pool, violations, holders_done, ok_after_close) = (
                    Arc::clone(&pool),
                    Arc::clone(&violations),
                    Arc::clone(&holders_done),
                    Arc::clone(&ok_after_close),
                );
                let hold_ms = if holder == 0 { 5 } else { 3 };
                let (task, _) = runtime
                    .state
                    .create_task(region, Budget::INFINITE, async move {
                        let cx = Cx::current().expect("lab task cx");
                        match pool
                            .acquire(&cx, || async {
                                Outcome::Ok(MockConnection::new(
                                    u32::try_from(holder).expect("holder fits u32") + 1,
                                ))
                            })
                            .await
                        {
                            Outcome::Ok(lease) => {
                                if pool.is_closed() {
                                    ok_after_close.fetch_add(1, Ordering::Relaxed);
                                }
                                asupersync::time::sleep(cx.now(), Duration::from_millis(hold_ms))
                                    .await;
                                drop(lease);
                            }
                            other => violations.record(format!(
                                "holder {holder}: acquire before close returned {other:?}"
                            )),
                        }
                        holders_done.fetch_add(1, Ordering::Relaxed);
                    })
                    .expect("spawn holder");
                runtime.scheduler.lock().schedule(task, 0);
            }

            // The waiter: acquires while both leases are held, so it can only
            // ever end with a lease from before the close or the closed error.
            {
                let (pool, violations, ok_after_close) = (
                    Arc::clone(&pool),
                    Arc::clone(&violations),
                    Arc::clone(&ok_after_close),
                );
                let (waiter_outcome, waiter_done) =
                    (Arc::clone(&waiter_outcome), Arc::clone(&waiter_done));
                let (task, _) = runtime
                    .state
                    .create_task(region, Budget::INFINITE, async move {
                        let cx = Cx::current().expect("lab task cx");
                        asupersync::time::sleep(cx.now(), Duration::from_millis(1)).await;
                        match pool
                            .acquire(&cx, || async { Outcome::Ok(MockConnection::new(9)) })
                            .await
                        {
                            Outcome::Ok(lease) => {
                                if pool.is_closed() {
                                    ok_after_close.fetch_add(1, Ordering::Relaxed);
                                }
                                drop(lease);
                                waiter_outcome.store(1, Ordering::Relaxed);
                            }
                            Outcome::Err(Error::Pool(PoolError {
                                kind: PoolErrorKind::Closed,
                                ..
                            })) => waiter_outcome.store(2, Ordering::Relaxed),
                            other => {
                                waiter_outcome.store(3, Ordering::Relaxed);
                                violations.record(format!("waiter: unexpected outcome {other:?}"));
                            }
                        }
                        waiter_done.store(true, Ordering::Relaxed);
                    })
                    .expect("spawn waiter");
                runtime.scheduler.lock().schedule(task, 0);
            }

            // The drainer: closes admission and waits for both retirements.
            {
                let (pool, violations, drain_done) = (
                    Arc::clone(&pool),
                    Arc::clone(&violations),
                    Arc::clone(&drain_done),
                );
                let (task, _) = runtime
                    .state
                    .create_task(region, Budget::INFINITE, async move {
                        let cx = Cx::current().expect("lab task cx");
                        let outcome = pool.close_and_drain(&cx).await;
                        if !matches!(outcome, Outcome::Ok(())) {
                            violations.record(format!("drain: expected Ok, got {outcome:?}"));
                        }
                        drain_done.store(true, Ordering::Relaxed);
                    })
                    .expect("spawn drainer");
                runtime.scheduler.lock().schedule(task, 0);
            }

            runtime.run_with_auto_advance();

            // Oracles (a), (b), (c): everyone finished, the drain completed,
            // the pool stayed closed, and no lease crossed the close line.
            if holders_done.load(Ordering::Relaxed) != 2 {
                violations.record(format!(
                    "holders did not all finish (hang or step exhaustion): {}",
                    holders_done.load(Ordering::Relaxed)
                ));
            }
            if !waiter_done.load(Ordering::Relaxed) {
                violations
                    .record("waiter did not finish (blocked forever after close?)".to_owned());
            }
            if waiter_outcome.load(Ordering::Relaxed) == 0 {
                violations.record("waiter outcome never recorded".to_owned());
            }
            if !drain_done.load(Ordering::Relaxed) {
                violations.record("close_and_drain never completed".to_owned());
            }
            if !pool.is_closed() {
                violations.record("pool not closed after drain".to_owned());
            }
            if ok_after_close.load(Ordering::Relaxed) > 0 {
                violations.record("a lease was handed out after the pool closed".to_owned());
            }
            // Oracle (e): stats invariants after the drain. Every lease was
            // either already dropped pre-close (idle) or retired post-close
            // (closed), so the pool must be empty with balanced counters.
            let stats = pool.stats();
            if stats.total_connections != 0
                || stats.idle_connections != 0
                || stats.active_connections != 0
            {
                violations.record(format!("stats: pool did not drain empty: {stats:?}"));
            }
            if stats.connections_created != 2 || stats.connections_closed != 2 {
                violations.record(format!(
                    "stats: created/closed unbalanced (2 expected): {stats:?}"
                ));
            }
        });

        eprintln!(
            "dpor pool drain: {} runs, {} schedule classes",
            report.total_runs, report.unique_classes
        );
        // DporExplorer may stop before `runs` when race-guided candidate
        // derivation is exhausted (sleep-set dedup prunes the rest); require
        // meaningful exploration rather than an exact count.
        assert!(
            report.total_runs >= 8,
            "exploration must actually sweep schedules, ran {}",
            report.total_runs
        );
        let seeds = report.violation_seeds();
        violations.take("dpor pool close_and_drain", &seeds);
        assert!(
            !report.has_violations(),
            "lab runtime invariants violated (seeds {seeds:?})"
        );
    }

    /// A connection whose `begin_with` fails with a retryable
    /// serialization error for the first `failures` attempts (shared
    /// counter, so two concurrent callers drain the budget together —
    /// exactly the interleaving that decides who exhausts their retries).
    struct FlakyBegin {
        inner: MockConnection,
        failures_left: Arc<AtomicU64>,
    }

    impl FlakyBegin {
        fn new(failures: u64) -> Self {
            Self {
                inner: MockConnection::new(70 + u32::try_from(failures).expect("failures fit u32")),
                failures_left: Arc::new(AtomicU64::new(failures)),
            }
        }
    }

    impl Connection for FlakyBegin {
        type Tx<'conn>
            = <MockConnection as Connection>::Tx<'conn>
        where
            Self: 'conn;

        fn dialect(&self) -> Dialect {
            self.inner.dialect()
        }

        async fn begin_with(
            &self,
            cx: &Cx,
            isolation: IsolationLevel,
        ) -> Outcome<Self::Tx<'_>, Error> {
            if self.failures_left.load(Ordering::Relaxed) > 0 {
                self.failures_left.fetch_sub(1, Ordering::Relaxed);
                return Outcome::Err(Error::Query(QueryError {
                    kind: QueryErrorKind::Serialization,
                    sql: Some("BEGIN".to_owned()),
                    sqlstate: Some("40001".to_owned()),
                    message: "injected serialization failure".to_owned(),
                    detail: None,
                    hint: None,
                    position: None,
                    source: None,
                }));
            }
            self.inner.begin_with(cx, isolation).await
        }

        async fn query(&self, cx: &Cx, sql: &str, params: &[Value]) -> Outcome<Vec<Row>, Error> {
            self.inner.query(cx, sql, params).await
        }

        async fn query_one(
            &self,
            cx: &Cx,
            sql: &str,
            params: &[Value],
        ) -> Outcome<Option<Row>, Error> {
            self.inner.query_one(cx, sql, params).await
        }

        async fn execute(&self, cx: &Cx, sql: &str, params: &[Value]) -> Outcome<u64, Error> {
            self.inner.execute(cx, sql, params).await
        }

        async fn insert(&self, cx: &Cx, sql: &str, params: &[Value]) -> Outcome<i64, Error> {
            self.inner.insert(cx, sql, params).await
        }

        async fn batch(
            &self,
            cx: &Cx,
            statements: &[(String, Vec<Value>)],
        ) -> Outcome<Vec<u64>, Error> {
            self.inner.batch(cx, statements).await
        }

        async fn begin(&self, cx: &Cx) -> Outcome<Self::Tx<'_>, Error> {
            self.begin_with(cx, IsolationLevel::ReadCommitted).await
        }

        fn supports_transaction_mode(&self, mode: TransactionMode) -> bool {
            self.inner.supports_transaction_mode(mode)
        }

        async fn commit_transaction(&self, cx: &Cx) -> Outcome<(), Error> {
            self.inner.commit_transaction(cx).await
        }

        async fn rollback_transaction(&self, cx: &Cx) -> Outcome<(), Error> {
            self.inner.rollback_transaction(cx).await
        }

        async fn prepare(&self, cx: &Cx, sql: &str) -> Outcome<PreparedStatement, Error> {
            self.inner.prepare(cx, sql).await
        }

        async fn query_prepared(
            &self,
            cx: &Cx,
            stmt: &PreparedStatement,
            params: &[Value],
        ) -> Outcome<Vec<Row>, Error> {
            self.inner.query_prepared(cx, stmt, params).await
        }

        async fn execute_prepared(
            &self,
            cx: &Cx,
            stmt: &PreparedStatement,
            params: &[Value],
        ) -> Outcome<u64, Error> {
            self.inner.execute_prepared(cx, stmt, params).await
        }

        async fn ping(&self, cx: &Cx) -> Outcome<(), Error> {
            self.inner.ping(cx).await
        }

        async fn validate_for_pool(&self, cx: &Cx, check_connection: bool) -> Outcome<(), Error> {
            self.inner.validate_for_pool(cx, check_connection).await
        }

        async fn close(self, cx: &Cx) -> Result<(), Error> {
            self.inner.close(cx).await
        }

        async fn close_for_pool(self, cx: &Cx) -> Result<(), Error>
        where
            Self: Sized,
        {
            self.inner.close_for_pool(cx).await
        }
    }

    /// Two concurrent `retry_transaction` callers share one connection whose
    /// begin path fails with a retryable serialization error `failures`
    /// times. Oracles: every caller ends in `Ok` or `RetriesExhausted`,
    /// never hangs, and the lab invariants hold under every explored
    /// schedule.
    #[test]
    fn dpor_retry_combinator_callers_always_terminate() {
        for (failures, expect_ok) in [(2, true), (11, false)] {
            let (base_seed, runs) = exploration_seed(0x50F1_0002 + failures, 32);
            let mut explorer = DporExplorer::new(
                ExplorerConfig::new(base_seed, runs)
                    .worker_count(1)
                    .max_steps(4_000),
            );
            let violations = Arc::new(Violations::default());
            let report = explorer.explore(|runtime| {
                let conn = Arc::new(FlakyBegin::new(failures));
                let region = runtime.state.create_root_region(Budget::INFINITE);
                let violations = Arc::new(Violations::default());
                let callers_done = Arc::new(AtomicU64::new(0));
                let oks = Arc::new(AtomicU64::new(0));
                let exhausted = Arc::new(AtomicU64::new(0));

                for caller in 0..2u64 {
                    let (conn, violations, callers_done, oks, exhausted) = (
                        Arc::clone(&conn),
                        Arc::clone(&violations),
                        Arc::clone(&callers_done),
                        Arc::clone(&oks),
                        Arc::clone(&exhausted),
                    );
                    let (task, _) = runtime
                        .state
                        .create_task(region, Budget::INFINITE, async move {
                            let cx = Cx::current().expect("lab task cx");
                            let outcome = retry_transaction(
                                &cx,
                                conn.as_ref(),
                                TransactionOptions::new(),
                                &RetryPolicy::default(),
                                async |cx: &Cx, tx| {
                                    tx.execute(cx, "INSERT INTO t VALUES (1)", &[])
                                        .await
                                        .map(|_| ())
                                },
                            )
                            .await;
                            match outcome {
                                Outcome::Ok(()) => {
                                    oks.fetch_add(1, Ordering::Relaxed);
                                }
                                Outcome::Err(Error::Transaction(e))
                                    if e.kind == TransactionErrorKind::RetriesExhausted =>
                                {
                                    exhausted.fetch_add(1, Ordering::Relaxed);
                                }
                                Outcome::Err(e) => violations.record(format!(
                                    "retry caller {caller}: unexpected Err {e:?}"
                                )),
                                Outcome::Cancelled(r) => violations.record(format!(
                                    "retry caller {caller}: unexpected Cancelled {r:?}"
                                )),
                                Outcome::Panicked(p) => violations.record(format!(
                                    "retry caller {caller}: panicked {p:?}"
                                )),
                            }
                            callers_done.fetch_add(1, Ordering::Relaxed);
                        })
                        .expect("spawn retry caller");
                    runtime.scheduler.lock().schedule(task, 0);
                }

                runtime.run_with_auto_advance();

                if callers_done.load(Ordering::Relaxed) != 2 {
                    violations.record(format!(
                        "retry callers did not finish (hung future?): {}",
                        callers_done.load(Ordering::Relaxed)
                    ));
                }
                let oks = oks.load(Ordering::Relaxed);
                let exhausted = exhausted.load(Ordering::Relaxed);
                if expect_ok && oks != 2 {
                    violations.record(format!(
                        "with {failures} injected failures both callers must eventually commit, got {oks} ok"
                    ));
                }
                if !expect_ok && oks + exhausted != 2 {
                    violations.record(format!(
                        "with {failures} injected failures every caller must end Ok or exhausted, got {oks} ok / {exhausted} exhausted"
                    ));
                }
            });

            eprintln!(
                "dpor retry ({failures} injected failures): {} runs, {} classes",
                report.total_runs, report.unique_classes
            );
            assert!(
                report.total_runs >= 8,
                "exploration must actually sweep schedules, ran {}",
                report.total_runs
            );
            let seeds = report.violation_seeds();
            violations.take(
                &format!("dpor retry ({failures} injected failures)"),
                &seeds,
            );
            assert!(
                !report.has_violations(),
                "lab runtime invariants violated (seeds {seeds:?})"
            );
            let _ = expect_ok;
        }
    }

    // ------------------------------------------------------------------
    // Budget semantics (`bd-x6jl.4`, part 4a): the effective acquire wait
    // deadline is min(acquire_timeout, Cx budget deadline). A budget
    // smaller than `acquire_timeout` wins; a larger budget never extends
    // the pool's own timeout. Verified at the exact virtual instant.
    // ------------------------------------------------------------------

    #[test]
    fn lab_acquire_budget_deadline_wins_over_acquire_timeout() {
        let mut runtime = LabRuntime::new(LabConfig::new(0x50F1_0004).max_steps(200_000));
        let driver = runtime
            .state
            .timer_driver_handle()
            .expect("lab runtime timer driver");
        let pool = Arc::new(Pool::with_timer_driver(
            PoolConfig::new(1)
                .acquire_timeout(5_000)
                .test_on_checkout(false),
            driver,
        ));
        let outcome_record = Arc::new(std::sync::Mutex::new(None::<(u64, bool, bool)>));
        let region = runtime.state.create_root_region(Budget::INFINITE);
        // The sole lease is taken and held well past the budget deadline.
        let holder_pool = Arc::clone(&pool);
        let (holder_task, _) = runtime
            .state
            .create_task(region, Budget::INFINITE, async move {
                let cx = Cx::current().expect("lab task cx");
                let lease = holder_pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(1)) })
                    .await
                    .expect("holder acquires the only lease");
                asupersync::time::sleep(cx.now(), Duration::from_secs(30)).await;
                drop(lease);
            })
            .expect("spawn holder");
        runtime.scheduler.lock().schedule(holder_task, 0);

        // The waiter's task budget expires at virtual T+2000ms: acquire must
        // return the budget-attributed timeout at exactly that instant, even
        // though `acquire_timeout` is 5000ms.
        let waiter_pool = Arc::clone(&pool);
        let waiter_budget = Budget::new().with_deadline(Time::from_millis(2_000));
        let outcome_writer = Arc::clone(&outcome_record);
        let (waiter_task, _) = runtime
            .state
            .create_task(region, waiter_budget, async move {
                let cx = Cx::current().expect("lab task cx");
                let outcome = waiter_pool
                    .acquire(&cx, || async { Outcome::Ok(MockConnection::new(2)) })
                    .await;
                let instant = cx.now().as_millis();
                match &outcome {
                    Outcome::Err(Error::Pool(PoolError {
                        kind: PoolErrorKind::Timeout,
                        message,
                        ..
                    })) => {
                        let budget_limited = message.contains("budget deadline");
                        *outcome_writer.lock().unwrap() = Some((instant, true, budget_limited));
                    }
                    _ => *outcome_writer.lock().unwrap() = Some((instant, false, false)),
                }
            })
            .expect("spawn waiter");
        runtime.scheduler.lock().schedule(waiter_task, 0);

        runtime.run_with_auto_advance();

        let (instant, timed_out, budget_limited) = outcome_record
            .lock()
            .unwrap()
            .expect("waiter recorded an outcome");
        assert_eq!(instant, 2_000, "budget deadline fires at exactly T+2000ms");
        assert!(timed_out, "waiter must observe PoolErrorKind::Timeout");
        assert!(
            budget_limited,
            "the timeout message must attribute the budget"
        );
        assert_eq!(pool.stats().timeouts, 1);
    }
}
