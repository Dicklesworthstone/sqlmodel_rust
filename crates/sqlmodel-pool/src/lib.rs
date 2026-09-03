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
//! With `test_on_checkout` (the default) every idle connection is pinged before
//! it is handed out; one that fails is closed and replaced transparently, so a
//! server-side kill of an idle connection costs one reconnect, never an error.
//! Returning a connection runs no check (a return is a synchronous `Drop`), so
//! a lease whose statement failed with a connection error should be
//! [`PooledConnection::detach`]ed rather than dropped back into the pool;
//! dropped, it is handed out again and fails its next statement. The e2e pool
//! scenario asserts both behaviours against PostgreSQL and MySQL.
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
    Budget, CancelReason, Cx, Outcome,
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
    /// Maximum time to wait for a connection in milliseconds
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

    /// Set acquire timeout in milliseconds.
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
    /// crate-level "Health-check rule"). There is no test on return: a return
    /// is a synchronous `Drop`; detach a lease you know to be dead instead.
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
    created_at: asupersync::time::Time,
    /// When this connection was last used
    last_used: asupersync::time::Time,
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
    /// completed. Keeping the slot active through close is what makes
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

#[allow(clippy::result_large_err)] // Error is the crate's rich public error type.
fn close_connection_blocking<C: Connection>(conn: C, context: &'static str) -> Result<(), Error> {
    let runtime = match RuntimeBuilder::current_thread().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::warn!(
                context,
                error = %error,
                "failed to build runtime while closing pooled connection"
            );
            drop(conn);
            return Err(Error::Custom(format!(
                "failed to build runtime while closing pooled connection: {error}"
            )));
        }
    };
    let cx = runtime.request_cx_with_budget(Budget::INFINITE);
    let result = runtime.block_on(async { conn.close_for_pool(&cx).await });
    if let Err(error) = &result {
        tracing::warn!(
            context,
            error = %error,
            "failed to close pooled connection explicitly"
        );
    }
    result
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

/// Owns an active connection while checkout validation is in flight.
///
/// On hard cancellation the guard drops the connection resource before it
/// releases the active slot, preserving the drain boundary without requiring
/// asynchronous work from `Drop`.
struct ActiveConnectionGuard<C: Connection> {
    pool: Arc<PoolShared<C>>,
    meta: Option<ConnectionMeta<C>>,
    armed: bool,
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
        // Async Drop is impossible. Dropping the resource itself is the
        // hard-cancellation fallback; do that before releasing its pool slot so
        // a concurrent drainer cannot observe quiescence too early.
        drop(self.meta.take());
        self.release("checkout validation future drop");
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
    /// This method will:
    /// 1. Return an idle connection if one is available
    /// 2. Create a new connection if below capacity
    /// 3. Wait for a connection to become available (up to timeout)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The pool is closed
    /// - The acquire timeout is exceeded
    /// - Cancellation is requested via the `Cx` context
    ///
    /// An idle connection that fails its checkout ping (`test_on_checkout`)
    /// is closed and the acquire moves on to the next idle connection or a
    /// new one; it is not an error.
    pub async fn acquire<F, Fut>(&self, cx: &Cx, factory: F) -> Outcome<PooledConnection<C>, Error>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Outcome<C, Error>>,
    {
        let clock = self.shared.clock.clone();
        let deadline = clock.now() + Duration::from_millis(self.config().acquire_timeout_ms);
        let test_on_checkout = self.config().test_on_checkout;
        let max_lifetime = Duration::from_millis(self.config().max_lifetime_ms);
        let idle_timeout = Duration::from_millis(self.config().idle_timeout_ms);

        loop {
            // Check cancellation
            if cx.is_cancel_requested() {
                return Outcome::Cancelled(CancelReason::user("pool acquire cancelled"));
            }

            // Check timeout
            if clock.now() >= deadline {
                self.shared.timeouts.fetch_add(1, Ordering::Relaxed);
                return Outcome::Err(Error::Pool(PoolError {
                    kind: PoolErrorKind::Timeout,
                    message: "acquire timeout: no connections available".to_string(),
                    source: None,
                }));
            }

            // Try to get an idle connection or determine if we can create new
            let (action, retired) = {
                let mut inner = match self.shared.lock_or_error("acquire") {
                    Ok(guard) => guard,
                    Err(e) => return Outcome::Err(e),
                };
                // Reserve before moving any idle entry into active retirement
                // accounting. Every removed entry is immediately protected by
                // an armed guard, so panic or hard cancellation cannot strand
                // later entries in a raw vector.
                let mut retired = Vec::with_capacity(inner.idle.len());

                let action = if inner.closed {
                    AcquireAction::PoolClosed
                } else {
                    // Try to get an idle connection
                    let mut found_conn = None;
                    while let Some(mut meta) = inner.idle.pop_front() {
                        // Check if connection is too old
                        if meta.age() > max_lifetime {
                            inner.active_count += 1;
                            retired
                                .push(ActiveConnectionGuard::new(Arc::clone(&self.shared), meta));
                            continue;
                        }

                        // Check if connection has been idle too long
                        if meta.idle_time() > idle_timeout {
                            inner.active_count += 1;
                            retired
                                .push(ActiveConnectionGuard::new(Arc::clone(&self.shared), meta));
                            continue;
                        }

                        if !retired.is_empty() {
                            // Retire removed connections before selecting or
                            // reserving an active slot. Otherwise dropping the
                            // acquire future during an async retirement close
                            // would strand that selected slot forever.
                            inner.idle.push_front(meta);
                            break;
                        }

                        // Found a valid connection
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
                        // No idle connections, can we create new?
                        inner.total_count += 1;
                        inner.active_count += 1;
                        AcquireAction::CreateNew
                    } else {
                        // Must wait
                        inner.waiter_count += 1;
                        AcquireAction::Wait
                    }
                };
                (action, retired)
            };

            // Teardown may perform driver I/O. Keep it outside the pool mutex
            // so one slow close cannot block returns, acquires, or shutdown.
            for guard in retired {
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
                AcquireAction::RetireAndRetry => {
                    continue;
                }
                AcquireAction::PoolClosed => {
                    return Outcome::Err(Error::Pool(PoolError {
                        kind: PoolErrorKind::Closed,
                        message: "pool has been closed".to_string(),
                        source: None,
                    }));
                }
                AcquireAction::ValidateExisting(meta) => {
                    // Validate and wrap the connection (lock is released). A
                    // dead idle connection has been closed by now; go round
                    // again for the next idle one or a fresh connection.
                    match self.validate_and_wrap(cx, meta, test_on_checkout).await {
                        Outcome::Ok(Some(pooled)) => return Outcome::Ok(pooled),
                        Outcome::Ok(None) => {
                            tracing::warn!(
                                "pooled connection failed its checkout ping; closed and replaced"
                            );
                            continue;
                        }
                        Outcome::Err(e) => return Outcome::Err(e),
                        Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                        Outcome::Panicked(p) => return Outcome::Panicked(p),
                    }
                }
                AcquireAction::CreateNew => {
                    // Create new connection outside of lock
                    let mut slot_guard = ActiveSlotGuard::new(Arc::clone(&self.shared));
                    match factory().await {
                        Outcome::Ok(conn) => {
                            self.shared
                                .connections_created
                                .fetch_add(1, Ordering::Relaxed);
                            let publish = match self.shared.lock_or_error("acquire_publish") {
                                Ok(inner) if inner.closed => {
                                    let retirement = ActiveConnectionGuard::new(
                                        Arc::clone(&self.shared),
                                        ConnectionMeta::new(conn, Arc::clone(&clock)),
                                    );
                                    slot_guard.disarm();
                                    FactoryPublish::Retire {
                                        guard: retirement,
                                        error: Error::Pool(PoolError {
                                            kind: PoolErrorKind::Closed,
                                            message: "pool has been closed".to_string(),
                                            source: None,
                                        }),
                                        context: "acquire publish after pool closure",
                                    }
                                }
                                Ok(_inner) => {
                                    // Disarming while still holding the pool
                                    // mutex is the admission publication point.
                                    // A close either precedes this check and
                                    // rejects the connection, or follows it and
                                    // waits for this active checkout.
                                    self.shared.acquires.fetch_add(1, Ordering::Relaxed);
                                    let meta = ConnectionMeta::new(conn, Arc::clone(&clock));
                                    slot_guard.disarm();
                                    FactoryPublish::Published(PooledConnection::new(
                                        meta,
                                        Arc::downgrade(&self.shared),
                                    ))
                                }
                                Err(error) => {
                                    let retirement = ActiveConnectionGuard::new(
                                        Arc::clone(&self.shared),
                                        ConnectionMeta::new(conn, Arc::clone(&clock)),
                                    );
                                    slot_guard.disarm();
                                    FactoryPublish::Retire {
                                        guard: retirement,
                                        error,
                                        context: "acquire publish bookkeeping failure",
                                    }
                                }
                            };
                            match publish {
                                FactoryPublish::Published(pooled) => {
                                    return Outcome::Ok(pooled);
                                }
                                FactoryPublish::Retire {
                                    guard,
                                    error,
                                    context,
                                } => match guard.close(cx, context).await {
                                    RetirementOutcome::Closed => return Outcome::Err(error),
                                    RetirementOutcome::Cancelled(reason) => {
                                        return Outcome::Cancelled(reason);
                                    }
                                    RetirementOutcome::Failed(close_error) => {
                                        return Outcome::Err(close_error);
                                    }
                                },
                            }
                        }
                        Outcome::Err(e) => {
                            slot_guard.release("connection factory error");
                            return Outcome::Err(e);
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
                AcquireAction::Wait => {
                    // Wait for a connection to become available
                    let remaining =
                        Duration::from_nanos(deadline.as_nanos().saturating_sub(clock.now().as_nanos()));
                    if remaining.is_zero() {
                        if let Ok(mut inner) = self.shared.lock_or_error("acquire_timeout") {
                            inner.waiter_count -= 1;
                        }
                        self.shared.timeouts.fetch_add(1, Ordering::Relaxed);
                        return Outcome::Err(Error::Pool(PoolError {
                            kind: PoolErrorKind::Timeout,
                            message: "acquire timeout: no connections available".to_string(),
                            source: None,
                        }));
                    }

                    // Wait for a return notification or a short slice of the
                    // deadline (the slice keeps cancellation checks frequent),
                    // yielding to the runtime instead of blocking its thread.
                    // A release between the bookkeeping above and this await is
                    // not lost: `Notify` stores a `notify_one` permit.
                    let wait_time = remaining.min(Duration::from_millis(100));
                    {
                        let mut notified = std::pin::pin!(self.shared.conn_available.notified());
                        let mut slice =
                            std::pin::pin!(asupersync::time::sleep(cx.now(), wait_time));
                        let _ = Select::new(notified.as_mut(), slice.as_mut()).await;
                    }

                    // Decrement waiter count after waking
                    {
                        if let Ok(mut inner) = self.shared.lock_or_error("acquire_wake") {
                            inner.waiter_count = inner.waiter_count.saturating_sub(1);
                        }
                    }

                    // Loop back to try again
                }
            }
        }
    }

    /// Validate a connection and wrap it in a PooledConnection.
    /// Ping (when asked) and hand out an idle connection. `Ok(None)` means the
    /// ping failed and the connection has been closed; the caller picks
    /// another.
    async fn validate_and_wrap(
        &self,
        cx: &Cx,
        meta: ConnectionMeta<C>,
        test_on_checkout: bool,
    ) -> Outcome<Option<PooledConnection<C>>, Error> {
        let guard = ActiveConnectionGuard::new(Arc::clone(&self.shared), meta);
        if test_on_checkout {
            // Validate the connection
            match guard.connection().ping(cx).await {
                Outcome::Ok(()) => {
                    self.shared.acquires.fetch_add(1, Ordering::Relaxed);
                    Outcome::Ok(Some(PooledConnection::new(
                        guard.into_meta(),
                        Arc::downgrade(&self.shared),
                    )))
                }
                Outcome::Err(_) | Outcome::Cancelled(_) | Outcome::Panicked(_) => {
                    // Connection is invalid. Keep it active until explicit
                    // retirement completes so close-and-drain cannot return
                    // while the driver still owns the resource.
                    match guard
                        .close(cx, "pooled connection checkout validation failure")
                        .await
                    {
                        RetirementOutcome::Closed => {}
                        RetirementOutcome::Cancelled(reason) => {
                            return Outcome::Cancelled(reason);
                        }
                        RetirementOutcome::Failed(error) => return Outcome::Err(error),
                    }
                    Outcome::Ok(None)
                }
            }
        } else {
            self.shared.acquires.fetch_add(1, Ordering::Relaxed);
            Outcome::Ok(Some(PooledConnection::new(
                guard.into_meta(),
                Arc::downgrade(&self.shared),
            )))
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
        &self,
        retired: Vec<ActiveConnectionGuard<C>>,
        context: &'static str,
    ) {
        for guard in retired {
            let runtime = match RuntimeBuilder::current_thread().build() {
                Ok(runtime) => runtime,
                Err(error) => {
                    tracing::warn!(
                        context,
                        error = %error,
                        "failed to build runtime while retiring pooled connection"
                    );
                    let error = Error::Custom(format!(
                        "failed to build runtime while retiring pooled connection: {error}"
                    ));
                    self.shared.record_retirement_failure(context, &error);
                    drop(guard);
                    continue;
                }
            };
            // Runtime-owned request context with no deadline: retirement must not be
            // cut short, and `Cx::for_testing` only exists behind asupersync's
            // `test-internals` feature (it compiled here purely through dev-dependency
            // feature unification, and broke `cargo doc`).
            let cx = runtime.request_cx_with_budget(Budget::INFINITE);
            let _ = runtime.block_on(guard.close(&cx, context));
        }
    }

    /// Close all currently idle connections.
    ///
    /// If the pool mutex is poisoned, this logs an error and leaves the idle
    /// inventory untouched because its accounting cannot be mutated safely.
    pub fn clear_idle(&self) {
        let retired = self.begin_idle_retirement();
        self.close_retirements_blocking(retired, "pool clear_idle");
    }

    /// Close the pool, preventing new connections and closing all idle connections.
    ///
    /// If the pool mutex is poisoned, this logs an error but still wakes waiters.
    pub fn close(&self) {
        let retired = self.begin_close();
        self.close_retirements_blocking(retired, "pool close");

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
    /// when returned; this future completes only after their explicit
    /// `close_for_pool` hooks finish and the active count reaches zero.
    ///
    /// The wait is cancellation- and deadline-aware through `cx`. Cancellation
    /// returns [`Outcome::Cancelled`] without reopening the pool: `closed`
    /// remains a one-way lifecycle transition, and a later caller may resume
    /// draining. Dropping this future has the same persistent-close property.
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
enum AcquireAction<C> {
    /// Expired idle connections were removed and must retire before retrying.
    RetireAndRetry,
    /// Pool is closed
    PoolClosed,
    /// Found an existing connection to validate
    ValidateExisting(ConnectionMeta<C>),
    /// Create a new connection
    CreateNew,
    /// Wait for a connection to become available
    Wait,
}

enum FactoryPublish<C: Connection> {
    Published(PooledConnection<C>),
    Retire {
        guard: ActiveConnectionGuard<C>,
        error: Error,
        context: &'static str,
    },
}

/// A connection borrowed from the pool.
///
/// When dropped, the connection is automatically returned to the pool, also
/// while a panic unwinds the holder. The connection can be used via `Deref`
/// and `DerefMut`.
pub struct PooledConnection<C: Connection> {
    /// The connection metadata (Some while held, None after return)
    meta: Option<ConnectionMeta<C>>,
    /// Weak reference to pool for returning
    pool: Weak<PoolShared<C>>,
}

impl<C: Connection> PooledConnection<C> {
    fn new(meta: ConnectionMeta<C>, pool: Weak<PoolShared<C>>) -> Self {
        Self {
            meta: Some(meta),
            pool,
        }
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
                        if let Err(error) = close_connection_blocking(meta.conn, context) {
                            pool.record_retirement_failure(context, &error);
                        }
                        pool.connections_closed.fetch_add(1, Ordering::Relaxed);
                        pool.release_active_slot(context);
                        return;
                    }
                };

                if inner.closed {
                    drop(inner);
                    let context = "pooled connection drop closed pool";
                    if let Err(error) = close_connection_blocking(meta.conn, context) {
                        pool.record_retirement_failure(context, &error);
                    }
                    pool.connections_closed.fetch_add(1, Ordering::Relaxed);
                    pool.release_active_slot("pooled connection drop closed pool");
                    return;
                }

                // Check max lifetime
                let max_lifetime = Duration::from_millis(inner.config.max_lifetime_ms);
                if meta.age() > max_lifetime {
                    drop(inner);
                    let context = "pooled connection drop max lifetime";
                    if let Err(error) = close_connection_blocking(meta.conn, context) {
                        pool.record_retirement_failure(context, &error);
                    }
                    pool.connections_closed.fetch_add(1, Ordering::Relaxed);
                    pool.release_active_slot("pooled connection drop max lifetime");
                    return;
                }

                inner.active_count -= 1;
                inner.idle.push_back(meta);

                drop(inner);
                pool.conn_available.notify_one();
            } else {
                let _ = close_connection_blocking(meta.conn, "pooled connection drop missing pool");
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
    use asupersync::time::VirtualClock;
    use asupersync::{Budget, Time};
    use sqlmodel_core::connection::{IsolationLevel, PreparedStatement, TransactionOps};
    use sqlmodel_core::error::{ConnectionError, ConnectionErrorKind};
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

    /// A mock connection for testing pool behavior.
    #[derive(Debug)]
    struct MockConnection {
        id: u32,
        ping_should_fail: Arc<AtomicBool>,
        /// Incremented each time the pool retires this connection via
        /// `close_for_pool` (as opposed to a caller-owned `close`).
        pool_close_calls: Arc<AtomicUsize>,
        /// When true, `close_for_pool` remains pending until its future is
        /// dropped. Used to prove slot guards across close await points.
        pool_close_pending: bool,
        /// When true, pool retirement returns a deterministic driver error.
        pool_close_should_fail: bool,
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
                pool_close_calls: Arc::new(AtomicUsize::new(0)),
                pool_close_pending: false,
                pool_close_should_fail: false,
                pool_shared: None,
                pool_lock_was_free: None,
            }
        }

        #[allow(dead_code)]
        fn with_ping_behavior(id: u32, should_fail: Arc<AtomicBool>) -> Self {
            Self {
                id,
                ping_should_fail: should_fail,
                pool_close_calls: Arc::new(AtomicUsize::new(0)),
                pool_close_pending: false,
                pool_close_should_fail: false,
                pool_shared: None,
                pool_lock_was_free: None,
            }
        }

        fn with_pool_close_counter(id: u32, pool_close_calls: Arc<AtomicUsize>) -> Self {
            Self {
                id,
                ping_should_fail: Arc::new(AtomicBool::new(false)),
                pool_close_calls,
                pool_close_pending: false,
                pool_close_should_fail: false,
                pool_shared: None,
                pool_lock_was_free: None,
            }
        }

        fn with_pool_close_probe(
            id: u32,
            pool_close_calls: Arc<AtomicUsize>,
            pool_shared: Weak<PoolShared<MockConnection>>,
            pool_lock_was_free: Arc<AtomicBool>,
        ) -> Self {
            Self {
                id,
                ping_should_fail: Arc::new(AtomicBool::new(false)),
                pool_close_calls,
                pool_close_pending: false,
                pool_close_should_fail: false,
                pool_shared: Some(pool_shared),
                pool_lock_was_free: Some(pool_lock_was_free),
            }
        }

        fn with_pending_pool_close(id: u32, pool_close_calls: Arc<AtomicUsize>) -> Self {
            Self {
                id,
                ping_should_fail: Arc::new(AtomicBool::new(false)),
                pool_close_calls,
                pool_close_pending: true,
                pool_close_should_fail: false,
                pool_shared: None,
                pool_lock_was_free: None,
            }
        }

        fn with_failing_pool_close(id: u32) -> Self {
            Self {
                id,
                ping_should_fail: Arc::new(AtomicBool::new(false)),
                pool_close_calls: Arc::new(AtomicUsize::new(0)),
                pool_close_pending: false,
                pool_close_should_fail: true,
                pool_shared: None,
                pool_lock_was_free: None,
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

        async fn ping(&self, _cx: &Cx) -> Outcome<(), Error> {
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

        async fn close(self, _cx: &Cx) -> Result<(), Error> {
            Ok(())
        }

        async fn close_for_pool(self, _cx: &Cx) -> Result<(), Error> {
            if let (Some(pool_shared), Some(pool_lock_was_free)) =
                (self.pool_shared.as_ref(), self.pool_lock_was_free.as_ref())
            {
                let mutex_is_available = pool_shared
                    .upgrade()
                    .is_none_or(|shared| shared.inner.try_lock().is_ok());
                pool_lock_was_free.store(mutex_is_available, Ordering::Relaxed);
            }
            self.pool_close_calls.fetch_add(1, Ordering::Relaxed);
            if self.pool_close_pending {
                std::future::pending::<()>().await;
            }
            if self.pool_close_should_fail {
                return Err(Error::Custom("mock pool close failure".to_string()));
            }
            Ok(())
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
            inner.idle.push_back(ConnectionMeta::new(MockConnection::with_failing_pool_close(1), test_clock()));
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
            ConnectionMeta::new(MockConnection::with_pool_close_counter(
                1,
                Arc::clone(&pool_close_calls),
            ), test_clock()),
            Arc::downgrade(&pool.shared),
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
        );
        let second = PooledConnection::new(
            ConnectionMeta::new(MockConnection::new(2), test_clock()),
            Arc::downgrade(&pool.shared),
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
            inner.idle.push_back(ConnectionMeta::new(MockConnection::with_pending_pool_close(1, Arc::clone(&pool_close_calls)), test_clock()));
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
            inner.idle.push_back(ConnectionMeta::new(failed, test_clock()));
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
            inner
                .idle
                .push_back(ConnectionMeta::new(MockConnection::with_pool_close_probe(
                    1,
                    Arc::clone(&pool_close_calls),
                    Arc::downgrade(&pool.shared),
                    Arc::clone(&pool_lock_was_free),
                ), test_clock()));
        }

        pool.close();

        assert!(pool.is_closed());
        assert_eq!(pool_close_calls.load(Ordering::Relaxed), 1);
        assert!(pool_lock_was_free.load(Ordering::Relaxed));
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
            inner.idle.push_back(ConnectionMeta::new(failed, test_clock()));
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
        use std::thread;

        let pool: Pool<MockConnection> = Pool::new(PoolConfig::new(5));

        // Properly initialize pool state as if acquire happened
        {
            let mut inner = pool.shared.inner.lock().unwrap();
            inner.total_count = 1;
            inner.active_count = 1;
        }

        let meta = ConnectionMeta::new(MockConnection::new(1), test_clock());
        let pooled = PooledConnection::new(meta, Arc::downgrade(&pool.shared));

        // Should have some small positive age
        assert!(pooled.age() >= Duration::ZERO);

        thread::sleep(Duration::from_millis(5));
        assert!(pooled.age() > Duration::ZERO);
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
        let pooled = PooledConnection::new(meta, Arc::downgrade(&pool.shared));

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
        let pooled = PooledConnection::new(meta, Arc::downgrade(&pool.shared));

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
        let pooled = PooledConnection::new(meta, Arc::downgrade(&pool.shared));

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
        let pooled = PooledConnection::new(meta, Arc::downgrade(&pool.shared));

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
        let mut pooled = PooledConnection::new(meta, Arc::downgrade(&pool.shared));

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
        let pooled = PooledConnection::new(meta, Arc::downgrade(&pool.shared));

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

        let wait: AcquireAction<MockConnection> = AcquireAction::Wait;
        assert!(matches!(wait, AcquireAction::Wait));

        let meta = ConnectionMeta::new(MockConnection::new(1), test_clock());
        let validate: AcquireAction<MockConnection> = AcquireAction::ValidateExisting(meta);
        assert!(matches!(validate, AcquireAction::ValidateExisting(_)));
    }

    #[test]
    fn test_pool_shared_atomic_counters() {
        let shared = PoolShared::<MockConnection>::new(PoolConfig::new(5));

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

        let shared = Arc::new(PoolShared::<MockConnection>::new(PoolConfig::new(5)));

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

        let shared = Arc::new(PoolShared::<MockConnection>::new(PoolConfig::new(5)));

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
        let pooled = PooledConnection::new(meta, Arc::downgrade(&pool.shared));

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
        let pooled = PooledConnection::new(meta, Arc::downgrade(&pool.shared));

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
}
