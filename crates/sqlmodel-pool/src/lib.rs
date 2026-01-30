//! Connection pooling for SQLModel Rust using asupersync.
//!
//! This crate provides a connection pool that integrates with
//! asupersync's structured concurrency model.
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
//! let pool = Pool::new(config, || async {
//!     // Factory function to create new connections
//!     PgConnection::connect(&cx, &pg_config).await
//! });
//!
//! // Acquire a connection
//! let conn = pool.acquire(&cx).await?;
//!
//! // Use the connection (automatically returned to pool on drop)
//! conn.query(&cx, "SELECT 1", &[]).await?;
//! ```

pub mod replica;
pub use replica::{ReplicaPool, ReplicaStrategy};

use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use asupersync::{CancelReason, Cx, Outcome};
use sqlmodel_core::error::{ConnectionError, ConnectionErrorKind, PoolError, PoolErrorKind};
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
    /// Test connections before giving them out
    pub test_on_checkout: bool,
    /// Test connections when returning them to the pool
    pub test_on_return: bool,
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
            test_on_return: false,
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

    /// Enable/disable test on checkout.
    #[must_use]
    pub fn test_on_checkout(mut self, enabled: bool) -> Self {
        self.test_on_checkout = enabled;
        self
    }

    /// Enable/disable test on return.
    #[must_use]
    pub fn test_on_return(mut self, enabled: bool) -> Self {
        self.test_on_return = enabled;
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
#[derive(Debug)]
struct ConnectionMeta<C> {
    /// The actual connection
    conn: C,
    /// When this connection was created
    created_at: Instant,
    /// When this connection was last used
    last_used: Instant,
}

impl<C> ConnectionMeta<C> {
    fn new(conn: C) -> Self {
        let now = Instant::now();
        Self {
            conn,
            created_at: now,
            last_used: now,
        }
    }

    fn touch(&mut self) {
        self.last_used = Instant::now();
    }

    fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    fn idle_time(&self) -> Duration {
        self.last_used.elapsed()
    }
}

/// Internal pool state shared between pool and connections.
struct PoolInner<C> {
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

impl<C> PoolInner<C> {
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
struct PoolShared<C> {
    /// Protected pool state
    inner: Mutex<PoolInner<C>>,
    /// Notifies waiters when connections become available
    conn_available: Condvar,
    /// Statistics counters (atomic for lock-free reads)
    connections_created: AtomicU64,
    connections_closed: AtomicU64,
    acquires: AtomicU64,
    timeouts: AtomicU64,
}

impl<C> PoolShared<C> {
    fn new(config: PoolConfig) -> Self {
        Self {
            inner: Mutex::new(PoolInner::new(config)),
            conn_available: Condvar::new(),
            connections_created: AtomicU64::new(0),
            connections_closed: AtomicU64::new(0),
            acquires: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
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
    #[must_use]
    pub fn new(config: PoolConfig) -> Self {
        Self {
            shared: Arc::new(PoolShared::new(config)),
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
    /// - Connection validation fails (if `test_on_checkout` is enabled)
    pub async fn acquire<F, Fut>(&self, cx: &Cx, factory: F) -> Outcome<PooledConnection<C>, Error>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Outcome<C, Error>>,
    {
        let deadline = Instant::now() + Duration::from_millis(self.config().acquire_timeout_ms);
        let test_on_checkout = self.config().test_on_checkout;
        let max_lifetime = Duration::from_millis(self.config().max_lifetime_ms);
        let idle_timeout = Duration::from_millis(self.config().idle_timeout_ms);

        loop {
            // Check cancellation
            if cx.is_cancel_requested() {
                return Outcome::Cancelled(CancelReason::user("pool acquire cancelled"));
            }

            // Check timeout
            if Instant::now() >= deadline {
                self.shared.timeouts.fetch_add(1, Ordering::Relaxed);
                return Outcome::Err(Error::Pool(PoolError {
                    kind: PoolErrorKind::Timeout,
                    message: "acquire timeout: no connections available".to_string(),
                    source: None,
                }));
            }

            // Try to get an idle connection or determine if we can create new
            let action = {
                let mut inner = match self.shared.lock_or_error("acquire") {
                    Ok(guard) => guard,
                    Err(e) => return Outcome::Err(e),
                };

                if inner.closed {
                    AcquireAction::PoolClosed
                } else {
                    // Try to get an idle connection
                    let mut found_conn = None;
                    while let Some(mut meta) = inner.idle.pop_front() {
                        // Check if connection is too old
                        if meta.age() > max_lifetime {
                            inner.total_count -= 1;
                            self.shared
                                .connections_closed
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }

                        // Check if connection has been idle too long
                        if meta.idle_time() > idle_timeout {
                            inner.total_count -= 1;
                            self.shared
                                .connections_closed
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }

                        // Found a valid connection
                        meta.touch();
                        inner.active_count += 1;
                        found_conn = Some(meta);
                        break;
                    }

                    if let Some(meta) = found_conn {
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
                }
            };

            match action {
                AcquireAction::PoolClosed => {
                    return Outcome::Err(Error::Pool(PoolError {
                        kind: PoolErrorKind::Closed,
                        message: "pool has been closed".to_string(),
                        source: None,
                    }));
                }
                AcquireAction::ValidateExisting(meta) => {
                    // Validate and wrap the connection (lock is released)
                    return self.validate_and_wrap(cx, meta, test_on_checkout).await;
                }
                AcquireAction::CreateNew => {
                    // Create new connection outside of lock
                    match factory().await {
                        Outcome::Ok(conn) => {
                            self.shared
                                .connections_created
                                .fetch_add(1, Ordering::Relaxed);
                            self.shared.acquires.fetch_add(1, Ordering::Relaxed);
                            let meta = ConnectionMeta::new(conn);
                            return Outcome::Ok(PooledConnection::new(
                                meta,
                                Arc::downgrade(&self.shared),
                            ));
                        }
                        Outcome::Err(e) => {
                            // Failed to create, decrement counts
                            if let Ok(mut inner) = self.shared.lock_or_error("acquire_cleanup") {
                                inner.total_count -= 1;
                                inner.active_count -= 1;
                            }
                            // Even if we can't decrement counts, still return the original error
                            return Outcome::Err(e);
                        }
                        Outcome::Cancelled(reason) => {
                            if let Ok(mut inner) = self.shared.lock_or_error("acquire_cleanup") {
                                inner.total_count -= 1;
                                inner.active_count -= 1;
                            }
                            return Outcome::Cancelled(reason);
                        }
                        Outcome::Panicked(info) => {
                            if let Ok(mut inner) = self.shared.lock_or_error("acquire_cleanup") {
                                inner.total_count -= 1;
                                inner.active_count -= 1;
                            }
                            return Outcome::Panicked(info);
                        }
                    }
                }
                AcquireAction::Wait => {
                    // Wait for a connection to become available
                    let remaining = deadline.saturating_duration_since(Instant::now());
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

                    // Wait with timeout (use shorter interval for cancellation checks)
                    let wait_time = remaining.min(Duration::from_millis(100));
                    {
                        let inner = match self.shared.lock_or_error("acquire_wait") {
                            Ok(guard) => guard,
                            Err(e) => return Outcome::Err(e),
                        };
                        // wait_timeout can also return a poisoned error, handle it
                        let _ = self
                            .shared
                            .conn_available
                            .wait_timeout(inner, wait_time)
                            .map_err(|_| {
                                tracing::error!("Pool mutex poisoned during wait_timeout");
                            });
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
    async fn validate_and_wrap(
        &self,
        cx: &Cx,
        meta: ConnectionMeta<C>,
        test_on_checkout: bool,
    ) -> Outcome<PooledConnection<C>, Error> {
        if test_on_checkout {
            // Validate the connection
            match meta.conn.ping(cx).await {
                Outcome::Ok(()) => {
                    self.shared.acquires.fetch_add(1, Ordering::Relaxed);
                    Outcome::Ok(PooledConnection::new(meta, Arc::downgrade(&self.shared)))
                }
                Outcome::Err(_) | Outcome::Cancelled(_) | Outcome::Panicked(_) => {
                    // Connection is invalid, decrement counts and try again
                    {
                        if let Ok(mut inner) = self.shared.lock_or_error("validate_cleanup") {
                            inner.total_count -= 1;
                            inner.active_count -= 1;
                        }
                    }
                    self.shared
                        .connections_closed
                        .fetch_add(1, Ordering::Relaxed);
                    // Return error - caller should retry
                    Outcome::Err(Error::Connection(ConnectionError {
                        kind: ConnectionErrorKind::Disconnected,
                        message: "connection validation failed".to_string(),
                        source: None,
                    }))
                }
            }
        } else {
            self.shared.acquires.fetch_add(1, Ordering::Relaxed);
            Outcome::Ok(PooledConnection::new(meta, Arc::downgrade(&self.shared)))
        }
    }

    /// Close the pool, preventing new connections and closing all idle connections.
    ///
    /// If the pool mutex is poisoned, this logs an error but still wakes waiters.
    pub fn close(&self) {
        match self.shared.inner.lock() {
            Ok(mut inner) => {
                inner.closed = true;

                // Close all idle connections
                let idle_count = inner.idle.len();
                inner.idle.clear();
                inner.total_count -= idle_count;
                self.shared
                    .connections_closed
                    .fetch_add(idle_count as u64, Ordering::Relaxed);
                drop(inner);
            }
            Err(poisoned) => {
                // Recover from poisoning - we still want to mark the pool as closed
                // and wake waiters even if counts may be inconsistent.
                tracing::error!(
                    "Pool mutex poisoned during close; attempting recovery. \
                     Pool state may be inconsistent."
                );
                let mut inner = poisoned.into_inner();
                inner.closed = true;
                let idle_count = inner.idle.len();
                inner.idle.clear();
                inner.total_count -= idle_count;
                self.shared
                    .connections_closed
                    .fetch_add(idle_count as u64, Ordering::Relaxed);
            }
        }

        // Wake all waiters so they see the pool is closed
        self.shared.conn_available.notify_all();
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

/// Action to take when acquiring a connection.
enum AcquireAction<C> {
    /// Pool is closed
    PoolClosed,
    /// Found an existing connection to validate
    ValidateExisting(ConnectionMeta<C>),
    /// Create a new connection
    CreateNew,
    /// Wait for a connection to become available
    Wait,
}

/// A connection borrowed from the pool.
///
/// When dropped, the connection is automatically returned to the pool.
/// The connection can be used via `Deref` and `DerefMut`.
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
        if let Some(pool) = self.pool.upgrade() {
            // Try to update pool counters, but don't panic if mutex is poisoned.
            // The connection is being detached anyway, so counts being off is acceptable.
            match pool.inner.lock() {
                Ok(mut inner) => {
                    inner.total_count -= 1;
                    inner.active_count -= 1;
                    pool.connections_closed.fetch_add(1, Ordering::Relaxed);
                }
                Err(_poisoned) => {
                    tracing::error!(
                        "Pool mutex poisoned during detach; pool counters will be inconsistent"
                    );
                    // Still increment the atomic counter for tracking
                    pool.connections_closed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        self.meta.take().expect("connection already detached").conn
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
                // Return to pool - but if mutex is poisoned, we must not panic in Drop.
                // Instead, log the error and leak the connection.
                let mut inner = match pool.inner.lock() {
                    Ok(guard) => guard,
                    Err(_poisoned) => {
                        tracing::error!(
                            "Pool mutex poisoned during connection return; \
                             connection will be leaked. A thread panicked while holding the lock."
                        );
                        // Connection is leaked - we can't safely return it or update counts.
                        // The pool is likely in a bad state anyway.
                        return;
                    }
                };

                if inner.closed {
                    inner.total_count -= 1;
                    inner.active_count -= 1;
                    pool.connections_closed.fetch_add(1, Ordering::Relaxed);
                    return;
                }

                // Check max lifetime
                let max_lifetime = Duration::from_millis(inner.config.max_lifetime_ms);
                if meta.age() > max_lifetime {
                    inner.total_count -= 1;
                    inner.active_count -= 1;
                    pool.connections_closed.fetch_add(1, Ordering::Relaxed);
                    return;
                }

                inner.active_count -= 1;
                inner.idle.push_back(meta);

                drop(inner);
                pool.conn_available.notify_one();
            }
            // If pool is gone, connection is just dropped
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
    use sqlmodel_core::connection::{IsolationLevel, PreparedStatement, TransactionOps};
    use sqlmodel_core::{Row, Value};
    use std::sync::atomic::AtomicBool;

    /// A mock connection for testing pool behavior.
    #[derive(Debug)]
    struct MockConnection {
        id: u32,
        ping_should_fail: Arc<AtomicBool>,
    }

    impl MockConnection {
        fn new(id: u32) -> Self {
            Self {
                id,
                ping_should_fail: Arc::new(AtomicBool::new(false)),
            }
        }

        #[allow(dead_code)]
        fn with_ping_behavior(id: u32, should_fail: Arc<AtomicBool>) -> Self {
            Self {
                id,
                ping_should_fail: should_fail,
            }
        }
    }

    /// Mock transaction for MockConnection.
    struct MockTx;

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
        assert!(!config.test_on_return);
    }

    #[test]
    fn test_config_builder() {
        let config = PoolConfig::new(20)
            .min_connections(5)
            .idle_timeout(60_000)
            .acquire_timeout(5_000)
            .max_lifetime(300_000)
            .test_on_checkout(false)
            .test_on_return(true);

        assert_eq!(config.min_connections, 5);
        assert_eq!(config.max_connections, 20);
        assert_eq!(config.idle_timeout_ms, 60_000);
        assert_eq!(config.acquire_timeout_ms, 5_000);
        assert_eq!(config.max_lifetime_ms, 300_000);
        assert!(!config.test_on_checkout);
        assert!(config.test_on_return);
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
        use std::thread;

        // Create a dummy type for testing
        struct DummyConn;

        let meta = ConnectionMeta::new(DummyConn);
        let initial_age = meta.age();

        // Small sleep to ensure time passes
        thread::sleep(Duration::from_millis(10));

        // Age should have increased
        assert!(meta.age() > initial_age);
        assert!(meta.idle_time() > Duration::ZERO);
    }

    #[test]
    fn test_connection_meta_touch() {
        use std::thread;

        struct DummyConn;

        let mut meta = ConnectionMeta::new(DummyConn);

        // Small sleep to build up some idle time
        thread::sleep(Duration::from_millis(10));
        let idle_before_touch = meta.idle_time();
        assert!(idle_before_touch > Duration::ZERO);

        // Touch should reset idle time
        meta.touch();
        let idle_after_touch = meta.idle_time();

        // After touch, idle time should be very small (less than before)
        assert!(idle_after_touch < idle_before_touch);
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
            .push_back(ConnectionMeta::new(MockConnection::new(1)));
        inner
            .idle
            .push_back(ConnectionMeta::new(MockConnection::new(2)));

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

        let meta = ConnectionMeta::new(MockConnection::new(1));
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

        let meta = ConnectionMeta::new(MockConnection::new(42));
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

        let meta = ConnectionMeta::new(MockConnection::new(1));
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

        let meta = ConnectionMeta::new(MockConnection::new(1));
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

        let meta = ConnectionMeta::new(MockConnection::new(99));
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

        let meta = ConnectionMeta::new(MockConnection::new(1));
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

        let meta = ConnectionMeta::new(MockConnection::new(1));
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
        let closed: AcquireAction<MockConnection> = AcquireAction::PoolClosed;
        assert!(matches!(closed, AcquireAction::PoolClosed));

        let create: AcquireAction<MockConnection> = AcquireAction::CreateNew;
        assert!(matches!(create, AcquireAction::CreateNew));

        let wait: AcquireAction<MockConnection> = AcquireAction::Wait;
        assert!(matches!(wait, AcquireAction::Wait));

        let meta = ConnectionMeta::new(MockConnection::new(1));
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
                .push_back(ConnectionMeta::new(MockConnection::new(1)));
            inner
                .idle
                .push_back(ConnectionMeta::new(MockConnection::new(2)));
            inner
                .idle
                .push_back(ConnectionMeta::new(MockConnection::new(3)));
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
    // Tier 3 (Drop): Log error and leak connection (don't panic)

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
                .push_back(ConnectionMeta::new(MockConnection::new(1)));
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
        let meta = ConnectionMeta::new(MockConnection::new(1));
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
        let meta = ConnectionMeta::new(MockConnection::new(42));
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

            // Now panic while "using" the connection
            // (In real code, this would be while holding a PooledConnection)
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
                .push_back(ConnectionMeta::new(MockConnection::new(1)));
            inner
                .idle
                .push_back(ConnectionMeta::new(MockConnection::new(2)));
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
