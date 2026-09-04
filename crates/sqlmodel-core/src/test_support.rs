//! Cancellation-injection test support (`bd-x6jl.2`).
//!
//! [`CancelAt`] wraps any [`Connection`] and counts every delegated call as a
//! cancellation checkpoint. The k-th delegated call observes
//! [`Cx::set_cancel_requested`] immediately before it is forwarded, so the
//! driver's own pre-flight `cancel_requested(cx)` guard is what returns
//! [`Outcome::Cancelled`] — the sweep exercises the real code path, not a
//! mock. A run with `cancel_at_call == 0` injects nothing and simply records
//! the call sequence, which is how a sweep discovers `K_max` for an operation.
//!
//! Transaction calls (commit, rollback, savepoints) are intercepted too:
//! [`CancelAt::begin*`] hand out a [`CancelAtTx`] that feeds the same call
//! log, so a sweep can assert that no `commit` happens after a cancellation
//! point and that a dropped transaction rolled back.
//!
//! This module is test infrastructure: it is compiled only under
//! `#[cfg(any(test, feature = "test-support"))]` and never shipped in the
//! default feature set.

use crate::connection::{
    Connection, IsolationLevel, TransactionMode, TransactionOps, TransactionOptions,
};
use crate::{Cx, Error, Outcome, PreparedStatement, Row, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// One delegated call in the execution log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallRecord {
    /// Which method was delegated: `"query"`, `"execute"`, `"begin"`,
    /// `"tx.commit"`, and so on.
    pub call: &'static str,
    /// The SQL text when the method carries one.
    pub sql: Option<String>,
    /// `true` when cancellation was injected immediately before this call.
    pub cancelled_before: bool,
}

/// Shared counting/log state between a [`CancelAt`] and its [`CancelAtTx`]s.
#[derive(Debug, Default)]
struct SweepState {
    calls: AtomicU64,
    injected: AtomicBool,
    log: Mutex<Vec<CallRecord>>,
}

impl SweepState {
    fn lock(&self) -> MutexGuard<'_, Vec<CallRecord>> {
        // Test support only: a panic while holding the log lock must not lose
        // the recorded sequence, so poisoning is tolerated instead of unwrapped.
        self.log.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn arm(&self, cx: &Cx, call: &'static str, sql: Option<&str>, cancel_at_call: u64) {
        let ordinal = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let cancel_here = cancel_at_call != 0 && ordinal == cancel_at_call;
        if cancel_here {
            cx.set_cancel_requested(true);
            self.injected.store(true, Ordering::SeqCst);
        }
        self.lock().push(CallRecord {
            call,
            sql: sql.map(str::to_owned),
            cancelled_before: cancel_here,
        });
    }
}

/// A [`Connection`] wrapper that cancels the operation's `Cx` immediately
/// before its `cancel_at_call`-th delegated call (1-based; `0` never cancels).
///
/// The wrapper itself always delegates: the injected flag is observed by the
/// driver's own cancellation guard, so the sweep proves the driver returns
/// `Outcome::Cancelled` for an already-cancelled context at every checkpoint.
#[derive(Debug)]
pub struct CancelAt<C> {
    inner: C,
    cancel_at_call: u64,
    state: Arc<SweepState>,
}

impl<C> CancelAt<C> {
    /// Wrap `inner`; the `cancel_at_call`-th delegated call (1-based) observes
    /// cancellation. `0` runs the operation untouched and only records calls.
    pub fn new(inner: C, cancel_at_call: u64) -> Self {
        Self {
            inner,
            cancel_at_call,
            state: Arc::new(SweepState::default()),
        }
    }

    /// Number of delegated calls so far (`K_max` after a `cancel_at_call == 0`
    /// run).
    pub fn calls_made(&self) -> u64 {
        self.state.calls.load(Ordering::SeqCst)
    }

    /// Whether cancellation was injected during the run.
    pub fn cancellation_injected(&self) -> bool {
        self.state.injected.load(Ordering::SeqCst)
    }

    /// The full delegated-call sequence in execution order.
    pub fn log(&self) -> Vec<CallRecord> {
        self.state.lock().clone()
    }

    /// The wrapped driver connection (used by sweeps to snapshot state).
    pub fn inner(&self) -> &C {
        &self.inner
    }

    fn tx_wrapper<T>(&self, tx: T) -> CancelAtTx<T> {
        CancelAtTx {
            inner: tx,
            cancel_at_call: self.cancel_at_call,
            state: Arc::clone(&self.state),
        }
    }
}

impl<C> Connection for CancelAt<C>
where
    C: Connection,
    for<'conn> C::Tx<'conn>: Sync,
{
    type Tx<'conn>
        = CancelAtTx<C::Tx<'conn>>
    where
        Self: 'conn;

    fn dialect(&self) -> crate::Dialect {
        self.inner.dialect()
    }

    async fn query(&self, cx: &Cx, sql: &str, params: &[Value]) -> Outcome<Vec<Row>, Error> {
        self.state.arm(cx, "query", Some(sql), self.cancel_at_call);
        self.inner.query(cx, sql, params).await
    }

    async fn query_one(&self, cx: &Cx, sql: &str, params: &[Value]) -> Outcome<Option<Row>, Error> {
        self.state
            .arm(cx, "query_one", Some(sql), self.cancel_at_call);
        self.inner.query_one(cx, sql, params).await
    }

    async fn execute(&self, cx: &Cx, sql: &str, params: &[Value]) -> Outcome<u64, Error> {
        self.state.arm(cx, "execute", Some(sql), self.cancel_at_call);
        self.inner.execute(cx, sql, params).await
    }

    async fn insert(&self, cx: &Cx, sql: &str, params: &[Value]) -> Outcome<i64, Error> {
        self.state.arm(cx, "insert", Some(sql), self.cancel_at_call);
        self.inner.insert(cx, sql, params).await
    }

    async fn batch(
        &self,
        cx: &Cx,
        statements: &[(String, Vec<Value>)],
    ) -> Outcome<Vec<u64>, Error> {
        self.state
            .arm(cx, "batch", statements.first().map(|(sql, _)| sql.as_str()),
            self.cancel_at_call);
        self.inner.batch(cx, statements).await
    }

    async fn begin(&self, cx: &Cx) -> Outcome<Self::Tx<'_>, Error> {
        self.state.arm(cx, "begin", None, self.cancel_at_call);
        self.inner
            .begin(cx)
            .await
            .map(|tx| self.tx_wrapper(tx))
    }

    async fn begin_with(&self, cx: &Cx, isolation: IsolationLevel) -> Outcome<Self::Tx<'_>, Error> {
        self.state.arm(cx, "begin_with", None, self.cancel_at_call);
        self.inner
            .begin_with(cx, isolation)
            .await
            .map(|tx| self.tx_wrapper(tx))
    }

    fn supports_transaction_mode(&self, mode: TransactionMode) -> bool {
        self.inner.supports_transaction_mode(mode)
    }

    async fn begin_with_options(
        &self,
        cx: &Cx,
        options: TransactionOptions,
    ) -> Outcome<Self::Tx<'_>, Error> {
        self.state
            .arm(cx, "begin_with_options", None, self.cancel_at_call);
        self.inner
            .begin_with_options(cx, options)
            .await
            .map(|tx| self.tx_wrapper(tx))
    }

    async fn prepare(&self, cx: &Cx, sql: &str) -> Outcome<PreparedStatement, Error> {
        self.state.arm(cx, "prepare", Some(sql), self.cancel_at_call);
        self.inner.prepare(cx, sql).await
    }

    async fn query_prepared(
        &self,
        cx: &Cx,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> Outcome<Vec<Row>, Error> {
        self.state
            .arm(cx, "query_prepared", Some(stmt.sql()), self.cancel_at_call);
        self.inner.query_prepared(cx, stmt, params).await
    }

    async fn execute_prepared(
        &self,
        cx: &Cx,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> Outcome<u64, Error> {
        self.state
            .arm(cx, "execute_prepared", Some(stmt.sql()), self.cancel_at_call);
        self.inner.execute_prepared(cx, stmt, params).await
    }

    async fn ping(&self, cx: &Cx) -> Outcome<(), Error> {
        self.state.arm(cx, "ping", None, self.cancel_at_call);
        self.inner.ping(cx).await
    }

    async fn close(self, cx: &Cx) -> Result<(), Error> {
        self.state.arm(cx, "close", None, self.cancel_at_call);
        self.inner.close(cx).await
    }

    async fn close_for_pool(self, cx: &Cx) -> Result<(), Error>
    where
        Self: Sized,
    {
        self.state
            .arm(cx, "close_for_pool", None, self.cancel_at_call);
        self.inner.close_for_pool(cx).await
    }
}

/// `rollback`.
///
/// `T: Sync` is required because the wrapper shares the sweep log behind
/// `&self` across awaits; the drivers' transaction types (built on
/// `TransactionInternal: Send + Sync`) satisfy it.
#[derive(Debug)]
pub struct CancelAtTx<T> {
    inner: T,
    cancel_at_call: u64,
    state: Arc<SweepState>,
}

impl<T: TransactionOps + Sync> TransactionOps for CancelAtTx<T> {
    async fn query(&self, cx: &Cx, sql: &str, params: &[Value]) -> Outcome<Vec<Row>, Error> {
        self.state
            .arm(cx, "tx.query", Some(sql), self.cancel_at_call);
        self.inner.query(cx, sql, params).await
    }

    async fn query_one(&self, cx: &Cx, sql: &str, params: &[Value]) -> Outcome<Option<Row>, Error> {
        self.state
            .arm(cx, "tx.query_one", Some(sql), self.cancel_at_call);
        self.inner.query_one(cx, sql, params).await
    }

    async fn execute(&self, cx: &Cx, sql: &str, params: &[Value]) -> Outcome<u64, Error> {
        self.state
            .arm(cx, "tx.execute", Some(sql), self.cancel_at_call);
        self.inner.execute(cx, sql, params).await
    }

    async fn savepoint(&self, cx: &Cx, name: &str) -> Outcome<(), Error> {
        self.state
            .arm(cx, "tx.savepoint", Some(name), self.cancel_at_call);
        self.inner.savepoint(cx, name).await
    }

    async fn rollback_to(&self, cx: &Cx, name: &str) -> Outcome<(), Error> {
        self.state
            .arm(cx, "tx.rollback_to", Some(name), self.cancel_at_call);
        self.inner.rollback_to(cx, name).await
    }

    async fn release(&self, cx: &Cx, name: &str) -> Outcome<(), Error> {
        self.state
            .arm(cx, "tx.release", Some(name), self.cancel_at_call);
        self.inner.release(cx, name).await
    }

    async fn commit(self, cx: &Cx) -> Outcome<(), Error> {
        self.state.arm(cx, "tx.commit", None, self.cancel_at_call);
        self.inner.commit(cx).await
    }

    async fn rollback(self, cx: &Cx) -> Outcome<(), Error> {
        self.state.arm(cx, "tx.rollback", None, self.cancel_at_call);
        self.inner.rollback(cx).await
    }
}
