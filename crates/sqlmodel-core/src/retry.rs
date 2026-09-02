//! Cancel-correct retry of transactions that fail with retryable errors.
//!
//! MVCC databases (PostgreSQL `SERIALIZABLE`, InnoDB deadlocks, FrankenSQLite
//! `BEGIN CONCURRENT`) reject some transactions at commit time with a
//! serialization failure or write conflict. Such failures are transient by
//! definition: re-running the same transaction usually succeeds. Every driver
//! in this workspace already classifies them (`Error::is_retryable()`); this
//! module supplies the loop that acts on that classification without violating
//! structured-concurrency rules:
//!
//! * a `Cancelled` or `Panicked` outcome is **never** retried and is propagated
//!   after a best-effort rollback;
//! * backoff sleeps never run past the `Cx` budget deadline;
//! * the number of attempts is bounded, and exhausting it returns
//!   [`crate::TransactionErrorKind::RetriesExhausted`] carrying the last error.
//!
//! ```ignore
//! use sqlmodel_core::{retry_transaction, RetryPolicy, TransactionOptions, TransactionOps, Value};
//!
//! let policy = RetryPolicy::default();
//! let moved = retry_transaction(&cx, &conn, TransactionOptions::concurrent(), &policy,
//!     async |cx, tx| {
//!         tx.execute(cx, "UPDATE accounts SET balance = balance - 10 WHERE id = ?1", &[Value::BigInt(1)]).await?;
//!         tx.execute(cx, "UPDATE accounts SET balance = balance + 10 WHERE id = ?1", &[Value::BigInt(2)]).await?;
//!         asupersync::Outcome::Ok(10)
//!     },
//! ).await;
//! ```

use crate::connection::{Connection, TransactionOps, TransactionOptions};
use crate::error::Error;
use asupersync::types::CancelReason;
use asupersync::{Cx, Outcome};
use std::time::Duration;

/// Bounds and pacing for [`retry_transaction`].
#[derive(Clone)]
pub struct RetryPolicy {
    /// Total attempts including the first one (minimum 1).
    pub max_attempts: u32,
    /// Delay before the second attempt; doubles on every further attempt.
    pub base_delay: Duration,
    /// Upper bound for a single backoff delay.
    pub max_delay: Duration,
    /// Randomize each delay uniformly in `0..=computed` ("full jitter") so that
    /// writers that conflicted once do not conflict again in lockstep.
    pub jitter: bool,
    /// Which errors are worth retrying. Defaults to [`Error::is_retryable`].
    pub retry_on: fn(&Error) -> bool,
}

impl std::fmt::Debug for RetryPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryPolicy")
            .field("max_attempts", &self.max_attempts)
            .field("base_delay", &self.base_delay)
            .field("max_delay", &self.max_delay)
            .field("jitter", &self.jitter)
            .finish_non_exhaustive()
    }
}

impl Default for RetryPolicy {
    /// Five attempts, 10 ms base delay doubling up to 500 ms, full jitter,
    /// retrying whatever [`Error::is_retryable`] accepts.
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(500),
            jitter: true,
            retry_on: Error::is_retryable,
        }
    }
}

impl RetryPolicy {
    /// The default policy (see [`Default`]).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the total number of attempts (values below 1 are treated as 1).
    #[must_use]
    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }

    /// Set the base delay before the second attempt.
    #[must_use]
    pub fn base_delay(mut self, delay: Duration) -> Self {
        self.base_delay = delay;
        self
    }

    /// Set the maximum delay between attempts.
    #[must_use]
    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Enable or disable full jitter.
    #[must_use]
    pub fn jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// Choose which errors are retried.
    #[must_use]
    pub fn retry_on(mut self, predicate: fn(&Error) -> bool) -> Self {
        self.retry_on = predicate;
        self
    }

    /// No waiting between attempts (useful in tests and when the caller paces
    /// retries itself).
    #[must_use]
    pub fn immediate() -> Self {
        Self::default()
            .base_delay(Duration::ZERO)
            .max_delay(Duration::ZERO)
            .jitter(false)
    }

    /// Delay to wait after `failed_attempt` (1-based) failed, before the next
    /// attempt. Exponential from `base_delay`, capped at `max_delay`, with
    /// full jitter derived deterministically from `seed` when enabled.
    #[must_use]
    pub fn delay_after(&self, failed_attempt: u32, seed: u64) -> Duration {
        if self.base_delay.is_zero() {
            return Duration::ZERO;
        }
        let exponent = failed_attempt.saturating_sub(1).min(31);
        let scaled = self
            .base_delay
            .checked_mul(1u32 << exponent)
            .unwrap_or(self.max_delay)
            .min(self.max_delay);
        if !self.jitter || scaled.is_zero() {
            return scaled;
        }
        // xorshift64*: cheap, deterministic, good enough to de-synchronize writers.
        let mut x = seed ^ 0x9E37_79B9_7F4A_7C15 ^ u64::from(failed_attempt);
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        let r = x.wrapping_mul(0x2545_F491_4F6C_DD1D);
        // `scaled` is capped at `max_delay`, so this only saturates on absurd policies.
        let nanos = u64::try_from(scaled.as_nanos()).unwrap_or(u64::MAX);
        Duration::from_nanos(r % nanos.saturating_add(1))
    }

    fn should_retry(&self, error: &Error, attempt: u32) -> bool {
        attempt < self.max_attempts && (self.retry_on)(error)
    }
}

/// Outcome of waiting out a backoff delay.
enum Backoff {
    Waited,
    /// The `Cx` budget deadline would pass before the delay elapses.
    BudgetExceeded,
}

/// Sleep for `delay` unless that would overrun the `Cx` budget deadline.
/// What a caller running its own retry loop should do after a failed attempt.
#[derive(Debug)]
pub enum RetryDecision {
    /// The error was retryable and the backoff delay has elapsed: run the next attempt.
    Retry,
    /// Stop and return this error (not retryable, attempts exhausted, or the
    /// `Cx` deadline would pass during the backoff).
    GiveUp(Error),
}

impl RetryPolicy {
    /// Decide what to do after attempt number `attempt` (1-based) failed with
    /// `error`, sleeping the jittered backoff first when the answer is
    /// [`RetryDecision::Retry`]. Never sleeps past the `Cx` budget deadline;
    /// in that case the decision is to give up with
    /// [`crate::TransactionErrorKind::RetriesExhausted`].
    ///
    /// This is the building block [`retry_transaction`] and
    /// `Session::with_retry` share; use it directly for a retry loop around
    /// anything else.
    pub async fn after_failure(&self, cx: &Cx, attempt: u32, error: Error) -> RetryDecision {
        if !self.should_retry(&error, attempt) {
            return RetryDecision::GiveUp(finalize_error(error, attempt));
        }
        tracing::debug!(
            target: "sqlmodel_core::retry",
            attempt,
            max_attempts = self.max_attempts,
            error = %error,
            "attempt failed with a retryable error; backing off before retrying"
        );
        match backoff(cx, self.delay_after(attempt, seed(cx, attempt))).await {
            Backoff::Waited => RetryDecision::Retry,
            Backoff::BudgetExceeded => {
                tracing::warn!(
                    target: "sqlmodel_core::retry",
                    attempt,
                    "budget deadline too close for another backoff; giving up"
                );
                RetryDecision::GiveUp(Error::retries_exhausted(attempt, &error))
            }
        }
    }
}

async fn backoff(cx: &Cx, delay: Duration) -> Backoff {
    if delay.is_zero() {
        return Backoff::Waited;
    }
    let now = cx.now();
    if let Some(deadline) = cx.budget().deadline
        && now + delay > deadline
    {
        return Backoff::BudgetExceeded;
    }
    asupersync::time::sleep(now, delay).await;
    Backoff::Waited
}

/// Run `body` inside a transaction, retrying the whole transaction when it
/// fails with an error the policy accepts.
///
/// Each attempt begins a fresh transaction with `options`, runs `body`, and
/// commits. Semantics:
///
/// * `body` returns `Ok(v)` and commit succeeds: returns `Ok(v)`.
/// * `body` or commit returns a retryable `Err` and attempts remain: the
///   transaction is rolled back (best effort), the backoff delay is awaited,
///   and the loop repeats. `body` is therefore invoked once per attempt and
///   must be re-runnable; keep side effects outside the database out of it.
/// * `body` or commit returns a non-retryable `Err`: rolled back and returned
///   as is.
/// * attempts exhausted: rolled back and returned as
///   [`crate::TransactionErrorKind::RetriesExhausted`] whose message carries
///   the last error.
/// * `Cancelled` or `Panicked` from `begin`, `body`, or commit: rolled back
///   (best effort) and propagated immediately. Cancellation is never retried.
/// * a backoff that would exceed the `Cx` budget deadline ends the loop with
///   `RetriesExhausted` instead of sleeping past the deadline.
/// * an unsupported [`crate::TransactionMode`] fails on the first `begin` and
///   is not retried (it is not a retryable error).
///
/// The returned future is not `Send` because `body`'s future type cannot be
/// constrained; drive it from the task that owns the connection.
pub async fn retry_transaction<'c, C, T, F>(
    cx: &Cx,
    conn: &'c C,
    options: TransactionOptions,
    policy: &RetryPolicy,
    mut body: F,
) -> Outcome<T, Error>
where
    C: Connection,
    F: AsyncFnMut(&Cx, &C::Tx<'c>) -> Outcome<T, Error>,
{
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        if cx.is_cancel_requested() {
            return Outcome::Cancelled(CancelReason::user("retry_transaction cancelled"));
        }

        let tx = match Connection::begin_with_options(conn, cx, options).await {
            Outcome::Ok(tx) => tx,
            Outcome::Err(e) => match policy.after_failure(cx, attempt, e).await {
                RetryDecision::Retry => continue,
                RetryDecision::GiveUp(e) => return Outcome::Err(e),
            },
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let failure = match body(cx, &tx).await {
            Outcome::Ok(value) => match tx.commit(cx).await {
                Outcome::Ok(()) => return Outcome::Ok(value),
                // `commit(self)` consumed the transaction; the driver has already
                // discarded it on failure, so there is nothing left to roll back.
                Outcome::Err(e) => e,
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            },
            Outcome::Err(e) => {
                let _ = tx.rollback(cx).await;
                e
            }
            Outcome::Cancelled(r) => {
                let _ = tx.rollback(cx).await;
                return Outcome::Cancelled(r);
            }
            Outcome::Panicked(p) => {
                let _ = tx.rollback(cx).await;
                return Outcome::Panicked(p);
            }
        };

        match policy.after_failure(cx, attempt, failure).await {
            RetryDecision::Retry => {}
            RetryDecision::GiveUp(e) => return Outcome::Err(e),
        }
    }
}

/// The error to hand back when the loop stops: the raw error if this was the
/// first attempt (nothing was retried), otherwise `RetriesExhausted` naming it.
fn finalize_error(last: Error, attempt: u32) -> Error {
    if attempt <= 1 && !last.is_retryable() {
        last
    } else if attempt <= 1 {
        // Retryable but the policy allows a single attempt: still report the
        // exhaustion so callers can tell "gave up" from "not retryable".
        Error::retries_exhausted(attempt, &last)
    } else {
        Error::retries_exhausted(attempt, &last)
    }
}

fn seed(cx: &Cx, attempt: u32) -> u64 {
    cx.now().as_nanos() ^ (u64::from(attempt) << 32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{IsolationLevel, PreparedStatement, TransactionMode};
    use crate::error::{QueryError, QueryErrorKind, TransactionErrorKind};
    use crate::row::Row;
    use crate::value::Value;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A connection whose commits fail with a serialization error a scripted
    /// number of times, recording every begin/commit/rollback.
    struct ScriptedConn {
        commit_failures_remaining: Arc<AtomicU32>,
        begins: Arc<AtomicU32>,
        commits: Arc<AtomicU32>,
        rollbacks: Arc<AtomicU32>,
        supports_concurrent: bool,
        last_mode: std::sync::Mutex<Option<TransactionMode>>,
    }

    impl ScriptedConn {
        fn failing_commits(n: u32) -> Self {
            Self {
                commit_failures_remaining: Arc::new(AtomicU32::new(n)),
                begins: Arc::new(AtomicU32::new(0)),
                commits: Arc::new(AtomicU32::new(0)),
                rollbacks: Arc::new(AtomicU32::new(0)),
                supports_concurrent: true,
                last_mode: std::sync::Mutex::new(None),
            }
        }
    }

    struct ScriptedTx {
        commit_failures_remaining: Arc<AtomicU32>,
        commits: Arc<AtomicU32>,
        rollbacks: Arc<AtomicU32>,
    }

    fn serialization_failure() -> Error {
        Error::Query(QueryError {
            kind: QueryErrorKind::Serialization,
            sql: None,
            sqlstate: Some("40001".into()),
            message: "could not serialize access due to concurrent update".into(),
            detail: None,
            hint: None,
            position: None,
            source: None,
        })
    }

    fn syntax_error() -> Error {
        Error::Query(QueryError {
            kind: QueryErrorKind::Syntax,
            sql: None,
            sqlstate: None,
            message: "syntax error".into(),
            detail: None,
            hint: None,
            position: None,
            source: None,
        })
    }

    #[allow(clippy::unused_async_trait_impl)]
    impl TransactionOps for ScriptedTx {
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
            Outcome::Ok(1)
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
            self.commits.fetch_add(1, Ordering::SeqCst);
            let remaining = self.commit_failures_remaining.load(Ordering::SeqCst);
            if remaining > 0 {
                self.commit_failures_remaining
                    .store(remaining - 1, Ordering::SeqCst);
                return Outcome::Err(serialization_failure());
            }
            Outcome::Ok(())
        }
        async fn rollback(self, _cx: &Cx) -> Outcome<(), Error> {
            self.rollbacks.fetch_add(1, Ordering::SeqCst);
            Outcome::Ok(())
        }
    }

    #[allow(clippy::unused_async_trait_impl)]
    impl Connection for ScriptedConn {
        type Tx<'conn> = ScriptedTx;

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
        async fn begin(&self, cx: &Cx) -> Outcome<Self::Tx<'_>, Error> {
            self.begin_with(cx, IsolationLevel::default()).await
        }
        async fn begin_with(
            &self,
            _cx: &Cx,
            _isolation: IsolationLevel,
        ) -> Outcome<Self::Tx<'_>, Error> {
            self.begins.fetch_add(1, Ordering::SeqCst);
            Outcome::Ok(ScriptedTx {
                commit_failures_remaining: Arc::clone(&self.commit_failures_remaining),
                commits: Arc::clone(&self.commits),
                rollbacks: Arc::clone(&self.rollbacks),
            })
        }
        fn supports_transaction_mode(&self, mode: TransactionMode) -> bool {
            *self.last_mode.lock().unwrap() = Some(mode);
            match mode {
                TransactionMode::Default => true,
                TransactionMode::Concurrent => self.supports_concurrent,
                _ => false,
            }
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
            Outcome::Ok(())
        }
        async fn close(self, _cx: &Cx) -> crate::Result<()> {
            Ok(())
        }
    }

    fn run<T>(fut: impl Future<Output = T>) -> T {
        asupersync::runtime::RuntimeBuilder::current_thread()
            .build()
            .expect("runtime")
            .block_on(fut)
    }

    #[test]
    fn succeeds_after_two_serialization_failures() {
        let conn = ScriptedConn::failing_commits(2);
        let cx = Cx::for_testing();
        let policy = RetryPolicy::immediate();
        let body_calls = AtomicU32::new(0);

        let out = run(retry_transaction(
            &cx,
            &conn,
            TransactionOptions::concurrent(),
            &policy,
            async |cx, tx| {
                body_calls.fetch_add(1, Ordering::SeqCst);
                tx.execute(cx, "UPDATE t SET v = v + 1", &[]).await
            },
        ));

        assert!(matches!(out, Outcome::Ok(1)), "{out:?}");
        assert_eq!(
            body_calls.load(Ordering::SeqCst),
            3,
            "body runs once per attempt"
        );
        assert_eq!(conn.begins.load(Ordering::SeqCst), 3);
        assert_eq!(conn.commits.load(Ordering::SeqCst), 3);
        // Commit consumed the tx on failure; no explicit rollback is issued after a failed commit.
        assert_eq!(conn.rollbacks.load(Ordering::SeqCst), 0);
        assert_eq!(
            *conn.last_mode.lock().unwrap(),
            Some(TransactionMode::Concurrent),
            "the requested mode reached the driver"
        );
    }

    #[test]
    fn exhausting_attempts_reports_retries_exhausted_with_last_error() {
        let conn = ScriptedConn::failing_commits(u32::MAX);
        let cx = Cx::for_testing();
        let policy = RetryPolicy::immediate().max_attempts(3);

        let out = run(retry_transaction(
            &cx,
            &conn,
            TransactionOptions::new(),
            &policy,
            async |_cx, _tx| Outcome::Ok(()),
        ));

        match out {
            Outcome::Err(Error::Transaction(t)) => {
                assert_eq!(t.kind, TransactionErrorKind::RetriesExhausted);
                assert!(t.message.contains("3 attempt(s)"), "{}", t.message);
                assert!(t.message.contains("serialize"), "{}", t.message);
            }
            other => panic!("expected RetriesExhausted, got {other:?}"),
        }
        assert_eq!(conn.begins.load(Ordering::SeqCst), 3);
        assert_eq!(conn.commits.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn non_retryable_error_is_returned_unchanged_after_one_rollback() {
        let conn = ScriptedConn::failing_commits(0);
        let cx = Cx::for_testing();
        let policy = RetryPolicy::immediate();

        let out = run(retry_transaction(
            &cx,
            &conn,
            TransactionOptions::new(),
            &policy,
            async |_cx, _tx| -> Outcome<(), Error> { Outcome::Err(syntax_error()) },
        ));

        match out {
            Outcome::Err(Error::Query(q)) => assert_eq!(q.kind, QueryErrorKind::Syntax),
            other => panic!("expected the original syntax error, got {other:?}"),
        }
        assert_eq!(conn.begins.load(Ordering::SeqCst), 1);
        assert_eq!(conn.rollbacks.load(Ordering::SeqCst), 1);
        assert_eq!(conn.commits.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn cancelled_body_is_rolled_back_and_never_retried() {
        let conn = ScriptedConn::failing_commits(0);
        let cx = Cx::for_testing();
        let policy = RetryPolicy::immediate();

        let out = run(retry_transaction(
            &cx,
            &conn,
            TransactionOptions::new(),
            &policy,
            async |_cx, _tx| -> Outcome<(), Error> {
                Outcome::Cancelled(CancelReason::user("caller cancelled"))
            },
        ));

        assert!(matches!(out, Outcome::Cancelled(_)), "{out:?}");
        assert_eq!(
            conn.begins.load(Ordering::SeqCst),
            1,
            "no retry after cancellation"
        );
        assert_eq!(conn.rollbacks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn pre_cancelled_cx_returns_cancelled_before_touching_the_database() {
        let conn = ScriptedConn::failing_commits(0);
        let cx = Cx::for_testing();
        cx.set_cancel_requested(true);
        let policy = RetryPolicy::immediate();

        let out = run(retry_transaction(
            &cx,
            &conn,
            TransactionOptions::new(),
            &policy,
            async |_cx, _tx| Outcome::Ok(()),
        ));

        assert!(matches!(out, Outcome::Cancelled(_)), "{out:?}");
        assert_eq!(conn.begins.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn unsupported_transaction_mode_fails_fast_without_retrying() {
        let mut conn = ScriptedConn::failing_commits(0);
        conn.supports_concurrent = false;
        let cx = Cx::for_testing();
        let policy = RetryPolicy::immediate();

        let out = run(retry_transaction(
            &cx,
            &conn,
            TransactionOptions::concurrent(),
            &policy,
            async |_cx, _tx| Outcome::Ok(()),
        ));

        match out {
            Outcome::Err(Error::Transaction(t)) => {
                assert_eq!(t.kind, TransactionErrorKind::UnsupportedMode);
                assert!(t.message.contains("concurrent"), "{}", t.message);
            }
            other => panic!("expected UnsupportedMode, got {other:?}"),
        }
        assert_eq!(
            conn.begins.load(Ordering::SeqCst),
            0,
            "begin_with is never reached"
        );
    }

    #[test]
    fn delay_schedule_is_exponential_capped_and_jitter_bounded() {
        let policy = RetryPolicy::new()
            .base_delay(Duration::from_millis(10))
            .max_delay(Duration::from_millis(100))
            .jitter(false);
        assert_eq!(policy.delay_after(1, 0), Duration::from_millis(10));
        assert_eq!(policy.delay_after(2, 0), Duration::from_millis(20));
        assert_eq!(policy.delay_after(3, 0), Duration::from_millis(40));
        assert_eq!(policy.delay_after(4, 0), Duration::from_millis(80));
        assert_eq!(
            policy.delay_after(5, 0),
            Duration::from_millis(100),
            "capped"
        );
        assert_eq!(
            policy.delay_after(40, 0),
            Duration::from_millis(100),
            "no overflow"
        );

        let jittered = policy.clone().jitter(true);
        for seed in 0..1000u64 {
            let d = jittered.delay_after(3, seed);
            assert!(
                d <= Duration::from_millis(40),
                "jitter never exceeds the computed delay"
            );
        }
        assert_eq!(
            jittered.delay_after(3, 42),
            jittered.delay_after(3, 42),
            "deterministic for a given seed"
        );
        assert_eq!(RetryPolicy::immediate().delay_after(3, 7), Duration::ZERO);
    }

    #[test]
    fn max_attempts_floor_is_one() {
        assert_eq!(RetryPolicy::new().max_attempts(0).max_attempts, 1);
    }
}
