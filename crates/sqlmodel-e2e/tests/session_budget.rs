//! Budget/timeout exhaustion semantics for the session unit of work
//! (`bd-x6jl.4`, part 4b).
//!
//! Semantics (documented in `sqlmodel-core/src/connection.rs`):
//!
//! * A `Cx` whose budget the runtime has exhausted (deadline passed) makes
//!   `Session::flush` stop at the next statement boundary with
//!   `Outcome::Err(Error::Timeout)`.
//! * The flush's open transaction is left for the caller: rolling it back
//!   restores the pre-flush state exactly — a budget can never produce a
//!   partially-flushed durable state.
//! * A flush that fits inside the budget commits normally.
//!
//! The sweep gives every budget a wall-clock window sized in statement
//! units (`budget_ms` / `STMT_MS`) and a connection that spends `STMT_MS`
//! per statement, so each cutoff lands on a statement boundary.

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Budget, Cx, Outcome};
use serde::{Deserialize, Serialize};
use sqlmodel::prelude::*;
use sqlmodel::Session;
use sqlmodel_core::PreparedStatement;
use sqlmodel_e2e::expect_outcome;
use sqlmodel_sqlite::SqliteConnection;
use std::time::Duration;

/// Wall-clock cost attributed to each statement by [`SleepingConnection`].
const STMT_MS: u64 = 3;

struct SleepingConnection {
    inner: SqliteConnection,
}

impl SleepingConnection {
    fn new(inner: SqliteConnection) -> Self {
        Self { inner }
    }

    fn inner(&self) -> &SqliteConnection {
        &self.inner
    }
}

impl Connection for SleepingConnection {
    type Tx<'conn>
        = <SqliteConnection as Connection>::Tx<'conn>
    where
        Self: 'conn;

    fn dialect(&self) -> Dialect {
        self.inner.dialect()
    }

    async fn query(&self, cx: &Cx, sql: &str, params: &[Value]) -> Outcome<Vec<Row>, Error> {
        std::thread::sleep(Duration::from_millis(STMT_MS));
        self.inner.query(cx, sql, params).await
    }

    async fn query_one(
        &self,
        cx: &Cx,
        sql: &str,
        params: &[Value],
    ) -> Outcome<Option<Row>, Error> {
        std::thread::sleep(Duration::from_millis(STMT_MS));
        self.inner.query_one(cx, sql, params).await
    }

    async fn close(self, cx: &Cx) -> sqlmodel_core::Result<()> {
        self.inner.close(cx).await
    }
}

fn enforce_budget(cx: &Cx) -> Outcome<(), Error> {
    if cx.is_cancel_requested() {
        return Outcome::Cancelled(
            cx.cancel_reason()
                .unwrap_or_else(|| CancelReason::user("cancelled at statement boundary")),
        );
    }
    if let Some(deadline) = cx.budget().deadline {
        if cx.now() >= deadline {
            return Outcome::Err(Error::Timeout);
        }
    }
    Outcome::Ok(())
}
        self.inner.insert(cx, sql, params).await
    }

    async fn batch(
        &self,
        cx: &Cx,
        statements: &[(String, Vec<Value>)],
    ) -> Outcome<Vec<u64>, Error> {
        std::thread::sleep(Duration::from_millis(STMT_MS));
        self.inner.batch(cx, statements).await
    }

    async fn begin(&self, cx: &Cx) -> Outcome<Self::Tx<'_>, Error> {
        self.inner.begin(cx).await
    }

    async fn begin_with(
        &self,
        cx: &Cx,
        isolation: IsolationLevel,
    ) -> Outcome<Self::Tx<'_>, Error> {
        self.inner.begin_with(cx, isolation).await
    }

    fn supports_transaction_mode(&self, mode: TransactionMode) -> bool {
        self.inner.supports_transaction_mode(mode)
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

    async fn close(self, cx: &Cx) -> sqlmodel_core::Result<()> {
        self.inner.close(cx).await
    }
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_budget_rows")]
struct Row_ {
    #[sqlmodel(primary_key)]
    id: i64,
    payload: String,
    #[sqlmodel(nullable)]
    extra: Option<String>,
}

fn row(id: i64, payload: &str, extra: Option<&str>) -> Row_ {
    Row_ {
        id,
        payload: payload.to_owned(),
        extra: extra.map(str::to_owned),
    }
}

async fn state_dump(cx: &Cx, conn: &SqliteConnection) -> Vec<String> {
    let rows = expect_outcome(
        conn.query(
            cx,
            "SELECT CAST(id AS TEXT), payload, CAST(extra AS TEXT) FROM e2e_budget_rows ORDER BY id",
            &[],
        )
        .await,
        "budget sweep snapshot",
    );
    rows.iter()
        .map(|r| {
            (0..r.len())
                .map(|i| r.get_as::<String>(i).unwrap_or_else(|_| "?".to_owned()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

async fn flush_three(cx: &Cx, s: &mut Session<SleepingConnection>) -> Outcome<(), Error> {
    s.add(&row(1, "alpha-one", Some("x")));
    s.add(&row(2, "beta-two", None));
    s.add(&row(3, "gamma-three", Some("y")));
    s.flush(cx).await
}

fn fixture_ddl() -> [&'static str; 2] {
    [
        "CREATE TABLE e2e_budget_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, extra TEXT)",
        "INSERT INTO e2e_budget_rows (id, payload) VALUES (1, 'seed-a'), (2, 'seed-b')",
    ]
}

#[test]
fn flush_budget_deadline_boundary_sweep() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("asupersync runtime");
    rt.block_on(async {
        // Budget windows of 0..=24ms against statements that cost 3ms each:
        // the boundary between Timeout and Ok sweeps across all statement
        // counts, and monotonicity is asserted.
        let mut first_ok: Option<u64> = None;
        let mut prev_ok = false;
        for budget_ms in 0..=24u64 {
            let fixture_cx = Cx::for_testing();
            let conn = SqliteConnection::open_memory().expect("open :memory:");
            for ddl in fixture_ddl() {
                expect_outcome(conn.execute(&fixture_cx, ddl, &[]).await, "budget sweep fixture");
            }
            let before = state_dump(&fixture_cx, &conn).await;
            let expected_after_flush = vec![
                "1|alpha-one|x".to_owned(),
                "2|beta-two|".to_owned(),
                "3|gamma-three|y".to_owned(),
            ];

            let start = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock");
            let deadline =
                asupersync::types::Time::from_nanos(start.as_nanos() as u64 + budget_ms * 1_000_000);
            let budget = Budget::new().with_deadline(deadline);
            let cx = Cx::for_testing_with_budget(budget);

            let mut s = Session::new(SleepingConnection::new(conn));
            let outcome = flush_three(&cx, &mut s).await;

            let ok = matches!(outcome, Outcome::Ok(()));
            match &outcome {
                Outcome::Ok(()) => {
                    assert!(
                        !prev_ok || first_ok.is_some_and(|first| budget_ms >= first),
                        "budget {budget_ms}ms: Ok after a Timeout violates monotonicity"
                    );
                    let after = state_dump(&cx, s.connection().inner()).await;
                    assert_eq!(
                        after, expected_after_flush,
                        "budget {budget_ms}ms: successful flush must commit all rows"
                    );
                }
                Outcome::Err(Error::Timeout) => {
                    // The flush's transaction stays open; the caller rolls it
                    // back and the state is exactly the pre-flush snapshot.
                    expect_outcome(s.rollback(&cx).await, "rollback after budget timeout");
                    let after = state_dump(&cx, s.connection().inner()).await;
                    assert_eq!(
                        after, before,
                        "budget {budget_ms}ms: timed-out flush left partial state"
                    );
                }
                Outcome::Cancelled(r) => panic!(
                    "budget {budget_ms}ms: budget exhaustion must be Err(Timeout), got Cancelled {r:?}"
                ),
                Outcome::Err(e) => panic!(
                    "budget {budget_ms}ms: budget exhaustion must be Err(Timeout), got {e:?}"
                ),
                Outcome::Panicked(p) => panic!("budget {budget_ms}ms: panicked {p:?}"),
            }
            if ok && first_ok.is_none() {
                first_ok = Some(budget_ms);
            }
            prev_ok = ok;
        }
        let first = first_ok.expect("at least one budget must allow the flush");
        eprintln!("flush budget sweep: first Ok at {first}ms window (STMT_MS={STMT_MS})");
    });
}
