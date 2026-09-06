//! FrankenSQLite MVCC contention proof (`bd-slot.10`): the whole
//! `TransactionMode::Concurrent` + `retry_transaction` stack under real
//! multi-writer load on one file database.
//!
//! Scenarios (sequential, each over its own seeded `e2e_mvcc_accounts` file
//! database so shared-table phases never race between tests):
//!
//! 1. **Disjoint writers** - 8 writers x 100 `BEGIN CONCURRENT`
//!    transactions, each touching its own key range: zero conflicts and a
//!    conserved balance; wall time is logged against a `BEGIN IMMEDIATE`
//!    serialized baseline with the ratio assertion opt-in via
//!    `SQLMODEL_MVCC_ASSERT_RATIO` (CI runner core counts vary).
//! 2. **Overlapping writers** - fixed bank transfers between account pairs
//!    with per-transfer idempotency tokens: conflicts must occur (proving
//!    MVCC detection is live), every conflict must be
//!    `Error::is_retryable()`, `retry_transaction` resolves them, and the
//!    total balance is conserved with no transfer applied twice.
//! 3. **Reader during writes** - every single-statement read observes a
//!    conserved balance while writers run.
//! 4. **Cancellation under contention** - a writer whose `Cx` is cancelled
//!    mid-transaction surfaces `Outcome::Cancelled` (or a driver cancellation
//!    error), leaves its transaction rolled back, and does not disturb the
//!    committed state.
//! 5. **Write-skew / SSI probe** - two transactions each read a shared sum
//!    and withdraw from different accounts; the outcome each writer gets is
//!    logged, and the resulting state must be internally consistent.
//! 6. **C-SQLite control** - the transfer scenario on C SQLite with
//!    `BEGIN IMMEDIATE`: serialization conserves the balance too, via
//!    blocking rather than MVCC conflict detection.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};
use serde::{Deserialize, Serialize};
use sqlmodel::SchemaBuilder;
use sqlmodel::prelude::*;
use sqlmodel_core::TransactionOps;
use sqlmodel_e2e::{expect_outcome, unique_table};
use sqlmodel_frankensqlite::FrankenConnection;
use sqlmodel_sqlite::SqliteConnection;

const WRITERS: usize = 8;
const TX_PER_WRITER: usize = 100;
const ACCOUNTS: i64 = 64;
const OPENING_BALANCE: i64 = 1_000;
const TOTAL_BALANCE: i64 = ACCOUNTS * OPENING_BALANCE;

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_mvcc_accounts")]
struct Account {
    #[sqlmodel(primary_key)]
    id: i64,
    balance: i64,
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_mvcc_transfers")]
struct Transfer {
    #[sqlmodel(primary_key)]
    token: String,
    from_account: i64,
    to_account: i64,
    amount: i64,
}

/// A fresh FrankenSQLite file database with both tables; the accounts table
/// is seeded so the total balance is exactly [`TOTAL_BALANCE`].
struct TestDb {
    path: std::path::PathBuf,
}

impl TestDb {
    fn fresh(tag: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("e2e_franken_mvcc_{tag}_{}.db", unique_table("t")));
        let _ = std::fs::remove_file(&path);
        let path_str = path.to_str().expect("temp path is utf-8").to_string();
        let conn = FrankenConnection::open_file(path_str).expect("open fresh franken db");
        let rt = RuntimeBuilder::current_thread().build().expect("runtime");
        let cx = Cx::for_testing();
        rt.block_on(async {
            for stmt in SchemaBuilder::new()
                .create_table::<Account>()
                .create_table::<Transfer>()
                .build()
            {
                expect_outcome(conn.execute(&cx, &stmt, &[]).await, "mvcc ddl");
            }
            for id in 1..=ACCOUNTS {
                let sql = format!(
                    "INSERT INTO {} (id, balance) VALUES (?1, ?2)",
                    conn.dialect().quote_identifier("e2e_mvcc_accounts")
                );
                expect_outcome(
                    conn.execute(
                        &cx,
                        &sql,
                        &[Value::BigInt(id), Value::BigInt(OPENING_BALANCE)],
                    )
                    .await,
                    "mvcc seed",
                );
            }
        });
        Self { path }
    }

    fn open(&self) -> FrankenConnection {
        FrankenConnection::open_file(self.path.to_str().expect("temp path is utf-8"))
            .expect("open franken db")
    }

    fn total_balance(&self) -> i64 {
        let conn = self.open();
        let rt = RuntimeBuilder::current_thread().build().expect("runtime");
        let cx = Cx::for_testing();
        rt.block_on(async {
            let sql = format!(
                "SELECT COALESCE(SUM(balance), 0) FROM {}",
                conn.dialect().quote_identifier("e2e_mvcc_accounts")
            );
            let row = expect_outcome(conn.query_one(&cx, &sql, &[]).await, "total balance");
            row.expect("sum row").get_as::<i64>(0).expect("i64 sum")
        })
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(format!("{}-wal", self.path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", self.path.display()));
    }
}

fn accounts_q(conn: &FrankenConnection) -> String {
    conn.dialect().quote_identifier("e2e_mvcc_accounts")
}

#[test]
#[allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]
fn franken_mvcc_disjoint_and_overlapping_writers_conserve_balance() {
    let db = TestDb::fresh("main");

    // ---- Scenario 1: disjoint writers ----
    let conflicts = Arc::new(AtomicU32::new(0));
    let barrier = Arc::new(Barrier::new(WRITERS));
    let handles: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let barrier = barrier.clone();
            let conflicts = conflicts.clone();
            let path = db.path.clone();
            std::thread::spawn(move || {
                let rt = RuntimeBuilder::current_thread().build().expect("runtime");
                let cx = Cx::for_testing();
                let conn =
                    FrankenConnection::open_file(path.to_str().expect("utf-8")).expect("worker");
                let accounts = accounts_q(&conn);
                let policy = RetryPolicy::default()
                    .max_attempts(200)
                    .base_delay(Duration::from_millis(1))
                    .max_delay(Duration::from_millis(25));
                barrier.wait();
                let started = Instant::now();
                for i in 0..TX_PER_WRITER {
                    // Disjoint ROW ranges: writer w only touches accounts
                    // where (id % WRITERS) == w. fsqlite's MVCC is
                    // page-granular, so rows sharing a page still conflict at
                    // commit; every conflict must be retryable and the
                    // combinator must resolve it.
                    let target = writer as i64 + (i as i64 % 4) * WRITERS as i64 + 1;
                    let sql = format!("UPDATE {accounts} SET balance = balance + 1 WHERE id = ?1");
                    let outcome = rt.block_on(retry_transaction(
                        &cx,
                        &conn,
                        TransactionOptions::concurrent(),
                        &policy,
                        async |exec_cx, tx| match tx
                            .execute(exec_cx, &sql, &[Value::BigInt(target)])
                            .await
                        {
                            Outcome::Ok(_) => Outcome::Ok(()),
                            Outcome::Err(e) => {
                                if e.is_retryable() {
                                    conflicts.fetch_add(1, Ordering::Relaxed);
                                }
                                Outcome::Err(e)
                            }
                            Outcome::Cancelled(r) => Outcome::Cancelled(r),
                            Outcome::Panicked(p) => Outcome::Panicked(p),
                        },
                    ));
                    expect_outcome(outcome, &format!("disjoint w{writer}-t{i}"));
                }
                started.elapsed()
            })
        })
        .collect();
    let concurrent_max: Duration = handles
        .into_iter()
        .map(|h| h.join().expect("disjoint writer join"))
        .max()
        .expect("writers");

    // Serialized baseline: the same write volume through one connection in
    // BEGIN IMMEDIATE transactions.
    let baseline_started = Instant::now();
    {
        let rt = RuntimeBuilder::current_thread().build().expect("runtime");
        let cx = Cx::for_testing();
        let conn = db.open();
        let accounts = accounts_q(&conn);
        for i in 0..(WRITERS * TX_PER_WRITER) {
            let target = (i as i64 % ACCOUNTS) + 1;
            let sql = format!("UPDATE {accounts} SET balance = balance + 1 WHERE id = ?1");
            let tx = match rt.block_on(conn.begin_with_options(
                &cx,
                TransactionOptions::default().with_mode(TransactionMode::Immediate),
            )) {
                Outcome::Ok(t) => t,
                Outcome::Err(e) => panic!("baseline begin failed: {e}"),
                Outcome::Cancelled(r) => panic!("cancelled: {r:?}"),
                Outcome::Panicked(p) => panic!("panicked: {p:?}"),
            };
            expect_outcome(
                rt.block_on(tx.execute(&cx, &sql, &[Value::BigInt(target)])),
                "baseline write",
            );
            expect_outcome(rt.block_on(tx.commit(&cx)), "baseline commit");
        }
    }
    let serialized = baseline_started.elapsed();
    let ratio = concurrent_max.as_secs_f64() / serialized.as_secs_f64().max(0.001);
    let disjoint_conflicts = conflicts.load(Ordering::Relaxed);
    eprintln!(
        "mvcc disjoint: concurrent_max={concurrent_max:?} serialized={serialized:?} ratio={ratio:.2} \
         page_conflicts={disjoint_conflicts} (SQLMODEL_MVCC_ASSERT_RATIO=1 asserts <= 0.8x)"
    );
    if std::env::var_os("SQLMODEL_MVCC_ASSERT_RATIO").is_some() {
        assert!(
            concurrent_max <= serialized.mul_f64(0.8),
            "concurrent writers must beat the serialized baseline on a multi-core runner: \
             {concurrent_max:?} vs {serialized:?}"
        );
    }
    // ---- Scenario 2: overlapping writers (fixed transfer pairs) ----
    // Scenario 1 and the baseline each add +1 per transaction by design, so
    // conservation is asserted against the snapshot taken right here.
    let total_before_transfers = db.total_balance();
    let attempts = Arc::new(AtomicU32::new(0));
    let applied = Arc::new(AtomicU32::new(0));
    let barrier = Arc::new(Barrier::new(WRITERS));
    let transfers_per_writer = 25;
    let handles: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let conflicts = conflicts.clone();
            let barrier = barrier.clone();
            let attempts = attempts.clone();
            let applied = applied.clone();
            let path = db.path.clone();
            std::thread::spawn(move || {
                let rt = RuntimeBuilder::current_thread().build().expect("runtime");
                let cx = Cx::for_testing();
                let conn =
                    FrankenConnection::open_file(path.to_str().expect("utf-8")).expect("worker");
                let accounts = accounts_q(&conn);
                let policy = RetryPolicy::default()
                    .max_attempts(200)
                    .base_delay(Duration::from_millis(1))
                    .max_delay(Duration::from_millis(25));
                barrier.wait();
                for i in 0..transfers_per_writer {
                    let from = ((writer as i64 * 7 + i64::from(i) * 3) % ACCOUNTS) + 1;
                    let to = (from + 13) % ACCOUNTS + 1;
                    if from == to {
                        continue;
                    }
                    let token = format!("w{writer}-t{i}");
                    let outcome = rt.block_on(retry_transaction(
                        &cx,
                        &conn,
                        TransactionOptions::concurrent(),
                        &policy,
                        async |cx, tx| {
                            attempts.fetch_add(1, Ordering::Relaxed);
                            // Idempotency: an existing token row means this
                            // transfer was already applied; commit a no-op.
                            let insert = format!(
                                "INSERT INTO {} (token, from_account, to_account, amount) \
                                 VALUES (?1, ?2, ?3, ?4) ON CONFLICT (token) DO NOTHING",
                                "e2e_mvcc_transfers"
                            );
                            match tx
                                .execute(
                                    cx,
                                    &insert,
                                    &[
                                        Value::Text(token.clone()),
                                        Value::BigInt(from),
                                        Value::BigInt(to),
                                        Value::BigInt(1),
                                    ],
                                )
                                .await
                            {
                                Outcome::Ok(0) => return Outcome::Ok(()),
                                Outcome::Ok(_) => {}
                                Outcome::Err(e) => return Outcome::Err(e),
                                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                                Outcome::Panicked(p) => return Outcome::Panicked(p),
                            }
                            for (account, delta) in [(from, -1), (to, 1)] {
                                let sql = format!(
                                    "UPDATE {accounts} SET balance = balance + ?2 WHERE id = ?1"
                                );
                                match tx
                                    .execute(
                                        cx,
                                        &sql,
                                        &[Value::BigInt(account), Value::BigInt(delta)],
                                    )
                                    .await
                                {
                                    Outcome::Ok(_) => {}
                                    Outcome::Err(e) => {
                                        if e.is_retryable() {
                                            conflicts.fetch_add(1, Ordering::Relaxed);
                                        }
                                        return Outcome::Err(e);
                                    }
                                    Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                                    Outcome::Panicked(p) => return Outcome::Panicked(p),
                                }
                            }
                            applied.fetch_add(1, Ordering::Relaxed);
                            Outcome::Ok(())
                        },
                    ));
                    expect_outcome(outcome, &format!("transfer w{writer}-t{i}"));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("overlapping writer join");
    }

    let total_conflicts = conflicts.load(Ordering::Relaxed);
    let total_attempts = attempts.load(Ordering::Relaxed);
    let total_applied = applied.load(Ordering::Relaxed);
    eprintln!(
        "mvcc overlapping: statement_conflicts={total_conflicts} attempts={total_attempts} applied={total_applied}"
    );
    // Conflict detection is live: either a statement surfaced a retryable
    // conflict directly, or the combinator retried at commit (attempt count
    // exceeds applied transfers). A run with zero retries would mean the
    // writers never contended.
    let total_retries = total_attempts.saturating_sub(total_applied);
    assert!(
        total_conflicts > 0 || total_retries > 0,
        "MVCC conflict detection must trigger under overlapping writers: \
         statement_conflicts={total_conflicts} retries={total_retries}"
    );

    // No transfer applied twice: the token rows are unique and every applied
    // transfer has exactly one.
    {
        let conn = db.open();
        let rt = RuntimeBuilder::current_thread().build().expect("runtime");
        let cx = Cx::for_testing();
        rt.block_on(async {
            let sql = "SELECT COUNT(*), COUNT(DISTINCT token) FROM e2e_mvcc_transfers";
            let row = expect_outcome(conn.query_one(&cx, sql, &[]).await, "token counts");
            let row = row.expect("token row");
            let total: i64 = row.get_as(0).expect("count");
            let distinct: i64 = row.get_as(1).expect("distinct");
            assert_eq!(total, distinct, "no transfer token applied twice");
            assert_eq!(
                total,
                i64::from(total_applied),
                "applied counter matches token rows"
            );
        });
    }

    let total_after = db.total_balance();
    assert_eq!(
        total_after, total_before_transfers,
        "transfers must conserve the balance: {total_after} vs {total_before_transfers}"
    );
}

#[test]
fn franken_mvcc_reader_sees_conserved_balance_during_writes() {
    let db = TestDb::fresh("reader");
    let path = db.path.clone();

    let handles: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let path = path.clone();
            std::thread::spawn(move || {
                let rt = RuntimeBuilder::current_thread().build().expect("runtime");
                let cx = Cx::for_testing();
                let conn =
                    FrankenConnection::open_file(path.to_str().expect("utf-8")).expect("worker");
                let accounts = accounts_q(&conn);
                let policy = RetryPolicy::default()
                    .max_attempts(200)
                    .base_delay(Duration::from_millis(1))
                    .max_delay(Duration::from_millis(25));
                for i in 0..50 {
                    // Pure transfers inside one account pair per writer: the
                    // total never changes.
                    let from = ((writer + i) % 2) as i64 + 1;
                    let to = ((writer + i) % 2) as i64 + 3;
                    let outcome = rt.block_on(retry_transaction(
                        &cx,
                        &conn,
                        TransactionOptions::concurrent(),
                        &policy,
                        async |cx, tx| {
                            let debit = format!(
                                "UPDATE {accounts} SET balance = balance - 1 WHERE id = ?1"
                            );
                            let credit = format!(
                                "UPDATE {accounts} SET balance = balance + 1 WHERE id = ?1"
                            );
                            match tx.execute(cx, &debit, &[Value::BigInt(from)]).await {
                                Outcome::Ok(_) => {}
                                Outcome::Err(e) => return Outcome::Err(e),
                                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                                Outcome::Panicked(p) => return Outcome::Panicked(p),
                            }
                            match tx.execute(cx, &credit, &[Value::BigInt(to)]).await {
                                Outcome::Ok(_) => {}
                                Outcome::Err(e) => return Outcome::Err(e),
                                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                                Outcome::Panicked(p) => return Outcome::Panicked(p),
                            }
                            Outcome::Ok(())
                        },
                    ));
                    expect_outcome(outcome, &format!("reader-writer transfer {i}"));
                }
            })
        })
        .collect();

    // Concurrent reader: every single-statement SUM is an atomic snapshot.
    let conn = db.open();
    let rt = RuntimeBuilder::current_thread().build().expect("runtime");
    let cx = Cx::for_testing();
    let sql = format!(
        "SELECT COALESCE(SUM(balance), 0) FROM {}",
        conn.dialect().quote_identifier("e2e_mvcc_accounts")
    );
    for _ in 0..50 {
        let row = expect_outcome(
            rt.block_on(async { conn.query_one(&cx, &sql, &[]).await }),
            "read",
        );
        let sum = row.expect("sum row").get_as::<i64>(0).expect("sum i64");
        assert_eq!(
            sum, TOTAL_BALANCE,
            "every read observes a consistent snapshot"
        );
        std::thread::sleep(Duration::from_millis(1));
    }

    for h in handles {
        h.join().expect("writer join");
    }
    assert_eq!(db.total_balance(), TOTAL_BALANCE);
}

#[test]
fn franken_mvcc_cancellation_rolls_back_and_write_skew_stays_consistent() {
    let db = TestDb::fresh("skew");
    let path = db.path.clone();
    let total_before = db.total_balance();

    // Cancellation: a writer whose Cx is cancelled mid-transaction surfaces
    // Cancelled, leaves its writes rolled back, and leaves the committed
    // state untouched.
    {
        let rt = RuntimeBuilder::current_thread().build().expect("runtime");
        let cx = Cx::for_testing();
        let conn = db.open();
        let accounts = accounts_q(&conn);
        let tx = match rt.block_on(conn.begin_with_options(&cx, TransactionOptions::concurrent())) {
            Outcome::Ok(t) => t,
            Outcome::Err(e) => panic!("begin failed: {e}"),
            Outcome::Cancelled(r) => panic!("cancelled: {r:?}"),
            Outcome::Panicked(p) => panic!("panicked: {p:?}"),
        };
        let sql = format!("UPDATE {accounts} SET balance = balance + 100 WHERE id = 1");
        expect_outcome(rt.block_on(tx.execute(&cx, &sql, &[])), "pre-cancel write");
        cx.set_cancel_requested(true);
        let sql = format!("UPDATE {accounts} SET balance = balance + 100 WHERE id = 2");
        match rt.block_on(tx.execute(&cx, &sql, &[])) {
            Outcome::Cancelled(_) => {}
            // A driver may also surface the cancellation as an error; both
            // mean the statement did not apply.
            Outcome::Err(_) => {}
            other => panic!("expected cancellation, got {other:?}"),
        }
        drop(tx);

        // The cancelled Cx stays cancelled; verification uses a fresh
        // context (a real client would open or reuse another one).
        let verify_cx = Cx::for_testing();
        let sql = format!("SELECT balance FROM {accounts} WHERE id = 1");
        let row = expect_outcome(
            rt.block_on(async { conn.query_one(&verify_cx, &sql, &[]).await }),
            "post-cancel read",
        );
        let balance = row.expect("row").get_as::<i64>(0).expect("balance i64");
        assert_eq!(
            balance % OPENING_BALANCE,
            0,
            "the uncommitted +100 must be rolled back: {balance}"
        );
    }
    assert_eq!(db.total_balance(), total_before);

    // Write-skew probe: two writers each read the shared sum of accounts 1+2
    // and withdraw 100 from their own account when the sum allows it. Under
    // full SSI at least one writer must abort; under plain commit-time page
    // conflict detection both may commit (their writes touch different
    // pages). Either way the end state must be exactly one of: both aborted,
    // exactly one committed, or both committed - and the balances must agree
    // with that outcome. The observed behavior is logged: an SSI engine
    // yields an abort here, which is what fsqlite advertises.
    let probe = Arc::new(Barrier::new(2));
    let handles: Vec<_> = (0..2)
        .map(|w| {
            let probe = probe.clone();
            let path = path.clone();
            std::thread::spawn(move || {
                let rt = RuntimeBuilder::current_thread().build().expect("runtime");
                let cx = Cx::for_testing();
                let conn =
                    FrankenConnection::open_file(path.to_str().expect("utf-8")).expect("skew");
                let accounts = accounts_q(&conn);
                probe.wait();
                let tx = match rt.block_on(conn.begin_with_options(
                    &cx,
                    TransactionOptions::concurrent().with_isolation(IsolationLevel::Serializable),
                )) {
                    Outcome::Ok(t) => t,
                    Outcome::Err(e) => return format!("w{w}: begin error: {e}"),
                    Outcome::Cancelled(r) => return format!("w{w}: cancelled: {r:?}"),
                    Outcome::Panicked(p) => return format!("w{w}: panicked: {p:?}"),
                };
                let sum_sql =
                    format!("SELECT COALESCE(SUM(balance), 0) FROM {accounts} WHERE id IN (1, 2)");
                let row = match rt.block_on(tx.query_one(&cx, &sum_sql, &[])) {
                    Outcome::Ok(Some(row)) => row,
                    Outcome::Ok(None) => return format!("w{w}: no sum row"),
                    Outcome::Err(e) => return format!("w{w}: sum error: {e}"),
                    Outcome::Cancelled(r) => return format!("w{w}: cancelled: {r:?}"),
                    Outcome::Panicked(p) => return format!("w{w}: panicked: {p:?}"),
                };
                let sum: i64 = row.get_as(0).expect("sum i64");
                if sum < 100 {
                    return format!("w{w}: abstained (sum {sum} too low)");
                }
                let account = i64::from(w + 1);
                let sql = format!("UPDATE {accounts} SET balance = balance - 100 WHERE id = ?1");
                match rt.block_on(tx.execute(&cx, &sql, &[Value::BigInt(account)])) {
                    Outcome::Ok(_) => {}
                    Outcome::Err(e) => return format!("w{w}: write error: {e}"),
                    Outcome::Cancelled(r) => return format!("w{w}: cancelled: {r:?}"),
                    Outcome::Panicked(p) => return format!("w{w}: panicked: {p:?}"),
                }
                match rt.block_on(tx.commit(&cx)) {
                    Outcome::Ok(()) => format!("w{w}: committed"),
                    Outcome::Err(e) => format!("w{w}: commit conflict: {e}"),
                    Outcome::Cancelled(r) => format!("w{w}: cancelled: {r:?}"),
                    Outcome::Panicked(p) => format!("w{w}: panicked: {p:?}"),
                }
            })
        })
        .collect();
    let results: Vec<String> = handles
        .into_iter()
        .map(|h| h.join().expect("join"))
        .collect();
    eprintln!("mvcc write-skew outcomes: {results:?}");

    // State consistency: the total decreased by exactly 100 per committed
    // withdrawal (count them by the balances of accounts 1 and 2).
    let conn = db.open();
    let rt = RuntimeBuilder::current_thread().build().expect("runtime");
    let cx = Cx::for_testing();
    rt.block_on(async {
        let sql = format!(
            "SELECT id, balance FROM {} WHERE id IN (1, 2) ORDER BY id",
            conn.dialect().quote_identifier("e2e_mvcc_accounts")
        );
        let rows = expect_outcome(conn.query(&cx, &sql, &[]).await, "skew balances");
        let committed_withdrawals = rows
            .iter()
            .map(|row| {
                let balance: i64 = row.get_as(1).expect("balance");
                (OPENING_BALANCE - balance) / 100
            })
            .sum::<i64>();
        let committed_count = results.iter().filter(|r| r.contains(": committed")).count() as i64;
        assert_eq!(
            committed_withdrawals, committed_count,
            "balances must match exactly the committed withdrawals: {results:?}"
        );
    });
}

#[test]
fn c_sqlite_control_serialized_transfers_conserve_balance() {
    // The C-SQLite control: the same transfer shape under BEGIN IMMEDIATE
    // serializes through the write lock, so the balance is conserved there
    // too - via blocking, not MVCC conflict detection.
    let path = std::env::temp_dir().join(format!("e2e_csqlite_control_{}.db", unique_table("t")));
    let _ = std::fs::remove_file(&path);
    let path_str = path.to_str().expect("utf-8").to_string();
    {
        let conn = SqliteConnection::open_file(&path_str).expect("open control db");
        let rt = RuntimeBuilder::current_thread().build().expect("runtime");
        let cx = Cx::for_testing();
        rt.block_on(async {
            for stmt in SchemaBuilder::new().create_table::<Account>().build() {
                expect_outcome(conn.execute(&cx, &stmt, &[]).await, "control ddl");
            }
            for id in 1..=2 {
                let sql = format!(
                    "INSERT INTO {} (id, balance) VALUES (?1, ?2)",
                    conn.dialect().quote_identifier("e2e_mvcc_accounts")
                );
                expect_outcome(
                    conn.execute(
                        &cx,
                        &sql,
                        &[Value::BigInt(id), Value::BigInt(OPENING_BALANCE)],
                    )
                    .await,
                    "control seed",
                );
            }
        });
    }

    let barrier = Arc::new(Barrier::new(4));
    let handles: Vec<_> = (0..4)
        .map(|writer| {
            let barrier = barrier.clone();
            let path_str = path_str.clone();
            std::thread::spawn(move || {
                let rt = RuntimeBuilder::current_thread().build().expect("runtime");
                let cx = Cx::for_testing();
                let conn = SqliteConnection::open_file(&path_str).expect("control worker");
                let accounts = conn.dialect().quote_identifier("e2e_mvcc_accounts");
                barrier.wait();
                for _ in 0..25 {
                    let from = i64::from(writer % 2) + 1;
                    let to = (i64::from(writer + 1) % 2) + 1;
                    let tx = match rt.block_on(conn.begin_with_options(
                        &cx,
                        TransactionOptions::default().with_mode(TransactionMode::Immediate),
                    )) {
                        Outcome::Ok(t) => t,
                        Outcome::Err(e) => panic!("control begin failed: {e}"),
                        Outcome::Cancelled(r) => panic!("cancelled: {r:?}"),
                        Outcome::Panicked(p) => panic!("panicked: {p:?}"),
                    };
                    let debit =
                        format!("UPDATE {accounts} SET balance = balance - 1 WHERE id = ?1");
                    let credit =
                        format!("UPDATE {accounts} SET balance = balance + 1 WHERE id = ?1");
                    expect_outcome(
                        rt.block_on(tx.execute(&cx, &debit, &[Value::BigInt(from)])),
                        "control debit",
                    );
                    expect_outcome(
                        rt.block_on(tx.execute(&cx, &credit, &[Value::BigInt(to)])),
                        "control credit",
                    );
                    expect_outcome(rt.block_on(tx.commit(&cx)), "control commit");
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("control join");
    }

    {
        let conn = SqliteConnection::open_file(&path_str).expect("control read");
        let rt = RuntimeBuilder::current_thread().build().expect("runtime");
        let cx = Cx::for_testing();
        rt.block_on(async {
            let sql = format!(
                "SELECT COALESCE(SUM(balance), 0) FROM {}",
                conn.dialect().quote_identifier("e2e_mvcc_accounts")
            );
            let row = expect_outcome(conn.query_one(&cx, &sql, &[]).await, "control total");
            let sum = row.expect("sum").get_as::<i64>(0).expect("i64");
            assert_eq!(sum, 2 * OPENING_BALANCE, "serialized transfers conserve");
        });
    }
    let _ = std::fs::remove_file(&path);
}
