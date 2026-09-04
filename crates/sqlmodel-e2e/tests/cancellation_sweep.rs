//! Exhaustive cancellation-injection sweeps (`bd-x6jl.2`).
//!
//! Every operation below is run once with no cancellation to discover its
//! checkpoint count `K_max` (one checkpoint per delegated connection call),
//! then once per checkpoint `k = 1..=K_max` with cancellation injected into
//! the `Cx` immediately before the k-th call. Because the injection only
//! flips the `Cx` flag, the driver's own `cancel_requested(cx)` pre-flight
//! guard is what must return `Outcome::Cancelled` — a sweep failure means a
//! checkpoint that silently proceeded, which is exactly the class of bug the
//! "cancel-correct operations" claim forbids.
//!
//! Oracles per cancelled run:
//! 1. the outcome is `Outcome::Cancelled` (never `Ok`, `Err`, or `Panicked`);
//! 2. the call log marks checkpoint `k` as `cancelled_before`;
//! 3. no `commit` runs after the cancellation point (a dropped transaction
//!    must roll back, never commit);
//! 4. unless the operation is non-atomic by design, the database state
//!    matches the oracle: `Snapshot` (equal to the pre-operation snapshot —
//!    transactional operations restore it by rollback, single-call
//!    operations never executed their mutation), `FirstKApplied` (exactly
//!    the first `k - 1` statements applied), or `AllowedExtra` (one of a
//!    documented set of partial states).
//!
//! Runs on C SQLite in memory (per the bead): every iteration gets a fresh
//! `Cx` and a fresh database, so cancellation stickiness and leftover state
//! cannot leak between checkpoints.

#![allow(clippy::result_large_err)]

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};
use serde::{Deserialize, Serialize};
use sqlmodel::Session;
use sqlmodel::prelude::*;
use sqlmodel_core::test_support::CancelAt;
use sqlmodel_core::{
    Lazy, LinkTableInfo, RelatedMany, RetryPolicy, TransactionOptions, retry_transaction,
};
use sqlmodel_e2e::expect_outcome;
use sqlmodel_schema::{Migration, MigrationRunner, create_all};
use sqlmodel_sqlite::SqliteConnection;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Simple row for builder-operation sweeps.
#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_cancel_items")]
struct Item {
    #[sqlmodel(primary_key, auto_increment)]
    id: Option<i64>,
    name: String,
    #[sqlmodel(nullable)]
    owner: Option<i64>,
}

fn item_of(id: i64, name: &str) -> Item {
    Item {
        id: Some(id),
        name: name.to_owned(),
        owner: None,
    }
}

/// One-to-many + many-to-many fixture for session sweeps.
#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_cancel_parents")]
struct Parent {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
    #[sqlmodel(relationship(model = "Child", foreign_key = "parent_id"))]
    children: RelatedMany<Child>,
    #[sqlmodel(relationship(
        model = "Tag",
        many_to_many,
        link_table(
            table = "e2e_cancel_parent_tags",
            local_column = "parent_id",
            remote_column = "tag_id"
        )
    ))]
    tags: RelatedMany<Tag>,
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_cancel_children")]
struct Child {
    #[sqlmodel(primary_key)]
    id: i64,
    title: String,
    #[sqlmodel(primary_key, foreign_key = "e2e_cancel_parents.id")]
    parent_id: i64,
    /// Many-to-one, loaded on demand through `Session::load_lazy` /
    /// `Session::load_many`.
    #[sqlmodel(relationship(model = "Parent", foreign_key = "parent_id"))]
    parent: Lazy<Parent>,
}

fn child(id: i64, parent_id: i64, title: &str) -> Child {
    Child {
        id,
        title: title.to_owned(),
        parent_id,
        parent: Lazy::from_fk(parent_id),
    }
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_cancel_tags")]
struct Tag {
    #[sqlmodel(primary_key)]
    id: i64,
    label: String,
}

/// Joined-table inheritance fixture for the polymorphic sweeps. The models
/// use the bare `table` attribute on purpose: with an explicit
/// `table = "..."` the derive's joined-child inference (parse.rs) does not
/// fire and the polymorphic select rejects the model pair.
#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table, inheritance = "joined")]
struct Person {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table, inherits = "Person")]
struct Student {
    #[sqlmodel(parent)]
    person: Person,
    #[sqlmodel(primary_key)]
    id: i64,
    grade: String,
}

struct Fixture {
    ddl: Vec<&'static str>,
    seed: Vec<&'static str>,
}

fn items_fixture() -> Fixture {
    Fixture {
        ddl: vec![
            "CREATE TABLE e2e_cancel_items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, owner INTEGER)",
        ],
        seed: vec![
            "INSERT INTO e2e_cancel_items (id, name, owner) VALUES (1, 'alpha', 10)",
            "INSERT INTO e2e_cancel_items (id, name, owner) VALUES (2, 'beta', NULL)",
        ],
    }
}

fn family_fixture() -> Fixture {
    Fixture {
        ddl: vec![
            "CREATE TABLE e2e_cancel_parents (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            "CREATE TABLE e2e_cancel_children (id INTEGER PRIMARY KEY, title TEXT NOT NULL, parent_id INTEGER NOT NULL REFERENCES e2e_cancel_parents(id))",
            "CREATE TABLE e2e_cancel_tags (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
            "CREATE TABLE e2e_cancel_parent_tags (parent_id INTEGER NOT NULL REFERENCES e2e_cancel_parents(id), tag_id INTEGER NOT NULL REFERENCES e2e_cancel_tags(id))",
        ],
        seed: vec![
            "INSERT INTO e2e_cancel_parents (id, name) VALUES (1, 'ann'), (2, 'bob')",
            "INSERT INTO e2e_cancel_children (id, title, parent_id) VALUES (11, 'first', 1), (12, 'second', 1), (13, 'third', 2)",
            "INSERT INTO e2e_cancel_tags (id, label) VALUES (7, 'rust'), (8, 'sql')",
            "INSERT INTO e2e_cancel_parent_tags (parent_id, tag_id) VALUES (1, 7), (1, 8)",
        ],
    }
}

fn inheritance_fixture() -> Fixture {
    Fixture {
        ddl: vec![
            "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            "CREATE TABLE students (id INTEGER PRIMARY KEY, grade TEXT NOT NULL)",
        ],
        seed: vec![
            "INSERT INTO people (id, name) VALUES (1, 'ada'), (2, 'bob'), (3, 'cyd')",
            "INSERT INTO students (id, grade) VALUES (2, 'A')",
        ],
    }
}

async fn seeded_db(cx: &Cx, fixture: &Fixture) -> SqliteConnection {
    let conn = SqliteConnection::open_memory().expect("open :memory:");
    for sql in fixture.ddl.iter().chain(fixture.seed.iter()) {
        expect_outcome(conn.execute(cx, sql, &[]).await, "fixture statement");
    }
    conn
}

// ---------------------------------------------------------------------------
// State snapshots (the oracle's notion of "the database")
// ---------------------------------------------------------------------------

async fn items_state(cx: &Cx, conn: &SqliteConnection) -> Vec<String> {
    let rows = expect_outcome(
        conn.query(
            cx,
            "SELECT id, name, owner FROM e2e_cancel_items ORDER BY id",
            &[],
        )
        .await,
        "snapshot items",
    );
    rows.iter()
        .map(|r| {
            format!(
                "{}|{}|{:?}",
                r.get_as::<i64>(0).expect("id"),
                r.get_as::<String>(1).expect("name"),
                r.get_as::<Option<i64>>(2).expect("owner")
            )
        })
        .collect()
}

async fn snapshot_table(cx: &Cx, conn: &SqliteConnection, sql: &str) -> Vec<String> {
    expect_outcome(conn.query(cx, sql, &[]).await, "snapshot table")
        .iter()
        .map(|r| {
            (0..r.len())
                .map(|i| r.get_as::<String>(i).unwrap_or_else(|_| "?".to_owned()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

async fn family_state(cx: &Cx, conn: &SqliteConnection) -> Vec<String> {
    // get_as::<String> on integers is driver-dependent, so every column is
    // read through a CAST to TEXT here.
    let mut out = Vec::new();
    for sql in [
        "SELECT CAST(id AS TEXT), name FROM e2e_cancel_parents ORDER BY id",
        "SELECT CAST(id AS TEXT), title, CAST(parent_id AS TEXT) FROM e2e_cancel_children ORDER BY id",
        "SELECT CAST(id AS TEXT), label FROM e2e_cancel_tags ORDER BY id",
        "SELECT CAST(parent_id AS TEXT), CAST(tag_id AS TEXT) FROM e2e_cancel_parent_tags ORDER BY parent_id, tag_id",
    ] {
        out.extend(snapshot_table(cx, conn, sql).await);
    }
    out
}

async fn inheritance_state(cx: &Cx, conn: &SqliteConnection) -> Vec<String> {
    let mut out = Vec::new();
    for sql in [
        "SELECT CAST(id AS TEXT), name FROM people ORDER BY id",
        "SELECT CAST(id AS TEXT), grade FROM students ORDER BY id",
    ] {
        out.extend(snapshot_table(cx, conn, sql).await);
    }
    out
}

async fn schema_state(cx: &Cx, conn: &SqliteConnection) -> Vec<String> {
    let rows = expect_outcome(
        conn.query(
            cx,
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
            &[],
        )
        .await,
        "snapshot schema",
    );
    rows.iter()
        .map(|r| r.get_as::<String>(0).expect("table name"))
        .collect()
}

// ---------------------------------------------------------------------------
// The sweep driver
// ---------------------------------------------------------------------------

/// What a cancelled run must leave behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Oracle {
    /// Database state equals the pre-operation snapshot for every checkpoint.
    Snapshot,
    /// Non-atomic by design: exactly the first `k - 1` statements of the
    /// operation were applied. `state` must return the sorted list of applied
    /// schema object names, and `applied_by_op` lists the names the operation
    /// adds in order.
    FirstKApplied {
        applied_by_op: &'static [&'static str],
    },
    /// Non-atomic by design with multiple committed steps: every entry names
    /// one documented partial state as the set of schema objects that may
    /// additionally exist relative to the pre-operation snapshot. Any other
    /// extra set fails the sweep.
    AllowedExtra {
        allowed: &'static [&'static [&'static str]],
    },
}

async fn sweep(
    name: &str,
    fixture: &Fixture,
    oracle: Oracle,
    state: impl AsyncFn(&Cx, &SqliteConnection) -> Vec<String>,
    op: impl AsyncFn(&Cx, &CancelAt<SqliteConnection>) -> Outcome<(), Error>,
) {
    // Baseline run: no cancellation. Discovers K_max and proves the seeded
    // fixture supports the operation.
    let cx = Cx::for_testing();
    let conn = seeded_db(&cx, fixture).await;
    let probe = CancelAt::new(conn, 0);
    match op(&cx, &probe).await {
        Outcome::Ok(()) => {}
        other => panic!("{name}: baseline run did not succeed: {other:?}"),
    }
    let k_max = usize::try_from(probe.calls_made()).expect("call count fits usize");
    assert!(
        k_max >= 1,
        "{name}: operation made no delegated connection calls"
    );
    eprintln!("cancel-sweep {name}: K_max={k_max}");

    for k in 1..=k_max {
        let cx = Cx::for_testing();
        let conn = seeded_db(&cx, fixture).await;
        let before = state(&cx, &conn).await;
        let wrapped = CancelAt::new(conn, u64::try_from(k).expect("checkpoint index fits u64"));
        let outcome = op(&cx, &wrapped).await;
        let log = wrapped.log();
        match outcome {
            Outcome::Cancelled(_) => {}
            Outcome::Ok(()) => panic!(
                "{name}: cancellation at checkpoint {k}/{k_max} was not observed (operation completed); log {log:?}"
            ),
            Outcome::Err(e) => panic!(
                "{name}: checkpoint {k}/{k_max} produced Err({e:?}) instead of Cancelled; log {log:?}"
            ),
            Outcome::Panicked(p) => {
                panic!("{name}: checkpoint {k}/{k_max} panicked ({p:?}); log {log:?}")
            }
        }
        assert!(
            wrapped.cancellation_injected(),
            "{name}: checkpoint {k} never injected cancellation"
        );
        assert!(
            log.get(k - 1).is_some_and(|r| r.cancelled_before),
            "{name}: checkpoint {k} is not marked cancelled-before; log {log:?}"
        );
        assert!(
            !log.iter()
                .any(|r| r.call == "tx.commit" && !r.cancelled_before),
            "{name}: a commit ran at or after the cancellation point; log {log:?}"
        );
        // The run Cx is cancelled by the sweep; state reads need a clean one.
        let snap_cx = Cx::for_testing();
        let after = state(&snap_cx, wrapped.inner()).await;
        match oracle {
            Oracle::Snapshot => {
                assert_eq!(
                    after, before,
                    "{name}: partial state after cancellation at checkpoint {k}/{k_max}; log {log:?}"
                );
            }
            Oracle::FirstKApplied { applied_by_op } => {
                for (i, object) in applied_by_op.iter().enumerate() {
                    let applied = after.iter().any(|line| line.contains(object));
                    let want = i < k - 1;
                    assert_eq!(
                        applied, want,
                        "{name}: checkpoint {k}/{k_max}: schema object {object:?} applied={applied}, want applied={want}; state {after:?}"
                    );
                }
            }
            Oracle::AllowedExtra { allowed } => {
                let extras: Vec<&str> = after
                    .iter()
                    .filter(|line| !before.contains(*line))
                    .map(String::as_str)
                    .collect();
                assert!(
                    allowed.iter().any(|state| state == &extras.as_slice()),
                    "{name}: checkpoint {k}/{k_max} left an undocumented partial state {extras:?}; allowed {allowed:?}; full state {after:?}"
                );
            }
        }
    }
    eprintln!("cancel-sweep {name}: k=1..={k_max} all Cancelled, oracle {oracle:?} OK");
}

// ---------------------------------------------------------------------------
// Sweeps
// ---------------------------------------------------------------------------

#[test]
fn cancellation_sweep_builder_and_schema_ops_on_c_sqlite_memory() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("asupersync runtime");
    rt.block_on(async {
        let items = items_fixture();

        sweep(
            "insert! execute",
            &items,
            Oracle::Snapshot,
            items_state,
            async |cx, conn| {
                insert!(&item_of(99, "inserted"))
                    .execute(cx, conn)
                    .await
                    .map(|_| ())
            },
        )
        .await;

        sweep(
            "select all",
            &items,
            Oracle::Snapshot,
            items_state,
            async |cx, conn| select!(Item).all(cx, conn).await.map(|_| ()),
        )
        .await;

        sweep(
            "select first",
            &items,
            Oracle::Snapshot,
            items_state,
            async |cx, conn| select!(Item).first(cx, conn).await.map(|_| ()),
        )
        .await;

        sweep(
            "select one_or_none",
            &items,
            Oracle::Snapshot,
            items_state,
            async |cx, conn| {
                select!(Item)
                    .filter(Expr::col("id").eq(1))
                    .one_or_none(cx, conn)
                    .await
                    .map(|_| ())
            },
        )
        .await;

        sweep(
            "select count",
            &items,
            Oracle::Snapshot,
            items_state,
            async |cx, conn| select!(Item).count(cx, conn).await.map(|_| ()),
        )
        .await;

        sweep(
            "select exists",
            &items,
            Oracle::Snapshot,
            items_state,
            async |cx, conn| select!(Item).exists(cx, conn).await.map(|_| ()),
        )
        .await;

        sweep(
            "update! execute",
            &items,
            Oracle::Snapshot,
            items_state,
            async |cx, conn| {
                update!(&item_of(1, "updated"))
                    .execute(cx, conn)
                    .await
                    .map(|_| ())
            },
        )
        .await;

        sweep(
            "delete! execute",
            &items,
            Oracle::Snapshot,
            items_state,
            async |cx, conn| {
                delete!(Item)
                    .filter(Expr::col("id").eq(1))
                    .execute(cx, conn)
                    .await
                    .map(|_| ())
            },
        )
        .await;

        // `create_all` applies each DDL statement in its own implicit
        // transaction (autocommit): it is non-atomic by design. The exact
        // partial state after a cancellation at checkpoint k is "the first
        // k-1 tables exist".
        let create_all_ddl = [
            "CREATE TABLE e2e_cancel_a (id INTEGER PRIMARY KEY)",
            "CREATE TABLE e2e_cancel_b (id INTEGER PRIMARY KEY)",
            "CREATE TABLE e2e_cancel_c (id INTEGER PRIMARY KEY)",
        ];
        let create_fixture = Fixture {
            ddl: vec![],
            seed: vec![],
        };
        let schemas: Vec<&str> = create_all_ddl.to_vec();
        let applied: &'static [&'static str] = &["e2e_cancel_a", "e2e_cancel_b", "e2e_cancel_c"];
        sweep(
            "create_all",
            &create_fixture,
            Oracle::FirstKApplied {
                applied_by_op: applied,
            },
            schema_state,
            async |cx, conn| create_all(cx, conn, &schemas).await,
        )
        .await;

        // Migration runner: one migration whose up-SQL creates a table. The
        // runner is non-atomic by design: the tracking table is created in
        // its own committed step before any migration runs. The documented
        // partial states after a cancellation are: nothing yet, only the
        // tracking table, or tracking table plus the fully applied migration.
        let migration_fixture = Fixture {
            ddl: vec![],
            seed: vec![],
        };
        let runner = MigrationRunner::new(vec![Migration {
            id: "001".to_owned(),
            description: "create migrated table".to_owned(),
            up: "CREATE TABLE e2e_cancel_migrated (id INTEGER PRIMARY KEY)".to_owned(),
            down: "DROP TABLE e2e_cancel_migrated".to_owned(),
        }]);
        sweep(
            "MigrationRunner::migrate",
            &migration_fixture,
            Oracle::AllowedExtra {
                allowed: &[
                    &[],
                    &["_sqlmodel_migrations"],
                    &["_sqlmodel_migrations", "e2e_cancel_migrated"],
                ],
            },
            schema_state,
            async |cx, conn| runner.migrate(cx, conn).await.map(|_| ()),
        )
        .await;
    });
}

#[test]
fn cancellation_sweep_transaction_and_inheritance_ops_on_c_sqlite_memory() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("asupersync runtime");
    rt.block_on(async {
        let items = items_fixture();

        sweep(
            "retry_transaction",
            &items,
            Oracle::Snapshot,
            items_state,
            async |cx, conn| {
                retry_transaction(
                    cx,
                    conn,
                    TransactionOptions::new(),
                    &RetryPolicy::default(),
                    async |cx: &Cx, tx| {
                        tx.execute(
                            cx,
                            "INSERT INTO e2e_cancel_items (id, name, owner) VALUES (50, 'retried', NULL)",
                            &[],
                        )
                        .await
                        .map(|_| ())
                    },
                )
                .await
            },
        )
        .await;

        let people = inheritance_fixture();

        sweep(
            "select polymorphic_joined all",
            &people,
            Oracle::Snapshot,
            inheritance_state,
            async |cx, conn| {
                select!(Person)
                    .polymorphic_joined::<Student>()
                    .all(cx, conn)
                    .await
                    .map(|_| ())
            },
        )
        .await;

        sweep(
            "select joined child all",
            &people,
            Oracle::Snapshot,
            inheritance_state,
            async |cx, conn| select!(Student).all(cx, conn).await.map(|_| ()),
        )
        .await;

        // Inserting through joined inheritance writes both the parent and the
        // child table; every checkpoint must leave either both or neither.
        sweep(
            "insert! joined-inheritance execute",
            &people,
            Oracle::Snapshot,
            inheritance_state,
            async |cx, conn| {
                let enrollment = Student {
                    person: Person {
                        id: 9,
                        name: "new student".to_owned(),
                    },
                    id: 9,
                    grade: "B".to_owned(),
                };
                insert!(&enrollment).execute(cx, conn).await.map(|_| ())
            },
        )
        .await;
    });
}

/// Session owns its connection, so this variant hands the wrapper to
/// `Session::new` by value and reaches it again through
/// `Session::connection` for the call log and snapshots.
async fn sweep_session<State, Op>(name: &str, fixture: &Fixture, state: State, op: Op)
where
    State: AsyncFn(&Cx, &SqliteConnection) -> Vec<String>,
    Op: AsyncFn(&mut Session<CancelAt<SqliteConnection>>, &Cx) -> Outcome<(), Error>,
{
    let cx = Cx::for_testing();
    let conn = seeded_db(&cx, fixture).await;
    let mut s = Session::new(CancelAt::new(conn, 0));
    match op(&mut s, &cx).await {
        Outcome::Ok(()) => {}
        other => panic!("{name}: baseline run did not succeed: {other:?}"),
    }
    let k_max = usize::try_from(s.connection().calls_made()).expect("call count fits usize");
    assert!(k_max >= 1, "{name}: operation made no connection calls");
    eprintln!("cancel-sweep {name}: K_max={k_max}");

    for k in 1..=k_max {
        let cx = Cx::for_testing();
        let conn = seeded_db(&cx, fixture).await;
        let before = state(&cx, &conn).await;
        let mut s = Session::new(CancelAt::new(
            conn,
            u64::try_from(k).expect("checkpoint index fits u64"),
        ));
        let outcome = op(&mut s, &cx).await;
        let log = s.connection().log();
        match outcome {
            Outcome::Cancelled(_) => {}
            Outcome::Ok(()) => panic!(
                "{name}: cancellation at checkpoint {k}/{k_max} was not observed; log {log:?}"
            ),
            Outcome::Err(e) => panic!(
                "{name}: checkpoint {k}/{k_max} produced Err({e:?}) instead of Cancelled; log {log:?}"
            ),
            Outcome::Panicked(p) => {
                panic!("{name}: checkpoint {k}/{k_max} panicked ({p:?}); log {log:?}")
            }
        }
        assert!(
            !log.iter()
                .any(|r| r.call == "tx.commit" && !r.cancelled_before),
            "{name}: a commit ran at or after the cancellation point; log {log:?}"
        );
        // A cancelled op leaves the session's transaction open (the caller
        // owns the rollback, as in tests/session.rs). Close it with a clean
        // Cx; the rollback then restores the snapshot.
        let snap_cx = Cx::for_testing();
        if s.in_transaction() {
            match s.rollback(&snap_cx).await {
                Outcome::Ok(()) => {}
                Outcome::Err(e) => panic!(
                    "{name}: rollback after cancellation at checkpoint {k}/{k_max} failed: {e:?}; log {log:?}"
                ),
                Outcome::Cancelled(_) => panic!(
                    "{name}: rollback after cancellation at checkpoint {k}/{k_max} was itself cancelled; log {log:?}"
                ),
                Outcome::Panicked(p) => panic!(
                    "{name}: rollback after cancellation at checkpoint {k}/{k_max} panicked ({p:?}); log {log:?}"
                ),
            }
        }
        let after = state(&snap_cx, s.connection().inner()).await;
        assert_eq!(
            after, before,
            "{name}: partial state after cancellation at checkpoint {k}/{k_max}; log {log:?}"
        );
    }
    eprintln!("cancel-sweep {name}: k=1..={k_max} all Cancelled, oracle Snapshot OK");
}

fn parent(id: i64, name: &str) -> Parent {
    Parent {
        id,
        name: name.to_owned(),
        children: RelatedMany::new("parent_id"),
        tags: RelatedMany::with_link_table(LinkTableInfo::new(
            "e2e_cancel_parent_tags",
            "parent_id",
            "tag_id",
        )),
    }
}

#[test]
fn cancellation_sweep_session_ops_on_c_sqlite_memory() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("asupersync runtime");
    rt.block_on(async {
        let family = family_fixture();
        let link = LinkTableInfo::new("e2e_cancel_parent_tags", "parent_id", "tag_id");

        sweep_session("session get", &family, family_state, async |s, cx| {
            s.get::<Parent>(cx, 1).await.map(|_| ())
        })
        .await;

        sweep_session("session add+flush", &family, family_state, async |s, cx| {
            s.add(&parent(9, "new parent"));
            s.flush(cx).await
        })
        .await;

        sweep_session("session commit", &family, family_state, async |s, cx| {
            s.add(&parent(9, "committed parent"));
            s.commit(cx).await
        })
        .await;

        sweep_session("session rollback", &family, family_state, async |s, cx| {
            s.add(&parent(9, "rolled back parent"));
            match s.flush(cx).await {
                Outcome::Ok(()) => {}
                other => return other,
            }
            s.rollback(cx).await
        })
        .await;

        sweep_session("session refresh", &family, family_state, async |s, cx| {
            let ann = parent(1, "ann");
            s.refresh(cx, &ann).await.map(|_| ())
        })
        .await;

        sweep_session("session merge", &family, family_state, async |s, cx| {
            s.merge(cx, parent(1, "merged ann"), true).await.map(|_| ())
        })
        .await;

        sweep_session("session load_lazy", &family, family_state, async |s, cx| {
            // The child was seeded with parent_id = 1; `load_lazy` must fetch
            // exactly that parent row through the wrapper.
            let kid = child(11, 1, "first");
            s.load_lazy(&kid.parent, cx).await.map(|_| ())
        })
        .await;

        sweep_session("session load_many", &family, family_state, async |s, cx| {
            let kids = vec![child(11, 1, "first"), child(13, 2, "third")];
            s.load_many(cx, &kids, |c: &Child| &c.parent)
                .await
                .map(|_| ())
        })
        .await;

        sweep_session(
            "session load_one_to_many",
            &family,
            family_state,
            async |s, cx| {
                let mut parents = vec![parent(1, "ann")];
                s.load_one_to_many(cx, &mut parents, |p| &mut p.children, |p| Value::from(p.id))
                    .await
                    .map(|_| ())
            },
        )
        .await;

        sweep_session(
            "session load_many_to_many",
            &family,
            family_state,
            async |s, cx| {
                let mut parents = vec![parent(1, "ann")];
                s.load_many_to_many_pk(
                    cx,
                    &mut parents,
                    |p| &mut p.tags,
                    |p| vec![Value::from(p.id)],
                    &link,
                )
                .await
                .map(|_| ())
            },
        )
        .await;
    });
}

#[test]
fn cancellation_sweep_pool_ops_on_c_sqlite_memory() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("asupersync runtime");
    rt.block_on(async {
        // The pool's cancellation contract is a pre-flight guard on its own
        // entry (pool acquire/drain are not sequences of delegated connection
        // calls), so this sweep asserts the already-cancelled-Cx contract and
        // balanced counters directly; checkpoint-exhaustive pool timing lives
        // in sqlmodel-pool's lab tests (bd-x6jl.1).
        let cancelled = Cx::for_testing();
        cancelled.cancel_with(asupersync::CancelKind::User, Some("pool acquire sweep"));
        let pool: sqlmodel_pool::Pool<CancelAt<SqliteConnection>> =
            sqlmodel_pool::Pool::new(sqlmodel_pool::PoolConfig::new(2).test_on_checkout(false));
        match pool
            .acquire(&cancelled, || async {
                // Never executed for an already-cancelled Cx; the Err keeps
                // the factory's type honest if that ever changes.
                Outcome::Err(Error::Custom(
                    "pool acquire factory ran for a cancelled Cx".to_owned(),
                ))
            })
            .await
        {
            Outcome::Cancelled(_) => {}
            Outcome::Ok(_) => panic!("cancelled acquire must not hand out a lease"),
            Outcome::Err(_) => panic!("cancelled acquire must be Cancelled, not Err"),
            Outcome::Panicked(_) => panic!("cancelled acquire must be Cancelled, not Panicked"),
        }
        assert_eq!(pool.stats().acquires, 0, "no lease was handed out");
        assert_eq!(pool.stats().connections_created, 0, "factory never ran");
    });
}
