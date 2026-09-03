//! C SQLite versus FrankenSQLite at the ORM level, in lockstep.
//!
//! Two SQLite drivers ship (`sqlmodel-sqlite` over the C library and the
//! pure-Rust `sqlmodel-frankensqlite`). Every operation of one script runs on
//! both, and every observation (rows, affected counts, generated ids,
//! introspection, error *kinds*) must agree, except for the entries of
//! [`KNOWN_DIVERGENCES`]. A divergence that is not listed fails the test; a
//! listed one that no longer diverges also fails ("stale divergence: remove
//! it"), so the list stays truthful across FrankenSQLite's rapid releases.
//!
//! Rows are compared after normalizing integer and float widths (FrankenSQLite
//! reports every INTEGER as `BigInt`, C SQLite narrows to `Int` when the value
//! fits; the ORM's conversions accept both). Storage classes are compared
//! separately through `typeof()`.

use asupersync::Cx;
use sqlmodel::prelude::*;
use sqlmodel::{DeleteBuilder, SchemaBuilder, UpdateBuilder};
use sqlmodel_e2e::{remove_db_family, temp_db_path};
use sqlmodel_frankensqlite::FrankenConnection;
use sqlmodel_schema::Introspector;
use sqlmodel_sqlite::SqliteConnection;

/// Operations whose observations are allowed to differ, with the reason. Keep
/// this list short and every entry justified; the test fails if an entry
/// stops diverging.
///
/// The C SQLite side of every entry was cross-checked against the `sqlite3`
/// CLI (3.46): a failed INSERT and both UPSERT paths leave
/// `last_insert_rowid()` unchanged, `INSERT ... RETURNING` sets it to the new
/// rowid, and `sqlite_master.sql` stores `CREATE TABLE IF NOT EXISTS` as
/// `CREATE TABLE`. FrankenSQLite (fsqlite 0.3.14) differs on exactly these.
/// `insert!(..).execute()` after a *successful* insert agrees on both drivers
/// (see "insert team", "insert plain"); the drift shows only when the counter
/// is read without a successful insert in between.
const KNOWN_DIVERGENCES: &[(&str, &str)] = &[
    (
        "upsert do update",
        "fsqlite bumps last_insert_rowid() for a failed INSERT (the rowid reserved for the \
         rejected row leaks into the counter); C SQLite leaves it unchanged, so the value an \
         updating UPSERT reports differs",
    ),
    (
        "upsert do nothing",
        "same stale counter as `upsert do update` (a DO NOTHING upsert changes it on neither side)",
    ),
    (
        "rowid after insert returning",
        "fsqlite does not set last_insert_rowid() to the rowid of an INSERT ... RETURNING row; \
         C SQLite does",
    ),
    (
        "rowid after delete returning",
        "carries the `rowid after insert returning` value: neither side changes the counter \
         on UPDATE/DELETE",
    ),
    (
        "players ddl",
        "fsqlite stores the CREATE TABLE text verbatim in sqlite_master (keeps `IF NOT EXISTS`); \
         C SQLite stores it normalized as `CREATE TABLE`. The introspector's sqlite_master \
         parsing accepts both",
    ),
];

#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table = "diff_teams")]
struct Team {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(unique)]
    name: String,
    #[sqlmodel(nullable)]
    motto: Option<String>,
}

#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table = "diff_players")]
struct Player {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(foreign_key = "diff_teams.id")]
    team_id: i64,
    #[sqlmodel(index = "diff_players_name_idx")]
    name: String,
    score: i32,
    active: bool,
    #[sqlmodel(nullable)]
    weight: Option<f64>,
}

fn team(id: i64, name: &str, motto: Option<&str>) -> Team {
    Team {
        id,
        name: name.into(),
        motto: motto.map(str::to_owned),
    }
}

fn player(
    id: i64,
    team_id: i64,
    name: &str,
    score: i32,
    active: bool,
    weight: Option<f64>,
) -> Player {
    Player {
        id,
        team_id,
        name: name.into(),
        score,
        active,
        weight,
    }
}

/// What one operation produced, normalized so both drivers can be compared.
#[derive(Debug, Clone, PartialEq)]
enum Observed {
    Rows(Vec<Vec<Value>>),
    Affected(u64),
    Id(i64),
    Text(String),
    Failed(String),
}

fn normalize(v: &Value) -> Value {
    match v {
        Value::TinyInt(i) => Value::BigInt(i64::from(*i)),
        Value::SmallInt(i) => Value::BigInt(i64::from(*i)),
        Value::Int(i) => Value::BigInt(i64::from(*i)),
        Value::Bool(b) => Value::BigInt(i64::from(*b)),
        Value::Float(f) => Value::Double(f64::from(*f)),
        other => other.clone(),
    }
}

fn error_kind(e: &Error) -> String {
    match e {
        Error::Query(q) => format!("query/{:?}", q.kind),
        Error::Connection(_) => "connection".into(),
        Error::Transaction(_) => "transaction".into(),
        Error::Type(_) => "type".into(),
        Error::Schema(_) => "schema".into(),
        _ => "other".into(),
    }
}

fn rows(outcome: Outcome<Vec<Row>, Error>) -> Observed {
    match outcome {
        Outcome::Ok(rows) => Observed::Rows(
            rows.iter()
                .map(|r| r.values().map(normalize).collect())
                .collect(),
        ),
        Outcome::Err(e) => Observed::Failed(error_kind(&e)),
        Outcome::Cancelled(_) => Observed::Failed("cancelled".into()),
        Outcome::Panicked(_) => Observed::Failed("panicked".into()),
    }
}

fn one_row(outcome: Outcome<Option<Row>, Error>) -> Observed {
    match outcome {
        Outcome::Ok(row) => Observed::Rows(
            row.iter()
                .map(|r| r.values().map(normalize).collect())
                .collect(),
        ),
        Outcome::Err(e) => Observed::Failed(error_kind(&e)),
        Outcome::Cancelled(_) => Observed::Failed("cancelled".into()),
        Outcome::Panicked(_) => Observed::Failed("panicked".into()),
    }
}

fn affected(outcome: Outcome<u64, Error>) -> Observed {
    match outcome {
        Outcome::Ok(n) => Observed::Affected(n),
        Outcome::Err(e) => Observed::Failed(error_kind(&e)),
        Outcome::Cancelled(_) => Observed::Failed("cancelled".into()),
        Outcome::Panicked(_) => Observed::Failed("panicked".into()),
    }
}

fn id(outcome: Outcome<i64, Error>) -> Observed {
    match outcome {
        Outcome::Ok(n) => Observed::Id(n),
        Outcome::Err(e) => Observed::Failed(error_kind(&e)),
        Outcome::Cancelled(_) => Observed::Failed("cancelled".into()),
        Outcome::Panicked(_) => Observed::Failed("panicked".into()),
    }
}

/// Raw rows of a `SELECT` built by the query builder.
async fn raw<C: Connection, M: Model>(cx: &Cx, conn: &C, query: sqlmodel::Select<M>) -> Observed {
    let (sql, params) = query.build_with_dialect(Dialect::Sqlite);
    rows(conn.query(cx, &sql, &params).await)
}

/// The script both drivers run. Every step has a unique id (the divergence
/// list refers to it) and produces one observation.
async fn script<C: Connection>(cx: &Cx, conn: &C) -> Vec<(&'static str, Observed)> {
    let mut out: Vec<(&'static str, Observed)> = Vec::new();
    let dialect = Dialect::Sqlite;
    let players_q = dialect.quote_identifier(<Player as Model>::TABLE_NAME);
    let teams_q = dialect.quote_identifier(<Team as Model>::TABLE_NAME);

    // Schema.
    out.push((
        "pragma foreign_keys",
        affected(conn.execute(cx, "PRAGMA foreign_keys = ON", &[]).await),
    ));
    let mut ddl_ok = 0u64;
    for stmt in SchemaBuilder::new()
        .dialect(dialect)
        .create_table::<Team>()
        .create_table::<Player>()
        .build()
    {
        if let Outcome::Ok(_) = conn.execute(cx, &stmt, &[]).await {
            ddl_ok += 1;
        }
    }
    out.push(("ddl statements applied", Observed::Affected(ddl_ok)));

    // Inserts, single and bulk, plus the constraint errors.
    for t in [
        team(1, "red", None),
        team(2, "blue", Some("go blue")),
        team(3, "green", None),
    ] {
        out.push(("insert team", id(insert!(&t).execute(cx, conn).await)));
    }
    let players: Vec<Player> = (1..=12)
        .map(|i| {
            player(
                i,
                (i - 1) % 3 + 1,
                &format!("player-{i}"),
                i32::try_from(i * 10).unwrap(),
                i % 2 == 0,
                if i % 4 == 0 {
                    None
                } else {
                    Some(f64::from(i32::try_from(i).unwrap()) * 1.5)
                },
            )
        })
        .collect();
    out.push((
        "bulk insert players",
        affected(insert_many!(&players).execute(cx, conn).await),
    ));
    out.push((
        "duplicate primary key",
        id(insert!(&team(1, "again", None)).execute(cx, conn).await),
    ));
    out.push((
        "duplicate unique",
        id(insert!(&team(9, "red", None)).execute(cx, conn).await),
    ));
    out.push((
        "foreign key violation",
        id(insert!(&player(999, 42, "orphan", 0, true, None))
            .execute(cx, conn)
            .await),
    ));
    out.push((
        "upsert do update",
        id(insert!(&team(1, "crimson", Some("still red")))
            .on_conflict_do_update(&["name", "motto"])
            .execute(cx, conn)
            .await),
    ));
    out.push((
        "upsert do nothing",
        id(insert!(&team(2, "ignored", None))
            .on_conflict_do_nothing()
            .execute(cx, conn)
            .await),
    ));
    out.push((
        "teams after upserts",
        raw(cx, conn, select!(Team).order_by(Expr::col("id").asc())).await,
    ));

    // Reads through the builder.
    out.push((
        "select all players",
        raw(cx, conn, select!(Player).order_by(Expr::col("id").asc())).await,
    ));
    out.push((
        "filter eq and gt",
        raw(
            cx,
            conn,
            select!(Player)
                .filter(Expr::col("team_id").eq(2))
                .filter(Expr::col("score").gt(30))
                .order_by(Expr::col("id").asc()),
        )
        .await,
    ));
    out.push((
        "filter like in between null",
        raw(
            cx,
            conn,
            select!(Player)
                .filter(Expr::col("name").like("player-1%"))
                .filter(Expr::col("team_id").in_list(vec![1, 2]))
                .filter(Expr::col("score").between(10, 110))
                .filter(Expr::col("weight").is_not_null())
                .order_by(Expr::col("score").desc()),
        )
        .await,
    ));
    out.push((
        "distinct limit offset",
        raw(
            cx,
            conn,
            select!(Player)
                .columns(&["team_id"])
                .distinct()
                .order_by(Expr::col("team_id").asc())
                .limit(2)
                .offset(1),
        )
        .await,
    ));
    out.push((
        "group by aggregates",
        rows(
            conn.query(
                cx,
                &format!(
                    "SELECT team_id, COUNT(*), SUM(score), AVG(score), MIN(weight), MAX(weight) \
                     FROM {players_q} GROUP BY team_id HAVING COUNT(*) > 1 ORDER BY team_id"
                ),
                &[],
            )
            .await,
        ),
    ));
    out.push((
        "scalar functions",
        rows(
            conn.query(
                cx,
                &format!(
                    "SELECT UPPER(name), LENGTH(name), ABS(-score), ROUND(weight, 1), \
                     COALESCE(weight, -1), CASE WHEN active THEN 'y' ELSE 'n' END, \
                     name || '/' || CAST(score AS TEXT) FROM {players_q} ORDER BY id"
                ),
                &[],
            )
            .await,
        ),
    ));
    out.push((
        "inner join",
        raw(
            cx,
            conn,
            select!(Player)
                .join(Join::inner(
                    <Team as Model>::TABLE_NAME,
                    Expr::qualified(<Player as Model>::TABLE_NAME, "team_id")
                        .eq(Expr::qualified(<Team as Model>::TABLE_NAME, "id")),
                ))
                .filter(Expr::qualified(<Team as Model>::TABLE_NAME, "name").eq("blue"))
                .order_by(Expr::qualified(<Player as Model>::TABLE_NAME, "id").asc()),
        )
        .await,
    ));
    out.push((
        "in subquery",
        raw(
            cx,
            conn,
            select!(Player)
                .filter(
                    Expr::col("team_id").in_query(
                        select!(Team)
                            .columns(&["id"])
                            .filter(Expr::col("name").eq("green"))
                            .into_query(),
                    ),
                )
                .order_by(Expr::col("id").asc()),
        )
        .await,
    ));
    out.push((
        "exists",
        raw(
            cx,
            conn,
            select!(Team)
                .filter(
                    select!(Player)
                        .filter(Expr::raw("diff_players.team_id = diff_teams.id"))
                        .filter(Expr::col("score").gt(100))
                        .into_exists(),
                )
                .order_by(Expr::col("id").asc()),
        )
        .await,
    ));
    out.push((
        "count",
        rows(
            conn.query(cx, &format!("SELECT COUNT(*) FROM {players_q}"), &[])
                .await,
        ),
    ));
    out.push((
        "storage classes",
        rows(
            conn.query(
                cx,
                &format!(
                    "SELECT typeof(id), typeof(team_id), typeof(name), typeof(score), \
                     typeof(active), typeof(weight) FROM {players_q} WHERE id IN (1, 4) ORDER BY id"
                ),
                &[],
            )
            .await,
        ),
    ));

    // Writes through the builder.
    out.push((
        "update by filter",
        affected(
            UpdateBuilder::<Player>::empty()
                .set("score", 500)
                .filter(Expr::col("team_id").eq(3))
                .execute(cx, conn)
                .await,
        ),
    ));
    out.push((
        "delete by filter",
        affected(
            DeleteBuilder::<Player>::new()
                .filter(Expr::col("id").eq(12))
                .execute(cx, conn)
                .await,
        ),
    ));
    out.push((
        "insert returning",
        one_row(
            insert!(&player(50, 1, "fifty", 5, false, Some(1.25)))
                .returning()
                .execute_returning(cx, conn)
                .await,
        ),
    ));
    out.push((
        "rowid after insert returning",
        rows(conn.query(cx, "SELECT last_insert_rowid()", &[]).await),
    ));
    out.push((
        "update returning",
        rows(
            UpdateBuilder::<Player>::empty()
                .set("active", true)
                .filter(Expr::col("id").eq(50))
                .returning()
                .execute_returning(cx, conn)
                .await,
        ),
    ));
    out.push((
        "delete returning",
        rows(
            DeleteBuilder::<Player>::new()
                .filter(Expr::col("id").eq(50))
                .returning()
                .execute_returning(cx, conn)
                .await,
        ),
    ));
    out.push((
        "rowid after delete returning",
        rows(conn.query(cx, "SELECT last_insert_rowid()", &[]).await),
    ));
    out.push((
        "insert plain",
        id(insert!(&player(51, 2, "fifty-one", 7, true, None))
            .execute(cx, conn)
            .await),
    ));
    out.push((
        "rowid after plain insert",
        rows(conn.query(cx, "SELECT last_insert_rowid()", &[]).await),
    ));
    out.push((
        "rowid of player 51",
        rows(
            conn.query(
                cx,
                &format!("SELECT rowid, id FROM {players_q} WHERE id = 51"),
                &[],
            )
            .await,
        ),
    ));
    out.push((
        "players ddl",
        rows(
            conn.query(
                cx,
                "SELECT sql FROM sqlite_master WHERE name = 'diff_players'",
                &[],
            )
            .await,
        ),
    ));
    out.push(("rowid after failed insert", {
        let _ = insert!(&player(51, 2, "again", 7, true, None))
            .execute(cx, conn)
            .await;
        rows(conn.query(cx, "SELECT last_insert_rowid()", &[]).await)
    }));
    out.push(("rowid after upsert do update", {
        let _ = insert!(&team(2, "navy", None))
            .on_conflict_do_update(&["name"])
            .execute(cx, conn)
            .await;
        rows(conn.query(cx, "SELECT last_insert_rowid()", &[]).await)
    }));

    // A rolled-back transaction leaves no trace; a committed one does.
    match conn.begin(cx).await {
        Outcome::Ok(tx) => {
            let inserted = affected(
                tx.execute(
                    cx,
                    &format!(
                        "INSERT INTO {players_q} (id, team_id, name, score, active, weight) \
                         VALUES (60, 1, 'sixty', 6, 1, NULL)"
                    ),
                    &[],
                )
                .await,
            );
            out.push(("insert inside transaction", inserted));
            out.push((
                "rollback",
                match tx.rollback(cx).await {
                    Outcome::Ok(()) => Observed::Text("rolled back".into()),
                    Outcome::Err(e) => Observed::Failed(error_kind(&e)),
                    _ => Observed::Failed("cancelled".into()),
                },
            ));
        }
        Outcome::Err(e) => out.push(("begin", Observed::Failed(error_kind(&e)))),
        _ => out.push(("begin", Observed::Failed("cancelled".into()))),
    }
    out.push((
        "count after rollback",
        rows(
            conn.query(cx, &format!("SELECT COUNT(*) FROM {players_q}"), &[])
                .await,
        ),
    ));
    match conn.begin(cx).await {
        Outcome::Ok(tx) => {
            let _ = tx
                .execute(
                    cx,
                    &format!("UPDATE {teams_q} SET motto = 'committed' WHERE id = 3"),
                    &[],
                )
                .await;
            out.push((
                "commit",
                match tx.commit(cx).await {
                    Outcome::Ok(()) => Observed::Text("committed".into()),
                    Outcome::Err(e) => Observed::Failed(error_kind(&e)),
                    _ => Observed::Failed("cancelled".into()),
                },
            ));
        }
        Outcome::Err(e) => out.push(("begin 2", Observed::Failed(error_kind(&e)))),
        _ => out.push(("begin 2", Observed::Failed("cancelled".into()))),
    }
    out.push((
        "team 3 after commit",
        raw(cx, conn, select!(Team).filter(Expr::col("id").eq(3))).await,
    ));

    // Introspection of the same DDL.
    let introspector = Introspector::new(dialect);
    for table in [<Team as Model>::TABLE_NAME, <Player as Model>::TABLE_NAME] {
        let observed = match introspector.table_info(cx, conn, table).await {
            Outcome::Ok(info) => Observed::Text(format!("{info:#?}")),
            Outcome::Err(e) => Observed::Failed(error_kind(&e)),
            _ => Observed::Failed("cancelled".into()),
        };
        out.push((
            if table == <Team as Model>::TABLE_NAME {
                "introspect teams"
            } else {
                "introspect players"
            },
            observed,
        ));
    }
    out.push((
        "table names",
        match introspector.table_names(cx, conn).await {
            Outcome::Ok(mut names) => {
                names.retain(|n| n.starts_with("diff_"));
                names.sort();
                Observed::Text(names.join(","))
            }
            Outcome::Err(e) => Observed::Failed(error_kind(&e)),
            _ => Observed::Failed("cancelled".into()),
        },
    ));

    // A syntax error and a missing table report the same error kind.
    out.push(("syntax error", rows(conn.query(cx, "SELEC 1", &[]).await)));
    out.push((
        "missing table",
        rows(conn.query(cx, "SELECT * FROM diff_missing", &[]).await),
    ));
    out
}

/// The `fsqlite` version this workspace pins (from `Cargo.lock`), for the
/// divergence report.
fn fsqlite_version() -> &'static str {
    let lock = include_str!("../../../Cargo.lock");
    let mut lines = lock.lines();
    while let Some(line) = lines.next() {
        if line.trim() == "name = \"fsqlite\""
            && let Some(version) = lines.next()
        {
            return version
                .trim()
                .trim_start_matches("version = ")
                .trim_matches('"');
        }
    }
    "unknown"
}

#[test]
fn c_sqlite_and_frankensqlite_agree_on_the_orm_script() {
    let cx = Cx::for_testing();
    let c_path = temp_db_path("diff_c");
    let f_path = temp_db_path("diff_f");
    let c_conn = SqliteConnection::open_file(c_path.to_string_lossy().into_owned())
        .expect("open C SQLite file database");
    let f_conn = FrankenConnection::open_file(f_path.to_string_lossy().into_owned())
        .expect("open FrankenSQLite file database");
    let rt = asupersync::runtime::RuntimeBuilder::current_thread()
        .build()
        .expect("asupersync runtime");
    let (c_obs, f_obs) = rt.block_on(async {
        let c = script(&cx, &c_conn).await;
        let f = script(&cx, &f_conn).await;
        (c, f)
    });
    drop(c_conn);
    drop(f_conn);
    remove_db_family(&c_path);
    remove_db_family(&f_path);

    assert_eq!(
        c_obs.len(),
        f_obs.len(),
        "both drivers ran the whole script"
    );
    let mut unexpected = Vec::new();
    let mut diverged: Vec<&str> = Vec::new();
    let verbose = std::env::var_os("SQLMODEL_DIFF_VERBOSE").is_some();
    for ((c_op, c_val), (f_op, f_val)) in c_obs.iter().zip(f_obs.iter()) {
        assert_eq!(c_op, f_op, "script order");
        if verbose {
            eprintln!("{c_op}: c-sqlite {c_val:?} | frankensqlite {f_val:?}");
        }
        if c_val == f_val {
            continue;
        }
        diverged.push(c_op);
        if KNOWN_DIVERGENCES.iter().any(|(op, _)| op == c_op) {
            continue;
        }
        unexpected.push(format!(
            "{c_op}:\n  c-sqlite {}: {c_val:#?}\n  frankensqlite (fsqlite {}): {f_val:#?}",
            sqlmodel_sqlite::sqlite_version(),
            fsqlite_version()
        ));
    }
    let stale: Vec<&str> = KNOWN_DIVERGENCES
        .iter()
        .filter(|(op, _)| !diverged.contains(op))
        .map(|(op, _)| *op)
        .collect();
    eprintln!(
        "sqlite differential: {} operations, {} known divergences ({}), C SQLite {}, fsqlite {}",
        c_obs.len(),
        diverged.len(),
        KNOWN_DIVERGENCES
            .iter()
            .map(|(op, why)| format!("{op}: {why}"))
            .collect::<Vec<_>>()
            .join("; "),
        sqlmodel_sqlite::sqlite_version(),
        fsqlite_version()
    );
    assert!(
        unexpected.is_empty(),
        "unlisted divergences between C SQLite and FrankenSQLite:\n{}",
        unexpected.join("\n")
    );
    assert!(
        stale.is_empty(),
        "stale divergences (no longer differ; remove them from KNOWN_DIVERGENCES): {stale:?}"
    );
}
