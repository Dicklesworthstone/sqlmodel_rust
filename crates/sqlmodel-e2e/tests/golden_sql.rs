//! Golden per-dialect SQL snapshots for the query builders.
//!
//! Every operation of the corpus below is rendered for PostgreSQL, SQLite,
//! and MySQL and compared, byte for byte after trimming trailing whitespace,
//! against `golden/<dialect>/<op>.sql`. Identifier quoting, placeholder
//! style (`$n` / `?n` / `?`), keyword casing, the `table__column` aliases of
//! eager projections, and the implicit discriminator/parent joins of
//! inheritance are all part of what is protected: a regression in any of
//! them shows up as a reviewable diff, where a substring assertion would let
//! it through. A mismatch prints a unified diff and fails; the snapshots are
//! regenerated only with `SQLMODEL_UPDATE_GOLDEN=1`, so an update is a
//! deliberate, reviewable change.
//!
//! The session's cascade-delete plan is captured through a recording
//! connection on in-memory C SQLite, so it exists for the SQLite dialect
//! only.

use std::fmt::Write as _;
use std::path::PathBuf;

use asupersync::Cx;
use serde::{Deserialize, Serialize};
use sqlmodel::prelude::*;
use sqlmodel::{DeleteBuilder, EagerLoader, SchemaBuilder, Session, UpdateBuilder};
use sqlmodel_core::{Lazy, RelatedMany};
use sqlmodel_e2e::{CapturingConnection, expect_outcome};
use sqlmodel_query::{Cte, SetOperation, WithQuery};
use sqlmodel_sqlite::SqliteConnection;

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "teams")]
struct Team {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(unique, column = "team_name")]
    name: String,
    #[sqlmodel(nullable, column_comment = "free text")]
    motto: Option<String>,
    #[sqlmodel(relationship(model = "Player", foreign_key = "team_id", cascade_delete))]
    players: RelatedMany<Player>,
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "players")]
struct Player {
    #[sqlmodel(primary_key, auto_increment)]
    id: Option<i64>,
    #[sqlmodel(foreign_key = "teams.id", on_delete = "CASCADE")]
    team_id: i64,
    #[sqlmodel(index = "players_name_idx", sql_type = "VARCHAR(40)")]
    name: String,
    #[sqlmodel(default = "0")]
    score: i32,
    active: bool,
    #[sqlmodel(nullable)]
    weight: Option<f64>,
    #[sqlmodel(relationship(model = "Team", foreign_key = "team_id"))]
    team: Lazy<Team>,
}

/// Joined-table inheritance: `Student` rows live in `student` joined to
/// `person`.
#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table, inheritance = "joined")]
struct Person {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
}

#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table, inherits = "Person")]
struct Student {
    #[sqlmodel(parent)]
    person: Person,
    #[sqlmodel(primary_key)]
    id: i64,
    grade: String,
}

#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table, inherits = "Person")]
struct Teacher {
    #[sqlmodel(parent)]
    person: Person,
    #[sqlmodel(primary_key)]
    id: i64,
    subject: String,
}

#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table = "staff", inherits = "Person")]
struct Staff {
    #[sqlmodel(parent)]
    person: Person,
    #[sqlmodel(primary_key)]
    id: i64,
    office: String,
}

#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table = "alumni", inherits = "Person")]
struct Alumni {
    #[sqlmodel(parent)]
    person: Person,
    #[sqlmodel(primary_key)]
    id: i64,
    graduation_year: i64,
}

/// Concrete-table inheritance: every child owns the full column set in its
/// own table; the base is abstract (no rows of its own).
#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table, inheritance = "concrete")]
struct CtiContent {
    #[sqlmodel(primary_key)]
    id: i64,
    title: String,
}

#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table, inheritance = "concrete", inherits = "CtiContent")]
struct CtiArticle {
    #[sqlmodel(primary_key)]
    id: i64,
    title: String,
    body: String,
}

#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table, inheritance = "concrete", inherits = "CtiContent")]
struct CtiVideo {
    #[sqlmodel(primary_key)]
    id: i64,
    title: String,
    duration: i64,
}

fn team(id: i64, name: &str, motto: Option<&str>) -> Team {
    Team {
        id,
        name: name.into(),
        motto: motto.map(str::to_owned),
        players: RelatedMany::new("team_id"),
    }
}

fn player(id: Option<i64>, team_id: i64, name: &str) -> Player {
    Player {
        id,
        team_id,
        name: name.into(),
        score: 7,
        active: true,
        weight: Some(70.5),
        team: Lazy::from_fk(team_id),
    }
}

const DIALECTS: [Dialect; 3] = [Dialect::Postgres, Dialect::Sqlite, Dialect::Mysql];

fn dialect_dir(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::Postgres => "postgres",
        Dialect::Sqlite => "sqlite",
        Dialect::Mysql => "mysql",
    }
}

/// Every statement the corpus renders for `dialect`: (op, sql, params).
fn corpus(dialect: Dialect) -> Vec<(&'static str, String, Vec<Value>)> {
    let mut out: Vec<(&'static str, String, Vec<Value>)> = Vec::new();
    let mut push = |op: &'static str, (sql, params): (String, Vec<Value>)| {
        out.push((op, sql, params));
    };
    let ddl = |statements: Vec<String>| (statements.join(";\n"), Vec::new());

    // DDL, including the field index, the column comment, the identity /
    // rowid / AUTO_INCREMENT key, and the joined-inheritance child table.
    push(
        "ddl_teams",
        ddl(SchemaBuilder::new()
            .dialect(dialect)
            .create_table::<Team>()
            .build()),
    );
    push(
        "ddl_players",
        ddl(SchemaBuilder::new()
            .dialect(dialect)
            .create_table::<Player>()
            .build()),
    );
    push(
        "ddl_person_student",
        ddl(SchemaBuilder::new()
            .dialect(dialect)
            .create_table::<Person>()
            .create_table::<Student>()
            .build()),
    );

    // INSERT shapes.
    push(
        "insert_team",
        insert!(&team(1, "red", Some("go"))).build_with_dialect(dialect),
    );
    push(
        "insert_player_generated_id",
        insert!(&player(None, 1, "ann")).build_with_dialect(dialect),
    );
    let players = vec![player(Some(10), 1, "bob"), player(Some(11), 2, "cy")];
    push(
        "insert_many_players",
        insert_many!(&players).build_with_dialect(dialect),
    );
    push(
        "upsert_do_update",
        insert!(&team(1, "crimson", None))
            .on_conflict_do_update(&["team_name", "motto"])
            .build_with_dialect(dialect),
    );
    push(
        "upsert_target_do_update",
        insert!(&team(9, "red", None))
            .on_conflict_target_do_update(&["team_name"], &["motto"])
            .build_with_dialect(dialect),
    );
    push(
        "upsert_do_nothing",
        insert!(&team(1, "ignored", None))
            .on_conflict_do_nothing()
            .build_with_dialect(dialect),
    );
    push(
        "insert_returning",
        insert!(&team(2, "blue", None))
            .returning()
            .build_with_dialect(dialect),
    );

    // SELECT shapes.
    push("select_all", select!(Player).build_with_dialect(dialect));
    push(
        "select_filters",
        select!(Player)
            .filter(Expr::col("team_id").eq(1))
            .filter(Expr::col("score").gt(10).or(Expr::col("active").eq(true)))
            .filter(Expr::col("name").like("a%"))
            .filter(Expr::col("id").in_list(vec![1, 2, 3]))
            .filter(Expr::col("score").between(1, 100))
            .filter(Expr::col("weight").is_not_null())
            .filter(Expr::col("motto").is_null().not())
            .build_with_dialect(dialect),
    );
    push(
        "select_order_paging",
        select!(Player)
            .order_by(Expr::col("score").desc())
            .order_by(Expr::col("id").asc())
            .limit(10)
            .offset(20)
            .build_with_dialect(dialect),
    );
    push(
        "select_distinct_columns",
        select!(Player)
            .columns(&["team_id"])
            .distinct()
            .order_by(Expr::col("team_id").asc())
            .build_with_dialect(dialect),
    );
    push(
        "select_group_by_having",
        select!(Player)
            .columns(&["team_id", "COUNT(*)", "SUM(score)"])
            .group_by(&["team_id"])
            .having(Expr::raw("COUNT(*)").gt(1))
            .build_with_dialect(dialect),
    );
    push(
        "select_inner_join",
        select!(Player)
            .join(Join::inner(
                "teams",
                Expr::qualified("players", "team_id").eq(Expr::qualified("teams", "id")),
            ))
            .filter(Expr::qualified("teams", "team_name").eq("red"))
            .order_by(Expr::qualified("players", "id").asc())
            .build_with_dialect(dialect),
    );
    push(
        "select_left_join",
        select!(Player)
            .join(Join::left(
                "teams",
                Expr::qualified("players", "team_id").eq(Expr::qualified("teams", "id")),
            ))
            .filter(Expr::qualified("teams", "id").is_null())
            .build_with_dialect(dialect),
    );
    push(
        "select_in_subquery",
        select!(Player)
            .filter(Expr::col("name").ne("ghost"))
            .filter(
                Expr::col("team_id").in_query(
                    select!(Team)
                        .columns(&["id"])
                        .filter(Expr::col("team_name").eq("red"))
                        .into_query(),
                ),
            )
            .build_with_dialect(dialect),
    );
    push(
        "select_exists",
        select!(Team)
            .filter(
                select!(Player)
                    .filter(Expr::raw("players.team_id = teams.id"))
                    .filter(Expr::col("score").gt(100))
                    .into_exists_with_dialect(dialect),
            )
            .build_with_dialect(dialect),
    );
    push(
        "select_for_update",
        select!(Team)
            .filter(Expr::col("id").eq(1))
            .for_update()
            .build_with_dialect(dialect),
    );
    let mut window_params = Vec::new();
    let ranked = Expr::row_number()
        .over()
        .partition_by(Expr::col("team_id"))
        .order_by_desc(Expr::col("score"))
        .build()
        .build_with_dialect(dialect, &mut window_params, 0);
    push(
        "select_window",
        select!(Player)
            .columns(&["id", "team_id", &ranked])
            .order_by(Expr::col("team_id").asc())
            .build_with_dialect(dialect),
    );

    // Eager projections and polymorphic inheritance selects.
    push(
        "select_eager_many_to_one",
        select!(Player)
            .eager(EagerLoader::new().include("team"))
            .filter(Expr::qualified("players", "id").in_list(vec![1, 2]))
            .order_by(Expr::qualified("players", "id").asc())
            .build_eager_sql_with_dialect(dialect),
    );
    push(
        "select_eager_one_to_many",
        select!(Team)
            .eager(EagerLoader::new().include("players"))
            .order_by(Expr::qualified("teams", "id").asc())
            .build_eager_sql_with_dialect(dialect),
    );
    push(
        "select_joined_child",
        select!(Student)
            .filter(Expr::col("grade").eq("A"))
            .build_with_dialect(dialect),
    );
    push(
        "select_polymorphic_joined",
        select!(Person)
            .polymorphic_joined::<Student>()
            .filter(Expr::qualified("people", "name").like("A%"))
            .order_by(Expr::qualified("people", "id").asc())
            .build_with_dialect(dialect),
    );
    push(
        "select_polymorphic_joined4",
        select!(Person)
            .polymorphic_joined4::<Student, Teacher, Staff, Alumni>()
            .filter(Expr::qualified("people", "name").like("A%"))
            .order_by(Expr::qualified("people", "id").asc())
            .build_with_dialect(dialect),
    );
    push(
        "select_polymorphic_concrete",
        select!(CtiContent)
            .polymorphic_concrete2::<CtiArticle, CtiVideo>()
            .filter(Expr::col("title").like("R%"))
            .order_by(Expr::col("id").asc())
            .limit(25)
            .build_with_dialect(dialect),
    );

    // UPDATE and DELETE shapes.
    push(
        "update_model",
        update!(&team(1, "crimson", Some("still red"))).build_with_dialect(dialect),
    );
    push(
        "update_set_filter",
        UpdateBuilder::<Player>::empty()
            .set("score", 0)
            .set("active", false)
            .filter(Expr::col("team_id").eq(2))
            .build_with_dialect(dialect),
    );
    push(
        "update_with_subquery_returning",
        UpdateBuilder::<Player>::empty()
            .set("score", 100)
            .filter(
                Expr::col("team_id").in_query(
                    select!(Team)
                        .columns(&["id"])
                        .filter(Expr::col("team_name").eq("green"))
                        .into_query(),
                ),
            )
            .returning()
            .build_with_dialect(dialect),
    );
    push(
        "delete_model",
        DeleteBuilder::from_model(&team(1, "red", None)).build_with_dialect(dialect),
    );
    push(
        "delete_filter",
        DeleteBuilder::<Player>::new()
            .filter(Expr::col("active").eq(false))
            .filter(Expr::col("score").lt(5))
            .build_with_dialect(dialect),
    );
    push(
        "delete_with_subquery_returning",
        DeleteBuilder::<Player>::new()
            .filter(Expr::col("team_id").not_in_query(select!(Team).columns(&["id"]).into_query()))
            .returning()
            .build_with_dialect(dialect),
    );

    // CTEs, a recursive CTE, and set operations.
    push(
        "cte_with",
        WithQuery::new()
            .with_cte(
                Cte::new("top_players")
                    .columns(&["id", "score"])
                    .as_select_with_params(
                        "SELECT id, score FROM players WHERE score > ?",
                        vec![Value::Int(50)],
                    ),
            )
            .select("SELECT * FROM top_players ORDER BY score DESC")
            .build_with_dialect(dialect),
    );
    push(
        "cte_recursive",
        WithQuery::new()
            .with_cte(
                Cte::recursive("nums")
                    .columns(&["n"])
                    .as_select("SELECT 1")
                    .union_all("SELECT n + 1 FROM nums WHERE n < 5"),
            )
            .select("SELECT n FROM nums")
            .build_with_dialect(dialect),
    );
    push(
        "set_union_except",
        SetOperation::new(
            "SELECT id FROM players WHERE team_id = ?",
            vec![Value::Int(1)],
        )
        .union(
            "SELECT id FROM players WHERE active = ?",
            vec![Value::Bool(true)],
        )
        .except(
            "SELECT id FROM players WHERE score < ?",
            vec![Value::Int(5)],
        )
        .order_by(Expr::col("id").asc())
        .limit(50)
        .build_with_dialect(dialect),
    );

    out
}

/// The DELETE statements `Session::flush` plans for a team whose `players`
/// relationship cascades, captured on in-memory C SQLite.
fn session_cascade_delete_plan() -> (String, Vec<Value>) {
    let cx = Cx::for_testing();
    let conn = SqliteConnection::open_memory().expect("in-memory C SQLite");
    let rt = asupersync::runtime::RuntimeBuilder::current_thread()
        .build()
        .expect("asupersync runtime");
    rt.block_on(async {
        for stmt in SchemaBuilder::new()
            .dialect(Dialect::Sqlite)
            .create_table::<Team>()
            .create_table::<Player>()
            .build()
        {
            expect_outcome(conn.execute(&cx, &stmt, &[]).await, "golden: ddl");
        }
        expect_outcome(
            conn.execute(
                &cx,
                "INSERT INTO teams (id, team_name, motto) VALUES (1, 'red', NULL)",
                &[],
            )
            .await,
            "golden: seed team",
        );
        expect_outcome(
            conn.execute(
                &cx,
                "INSERT INTO players (team_id, name, score, active, weight) \
                 VALUES (1, 'ann', 1, 1, NULL), (1, 'bob', 2, 0, NULL)",
                &[],
            )
            .await,
            "golden: seed players",
        );
        let mut session = Session::new(CapturingConnection::new(conn));
        let red: Team = expect_outcome(session.get(&cx, 1i64).await, "golden: get team")
            .expect("team 1 exists");
        session.connection().clear();
        session.delete(&red);
        expect_outcome(session.flush(&cx).await, "golden: flush cascade delete");
        let statements = session.connection().statements();
        let mut sql = String::new();
        let mut params = Vec::new();
        for (statement, values) in statements
            .iter()
            .filter(|(s, _)| s.trim_start().to_uppercase().starts_with("DELETE"))
        {
            if !sql.is_empty() {
                sql.push_str(";\n");
            }
            sql.push_str(statement);
            params.extend(values.iter().cloned());
        }
        expect_outcome(session.rollback(&cx).await, "golden: rollback");
        (sql, params)
    })
}

fn render(op: &str, dialect: Dialect, sql: &str, params: &[Value]) -> String {
    let mut text = String::new();
    let _ = writeln!(text, "-- op: {op}");
    let _ = writeln!(text, "-- dialect: {}", dialect_dir(dialect));
    for line in sql.lines() {
        let _ = writeln!(text, "{}", line.trim_end());
    }
    let _ = writeln!(text, "-- params: {params:?}");
    text
}

/// A small unified diff (whole-line LCS) so a mismatch reads like `diff -u`.
fn unified_diff(expected: &str, actual: &str) -> String {
    let old: Vec<&str> = expected.lines().collect();
    let new: Vec<&str> = actual.lines().collect();
    let mut lcs = vec![vec![0usize; new.len() + 1]; old.len() + 1];
    for i in (0..old.len()).rev() {
        for j in (0..new.len()).rev() {
            lcs[i][j] = if old[i] == new[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    let mut out = String::from("--- expected (golden)\n+++ actual\n");
    let (mut i, mut j) = (0, 0);
    while i < old.len() || j < new.len() {
        if i < old.len() && j < new.len() && old[i] == new[j] {
            let _ = writeln!(out, " {}", old[i]);
            i += 1;
            j += 1;
        } else if j < new.len() && (i >= old.len() || lcs[i][j + 1] >= lcs[i + 1][j]) {
            let _ = writeln!(out, "+{}", new[j]);
            j += 1;
        } else {
            let _ = writeln!(out, "-{}", old[i]);
            i += 1;
        }
    }
    out
}

#[test]
fn every_builder_statement_matches_its_golden_snapshot() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("golden");
    let update = std::env::var_os("SQLMODEL_UPDATE_GOLDEN").is_some();
    let mut failures = Vec::new();
    let mut checked = 0usize;

    let mut check = |dialect: Dialect, op: &str, sql: &str, params: &[Value]| {
        let dir = root.join(dialect_dir(dialect));
        let path = dir.join(format!("{op}.sql"));
        let actual = render(op, dialect, sql, params);
        checked += 1;
        if update {
            std::fs::create_dir_all(&dir).expect("golden dir");
            std::fs::write(&path, &actual).expect("write golden");
            return;
        }
        match std::fs::read_to_string(&path) {
            Ok(expected) if expected == actual => {}
            Ok(expected) => failures.push(format!(
                "{}/{op}: snapshot differs\n{}",
                dialect_dir(dialect),
                unified_diff(&expected, &actual)
            )),
            Err(_) => failures.push(format!(
                "{}/{op}: no snapshot at {} (run with SQLMODEL_UPDATE_GOLDEN=1 to create it)\n{actual}",
                dialect_dir(dialect),
                path.display()
            )),
        }
    };

    for dialect in DIALECTS {
        for (op, sql, params) in corpus(dialect) {
            check(dialect, op, &sql, &params);
        }
    }
    let (sql, params) = session_cascade_delete_plan();
    check(Dialect::Sqlite, "session_cascade_delete", &sql, &params);

    eprintln!(
        "golden sql: {checked} snapshots across {} dialects{}",
        DIALECTS.len(),
        if update { " (rewritten)" } else { "" }
    );
    assert!(
        failures.is_empty(),
        "{} golden SQL mismatches:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
