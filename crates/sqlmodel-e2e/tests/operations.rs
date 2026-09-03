//! Every query-builder *operation* executed on every driver: upserts, bulk
//! inserts, RETURNING, eager loading, the explicit `Join` builder, typed
//! subqueries, UPDATE/DELETE with subquery predicates, and raw projections.
//! `expressions.rs` proves the WHERE vocabulary; this file proves the
//! statements around it. Anything a driver cannot do is reported as skipped
//! with the reason, never passed silently.
//!
//! The unit tests pin the SQL text these builders render; this file proves the
//! databases accept that SQL and that the ORM maps the results back correctly.

use asupersync::Cx;
use sqlmodel::prelude::*;
use sqlmodel::{DeleteBuilder, SchemaBuilder, Select, UpdateBuilder};
use sqlmodel_core::{Lazy, RelatedMany};
use sqlmodel_e2e::{DriverUnderTest, Scenario, expect_outcome, run_on_every_driver};

#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table = "e2e_ops_teams")]
struct Team {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(unique)]
    name: String,
    /// One-to-many, hydrated by `all_eager`.
    #[sqlmodel(relationship(model = "Player", foreign_key = "team_id"))]
    players: RelatedMany<Player>,
}

#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table = "e2e_ops_players")]
struct Player {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
    score: i32,
    /// Nullable so a LEFT JOIN with no parent is part of the corpus.
    #[sqlmodel(nullable, foreign_key = "e2e_ops_teams.id")]
    team_id: Option<i64>,
    /// Many-to-one, hydrated by `all_eager`.
    #[sqlmodel(relationship(model = "Team", foreign_key = "team_id"))]
    team: Lazy<Team>,
}

const TEAMS: &str = <Team as Model>::TABLE_NAME;
const PLAYERS: &str = <Player as Model>::TABLE_NAME;

fn team(id: i64, name: &str) -> Team {
    Team {
        id,
        name: name.into(),
        players: RelatedMany::new("team_id"),
    }
}

/// Players 1..=99 rotate through teams 1, 2, 3; player 100 has no team.
fn player(id: i64) -> Player {
    let team_id = (id <= 99).then_some((id - 1) % 3 + 1);
    Player {
        id,
        name: format!("player-{id}"),
        score: i32::try_from(id).expect("small id"),
        team_id,
        team: Lazy::empty(),
    }
}

async fn team_name<C: Connection>(cx: &Cx, conn: &C, id: i64, label: &str) -> String {
    expect_outcome(
        select!(Team)
            .filter(Expr::col("id").eq(id))
            .one(cx, conn)
            .await,
        label,
    )
    .name
}

async fn count<C: Connection>(cx: &Cx, conn: &C, query: Select<Player>, label: &str) -> u64 {
    expect_outcome(query.count(cx, conn).await, label)
}

/// The ids `query` produces, ordered by the players' own id column.
async fn player_ids<C: Connection>(
    cx: &Cx,
    conn: &C,
    query: Select<Player>,
    label: &str,
) -> Vec<i64> {
    expect_outcome(
        query
            .order_by(Expr::qualified(PLAYERS, "id").asc())
            .all(cx, conn)
            .await,
        label,
    )
    .into_iter()
    .map(|p| p.id)
    .collect()
}

/// `SELECT id FROM teams WHERE name = ?` as a typed subquery.
fn team_ids_named(name: &str) -> sqlmodel::SelectQuery {
    select!(Team)
        .columns(&["id"])
        .filter(Expr::col("name").eq(name))
        .into_query()
}

struct Operations;

impl Scenario for Operations {
    async fn run<C: Connection>(&self, cx: &Cx, conn: &C, driver: &DriverUnderTest) {
        let d = driver.name();
        let dialect = driver.dialect();
        for table in [PLAYERS, TEAMS] {
            let quoted = dialect.quote_identifier(table);
            expect_outcome(
                conn.execute(cx, &format!("DROP TABLE IF EXISTS {quoted}"), &[])
                    .await,
                &format!("{d}: drop stale {table}"),
            );
        }
        for stmt in SchemaBuilder::new()
            .dialect(dialect)
            .create_table::<Team>()
            .create_table::<Player>()
            .build()
        {
            expect_outcome(
                conn.execute(cx, &stmt, &[]).await,
                &format!("{d}: ddl `{stmt}`"),
            );
        }
        for t in [team(1, "red"), team(2, "blue"), team(3, "green")] {
            expect_outcome(
                insert!(&t).execute(cx, conn).await,
                &format!("{d}: seed team {}", t.id),
            );
        }

        // ---- Upserts -------------------------------------------------------
        // Conflict on the primary key, update the named column.
        expect_outcome(
            insert!(&team(1, "crimson"))
                .on_conflict_do_update(&["name"])
                .execute(cx, conn)
                .await,
            &format!("{d}: upsert do update"),
        );
        assert_eq!(
            team_name(cx, conn, 1, "after upsert").await,
            "crimson",
            "{d}"
        );
        // Conflict on the primary key, keep the existing row.
        expect_outcome(
            insert!(&team(1, "ignored"))
                .on_conflict_do_nothing()
                .execute(cx, conn)
                .await,
            &format!("{d}: upsert do nothing"),
        );
        assert_eq!(
            team_name(cx, conn, 1, "after do nothing").await,
            "crimson",
            "{d}"
        );
        // Conflict on a UNIQUE column named as the target: no new row.
        expect_outcome(
            insert!(&team(9, "crimson"))
                .on_conflict_target_do_update(&["name"], &["name"])
                .execute(cx, conn)
                .await,
            &format!("{d}: upsert on unique target"),
        );
        let teams = expect_outcome(
            select!(Team)
                .order_by(Expr::col("id").asc())
                .all(cx, conn)
                .await,
            &format!("{d}: teams after unique-target upsert"),
        );
        assert_eq!(
            teams.iter().map(|t| t.id).collect::<Vec<_>>(),
            [1, 2, 3],
            "{d}: unique-target upsert must not add a row"
        );
        // Bulk upsert: one existing key updated, one new key inserted.
        expect_outcome(
            insert_many!(&[team(2, "navy"), team(4, "gold")])
                .on_conflict_do_update(&["name"])
                .execute(cx, conn)
                .await,
            &format!("{d}: bulk upsert"),
        );
        assert_eq!(
            team_name(cx, conn, 2, "bulk upsert existing").await,
            "navy",
            "{d}"
        );
        assert_eq!(
            team_name(cx, conn, 4, "bulk upsert new").await,
            "gold",
            "{d}"
        );
        expect_outcome(
            insert_many!(&[team(3, "ignored"), team(4, "ignored")])
                .on_conflict_do_nothing()
                .execute(cx, conn)
                .await,
            &format!("{d}: bulk do nothing"),
        );
        assert_eq!(
            team_name(cx, conn, 3, "bulk do nothing").await,
            "green",
            "{d}"
        );

        // ---- Bulk insert ---------------------------------------------------
        let players: Vec<Player> = (1..=100).map(player).collect();
        let inserted = expect_outcome(
            insert_many!(&players).execute(cx, conn).await,
            &format!("{d}: bulk insert"),
        );
        assert_eq!(inserted, 100, "{d}: bulk insert affected rows");
        assert_eq!(
            count(cx, conn, select!(Player), "count players").await,
            100,
            "{d}"
        );

        // ---- RETURNING -----------------------------------------------------
        if driver.supports_returning() {
            let row = expect_outcome(
                insert!(&player(101))
                    .returning()
                    .execute_returning(cx, conn)
                    .await,
                &format!("{d}: insert returning"),
            )
            .unwrap_or_else(|| panic!("{d}: insert RETURNING produced no row"));
            assert_eq!(
                row.get_named::<String>("name").unwrap(),
                "player-101",
                "{d}"
            );
            assert_eq!(row.get_named::<i64>("score").unwrap(), 101, "{d}");

            let rows = expect_outcome(
                UpdateBuilder::<Player>::empty()
                    .set("score", 1000)
                    .filter(Expr::col("id").eq(101))
                    .returning()
                    .execute_returning(cx, conn)
                    .await,
                &format!("{d}: update returning"),
            );
            assert_eq!(rows.len(), 1, "{d}");
            assert_eq!(rows[0].get_named::<i64>("score").unwrap(), 1000, "{d}");

            let rows = expect_outcome(
                DeleteBuilder::<Player>::new()
                    .filter(Expr::col("id").eq(101))
                    .returning()
                    .execute_returning(cx, conn)
                    .await,
                &format!("{d}: delete returning"),
            );
            assert_eq!(rows.len(), 1, "{d}");
            assert_eq!(
                rows[0].get_named::<String>("name").unwrap(),
                "player-101",
                "{d}"
            );
        } else {
            // MySQL has no RETURNING; `execute_returning` re-reads the rows by
            // primary key instead (see the CHANGELOG entry that added it).
            let row = expect_outcome(
                insert!(&player(101))
                    .returning()
                    .execute_returning(cx, conn)
                    .await,
                &format!("{d}: insert returning (re-select fallback)"),
            )
            .unwrap_or_else(|| panic!("{d}: insert RETURNING fallback produced no row"));
            assert_eq!(
                row.get_named::<String>("name").unwrap(),
                "player-101",
                "{d}"
            );
            let rows = expect_outcome(
                UpdateBuilder::<Player>::empty()
                    .set("score", 1000)
                    .filter(Expr::col("id").eq(101))
                    .returning()
                    .execute_returning(cx, conn)
                    .await,
                &format!("{d}: update returning (re-select fallback)"),
            );
            assert_eq!(rows.len(), 1, "{d}");
            assert_eq!(rows[0].get_named::<i64>("score").unwrap(), 1000, "{d}");
            let rows = expect_outcome(
                DeleteBuilder::<Player>::new()
                    .filter(Expr::col("id").eq(101))
                    .returning()
                    .execute_returning(cx, conn)
                    .await,
                &format!("{d}: delete returning (re-select fallback)"),
            );
            assert_eq!(rows.len(), 1, "{d}");
            assert_eq!(
                rows[0].get_named::<String>("name").unwrap(),
                "player-101",
                "{d}"
            );
        }
        assert_eq!(
            count(cx, conn, select!(Player), "after returning").await,
            100,
            "{d}"
        );

        // ---- Eager loading -------------------------------------------------
        // Many-to-one: players come back with their team loaded; the player
        // without a team is loaded-as-None, not left unloaded.
        let players = expect_outcome(
            select!(Player)
                .eager(EagerLoader::new().include("team"))
                .filter(Expr::qualified(PLAYERS, "id").in_list(vec![1, 2, 3, 100]))
                .order_by(Expr::qualified(PLAYERS, "id").asc())
                .all_eager(cx, conn)
                .await,
            &format!("{d}: eager many-to-one"),
        );
        assert_eq!(
            players.iter().map(|p| p.id).collect::<Vec<_>>(),
            [1, 2, 3, 100],
            "{d}"
        );
        for (p, expected) in players.iter().zip(["crimson", "navy", "green"]) {
            assert!(p.team.is_loaded(), "{d}: player {} team not loaded", p.id);
            assert_eq!(
                p.team.get().map(|t| t.name.as_str()),
                Some(expected),
                "{d}: player {} team",
                p.id
            );
        }
        let orphan = &players[3];
        assert!(
            orphan.team.is_loaded(),
            "{d}: orphan must be loaded-as-None"
        );
        assert!(orphan.team.get().is_none(), "{d}: orphan has no team");

        // One-to-many: one Team per row despite the fan-out, with its players.
        let teams = expect_outcome(
            select!(Team)
                .eager(EagerLoader::new().include("players"))
                .order_by(Expr::qualified(TEAMS, "id").asc())
                .all_eager(cx, conn)
                .await,
            &format!("{d}: eager one-to-many"),
        );
        assert_eq!(
            teams.iter().map(|t| t.id).collect::<Vec<_>>(),
            [1, 2, 3, 4],
            "{d}: eager result is deduplicated"
        );
        for t in &teams {
            assert!(
                t.players.is_loaded(),
                "{d}: team {} players not loaded",
                t.id
            );
            let expected_len = if t.id == 4 { 0 } else { 33 };
            assert_eq!(t.players.len(), expected_len, "{d}: team {} players", t.id);
            assert!(
                t.players.iter().all(|p| p.team_id == Some(t.id)),
                "{d}: team {} got another team's players",
                t.id
            );
        }

        // ---- Explicit Join builder -----------------------------------------
        let navy = player_ids(
            cx,
            conn,
            select!(Player)
                .join(Join::inner(
                    TEAMS,
                    Expr::qualified(PLAYERS, "team_id").eq(Expr::qualified(TEAMS, "id")),
                ))
                .filter(Expr::qualified(TEAMS, "name").eq("navy")),
            &format!("{d}: inner join"),
        )
        .await;
        assert_eq!(navy.len(), 33, "{d}: inner join rows");
        assert!(navy.iter().all(|id| (id - 1) % 3 + 1 == 2), "{d}: {navy:?}");
        let with_left_join = player_ids(
            cx,
            conn,
            select!(Player)
                .join(Join::left(
                    TEAMS,
                    Expr::qualified(PLAYERS, "team_id").eq(Expr::qualified(TEAMS, "id")),
                ))
                .filter(Expr::qualified(TEAMS, "id").is_null()),
            &format!("{d}: left join no match"),
        )
        .await;
        assert_eq!(with_left_join, [100], "{d}: LEFT JOIN keeps the orphan");

        // ---- Subqueries ----------------------------------------------------
        let green = player_ids(
            cx,
            conn,
            select!(Player).filter(Expr::col("team_id").in_query(team_ids_named("green"))),
            &format!("{d}: IN (typed subquery)"),
        )
        .await;
        assert_eq!(green.len(), 33, "{d}");
        assert!(
            green.iter().all(|id| (id - 1) % 3 + 1 == 3),
            "{d}: {green:?}"
        );
        // NOT IN excludes the NULL team_id as SQL requires (NULL NOT IN (...) is NULL).
        let not_green = player_ids(
            cx,
            conn,
            select!(Player).filter(Expr::col("team_id").not_in_query(team_ids_named("green"))),
            &format!("{d}: NOT IN (typed subquery)"),
        )
        .await;
        assert_eq!(not_green.len(), 66, "{d}: NOT IN drops NULLs");
        assert!(!not_green.contains(&100), "{d}");
        // The same through the string form of a subquery.
        let quoted_teams = dialect.quote_identifier(TEAMS);
        let green_raw = player_ids(
            cx,
            conn,
            select!(Player).filter(Expr::col("team_id").in_list(vec![Expr::subquery(format!(
                "SELECT id FROM {quoted_teams} WHERE name = 'green'"
            ))])),
            &format!("{d}: IN (string subquery)"),
        )
        .await;
        assert_eq!(green_raw, green, "{d}");
        // A scalar subquery in a comparison.
        let quoted_players = dialect.quote_identifier(PLAYERS);
        let above_min = count(
            cx,
            conn,
            select!(Player).filter(Expr::col("score").gt(Expr::subquery(format!(
                "SELECT MIN(score) FROM {quoted_players}"
            )))),
            &format!("{d}: scalar subquery"),
        )
        .await;
        assert_eq!(above_min, 99, "{d}");

        // ---- UPDATE / DELETE with subquery predicates ----------------------
        let updated = expect_outcome(
            UpdateBuilder::<Player>::empty()
                .set("score", 0)
                .filter(Expr::col("team_id").in_query(team_ids_named("green")))
                .execute(cx, conn)
                .await,
            &format!("{d}: update with subquery"),
        );
        assert_eq!(updated, 33, "{d}: update affected rows");
        let zeroed = count(
            cx,
            conn,
            select!(Player)
                .filter(Expr::col("score").eq(0))
                .filter(Expr::col("team_id").eq(3)),
            &format!("{d}: zeroed"),
        )
        .await;
        assert_eq!(zeroed, 33, "{d}");
        let deleted_none = expect_outcome(
            DeleteBuilder::<Player>::new()
                .filter(Expr::col("team_id").in_query(team_ids_named("gold")))
                .execute(cx, conn)
                .await,
            &format!("{d}: delete with empty subquery"),
        );
        assert_eq!(deleted_none, 0, "{d}");
        let deleted = expect_outcome(
            DeleteBuilder::<Player>::new()
                .filter(Expr::col("team_id").in_query(team_ids_named("navy")))
                .execute(cx, conn)
                .await,
            &format!("{d}: delete with subquery"),
        );
        assert_eq!(deleted, 33, "{d}: delete affected rows");
        assert_eq!(
            count(cx, conn, select!(Player), "after delete").await,
            67,
            "{d}"
        );

        // ---- Projections into raw rows -------------------------------------
        let (sql, params) = select!(Player)
            .columns(&["name", "score"])
            .filter(Expr::col("id").eq(1))
            .build_with_dialect(dialect);
        let rows = expect_outcome(
            conn.query(cx, &sql, &params).await,
            &format!("{d}: projection `{sql}`"),
        );
        assert_eq!(rows.len(), 1, "{d}");
        assert_eq!(rows[0].len(), 2, "{d}: projected column count");
        assert_eq!(
            rows[0].get_named::<String>("name").unwrap(),
            "player-1",
            "{d}"
        );
        assert_eq!(rows[0].get_named::<i64>("score").unwrap(), 1, "{d}");

        for table in [PLAYERS, TEAMS] {
            let quoted = dialect.quote_identifier(table);
            expect_outcome(
                conn.execute(cx, &format!("DROP TABLE IF EXISTS {quoted}"), &[])
                    .await,
                &format!("{d}: cleanup {table}"),
            );
        }
        eprintln!("{d}: operations corpus ok");
    }
}

#[test]
fn every_query_builder_operation_executes_on_every_available_driver() {
    let cx = Cx::for_testing();
    let ran = run_on_every_driver(&cx, &Operations);
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
    assert!(ran.contains(&"c-sqlite(memory)"), "{ran:?}");
}
