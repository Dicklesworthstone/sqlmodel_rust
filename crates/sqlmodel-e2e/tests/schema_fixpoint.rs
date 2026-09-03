//! Schema round trip on every driver: introspect the database, diff it
//! against the models, turn the diff into a migration, apply it, introspect
//! again, and require the diff to be empty. Then evolve a model and repeat.
//!
//! This is the fixpoint relation the schema tooling promises, across
//! `schema_diff`, `Migration::from_operations`, `MigrationRunner`, and
//! `Introspector`; the unit tests only ever compared SQL strings.

use asupersync::Cx;
use sqlmodel::prelude::*;
use sqlmodel_e2e::{DriverUnderTest, Scenario, expect_outcome, run_on_every_driver};
use sqlmodel_schema::diff::schema_diff;
use sqlmodel_schema::{
    DatabaseSchema, Introspector, Migration, MigrationRunner, ModelTuple, generator_for_dialect,
};

#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table = "e2e_fix_teams")]
struct Team {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(unique)]
    name: String,
    #[sqlmodel(nullable)]
    motto: Option<String>,
}

#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table = "e2e_fix_players")]
struct Player {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(foreign_key = "e2e_fix_teams.id")]
    team_id: i64,
    #[sqlmodel(index = "e2e_fix_players_name_idx")]
    name: String,
    score: i32,
    active: bool,
}

/// The same table as `Player` after a release: one more nullable column.
#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table = "e2e_fix_players")]
struct PlayerV2 {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(foreign_key = "e2e_fix_teams.id")]
    team_id: i64,
    #[sqlmodel(index = "e2e_fix_players_name_idx")]
    name: String,
    score: i32,
    active: bool,
    #[sqlmodel(nullable)]
    nickname: Option<String>,
}

/// Introspect only the tables this scenario owns (the network databases are
/// shared with other tests), as a schema the differ can compare.
async fn introspect_ours<C: Connection>(
    cx: &Cx,
    conn: &C,
    dialect: Dialect,
    names: &[&str],
    label: &str,
) -> DatabaseSchema {
    let introspector = Introspector::new(dialect);
    let present = expect_outcome(
        introspector.table_names(cx, conn).await,
        &format!("{label}: table_names"),
    );
    let mut schema = DatabaseSchema::new(dialect);
    for name in names {
        if present.iter().any(|t| t == name) {
            let info = expect_outcome(
                introspector.table_info(cx, conn, name).await,
                &format!("{label}: table_info {name}"),
            );
            schema.tables.insert((*name).to_string(), info);
        }
    }
    schema
}

struct Fixpoint;

impl Scenario for Fixpoint {
    async fn run<C: Connection>(&self, cx: &Cx, conn: &C, driver: &DriverUnderTest) {
        let d = driver.name();
        let dialect = driver.dialect();
        let q = |name: &str| dialect.quote_identifier(name);
        let tracking = "e2e_fix_history";
        let ours = [<Team as Model>::TABLE_NAME, <Player as Model>::TABLE_NAME];
        for t in [
            <Player as Model>::TABLE_NAME,
            <Team as Model>::TABLE_NAME,
            tracking,
        ] {
            expect_outcome(
                conn.execute(cx, &format!("DROP TABLE IF EXISTS {}", q(t)), &[])
                    .await,
                &format!("{d}: drop stale {t}"),
            );
        }
        let ddl = generator_for_dialect(dialect);

        // 1. Empty database -> the diff creates both tables.
        let expected = <(Team, Player) as ModelTuple>::database_schema(dialect);
        let current = introspect_ours(cx, conn, dialect, &ours, &format!("{d}: before")).await;
        let diff = schema_diff(&current, &expected);
        assert_eq!(
            diff.operations.len(),
            2,
            "{d}: two CREATE TABLE operations expected, got {:?}",
            diff.operations
        );
        let mut initial = Migration::from_operations(&diff.operations, &*ddl, "initial schema");
        initial.id = "0001_initial".into();
        eprintln!("{d}: initial migration up:\n{}", initial.up);
        let runner = MigrationRunner::new(vec![initial.clone()]).table_name(tracking);
        let applied = expect_outcome(runner.migrate(cx, conn).await, &format!("{d}: migrate"));
        assert_eq!(applied, vec!["0001_initial"], "{d}");

        // 2. Fixpoint: what the database now reports diffs to nothing.
        let current = introspect_ours(cx, conn, dialect, &ours, &format!("{d}: after")).await;
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.is_empty(),
            "{d}: introspected schema must match the models after applying the generated migration; \
             leftover operations: {:#?}\nwarnings: {:?}",
            diff.operations,
            diff.warnings
        );

        // 3. Evolve: a new nullable column on players -> exactly one AddColumn.
        let expected = <(Team, PlayerV2) as ModelTuple>::database_schema(dialect);
        let diff = schema_diff(&current, &expected);
        assert_eq!(
            diff.operations.len(),
            1,
            "{d}: one ADD COLUMN expected, got {:?}",
            diff.operations
        );
        let mut evolve = Migration::from_operations(&diff.operations, &*ddl, "add nickname");
        evolve.id = "0002_nickname".into();
        eprintln!("{d}: evolve migration up:\n{}", evolve.up);
        let runner = MigrationRunner::new(vec![initial, evolve]).table_name(tracking);
        let applied = expect_outcome(runner.migrate(cx, conn).await, &format!("{d}: evolve"));
        assert_eq!(applied, vec!["0002_nickname"], "{d}");
        let current = introspect_ours(cx, conn, dialect, &ours, &format!("{d}: evolved")).await;
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.is_empty(),
            "{d}: fixpoint after evolving; leftover operations: {:#?}",
            diff.operations
        );

        // 4. Rolling the evolution back restores the previous fixpoint.
        let rolled = expect_outcome(runner.rollback(cx, conn).await, &format!("{d}: rollback"));
        assert_eq!(rolled.as_deref(), Some("0002_nickname"), "{d}");
        let expected = <(Team, Player) as ModelTuple>::database_schema(dialect);
        let current = introspect_ours(cx, conn, dialect, &ours, &format!("{d}: rolled")).await;
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.is_empty(),
            "{d}: fixpoint after rollback; leftover operations: {:#?}",
            diff.operations
        );

        for t in [
            <Player as Model>::TABLE_NAME,
            <Team as Model>::TABLE_NAME,
            tracking,
        ] {
            expect_outcome(
                conn.execute(cx, &format!("DROP TABLE {}", q(t)), &[]).await,
                &format!("{d}: drop {t}"),
            );
        }
    }
}

#[test]
fn schema_diff_migration_and_introspection_reach_a_fixpoint_on_every_available_driver() {
    let cx = Cx::for_testing();
    let ran = run_on_every_driver(&cx, &Fixpoint);
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
}
