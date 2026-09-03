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

/// One release later: `name` becomes nullable. On SQLite that is a table
/// recreation (copy, drop, rename) which must preserve the rows and the
/// index; on the others it is an `ALTER COLUMN`.
#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table = "e2e_fix_players")]
struct PlayerV3 {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(foreign_key = "e2e_fix_teams.id")]
    team_id: i64,
    #[sqlmodel(index = "e2e_fix_players_name_idx", nullable)]
    name: Option<String>,
    score: i32,
    active: bool,
    #[sqlmodel(nullable)]
    nickname: Option<String>,
}

/// And one more: a nullable `captain_id` referencing teams. Adding a foreign
/// key is a table recreation on SQLite too (with rows and a NULL column).
#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table = "e2e_fix_players")]
struct PlayerV4 {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(foreign_key = "e2e_fix_teams.id")]
    team_id: i64,
    #[sqlmodel(index = "e2e_fix_players_name_idx", nullable)]
    name: Option<String>,
    score: i32,
    active: bool,
    #[sqlmodel(nullable)]
    nickname: Option<String>,
    #[sqlmodel(foreign_key = "e2e_fix_teams.id", nullable)]
    captain_id: Option<i64>,
}

/// The key moves from `id` to (`id`, `team_id`): a DropPrimaryKey followed by
/// an AddPrimaryKey, each a table recreation on SQLite and an `ALTER TABLE`
/// elsewhere. Rows must survive and the change must roll back.
#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table = "e2e_fix_players")]
struct PlayerV5 {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(primary_key, foreign_key = "e2e_fix_teams.id")]
    team_id: i64,
    #[sqlmodel(index = "e2e_fix_players_name_idx", nullable)]
    name: Option<String>,
    score: i32,
    active: bool,
    #[sqlmodel(nullable)]
    nickname: Option<String>,
    #[sqlmodel(foreign_key = "e2e_fix_teams.id", nullable)]
    captain_id: Option<i64>,
}

/// A log table created without any primary key (an append-only log) ...
#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table = "e2e_fix_log")]
struct LogV1 {
    at: i64,
    message: String,
}

/// ... which gains one once it holds rows.
#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table = "e2e_fix_log")]
struct LogV2 {
    #[sqlmodel(primary_key)]
    at: i64,
    message: String,
}

/// The primary-key operations of a diff, in order, for readable assertions.
fn key_ops(diff: &sqlmodel_schema::diff::SchemaDiff) -> Vec<&'static str> {
    use sqlmodel_schema::diff::SchemaOperation;
    diff.operations
        .iter()
        .map(|op| match op {
            SchemaOperation::DropPrimaryKey { .. } => "drop-pk",
            SchemaOperation::AddPrimaryKey { .. } => "add-pk",
            SchemaOperation::CreateTable { .. } => "create-table",
            _ => "other",
        })
        .collect()
}

async fn count<C: Connection>(cx: &Cx, conn: &C, table: &str, label: &str) -> i64 {
    let rows = expect_outcome(
        conn.query(cx, &format!("SELECT COUNT(*) FROM {table}"), &[])
            .await,
        label,
    );
    rows[0].get_as::<i64>(0).expect("count")
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
            <LogV1 as Model>::TABLE_NAME,
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

        // Rows that every later step must preserve.
        let teams_q = q(<Team as Model>::TABLE_NAME);
        let players_q = q(<Player as Model>::TABLE_NAME);
        for stmt in [
            format!("INSERT INTO {teams_q} (id, name) VALUES (1, 'red'), (2, 'blue')"),
            format!(
                "INSERT INTO {players_q} (id, team_id, name, score, active) \
                 VALUES (1, 1, 'ann', 10, TRUE), (2, 2, 'bob', 20, FALSE)"
            ),
        ] {
            expect_outcome(conn.execute(cx, &stmt, &[]).await, &format!("{d}: seed"));
        }

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
        let runner =
            MigrationRunner::new(vec![initial.clone(), evolve.clone()]).table_name(tracking);
        let applied = expect_outcome(runner.migrate(cx, conn).await, &format!("{d}: evolve"));
        assert_eq!(applied, vec!["0002_nickname"], "{d}");
        let current = introspect_ours(cx, conn, dialect, &ours, &format!("{d}: evolved")).await;
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.is_empty(),
            "{d}: fixpoint after evolving; leftover operations: {:#?}",
            diff.operations
        );

        // 4. Evolve again: `name` becomes nullable. On SQLite this is the
        // table-recreation path (rename, create, copy, drop, re-index).
        let expected = <(Team, PlayerV3) as ModelTuple>::database_schema(dialect);
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.iter().any(|op| matches!(
                op,
                sqlmodel_schema::diff::SchemaOperation::AlterColumnNullable { .. }
            )),
            "{d}: expected an AlterColumnNullable, got {:?}",
            diff.operations
        );
        let mut relax = Migration::from_operations(&diff.operations, &*ddl, "name nullable");
        relax.id = "0003_name_nullable".into();
        eprintln!("{d}: relax migration up:\n{}", relax.up);
        let runner = MigrationRunner::new(vec![initial.clone(), evolve.clone(), relax.clone()])
            .table_name(tracking);
        let applied = expect_outcome(runner.migrate(cx, conn).await, &format!("{d}: relax"));
        assert_eq!(applied, vec!["0003_name_nullable"], "{d}");
        let current = introspect_ours(cx, conn, dialect, &ours, &format!("{d}: relaxed")).await;
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.is_empty(),
            "{d}: fixpoint after relaxing; leftover operations: {:#?}",
            diff.operations
        );
        assert_eq!(
            count(cx, conn, &players_q, "players after recreate").await,
            2,
            "{d}"
        );
        let bob = expect_outcome(
            conn.query(
                cx,
                &format!(
                    "SELECT {} FROM {players_q} WHERE {} = 2",
                    q("name"),
                    q("id")
                ),
                &[],
            )
            .await,
            &format!("{d}: read bob"),
        );
        assert_eq!(
            bob[0].get_as::<String>(0).unwrap(),
            "bob",
            "{d}: data survived"
        );
        expect_outcome(
            conn.execute(
                cx,
                &format!(
                    "INSERT INTO {players_q} (id, team_id, name, score, active) \
                     VALUES (3, 1, NULL, 0, TRUE)"
                ),
                &[],
            )
            .await,
            &format!("{d}: NULL name accepted after relaxing"),
        );
        // A NULL name cannot survive the rollback to NOT NULL; remove it first.
        expect_outcome(
            conn.execute(
                cx,
                &format!("DELETE FROM {players_q} WHERE {} = 3", q("id")),
                &[],
            )
            .await,
            &format!("{d}: remove null-name row"),
        );

        // 4b. A new nullable column with a foreign key: ADD COLUMN plus ADD
        // FOREIGN KEY, the latter a table recreation on SQLite. Rows survive,
        // the key is enforced, and rolling it back reaches the fixpoint again.
        let expected = <(Team, PlayerV4) as ModelTuple>::database_schema(dialect);
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.iter().any(|op| matches!(
                op,
                sqlmodel_schema::diff::SchemaOperation::AddForeignKey { .. }
            )),
            "{d}: expected an AddForeignKey, got {:?}",
            diff.operations
        );
        let mut captain = Migration::from_operations(&diff.operations, &*ddl, "captain fk");
        captain.id = "0004_captain".into();
        eprintln!("{d}: captain migration up:\n{}", captain.up);
        let runner = MigrationRunner::new(vec![
            initial.clone(),
            evolve.clone(),
            relax.clone(),
            captain.clone(),
        ])
        .table_name(tracking);
        let applied = expect_outcome(runner.migrate(cx, conn).await, &format!("{d}: captain"));
        assert_eq!(applied, vec!["0004_captain"], "{d}");
        let current = introspect_ours(cx, conn, dialect, &ours, &format!("{d}: captained")).await;
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.is_empty(),
            "{d}: fixpoint after adding the foreign key; leftover operations: {:#?}",
            diff.operations
        );
        assert_eq!(
            count(cx, conn, &players_q, "players after fk recreate").await,
            2,
            "{d}"
        );
        expect_outcome(
            conn.execute(
                cx,
                &format!(
                    "UPDATE {players_q} SET {} = 2 WHERE {} = 1",
                    q("captain_id"),
                    q("id")
                ),
                &[],
            )
            .await,
            &format!("{d}: valid captain"),
        );
        if dialect == Dialect::Sqlite {
            expect_outcome(
                conn.execute(cx, "PRAGMA foreign_keys = ON", &[]).await,
                &format!("{d}: enable foreign keys"),
            );
        }
        assert!(
            matches!(
                conn.execute(
                    cx,
                    &format!(
                        "UPDATE {players_q} SET {} = 99 WHERE {} = 1",
                        q("captain_id"),
                        q("id")
                    ),
                    &[],
                )
                .await,
                Outcome::Err(_)
            ),
            "{d}: the new foreign key must be enforced"
        );

        // 4c. The key moves from `id` to (`id`, `team_id`): DropPrimaryKey then
        // AddPrimaryKey on a populated table (two recreations on SQLite, two
        // ALTER TABLEs elsewhere). Rows survive.
        let current =
            introspect_ours(cx, conn, dialect, &ours, &format!("{d}: before key change")).await;
        let expected = <(Team, PlayerV5) as ModelTuple>::database_schema(dialect);
        let diff = schema_diff(&current, &expected);
        assert_eq!(
            key_ops(&diff),
            ["drop-pk", "add-pk"],
            "{d}: {:?}",
            diff.operations
        );
        let mut composite = Migration::from_operations(&diff.operations, &*ddl, "composite key");
        composite.id = "0005_composite_key".into();
        eprintln!(
            "{d}: composite key migration up:\n{}\ndown:\n{}",
            composite.up, composite.down
        );
        let runner = MigrationRunner::new(vec![
            initial.clone(),
            evolve.clone(),
            relax.clone(),
            captain.clone(),
            composite.clone(),
        ])
        .table_name(tracking);
        let applied = expect_outcome(
            runner.migrate(cx, conn).await,
            &format!("{d}: composite key"),
        );
        assert_eq!(applied, vec!["0005_composite_key"], "{d}");
        let current =
            introspect_ours(cx, conn, dialect, &ours, &format!("{d}: composite key")).await;
        assert_eq!(
            current.tables[<Player as Model>::TABLE_NAME].primary_key,
            vec!["id".to_string(), "team_id".to_string()],
            "{d}: composite key introspected"
        );
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.is_empty(),
            "{d}: fixpoint after the key change; leftover operations: {:#?}",
            diff.operations
        );
        assert_eq!(
            count(cx, conn, &players_q, "players after key change").await,
            2,
            "{d}"
        );

        // 4d. A keyless log table gets rows, then a primary key.
        let ours_with_log = [
            <Team as Model>::TABLE_NAME,
            <Player as Model>::TABLE_NAME,
            <LogV1 as Model>::TABLE_NAME,
        ];
        let log_q = q(<LogV1 as Model>::TABLE_NAME);
        let expected = <(Team, PlayerV5, LogV1) as ModelTuple>::database_schema(dialect);
        let diff = schema_diff(&current, &expected);
        assert_eq!(
            key_ops(&diff),
            ["create-table"],
            "{d}: {:?}",
            diff.operations
        );
        let mut log = Migration::from_operations(&diff.operations, &*ddl, "keyless log");
        log.id = "0006_log".into();
        let runner = MigrationRunner::new(vec![
            initial.clone(),
            evolve.clone(),
            relax.clone(),
            captain.clone(),
            composite.clone(),
            log.clone(),
        ])
        .table_name(tracking);
        let applied = expect_outcome(runner.migrate(cx, conn).await, &format!("{d}: log"));
        assert_eq!(applied, vec!["0006_log"], "{d}");
        expect_outcome(
            conn.execute(
                cx,
                &format!(
                    "INSERT INTO {log_q} ({}, {}) VALUES (1, 'boot'), (2, 'ready')",
                    q("at"),
                    q("message")
                ),
                &[],
            )
            .await,
            &format!("{d}: seed log"),
        );
        let current = introspect_ours(
            cx,
            conn,
            dialect,
            &ours_with_log,
            &format!("{d}: keyless log"),
        )
        .await;
        assert!(
            current.tables[<LogV1 as Model>::TABLE_NAME]
                .primary_key
                .is_empty(),
            "{d}: log created without a key"
        );
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.is_empty(),
            "{d}: fixpoint with the keyless log; leftover operations: {:#?}",
            diff.operations
        );
        let expected = <(Team, PlayerV5, LogV2) as ModelTuple>::database_schema(dialect);
        let diff = schema_diff(&current, &expected);
        assert_eq!(key_ops(&diff), ["add-pk"], "{d}: {:?}", diff.operations);
        let mut log_key = Migration::from_operations(&diff.operations, &*ddl, "log key");
        log_key.id = "0007_log_key".into();
        eprintln!(
            "{d}: log key migration up:\n{}\ndown:\n{}",
            log_key.up, log_key.down
        );
        let runner = MigrationRunner::new(vec![
            initial.clone(),
            evolve.clone(),
            relax.clone(),
            captain.clone(),
            composite.clone(),
            log.clone(),
            log_key.clone(),
        ])
        .table_name(tracking);
        let applied = expect_outcome(runner.migrate(cx, conn).await, &format!("{d}: log key"));
        assert_eq!(applied, vec!["0007_log_key"], "{d}");
        let current = introspect_ours(
            cx,
            conn,
            dialect,
            &ours_with_log,
            &format!("{d}: keyed log"),
        )
        .await;
        assert_eq!(
            current.tables[<LogV1 as Model>::TABLE_NAME].primary_key,
            vec!["at".to_string()],
            "{d}: log key introspected"
        );
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.is_empty(),
            "{d}: fixpoint with the keyed log; leftover operations: {:#?}",
            diff.operations
        );
        assert_eq!(count(cx, conn, &log_q, "log after key").await, 2, "{d}");
        let duplicate = format!(
            "INSERT INTO {log_q} ({}, {}) VALUES (1, 'again')",
            q("at"),
            q("message")
        );
        assert!(
            matches!(conn.execute(cx, &duplicate, &[]).await, Outcome::Err(_)),
            "{d}: the new key must be enforced"
        );

        // 4e. Rolling the key changes back restores each fixpoint, rows intact,
        // and really removes the key (the duplicate is accepted again).
        let rolled = expect_outcome(runner.rollback(cx, conn).await, &format!("{d}: rollback 7"));
        assert_eq!(rolled.as_deref(), Some("0007_log_key"), "{d}");
        let expected = <(Team, PlayerV5, LogV1) as ModelTuple>::database_schema(dialect);
        let current = introspect_ours(
            cx,
            conn,
            dialect,
            &ours_with_log,
            &format!("{d}: unkeyed log"),
        )
        .await;
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.is_empty(),
            "{d}: fixpoint after dropping the log key; leftover operations: {:#?}",
            diff.operations
        );
        assert_eq!(
            count(cx, conn, &log_q, "log after key rollback").await,
            2,
            "{d}"
        );
        expect_outcome(
            conn.execute(cx, &duplicate, &[]).await,
            &format!("{d}: duplicate accepted once the key is gone"),
        );
        assert_eq!(
            count(cx, conn, &log_q, "log with duplicate").await,
            3,
            "{d}"
        );

        let rolled = expect_outcome(runner.rollback(cx, conn).await, &format!("{d}: rollback 6"));
        assert_eq!(rolled.as_deref(), Some("0006_log"), "{d}");
        let expected = <(Team, PlayerV5) as ModelTuple>::database_schema(dialect);
        let current =
            introspect_ours(cx, conn, dialect, &ours_with_log, &format!("{d}: no log")).await;
        assert!(
            !current.tables.contains_key(<LogV1 as Model>::TABLE_NAME),
            "{d}: the log table is gone"
        );
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.is_empty(),
            "{d}: fixpoint after dropping the log; leftover operations: {:#?}",
            diff.operations
        );

        let rolled = expect_outcome(runner.rollback(cx, conn).await, &format!("{d}: rollback 5"));
        assert_eq!(rolled.as_deref(), Some("0005_composite_key"), "{d}");
        let expected = <(Team, PlayerV4) as ModelTuple>::database_schema(dialect);
        let current = introspect_ours(cx, conn, dialect, &ours, &format!("{d}: single key")).await;
        assert_eq!(
            current.tables[<Player as Model>::TABLE_NAME].primary_key,
            vec!["id".to_string()],
            "{d}: the original key is back"
        );
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.is_empty(),
            "{d}: fixpoint after restoring the key; leftover operations: {:#?}",
            diff.operations
        );
        assert_eq!(
            count(cx, conn, &players_q, "players after key rollback").await,
            2,
            "{d}"
        );

        let rolled = expect_outcome(runner.rollback(cx, conn).await, &format!("{d}: rollback 4"));
        assert_eq!(rolled.as_deref(), Some("0004_captain"), "{d}");
        let expected = <(Team, PlayerV3) as ModelTuple>::database_schema(dialect);
        let current = introspect_ours(cx, conn, dialect, &ours, &format!("{d}: uncaptained")).await;
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.is_empty(),
            "{d}: fixpoint after rolling back the foreign key; leftover operations: {:#?}",
            diff.operations
        );
        assert_eq!(
            count(cx, conn, &players_q, "players after fk rollback").await,
            2,
            "{d}"
        );

        // 5. Rolling back twice restores the earlier fixpoints, rows intact.
        let rolled = expect_outcome(runner.rollback(cx, conn).await, &format!("{d}: rollback 3"));
        assert_eq!(rolled.as_deref(), Some("0003_name_nullable"), "{d}");
        let expected = <(Team, PlayerV2) as ModelTuple>::database_schema(dialect);
        let current = introspect_ours(cx, conn, dialect, &ours, &format!("{d}: unrelaxed")).await;
        let diff = schema_diff(&current, &expected);
        assert!(
            diff.operations.is_empty(),
            "{d}: fixpoint after rolling back the recreate; leftover operations: {:#?}",
            diff.operations
        );
        assert_eq!(
            count(cx, conn, &players_q, "players after rollback").await,
            2,
            "{d}"
        );

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
        assert_eq!(
            count(cx, conn, &players_q, "players at the end").await,
            2,
            "{d}"
        );
        assert_eq!(
            count(cx, conn, &teams_q, "teams at the end").await,
            2,
            "{d}"
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
