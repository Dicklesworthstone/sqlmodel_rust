//! Schema metamorphic oracle (`bd-slot.8`).
//!
//! Three laws that must hold for ANY schema, on every driver, instead of
//! hand-written expected-SQL cases:
//!
//! 1. **Fixpoint** — create a model's table on an empty database; the diff
//!    between the model's expected schema and introspection must be empty.
//!    If the differ still finds work, DDL, introspection, or the differ is
//!    wrong.
//! 2. **Commutation** — create A, apply `diff(A -> B)`, introspect: the
//!    result must be at a fixpoint for B.
//! 3. **Involution** — for every operation of `diff(A -> B)`: apply it, then
//!    apply its `inverse()`, and the state must return to what it was before
//!    that operation. Destructive operations have no `inverse()` (that is
//!    why rollback uses snapshots) and are skipped here, but the sweep
//!    asserts they stay within the documented destructive set.
//!
//! Plus **cross-dialect agreement**: `diff` produces the same
//! `SchemaOperation`s for every dialect (dialects change DDL text, not the
//! operations), asserted structurally without a database.
//!
//! The generated phase feeds 200 deterministically generated table
//! definitions (seeded LCG — no new dependency) through the fixpoint law on
//! all three SQLite variants, with commutation on a prefix, in under 60
//! seconds.

use asupersync::Cx;
use asupersync::runtime::RuntimeBuilder;
use serde::{Deserialize, Serialize};
use sqlmodel::prelude::*;
use sqlmodel_e2e::{DriverUnderTest, Scenario, expect_outcome, run_on_every_driver, temp_db_path};
use sqlmodel_frankensqlite::FrankenConnection;
use sqlmodel_schema::diff::{SchemaOperation, schema_diff};
use sqlmodel_schema::{
    ColumnInfo, DatabaseSchema, IndexInfo, Introspector, ParsedSqlType, TableInfo, expected_schema,
    generator_for_dialect,
};
use sqlmodel_sqlite::SqliteConnection;

// ---------------------------------------------------------------------------
// Corpus models
// ---------------------------------------------------------------------------

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_oracle_basic")]
struct Basic {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
    #[sqlmodel(nullable)]
    note: Option<String>,
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_oracle_auto")]
struct Auto {
    #[sqlmodel(primary_key, auto_increment)]
    id: Option<i64>,
    label: String,
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_oracle_defaults")]
struct Defaults {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(nullable, default = "0")]
    score: Option<i32>,
    #[sqlmodel(default = "5")]
    level: i32,
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_oracle_unique")]
struct Unique {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(unique)]
    handle: String,
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_oracle_fk")]
struct Fk {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(foreign_key = "e2e_oracle_basic.id", on_delete = "CASCADE")]
    basic_id: i64,
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_oracle_composite")]
struct Composite {
    #[sqlmodel(primary_key, foreign_key = "e2e_oracle_basic.id")]
    basic_id: i64,
    #[sqlmodel(primary_key)]
    seq: i64,
    payload: String,
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_oracle_indexed")]
struct Indexed {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(index = "e2e_oracle_idx_handle")]
    handle: String,
    #[sqlmodel(index = "e2e_oracle_idx_kind", nullable)]
    kind: Option<String>,
}

/// Commutation pair: V1 exists, V2 adds a nullable column plus an index.
#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_oracle_comm")]
struct CommV1 {
    #[sqlmodel(primary_key)]
    id: i64,
    code: String,
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_oracle_comm")]
struct CommV2 {
    #[sqlmodel(primary_key)]
    id: i64,
    code: String,
    #[sqlmodel(nullable, default = "0")]
    weight: Option<i32>,
    #[sqlmodel(index = "e2e_oracle_idx_comm")]
    tag: Option<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

fn assert_fixpoint(
    label: &str,
    dialect: Dialect,
    current: &DatabaseSchema,
    expected: &DatabaseSchema,
) {
    let diff = schema_diff(current, expected);
    assert!(
        diff.operations.is_empty(),
        "{label} ({dialect:?}): fixpoint violated — the differ still finds work: {:#?}\nwarnings: {:?}\ncurrent: {current:#?}\nexpected: {expected:#?}",
        diff.operations,
        diff.warnings
    );
}

async fn apply_operations<C: Connection>(
    cx: &Cx,
    conn: &C,
    dialect: Dialect,
    ops: &[SchemaOperation],
    label: &str,
) {
    let ddl = generator_for_dialect(dialect);
    for (i, stmt) in ddl.generate_all(ops).into_iter().enumerate() {
        expect_outcome(
            conn.execute(cx, &stmt, &[]).await,
            &format!("{label}: DDL #{i}: {stmt}"),
        );
    }
}

fn drop_stale_sql(dialect: Dialect, table: &str) -> String {
    let q = dialect.quote_identifier(table);
    format!("DROP TABLE IF EXISTS {q}")
}

// ---------------------------------------------------------------------------
// Corpus relations (every driver)
// ---------------------------------------------------------------------------

async fn run_corpus_relations<C: Connection>(cx: &Cx, conn: &C, driver: &DriverUnderTest) {
    let d = driver.name();
    let dialect = driver.dialect();
    let q = |name: &str| dialect.quote_identifier(name);

    for t in [
        "e2e_oracle_comm",
        "e2e_oracle_indexed",
        "e2e_oracle_composite",
        "e2e_oracle_fk",
        "e2e_oracle_unique",
        "e2e_oracle_defaults",
        "e2e_oracle_auto",
        "e2e_oracle_basic",
    ] {
        expect_outcome(
            conn.execute(cx, &drop_stale_sql(dialect, t), &[]).await,
            &format!("{d}: drop stale {t}"),
        );
    }

    // ---- Law 1: fixpoint for every corpus model -----------------------
    let corpus: Vec<DatabaseSchema> = vec![
        expected_schema::<Basic>(dialect),
        expected_schema::<Auto>(dialect),
        expected_schema::<Defaults>(dialect),
        expected_schema::<Unique>(dialect),
        expected_schema::<Fk>(dialect),
        expected_schema::<Composite>(dialect),
        expected_schema::<Indexed>(dialect),
    ];
    for expected in &corpus {
        let table = expected.tables.keys().next().expect("one table").clone();
        // Create via the differ itself: empty -> expected must be exactly one
        // CreateTable, whose generated DDL we execute.
        let empty = DatabaseSchema::new(dialect);
        let create = schema_diff(&empty, expected);
        assert_eq!(
            create.operations.len(),
            1,
            "{d} ({table}): expected a single CreateTable, got {:#?}",
            create.operations
        );
        apply_operations(
            cx,
            conn,
            dialect,
            &create.operations,
            &format!("{d}:{table}:create"),
        )
        .await;
        let current =
            introspect_ours(cx, conn, dialect, &[&table], &format!("{d}:{table}:fix")).await;
        assert_fixpoint(
            &format!("{d}: fixpoint {table}"),
            dialect,
            &current,
            expected,
        );
    }

    // ---- Law 2: commutation A -> B ------------------------------------
    expect_outcome(
        conn.execute(
            cx,
            &format!(
                "CREATE TABLE {} (id INTEGER PRIMARY KEY, code VARCHAR(255) NOT NULL)",
                q("e2e_oracle_comm")
            ),
            &[],
        )
        .await,
        &format!("{d}: create comm V1"),
    );
    let current = introspect_ours(
        cx,
        conn,
        dialect,
        &["e2e_oracle_comm"],
        &format!("{d}:comm:s1"),
    )
    .await;
    // The raw V1 table must itself be a fixpoint of the V1 model before the
    // V2 evolution is applied, so the oscillated operations come from a
    // verified baseline.
    let v1 = expected_schema::<CommV1>(dialect);
    let s1_baseline = introspect_ours(
        cx,
        conn,
        dialect,
        &["e2e_oracle_comm"],
        &format!("{d}:comm:v1"),
    )
    .await;
    assert_fixpoint(&format!("{d}: fixpoint CommV1"), dialect, &s1_baseline, &v1);
    let v2 = expected_schema::<CommV2>(dialect);
    let ops = schema_diff(&current, &v2);
    assert!(
        !ops.operations.is_empty(),
        "{d}: commutation V1 -> V2 must produce operations"
    );
    apply_operations(
        cx,
        conn,
        dialect,
        &ops.operations,
        &format!("{d}:comm:apply"),
    )
    .await;
    let after = introspect_ours(
        cx,
        conn,
        dialect,
        &["e2e_oracle_comm"],
        &format!("{d}:comm:s2"),
    )
    .await;
    assert_fixpoint(&format!("{d}: commutation at V2"), dialect, &after, &v2);

    // ---- Law 3: involution (per-op oscillation) -----------------------
    for i in 0..ops.operations.len() {
        // Fresh A-state per attempt; apply the prefix, snapshot, apply
        // ops[i], apply its inverse, and require the snapshot back.
        expect_outcome(
            conn.execute(cx, &drop_stale_sql(dialect, "e2e_oracle_comm"), &[])
                .await,
            &format!("{d}: involution reset"),
        );
        expect_outcome(
            conn.execute(
                cx,
                &format!(
                    "CREATE TABLE {} (id INTEGER PRIMARY KEY, code VARCHAR(255) NOT NULL)",
                    q("e2e_oracle_comm")
                ),
                &[],
            )
            .await,
            &format!("{d}: involution create V1"),
        );
        let s1 = introspect_ours(
            cx,
            conn,
            dialect,
            &["e2e_oracle_comm"],
            &format!("{d}:inv:s1:{i}"),
        )
        .await;
        let full = schema_diff(&s1, &v2);
        if i > 0 {
            apply_operations(
                cx,
                conn,
                dialect,
                &full.operations[..i],
                &format!("{d}:inv:prefix:{i}"),
            )
            .await;
        }
        let mid = introspect_ours(
            cx,
            conn,
            dialect,
            &["e2e_oracle_comm"],
            &format!("{d}:inv:mid:{i}"),
        )
        .await;
        let op = &full.operations[i];
        apply_operations(
            cx,
            conn,
            dialect,
            std::slice::from_ref(op),
            &format!("{d}:inv:op:{i}"),
        )
        .await;
        match op.inverse() {
            Some(SchemaOperation::DropColumn { table, column, .. }) => {
                // inverse() cannot know the post-op table shape, so it leaves
                // table_info empty — and the SQLite generator correctly
                // refuses a snapshot-less DROP COLUMN. Supply the freshly
                // introspected post-op table, exactly what rollback
                // machinery carries in its snapshots.
                let after_op = expect_outcome(
                    Introspector::new(dialect)
                        .table_info(cx, conn, &table)
                        .await,
                    &format!("{d}: involution mid introspection {i}"),
                );
                let undo = SchemaOperation::DropColumn {
                    table,
                    column,
                    table_info: Some(after_op),
                };
                apply_operations(
                    cx,
                    conn,
                    dialect,
                    std::slice::from_ref(&undo),
                    &format!("{d}:inv:undo:{i}"),
                )
                .await;
                let restored = introspect_ours(
                    cx,
                    conn,
                    dialect,
                    &["e2e_oracle_comm"],
                    &format!("{d}:inv:restored:{i}"),
                )
                .await;
                let back = schema_diff(&restored, &mid);
                assert!(
                    back.operations.is_empty(),
                    "{d}: involution violated at op {i} ({op:?}): applying the inverse left {:#?}\nwarnings: {:?}\nrestored: {restored:#?}\nmid: {mid:#?}",
                    back.operations,
                    back.warnings
                );
            }
            Some(inverse) => {
                apply_operations(
                    cx,
                    conn,
                    dialect,
                    std::slice::from_ref(&inverse),
                    &format!("{d}:inv:undo:{i}"),
                )
                .await;
                let restored = introspect_ours(
                    cx,
                    conn,
                    dialect,
                    &["e2e_oracle_comm"],
                    &format!("{d}:inv:restored:{i}"),
                )
                .await;
                let back = schema_diff(&restored, &mid);
                assert!(
                    back.operations.is_empty(),
                    "{d}: involution violated at op {i} ({op:?}): applying the inverse left {:#?}\nwarnings: {:?}\nrestored: {restored:#?}\nmid: {mid:#?}",
                    back.operations,
                    back.warnings
                );
            }
            None => {
                // Destructive operations have no inverse by design; the
                // snapshot-carrying rollback path covers them. Assert the
                // documented set so a new inverse-less op stays visible.
                assert!(
                    op.is_destructive(),
                    "{d}: op {i} lacks an inverse but is not documented destructive: {op:?}"
                );
            }
        }
    }
}

#[test]
fn schema_oracle_corpus_relations_hold_on_every_driver() {
    let cx = Cx::for_testing();
    let ran = run_on_every_driver(&cx, &SchemaOracle);
    assert!(
        ran.contains(&"c-sqlite(memory)") && ran.contains(&"frankensqlite"),
        "{ran:?}"
    );
}

struct SchemaOracle;

impl Scenario for SchemaOracle {
    async fn run<C: Connection>(&self, cx: &Cx, conn: &C, driver: &DriverUnderTest) {
        run_corpus_relations(cx, conn, driver).await;
    }
}

// ---------------------------------------------------------------------------
// Cross-dialect agreement (no database needed)
// ---------------------------------------------------------------------------

#[test]
fn cross_dialect_operations_agree() {
    let dialects = [Dialect::Sqlite, Dialect::Postgres, Dialect::Mysql];
    let mut rng = Lcg::new(0x00C0_FFEE);
    for case in 0..12 {
        let current_table = generate_table(&mut rng, case, "e2e_oracle_xd_cur");
        let expected_table = generate_table(&mut rng, case + 100, "e2e_oracle_xd_exp");
        let mut reference: Option<Vec<String>> = None;
        for dialect in dialects {
            let mut current = DatabaseSchema::new(dialect);
            current
                .tables
                .insert(current_table.name.clone(), current_table.clone());
            let mut expected = DatabaseSchema::new(dialect);
            expected
                .tables
                .insert(expected_table.name.clone(), expected_table.clone());
            let ops: Vec<String> = schema_diff(&current, &expected)
                .operations
                .iter()
                .map(|op| format!("{op:?}"))
                .collect();
            match &reference {
                None => reference = Some(ops),
                Some(reference) => assert_eq!(
                    &ops, reference,
                    "case {case} ({dialect:?}): the operation sequence differs across dialects"
                ),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Generated-schema phase
// ---------------------------------------------------------------------------

/// Deterministic 64-bit LCG — no external dependency, stable across runs.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 11
    }
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
}

/// Builds one generated table. Columns are engine-safe by construction:
/// indexed columns are `VARCHAR(64)`/`INTEGER` (never `TEXT`), the primary
/// key is a single integer column, and defaults are literals.
fn generate_table(rng: &mut Lcg, _case: usize, name: &str) -> TableInfo {
    let mut columns = Vec::new();
    let mut indexes = Vec::new();
    columns.push(ColumnInfo {
        name: "id".to_owned(),
        sql_type: "INTEGER".to_owned(),
        parsed_type: ParsedSqlType::parse("INTEGER"),
        nullable: false,
        default: None,
        primary_key: true,
        auto_increment: false,
        comment: None,
    });
    let extra = 1 + rng.below(4); // 1..=4 data columns
    for c in 0..extra {
        let kind = rng.below(4);
        let nullable = rng.below(2) == 0;
        let (sql_type, indexable) = match kind {
            0 => ("INTEGER", true),
            1 => ("VARCHAR(64)", true),
            2 => ("TEXT", false),
            _ => ("REAL", false),
        };
        let default = match rng.below(3) {
            0 => None,
            1 if kind == 0 => Some("42".to_owned()),
            2 if kind == 3 => Some("0.5".to_owned()),
            _ => None,
        };
        let col_name = format!("{name}_c{c}");
        if indexable && rng.below(3) == 0 {
            indexes.push(IndexInfo {
                name: format!("idx_{col_name}"),
                columns: vec![col_name.clone()],
                unique: false,
                index_type: None,
                primary: false,
            });
        }
        columns.push(ColumnInfo {
            name: col_name,
            sql_type: sql_type.to_owned(),
            parsed_type: ParsedSqlType::parse(sql_type),
            nullable,
            default,
            primary_key: false,
            auto_increment: false,
            comment: None,
        });
    }
    TableInfo {
        name: name.to_owned(),
        columns,
        primary_key: vec!["id".to_owned()],
        foreign_keys: Vec::new(),
        unique_constraints: Vec::new(),
        check_constraints: Vec::new(),
        indexes,
        comment: None,
    }
}

async fn run_generated_batch<C: Connection>(
    cx: &Cx,
    conn: &C,
    dialect: Dialect,
    driver: &str,
    count: usize,
) {
    let mut rng = Lcg::new(0x5EED_C0DE);
    for case in 0..count {
        let table = generate_table(&mut rng, case, "e2e_oracle_gen");
        let mut expected = DatabaseSchema::new(dialect);
        expected.tables.insert(table.name.clone(), table.clone());

        // Law 1 on the generated schema: create via the differ's own
        // CreateTable DDL, introspect, and require the diff to be empty.
        let empty = DatabaseSchema::new(dialect);
        let create = schema_diff(&empty, &expected);
        assert_eq!(
            create.operations.len(),
            1,
            "{driver} case {case}: one CreateTable expected, got {:#?}",
            create.operations
        );
        apply_operations(
            cx,
            conn,
            dialect,
            &create.operations,
            &format!("{driver}:gen:{case}:create"),
        )
        .await;
        let current = introspect_ours(
            cx,
            conn,
            dialect,
            &[&table.name],
            &format!("{driver}:gen:{case}:fix"),
        )
        .await;
        assert_fixpoint(
            &format!("{driver}: generated {case}"),
            dialect,
            &current,
            &expected,
        );

        // Commutation on every fifth case: turn the table into a widened
        // variant (extra column + index) and require the B fixpoint.
        if case % 5 == 0 {
            let mut widened = table.clone();
            widened.columns.push(sqlmodel_schema::ColumnInfo {
                name: format!("{}_extra", table.name),
                sql_type: "VARCHAR(64)".to_owned(),
                parsed_type: sqlmodel_schema::ParsedSqlType::parse("VARCHAR(64)"),
                nullable: true,
                default: None,
                primary_key: false,
                auto_increment: false,
                comment: None,
            });
            widened.indexes.push(sqlmodel_schema::IndexInfo {
                name: format!("idx_{}_extra", table.name),
                columns: vec![format!("{}_extra", table.name)],
                unique: false,
                index_type: None,
                primary: false,
            });
            let mut widened_schema = DatabaseSchema::new(dialect);
            widened_schema
                .tables
                .insert(widened.name.clone(), widened.clone());
            let ops = schema_diff(&current, &widened_schema);
            apply_operations(
                cx,
                conn,
                dialect,
                &ops.operations,
                &format!("{driver}:gen:{case}:commute"),
            )
            .await;
            let after = introspect_ours(
                cx,
                conn,
                dialect,
                &[&table.name],
                &format!("{driver}:gen:{case}:commuted"),
            )
            .await;
            assert_fixpoint(
                &format!("{driver}: generated commutation {case}"),
                dialect,
                &after,
                &widened_schema,
            );
        }

        // Clean up so the next case (and shared network databases) start from
        // an empty namespace.
        expect_outcome(
            conn.execute(cx, &drop_stale_sql(dialect, &table.name), &[])
                .await,
            &format!("{driver}:gen:{case}: drop"),
        );
    }
}

#[test]
fn generated_schemas_hold_the_laws_on_sqlite() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("asupersync runtime");
    let cx = Cx::for_testing();
    rt.block_on(async {
        let started = std::time::Instant::now();
        // 200 cases on each SQLite variant, per the success criterion.
        let memory = SqliteConnection::open_memory().expect("open :memory:");
        run_generated_batch(&cx, &memory, Dialect::Sqlite, "c-sqlite(memory)", 200).await;
        let path = temp_db_path("oracle-gen");
        let file =
            SqliteConnection::open_file(path.to_string_lossy().into_owned()).expect("open file");
        run_generated_batch(&cx, &file, Dialect::Sqlite, "c-sqlite(file)", 200).await;
        let franken = FrankenConnection::open_memory().expect("open franken :memory:");
        run_generated_batch(&cx, &franken, Dialect::Sqlite, "frankensqlite", 200).await;
        let elapsed = started.elapsed();
        eprintln!("generated-schema oracle: 3 x 200 cases in {elapsed:?} (target < 60s)");
        assert!(
            elapsed < std::time::Duration::from_secs(60),
            "generated phase must finish in under 60s, took {elapsed:?}"
        );
    });
}
