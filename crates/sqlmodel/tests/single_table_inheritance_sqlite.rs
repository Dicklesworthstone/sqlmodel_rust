#![cfg(feature = "c-sqlite-tests")]

//! Single-table inheritance (STI) end-to-end against a real SQLite database.
//!
//! Before this test, STI was verified only through mocked rows (facade unit
//! tests). Here the whole path runs: schema generation for the shared physical
//! table, discriminator auto-population on insert, implicit discriminator
//! filtering on SELECT / UPDATE / DELETE for child models, and base-model
//! reads that see every row.

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};
use serde::{Deserialize, Serialize};

use sqlmodel::SchemaBuilder;
use sqlmodel::prelude::*;
use sqlmodel_query::DeleteBuilder;
use sqlmodel_sqlite::SqliteConnection;

fn unwrap_outcome<T>(outcome: Outcome<T, Error>) -> T {
    match outcome {
        Outcome::Ok(v) => v,
        Outcome::Err(e) => panic!("unexpected error: {e}"),
        Outcome::Cancelled(r) => panic!("cancelled: {r:?}"),
        Outcome::Panicked(p) => panic!("panicked: {p:?}"),
    }
}

/// Base model: one physical table `employees`, discriminated by `kind`.
#[derive(sqlmodel::Model, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[sqlmodel(table = "employees", inheritance = "single", discriminator = "kind")]
struct Employee {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
    kind: String,
}

/// Child stored in `employees` with `kind = 'manager'`; no `kind` field of its own.
#[derive(sqlmodel::Model, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[sqlmodel(inherits = "Employee", discriminator_value = "manager")]
struct Manager {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
    #[sqlmodel(nullable)]
    department: Option<String>,
}

/// Second child stored in the same table with `kind = 'engineer'`.
#[derive(sqlmodel::Model, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[sqlmodel(inherits = "Employee", discriminator_value = "engineer")]
struct Engineer {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
    #[sqlmodel(nullable)]
    specialty: Option<String>,
}

async fn count_where(cx: &Cx, conn: &SqliteConnection, predicate: &str) -> i64 {
    let rows = unwrap_outcome(
        conn.query(
            cx,
            &format!("SELECT COUNT(*) FROM employees WHERE {predicate}"),
            &[],
        )
        .await,
    );
    rows[0].get_as::<i64>(0).unwrap()
}

#[test]
fn sti_models_share_one_table_and_children_are_discriminated() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = SqliteConnection::open_memory().expect("open sqlite memory db");

        // Metadata: children share the parent's physical table and inherit the discriminator.
        assert_eq!(<Manager as Model>::TABLE_NAME, "employees");
        assert_eq!(<Engineer as Model>::TABLE_NAME, "employees");
        assert_eq!(Manager::inheritance().discriminator_column, Some("kind"));
        assert_eq!(Manager::inheritance().discriminator_value, Some("manager"));
        assert_eq!(
            Engineer::inheritance().discriminator_value,
            Some("engineer")
        );

        // DDL: exactly one CREATE TABLE plus ALTER TABLE ADD COLUMN for each child-only column.
        let stmts = SchemaBuilder::new()
            .create_table::<Employee>()
            .create_table::<Manager>()
            .create_table::<Engineer>()
            .build();
        let creates = stmts
            .iter()
            .filter(|s| s.starts_with("CREATE TABLE"))
            .count();
        assert_eq!(
            creates, 1,
            "STI children must not create their own table: {stmts:?}"
        );
        assert!(
            stmts
                .iter()
                .any(|s| s.contains("ADD COLUMN") && s.contains("\"department\"")),
            "child-only column department must be added to employees: {stmts:?}"
        );
        assert!(
            stmts
                .iter()
                .any(|s| s.contains("ADD COLUMN") && s.contains("\"specialty\"")),
            "child-only column specialty must be added to employees: {stmts:?}"
        );
        for stmt in &stmts {
            unwrap_outcome(conn.execute(&cx, stmt, &[]).await);
        }

        // INSERT through each model. Children carry no `kind` field; to_row() must supply it.
        unwrap_outcome(
            insert!(&Employee {
                id: 1,
                name: "Plain".into(),
                kind: "employee".into(),
            })
            .execute(&cx, &conn)
            .await,
        );
        unwrap_outcome(
            insert!(&Manager {
                id: 2,
                name: "Mia".into(),
                department: Some("Platform".into()),
            })
            .execute(&cx, &conn)
            .await,
        );
        unwrap_outcome(
            insert!(&Engineer {
                id: 3,
                name: "Eli".into(),
                specialty: Some("Storage".into()),
            })
            .execute(&cx, &conn)
            .await,
        );
        unwrap_outcome(
            insert!(&Manager {
                id: 4,
                name: "Max".into(),
                department: None,
            })
            .execute(&cx, &conn)
            .await,
        );

        // Discriminator auto-population, observed with raw SQL (independent of the ORM).
        assert_eq!(count_where(&cx, &conn, "kind = 'manager'").await, 2);
        assert_eq!(count_where(&cx, &conn, "kind = 'engineer'").await, 1);
        assert_eq!(count_where(&cx, &conn, "kind = 'employee'").await, 1);
        assert_eq!(count_where(&cx, &conn, "1 = 1").await, 4);

        // SELECT on a child model is implicitly filtered by its discriminator.
        let managers: Vec<Manager> = unwrap_outcome(
            select!(Manager)
                .order_by(Expr::col("id").asc())
                .all(&cx, &conn)
                .await,
        );
        assert_eq!(
            managers.iter().map(|m| m.id).collect::<Vec<_>>(),
            vec![2, 4],
            "select!(Manager) must return only manager rows"
        );
        assert_eq!(managers[0].department.as_deref(), Some("Platform"));
        assert_eq!(managers[1].department, None);

        let engineers: Vec<Engineer> = unwrap_outcome(select!(Engineer).all(&cx, &conn).await);
        assert_eq!(engineers.len(), 1);
        assert_eq!(engineers[0].specialty.as_deref(), Some("Storage"));

        // Combining the implicit filter with an explicit one still narrows correctly.
        let named: Vec<Manager> = unwrap_outcome(
            select!(Manager)
                .filter(Expr::col("name").eq("Max"))
                .all(&cx, &conn)
                .await,
        );
        assert_eq!(named.len(), 1);
        assert_eq!(named[0].id, 4);

        // SELECT on the base model sees every row, with the discriminator column populated.
        let everyone: Vec<Employee> = unwrap_outcome(
            select!(Employee)
                .order_by(Expr::col("id").asc())
                .all(&cx, &conn)
                .await,
        );
        assert_eq!(everyone.len(), 4);
        assert_eq!(
            everyone.iter().map(|e| e.kind.as_str()).collect::<Vec<_>>(),
            vec!["employee", "manager", "engineer", "manager"]
        );

        // UPDATE through a child model keeps the discriminator and touches only that row.
        let updated = unwrap_outcome(
            update!(&Manager {
                id: 2,
                name: "Mia Renamed".into(),
                department: Some("Platform".into()),
            })
            .execute(&cx, &conn)
            .await,
        );
        assert_eq!(updated, 1);
        assert_eq!(
            count_where(
                &cx,
                &conn,
                "id = 2 AND kind = 'manager' AND name = 'Mia Renamed'"
            )
            .await,
            1
        );

        // DELETE through a child model with a filter that would match rows of other kinds
        // must only delete rows of that child kind.
        let deleted = unwrap_outcome(
            DeleteBuilder::<Manager>::new()
                .filter(Expr::col("id").ge(1))
                .execute(&cx, &conn)
                .await,
        );
        assert_eq!(
            deleted, 2,
            "delete!(Manager) with a broad filter must remove only manager rows"
        );
        assert_eq!(count_where(&cx, &conn, "kind = 'manager'").await, 0);
        assert_eq!(
            count_where(&cx, &conn, "1 = 1").await,
            2,
            "the plain employee and the engineer must survive a Manager delete"
        );

        // Base-model delete is unfiltered.
        let deleted_all = unwrap_outcome(
            DeleteBuilder::<Employee>::new()
                .filter(Expr::col("id").ge(1))
                .execute(&cx, &conn)
                .await,
        );
        assert_eq!(deleted_all, 2);
        assert_eq!(count_where(&cx, &conn, "1 = 1").await, 0);
    });
}
