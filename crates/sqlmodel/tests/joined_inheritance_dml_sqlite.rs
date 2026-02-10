use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};
use serde::{Deserialize, Serialize};

use sqlmodel::SchemaBuilder;
use sqlmodel::prelude::*;
use sqlmodel_query::{DeleteBuilder, UpdateBuilder};
use sqlmodel_sqlite::SqliteConnection;

fn unwrap_outcome<T>(outcome: Outcome<T, Error>) -> T {
    match outcome {
        Outcome::Ok(v) => v,
        Outcome::Err(e) => panic!("unexpected error: {e}"),
        Outcome::Cancelled(r) => panic!("cancelled: {r:?}"),
        Outcome::Panicked(p) => panic!("panicked: {p:?}"),
    }
}

// Joined table inheritance base model (auto-increment PK).
#[derive(sqlmodel::Model, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[sqlmodel(table, inheritance = "joined")]
struct Person {
    #[sqlmodel(primary_key, auto_increment)]
    id: Option<i64>,
    name: String,
}

// Joined table inheritance child model.
#[derive(sqlmodel::Model, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[sqlmodel(table, inherits = "Person")]
struct Student {
    #[sqlmodel(parent)]
    person: Person,

    // Child PK/FK to parent PK.
    #[sqlmodel(primary_key)]
    id: Option<i64>,

    grade: String,
}

#[test]
fn sqlite_joined_inheritance_dml_inserts_updates_deletes_base_and_child() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = SqliteConnection::open_memory().expect("open sqlite memory db");

        // DDL
        let stmts = SchemaBuilder::new()
            .create_table::<Person>()
            .create_table::<Student>()
            .build();
        for stmt in stmts {
            unwrap_outcome(conn.execute(&cx, &stmt, &[]).await);
        }

        // INSERT joined child: must insert base then child in one transaction, propagating id.
        let student0 = Student {
            person: Person {
                id: None,
                name: "Alice".to_string(),
            },
            id: None,
            grade: "A".to_string(),
        };

        let id = unwrap_outcome(insert!(&student0).execute(&cx, &conn).await);
        assert!(id > 0);

        // Verify both tables have the row.
        let person_table = sqlmodel_core::quote_ident(<Person as Model>::TABLE_NAME);
        let student_table = sqlmodel_core::quote_ident(<Student as Model>::TABLE_NAME);

        let people = unwrap_outcome(
            conn.query(
                &cx,
                &format!("SELECT id, name FROM {person_table} WHERE id = ?1"),
                &[Value::BigInt(id)],
            )
            .await,
        );
        assert_eq!(people.len(), 1);
        assert_eq!(people[0].get_as::<i64>(0).unwrap(), id);
        assert_eq!(people[0].get_named::<String>("name").unwrap(), "Alice");

        let student_rows = unwrap_outcome(
            conn.query(
                &cx,
                &format!("SELECT id, grade FROM {student_table} WHERE id = ?1"),
                &[Value::BigInt(id)],
            )
            .await,
        );
        assert_eq!(student_rows.len(), 1);
        assert_eq!(student_rows[0].get_as::<i64>(0).unwrap(), id);
        assert_eq!(student_rows[0].get_named::<String>("grade").unwrap(), "A");

        // UPDATE joined child: must update both base and child rows.
        let student1 = Student {
            person: Person {
                id: Some(id),
                name: "Alice2".to_string(),
            },
            id: Some(id),
            grade: "B".to_string(),
        };

        let updated = unwrap_outcome(update!(&student1).execute(&cx, &conn).await);
        // One row updated in each table (sum semantics).
        assert_eq!(updated, 2);

        let people2 = unwrap_outcome(
            conn.query(
                &cx,
                &format!("SELECT name FROM {person_table} WHERE id = ?1"),
                &[Value::BigInt(id)],
            )
            .await,
        );
        assert_eq!(people2[0].get_as::<String>(0).unwrap(), "Alice2");

        let students2 = unwrap_outcome(
            conn.query(
                &cx,
                &format!("SELECT grade FROM {student_table} WHERE id = ?1"),
                &[Value::BigInt(id)],
            )
            .await,
        );
        assert_eq!(students2[0].get_as::<String>(0).unwrap(), "B");

        // UPDATE joined child (explicit WHERE/SET): base+child table targeting with qualification.
        let updated_explicit = unwrap_outcome(
            UpdateBuilder::<Student>::empty()
                .set(&format!("{}.name", <Person as Model>::TABLE_NAME), "Alice3")
                .set("grade", "A+")
                .filter(Expr::qualified(<Student as Model>::TABLE_NAME, "id").eq(id))
                .execute(&cx, &conn)
                .await,
        );
        assert_eq!(updated_explicit, 2);

        let explicit_rows = unwrap_outcome(
            UpdateBuilder::<Student>::empty()
                .set("grade", "A++")
                .filter(Expr::qualified(<Student as Model>::TABLE_NAME, "id").eq(id))
                .returning()
                .execute_returning(&cx, &conn)
                .await,
        );
        assert_eq!(explicit_rows.len(), 1);
        assert_eq!(
            explicit_rows[0]
                .get_named::<String>(&format!("{}__name", <Person as Model>::TABLE_NAME))
                .unwrap(),
            "Alice3"
        );
        assert_eq!(
            explicit_rows[0]
                .get_named::<String>(&format!("{}__grade", <Student as Model>::TABLE_NAME))
                .unwrap(),
            "A++"
        );

        // Joined insert ON CONFLICT: explicit PK upsert updates both base and child tables.
        let upsert_model = Student {
            person: Person {
                id: Some(id),
                name: "Alice4".to_string(),
            },
            id: Some(id),
            grade: "A*".to_string(),
        };
        let upsert_id = unwrap_outcome(
            insert!(&upsert_model)
                .on_conflict_do_update(&["name", "grade"])
                .execute(&cx, &conn)
                .await,
        );
        assert_eq!(upsert_id, id);

        let people_after_upsert = unwrap_outcome(
            conn.query(
                &cx,
                &format!("SELECT name FROM {person_table} WHERE id = ?1"),
                &[Value::BigInt(id)],
            )
            .await,
        );
        assert_eq!(
            people_after_upsert[0].get_as::<String>(0).unwrap(),
            "Alice4"
        );
        let students_after_upsert = unwrap_outcome(
            conn.query(
                &cx,
                &format!("SELECT grade FROM {student_table} WHERE id = ?1"),
                &[Value::BigInt(id)],
            )
            .await,
        );
        assert_eq!(students_after_upsert[0].get_as::<String>(0).unwrap(), "A*");

        // Ambiguous unqualified joined column in explicit SET should fail with a clear error.
        let ambiguous_update = UpdateBuilder::<Student>::empty()
            .set("id", 123_i64)
            .filter(Expr::qualified(<Student as Model>::TABLE_NAME, "id").eq(id))
            .execute(&cx, &conn)
            .await;
        match ambiguous_update {
            Outcome::Err(e) => assert!(
                e.to_string()
                    .contains("ambiguous joined-table inheritance column 'id'"),
                "unexpected error: {e}"
            ),
            other => panic!("expected ambiguity error, got {other:?}"),
        }

        // ON CONFLICT with insert_returning is explicitly unsupported for joined inheritance.
        let conflict_returning = insert!(&upsert_model)
            .on_conflict_do_nothing()
            .execute_returning(&cx, &conn)
            .await;
        match conflict_returning {
            Outcome::Err(e) => assert!(
                e.to_string()
                    .contains("insert_returning does not support ON CONFLICT"),
                "unexpected error: {e}"
            ),
            other => panic!("expected insert_returning ON CONFLICT error, got {other:?}"),
        }

        // Insert two more rows for explicit DELETE semantics checks.
        let student_c = Student {
            person: Person {
                id: None,
                name: "Bob".to_string(),
            },
            id: None,
            grade: "C".to_string(),
        };
        let id2 = unwrap_outcome(insert!(&student_c).execute(&cx, &conn).await);
        let student_d = Student {
            person: Person {
                id: None,
                name: "Dana".to_string(),
            },
            id: None,
            grade: "D".to_string(),
        };
        let id3 = unwrap_outcome(insert!(&student_d).execute(&cx, &conn).await);

        // DELETE joined child (explicit WHERE): filter by child table and delete both child+parent.
        let deleted_explicit = unwrap_outcome(
            DeleteBuilder::<Student>::new()
                .filter(Expr::qualified(<Student as Model>::TABLE_NAME, "grade").eq("C"))
                .execute(&cx, &conn)
                .await,
        );
        assert_eq!(deleted_explicit, 2);
        let people_deleted_explicit = unwrap_outcome(
            conn.query(
                &cx,
                &format!("SELECT id FROM {person_table} WHERE id = ?1"),
                &[Value::BigInt(id2)],
            )
            .await,
        );
        assert_eq!(people_deleted_explicit.len(), 0);
        let students_deleted_explicit = unwrap_outcome(
            conn.query(
                &cx,
                &format!("SELECT id FROM {student_table} WHERE id = ?1"),
                &[Value::BigInt(id2)],
            )
            .await,
        );
        assert_eq!(students_deleted_explicit.len(), 0);

        // DELETE joined child returning uses base+child prefixed row shape.
        let deleted_rows = unwrap_outcome(
            DeleteBuilder::<Student>::new()
                .filter(Expr::qualified(<Student as Model>::TABLE_NAME, "grade").eq("D"))
                .returning()
                .execute_returning(&cx, &conn)
                .await,
        );
        assert_eq!(deleted_rows.len(), 1);
        assert_eq!(
            deleted_rows[0]
                .get_named::<String>(&format!("{}__name", <Person as Model>::TABLE_NAME))
                .unwrap(),
            "Dana"
        );
        assert_eq!(
            deleted_rows[0]
                .get_named::<String>(&format!("{}__grade", <Student as Model>::TABLE_NAME))
                .unwrap(),
            "D"
        );
        let people_after_returning_delete = unwrap_outcome(
            conn.query(
                &cx,
                &format!("SELECT id FROM {person_table} WHERE id = ?1"),
                &[Value::BigInt(id3)],
            )
            .await,
        );
        assert_eq!(people_after_returning_delete.len(), 0);
        let students_after_returning_delete = unwrap_outcome(
            conn.query(
                &cx,
                &format!("SELECT id FROM {student_table} WHERE id = ?1"),
                &[Value::BigInt(id3)],
            )
            .await,
        );
        assert_eq!(students_after_returning_delete.len(), 0);

        // DELETE joined child: must delete child then base.
        let deleted = unwrap_outcome(
            DeleteBuilder::from_model(&student1)
                .execute(&cx, &conn)
                .await,
        );
        // One row deleted in each table (sum semantics).
        assert_eq!(deleted, 2);

        let people3 = unwrap_outcome(
            conn.query(
                &cx,
                &format!("SELECT id FROM {person_table} WHERE id = ?1"),
                &[Value::BigInt(id)],
            )
            .await,
        );
        assert_eq!(people3.len(), 0);

        let deleted_student_rows = unwrap_outcome(
            conn.query(
                &cx,
                &format!("SELECT id FROM {student_table} WHERE id = ?1"),
                &[Value::BigInt(id)],
            )
            .await,
        );
        assert_eq!(deleted_student_rows.len(), 0);
    });
}
