use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};
use serde::{Deserialize, Serialize};

use sqlmodel::SchemaBuilder;
use sqlmodel::prelude::*;
use sqlmodel_sqlite::SqliteConnection;

fn unwrap_outcome<T>(outcome: Outcome<T, Error>) -> T {
    match outcome {
        Outcome::Ok(v) => v,
        Outcome::Err(e) => panic!("unexpected error: {e}"),
        Outcome::Cancelled(r) => panic!("cancelled: {r:?}"),
        Outcome::Panicked(p) => panic!("panicked: {p:?}"),
    }
}

// Joined table inheritance base model
#[derive(sqlmodel::Model, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[sqlmodel(table, inheritance = "joined")]
struct Person {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
}

// Joined table inheritance child model
#[derive(sqlmodel::Model, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[sqlmodel(table, inherits = "Person")]
struct Student {
    #[sqlmodel(parent)]
    person: Person,

    // Joined child table PK/FK to the parent table.
    #[sqlmodel(primary_key)]
    id: i64,

    grade: String,
}

#[test]
fn sqlite_joined_inheritance_select_hydrates_parent_and_polymorphic_base() {
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

        // Insert one joined child and one base-only row.
        let insert_person = format!(
            "INSERT INTO {} (id, name) VALUES (?1, ?2)",
            <Person as Model>::TABLE_NAME
        );
        unwrap_outcome(
            conn.execute(
                &cx,
                &insert_person,
                &[Value::BigInt(1), Value::Text("Alice".to_string())],
            )
            .await,
        );
        unwrap_outcome(
            conn.execute(
                &cx,
                &insert_person,
                &[Value::BigInt(2), Value::Text("Bob".to_string())],
            )
            .await,
        );

        let insert_student = format!(
            "INSERT INTO {} (id, grade) VALUES (?1, ?2)",
            <Student as Model>::TABLE_NAME
        );
        unwrap_outcome(
            conn.execute(
                &cx,
                &insert_student,
                &[Value::BigInt(1), Value::Text("A".to_string())],
            )
            .await,
        );

        // 1) Child query: must JOIN + hydrate embedded parent.
        let students = unwrap_outcome(sqlmodel::select!(Student).all(&cx, &conn).await);
        assert_eq!(students.len(), 1);
        assert_eq!(
            students[0],
            Student {
                person: Person {
                    id: 1,
                    name: "Alice".to_string(),
                },
                id: 1,
                grade: "A".to_string(),
            }
        );

        // 2) Base polymorphic query: base row stays base, joined row becomes child.
        let rows = unwrap_outcome(
            sqlmodel::select!(Person)
                .polymorphic_joined::<Student>()
                .order_by(OrderBy::asc(Expr::qualified(
                    <Person as Model>::TABLE_NAME,
                    "id",
                )))
                .all(&cx, &conn)
                .await,
        );

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            PolymorphicJoined::Child(Student {
                person: Person {
                    id: 1,
                    name: "Alice".to_string(),
                },
                id: 1,
                grade: "A".to_string(),
            })
        );
        assert_eq!(
            rows[1],
            PolymorphicJoined::Base(Person {
                id: 2,
                name: "Bob".to_string(),
            })
        );
    });
}
