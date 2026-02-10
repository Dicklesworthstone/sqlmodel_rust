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

#[derive(sqlmodel::Model, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[sqlmodel(table, inheritance = "joined")]
struct Person {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
}

#[derive(sqlmodel::Model, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[sqlmodel(table, inherits = "Person")]
struct Student {
    #[sqlmodel(parent)]
    person: Person,
    #[sqlmodel(primary_key)]
    id: i64,
    grade: String,
}

#[derive(sqlmodel::Model, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[sqlmodel(table, inherits = "Person")]
struct Teacher {
    #[sqlmodel(parent)]
    person: Person,
    #[sqlmodel(primary_key)]
    id: i64,
    subject: String,
}

#[derive(sqlmodel::Model, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[sqlmodel(table, inherits = "Person")]
struct Counselor {
    #[sqlmodel(parent)]
    person: Person,
    #[sqlmodel(primary_key)]
    id: i64,
    office: String,
}

#[test]
fn sqlite_joined_inheritance_polymorphic_joined3_hydrates_correct_variants() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = SqliteConnection::open_memory().expect("open sqlite memory db");

        let stmts = SchemaBuilder::new()
            .create_table::<Person>()
            .create_table::<Student>()
            .create_table::<Teacher>()
            .create_table::<Counselor>()
            .build();
        for stmt in stmts {
            unwrap_outcome(conn.execute(&cx, &stmt, &[]).await);
        }

        let person_table = sqlmodel_core::quote_ident(<Person as Model>::TABLE_NAME);
        let student_table = sqlmodel_core::quote_ident(<Student as Model>::TABLE_NAME);
        let teacher_table = sqlmodel_core::quote_ident(<Teacher as Model>::TABLE_NAME);
        let counselor_table = sqlmodel_core::quote_ident(<Counselor as Model>::TABLE_NAME);

        let insert_person = format!("INSERT INTO {person_table} (id, name) VALUES (?1, ?2)");
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
        unwrap_outcome(
            conn.execute(
                &cx,
                &insert_person,
                &[Value::BigInt(3), Value::Text("Carol".to_string())],
            )
            .await,
        );
        unwrap_outcome(
            conn.execute(
                &cx,
                &insert_person,
                &[Value::BigInt(4), Value::Text("Dave".to_string())],
            )
            .await,
        );

        let insert_student = format!("INSERT INTO {student_table} (id, grade) VALUES (?1, ?2)");
        unwrap_outcome(
            conn.execute(
                &cx,
                &insert_student,
                &[Value::BigInt(1), Value::Text("A".to_string())],
            )
            .await,
        );

        let insert_teacher = format!("INSERT INTO {teacher_table} (id, subject) VALUES (?1, ?2)");
        unwrap_outcome(
            conn.execute(
                &cx,
                &insert_teacher,
                &[Value::BigInt(2), Value::Text("math".to_string())],
            )
            .await,
        );

        let insert_counselor =
            format!("INSERT INTO {counselor_table} (id, office) VALUES (?1, ?2)");
        unwrap_outcome(
            conn.execute(
                &cx,
                &insert_counselor,
                &[Value::BigInt(3), Value::Text("west".to_string())],
            )
            .await,
        );

        let rows = unwrap_outcome(
            sqlmodel::select!(Person)
                .polymorphic_joined3::<Student, Teacher, Counselor>()
                .order_by(OrderBy::asc(Expr::qualified(
                    <Person as Model>::TABLE_NAME,
                    "id",
                )))
                .all(&cx, &conn)
                .await,
        );

        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows[0],
            PolymorphicJoined3::C1(Student {
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
            PolymorphicJoined3::C2(Teacher {
                person: Person {
                    id: 2,
                    name: "Bob".to_string(),
                },
                id: 2,
                subject: "math".to_string(),
            })
        );
        assert_eq!(
            rows[2],
            PolymorphicJoined3::C3(Counselor {
                person: Person {
                    id: 3,
                    name: "Carol".to_string(),
                },
                id: 3,
                office: "west".to_string(),
            })
        );
        assert_eq!(
            rows[3],
            PolymorphicJoined3::Base(Person {
                id: 4,
                name: "Dave".to_string(),
            })
        );
    });
}
