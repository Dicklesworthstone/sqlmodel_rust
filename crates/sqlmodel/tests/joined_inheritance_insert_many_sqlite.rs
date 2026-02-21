#![cfg(feature = "c-sqlite-tests")]

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};
use serde::{Deserialize, Serialize};

use sqlmodel::SchemaBuilder;
use sqlmodel::prelude::*;
use sqlmodel_query::insert_many;
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
    #[sqlmodel(primary_key, auto_increment)]
    id: Option<i64>,
    name: String,
}

#[derive(sqlmodel::Model, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[sqlmodel(table, inherits = "Person")]
struct Student {
    #[sqlmodel(parent)]
    person: Person,

    #[sqlmodel(primary_key)]
    id: Option<i64>,
    grade: String,
}

#[test]
fn sqlite_joined_inheritance_insert_many_inserts_base_and_child_rows() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = SqliteConnection::open_memory().expect("open sqlite memory db");

        let stmts = SchemaBuilder::new()
            .create_table::<Person>()
            .create_table::<Student>()
            .build();
        for stmt in stmts {
            unwrap_outcome(conn.execute(&cx, &stmt, &[]).await);
        }

        let empty: Vec<Student> = Vec::new();
        let empty_count = unwrap_outcome(insert_many!(&empty).execute(&cx, &conn).await);
        assert_eq!(empty_count, 0);

        // Mixed defaults: first + third rows use auto-generated PK, second uses explicit PK.
        let students = vec![
            Student {
                person: Person {
                    id: None,
                    name: "Alice".to_string(),
                },
                id: None,
                grade: "A".to_string(),
            },
            Student {
                person: Person {
                    id: Some(50),
                    name: "Bob".to_string(),
                },
                id: Some(50),
                grade: "B".to_string(),
            },
            Student {
                person: Person {
                    id: None,
                    name: "Cara".to_string(),
                },
                id: None,
                grade: "C".to_string(),
            },
        ];

        let inserted = unwrap_outcome(insert_many!(&students).execute(&cx, &conn).await);
        assert_eq!(inserted, 3);

        let person_table = sqlmodel_core::quote_ident(<Person as Model>::TABLE_NAME);
        let student_table = sqlmodel_core::quote_ident(<Student as Model>::TABLE_NAME);

        let people = unwrap_outcome(
            conn.query(
                &cx,
                &format!("SELECT id, name FROM {person_table} ORDER BY id"),
                &[],
            )
            .await,
        );
        let student_rows = unwrap_outcome(
            conn.query(
                &cx,
                &format!("SELECT id, grade FROM {student_table} ORDER BY id"),
                &[],
            )
            .await,
        );

        assert_eq!(people.len(), 3);
        assert_eq!(student_rows.len(), 3);

        let people_ids: Vec<i64> = people
            .iter()
            .map(|r| r.get_as::<i64>(0).expect("person id"))
            .collect();
        let student_ids: Vec<i64> = student_rows
            .iter()
            .map(|r| r.get_as::<i64>(0).expect("student id"))
            .collect();

        assert_eq!(people_ids, student_ids);
        assert!(people_ids.contains(&50));

        let joined = unwrap_outcome(
            conn.query(
                &cx,
                &format!(
                    "SELECT p.name, s.grade \
                     FROM {person_table} p \
                     JOIN {student_table} s ON s.id = p.id \
                     ORDER BY p.id"
                ),
                &[],
            )
            .await,
        );
        assert_eq!(joined.len(), 3);
        assert!(joined.iter().any(|r| {
            r.get_as::<String>(0).expect("name") == "Alice"
                && r.get_as::<String>(1).expect("grade") == "A"
        }));
        assert!(joined.iter().any(|r| {
            r.get_as::<String>(0).expect("name") == "Bob"
                && r.get_as::<String>(1).expect("grade") == "B"
        }));
        assert!(joined.iter().any(|r| {
            r.get_as::<String>(0).expect("name") == "Cara"
                && r.get_as::<String>(1).expect("grade") == "C"
        }));
    });
}
