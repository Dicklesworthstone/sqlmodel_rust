//! Joined-table inheritance polymorphic query with four child types
//! (`bd-kzp1.3`): the macro-generated `polymorphic_joined4` hydrates a
//! base-only row as `Base`, one row per child type as the right variant,
//! and orders across the join.
//!
//! Sibling scenarios cover arities 1-3; this proves the generated family
//! extends past the originally hand-written arities.

#![cfg(feature = "c-sqlite-tests")]

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
#[sqlmodel(table = "staff", inherits = "Person")]
struct StaffMember {
    #[sqlmodel(parent)]
    person: Person,
    #[sqlmodel(primary_key)]
    id: i64,
    office: String,
}

#[derive(sqlmodel::Model, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[sqlmodel(table = "alumni", inherits = "Person")]
struct Alumnus {
    #[sqlmodel(parent)]
    person: Person,
    #[sqlmodel(primary_key)]
    id: i64,
    graduation_year: i64,
}

#[test]
fn sqlite_joined_inheritance_polymorphic_joined4_hydrates_correct_variants() {
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
            .create_table::<StaffMember>()
            .create_table::<Alumnus>()
            .build();
        for stmt in stmts {
            unwrap_outcome(conn.execute(&cx, &stmt, &[]).await);
        }

        // Person 1 has an alumni row; 2-4 are one child kind each.
        for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Carol"), (4, "Dora")] {
            let sql = format!(
                "INSERT INTO {} (id, name) VALUES (?1, ?2)",
                sqlmodel_core::quote_ident(<Person as Model>::TABLE_NAME)
            );
            unwrap_outcome(
                conn.execute(
                    &cx,
                    &sql,
                    &[Value::BigInt(id), Value::Text(name.to_string())],
                )
                .await,
            );
        }
        for (table, column, id, value) in [
            ("students", "grade", 2, "A"),
            ("teachers", "subject", 3, "math"),
            ("staff", "office", 4, "bldg-2"),
        ] {
            let sql = format!(
                "INSERT INTO {} (id, {column}) VALUES (?1, ?2)",
                sqlmodel_core::quote_ident(table)
            );
            unwrap_outcome(
                conn.execute(
                    &cx,
                    &sql,
                    &[Value::BigInt(id), Value::Text(value.to_string())],
                )
                .await,
            );
        }
        let sql = format!(
            "INSERT INTO {} (id, graduation_year) VALUES (?1, ?2)",
            sqlmodel_core::quote_ident("alumni")
        );
        unwrap_outcome(
            conn.execute(&cx, &sql, &[Value::BigInt(1), Value::BigInt(2019)])
                .await,
        );

        let rows = unwrap_outcome(
            sqlmodel::select!(Person)
                .polymorphic_joined4::<Student, Teacher, StaffMember, Alumnus>()
                .order_by(sqlmodel::Expr::qualified("people", "id").asc())
                .all(&cx, &conn)
                .await,
        );

        assert_eq!(rows.len(), 4, "one row per person: {rows:?}");

        // Person 1 is base-only in the people/students/teachers/staff joins
        // but has an alumni row: the alumni prefix wins and hydrates as C4.
        let PolymorphicJoined4::C4(a) = &rows[0] else {
            panic!("row 0 should be Alumnus, got {:?}", rows[0]);
        };
        assert_eq!(a.person.name, "Alice");
        assert_eq!(a.graduation_year, 2019);

        let PolymorphicJoined4::C1(s) = &rows[1] else {
            panic!("row 1 should be Student, got {:?}", rows[1]);
        };
        assert_eq!(s.person.name, "Bob");
        assert_eq!(s.grade, "A");

        let PolymorphicJoined4::C2(t) = &rows[2] else {
            panic!("row 2 should be Teacher, got {:?}", rows[2]);
        };
        assert_eq!(t.person.name, "Carol");
        assert_eq!(t.subject, "math");

        let PolymorphicJoined4::C3(staff) = &rows[3] else {
            panic!("row 3 should be StaffMember, got {:?}", rows[3]);
        };
        assert_eq!(staff.person.name, "Dora");
        assert_eq!(staff.office, "bldg-2");
    });
}
