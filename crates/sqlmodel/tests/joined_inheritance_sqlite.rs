#![cfg(feature = "c-sqlite-tests")]

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tracing::field::Field;
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

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

#[derive(Default, Clone)]
struct TestCapture(Arc<Mutex<Vec<(tracing::Level, String, String)>>>);

struct TestSubscriber {
    capture: TestCapture,
}

impl Subscriber for TestSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _span: &Id, _values: &Record<'_>) {}
    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let meta = event.metadata();
        struct MessageVisitor(String);
        impl tracing::field::Visit for MessageVisitor {
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        let mut logs = self.capture.0.lock().unwrap();
        logs.push((*meta.level(), meta.target().to_string(), visitor.0));
    }
    fn enter(&self, _span: &Id) {}
    fn exit(&self, _span: &Id) {}
}

#[test]
fn sqlite_joined_inheritance_diagnostics_logging() {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();
    let capture = TestCapture::default();
    let subscriber = TestSubscriber {
        capture: capture.clone(),
    };

    tracing::subscriber::with_default(subscriber, || {
        rt.block_on(async {
            let conn = SqliteConnection::open_memory().expect("open sqlite memory db");

            // DDL creates tables with FK constraint logging
            let stmts = SchemaBuilder::new()
                .create_table::<Person>()
                .create_table::<Student>()
                .build();
            for stmt in stmts {
                unwrap_outcome(conn.execute(&cx, &stmt, &[]).await);
            }

            // Insert child model via insert!(...)
            let student = Student {
                person: Person {
                    id: 10,
                    name: "Charlie".to_string(),
                },
                id: 10,
                grade: "B".to_string(),
            };
            unwrap_outcome(sqlmodel::insert!(&student).execute(&cx, &conn).await);

            // Polymorphic query
            let rows = unwrap_outcome(
                sqlmodel::select!(Person)
                    .polymorphic_joined::<Student>()
                    .all(&cx, &conn)
                    .await,
            );
            assert_eq!(rows.len(), 1);
        });
    });

    let logs = capture.0.lock().unwrap().clone();

    // Check schema inheritance logging
    assert!(
        logs.iter().any(|(level, target, msg)| {
            *level == tracing::Level::DEBUG
                && target == "sqlmodel_schema::inheritance"
                && msg.contains("added joined inheritance foreign key to parent primary key")
        }),
        "expected schema FK logging, captured logs: {logs:?}"
    );

    // Check query inheritance mapping resolution logging
    assert!(
        logs.iter().any(|(level, target, msg)| {
            *level == tracing::Level::DEBUG
                && target == "sqlmodel_query::inheritance"
                && msg.contains("resolved inheritance mapping")
        }),
        "expected inheritance mapping resolution log, captured logs: {logs:?}"
    );

    // Check joined DML statement logging
    assert!(
        logs.iter().any(|(level, target, msg)| {
            *level == tracing::Level::DEBUG
                && target == "sqlmodel_query::inheritance"
                && msg.contains("joined DML statement")
        }),
        "expected joined DML statement log, captured logs: {logs:?}"
    );

    // Check polymorphic query TRACE logging: exactly once for the query statement (no per-row spam)
    let poly_trace_count = logs
        .iter()
        .filter(|(level, target, msg)| {
            *level == tracing::Level::TRACE
                && target == "sqlmodel_query::polymorphic"
                && msg.contains("polymorphic select")
        })
        .count();
    assert_eq!(
        poly_trace_count, 1,
        "expected exactly 1 polymorphic query TRACE event, got {poly_trace_count}"
    );
}
