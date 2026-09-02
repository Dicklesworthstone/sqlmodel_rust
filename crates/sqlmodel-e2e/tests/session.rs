//! `Session` (unit of work) against real databases. Until this file existed the
//! session layer had only ever talked to an in-crate mock connection.
//!
//! Every assertion about database state goes through a raw query on the
//! session's own connection, so the session cannot vouch for itself.

use asupersync::{CancelKind, Cx, Outcome};
use serde::{Deserialize, Serialize};
use sqlmodel::prelude::*;
use sqlmodel::{SchemaBuilder, Session};
use sqlmodel_core::RelatedMany;
use sqlmodel_e2e::{DriverUnderTest, OwnedScenario, expect_outcome, run_owned_on_every_driver};

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_session_authors")]
struct Author {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
    #[sqlmodel(nullable)]
    email: Option<String>,
    #[sqlmodel(relationship(model = "Book", foreign_key = "author_id"))]
    books: RelatedMany<Book>,
}

impl Author {
    fn new(id: i64, name: &str, email: Option<&str>) -> Self {
        Self {
            id,
            name: name.into(),
            email: email.map(str::to_owned),
            books: RelatedMany::new("author_id"),
        }
    }
}

#[derive(sqlmodel::Model, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_session_books")]
struct Book {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(foreign_key = "e2e_session_authors.id", on_delete = "CASCADE")]
    author_id: i64,
    title: String,
    #[sqlmodel(unique)]
    isbn: String,
}

fn book(id: i64, author_id: i64, title: &str, isbn: &str) -> Book {
    Book {
        id,
        author_id,
        title: title.into(),
        isbn: isbn.into(),
    }
}

async fn count<C: Connection>(cx: &Cx, conn: &C, table: &str, label: &str) -> i64 {
    let rows = expect_outcome(
        conn.query(cx, &format!("SELECT COUNT(*) FROM {table}"), &[])
            .await,
        label,
    );
    rows[0].get_as::<i64>(0).expect("count")
}

struct SessionScenario;

impl OwnedScenario for SessionScenario {
    async fn run<C: Connection + 'static>(&self, cx: &Cx, conn: C, driver: &DriverUnderTest) {
        let d = driver.name();
        let dialect = driver.dialect();
        let authors = dialect.quote_identifier(<Author as Model>::TABLE_NAME);
        let books = dialect.quote_identifier(<Book as Model>::TABLE_NAME);

        for t in [&books, &authors] {
            expect_outcome(
                conn.execute(cx, &format!("DROP TABLE IF EXISTS {t}"), &[])
                    .await,
                &format!("{d}: drop stale {t}"),
            );
        }
        if dialect == Dialect::Sqlite {
            expect_outcome(
                conn.execute(cx, "PRAGMA foreign_keys = ON", &[]).await,
                &format!("{d}: enable foreign keys"),
            );
        }
        for stmt in SchemaBuilder::new()
            .dialect(dialect)
            .create_table::<Author>()
            .create_table::<Book>()
            .build()
        {
            expect_outcome(
                conn.execute(cx, &stmt, &[]).await,
                &format!("{d}: ddl `{stmt}`"),
            );
        }

        let mut s = Session::new(conn);

        // add -> flush -> visible inside the transaction; commit -> persisted; expired after.
        let ann = Author::new(1, "Ann", None);
        let bob = Author::new(2, "Bob", Some("bob@example.com"));
        s.add(&ann);
        s.add(&bob);
        assert_eq!(s.object_state(&ann), Some(ObjectState::New), "{d}");
        assert_eq!(s.pending_new_count(), 2, "{d}");
        expect_outcome(s.flush(cx).await, &format!("{d}: flush inserts"));
        assert!(s.in_transaction(), "{d}: auto_begin opened a transaction");
        assert_eq!(s.pending_new_count(), 0, "{d}");
        assert_eq!(
            s.object_state(&ann),
            Some(ObjectState::Persistent),
            "{d}: flushed object is persistent"
        );
        assert_eq!(
            count(cx, s.connection(), &authors, "count after flush").await,
            2,
            "{d}: rows visible to the flushing connection"
        );
        expect_outcome(s.commit(cx).await, &format!("{d}: commit"));
        assert!(!s.in_transaction(), "{d}");
        assert_eq!(
            s.object_state(&ann),
            Some(ObjectState::Expired),
            "{d}: expire_on_commit"
        );
        assert_eq!(
            count(cx, s.connection(), &authors, "count after commit").await,
            2,
            "{d}"
        );

        // get() reloads an expired object from the database.
        let loaded: Author = expect_outcome(s.get(cx, 2i64).await, &format!("{d}: get 2"))
            .unwrap_or_else(|| panic!("{d}: author 2 missing"));
        assert_eq!(loaded.name, "Bob", "{d}");
        assert_eq!(loaded.email.as_deref(), Some("bob@example.com"), "{d}");

        // rollback discards a flushed-but-uncommitted insert.
        let cy = Author::new(3, "Cy", None);
        s.add(&cy);
        expect_outcome(s.flush(cx).await, &format!("{d}: flush cy"));
        assert_eq!(
            count(cx, s.connection(), &authors, "count with cy").await,
            3,
            "{d}"
        );
        expect_outcome(s.rollback(cx).await, &format!("{d}: rollback"));
        assert_eq!(
            count(cx, s.connection(), &authors, "count after rollback").await,
            2,
            "{d}: rollback removed the flushed row"
        );

        // Dirty tracking: only the changed column is written.
        let mut bob2 = loaded.clone();
        bob2.name = "Robert".into();
        s.mark_dirty(&bob2);
        assert!(s.is_modified(&bob2), "{d}: mark_dirty records a change");
        let changed = s.modified_attributes(&bob2);
        assert!(
            changed.iter().any(|c| c.contains("name")),
            "{d}: modified attributes {changed:?} must name `name`"
        );
        expect_outcome(s.commit(cx).await, &format!("{d}: commit update"));
        let row = expect_outcome(
            s.connection()
                .query(
                    cx,
                    &format!("SELECT name, email FROM {authors} WHERE id = 2"),
                    &[],
                )
                .await,
            &format!("{d}: read bob"),
        );
        assert_eq!(
            row[0].get_as::<String>(0).unwrap(),
            "Robert",
            "{d}: update written"
        );
        assert_eq!(
            row[0].get_as::<Option<String>>(1).unwrap().as_deref(),
            Some("bob@example.com"),
            "{d}: untouched column kept"
        );

        // delete -> commit.
        s.delete(&ann);
        expect_outcome(s.commit(cx).await, &format!("{d}: commit delete"));
        assert_eq!(
            count(cx, s.connection(), &authors, "count after delete").await,
            1,
            "{d}"
        );

        // A constraint violation surfaces as an error and the session stays usable.
        s.add(&book(1, 2, "First", "isbn-1"));
        s.add(&book(2, 2, "Second", "isbn-1"));
        match s.flush(cx).await {
            Outcome::Err(e) => eprintln!("{d}: unique violation surfaced as: {e}"),
            other => panic!("{d}: duplicate isbn must fail the flush, got {other:?}"),
        }
        expect_outcome(
            s.rollback(cx).await,
            &format!("{d}: rollback after violation"),
        );
        s.add(&book(3, 2, "Third", "isbn-3"));
        expect_outcome(s.commit(cx).await, &format!("{d}: commit after violation"));
        assert_eq!(
            count(cx, s.connection(), &books, "books after recovery").await,
            1,
            "{d}: only the valid book exists"
        );

        // One-to-many loading through the session.
        s.add(&book(4, 2, "Fourth", "isbn-4"));
        expect_outcome(s.commit(cx).await, &format!("{d}: commit fourth book"));
        let mut parents = vec![Author::new(2, "Robert", Some("bob@example.com"))];
        let loaded_children = expect_outcome(
            s.load_one_to_many(cx, &mut parents, |a| &mut a.books, |a| Value::from(a.id))
                .await,
            &format!("{d}: load_one_to_many"),
        );
        let titles: Vec<&str> = parents[0]
            .books
            .get()
            .unwrap_or(&[])
            .iter()
            .map(|b| b.title.as_str())
            .collect();
        assert_eq!(loaded_children, 2, "{d}: two children loaded");
        assert!(parents[0].books.is_loaded(), "{d}");
        assert!(
            titles.contains(&"Third") && titles.contains(&"Fourth"),
            "{d}: children {titles:?}"
        );

        // Database-side cascade: deleting the author removes the books.
        s.delete(&bob2);
        expect_outcome(s.commit(cx).await, &format!("{d}: commit delete author"));
        assert_eq!(
            count(cx, s.connection(), &authors, "authors after cascade").await,
            0,
            "{d}"
        );
        assert_eq!(
            count(cx, s.connection(), &books, "books after cascade").await,
            0,
            "{d}: ON DELETE CASCADE removed the books"
        );

        // A cancelled Cx makes flush return Cancelled and leaves the database unchanged.
        let cancelled = Cx::for_testing();
        cancelled.cancel_with(CancelKind::User, Some("e2e session"));
        s.add(&Author::new(9, "Never", None));
        match s.flush(&cancelled).await {
            Outcome::Cancelled(_) => {}
            other => panic!("{d}: flush with a cancelled Cx must be Cancelled, got {other:?}"),
        }
        if s.in_transaction() {
            expect_outcome(s.rollback(cx).await, &format!("{d}: rollback after cancel"));
        }
        assert_eq!(
            count(
                cx,
                s.connection(),
                &authors,
                "authors after cancelled flush"
            )
            .await,
            0,
            "{d}: cancelled flush wrote nothing"
        );

        for t in [&books, &authors] {
            expect_outcome(
                s.connection()
                    .execute(cx, &format!("DROP TABLE {t}"), &[])
                    .await,
                &format!("{d}: drop {t}"),
            );
        }
    }
}

#[test]
fn session_unit_of_work_works_on_every_available_driver() {
    let cx = Cx::for_testing();
    let ran = run_owned_on_every_driver(&cx, &SessionScenario);
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
    assert!(ran.contains(&"c-sqlite(memory)"), "{ran:?}");
}
