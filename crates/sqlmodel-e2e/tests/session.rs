//! `Session` (unit of work) against real databases. Until this file existed the
//! session layer had only ever talked to an in-crate mock connection.
//!
//! Every assertion about database state goes through a raw query on the
//! session's own connection, so the session cannot vouch for itself.

// The session's event callbacks are `FnMut() -> Result<(), Error>`; the size of
// `Error` is the library's contract, not something this test can change.
#![allow(clippy::result_large_err)]

use asupersync::{CancelKind, Cx, Outcome};
use serde::{Deserialize, Serialize};
use sqlmodel::prelude::*;
use sqlmodel::{SchemaBuilder, Session};
use sqlmodel_core::{Lazy, LinkTableInfo, RelatedMany};
use sqlmodel_e2e::{
    CapturingConnection, DriverUnderTest, OwnedScenario, expect_outcome, run_owned_on_every_driver,
};
use std::sync::{Arc, Mutex};

/// Author <-> Tag link table, seen from the author side.
const AUTHOR_TAGS: LinkTableInfo =
    LinkTableInfo::new("e2e_session_author_tags", "author_id", "tag_id");

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
    /// Many-to-many through a link table with plain foreign keys: the session
    /// itself must remove an author's link rows when the author is deleted.
    #[sqlmodel(relationship(
        model = "Tag",
        many_to_many,
        link_table(
            table = "e2e_session_author_tags",
            local_column = "author_id",
            remote_column = "tag_id"
        ),
        cascade_delete
    ))]
    tags: RelatedMany<Tag>,
}

impl Author {
    fn new(id: i64, name: &str, email: Option<&str>) -> Self {
        Self {
            id,
            name: name.into(),
            email: email.map(str::to_owned),
            books: RelatedMany::new("author_id"),
            tags: RelatedMany::with_link_table(AUTHOR_TAGS),
        }
    }
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_session_books")]
struct Book {
    #[sqlmodel(primary_key)]
    id: i64,
    #[sqlmodel(foreign_key = "e2e_session_authors.id", on_delete = "CASCADE")]
    author_id: i64,
    title: String,
    #[sqlmodel(unique)]
    isbn: String,
    /// Many-to-one, loaded on demand through `Session::load_many`.
    #[sqlmodel(relationship(model = "Author", foreign_key = "author_id"))]
    author: Lazy<Author>,
}

fn book(id: i64, author_id: i64, title: &str, isbn: &str) -> Book {
    Book {
        id,
        author_id,
        title: title.into(),
        isbn: isbn.into(),
        author: Lazy::from_fk(author_id),
    }
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "e2e_session_tags")]
struct Tag {
    #[sqlmodel(primary_key)]
    id: i64,
    label: String,
    #[sqlmodel(relationship(
        model = "Author",
        many_to_many,
        link_table(
            table = "e2e_session_author_tags",
            local_column = "tag_id",
            remote_column = "author_id"
        )
    ))]
    authors: RelatedMany<Author>,
}

impl Tag {
    fn new(id: i64, label: &str) -> Self {
        Self {
            id,
            label: label.into(),
            authors: RelatedMany::with_link_table(LinkTableInfo::new(
                "e2e_session_author_tags",
                "tag_id",
                "author_id",
            )),
        }
    }
}

/// The link table itself, so the schema builder can create it. Its foreign
/// keys deliberately have no `ON DELETE CASCADE`.
#[derive(sqlmodel::Model, Debug, Clone)]
#[sqlmodel(table = "e2e_session_author_tags")]
struct AuthorTag {
    #[sqlmodel(primary_key, foreign_key = "e2e_session_authors.id")]
    author_id: i64,
    #[sqlmodel(primary_key, foreign_key = "e2e_session_tags.id")]
    tag_id: i64,
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
        // `Author.tags` cascades through the link table, so the session deletes
        // from it whenever an author is deleted; it must exist here too.
        let tags = dialect.quote_identifier(<Tag as Model>::TABLE_NAME);
        let links = dialect.quote_identifier(<AuthorTag as Model>::TABLE_NAME);

        for t in [&links, &books, &tags, &authors] {
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
            .create_table::<Tag>()
            .create_table::<Book>()
            .create_table::<AuthorTag>()
            .build()
        {
            expect_outcome(
                conn.execute(cx, &stmt, &[]).await,
                &format!("{d}: ddl `{stmt}`"),
            );
        }

        // Every statement the session sends is recorded, so the scenario can
        // assert the SQL shape (which columns an UPDATE sets), not only rows.
        let mut s = Session::new(CapturingConnection::new(conn));

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
        s.connection().clear();
        s.mark_dirty(&bob2);
        assert!(s.is_modified(&bob2), "{d}: mark_dirty records a change");
        let changed = s.modified_attributes(&bob2);
        assert!(
            changed.iter().any(|c| c.contains("name")),
            "{d}: modified attributes {changed:?} must name `name`"
        );
        expect_outcome(s.commit(cx).await, &format!("{d}: commit update"));
        let updates: Vec<String> = s
            .connection()
            .statements()
            .into_iter()
            .filter(|(sql, _)| sql.starts_with("UPDATE"))
            .map(|(sql, _)| sql)
            .collect();
        assert_eq!(updates.len(), 1, "{d}: exactly one UPDATE: {updates:?}");
        assert!(
            updates[0].contains(&format!("SET {} = ", dialect.quote_identifier("name")))
                && !updates[0].contains(&dialect.quote_identifier("email")),
            "{d}: the UPDATE must set only the changed column: {}",
            updates[0]
        );
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

        for t in [&links, &books, &tags, &authors] {
            expect_outcome(
                s.connection()
                    .execute(cx, &format!("DROP TABLE {t}"), &[])
                    .await,
                &format!("{d}: drop {t}"),
            );
        }
    }
}

/// Relationships, lifecycle events, `merge`, and session-side cascades on a
/// real database. The link table has plain foreign keys, so deleting an author
/// succeeds only if the session removes that author's link rows first.
struct Relations;

impl OwnedScenario for Relations {
    async fn run<C: Connection + 'static>(&self, cx: &Cx, conn: C, driver: &DriverUnderTest) {
        let d = driver.name();
        let dialect = driver.dialect();
        let authors = dialect.quote_identifier(<Author as Model>::TABLE_NAME);
        let books = dialect.quote_identifier(<Book as Model>::TABLE_NAME);
        let tags = dialect.quote_identifier(<Tag as Model>::TABLE_NAME);
        let links = dialect.quote_identifier(<AuthorTag as Model>::TABLE_NAME);

        for t in [&links, &books, &tags, &authors] {
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
            .create_table::<Tag>()
            .create_table::<Book>()
            .create_table::<AuthorTag>()
            .build()
        {
            expect_outcome(
                conn.execute(cx, &stmt, &[]).await,
                &format!("{d}: ddl `{stmt}`"),
            );
        }

        let mut s = Session::new(conn);

        // Lifecycle events, recorded in the order they fire.
        let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
        let hook = |name: &'static str| {
            let log = Arc::clone(&log);
            move || -> sqlmodel_core::Result<()> {
                log.lock().unwrap().push(name);
                Ok(())
            }
        };
        s.on_before_flush(hook("before_flush"));
        s.on_after_flush(hook("after_flush"));
        s.on_before_commit(hook("before_commit"));
        s.on_after_commit(hook("after_commit"));
        s.on_after_rollback(hook("after_rollback"));
        let take_events = || {
            let mut guard = log.lock().unwrap();
            std::mem::take(&mut *guard)
        };

        let mut ann = Author::new(1, "Ann", None);
        let mut bob = Author::new(2, "Bob", None);
        let mut rust = Tag::new(1, "rust");
        let mut sql = Tag::new(2, "sql");
        let orm = Tag::new(3, "orm");
        s.add(&ann);
        s.add(&bob);
        s.add(&rust);
        s.add(&sql);
        s.add(&orm);
        expect_outcome(s.commit(cx).await, &format!("{d}: commit seed rows"));
        assert_eq!(
            take_events(),
            [
                "before_flush",
                "after_flush",
                "before_commit",
                "after_commit"
            ],
            "{d}: commit flushes first and fires every event once, in order"
        );

        // Many-to-many: relate in memory, persist the link rows explicitly.
        s.relate_many_to_many(&mut ann, |a| &mut a.tags, &mut rust, |t| &mut t.authors);
        s.relate_many_to_many(&mut ann, |a| &mut a.tags, &mut sql, |t| &mut t.authors);
        s.relate_many_to_many(&mut bob, |a| &mut a.tags, &mut rust, |t| &mut t.authors);
        let linked = expect_outcome(
            s.flush_related_many(
                cx,
                std::slice::from_mut(&mut ann),
                |a| &mut a.tags,
                |a| Value::from(a.id),
                &AUTHOR_TAGS,
            )
            .await,
            &format!("{d}: flush ann's links"),
        );
        assert_eq!(linked, 2, "{d}: two link rows for ann");
        let linked = expect_outcome(
            s.flush_related_many(
                cx,
                std::slice::from_mut(&mut bob),
                |a| &mut a.tags,
                |a| Value::from(a.id),
                &AUTHOR_TAGS,
            )
            .await,
            &format!("{d}: flush bob's links"),
        );
        assert_eq!(linked, 1, "{d}");
        assert_eq!(count(cx, s.connection(), &links, "links").await, 3, "{d}");

        // Load the many-to-many collection back through the link table.
        let mut fresh = vec![Author::new(1, "Ann", None), Author::new(2, "Bob", None)];
        let loaded = expect_outcome(
            s.load_many_to_many_pk(
                cx,
                &mut fresh,
                |a| &mut a.tags,
                |a| vec![Value::from(a.id)],
                &AUTHOR_TAGS,
            )
            .await,
            &format!("{d}: load_many_to_many"),
        );
        assert_eq!(loaded, 3, "{d}: three links resolved");
        let labels = |a: &Author| {
            let mut v: Vec<String> = a
                .tags
                .get()
                .unwrap_or(&[])
                .iter()
                .map(|t| t.label.clone())
                .collect();
            v.sort();
            v
        };
        assert_eq!(labels(&fresh[0]), ["rust", "sql"], "{d}: ann's tags");
        assert_eq!(labels(&fresh[1]), ["rust"], "{d}: bob's tags");

        // Many-to-one through `Lazy`: one query resolves every book's author.
        s.add(&book(1, 1, "Ann's book", "isbn-a"));
        s.add(&book(2, 2, "Bob's book", "isbn-b"));
        expect_outcome(s.commit(cx).await, &format!("{d}: commit books"));
        let library = vec![
            book(1, 1, "Ann's book", "isbn-a"),
            book(2, 2, "Bob's book", "isbn-b"),
        ];
        let loaded = expect_outcome(
            s.load_many(cx, &library, |b| &b.author).await,
            &format!("{d}: load_many"),
        );
        assert_eq!(loaded, 2, "{d}: two authors resolved");
        assert_eq!(
            library[0].author.get().map(|a| a.name.as_str()),
            Some("Ann"),
            "{d}: lazy author of book 1"
        );
        assert_eq!(
            library[1].author.get().map(|a| a.name.as_str()),
            Some("Bob"),
            "{d}: lazy author of book 2"
        );

        // Unrelate removes exactly one link row.
        s.unrelate_many_to_many(&mut ann, |a| &mut a.tags, &mut sql, |t| &mut t.authors);
        let unlinked = expect_outcome(
            s.flush_related_many(
                cx,
                std::slice::from_mut(&mut ann),
                |a| &mut a.tags,
                |a| Value::from(a.id),
                &AUTHOR_TAGS,
            )
            .await,
            &format!("{d}: flush unlink"),
        );
        assert_eq!(unlinked, 1, "{d}");
        assert_eq!(
            count(cx, s.connection(), &links, "links after unlink").await,
            2,
            "{d}"
        );

        // merge: a detached instance's state reaches the database on commit.
        let merged: Tag = expect_outcome(
            s.merge(cx, Tag::new(3, "object mapping"), true).await,
            &format!("{d}: merge"),
        );
        assert_eq!(
            merged.label, "object mapping",
            "{d}: merge returns the attached state"
        );
        expect_outcome(s.commit(cx).await, &format!("{d}: commit merge"));
        let rows = expect_outcome(
            s.connection()
                .query(
                    cx,
                    &format!(
                        "SELECT {} FROM {tags} WHERE {} = {}",
                        dialect.quote_identifier("label"),
                        dialect.quote_identifier("id"),
                        dialect.placeholder(1)
                    ),
                    &[Value::from(3i64)],
                )
                .await,
            &format!("{d}: read merged tag"),
        );
        assert_eq!(
            rows[0].get_as::<String>(0).unwrap(),
            "object mapping",
            "{d}: merged change persisted"
        );

        // Rollback fires its event and discards the flushed insert.
        take_events();
        s.add(&Tag::new(4, "temporary"));
        expect_outcome(s.flush(cx).await, &format!("{d}: flush temporary"));
        expect_outcome(s.rollback(cx).await, &format!("{d}: rollback temporary"));
        assert_eq!(
            take_events(),
            ["before_flush", "after_flush", "after_rollback"],
            "{d}: rollback event order"
        );
        assert_eq!(
            count(cx, s.connection(), &tags, "tags after rollback").await,
            3,
            "{d}"
        );

        // Session-side cascade: the link rows have no database cascade, so the
        // session must delete them before the author, or the FK rejects it.
        s.delete(&ann);
        expect_outcome(
            s.commit(cx).await,
            &format!("{d}: delete an author who still has link rows"),
        );
        assert_eq!(
            count(cx, s.connection(), &links, "links after delete").await,
            1,
            "{d}: only bob's link remains"
        );
        assert_eq!(
            count(cx, s.connection(), &authors, "authors after delete").await,
            1,
            "{d}"
        );
        assert_eq!(
            count(cx, s.connection(), &books, "books after delete").await,
            1,
            "{d}: database cascade removed ann's book"
        );

        for t in [&links, &books, &tags, &authors] {
            expect_outcome(
                s.connection()
                    .execute(cx, &format!("DROP TABLE {t}"), &[])
                    .await,
                &format!("{d}: drop {t}"),
            );
        }
    }
}

/// Both scenarios share table names on the shared network databases, so they
/// run in sequence inside one test; two test threads racing to
/// `DROP`/`CREATE` the same PostgreSQL table fail with a catalog conflict.
#[test]
fn session_unit_of_work_and_relationships_work_on_every_available_driver() {
    let cx = Cx::for_testing();
    let ran = run_owned_on_every_driver(&cx, &SessionScenario);
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
    assert!(ran.contains(&"c-sqlite(memory)"), "{ran:?}");

    let ran = run_owned_on_every_driver(&cx, &Relations);
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
    assert!(ran.contains(&"c-sqlite(memory)"), "{ran:?}");
}
