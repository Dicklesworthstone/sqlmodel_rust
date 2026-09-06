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

/// One author's `name` and `email`, read with raw SQL.
async fn name_and_email<C: Connection>(
    cx: &Cx,
    conn: &C,
    authors: &str,
    id: i64,
) -> (String, Option<String>) {
    let rows = expect_outcome(
        conn.query(
            cx,
            &format!("SELECT name, email FROM {authors} WHERE id = {id}"),
            &[],
        )
        .await,
        "read author",
    );
    (
        rows[0].get_as::<String>(0).unwrap(),
        rows[0].get_as::<Option<String>>(1).unwrap(),
    )
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

    let ran = run_owned_on_every_driver(&cx, &BatchOps);
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
    assert!(ran.contains(&"c-sqlite(memory)"), "{ran:?}");

    let ran = run_owned_on_every_driver(&cx, &IdentityAndLoads);
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
    assert!(ran.contains(&"c-sqlite(memory)"), "{ran:?}");

    let ran = run_owned_on_every_driver(&cx, &StatementReuse);
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
    assert!(ran.contains(&"c-sqlite(memory)"), "{ran:?}");
}

/// A server-side statement counter, where the server keeps one.
#[derive(Clone, Copy)]
enum StatementCounter {
    Prepares,
    Executes,
}

/// Read the counter from the same connection the session uses (both are
/// per-session values). `None` where the server has no such counter.
async fn statement_counter<C: Connection>(
    cx: &Cx,
    conn: &C,
    dialect: Dialect,
    which: StatementCounter,
    label: &str,
) -> Option<i64> {
    match (dialect, which) {
        (Dialect::Mysql, StatementCounter::Prepares) => {
            let rows = expect_outcome(
                conn.query(cx, "SHOW SESSION STATUS LIKE 'Com_stmt_prepare'", &[])
                    .await,
                label,
            );
            Some(
                rows[0]
                    .get_named::<String>("Value")
                    .unwrap()
                    .parse()
                    .unwrap(),
            )
        }
        (Dialect::Mysql, StatementCounter::Executes) => {
            let rows = expect_outcome(
                conn.query(cx, "SHOW SESSION STATUS LIKE 'Com_stmt_execute'", &[])
                    .await,
                label,
            );
            Some(
                rows[0]
                    .get_named::<String>("Value")
                    .unwrap()
                    .parse()
                    .unwrap(),
            )
        }
        (Dialect::Postgres, StatementCounter::Prepares) => {
            let rows = expect_outcome(
                conn.query(cx, "SELECT count(*) FROM pg_prepared_statements", &[])
                    .await,
                label,
            );
            Some(rows[0].get_as::<i64>(0).unwrap())
        }
        (Dialect::Postgres | Dialect::Sqlite, _) => None,
    }
}

/// What server-side statement reuse a `Session` actually gets, read from the
/// server: MySQL prepares each distinct statement once (the driver's
/// per-connection cache) and executes it per call; PostgreSQL prepares each
/// distinct statement once (the driver's 64-entry LRU cache) and executes it
/// per call; the SQLite drivers keep no statement cache. The README says exactly this.
struct StatementReuse;

impl OwnedScenario for StatementReuse {
    async fn run<C: Connection + 'static>(&self, cx: &Cx, conn: C, driver: &DriverUnderTest) {
        let d = driver.name();
        let dialect = driver.dialect();
        fresh_tables(cx, &conn, driver).await;
        // Seed outside the session so the gets below hit the database.
        for i in 1..=20 {
            expect_outcome(
                insert!(&Author::new(i, &format!("author{i}"), None))
                    .execute(cx, &conn)
                    .await,
                &format!("{d}: seed {i}"),
            );
        }
        let mut s = Session::new(conn);
        let prepares_before = statement_counter(
            cx,
            s.connection(),
            dialect,
            StatementCounter::Prepares,
            &format!("{d}: prepares before"),
        )
        .await;
        let executes_before = statement_counter(
            cx,
            s.connection(),
            dialect,
            StatementCounter::Executes,
            &format!("{d}: executes before"),
        )
        .await;
        for i in 1..=20i64 {
            let author: Option<Author> =
                expect_outcome(s.get(cx, i).await, &format!("{d}: get {i}"));
            assert_eq!(
                author.map(|a| a.name),
                Some(format!("author{i}")),
                "{d}: get {i}"
            );
        }
        let prepares_after = statement_counter(
            cx,
            s.connection(),
            dialect,
            StatementCounter::Prepares,
            &format!("{d}: prepares after"),
        )
        .await;
        let executes_after = statement_counter(
            cx,
            s.connection(),
            dialect,
            StatementCounter::Executes,
            &format!("{d}: executes after"),
        )
        .await;
        match dialect {
            Dialect::Mysql => {
                assert_eq!(
                    prepares_after.unwrap() - prepares_before.unwrap(),
                    1,
                    "{d}: the session's SELECT by key is prepared once"
                );
                assert_eq!(
                    executes_after.unwrap() - executes_before.unwrap(),
                    20,
                    "{d}: and executed per get"
                );
                eprintln!("{d}: 20 gets = 1 COM_STMT_PREPARE + 20 COM_STMT_EXECUTE (driver cache)");
            }
            Dialect::Postgres => {
                assert_eq!(
                    prepares_after.unwrap() - prepares_before.unwrap(),
                    1,
                    "{d}: the session's SELECT by key is prepared once"
                );
                eprintln!("{d}: 20 gets = 1 Parse + 20 Execute (driver cache)");
            }
            Dialect::Sqlite => {
                eprintln!("{d}: no statement cache in the driver; every query is prepared afresh");
            }
        }
    }
}

/// Drop and recreate the four tables the session scenarios share.
async fn fresh_tables<C: Connection>(cx: &Cx, conn: &C, driver: &DriverUnderTest) {
    let d = driver.name();
    let dialect = driver.dialect();
    let q = |t: &str| dialect.quote_identifier(t);
    for t in [
        <AuthorTag as Model>::TABLE_NAME,
        <Book as Model>::TABLE_NAME,
        <Tag as Model>::TABLE_NAME,
        <Author as Model>::TABLE_NAME,
    ] {
        expect_outcome(
            conn.execute(cx, &format!("DROP TABLE IF EXISTS {}", q(t)), &[])
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
}

/// What the identity map guarantees on a real database (counted through the
/// capturing connection), and the N+1 detector on real loads: a per-parent
/// loop crosses the threshold, one batch load does not.
struct IdentityAndLoads;

impl OwnedScenario for IdentityAndLoads {
    async fn run<C: Connection + 'static>(&self, cx: &Cx, conn: C, driver: &DriverUnderTest) {
        let d = driver.name();
        let dialect = driver.dialect();
        let authors = dialect.quote_identifier(<Author as Model>::TABLE_NAME);
        fresh_tables(cx, &conn, driver).await;
        let mut s = Session::new(CapturingConnection::new(conn));
        let selects = |s: &Session<CapturingConnection<C>>| {
            s.connection()
                .statements()
                .iter()
                .filter(|(sql, _)| sql.starts_with("SELECT"))
                .count()
        };

        for i in 1..=5 {
            s.add(&Author::new(i, &format!("author{i}"), None));
            for j in 1..=2 {
                s.add(&book(
                    i * 10 + j,
                    i,
                    &format!("book{i}-{j}"),
                    &format!("isbn-{i}-{j}"),
                ));
            }
        }
        expect_outcome(s.commit(cx).await, &format!("{d}: seed"));

        // 1. A second get for a tracked object issues no SELECT.
        s.connection().clear();
        let first: Author = expect_outcome(s.get(cx, 1i64).await, &format!("{d}: get 1"))
            .unwrap_or_else(|| panic!("{d}: author 1 missing"));
        assert_eq!(selects(&s), 1, "{d}: the first get queries");
        let again: Author = expect_outcome(s.get(cx, 1i64).await, &format!("{d}: get 1 again"))
            .unwrap_or_else(|| panic!("{d}: author 1 missing"));
        assert_eq!(
            selects(&s),
            1,
            "{d}: the second get is served by the identity map"
        );
        assert_eq!(first.name, again.name, "{d}");

        // 2. A change made behind the session's back is invisible until expire.
        expect_outcome(
            s.connection()
                .execute(
                    cx,
                    &format!(
                        "UPDATE {authors} SET {} = 'renamed' WHERE {} = 1",
                        dialect.quote_identifier("name"),
                        dialect.quote_identifier("id")
                    ),
                    &[],
                )
                .await,
            &format!("{d}: rename behind the session"),
        );
        let stale: Author = expect_outcome(s.get(cx, 1i64).await, &format!("{d}: get stale"))
            .unwrap_or_else(|| panic!("{d}: author 1 missing"));
        assert_eq!(
            stale.name, "author1",
            "{d}: identity map still serves the old state"
        );
        s.expire(&stale, None);
        assert!(s.is_expired(&stale), "{d}");
        let before = selects(&s);
        let fresh: Author = expect_outcome(s.get(cx, 1i64).await, &format!("{d}: get fresh"))
            .unwrap_or_else(|| panic!("{d}: author 1 missing"));
        assert_eq!(selects(&s), before + 1, "{d}: expire forces one reload");
        assert_eq!(fresh.name, "renamed", "{d}: reloaded state");

        // 3. refresh reloads immediately.
        expect_outcome(
            s.connection()
                .execute(
                    cx,
                    &format!(
                        "UPDATE {authors} SET {} = 'refreshed' WHERE {} = 1",
                        dialect.quote_identifier("name"),
                        dialect.quote_identifier("id")
                    ),
                    &[],
                )
                .await,
            &format!("{d}: rename again"),
        );
        let refreshed: Author =
            expect_outcome(s.refresh(cx, &fresh).await, &format!("{d}: refresh"))
                .unwrap_or_else(|| panic!("{d}: author 1 missing on refresh"));
        assert_eq!(refreshed.name, "refreshed", "{d}");

        // 4. N+1 detection: five per-parent loads of the same relationship
        // cross a threshold of 3; one batch load for the same parents does not.
        s.enable_n1_detection(3);
        s.reset_n1_tracking();
        let mut parents: Vec<Author> = (1..=5)
            .map(|i| Author::new(i, &format!("author{i}"), None))
            .collect();
        s.connection().clear();
        for parent in &mut parents {
            let loaded = expect_outcome(
                s.load_one_to_many(
                    cx,
                    std::slice::from_mut(parent),
                    |a| &mut a.books,
                    |a| Value::from(a.id),
                )
                .await,
                &format!("{d}: per-parent load"),
            );
            assert_eq!(loaded, 2, "{d}: two books per author");
        }
        assert_eq!(selects(&s), 5, "{d}: one SELECT per parent");
        let stats = s.n1_stats().expect("detection enabled");
        assert_eq!(stats.total_loads, 5, "{d}: {stats:?}");
        assert_eq!(
            stats.potential_n1, 1,
            "{d}: the loop is reported once: {stats:?}"
        );

        s.reset_n1_tracking();
        s.connection().clear();
        let mut parents: Vec<Author> = (1..=5)
            .map(|i| Author::new(i, &format!("author{i}"), None))
            .collect();
        let loaded = expect_outcome(
            s.load_one_to_many(cx, &mut parents, |a| &mut a.books, |a| Value::from(a.id))
                .await,
            &format!("{d}: batch load"),
        );
        assert_eq!(loaded, 10, "{d}");
        assert_eq!(selects(&s), 1, "{d}: one SELECT for the batch");
        let stats = s.n1_stats().expect("detection enabled");
        assert_eq!(stats.total_loads, 1, "{d}: {stats:?}");
        assert_eq!(
            stats.potential_n1, 0,
            "{d}: a batch load is not an N+1: {stats:?}"
        );

        // 5. Lazy many-to-one, per book, is the other N+1 shape.
        s.reset_n1_tracking();
        let library: Vec<Book> = (1..=5).map(|i| book(i * 10 + 1, i, "", "")).collect();
        for volume in &library {
            expect_outcome(
                s.load_lazy(&volume.author, cx).await,
                &format!("{d}: lazy author"),
            );
        }
        let stats = s.n1_stats().expect("detection enabled");
        assert_eq!(stats.total_loads, 5, "{d}: {stats:?}");
        assert_eq!(stats.potential_n1, 1, "{d}: {stats:?}");

        for t in [
            <AuthorTag as Model>::TABLE_NAME,
            <Book as Model>::TABLE_NAME,
            <Tag as Model>::TABLE_NAME,
            <Author as Model>::TABLE_NAME,
        ] {
            expect_outcome(
                s.connection()
                    .execute(
                        cx,
                        &format!("DROP TABLE {}", dialect.quote_identifier(t)),
                        &[],
                    )
                    .await,
                &format!("{d}: drop {t}"),
            );
        }
    }
}

/// The batch-shaped entry points: `add_all` and the `sqlmodel_update` family
/// (dictionary update, patch-model update that leaves `None` alone, and the
/// `update_fields` filter), each proven on the database with raw SQL.
struct BatchOps;

impl OwnedScenario for BatchOps {
    async fn run<C: Connection + 'static>(&self, cx: &Cx, conn: C, driver: &DriverUnderTest) {
        use sqlmodel_core::validate::{SqlModelUpdate, UpdateOptions};
        use std::collections::HashMap;

        let d = driver.name();
        let dialect = driver.dialect();
        let authors = dialect.quote_identifier(<Author as Model>::TABLE_NAME);
        fresh_tables(cx, &conn, driver).await;
        let mut s = Session::new(conn);

        // add_all: fifty objects, one flush, one commit.
        let batch: Vec<Author> = (1..=50)
            .map(|i| Author::new(i, &format!("author{i}"), None))
            .collect();
        s.add_all(&batch);
        assert_eq!(s.pending_new_count(), 50, "{d}");
        expect_outcome(s.flush(cx).await, &format!("{d}: flush add_all"));
        expect_outcome(s.commit(cx).await, &format!("{d}: commit add_all"));
        assert_eq!(
            count(cx, s.connection(), &authors, "after add_all").await,
            50,
            "{d}"
        );
        assert_eq!(s.debug_state().tracked, 50, "{d}: all fifty tracked");

        // A duplicate key inside a later batch fails the flush with the
        // driver's constraint error; after rollback the session is usable.
        let conn = s.into_connection();
        let mut s = Session::new(conn);
        s.add_all(&[Author::new(51, "new", None), Author::new(1, "dup", None)]);
        match s.flush(cx).await {
            Outcome::Err(e) => {
                assert!(matches!(e, Error::Query(_)), "{d}: {e}");
                eprintln!("{d}: duplicate key in add_all surfaced as: {e}");
            }
            other => panic!("{d}: duplicate key must fail the flush, got {other:?}"),
        }
        expect_outcome(s.rollback(cx).await, &format!("{d}: rollback dup"));
        assert_eq!(
            count(cx, s.connection(), &authors, "after failed batch").await,
            50,
            "{d}: nothing from the failed batch persisted"
        );
        s.add(&Author::new(51, "new", None));
        expect_outcome(s.commit(cx).await, &format!("{d}: commit after rollback"));
        assert_eq!(
            count(cx, s.connection(), &authors, "after recovery").await,
            51,
            "{d}"
        );

        // sqlmodel_update from a map: both columns change.
        let mut five: Author = expect_outcome(s.get(cx, 5i64).await, &format!("{d}: get 5"))
            .unwrap_or_else(|| panic!("{d}: author 5 missing"));
        five.sqlmodel_update(
            HashMap::from([
                ("name".to_string(), serde_json::json!("Renamed")),
                ("email".to_string(), serde_json::json!("five@example.com")),
            ]),
            UpdateOptions::default(),
        )
        .expect("update from map");
        assert_eq!(five.name, "Renamed", "{d}");
        s.mark_dirty(&five);
        expect_outcome(s.commit(cx).await, &format!("{d}: commit map update"));
        assert_eq!(
            name_and_email(cx, s.connection(), &authors, 5).await,
            ("Renamed".to_string(), Some("five@example.com".to_string())),
            "{d}: map update persisted"
        );

        // sqlmodel_update_from a patch model: None fields do not overwrite.
        let mut five: Author = expect_outcome(s.get(cx, 5i64).await, &format!("{d}: get 5 again"))
            .unwrap_or_else(|| panic!("{d}: author 5 missing"));
        let patch = Author::new(5, "Patched", None);
        five.sqlmodel_update_from(&patch, UpdateOptions::default())
            .expect("update from patch");
        assert_eq!(five.name, "Patched", "{d}");
        assert_eq!(
            five.email.as_deref(),
            Some("five@example.com"),
            "{d}: a None in the patch leaves the field alone"
        );
        s.mark_dirty(&five);
        expect_outcome(s.commit(cx).await, &format!("{d}: commit patch update"));
        assert_eq!(
            name_and_email(cx, s.connection(), &authors, 5).await,
            ("Patched".to_string(), Some("five@example.com".to_string())),
            "{d}: patch update persisted, email kept"
        );

        // update_fields restricts which keys of the input apply.
        let mut five: Author = expect_outcome(s.get(cx, 5i64).await, &format!("{d}: get 5 third"))
            .unwrap_or_else(|| panic!("{d}: author 5 missing"));
        five.sqlmodel_update(
            HashMap::from([
                ("name".to_string(), serde_json::json!("Only")),
                (
                    "email".to_string(),
                    serde_json::json!("ignored@example.com"),
                ),
            ]),
            UpdateOptions::default().update_fields(["name"]),
        )
        .expect("filtered update");
        s.mark_dirty(&five);
        expect_outcome(s.commit(cx).await, &format!("{d}: commit filtered update"));
        assert_eq!(
            name_and_email(cx, s.connection(), &authors, 5).await,
            ("Only".to_string(), Some("five@example.com".to_string())),
            "{d}: update_fields kept email"
        );

        for t in [
            <AuthorTag as Model>::TABLE_NAME,
            <Book as Model>::TABLE_NAME,
            <Tag as Model>::TABLE_NAME,
            <Author as Model>::TABLE_NAME,
        ] {
            expect_outcome(
                s.connection()
                    .execute(
                        cx,
                        &format!("DROP TABLE {}", dialect.quote_identifier(t)),
                        &[],
                    )
                    .await,
                &format!("{d}: drop {t}"),
            );
        }
    }
}
