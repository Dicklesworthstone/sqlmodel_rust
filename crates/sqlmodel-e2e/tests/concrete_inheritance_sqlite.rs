//! Concrete-table inheritance polymorphic queries end to end
//! (`bd-kzp1.2`): two child tables own their full column sets, and one
//! `polymorphic_concrete2` union returns fully-typed child rows — the
//! `__type` tag hydrates each row as the right variant in a single round
//! trip, with typed NULL fillers covering the columns a branch lacks.
//!
//! Runs on every available driver; see the harness docs for how the network
//! drivers join when their service URLs are set.

use asupersync::Cx;
use sqlmodel::SchemaBuilder;
use sqlmodel::prelude::*;
use sqlmodel_e2e::{DriverUnderTest, Scenario, expect_outcome, run_on_every_driver};

/// Abstract concrete-inheritance base: metadata only, no rows of its own.
#[derive(sqlmodel::Model, Debug, Clone, PartialEq)]
#[sqlmodel(table = "e2e_cti_contents", inheritance = "concrete")]
struct CtiContent {
    #[sqlmodel(primary_key)]
    id: i64,
    title: String,
}

#[derive(sqlmodel::Model, Debug, Clone, PartialEq)]
#[sqlmodel(
    table = "e2e_cti_articles",
    inheritance = "concrete",
    inherits = "CtiContent"
)]
struct CtiArticle {
    #[sqlmodel(primary_key)]
    id: i64,
    title: String,
    body: String,
}

#[derive(sqlmodel::Model, Debug, Clone, PartialEq)]
#[sqlmodel(
    table = "e2e_cti_videos",
    inheritance = "concrete",
    inherits = "CtiContent"
)]
struct CtiVideo {
    #[sqlmodel(primary_key)]
    id: i64,
    title: String,
    #[sqlmodel(nullable)]
    duration_secs: Option<i64>,
}

fn article(id: i64, title: &str, body: &str) -> CtiArticle {
    CtiArticle {
        id,
        title: title.into(),
        body: body.into(),
    }
}

fn video(id: i64, title: &str, duration_secs: Option<i64>) -> CtiVideo {
    CtiVideo {
        id,
        title: title.into(),
        duration_secs,
    }
}

async fn reset_tables<C: Connection>(cx: &Cx, conn: &C) -> Outcome<(), Error> {
    for table in ["e2e_cti_articles", "e2e_cti_videos"] {
        let quoted = conn.dialect().quote_identifier(table);
        expect_outcome(
            conn.execute(cx, &format!("DROP TABLE IF EXISTS {quoted}"), &[])
                .await,
            &format!("drop stale {table}"),
        );
    }
    for model_sql in [
        SchemaBuilder::new()
            .dialect(conn.dialect())
            .create_table::<CtiArticle>()
            .build(),
        SchemaBuilder::new()
            .dialect(conn.dialect())
            .create_table::<CtiVideo>()
            .build(),
    ] {
        for stmt in model_sql {
            expect_outcome(conn.execute(cx, &stmt, &[]).await, &format!("ddl `{stmt}`"));
        }
    }
    Outcome::Ok(())
}

struct ConcreteInheritance;

impl Scenario for ConcreteInheritance {
    async fn run<C: Connection>(&self, cx: &Cx, conn: &C, driver: &DriverUnderTest) {
        reset_tables(cx, conn).await;

        // Seed: two articles and two videos, interleaved ids so ordering by id
        // must merge the branches.
        for row in [
            article(1, "article-alpha", "alpha body"),
            article(3, "article-gamma", "gamma body"),
        ] {
            expect_outcome(
                insert!(&row).execute(cx, conn).await,
                &format!("seed {}: {}", driver.name(), row.title),
            );
        }
        for row in [
            video(2, "video-beta", Some(120)),
            video(4, "video-delta", None),
        ] {
            expect_outcome(
                insert!(&row).execute(cx, conn).await,
                &format!("seed {}: {}", driver.name(), row.title),
            );
        }

        // Every row hydrates as the right variant, ordered by id across the
        // union; child-specific columns round-trip (including a NULL through
        // the filler column).
        let rows = expect_outcome(
            select!(CtiContent)
                .polymorphic_concrete2::<CtiArticle, CtiVideo>()
                .order_by(Expr::col("id").asc())
                .all(cx, conn)
                .await,
            "polymorphic select all",
        );
        assert_eq!(rows.len(), 4, "both branches returned: {rows:?}");
        let PolymorphicConcrete2::C1(a) = &rows[0] else {
            panic!("row 0 should be CtiArticle, got {:?}", rows[0]);
        };
        assert_eq!(a.title, "article-alpha");
        assert_eq!(a.body, "alpha body");
        let PolymorphicConcrete2::C2(v) = &rows[1] else {
            panic!("row 1 should be CtiVideo, got {:?}", rows[1]);
        };
        assert_eq!(v.title, "video-beta");
        assert_eq!(v.duration_secs, Some(120));
        let PolymorphicConcrete2::C2(v) = &rows[3] else {
            panic!("row 3 should be CtiVideo, got {:?}", rows[3]);
        };
        // The NULL that arrived through the filler column stays NULL.
        assert_eq!(v.duration_secs, None);

        // The same filter applies to every branch.
        let rows = expect_outcome(
            select!(CtiContent)
                .polymorphic_concrete2::<CtiArticle, CtiVideo>()
                .filter(Expr::col("title").like("article-%"))
                .order_by(Expr::col("id").asc())
                .all(cx, conn)
                .await,
            "polymorphic select filtered",
        );
        assert_eq!(rows.len(), 2, "only article rows match: {rows:?}");
        for row in &rows {
            assert!(
                matches!(row, PolymorphicConcrete2::C1(_)),
                "filtered rows are all articles: {row:?}"
            );
        }

        // LIMIT scopes the union as a whole, not per branch.
        let rows = expect_outcome(
            select!(CtiContent)
                .polymorphic_concrete2::<CtiArticle, CtiVideo>()
                .order_by(Expr::col("id").desc())
                .limit(3)
                .all(cx, conn)
                .await,
            "polymorphic select limited",
        );
        assert_eq!(rows.len(), 3, "limit caps the union: {rows:?}");
        let ids: Vec<i64> = rows
            .iter()
            .map(|row| match row {
                PolymorphicConcrete2::C1(a) => a.id,
                PolymorphicConcrete2::C2(v) => v.id,
            })
            .collect();
        assert_eq!(ids, vec![4, 3, 2], "desc order across branches: {ids:?}");
    }
}

#[test]
fn concrete_inheritance_polymorphic_query_works_on_every_available_driver() {
    let cx = Cx::for_testing();
    let ran = run_on_every_driver(&cx, &ConcreteInheritance);
    assert!(
        ran.contains(&"frankensqlite"),
        "FrankenSQLite must be exercised: {ran:?}"
    );
    assert!(ran.contains(&"c-sqlite(memory)"));
}
