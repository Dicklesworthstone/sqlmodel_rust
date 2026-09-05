//! Minimal full-stack application used by CI's `size-and-startup` job
//! (`bd-4ttf.2`): it links the facade, the C SQLite driver, the derive
//! macros, and the schema/session/query paths, then does one real
//! round-trip against an in-memory database and prints the selected row as
//! JSON. Exit code 0 means the round-trip worked.
//!
//! Run: `cargo run --release --example minimal_app -p sqlmodel-e2e`

use serde::{Deserialize, Serialize};
use sqlmodel::SchemaBuilder;
use sqlmodel::prelude::*;

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "app_notes")]
struct Note {
    #[sqlmodel(primary_key, auto_increment)]
    id: Option<i64>,
    title: String,
    #[sqlmodel(nullable)]
    body: Option<String>,
}

fn main() {
    let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
        .build()
        .expect("runtime");
    let cx = Cx::for_testing();
    let outcome = runtime.block_on(run(&cx));
    match outcome {
        Outcome::Ok(json) => println!("{json}"),
        Outcome::Err(e) => {
            eprintln!("minimal_app failed: {e:?}");
            std::process::exit(1);
        }
        Outcome::Cancelled(r) => {
            eprintln!("minimal_app cancelled: {r:?}");
            std::process::exit(1);
        }
        Outcome::Panicked(p) => {
            eprintln!("minimal_app panicked: {p:?}");
            std::process::exit(1);
        }
    }
}

async fn run(cx: &Cx) -> Outcome<String, sqlmodel::Error> {
    let conn = sqlmodel_sqlite::SqliteConnection::open_memory().expect("open :memory:");

    let ddl = SchemaBuilder::new()
        .dialect(Dialect::Sqlite)
        .create_table::<Note>()
        .build();
    for stmt in ddl {
        if let Outcome::Err(e) = conn.execute(cx, &stmt, &[]).await {
            return Outcome::Err(e);
        }
    }

    let note = Note {
        id: None,
        title: "hello from the size job".to_owned(),
        body: Some("inserted by minimal_app".to_owned()),
    };
    if let Outcome::Err(e) = insert!(&note).execute(cx, &conn).await {
        return Outcome::Err(e);
    }

    match select!(Note)
        .filter(Expr::col("title").like("hello%"))
        .all(cx, &conn)
        .await
    {
        Outcome::Ok(rows) => match rows.into_iter().next() {
            Some(selected) => match serde_json::to_string_pretty(&selected) {
                Ok(json) => Outcome::Ok(json),
                Err(e) => Outcome::Err(sqlmodel::Error::Custom(e.to_string())),
            },
            None => Outcome::Err(sqlmodel::Error::Custom(
                "inserted note not found by select".to_owned(),
            )),
        },
        Outcome::Err(e) => Outcome::Err(e),
        Outcome::Cancelled(r) => Outcome::Cancelled(r),
        Outcome::Panicked(p) => Outcome::Panicked(p),
    }
}
