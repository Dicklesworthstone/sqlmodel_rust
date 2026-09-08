# Chapter 4: Relationships & Loading

In relational databases, entities are connected by foreign keys. **SQLModel Rust** provides strongly-typed relationship wrappers that model one-to-one, one-to-many, and many-to-many associations while strictly preventing hidden **N+1 query** performance bugs.

---

## Declaring Relationships

Relationships are modeled using three specialized container types:
- `Related<T>`: Represents a to-one relationship loaded alongside the parent.
- `Lazy<T>`: Represents a to-one relationship that can be loaded on demand via a `Session`.
- `RelatedMany<T>`: Represents a to-many relationship holding a collection of related records.

```rust
use serde::{Deserialize, Serialize};
use sqlmodel::prelude::*;
use sqlmodel_core::{Lazy, RelatedMany};

#[derive(Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "teams")]
pub struct Team {
    #[sqlmodel(primary_key, auto_increment)]
    pub id: Option<i64>,
    pub name: String,

    #[sqlmodel(relationship(model = "Player", foreign_key = "team_id", cascade_delete))]
    pub players: RelatedMany<Player>,
}

#[derive(Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "players")]
pub struct Player {
    #[sqlmodel(primary_key, auto_increment)]
    pub id: Option<i64>,

    #[sqlmodel(foreign_key = "teams.id", on_delete = "CASCADE")]
    pub team_id: i64,

    pub name: String,

    #[sqlmodel(relationship(model = "Team", foreign_key = "team_id"))]
    pub team: Lazy<Team>,
}
```

---

## Eager Loading (`EagerLoader`)

When fetching a parent entity, related entities can be loaded in a single query via `EagerLoader`:

```rust,no_run
use sqlmodel::prelude::*;
use sqlmodel::EagerLoader;

# #[derive(Model, Debug, Clone, serde::Serialize, serde::Deserialize)]
# #[sqlmodel(table = "teams")]
# pub struct Team {
#     #[sqlmodel(primary_key)] pub id: Option<i64>,
#     pub name: String,
#     #[sqlmodel(relationship(model = "Player", foreign_key = "team_id"))]
#     pub players: sqlmodel_core::RelatedMany<Player>,
# }
# #[derive(Model, Debug, Clone, serde::Serialize, serde::Deserialize)]
# #[sqlmodel(table = "players")]
# pub struct Player {
#     #[sqlmodel(primary_key)] pub id: Option<i64>,
#     pub team_id: i64,
#     pub name: String,
# }
async fn fetch_teams_with_players(cx: &Cx, conn: &impl Connection) -> Outcome<Vec<Team>, Error> {
    let loader = EagerLoader::new().include("players");

    select!(Team)
        .eager(loader)
        .all_eager(cx, conn)
        .await
}
```

Under the hood:
1. SQLModel builds an optimized `LEFT JOIN` between `teams` and `players`.
2. Emitted columns carry unambiguous aliases (e.g. `players__name`, `players__team_id`).
3. The derive macro generates `Model::hydrate_relationship`, which deduplicates parents and groups child rows into `RelatedMany<Player>` collections automatically.

---

## Explicit Lazy Loading

If a related entity is not eagerly loaded upfront, you can load it on demand using the `Session`:

```rust,no_run
use sqlmodel::prelude::*;
use sqlmodel::Session;

# #[derive(Model, Debug, Clone, serde::Serialize, serde::Deserialize)]
# #[sqlmodel(table = "teams")]
# pub struct Team {
#     #[sqlmodel(primary_key)] pub id: Option<i64>,
#     pub name: String,
# }
# #[derive(Model, Debug, Clone, serde::Serialize, serde::Deserialize)]
# #[sqlmodel(table = "players")]
# pub struct Player {
#     #[sqlmodel(primary_key)] pub id: Option<i64>,
#     pub team_id: i64,
#     pub name: String,
#     #[sqlmodel(relationship(model = "Team", foreign_key = "team_id"))]
#     pub team: sqlmodel_core::Lazy<Team>,
# }
async fn inspect_player_team(
    cx: &Cx,
    session: &mut Session<impl Connection>,
    player: &Player,
) -> Outcome<bool, Error> {
    // Explicitly load the lazy relationship
    session.load_lazy(&player.team, cx).await
}
```

---

## Batch Loading to Eliminate N+1 Queries

When dealing with lists of models, sequentially calling `load_lazy` in a loop produces $O(N)$ database queries. SQLModel provides batch loaders to fetch relations for an entire slice in a single round-trip:

```rust,no_run
use sqlmodel::prelude::*;
use sqlmodel::Session;

# #[derive(Model, Debug, Clone, serde::Serialize, serde::Deserialize)]
# #[sqlmodel(table = "teams")]
# pub struct Team {
#     #[sqlmodel(primary_key)] pub id: Option<i64>,
#     pub name: String,
# }
# #[derive(Model, Debug, Clone, serde::Serialize, serde::Deserialize)]
# #[sqlmodel(table = "players")]
# pub struct Player {
#     #[sqlmodel(primary_key)] pub id: Option<i64>,
#     pub team_id: i64,
#     pub name: String,
#     #[sqlmodel(relationship(model = "Team", foreign_key = "team_id"))]
#     pub team: sqlmodel_core::Lazy<Team>,
# }
async fn load_all_teams(
    cx: &Cx,
    session: &mut Session<impl Connection>,
    players: &[Player],
) -> Outcome<usize, Error> {
    // Collects all unique team_ids and executes:
    // SELECT * FROM teams WHERE id IN (?, ?, ...)
    session.load_many(cx, players, |p| &p.team).await
}
```

---

## Opt-in N+1 Query Detection

To catch accidental loop loading during development, `Session` provides an active N+1 detector:

```rust,no_run
use sqlmodel::prelude::*;
use sqlmodel::Session;

fn configure_debugging(session: &mut Session<impl Connection>) {
    // Set threshold: log warning if same relationship query fires > 5 times
    session.enable_n1_detection(5);
}
```

You can query `session.n1_stats()` at the end of a request cycle to inspect relationship load frequency and identify opportunities for eager or batch loading.

---

## Differences from Python SQLModel

- **No Implicit I/O on Field Access**: Python SQLAlchemy intercepts `player.team` via property descriptors and fires synchronous database queries in the background. In Rust, field access never triggers hidden I/O; loading requires explicit calls to `load_lazy` or `all_eager`.
- **Type-Enforced Loading States**: A relationship's availability is visible in the type system (`Lazy<T>` vs loaded value), preventing uninitialized field panics.
- **Batch Loaders by Default**: While Python developers often reach for libraries like `strawberry` dataloaders to prevent N+1 issues, SQLModel Rust provides `Session::load_many` and `Session::load_one_to_many` out of the box.
- **Built-in N+1 Metrics**: An integrated query frequency detector alerts developers to repeated relationship loads before code reaches production.
