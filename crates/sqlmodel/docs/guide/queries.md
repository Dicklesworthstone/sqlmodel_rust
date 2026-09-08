# Chapter 2: Query Building

SQLModel Rust features a type-safe, chainable query builder that compiles directly into engine-specific SQL. Instead of string concatenation or untyped dictionaries, queries are built using `select!`, `insert!`, `insert_many!`, `update!`, and `delete!` macros combined with `Expr` trees.

---

## SELECT Queries

The `select!` macro creates a query builder targeted at a specific model type:

```rust
use sqlmodel::prelude::*;
use sqlmodel::Dialect;

#[derive(Model, Debug, Clone)]
#[sqlmodel(table = "players")]
pub struct Player {
    #[sqlmodel(primary_key, auto_increment)]
    pub id: Option<i64>,
    pub team_id: i64,
    pub name: String,
    pub score: i32,
    pub active: bool,
}

# fn main() {
let (sql, params) = select!(Player)
    .order_by(Expr::col("score").desc())
    .order_by(Expr::col("id").asc())
    .limit(10)
    .offset(20)
    .build_with_dialect(Dialect::Sqlite);

assert_eq!(
    sql,
    "SELECT * FROM \"players\" ORDER BY \"score\" DESC, \"id\" ASC LIMIT 10 OFFSET 20"
);
assert!(params.is_empty());
# }
```

### Filtering with Expressions (`Expr`)
Filters are attached using `.filter(...)`. Multiple filters are combined with boolean `AND`:

```rust
use sqlmodel::prelude::*;
use sqlmodel::Dialect;

# #[derive(Model, Debug, Clone)]
# #[sqlmodel(table = "players")]
# pub struct Player {
#     #[sqlmodel(primary_key)]
#     pub id: i64,
#     pub team_id: i64,
#     pub name: String,
#     pub score: i32,
# }
# fn main() {
let query = select!(Player)
    .filter(Expr::col("team_id").eq(1i64))
    .filter(Expr::col("score").ge(100i32));

let (sql, params) = query.build_with_dialect(Dialect::Sqlite);
assert_eq!(
    sql,
    "SELECT * FROM \"players\" WHERE \"team_id\" = ?1 AND \"score\" >= ?2"
);
assert_eq!(params, vec![Value::from(1i64), Value::from(100i32)]);
# }
```

Common expression methods on `Expr::col("...")`:
- Equality: `.eq(val)`, `.ne(val)`
- Comparisons: `.gt(val)`, `.ge(val)`, `.lt(val)`, `.le(val)`
- Pattern Matching: `.like("pattern%")`, `.ilike("pattern%")` (Postgres only)
- Range & Inclusion: `.between(low, high)`, `.in_list(vec![...])`
- Nullity: `.is_null()`, `.is_not_null()`
- Logical combinations: `expr1.and(expr2)`, `expr1.or(expr2)`, `expr.not()`

---

## INSERT Operations

Single-model and bulk inserts are constructed using `insert!` and `insert_many!`:

```rust
use sqlmodel::prelude::*;
use sqlmodel::Dialect;

#[derive(Model, Debug, Clone)]
#[sqlmodel(table = "teams")]
pub struct Team {
    #[sqlmodel(primary_key)]
    pub id: i64,
    pub name: String,
}

# fn main() {
let team = Team { id: 1, name: "Crimson".into() };
let (sql, params) = insert!(&team).build_with_dialect(Dialect::Sqlite);

assert_eq!(sql, "INSERT INTO \"teams\" (\"id\", \"name\") VALUES (?1, ?2)");
assert_eq!(params, vec![Value::from(1i64), Value::from("Crimson")]);
# }
```

### Upserts (`ON CONFLICT`)
To perform upserts (insert-or-update), chain `.on_conflict_do_update()` or `.on_conflict_do_nothing()`:

```rust
use sqlmodel::prelude::*;
use sqlmodel::Dialect;

# #[derive(Model, Debug, Clone)]
# #[sqlmodel(table = "teams")]
# pub struct Team {
#     #[sqlmodel(primary_key)]
#     pub id: i64,
#     pub name: String,
# }
# fn main() {
let team = Team { id: 1, name: "Crimson".into() };
let (sql, _) = insert!(&team)
    .on_conflict_do_update(&["name"])
    .build_with_dialect(Dialect::Sqlite);

assert_eq!(
    sql,
    "INSERT INTO \"teams\" (\"id\", \"name\") VALUES (?1, ?2) ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""
);
# }
```

---

## UPDATE & DELETE Operations

Updates can target an existing model instance by primary key, or apply predicates over columns:

```rust
use sqlmodel::prelude::*;
use sqlmodel::Dialect;

#[derive(Model, Debug, Clone)]
#[sqlmodel(table = "teams")]
pub struct Team {
    #[sqlmodel(primary_key)]
    pub id: i64,
    pub name: String,
}

# fn main() {
let team = Team { id: 1, name: "Renamed".into() };

// UPDATE by model instance
let (update_sql, _) = update!(&team).build_with_dialect(Dialect::Sqlite);
assert_eq!(update_sql, "UPDATE \"teams\" SET \"name\" = ?1 WHERE \"id\" = ?2");

// DELETE with filter
let (delete_sql, _) = delete!(Team)
    .filter(Expr::col("id").eq(1))
    .build_with_dialect(Dialect::Sqlite);
assert_eq!(delete_sql, "DELETE FROM \"teams\" WHERE \"id\" = ?1");
# }
```

---

## RETURNING Clause Support

Calling `.returning()` on insert, update, or delete builders instructs the database to return the affected rows:
- On **PostgreSQL** and **SQLite**, `RETURNING *` is appended to the SQL.
- On **MySQL** (which lacks native `RETURNING`), SQLModel transparently snapshots and re-reads the affected rows by primary key inside a transaction.

```rust,no_run
use sqlmodel::prelude::*;

#[derive(Model, Debug, Clone)]
#[sqlmodel(table = "items")]
pub struct Item {
    #[sqlmodel(primary_key, auto_increment)]
    pub id: Option<i64>,
    pub name: String,
}

async fn create_item(cx: &Cx, conn: &impl Connection) -> Outcome<Option<Row>, Error> {
    let item = Item { id: None, name: "Widget".into() };
    insert!(&item).returning().execute_returning(cx, conn).await
}
```

---

## Executing Queries

When executing queries against a live connection, builders provide async execution methods:
- `.all(cx, conn)`: Fetches all matching rows deserialized as models.
- `.first(cx, conn)`: Returns `Option<T>` for the first row, or `None`.
- `.one(cx, conn)`: Expects exactly one row, returning an error if zero or multiple rows match.
- `.execute(cx, conn)`: Executes the statement and returns rows affected count or generated ID.

---

## Differences from Python SQLModel

- **Compile-time Macro DSL**: Python SQLModel uses functions like `select(Hero)` and operator overloads (`Hero.age > 18`). Rust uses macros `select!(Hero)` and explicit expression builders `Expr::col("age").gt(18)`.
- **Dialect Awareness Before Execution**: Query builders can be rendered to dialect SQL strings (`build_with_dialect`) without opening a database connection or binding live drivers.
- **Dialect-Agnostic RETURNING**: Python applications must handle MySQL's lack of `RETURNING` manually; SQLModel Rust unifies `execute_returning` across all engines.
- **Cancel-Aware Async Execution**: All query execution methods take `&Cx` as their first parameter, honoring cooperative cancellation deadlines.
