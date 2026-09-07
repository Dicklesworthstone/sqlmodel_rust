# Chapter 6: Schema & Migrations

**SQLModel Rust** includes a built-in schema generation, introspection, diffing, and migration system. Unlike Python SQLModel—which requires external tooling such as Alembic—SQLModel Rust provides first-class, transactional schema evolution within the library.

---

## Generating DDL with `SchemaBuilder`

To generate DDL statements from your Rust model definitions, use `SchemaBuilder`:

```rust
use sqlmodel::prelude::*;
use sqlmodel::{Dialect, SchemaBuilder};

#[derive(Model, Debug, Clone)]
#[sqlmodel(table = "users")]
pub struct User {
    #[sqlmodel(primary_key, auto_increment)]
    pub id: Option<i64>,

    #[sqlmodel(unique)]
    pub email: String,

    #[sqlmodel(index = "users_name_idx")]
    pub name: String,

    #[sqlmodel(default = "true")]
    pub is_active: bool,
}

# fn main() {
let statements = SchemaBuilder::new()
    .dialect(Dialect::Postgres)
    .create_table::<User>()
    .build();

assert_eq!(statements.len(), 2);
assert!(statements[0].contains("CREATE TABLE IF NOT EXISTS \"users\""));
assert!(statements[1].contains("CREATE INDEX IF NOT EXISTS \"users_name_idx\""));
# }
```

`SchemaBuilder` respects engine-specific syntax:
- Primary key auto-increment: `BIGINT GENERATED ALWAYS AS IDENTITY` on Postgres, `AUTO_INCREMENT` on MySQL, `AUTOINCREMENT` on SQLite.
- Quoting: `"` for PostgreSQL/SQLite, `` ` `` for MySQL.
- Default values, check constraints, foreign keys, and comments.

---

## Programmatic Table Creation

For quick starts and integration tests, you can execute table creation directly against an open connection:

```rust,no_run
use sqlmodel::prelude::*;
use sqlmodel::create_table;

# #[derive(Model, Debug, Clone)]
# #[sqlmodel(table = "users")]
# pub struct User { #[sqlmodel(primary_key)] pub id: i64 }
async fn init_database(cx: &Cx, conn: &impl Connection) -> Outcome<(), Error> {
    let sql = create_table::<User>().dialect(conn.dialect()).build();
    conn.execute(cx, &sql, &[]).await.map(|_| ())
}
```

---

## Database Introspection & Schema Diffing

SQLModel Rust can read an existing database's schema and compute declarative differences against target Rust models:

```rust,ignore
use sqlmodel::prelude::*;
use sqlmodel_schema::introspect::Introspector;
use sqlmodel_schema::diff::schema_diff;
use sqlmodel_schema::SchemaBuilder;

async fn check_drift(cx: &Cx, conn: &impl Connection) -> Outcome<(), Error> {
    // 1. Introspect live database schema
    let current_schema = match Introspector::new(conn.dialect()).introspect_all(cx, conn).await {
        Outcome::Ok(s) => s,
        Outcome::Err(e) => return Outcome::Err(e),
        Outcome::Cancelled(r) => return Outcome::Cancelled(r),
        Outcome::Panicked(p) => return Outcome::Panicked(p),
    };

    // 2. Define desired target schema
    let target_schema = SchemaBuilder::new()
        .dialect(conn.dialect())
        // add models...
        .to_schema();

    // 3. Compute differences
    let diff = schema_diff(&current_schema, &target_schema);
    if !diff.is_empty() {
        println!("Detected {} schema changes", diff.operations().len());
    }

    Outcome::Ok(())
}
```

---

## Running Versioned Migrations (`MigrationRunner`)

The `MigrationRunner` manages the execution of sequential migration files against a live database:

```rust,no_run
use sqlmodel::prelude::*;
use sqlmodel::{Migration, MigrationRunner};

async fn run_migrations(cx: &Cx, conn: &impl Connection) -> Outcome<Vec<String>, Error> {
    let migrations = vec![
        Migration::new(
            "0001_initial_schema",
            "initial schema",
            "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL UNIQUE);",
            "DROP TABLE users;",
        ),
        Migration::new(
            "0002_add_profile",
            "add profile bio",
            "ALTER TABLE users ADD COLUMN bio TEXT;",
            "ALTER TABLE users DROP COLUMN bio;",
        ),
    ];

    let runner = MigrationRunner::new(migrations);
    runner.migrate(cx, conn).await
}
```

### Key Migration Guarantees
1. **Version Tracking**: Records applied migrations in a dedicated `__sqlmodel_migrations` table with version names, timestamps, and cryptographic checksums.
2. **Checksum Drift Detection**: If a migration that was already applied is modified on disk, the runner halts with a checksum mismatch error to prevent accidental corruption.
3. **Transactional DDL**:
   - On **PostgreSQL** and **SQLite**, each migration and its version record are applied within a single transaction. If any statement fails, the entire migration rolls back cleanly.
   - On **MySQL**, where DDL causes implicit commits, statements execute sequentially and failure reports the exact statement that faulted.

---

## Differences from Python SQLModel

- **Integrated Migration Tooling**: Python SQLModel delegates all migrations to Alembic. SQLModel Rust includes built-in DDL generation, live introspection, schema diffing, and migration runners natively.
- **Transactional Atomicity**: Migrations on PostgreSQL and SQLite are wrapped in ACID transactions by default, preventing half-applied schema states.
- **Automated Drift Detection**: Introspection and schema diffing allow continuous verification that Rust models accurately represent the production database.
