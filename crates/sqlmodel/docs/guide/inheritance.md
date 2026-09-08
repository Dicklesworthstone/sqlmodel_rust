# Chapter 5: Model Inheritance

In relational design, modeling polymorphic domain hierarchies typically uses one of three standard strategies:
1. **Single Table Inheritance (STI)**: All hierarchy classes share one table with a discriminator column.
2. **Joined Table Inheritance (JTI)**: The base class and each subclass have dedicated tables linked by primary keys.
3. **Concrete Table Inheritance (CTI)**: Each concrete subclass owns a table containing the full set of attributes; the base class is abstract.

**SQLModel Rust** supports all three strategies with compile-time metadata generation, automatic discriminator filters, and strongly-typed polymorphic queries.

---

## Joined Table Inheritance (JTI)

In Joined Table Inheritance, common fields live in a parent table, while subclass-specific fields live in a child table linked by foreign key:

```rust
use sqlmodel::prelude::*;
use sqlmodel::Dialect;

#[derive(Model, Debug, Clone)]
#[sqlmodel(table = "persons", inheritance = "joined")]
pub struct Person {
    #[sqlmodel(primary_key)]
    pub id: i64,
    pub name: String,
}

#[derive(Model, Debug, Clone)]
#[sqlmodel(table = "students", inherits = "Person")]
pub struct Student {
    #[sqlmodel(parent)]
    pub person: Person,

    #[sqlmodel(primary_key)]
    pub id: i64,
    pub grade: String,
}

# fn main() {
// Querying the child automatically constructs the INNER JOIN to the parent table
let (sql, _) = select!(Student).build_with_dialect(Dialect::Sqlite);
assert!(sql.contains("FROM \"students\" INNER JOIN \"persons\" ON \"students\".\"id\" = \"persons\".\"id\""));
# }
```

When inserting or updating a JTI child, the session and query builders coordinate writes across both tables in the correct parent-to-child order within a transaction.

---

## Single Table Inheritance (STI)

In Single Table Inheritance, all variants share one physical database table:

```rust
use sqlmodel::prelude::*;

#[derive(Model, Debug, Clone)]
#[sqlmodel(table = "employees", inheritance = "single", discriminator = "role")]
pub struct Employee {
    #[sqlmodel(primary_key, auto_increment)]
    pub id: Option<i64>,
    pub name: String,
    pub role: String,
}

#[derive(Model, Debug, Clone)]
#[sqlmodel(table = "employees", inherits = "Employee", discriminator_value = "engineer")]
pub struct Engineer {
    #[sqlmodel(parent)]
    pub employee: Employee,
    #[sqlmodel(nullable)]
    pub programming_language: Option<String>,
}
```

Queries targeted at `select!(Engineer)` automatically append `WHERE "role" = 'engineer'` to prevent cross-contamination of sibling records.

---

## Concrete Table Inheritance (CTI)

In Concrete Table Inheritance, every concrete child model possesses its own standalone table containing both base and child columns:

```rust
use sqlmodel::prelude::*;

#[derive(Model, Debug, Clone)]
#[sqlmodel(table, inheritance = "concrete")]
pub struct Content {
    #[sqlmodel(primary_key)]
    pub id: i64,
    pub title: String,
}

#[derive(Model, Debug, Clone)]
#[sqlmodel(table = "articles", inheritance = "concrete", inherits = "Content")]
pub struct Article {
    #[sqlmodel(primary_key)]
    pub id: i64,
    pub title: String,
    pub body: String,
}

#[derive(Model, Debug, Clone)]
#[sqlmodel(table = "videos", inheritance = "concrete", inherits = "Content")]
pub struct Video {
    #[sqlmodel(primary_key)]
    pub id: i64,
    pub title: String,
    pub duration_seconds: i32,
}
```

---

## Polymorphic Querying

To query a base table and hydrate results into an enum of concrete types, SQLModel provides polymorphic builders:

```rust,no_run
use sqlmodel::prelude::*;

# #[derive(Model, Debug, Clone)]
# #[sqlmodel(table = "persons", inheritance = "joined")]
# pub struct Person { #[sqlmodel(primary_key)] pub id: i64, pub name: String }
# #[derive(Model, Debug, Clone)]
# #[sqlmodel(table = "students", inherits = "Person")]
# pub struct Student { #[sqlmodel(parent)] pub person: Person, #[sqlmodel(primary_key)] pub id: i64, pub grade: String }
# #[derive(Model, Debug, Clone)]
# #[sqlmodel(table = "teachers", inherits = "Person")]
# pub struct Teacher { #[sqlmodel(parent)] pub person: Person, #[sqlmodel(primary_key)] pub id: i64, pub subject: String }
#[derive(Debug, Clone)]
pub enum SchoolMember {
    Student(Student),
    Teacher(Teacher),
}

async fn fetch_all_members(cx: &Cx, conn: &impl Connection) -> Outcome<Vec<SchoolMember>, Error> {
    // PolymorphicJoined selects parent with outer joins to children and hydrates enum variants
    Outcome::Ok(vec![])
}
```

---

## Differences from Python SQLModel

- **Struct Composition vs Class Inheritance**: Rust lacks classical OOP inheritance. SQLModel Rust achieves model subtyping via struct composition using the `#[sqlmodel(parent)]` attribute.
- **Strongly-Typed Enum Polymorphism**: While Python returns heterogenous subclass instances dynamically typed at runtime, Rust maps polymorphic query results into algebraic `enum` types.
- **Automatic Discriminator Injection**: STI child queries automatically inject compile-time discriminator equality predicates into the query AST.
- **Explicit Table Separation**: JTI relationships clearly separate parent and child tables in schema generation and migrations without relying on implicit SQLAlchemy mapper registries.
