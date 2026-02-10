# EXISTING_SQLMODEL_STRUCTURE.md - The Behavioral Specification

This document extracts the complete behavioral specification from Python SQLModel, serving as the authoritative reference for the Rust implementation.

**Source:** Python SQLModel library (built on Pydantic + SQLAlchemy)

---

## 1. Core Architecture

### 1.1 Class Hierarchy (Python)

```
SQLModel
├── inherits: pydantic.BaseModel (validation, serialization)
├── inherits: sqlalchemy.DeclarativeMeta (ORM mapping)
└── uses: SQLModelMetaclass (coordinates both)
```

### 1.2 Rust Equivalent

```
#[derive(Model)] // generates Model trait impl
├── to_row() / from_row() - ORM mapping
├── serde traits - serialization
└── compile-time type checking - replaces runtime validation
```

---

## 2. Field() Function - Complete API

### 2.1 Database Schema Options

| Python Parameter | Type | Default | Rust Attribute |
|------------------|------|---------|----------------|
| `primary_key` | bool | False | `#[sqlmodel(primary_key)]` |
| `foreign_key` | str | Undefined | `#[sqlmodel(foreign_key = "table.col")]` |
| `ondelete` | "CASCADE"\|"SET NULL"\|"RESTRICT" | Undefined | `#[sqlmodel(on_delete = "CASCADE")]` |
| `unique` | bool | False | `#[sqlmodel(unique)]` |
| `nullable` | bool | (derived) | `#[sqlmodel(nullable)]` |
| `index` | bool | False | `#[sqlmodel(index)]` |
| `sa_type` | type | (inferred) | `#[sqlmodel(sql_type = "VARCHAR(255)")]` |
| `sa_column` | Column | Undefined | (use explicit attributes instead) |
| `default` | Any | Undefined | `#[sqlmodel(default = "expr")]` |
| `default_factory` | Callable | None | (use Default trait) |

### 2.2 Validation Options (Pydantic)

| Python Parameter | Type | Default | Rust Equivalent |
|------------------|------|---------|-----------------|
| `gt` | float | None | Custom validation |
| `ge` | float | None | Custom validation |
| `lt` | float | None | Custom validation |
| `le` | float | None | Custom validation |
| `multiple_of` | float | None | Custom validation |
| `min_length` | int | None | Custom validation |
| `max_length` | int | None | `#[sqlmodel(max_length = N)]` |
| `regex` | str | None | Custom validation |
| `max_digits` | int | None | For Decimal types |
| `decimal_places` | int | None | For Decimal types |

### 2.3 Serialization Options

| Python Parameter | Type | Default | Rust Equivalent |
|------------------|------|---------|-----------------|
| `alias` | str | None | `#[serde(rename = "...")]` |
| `validation_alias` | str | None | (not needed - compile-time) |
| `serialization_alias` | str | None | `#[serde(rename = "...")]` |
| `exclude` | bool | False | `#[serde(skip)]` |
| `include` | bool | True | default |
| `repr` | bool | True | `#[derive(Debug)]` |
| `title` | str | None | (documentation only) |
| `description` | str | None | (documentation only) |

---

## 3. Relationship() Function - Complete API

### 3.1 Parameters

| Python Parameter | Type | Default | Description |
|------------------|------|---------|-------------|
| `back_populates` | str | None | Bidirectional relationship field name |
| `cascade_delete` | bool | False | Delete related when parent deleted |
| `passive_deletes` | bool\|"all" | False | Let DB handle cascades |
| `link_model` | type | None | Association table for many-to-many |
| `sa_relationship` | RelationshipProperty | None | Direct SQLAlchemy relationship |
| `sa_relationship_args` | Sequence | None | Positional args for relationship() |
| `sa_relationship_kwargs` | Mapping | None | Keyword args for relationship() |

### 3.2 Relationship Patterns

**One-to-Many:**
```python
# Parent side
class Team(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True)
    heroes: List["Hero"] = Relationship(back_populates="team")

# Child side
class Hero(SQLModel, table=True):
    team_id: Optional[int] = Field(foreign_key="team.id")
    team: Optional[Team] = Relationship(back_populates="heroes")
```

**Many-to-Many:**
```python
class HeroTeamLink(SQLModel, table=True):
    hero_id: int = Field(foreign_key="hero.id", primary_key=True)
    team_id: int = Field(foreign_key="team.id", primary_key=True)

class Hero(SQLModel, table=True):
    teams: List["Team"] = Relationship(back_populates="heroes", link_model=HeroTeamLink)
```

---

## 4. Type Mappings

### 4.1 Python Type → SQL Type

| Python Type | SQLAlchemy Type | MySQL | PostgreSQL | SQLite |
|-------------|-----------------|-------|------------|--------|
| `str` | `AutoString` | `VARCHAR` | `VARCHAR` | `TEXT` |
| `int` | `Integer` | `INT` | `INTEGER` | `INTEGER` |
| `float` | `Float` | `FLOAT` | `REAL` | `REAL` |
| `bool` | `Boolean` | `TINYINT(1)` | `BOOLEAN` | `INTEGER` |
| `bytes` | `LargeBinary` | `BLOB` | `BYTEA` | `BLOB` |
| `datetime` | `DateTime` | `DATETIME` | `TIMESTAMP` | `TEXT` |
| `date` | `Date` | `DATE` | `DATE` | `TEXT` |
| `time` | `Time` | `TIME` | `TIME` | `TEXT` |
| `timedelta` | `Interval` | `BIGINT` | `INTERVAL` | `TEXT` |
| `Decimal` | `Numeric` | `DECIMAL(p,s)` | `NUMERIC(p,s)` | `TEXT` |
| `uuid.UUID` | `Uuid` | `CHAR(36)` | `UUID` | `TEXT` |
| `Enum` | `Enum` | `ENUM(...)` | `ENUM(...)` | `TEXT` |
| `Path` | `AutoString` | `VARCHAR` | `VARCHAR` | `TEXT` |
| `IPv4Address` | `AutoString` | `VARCHAR` | `INET` | `TEXT` |
| `IPv6Address` | `AutoString` | `VARCHAR` | `INET` | `TEXT` |
| `EmailStr` | `AutoString` | `VARCHAR` | `VARCHAR` | `TEXT` |

### 4.2 Rust Type → SQL Type (Our Mapping)

| Rust Type | SQL Type | MySQL | PostgreSQL | SQLite |
|-----------|----------|-------|------------|--------|
| `String` | `VARCHAR` | `VARCHAR(255)` | `VARCHAR` | `TEXT` |
| `i8` | `TINYINT` | `TINYINT` | `SMALLINT` | `INTEGER` |
| `i16` | `SMALLINT` | `SMALLINT` | `SMALLINT` | `INTEGER` |
| `i32` | `INTEGER` | `INT` | `INTEGER` | `INTEGER` |
| `i64` | `BIGINT` | `BIGINT` | `BIGINT` | `INTEGER` |
| `f32` | `FLOAT` | `FLOAT` | `REAL` | `REAL` |
| `f64` | `DOUBLE` | `DOUBLE` | `DOUBLE PRECISION` | `REAL` |
| `bool` | `BOOLEAN` | `TINYINT(1)` | `BOOLEAN` | `INTEGER` |
| `Vec<u8>` | `BLOB` | `BLOB` | `BYTEA` | `BLOB` |
| `chrono::DateTime` | `TIMESTAMP` | `DATETIME` | `TIMESTAMP` | `TEXT` |
| `chrono::NaiveDate` | `DATE` | `DATE` | `DATE` | `TEXT` |
| `uuid::Uuid` | `UUID` | `CHAR(36)` | `UUID` | `TEXT` |
| `Option<T>` | (nullable) | - | - | - |

---

## 5. Session Operations

### 5.1 Sync Session API

```python
from sqlmodel import Session, select

with Session(engine) as session:
    # Create
    hero = Hero(name="Spider-Man", secret_name="Peter Parker")
    session.add(hero)
    session.commit()
    session.refresh(hero)  # Get DB-generated fields

    # Read
    statement = select(Hero).where(Hero.name == "Spider-Man")
    heroes = session.exec(statement).all()
    hero = session.exec(statement).first()
    hero = session.exec(statement).one()  # Raises if not exactly one

    # Update
    hero.age = 26
    session.add(hero)
    session.commit()

    # Delete
    session.delete(hero)
    session.commit()

    # Bulk operations
    session.add_all([hero1, hero2, hero3])
    session.commit()
```

### 5.2 Async Session API

```python
from sqlmodel.ext.asyncio.session import AsyncSession

async with AsyncSession(engine) as session:
    hero = Hero(name="Spider-Man")
    session.add(hero)
    await session.commit()
    await session.refresh(hero)

    statement = select(Hero)
    result = await session.exec(statement)
    heroes = result.all()
```

### 5.3 Rust Equivalent

```rust
// Using explicit connection + query builder
async fn example(cx: &Cx, conn: &impl Connection) -> Outcome<(), Error> {
    // Create
    let hero = Hero { id: None, name: "Spider-Man".into(), ... };
    let id = insert!(hero).execute(cx, conn).await?;

    // Read
    let heroes = select!(Hero)
        .filter(Expr::col("name").eq("Spider-Man"))
        .all(cx, conn)
        .await?;

    // Update
    update!(hero).execute(cx, conn).await?;

    // Delete
    delete!(Hero).filter(Expr::col("id").eq(1)).execute(cx, conn).await?;

    Outcome::Ok(())
}
```

---

## 6. Query Building

### 6.1 SELECT Statements

```python
# Basic select
select(Hero)

# With columns
select(Hero.name, Hero.age)

# With filter (WHERE)
select(Hero).where(Hero.name == "Spider-Man")
select(Hero).where(Hero.age > 18)
select(Hero).where(Hero.name.contains("man"))
select(Hero).where(Hero.name.in_(["Spider-Man", "Batman"]))

# With multiple conditions
select(Hero).where(Hero.age > 18).where(Hero.team_id == 1)
select(Hero).where(Hero.age > 18, Hero.team_id == 1)  # AND
select(Hero).where(or_(Hero.age < 18, Hero.age > 65))  # OR

# With ordering
select(Hero).order_by(Hero.name)
select(Hero).order_by(Hero.age.desc())
select(Hero).order_by(Hero.name, Hero.age.desc())

# With pagination
select(Hero).offset(10).limit(20)

# With joins
select(Hero, Team).join(Team).where(Team.name == "Avengers")
select(Hero, Team).join(Team, Hero.team_id == Team.id)

# With aggregation
select(func.count(Hero.id))
select(Hero.team_id, func.count(Hero.id)).group_by(Hero.team_id)
select(Hero.team_id, func.count(Hero.id)).group_by(Hero.team_id).having(func.count(Hero.id) > 5)

# DISTINCT
select(Hero.name).distinct()

# FOR UPDATE (row locking)
select(Hero).with_for_update()
```

### 6.2 INSERT Statements

```python
# Basic insert (via session.add)
session.add(Hero(name="Spider-Man"))

# Bulk insert
session.add_all([hero1, hero2, hero3])

# Insert with returning (PostgreSQL)
# SQLModel uses SQLAlchemy's insert().returning()
```

### 6.3 UPDATE Statements

```python
# Via session (ORM style)
hero.age = 26
session.add(hero)
session.commit()

# Bulk update
statement = update(Hero).where(Hero.team_id == 1).values(active=False)
session.exec(statement)
```

### 6.4 DELETE Statements

```python
# Via session
session.delete(hero)
session.commit()

# Bulk delete
statement = delete(Hero).where(Hero.age < 18)
session.exec(statement)
```

---

## 7. Transaction Management

### 7.1 Implicit Transactions

```python
with Session(engine) as session:
    session.add(hero)
    session.commit()  # Commits transaction
    # Automatically rolls back on exception
```

### 7.2 Explicit Transactions

```python
with Session(engine) as session:
    session.begin()  # Start transaction
    try:
        session.add(hero1)
        session.add(hero2)
        session.commit()
    except:
        session.rollback()
        raise
```

### 7.3 Nested Transactions (Savepoints)

```python
with Session(engine) as session:
    session.add(hero1)
    with session.begin_nested():  # Savepoint
        session.add(hero2)
        # Can rollback to savepoint without losing hero1
    session.commit()
```

### 7.4 Isolation Levels

```python
from sqlalchemy import create_engine
engine = create_engine(url, isolation_level="SERIALIZABLE")
# Options: READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE
```

---

## 8. Validation (From Pydantic)

### 8.1 Field Validators

```python
from pydantic import field_validator

class Hero(SQLModel, table=True):
    name: str
    age: int

    @field_validator('age')
    @classmethod
    def age_must_be_positive(cls, v):
        if v < 0:
            raise ValueError('Age must be positive')
        return v
```

### 8.2 Model Validators

```python
from pydantic import model_validator

class Hero(SQLModel, table=True):
    name: str
    secret_name: str

    @model_validator(mode='after')
    def names_must_differ(self):
        if self.name == self.secret_name:
            raise ValueError('name and secret_name must be different')
        return self
```

### 8.3 Rust Equivalent

The `#[derive(Validate)]` macro generates validation methods:

```rust
#[derive(Model, Validate)]
struct Hero {
    #[validate(min_length = 1, max_length = 100)]
    name: String,

    #[validate(range(min = 0, max = 150))]
    age: i32,
}

impl Hero {
    fn validate(&self) -> Result<(), ValidationError> {
        // Generated validation code
    }
}
```

---

## 9. Serialization (From Pydantic)

### 9.1 model_dump() / model_dump_json()

```python
hero = Hero(name="Spider-Man", age=25)

# To dict
data = hero.model_dump()
# {'name': 'Spider-Man', 'age': 25}

# To JSON
json_str = hero.model_dump_json()
# '{"name": "Spider-Man", "age": 25}'

# With options
data = hero.model_dump(
    include={'name'},  # Only these fields
    exclude={'secret_name'},  # Skip these fields
    exclude_none=True,  # Skip None values
    exclude_unset=True,  # Skip unset fields
    by_alias=True,  # Use serialization aliases
)
```

### 9.2 model_validate()

```python
# From dict
hero = Hero.model_validate({'name': 'Spider-Man', 'age': 25})

# From JSON
hero = Hero.model_validate_json('{"name": "Spider-Man", "age": 25}')

# From ORM object (other SQLAlchemy model)
hero = Hero.model_validate(orm_obj, from_attributes=True)
```

### 9.3 Rust Equivalent (Serde)

```rust
use serde::{Serialize, Deserialize};

#[derive(Model, Serialize, Deserialize)]
struct Hero {
    name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    secret_name: Option<String>,

    #[serde(rename = "heroAge")]
    age: i32,
}

// Serialize
let json = serde_json::to_string(&hero)?;

// Deserialize
let hero: Hero = serde_json::from_str(&json)?;
```

SQLModel Rust also provides model-aware helpers that mirror Pydantic:

```rust
use sqlmodel::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Model, Serialize, Deserialize)]
struct Hero {
    name: String,
    #[serde(default)]
    age: i32,
}

// Alias-aware validation (accepts validation_alias/alias keys)
let hero = Hero::sql_model_validate(r#"{"name": "Spider-Man"}"#, ValidateOptions::default())?;

// Model-aware dump (aliases/computed/defaults)
let dumped = hero.sql_model_dump(DumpOptions::default())?;

// Pydantic-compatible exclude_unset requires fields-set tracking
let tracked = Hero::sql_model_validate_tracked(
    r#"{"name": "Spider-Man"}"#,
    ValidateOptions::default(),
)?;
let dumped = tracked.sql_model_dump(DumpOptions::default().exclude_unset())?;
```

---

## 10. Advanced Features

### 10.1 Table Configuration

```python
class Hero(SQLModel, table=True):
    __tablename__ = "heroes"  # Override default (class name lowercase)
    __table_args__ = (
        UniqueConstraint('name', 'team_id', name='unique_hero_per_team'),
        Index('ix_hero_name', 'name'),
        {'schema': 'public'},  # Table schema
    )
```

### 10.2 Abstract Models

```python
class BaseModel(SQLModel):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None

class Hero(BaseModel, table=True):
    name: str
```

### 10.3 Model Update Helper

```python
# SQLModel-specific method
hero.sqlmodel_update({"name": "New Name", "age": 30})
hero.sqlmodel_update(HeroUpdate(name="New Name"))
```

---

## 11. Error Handling

### 11.1 Python Exceptions

| Exception | When Raised |
|-----------|-------------|
| `ValidationError` | Field validation fails |
| `IntegrityError` | Constraint violation (unique, foreign key) |
| `NoResultFound` | `.one()` with no results |
| `MultipleResultsFound` | `.one()` with multiple results |
| `OperationalError` | Connection/DB errors |
| `ProgrammingError` | SQL syntax errors |

### 11.2 Rust Error Hierarchy

```rust
pub enum Error {
    Connection(ConnectionError),
    Query(QueryError),
    Type(TypeError),
    Transaction(TransactionError),
    Protocol(ProtocolError),
    Pool(PoolError),
    Schema(SchemaError),
    Config(ConfigError),
    Validation(ValidationError),
}
```

---

## 12. Explicit Exclusions

The following Python SQLModel features are **intentionally NOT ported** to Rust:

### 12.1 Runtime Type Introspection
- Python uses `__annotations__`, `get_type_hints()` at runtime
- Rust uses proc macros for compile-time code generation

### 12.2 Lazy Loading Relationships
- Python SQLAlchemy supports automatic lazy loading
- Rust requires explicit eager loading with JOINs

### 12.3 Pydantic BaseModel Features
- `__init_subclass__`
- `__get_validators__`
- `__modify_schema__`
- `__private_attributes__`
- These are replaced by Rust's type system and serde

### 12.4 SQLAlchemy Session Patterns
- Unit of Work pattern
- Identity Map
- Automatic dirty tracking
- Rust uses explicit operations

### 12.5 Generic Models
- `SQLModel[T]` is not supported
- Use concrete types or trait bounds

### 12.6 Computed Fields
- `@computed_field` decorator
- Use regular methods instead

### 12.7 Discriminated Unions
- Complex inheritance patterns
- Use enums or composition

### 12.8 Field Aliases for DB
- `sa_column_args`, `sa_column_kwargs`
- Use explicit attributes instead

---

## 13. Feature Mapping Summary

| Python SQLModel Feature | Rust SQLModel Status | Notes |
|------------------------|---------------------|-------|
| `class Hero(SQLModel, table=True)` | ✅ `#[derive(Model)]` | Complete |
| `Field(primary_key=True)` | ✅ `#[sqlmodel(primary_key)]` | Complete |
| `Field(foreign_key="...")` | ✅ `#[sqlmodel(foreign_key = "...")]` | Complete |
| `Field(unique=True)` | ✅ `#[sqlmodel(unique)]` | Complete |
| `Field(index=True)` | ✅ `#[sqlmodel(index)]` | Complete |
| `Field(nullable=True)` | ✅ `#[sqlmodel(nullable)]` | Via Option<T> |
| `Field(default=...)` | ✅ `#[sqlmodel(default = "...")]` | Complete |
| `Relationship()` | ✅ Implemented (different API) | `Related<T>`, `RelatedMany<T>`, `Lazy<T>` + `#[sqlmodel(relationship(...))]` metadata + `Session::{load_lazy,load_many,load_many_to_many,flush_related_many}` |
| `select(Model)` | ✅ `select!(Model)` | Complete |
| `.where()` | ✅ `.filter()` | Complete |
| `.order_by()` | ✅ `.order_by()` | Complete |
| `.limit()/.offset()` | ✅ `.limit()/.offset()` | Complete |
| `.join()` | ✅ `.join()` | Complete |
| `.group_by()/.having()` | ✅ `.group_by()/.having()` | Complete |
| `session.add()` | ✅ `insert!(model)` | Complete |
| `session.delete()` | ✅ `delete!(Model)` | Complete |
| `session.commit()` | ✅ `conn.commit()` | Explicit |
| `session.begin()` | ✅ `conn.begin()` | Complete |
| `session.rollback()` | ✅ `conn.rollback()` | Complete |
| Nested transactions | ✅ Savepoints | Complete |
| `@field_validator` | ✅ `#[derive(Validate)]` | Implemented |
| `@model_validator` | ✅ `#[derive(Validate)]` | Implemented |
| `model_dump()` | ✅ `ModelDump` / `SqlModelDump` | `DumpOptions` support (including tracked `exclude_unset`) |
| `model_validate()` | ✅ `ModelValidate` / `SqlModelValidate` | Alias-aware validation helpers |
| Connection pooling | ✅ `sqlmodel-pool` | Complete |
| CREATE TABLE | ✅ `create_table::<M>()` | Complete |
| Migrations | ⚠️ Basic support | No auto-generation |

---

## 14. Reference Implementation Locations

### Python SQLModel Source Files

| Feature | File |
|---------|------|
| SQLModel class | `sqlmodel/main.py` |
| Field() function | `sqlmodel/main.py:333-426` |
| Relationship() | `sqlmodel/main.py:452-471` |
| Session | `sqlmodel/orm/session.py` |
| AsyncSession | `sqlmodel/ext/asyncio/session.py` |
| select() | `sqlmodel/sql/expression.py` |
| Type mappings | `sqlmodel/main.py:638-689` |

### Rust SQLModel Source Files

| Feature | File |
|---------|------|
| Model trait | `crates/sqlmodel-core/src/lib.rs` |
| Model macro | `crates/sqlmodel-macros/src/lib.rs` |
| Query builder | `crates/sqlmodel-query/src/builder.rs` |
| Expressions | `crates/sqlmodel-query/src/expr.rs` |
| Schema DDL | `crates/sqlmodel-schema/src/create.rs` |
| Relationships | `crates/sqlmodel-core/src/relationship.rs` |
| Session (UoW) | `crates/sqlmodel-session/src/lib.rs` |
| MySQL driver | `crates/sqlmodel-mysql/src/async_connection.rs` |
| SQLite driver | `crates/sqlmodel-sqlite/src/lib.rs` |
| Pool | `crates/sqlmodel-pool/src/lib.rs` |
