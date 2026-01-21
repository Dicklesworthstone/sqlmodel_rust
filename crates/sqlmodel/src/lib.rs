//! SQLModel Rust - SQL databases in Rust, designed to be intuitive and type-safe.
//!
//! SQLModel Rust is a Rust port of Python's SQLModel library, providing:
//!
//! - Type-safe database operations with compile-time checks
//! - ORM-style struct mapping with derive macros
//! - Fluent query builder API
//! - Connection pooling with structured concurrency
//! - Migration support
//!
//! # Quick Start
//!
//! ```ignore
//! use sqlmodel::prelude::*;
//!
//! #[derive(Model, Debug)]
//! #[sqlmodel(table = "heroes")]
//! struct Hero {
//!     #[sqlmodel(primary_key, auto_increment)]
//!     id: Option<i64>,
//!     name: String,
//!     secret_name: String,
//!     age: Option<i32>,
//! }
//!
//! async fn main_example(cx: &Cx, conn: &impl Connection) {
//!     // Create a hero
//!     let hero = Hero {
//!         id: None,
//!         name: "Spider-Man".to_string(),
//!         secret_name: "Peter Parker".to_string(),
//!         age: Some(25),
//!     };
//!
//!     // Insert
//!     let id = insert!(hero).execute(cx, conn).await.unwrap();
//!
//!     // Query
//!     let heroes = select!(Hero)
//!         .filter(Expr::col("age").gt(18))
//!         .all(cx, conn)
//!         .await
//!         .unwrap();
//!
//!     // Update
//!     let mut hero = heroes.into_iter().next().unwrap();
//!     hero.age = Some(26);
//!     update!(hero).execute(cx, conn).await.unwrap();
//!
//!     // Delete
//!     delete!(Hero)
//!         .filter(Expr::col("name").eq("Spider-Man"))
//!         .execute(cx, conn)
//!         .await
//!         .unwrap();
//! }
//! ```
//!
//! # Features
//!
//! - **Zero-cost abstractions**: Compile-time code generation, no runtime reflection
//! - **Structured concurrency**: Built on asupersync for cancel-correct operations
//! - **Type safety**: SQL types mapped to Rust types with compile-time checks
//! - **Fluent API**: Chainable query builder methods
//! - **Connection pooling**: Efficient connection reuse
//! - **Migrations**: Version-controlled schema changes

// Re-export all public types from sub-crates
pub use sqlmodel_core::connection::{ConnectionConfig, SslMode, Transaction};
pub use sqlmodel_core::{
    // asupersync re-exports
    Budget,
    // Core types
    Connection,
    Cx,
    Error,
    Field,
    FieldInfo,
    Model,
    Outcome,
    RegionId,
    Result,
    Row,
    SqlType,
    TaskId,
    TypeInfo,
    Value,
};

pub use sqlmodel_macros::{Model, Validate};

pub use sqlmodel_query::{
    BinaryOp, Expr, Join, JoinType, Limit, Offset, OrderBy, QueryBuilder, Select, UnaryOp, Where,
    delete, insert, raw_execute, raw_query, select, update,
};

pub use sqlmodel_schema::{
    CreateTable, Migration, MigrationRunner, MigrationStatus, SchemaBuilder, create_all,
    create_table, drop_table,
};

pub use sqlmodel_pool::{Pool, PoolConfig, PoolStats, PooledConnection};

// Console integration (feature-gated)
#[cfg(feature = "console")]
pub use sqlmodel_console::{
    // Core console types
    ConsoleAware,
    OutputMode,
    SqlModelConsole,
    Theme,
    // Renderables
    renderables::{ErrorPanel, ErrorSeverity, PoolHealth, PoolStatsProvider, PoolStatusDisplay},
};

/// Prelude module for convenient imports.
///
/// ```ignore
/// use sqlmodel::prelude::*;
/// ```
pub mod prelude {
    pub use crate::{
        // asupersync
        Budget,
        // Core traits and types (Model is the trait)
        Connection,
        Cx,
        Error,
        // Query building
        Expr,
        Join,
        JoinType,
        Migration,
        MigrationRunner,
        Model,
        OrderBy,
        Outcome,
        // Pool
        Pool,
        PoolConfig,
        RegionId,
        Result,
        Row,
        Select,
        TaskId,
        Value,
        // Schema
        create_table,
        // Macros
        delete,
        insert,
        select,
        update,
    };
    // Derive macros (re-export only Validate since Model trait conflicts)
    pub use sqlmodel_macros::Validate;

    // Console types when feature enabled
    #[cfg(feature = "console")]
    pub use crate::{
        ConsoleAware, ErrorPanel, ErrorSeverity, OutputMode, PoolHealth, PoolStatsProvider,
        PoolStatusDisplay, SqlModelConsole, Theme,
    };
}
