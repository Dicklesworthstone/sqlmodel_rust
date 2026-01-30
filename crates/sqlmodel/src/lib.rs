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
    Hybrid,
    // Inheritance types
    InheritanceInfo,
    InheritanceStrategy,
    Model,
    Outcome,
    RegionId,
    Result,
    Row,
    SqlEnum,
    SqlType,
    TaskId,
    TypeInfo,
    Value,
};

pub use sqlmodel_macros::{Model, SqlEnum, Validate};

pub use sqlmodel_query::{
    BinaryOp, Expr, Join, JoinType, Limit, Offset, OrderBy, QueryBuilder, Select, UnaryOp, Where,
    delete, insert, raw_execute, raw_query, select, update,
};

pub use sqlmodel_schema::{
    CreateTable, Migration, MigrationRunner, MigrationStatus, SchemaBuilder, create_all,
    create_table, drop_table,
};

pub use sqlmodel_pool::{
    Pool, PoolConfig, PoolStats, PooledConnection, ReplicaPool, ReplicaStrategy,
};

// Session management
pub mod session;
pub use session::{Session, SessionBuilder};

// Console-enabled session extension trait
#[cfg(feature = "console")]
pub use session::ConnectionBuilderExt;

// Global console support (feature-gated)
#[cfg(feature = "console")]
mod global_console;
#[cfg(feature = "console")]
pub use global_console::{
    global_console, has_global_console, init_auto_console, set_global_console,
    set_global_shared_console,
};

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

// ============================================================================
// Generic Model Support Tests
// ============================================================================
//
// These compile-time tests verify that the Model derive macro correctly handles
// generic type parameters at the parsing and code generation level.
//
// IMPORTANT CONSTRAINTS for Generic Models:
// When using generic type parameters in Model fields, the type must satisfy:
// - Send + Sync (Model trait bounds)
// - Into<Value> / From<Value> conversions (for to_row/from_row)
//
// The easiest patterns for generic models are:
// 1. Use generics only for non-database fields (with #[sqlmodel(skip)])
// 2. Use concrete types for database fields, generics for metadata
// 3. Use PhantomData for type markers

#[cfg(test)]
mod generic_model_tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::marker::PhantomData;

    // Pattern 1: Generic with skipped field
    // The generic type is not stored in the database
    #[derive(Model, Debug, Clone, Serialize, Deserialize)]
    struct TaggedModel<T: Clone + std::fmt::Debug + Send + Sync + Default> {
        #[sqlmodel(primary_key)]
        id: i64,
        name: String,
        #[sqlmodel(skip)]
        _marker: PhantomData<T>,
    }

    // Pattern 2: Concrete model with generic metadata
    // Database fields are concrete, generic is for compile-time type safety
    #[derive(Model, Debug, Clone, Serialize, Deserialize)]
    struct TypedResponse<T: Send + Sync> {
        #[sqlmodel(primary_key)]
        id: i64,
        status_code: i32,
        body: String,
        #[sqlmodel(skip)]
        _type: PhantomData<T>,
    }

    // Test marker types for TypedResponse
    #[derive(Debug, Clone, Default)]
    struct UserData;

    #[derive(Debug, Clone, Default)]
    struct OrderData;

    #[test]
    fn test_generic_model_with_phantom_data() {
        // TaggedModel compiles and works with any marker type
        let model: TaggedModel<UserData> = TaggedModel {
            id: 1,
            name: "test".to_string(),
            _marker: PhantomData,
        };
        assert_eq!(model.id, 1);
        assert_eq!(model.name, "test");
    }

    #[test]
    fn test_generic_model_fields() {
        // Verify TaggedModel has correct fields (skip fields are excluded from to_row)
        let fields = <TaggedModel<UserData> as Model>::fields();
        // _marker is skipped, so only id and name
        assert_eq!(fields.len(), 2);
        assert!(fields.iter().any(|f| f.name == "id"));
        assert!(fields.iter().any(|f| f.name == "name"));
    }

    #[test]
    fn test_generic_model_table_name() {
        // Table name should be derived from struct name (pluralized by default)
        assert_eq!(<TaggedModel<UserData> as Model>::TABLE_NAME, "tagged_models");
        assert_eq!(<TypedResponse<UserData> as Model>::TABLE_NAME, "typed_responses");
    }

    #[test]
    fn test_generic_model_primary_key() {
        assert_eq!(
            <TaggedModel<UserData> as Model>::PRIMARY_KEY,
            &["id"]
        );
    }

    #[test]
    fn test_generic_model_type_safety() {
        // Different type parameters create distinct types at compile time
        let user_response: TypedResponse<UserData> = TypedResponse {
            id: 1,
            status_code: 200,
            body: r#"{"name": "Alice"}"#.to_string(),
            _type: PhantomData,
        };

        let order_response: TypedResponse<OrderData> = TypedResponse {
            id: 2,
            status_code: 201,
            body: r#"{"order_id": 123}"#.to_string(),
            _type: PhantomData,
        };

        // These are different types - can't accidentally mix them
        assert_eq!(user_response.id, 1);
        assert_eq!(order_response.id, 2);
    }

    #[test]
    fn test_generic_model_to_row() {
        let model: TaggedModel<UserData> = TaggedModel {
            id: 1,
            name: "test".to_string(),
            _marker: PhantomData,
        };
        let row = model.to_row();
        // Only non-skipped fields
        assert_eq!(row.len(), 2);
        assert!(row.iter().any(|(name, _)| *name == "id"));
        assert!(row.iter().any(|(name, _)| *name == "name"));
    }

    #[test]
    fn test_generic_model_primary_key_value() {
        let model: TaggedModel<UserData> = TaggedModel {
            id: 42,
            name: "test".to_string(),
            _marker: PhantomData,
        };
        let pk = model.primary_key_value();
        assert_eq!(pk.len(), 1);
        assert_eq!(pk[0], Value::BigInt(42));
    }

    #[test]
    fn test_generic_model_is_new() {
        let new_model: TaggedModel<UserData> = TaggedModel {
            id: 0,
            name: "new".to_string(),
            _marker: PhantomData,
        };
        // Note: is_new() depends on the implementation - typically checks if pk is default
        let _ = new_model.is_new(); // Just verify it compiles
    }
}

// ============================================================================
// Table Inheritance Integration Tests
// ============================================================================
//
// These tests verify that the Model derive macro correctly handles table
// inheritance attributes and generates the appropriate inheritance() method.

#[cfg(test)]
mod inheritance_tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    // InheritanceStrategy is re-exported from crate root
    use crate::InheritanceStrategy;

    // Single table inheritance base model with discriminator column
    #[derive(Model, Debug, Clone, Serialize, Deserialize)]
    #[sqlmodel(table, inheritance = "single", discriminator = "type_")]
    struct Employee {
        #[sqlmodel(primary_key)]
        id: i64,
        name: String,
        type_: String,
    }

    // Single table inheritance child model
    #[derive(Model, Debug, Clone, Serialize, Deserialize)]
    #[sqlmodel(inherits = "Employee", discriminator_value = "manager")]
    struct Manager {
        #[sqlmodel(primary_key)]
        id: i64,
        department: String,
    }

    // Joined table inheritance base model
    #[derive(Model, Debug, Clone, Serialize, Deserialize)]
    #[sqlmodel(table, inheritance = "joined")]
    struct Person {
        #[sqlmodel(primary_key)]
        id: i64,
        name: String,
    }

    // Joined table inheritance child model
    #[derive(Model, Debug, Clone, Serialize, Deserialize)]
    #[sqlmodel(table, inherits = "Person")]
    struct Student {
        #[sqlmodel(primary_key)]
        student_id: i64,
        grade: String,
    }

    // Concrete table inheritance base model
    #[derive(Model, Debug, Clone, Serialize, Deserialize)]
    #[sqlmodel(table, inheritance = "concrete")]
    struct BaseEntity {
        #[sqlmodel(primary_key)]
        id: i64,
        created_at: i64,
    }

    // Normal model without inheritance
    #[derive(Model, Debug, Clone, Serialize, Deserialize)]
    #[sqlmodel(table)]
    struct NormalModel {
        #[sqlmodel(primary_key)]
        id: i64,
        data: String,
    }

    #[test]
    fn test_single_table_inheritance_base() {
        let info = <Employee as Model>::inheritance();
        assert_eq!(info.strategy, InheritanceStrategy::Single);
        assert!(info.parent.is_none());
        assert_eq!(info.discriminator_column, Some("type_"));
        assert!(info.discriminator_value.is_none());
        assert!(info.is_base());
        assert!(!info.is_child());
    }

    #[test]
    fn test_single_table_inheritance_child() {
        let info = <Manager as Model>::inheritance();
        assert_eq!(info.parent, Some("Employee"));
        assert_eq!(info.discriminator_value, Some("manager"));
        assert!(info.is_child());
        assert!(!info.is_base());
    }

    #[test]
    fn test_joined_table_inheritance_base() {
        let info = <Person as Model>::inheritance();
        assert_eq!(info.strategy, InheritanceStrategy::Joined);
        assert!(info.parent.is_none());
        assert!(info.is_base());
    }

    #[test]
    fn test_joined_table_inheritance_child() {
        let info = <Student as Model>::inheritance();
        assert_eq!(info.parent, Some("Person"));
        assert!(info.is_child());
    }

    #[test]
    fn test_concrete_table_inheritance_base() {
        let info = <BaseEntity as Model>::inheritance();
        assert_eq!(info.strategy, InheritanceStrategy::Concrete);
        assert!(info.parent.is_none());
        assert!(info.is_base());
    }

    #[test]
    fn test_no_inheritance() {
        let info = <NormalModel as Model>::inheritance();
        assert_eq!(info.strategy, InheritanceStrategy::None);
        assert!(info.parent.is_none());
        assert!(info.discriminator_value.is_none());
        assert!(!info.is_base());
        assert!(!info.is_child());
    }

    #[test]
    fn test_inheritance_strategy_methods() {
        // Single table uses discriminator
        let single = <Employee as Model>::inheritance();
        assert!(single.strategy.uses_discriminator());
        assert!(!single.strategy.requires_join());

        // Joined table requires join
        let joined = <Person as Model>::inheritance();
        assert!(!joined.strategy.uses_discriminator());
        assert!(joined.strategy.requires_join());

        // Concrete table neither
        let concrete = <BaseEntity as Model>::inheritance();
        assert!(!concrete.strategy.uses_discriminator());
        assert!(!concrete.strategy.requires_join());
    }
}

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
        Hybrid,
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
        // Session
        Session,
        SessionBuilder,
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
    // Derive macros (re-export only Validate/SqlEnum since Model trait conflicts)
    pub use sqlmodel_macros::{SqlEnum, Validate};

    // Console types when feature enabled
    #[cfg(feature = "console")]
    pub use crate::{
        // Types and traits
        ConnectionBuilderExt,
        ConsoleAware,
        ErrorPanel,
        ErrorSeverity,
        OutputMode,
        PoolHealth,
        PoolStatsProvider,
        PoolStatusDisplay,
        SqlModelConsole,
        Theme,
        // Global console functions
        global_console,
        has_global_console,
        init_auto_console,
        set_global_console,
        set_global_shared_console,
    };
}
