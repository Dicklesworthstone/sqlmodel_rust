//! Type-safe SQL query builder for SQLModel Rust.
//!
//! This crate provides a fluent API for building SQL queries
//! with compile-time type safety.

pub mod builder;
pub mod clause;
pub mod cte;
pub mod eager;
pub mod expr;
pub mod join;
pub mod select;
pub mod set_ops;

pub use builder::{
    DeleteBuilder, InsertBuilder, InsertManyBuilder, OnConflict, QueryBuilder, SetClause,
    UpdateBuilder,
};
pub use clause::{Limit, Offset, OrderBy, Where};
pub use cte::{Cte, CteRef, WithQuery};
pub use eager::{EagerLoader, IncludePath};
pub use expr::{
    BinaryOp, Expr, UnaryOp, WindowBuilder, WindowFrame, WindowFrameBound, WindowFrameType,
};
pub use join::{Join, JoinType};
pub use select::Select;
pub use set_ops::{
    SetOpType, SetOperation, except, except_all, intersect, intersect_all, union, union_all,
};

use asupersync::{Cx, Outcome};
use sqlmodel_core::{Connection, Row, Value};

/// Create a SELECT query for a model.
///
/// # Example
///
/// ```ignore
/// let heroes = select!(Hero)
///     .filter(Hero::age.gt(18))
///     .order_by(Hero::name.asc())
///     .all(cx, &conn)
///     .await?;
/// ```
#[macro_export]
macro_rules! select {
    ($model:ty) => {
        $crate::Select::<$model>::new()
    };
}

/// Create an INSERT query for a model.
///
/// # Example
///
/// ```ignore
/// let id = insert!(hero)
///     .execute(cx, &conn)
///     .await?;
/// ```
#[macro_export]
macro_rules! insert {
    ($model:expr) => {
        $crate::builder::InsertBuilder::new($model)
    };
}

/// Create a bulk INSERT query for multiple models.
///
/// # Example
///
/// ```ignore
/// let heroes = vec![hero1, hero2, hero3];
/// let count = insert_many!(heroes)
///     .execute(cx, &conn)
///     .await?;
///
/// // With UPSERT
/// insert_many!(heroes)
///     .on_conflict_do_update(&["name", "age"])
///     .execute(cx, &conn)
///     .await?;
/// ```
#[macro_export]
macro_rules! insert_many {
    ($models:expr) => {
        $crate::builder::InsertManyBuilder::new($models)
    };
}

/// Create an UPDATE query for a model.
///
/// # Example
///
/// ```ignore
/// update!(hero)
///     .execute(cx, &conn)
///     .await?;
/// ```
#[macro_export]
macro_rules! update {
    ($model:expr) => {
        $crate::builder::UpdateBuilder::new($model)
    };
}

/// Create a DELETE query for a model.
///
/// # Example
///
/// ```ignore
/// delete!(Hero)
///     .filter(Hero::age.lt(18))
///     .execute(cx, &conn)
///     .await?;
/// ```
#[macro_export]
macro_rules! delete {
    ($model:ty) => {
        $crate::builder::DeleteBuilder::<$model>::new()
    };
}

/// Raw SQL query execution.
///
/// For queries that can't be expressed with the type-safe builder.
pub async fn raw_query<C: Connection>(
    cx: &Cx,
    conn: &C,
    sql: &str,
    params: &[Value],
) -> Outcome<Vec<Row>, sqlmodel_core::Error> {
    conn.query(cx, sql, params).await
}

/// Raw SQL statement execution.
pub async fn raw_execute<C: Connection>(
    cx: &Cx,
    conn: &C,
    sql: &str,
    params: &[Value],
) -> Outcome<u64, sqlmodel_core::Error> {
    conn.execute(cx, sql, params).await
}
