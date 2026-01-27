//! Model trait for ORM-style struct mapping.
//!
//! The `Model` trait defines the contract for structs that can be
//! mapped to database tables. It is typically derived using the
//! `#[derive(Model)]` macro from `sqlmodel-macros`.

use crate::Result;
use crate::field::FieldInfo;
use crate::relationship::RelationshipInfo;
use crate::row::Row;
use crate::value::Value;

/// Trait for types that can be mapped to database tables.
///
/// This trait provides metadata about the table structure and
/// methods for converting between Rust structs and database rows.
///
/// # Example
///
/// ```ignore
/// use sqlmodel::Model;
///
/// #[derive(Model)]
/// #[sqlmodel(table = "heroes")]
/// struct Hero {
///     #[sqlmodel(primary_key)]
///     id: Option<i64>,
///     name: String,
///     secret_name: String,
///     age: Option<i32>,
/// }
/// ```
pub trait Model: Sized + Send + Sync {
    /// The name of the database table.
    const TABLE_NAME: &'static str;

    /// The primary key column name(s).
    const PRIMARY_KEY: &'static [&'static str];

    /// Relationship metadata for this model.
    ///
    /// The derive macro will populate this for relationship fields; models with
    /// no relationships can rely on the default empty slice.
    const RELATIONSHIPS: &'static [RelationshipInfo] = &[];

    /// Get field metadata for all columns.
    fn fields() -> &'static [FieldInfo];

    /// Convert this model instance to a row of values.
    fn to_row(&self) -> Vec<(&'static str, Value)>;

    /// Construct a model instance from a database row.
    #[allow(clippy::result_large_err)]
    fn from_row(row: &Row) -> Result<Self>;

    /// Get the value of the primary key field(s).
    fn primary_key_value(&self) -> Vec<Value>;

    /// Check if this is a new record (primary key is None/default).
    fn is_new(&self) -> bool;
}

/// Marker trait for models that support automatic ID generation.
pub trait AutoIncrement: Model {
    /// Set the auto-generated ID after insert.
    fn set_id(&mut self, id: i64);
}

/// Trait for models that track creation/update timestamps.
pub trait Timestamps: Model {
    /// Set the created_at timestamp.
    fn set_created_at(&mut self, timestamp: i64);

    /// Set the updated_at timestamp.
    fn set_updated_at(&mut self, timestamp: i64);
}

/// Trait for soft-deletable models.
pub trait SoftDelete: Model {
    /// Mark the model as deleted.
    fn mark_deleted(&mut self);

    /// Check if the model is deleted.
    fn is_deleted(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FieldInfo, Row, SqlType, Value};

    #[derive(Debug)]
    struct TestModel;

    impl Model for TestModel {
        const TABLE_NAME: &'static str = "test_models";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] =
                &[FieldInfo::new("id", "id", SqlType::Integer).primary_key(true)];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> Result<Self> {
            Ok(Self)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![Value::from(1_i64)]
        }

        fn is_new(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_default_relationships_is_empty() {
        assert!(TestModel::RELATIONSHIPS.is_empty());
    }
}
