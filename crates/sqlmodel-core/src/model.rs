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

/// Behavior for handling extra fields during validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExtraFieldsBehavior {
    /// Allow extra fields (ignore them).
    #[default]
    Ignore,
    /// Forbid extra fields (return validation error).
    Forbid,
    /// Allow extra fields and preserve them.
    Allow,
}

/// Model-level configuration matching Pydantic's model_config.
///
/// This struct holds configuration options that affect model behavior
/// during validation, serialization, and database operations.
///
/// # Example
///
/// ```ignore
/// #[derive(Model)]
/// #[sqlmodel(
///     table,
///     from_attributes,
///     validate_assignment,
///     extra = "forbid",
///     strict,
///     populate_by_name,
///     use_enum_values
/// )]
/// struct User {
///     // ...
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct ModelConfig {
    /// Whether this model maps to a database table.
    /// If true, DDL can be generated for this model.
    pub table: bool,

    /// Allow reading data from object attributes (ORM mode).
    /// When true, model_validate can accept objects with attributes
    /// in addition to dicts.
    pub from_attributes: bool,

    /// Validate field values when they are assigned.
    /// When true, assignment to fields triggers validation.
    pub validate_assignment: bool,

    /// How to handle extra fields during validation.
    pub extra: ExtraFieldsBehavior,

    /// Enable strict type checking during validation.
    /// When true, types must match exactly (no coercion).
    pub strict: bool,

    /// Allow populating fields by either name or alias.
    /// When true, both the field name and any aliases are accepted
    /// during deserialization.
    pub populate_by_name: bool,

    /// Use enum values instead of names during serialization.
    /// When true, enum fields serialize to their underlying values
    /// rather than variant names.
    pub use_enum_values: bool,

    /// Allow arbitrary types in fields.
    /// When true, fields can use types that aren't natively supported
    /// by the validation system.
    pub arbitrary_types_allowed: bool,

    /// Defer model validation to allow forward references.
    /// When true, validation of field types is deferred until
    /// the model is first used.
    pub defer_build: bool,

    /// Revalidate instances when converting to this model.
    /// Controls whether existing model instances are revalidated.
    pub revalidate_instances: bool,

    /// Custom JSON schema extra data.
    /// Additional data to include in generated JSON schema.
    pub json_schema_extra: Option<&'static str>,

    /// Title for JSON schema generation.
    pub title: Option<&'static str>,
}

impl ModelConfig {
    /// Create a new ModelConfig with all defaults.
    pub const fn new() -> Self {
        Self {
            table: false,
            from_attributes: false,
            validate_assignment: false,
            extra: ExtraFieldsBehavior::Ignore,
            strict: false,
            populate_by_name: false,
            use_enum_values: false,
            arbitrary_types_allowed: false,
            defer_build: false,
            revalidate_instances: false,
            json_schema_extra: None,
            title: None,
        }
    }

    /// Create a config for a database table model.
    pub const fn table() -> Self {
        Self {
            table: true,
            from_attributes: false,
            validate_assignment: false,
            extra: ExtraFieldsBehavior::Ignore,
            strict: false,
            populate_by_name: false,
            use_enum_values: false,
            arbitrary_types_allowed: false,
            defer_build: false,
            revalidate_instances: false,
            json_schema_extra: None,
            title: None,
        }
    }
}

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

    /// Get the model configuration.
    ///
    /// Returns model-level configuration that affects validation,
    /// serialization, and database operations.
    fn model_config() -> ModelConfig {
        ModelConfig::new()
    }
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

/// Lifecycle event hooks for model instances.
///
/// Models can implement this trait to receive callbacks at various points
/// in the persistence lifecycle: before/after insert, update, and delete.
///
/// All methods have default no-op implementations, so you only need to
/// override the ones you care about.
///
/// # Example
///
/// ```ignore
/// use sqlmodel_core::{Model, ModelEvents, Result};
///
/// #[derive(Model)]
/// struct User {
///     id: Option<i64>,
///     name: String,
///     created_at: Option<i64>,
///     updated_at: Option<i64>,
/// }
///
/// impl ModelEvents for User {
///     fn before_insert(&mut self) -> Result<()> {
///         let now = std::time::SystemTime::now()
///             .duration_since(std::time::UNIX_EPOCH)
///             .unwrap()
///             .as_secs() as i64;
///         self.created_at = Some(now);
///         self.updated_at = Some(now);
///         Ok(())
///     }
///
///     fn before_update(&mut self) -> Result<()> {
///         let now = std::time::SystemTime::now()
///             .duration_since(std::time::UNIX_EPOCH)
///             .unwrap()
///             .as_secs() as i64;
///         self.updated_at = Some(now);
///         Ok(())
///     }
/// }
/// ```
pub trait ModelEvents: Model {
    /// Called before a new instance is inserted into the database.
    ///
    /// Use this to set default values, validate data, or perform
    /// any pre-insert logic. Return an error to abort the insert.
    #[allow(unused_variables, clippy::result_large_err)]
    fn before_insert(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called after an instance has been successfully inserted.
    ///
    /// The instance now has its auto-generated ID (if applicable).
    /// Use this for post-insert notifications or logging.
    #[allow(unused_variables, clippy::result_large_err)]
    fn after_insert(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called before an existing instance is updated in the database.
    ///
    /// Use this to update timestamps, validate changes, or perform
    /// any pre-update logic. Return an error to abort the update.
    #[allow(unused_variables, clippy::result_large_err)]
    fn before_update(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called after an instance has been successfully updated.
    ///
    /// Use this for post-update notifications or logging.
    #[allow(unused_variables, clippy::result_large_err)]
    fn after_update(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called before an instance is deleted from the database.
    ///
    /// Use this for cleanup, validation, or any pre-delete logic.
    /// Return an error to abort the delete.
    #[allow(unused_variables, clippy::result_large_err)]
    fn before_delete(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called after an instance has been successfully deleted.
    ///
    /// Use this for post-delete notifications or logging.
    #[allow(unused_variables, clippy::result_large_err)]
    fn after_delete(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called after an instance has been loaded from the database.
    ///
    /// Use this to perform post-load initialization or validation.
    #[allow(unused_variables, clippy::result_large_err)]
    fn on_load(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called after an instance has been refreshed from the database.
    ///
    /// Use this to handle any logic needed after a refresh operation.
    #[allow(unused_variables, clippy::result_large_err)]
    fn on_refresh(&mut self) -> Result<()> {
        Ok(())
    }
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

    // Test default ModelEvents implementation
    impl ModelEvents for TestModel {}

    #[test]
    fn test_model_events_default_before_insert() {
        let mut model = TestModel;
        assert!(model.before_insert().is_ok());
    }

    #[test]
    fn test_model_events_default_after_insert() {
        let mut model = TestModel;
        assert!(model.after_insert().is_ok());
    }

    #[test]
    fn test_model_events_default_before_update() {
        let mut model = TestModel;
        assert!(model.before_update().is_ok());
    }

    #[test]
    fn test_model_events_default_after_update() {
        let mut model = TestModel;
        assert!(model.after_update().is_ok());
    }

    #[test]
    fn test_model_events_default_before_delete() {
        let mut model = TestModel;
        assert!(model.before_delete().is_ok());
    }

    #[test]
    fn test_model_events_default_after_delete() {
        let mut model = TestModel;
        assert!(model.after_delete().is_ok());
    }

    #[test]
    fn test_model_events_default_on_load() {
        let mut model = TestModel;
        assert!(model.on_load().is_ok());
    }

    #[test]
    fn test_model_events_default_on_refresh() {
        let mut model = TestModel;
        assert!(model.on_refresh().is_ok());
    }

    // Test custom ModelEvents implementation that modifies state
    #[derive(Debug)]
    struct TimestampedModel {
        id: Option<i64>,
        created_at: i64,
        updated_at: i64,
    }

    impl Model for TimestampedModel {
        const TABLE_NAME: &'static str = "timestamped_models";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] =
                &[FieldInfo::new("id", "id", SqlType::Integer).primary_key(true)];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![("id", self.id.map(Value::from).unwrap_or(Value::Null))]
        }

        fn from_row(_row: &Row) -> Result<Self> {
            Ok(Self {
                id: Some(1),
                created_at: 0,
                updated_at: 0,
            })
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![self.id.map(Value::from).unwrap_or(Value::Null)]
        }

        fn is_new(&self) -> bool {
            self.id.is_none()
        }
    }

    impl ModelEvents for TimestampedModel {
        fn before_insert(&mut self) -> Result<()> {
            // Simulate setting created_at timestamp
            self.created_at = 1000;
            self.updated_at = 1000;
            Ok(())
        }

        fn before_update(&mut self) -> Result<()> {
            // Simulate updating updated_at timestamp
            self.updated_at = 2000;
            Ok(())
        }
    }

    #[test]
    fn test_model_events_custom_before_insert_sets_timestamps() {
        let mut model = TimestampedModel {
            id: None,
            created_at: 0,
            updated_at: 0,
        };

        assert_eq!(model.created_at, 0);
        assert_eq!(model.updated_at, 0);

        model.before_insert().unwrap();

        assert_eq!(model.created_at, 1000);
        assert_eq!(model.updated_at, 1000);
    }

    #[test]
    fn test_model_events_custom_before_update_sets_timestamp() {
        let mut model = TimestampedModel {
            id: Some(1),
            created_at: 1000,
            updated_at: 1000,
        };

        model.before_update().unwrap();

        // created_at should remain unchanged
        assert_eq!(model.created_at, 1000);
        // updated_at should be updated
        assert_eq!(model.updated_at, 2000);
    }

    #[test]
    fn test_model_events_custom_defaults_still_work() {
        // Ensure overriding some methods doesn't break the defaults
        let mut model = TimestampedModel {
            id: Some(1),
            created_at: 0,
            updated_at: 0,
        };

        // These use default implementations
        assert!(model.after_insert().is_ok());
        assert!(model.after_update().is_ok());
        assert!(model.before_delete().is_ok());
        assert!(model.after_delete().is_ok());
        assert!(model.on_load().is_ok());
        assert!(model.on_refresh().is_ok());
    }

    // ==================== ModelConfig Tests ====================

    #[test]
    fn test_model_config_new_defaults() {
        let config = ModelConfig::new();
        assert!(!config.table);
        assert!(!config.from_attributes);
        assert!(!config.validate_assignment);
        assert_eq!(config.extra, ExtraFieldsBehavior::Ignore);
        assert!(!config.strict);
        assert!(!config.populate_by_name);
        assert!(!config.use_enum_values);
        assert!(!config.arbitrary_types_allowed);
        assert!(!config.defer_build);
        assert!(!config.revalidate_instances);
        assert!(config.json_schema_extra.is_none());
        assert!(config.title.is_none());
    }

    #[test]
    fn test_model_config_table_constructor() {
        let config = ModelConfig::table();
        assert!(config.table);
        assert!(!config.from_attributes);
    }

    #[test]
    fn test_extra_fields_behavior_default() {
        let behavior = ExtraFieldsBehavior::default();
        assert_eq!(behavior, ExtraFieldsBehavior::Ignore);
    }

    #[test]
    fn test_model_default_config() {
        // TestModel uses default implementation of model_config()
        let config = TestModel::model_config();
        assert!(!config.table);
        assert!(!config.from_attributes);
    }
}
