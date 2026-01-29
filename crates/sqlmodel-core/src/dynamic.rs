//! Dynamic model creation at runtime.
//!
//! Provides `DynamicModel` for working with tables whose schema
//! is not known at compile time.

use std::collections::HashMap;

use crate::row::Row;
use crate::types::SqlType;
use crate::value::Value;

/// A column definition for dynamic models.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// Column name in the database.
    pub name: String,
    /// SQL type.
    pub sql_type: SqlType,
    /// Whether this column is nullable.
    pub nullable: bool,
    /// Whether this is a primary key column.
    pub primary_key: bool,
    /// Whether this is auto-incrementing.
    pub auto_increment: bool,
    /// Default value expression.
    pub default: Option<String>,
}

impl ColumnDef {
    /// Create a new column definition.
    pub fn new(name: impl Into<String>, sql_type: SqlType) -> Self {
        Self {
            name: name.into(),
            sql_type,
            nullable: false,
            primary_key: false,
            auto_increment: false,
            default: None,
        }
    }

    /// Mark as nullable.
    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    /// Mark as primary key.
    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self
    }

    /// Mark as auto-incrementing.
    pub fn auto_increment(mut self) -> Self {
        self.auto_increment = true;
        self
    }

    /// Set default value expression.
    pub fn with_default(mut self, default: impl Into<String>) -> Self {
        self.default = Some(default.into());
        self
    }
}

/// A dynamically-defined model for tables whose schema is determined at runtime.
///
/// Unlike compile-time `Model` structs, `DynamicModel` stores column definitions
/// and values in hash maps, trading type safety for flexibility.
///
/// # Example
///
/// ```
/// use sqlmodel_core::dynamic::{DynamicModel, ColumnDef};
/// use sqlmodel_core::types::SqlType;
/// use sqlmodel_core::value::Value;
///
/// let mut model = DynamicModel::new("users");
/// model.add_column(ColumnDef::new("id", SqlType::BigInt).primary_key().auto_increment());
/// model.add_column(ColumnDef::new("name", SqlType::Text));
/// model.add_column(ColumnDef::new("email", SqlType::Text));
///
/// model.set("name", Value::Text("Alice".to_string()));
/// model.set("email", Value::Text("alice@example.com".to_string()));
///
/// assert_eq!(model.get("name").unwrap().as_str(), Some("Alice"));
/// ```
#[derive(Debug, Clone)]
pub struct DynamicModel {
    /// The table name.
    table_name: String,
    /// Column definitions in insertion order.
    columns: Vec<ColumnDef>,
    /// Current values by column name.
    values: HashMap<String, Value>,
}

impl DynamicModel {
    /// Create a new dynamic model for the given table.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            columns: Vec::new(),
            values: HashMap::new(),
        }
    }

    /// Add a column definition.
    pub fn add_column(&mut self, column: ColumnDef) {
        self.columns.push(column);
    }

    /// Get the table name.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get column definitions.
    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    /// Get primary key column names.
    pub fn primary_key_columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name.as_str())
            .collect()
    }

    /// Set a value for a column.
    pub fn set(&mut self, column: impl Into<String>, value: Value) {
        self.values.insert(column.into(), value);
    }

    /// Get a value for a column.
    pub fn get(&self, column: &str) -> Option<&Value> {
        self.values.get(column)
    }

    /// Remove a value, returning it.
    pub fn remove(&mut self, column: &str) -> Option<Value> {
        self.values.remove(column)
    }

    /// Check if a column has a value set.
    pub fn has(&self, column: &str) -> bool {
        self.values.contains_key(column)
    }

    /// Get all column-value pairs for non-null, non-auto-increment columns.
    ///
    /// Suitable for building INSERT statements.
    pub fn to_insert_pairs(&self) -> Vec<(&str, &Value)> {
        self.columns
            .iter()
            .filter(|c| !c.auto_increment || self.values.contains_key(&c.name))
            .filter_map(|c| self.values.get(&c.name).map(|v| (c.name.as_str(), v)))
            .collect()
    }

    /// Get primary key values.
    pub fn primary_key_values(&self) -> Vec<Value> {
        self.columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| self.values.get(&c.name).cloned().unwrap_or(Value::Null))
            .collect()
    }

    /// Populate from a database row.
    #[allow(clippy::result_large_err)]
    pub fn from_row(&mut self, row: &Row) -> crate::Result<()> {
        for col in &self.columns {
            if let Ok(value) = row.get_named::<Value>(&col.name) {
                self.values.insert(col.name.clone(), value);
            } else if col.nullable {
                self.values.insert(col.name.clone(), Value::Null);
            }
        }
        Ok(())
    }

    /// Create a new DynamicModel instance from a row.
    #[allow(clippy::result_large_err)]
    pub fn new_from_row(
        table_name: impl Into<String>,
        columns: Vec<ColumnDef>,
        row: &Row,
    ) -> crate::Result<Self> {
        let mut model = Self {
            table_name: table_name.into(),
            columns,
            values: HashMap::new(),
        };
        model.from_row(row)?;
        Ok(model)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dynamic_model_basic() {
        let mut model = DynamicModel::new("users");
        model.add_column(
            ColumnDef::new("id", SqlType::BigInt)
                .primary_key()
                .auto_increment(),
        );
        model.add_column(ColumnDef::new("name", SqlType::Text));

        model.set("name", Value::Text("Alice".to_string()));

        assert_eq!(model.table_name(), "users");
        assert_eq!(model.get("name").unwrap().as_str(), Some("Alice"));
        assert!(!model.has("id"));
        assert!(model.has("name"));
    }

    #[test]
    fn test_primary_key_columns() {
        let mut model = DynamicModel::new("users");
        model.add_column(ColumnDef::new("id", SqlType::BigInt).primary_key());
        model.add_column(ColumnDef::new("name", SqlType::Text));

        assert_eq!(model.primary_key_columns(), vec!["id"]);
    }

    #[test]
    fn test_insert_pairs_skip_auto_increment() {
        let mut model = DynamicModel::new("users");
        model.add_column(
            ColumnDef::new("id", SqlType::BigInt)
                .primary_key()
                .auto_increment(),
        );
        model.add_column(ColumnDef::new("name", SqlType::Text));

        model.set("name", Value::Text("Alice".to_string()));

        let pairs = model.to_insert_pairs();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, "name");
    }

    #[test]
    fn test_primary_key_values() {
        let mut model = DynamicModel::new("users");
        model.add_column(ColumnDef::new("id", SqlType::BigInt).primary_key());
        model.add_column(ColumnDef::new("name", SqlType::Text));

        model.set("id", Value::BigInt(42));
        model.set("name", Value::Text("Alice".to_string()));

        let pk = model.primary_key_values();
        assert_eq!(pk, vec![Value::BigInt(42)]);
    }
}
