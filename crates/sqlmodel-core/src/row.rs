//! Database row representation.

use crate::Result;
use crate::error::{Error, TypeError};
use crate::value::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Column metadata shared across all rows in a result set.
///
/// This struct is wrapped in `Arc` so all rows from the same query share
/// the same column information, saving memory for large result sets.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column names in order
    names: Vec<String>,
    /// Name -> index mapping for O(1) lookup
    name_to_index: HashMap<String, usize>,
}

impl ColumnInfo {
    /// Create new column info from a list of column names.
    pub fn new(names: Vec<String>) -> Self {
        let name_to_index = names
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();
        Self {
            names,
            name_to_index,
        }
    }

    /// Get the number of columns.
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// Check if there are no columns.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// Get the index of a column by name.
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).copied()
    }

    /// Get the name of a column by index.
    pub fn name_at(&self, index: usize) -> Option<&str> {
        self.names.get(index).map(String::as_str)
    }

    /// Check if a column exists.
    pub fn contains(&self, name: &str) -> bool {
        self.name_to_index.contains_key(name)
    }

    /// Get all column names.
    pub fn names(&self) -> &[String] {
        &self.names
    }
}

/// A single row returned from a database query.
///
/// Rows provide both index-based and name-based access to column values.
/// Column metadata is shared via `Arc` for memory efficiency.
#[derive(Debug, Clone)]
pub struct Row {
    /// Column values in order
    values: Vec<Value>,
    /// Shared column metadata
    columns: Arc<ColumnInfo>,
}

impl Row {
    /// Create a new row with the given columns and values.
    ///
    /// For multiple rows from the same result set, prefer `with_columns`
    /// to share the column metadata.
    pub fn new(column_names: Vec<String>, values: Vec<Value>) -> Self {
        let columns = Arc::new(ColumnInfo::new(column_names));
        Self { values, columns }
    }

    /// Extract a subset of columns with a given prefix.
    ///
    /// This is useful for eager loading where columns are aliased like
    /// `table__column`. This method extracts columns matching `prefix__*`
    /// and returns a new Row with the prefix stripped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let row = Row::new(
    ///     vec!["heroes__id", "heroes__name", "teams__id", "teams__name"],
    ///     vec![Value::Int(1), Value::Text("Hero".into()), Value::Int(10), Value::Text("Team".into())],
    /// );
    /// let hero_row = row.subset_by_prefix("heroes");
    /// // hero_row has columns: ["id", "name"] with values [1, "Hero"]
    /// ```
    #[must_use]
    pub fn subset_by_prefix(&self, prefix: &str) -> Self {
        let prefix_with_sep = format!("{}__", prefix);
        let mut names = Vec::new();
        let mut values = Vec::new();

        for (name, value) in self.iter() {
            if let Some(stripped) = name.strip_prefix(&prefix_with_sep) {
                names.push(stripped.to_string());
                values.push(value.clone());
            }
        }

        Self::new(names, values)
    }

    /// Check if this row has any columns with the given prefix.
    ///
    /// Useful for checking if a LEFT JOIN returned NULL (no matching rows).
    #[must_use]
    pub fn has_prefix(&self, prefix: &str) -> bool {
        let prefix_with_sep = format!("{}__", prefix);
        self.column_names()
            .any(|name| name.starts_with(&prefix_with_sep))
    }

    /// Check if all values with a given prefix are NULL.
    ///
    /// Used to detect LEFT JOIN rows where no related record exists.
    #[must_use]
    pub fn prefix_is_all_null(&self, prefix: &str) -> bool {
        let prefix_with_sep = format!("{}__", prefix);
        let mut found_any = false;

        for (name, value) in self.iter() {
            if name.starts_with(&prefix_with_sep) {
                found_any = true;
                if !value.is_null() {
                    return false;
                }
            }
        }

        // If we found no columns with prefix, consider it "all null"
        !found_any || found_any
    }

    /// Create a new row with shared column metadata.
    ///
    /// This is more efficient for creating multiple rows from the same query.
    pub fn with_columns(columns: Arc<ColumnInfo>, values: Vec<Value>) -> Self {
        Self { values, columns }
    }

    /// Get the shared column metadata.
    ///
    /// Use this to create additional rows that share the same column info.
    pub fn column_info(&self) -> Arc<ColumnInfo> {
        Arc::clone(&self.columns)
    }

    /// Get the number of columns in this row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if this row is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get a value by column index. O(1) operation.
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Get a value by column name. O(1) operation via HashMap lookup.
    pub fn get_by_name(&self, name: &str) -> Option<&Value> {
        self.columns.index_of(name).and_then(|i| self.values.get(i))
    }

    /// Check if a column exists by name.
    pub fn contains_column(&self, name: &str) -> bool {
        self.columns.contains(name)
    }

    /// Get a typed value by column index.
    #[allow(clippy::result_large_err)]
    pub fn get_as<T: FromValue>(&self, index: usize) -> Result<T> {
        let value = self.get(index).ok_or_else(|| {
            Error::Type(TypeError {
                expected: std::any::type_name::<T>(),
                actual: format!(
                    "index {} out of bounds (row has {} columns)",
                    index,
                    self.len()
                ),
                column: None,
                rust_type: None,
            })
        })?;
        T::from_value(value)
    }

    /// Get a typed value by column name.
    #[allow(clippy::result_large_err)]
    pub fn get_named<T: FromValue>(&self, name: &str) -> Result<T> {
        let value = self.get_by_name(name).ok_or_else(|| {
            Error::Type(TypeError {
                expected: std::any::type_name::<T>(),
                actual: format!("column '{}' not found", name),
                column: Some(name.to_string()),
                rust_type: None,
            })
        })?;
        T::from_value(value).map_err(|e| match e {
            Error::Type(mut te) => {
                te.column = Some(name.to_string());
                Error::Type(te)
            }
            e => e,
        })
    }

    /// Get all column names.
    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.names().iter().map(String::as_str)
    }

    /// Iterate over all values.
    pub fn values(&self) -> impl Iterator<Item = &Value> {
        self.values.iter()
    }

    /// Iterate over (column_name, value) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.columns
            .names()
            .iter()
            .map(String::as_str)
            .zip(self.values.iter())
    }
}

/// Trait for converting from a `Value` to a typed value.
pub trait FromValue: Sized {
    /// Convert from a Value, returning an error if the conversion fails.
    #[allow(clippy::result_large_err)]
    fn from_value(value: &Value) -> Result<Self>;
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Result<Self> {
        value.as_bool().ok_or_else(|| {
            Error::Type(TypeError {
                expected: "bool",
                actual: value.type_name().to_string(),
                column: None,
                rust_type: None,
            })
        })
    }
}

impl FromValue for i8 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::TinyInt(v) => Ok(*v),
            Value::Bool(v) => Ok(if *v { 1 } else { 0 }),
            _ => Err(Error::Type(TypeError {
                expected: "i8",
                actual: value.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl FromValue for i16 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::TinyInt(v) => Ok(i16::from(*v)),
            Value::SmallInt(v) => Ok(*v),
            Value::Bool(v) => Ok(if *v { 1 } else { 0 }),
            _ => Err(Error::Type(TypeError {
                expected: "i16",
                actual: value.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl FromValue for i32 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::TinyInt(v) => Ok(i32::from(*v)),
            Value::SmallInt(v) => Ok(i32::from(*v)),
            Value::Int(v) => Ok(*v),
            Value::Bool(v) => Ok(if *v { 1 } else { 0 }),
            _ => Err(Error::Type(TypeError {
                expected: "i32",
                actual: value.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Result<Self> {
        value.as_i64().ok_or_else(|| {
            Error::Type(TypeError {
                expected: "i64",
                actual: value.type_name().to_string(),
                column: None,
                rust_type: None,
            })
        })
    }
}

impl FromValue for u8 {
    fn from_value(value: &Value) -> Result<Self> {
        let v = value.as_i64().ok_or_else(|| {
            Error::Type(TypeError {
                expected: "u8",
                actual: value.type_name().to_string(),
                column: None,
                rust_type: None,
            })
        })?;
        u8::try_from(v).map_err(|_| {
            Error::Type(TypeError {
                expected: "u8",
                actual: format!("value {} out of range", v),
                column: None,
                rust_type: None,
            })
        })
    }
}

impl FromValue for u16 {
    fn from_value(value: &Value) -> Result<Self> {
        let v = value.as_i64().ok_or_else(|| {
            Error::Type(TypeError {
                expected: "u16",
                actual: value.type_name().to_string(),
                column: None,
                rust_type: None,
            })
        })?;
        u16::try_from(v).map_err(|_| {
            Error::Type(TypeError {
                expected: "u16",
                actual: format!("value {} out of range", v),
                column: None,
                rust_type: None,
            })
        })
    }
}

impl FromValue for u32 {
    fn from_value(value: &Value) -> Result<Self> {
        let v = value.as_i64().ok_or_else(|| {
            Error::Type(TypeError {
                expected: "u32",
                actual: value.type_name().to_string(),
                column: None,
                rust_type: None,
            })
        })?;
        u32::try_from(v).map_err(|_| {
            Error::Type(TypeError {
                expected: "u32",
                actual: format!("value {} out of range", v),
                column: None,
                rust_type: None,
            })
        })
    }
}

impl FromValue for u64 {
    fn from_value(value: &Value) -> Result<Self> {
        let v = value.as_i64().ok_or_else(|| {
            Error::Type(TypeError {
                expected: "u64",
                actual: value.type_name().to_string(),
                column: None,
                rust_type: None,
            })
        })?;
        u64::try_from(v).map_err(|_| {
            Error::Type(TypeError {
                expected: "u64",
                actual: format!("value {} out of range", v),
                column: None,
                rust_type: None,
            })
        })
    }
}

#[allow(clippy::cast_possible_truncation)]
impl FromValue for f32 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Float(v) => Ok(*v),
            Value::Double(v) => Ok(*v as f32),
            Value::TinyInt(v) => Ok(f32::from(*v)),
            Value::SmallInt(v) => Ok(f32::from(*v)),
            Value::Int(v) => Ok(*v as f32),
            Value::BigInt(v) => Ok(*v as f32),
            _ => Err(Error::Type(TypeError {
                expected: "f32",
                actual: value.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value) -> Result<Self> {
        value.as_f64().ok_or_else(|| {
            Error::Type(TypeError {
                expected: "f64",
                actual: value.type_name().to_string(),
                column: None,
                rust_type: None,
            })
        })
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s.clone()),
            Value::Decimal(s) => Ok(s.clone()),
            _ => Err(Error::Type(TypeError {
                expected: "String",
                actual: value.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Bytes(b) => Ok(b.clone()),
            Value::Text(s) => Ok(s.as_bytes().to_vec()),
            _ => Err(Error::Type(TypeError {
                expected: "Vec<u8>",
                actual: value.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: &Value) -> Result<Self> {
        if value.is_null() {
            Ok(None)
        } else {
            T::from_value(value).map(Some)
        }
    }
}

impl FromValue for Value {
    fn from_value(value: &Value) -> Result<Self> {
        Ok(value.clone())
    }
}

impl FromValue for serde_json::Value {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Json(v) => Ok(v.clone()),
            Value::Text(s) => serde_json::from_str(s).map_err(|e| {
                Error::Type(TypeError {
                    expected: "valid JSON",
                    actual: format!("invalid JSON: {}", e),
                    column: None,
                    rust_type: None,
                })
            }),
            _ => Err(Error::Type(TypeError {
                expected: "JSON",
                actual: value.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

impl FromValue for [u8; 16] {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Uuid(v) => Ok(*v),
            Value::Bytes(v) if v.len() == 16 => {
                let mut arr = [0u8; 16];
                arr.copy_from_slice(v);
                Ok(arr)
            }
            _ => Err(Error::Type(TypeError {
                expected: "UUID (16 bytes)",
                actual: value.type_name().to_string(),
                column: None,
                rust_type: None,
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_basic_access() {
        let row = Row::new(
            vec!["id".to_string(), "name".to_string(), "age".to_string()],
            vec![
                Value::Int(1),
                Value::Text("Alice".to_string()),
                Value::Int(30),
            ],
        );

        assert_eq!(row.len(), 3);
        assert!(!row.is_empty());

        // Index access
        assert_eq!(row.get(0), Some(&Value::Int(1)));
        assert_eq!(row.get(1), Some(&Value::Text("Alice".to_string())));
        assert_eq!(row.get(3), None);

        // Name access
        assert_eq!(row.get_by_name("id"), Some(&Value::Int(1)));
        assert_eq!(
            row.get_by_name("name"),
            Some(&Value::Text("Alice".to_string()))
        );
        assert_eq!(row.get_by_name("missing"), None);
    }

    #[test]
    fn test_row_typed_access() {
        let row = Row::new(
            vec!["id".to_string(), "name".to_string()],
            vec![Value::Int(42), Value::Text("Bob".to_string())],
        );

        // Typed index access
        assert_eq!(row.get_as::<i32>(0).unwrap(), 42);
        assert_eq!(row.get_as::<i64>(0).unwrap(), 42);
        assert_eq!(row.get_as::<String>(1).unwrap(), "Bob");

        // Typed name access
        assert_eq!(row.get_named::<i32>("id").unwrap(), 42);
        assert_eq!(row.get_named::<String>("name").unwrap(), "Bob");
    }

    #[test]
    fn test_row_type_errors() {
        let row = Row::new(
            vec!["id".to_string()],
            vec![Value::Text("not a number".to_string())],
        );

        // Type mismatch
        assert!(row.get_named::<i32>("id").is_err());

        // Column not found
        assert!(row.get_named::<i32>("missing").is_err());

        // Index out of bounds
        assert!(row.get_as::<i32>(99).is_err());
    }

    #[test]
    fn test_row_null_handling() {
        let row = Row::new(vec!["nullable".to_string()], vec![Value::Null]);

        // Option handles NULL gracefully
        assert_eq!(row.get_named::<Option<i32>>("nullable").unwrap(), None);

        // Non-optional type fails for NULL
        assert!(row.get_named::<i32>("nullable").is_err());
    }

    #[test]
    fn test_row_iterators() {
        let row = Row::new(
            vec!["a".to_string(), "b".to_string()],
            vec![Value::Int(1), Value::Int(2)],
        );

        // Column names iterator
        let names: Vec<_> = row.column_names().collect();
        assert_eq!(names, vec!["a", "b"]);

        // Values iterator
        let values: Vec<_> = row.values().collect();
        assert_eq!(values, vec![&Value::Int(1), &Value::Int(2)]);

        // Pairs iterator
        let pairs: Vec<_> = row.iter().collect();
        assert_eq!(pairs, vec![("a", &Value::Int(1)), ("b", &Value::Int(2))]);
    }

    #[test]
    fn test_row_shared_columns() {
        let columns = Arc::new(ColumnInfo::new(vec!["id".to_string(), "name".to_string()]));

        let row1 = Row::with_columns(
            Arc::clone(&columns),
            vec![Value::Int(1), Value::Text("Alice".to_string())],
        );
        let row2 = Row::with_columns(
            Arc::clone(&columns),
            vec![Value::Int(2), Value::Text("Bob".to_string())],
        );

        // Both rows share the same column info
        assert!(Arc::ptr_eq(&row1.column_info(), &row2.column_info()));

        // Both work correctly
        assert_eq!(row1.get_named::<i32>("id").unwrap(), 1);
        assert_eq!(row2.get_named::<i32>("id").unwrap(), 2);
    }

    #[test]
    fn test_row_contains_column() {
        let row = Row::new(vec!["exists".to_string()], vec![Value::Int(1)]);

        assert!(row.contains_column("exists"));
        assert!(!row.contains_column("missing"));
    }

    #[test]
    fn test_column_info() {
        let info = ColumnInfo::new(vec![
            "id".to_string(),
            "name".to_string(),
            "age".to_string(),
        ]);

        assert_eq!(info.len(), 3);
        assert!(!info.is_empty());

        assert_eq!(info.index_of("id"), Some(0));
        assert_eq!(info.index_of("name"), Some(1));
        assert_eq!(info.index_of("missing"), None);

        assert_eq!(info.name_at(0), Some("id"));
        assert_eq!(info.name_at(1), Some("name"));
        assert_eq!(info.name_at(99), None);

        assert!(info.contains("id"));
        assert!(!info.contains("missing"));
    }

    #[test]
    fn test_from_value_all_types() {
        // bool
        assert!(bool::from_value(&Value::Bool(true)).unwrap());
        assert!(bool::from_value(&Value::Int(1)).unwrap());
        assert!(!bool::from_value(&Value::Int(0)).unwrap());

        // i8
        assert_eq!(i8::from_value(&Value::TinyInt(42)).unwrap(), 42);

        // i16
        assert_eq!(i16::from_value(&Value::SmallInt(100)).unwrap(), 100);
        assert_eq!(i16::from_value(&Value::TinyInt(10)).unwrap(), 10);

        // i32
        assert_eq!(i32::from_value(&Value::Int(1000)).unwrap(), 1000);

        // i64
        assert_eq!(i64::from_value(&Value::BigInt(10000)).unwrap(), 10000);

        // f32
        let pi_f32 = std::f32::consts::PI;
        let from_float = f32::from_value(&Value::Float(pi_f32)).unwrap();
        assert!((from_float - pi_f32).abs() < 1e-6);

        // f64
        let pi_f64 = std::f64::consts::PI;
        let from_double = f64::from_value(&Value::Double(pi_f64)).unwrap();
        assert!((from_double - pi_f64).abs() < 1e-12);

        // String
        assert_eq!(
            String::from_value(&Value::Text("hello".to_string())).unwrap(),
            "hello"
        );

        // Vec<u8>
        assert_eq!(
            Vec::<u8>::from_value(&Value::Bytes(vec![1, 2, 3])).unwrap(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn test_empty_row() {
        let row = Row::new(vec![], vec![]);
        assert!(row.is_empty());
        assert_eq!(row.len(), 0);
        assert_eq!(row.get(0), None);
        assert!(row.get_as::<i32>(0).is_err());
    }

    #[test]
    fn test_large_row() {
        // Test with many columns
        let n = 100;
        let names: Vec<_> = (0..n).map(|i| format!("col_{}", i)).collect();
        let values: Vec<_> = (0..n).map(Value::Int).collect();
        let row = Row::new(names, values);

        assert_eq!(row.len(), n as usize);
        assert_eq!(row.get_named::<i32>("col_0").unwrap(), 0);
        assert_eq!(row.get_named::<i32>("col_50").unwrap(), 50);
        assert_eq!(row.get_named::<i32>("col_99").unwrap(), 99);
    }

    #[test]
    fn test_subset_by_prefix() {
        let row = Row::new(
            vec![
                "heroes__id".to_string(),
                "heroes__name".to_string(),
                "teams__id".to_string(),
                "teams__name".to_string(),
            ],
            vec![
                Value::Int(1),
                Value::Text("Batman".to_string()),
                Value::Int(10),
                Value::Text("Justice League".to_string()),
            ],
        );

        // Extract heroes columns
        let heroes_row = row.subset_by_prefix("heroes");
        assert_eq!(heroes_row.len(), 2);
        assert_eq!(heroes_row.get_named::<i32>("id").unwrap(), 1);
        assert_eq!(heroes_row.get_named::<String>("name").unwrap(), "Batman");

        // Extract teams columns
        let teams_row = row.subset_by_prefix("teams");
        assert_eq!(teams_row.len(), 2);
        assert_eq!(teams_row.get_named::<i32>("id").unwrap(), 10);
        assert_eq!(
            teams_row.get_named::<String>("name").unwrap(),
            "Justice League"
        );

        // Non-existent prefix returns empty row
        let empty_row = row.subset_by_prefix("powers");
        assert!(empty_row.is_empty());
    }

    #[test]
    fn test_has_prefix() {
        let row = Row::new(
            vec!["heroes__id".to_string(), "teams__id".to_string()],
            vec![Value::Int(1), Value::Int(10)],
        );

        assert!(row.has_prefix("heroes"));
        assert!(row.has_prefix("teams"));
        assert!(!row.has_prefix("powers"));
    }

    #[test]
    fn test_prefix_is_all_null() {
        let row = Row::new(
            vec![
                "heroes__id".to_string(),
                "heroes__name".to_string(),
                "teams__id".to_string(),
                "teams__name".to_string(),
            ],
            vec![
                Value::Int(1),
                Value::Text("Batman".to_string()),
                Value::Null,
                Value::Null,
            ],
        );

        // Heroes have values
        assert!(!row.prefix_is_all_null("heroes"));

        // Teams are all NULL (LEFT JOIN with no match)
        assert!(row.prefix_is_all_null("teams"));

        // Non-existent prefix is considered "all null"
        assert!(row.prefix_is_all_null("powers"));
    }
}
