//! Query builders for INSERT, UPDATE, DELETE operations.
//!
//! This module provides fluent builders for CRUD operations with support for:
//! - RETURNING clause (PostgreSQL)
//! - Bulk inserts
//! - UPSERT (ON CONFLICT)
//! - Explicit column SET for updates
//! - Model-based deletes

use crate::clause::Where;
use crate::expr::{Dialect, Expr};
use asupersync::{Cx, Outcome};
use sqlmodel_core::{Connection, Model, Row, Value};
use std::marker::PhantomData;

/// Conflict resolution strategy for INSERT operations.
///
/// Used with PostgreSQL's ON CONFLICT clause for UPSERT operations.
#[derive(Debug, Clone)]
pub enum OnConflict {
    /// Do nothing on conflict (INSERT ... ON CONFLICT DO NOTHING)
    DoNothing,
    /// Update specified columns on conflict (INSERT ... ON CONFLICT DO UPDATE SET ...)
    DoUpdate {
        /// The columns to update. If empty, all non-primary-key columns are updated.
        columns: Vec<String>,
        /// The conflict target (column names). If empty, uses primary key.
        target: Vec<String>,
    },
}

/// INSERT query builder.
///
/// # Example
///
/// ```ignore
/// // Simple insert
/// let id = insert!(hero).execute(cx, &conn).await?;
///
/// // Insert with RETURNING
/// let row = insert!(hero).returning().execute_returning(cx, &conn).await?;
///
/// // Insert with UPSERT
/// let id = insert!(hero)
///     .on_conflict_do_nothing()
///     .execute(cx, &conn).await?;
/// ```
#[derive(Debug)]
pub struct InsertBuilder<'a, M: Model> {
    model: &'a M,
    returning: bool,
    on_conflict: Option<OnConflict>,
}

impl<'a, M: Model> InsertBuilder<'a, M> {
    /// Create a new INSERT builder for the given model instance.
    pub fn new(model: &'a M) -> Self {
        Self {
            model,
            returning: false,
            on_conflict: None,
        }
    }

    /// Add RETURNING * clause to return the inserted row.
    ///
    /// Use with `execute_returning()` to get the inserted row.
    pub fn returning(mut self) -> Self {
        self.returning = true;
        self
    }

    /// Handle conflicts by doing nothing (PostgreSQL ON CONFLICT DO NOTHING).
    ///
    /// This allows the insert to silently succeed even if it would violate
    /// a unique constraint.
    pub fn on_conflict_do_nothing(mut self) -> Self {
        self.on_conflict = Some(OnConflict::DoNothing);
        self
    }

    /// Handle conflicts by updating specified columns (UPSERT).
    ///
    /// If `columns` is empty, all non-primary-key columns are updated.
    /// The conflict target defaults to the primary key.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Update name and age on conflict
    /// insert!(hero)
    ///     .on_conflict_do_update(&["name", "age"])
    ///     .execute(cx, &conn).await?;
    /// ```
    pub fn on_conflict_do_update(mut self, columns: &[&str]) -> Self {
        self.on_conflict = Some(OnConflict::DoUpdate {
            columns: columns.iter().map(|s| s.to_string()).collect(),
            target: Vec::new(), // Default to primary key
        });
        self
    }

    /// Handle conflicts by updating columns with a specific conflict target.
    ///
    /// # Arguments
    ///
    /// * `target` - The columns that form the unique constraint to match
    /// * `columns` - The columns to update on conflict
    pub fn on_conflict_target_do_update(mut self, target: &[&str], columns: &[&str]) -> Self {
        self.on_conflict = Some(OnConflict::DoUpdate {
            columns: columns.iter().map(|s| s.to_string()).collect(),
            target: target.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    /// Build the INSERT SQL and parameters with default dialect (Postgres).
    pub fn build(&self) -> (String, Vec<Value>) {
        self.build_with_dialect(Dialect::default())
    }

    /// Build the INSERT SQL and parameters with specific dialect.
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let row = self.model.to_row();
        let fields = M::fields();

        // Filter out auto-increment fields when inserting new records
        let insert_fields: Vec<_> = row
            .iter()
            .filter(|(name, value)| {
                // Skip NULL values for auto-increment fields
                let field = fields.iter().find(|f| f.name == *name);
                if let Some(f) = field {
                    if f.auto_increment && matches!(value, Value::Null) {
                        return false;
                    }
                }
                true
            })
            .collect();

        let columns: Vec<_> = insert_fields.iter().map(|(name, _)| *name).collect();
        let values: Vec<_> = insert_fields
            .iter()
            .map(|(_, value)| value.clone())
            .collect();

        let placeholders: Vec<_> = (1..=values.len()).map(|i| dialect.placeholder(i)).collect();

        let mut sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            M::TABLE_NAME,
            columns.join(", "),
            placeholders.join(", ")
        );

        // Add ON CONFLICT clause if specified
        if let Some(on_conflict) = &self.on_conflict {
            match on_conflict {
                OnConflict::DoNothing => {
                    sql.push_str(" ON CONFLICT DO NOTHING");
                }
                OnConflict::DoUpdate { columns, target } => {
                    sql.push_str(" ON CONFLICT");

                    // Add target if specified, otherwise use primary key
                    if !target.is_empty() {
                        sql.push_str(" (");
                        sql.push_str(&target.join(", "));
                        sql.push_str(")");
                    } else if !M::PRIMARY_KEY.is_empty() {
                        sql.push_str(" (");
                        sql.push_str(&M::PRIMARY_KEY.join(", "));
                        sql.push_str(")");
                    }

                    sql.push_str(" DO UPDATE SET ");

                    // If columns is empty, update all non-PK columns
                    let update_cols: Vec<String> = if columns.is_empty() {
                        insert_fields
                            .iter()
                            .filter(|(name, _)| !M::PRIMARY_KEY.contains(name))
                            .map(|(name, _)| format!("{} = EXCLUDED.{}", name, name))
                            .collect()
                    } else {
                        columns
                            .iter()
                            .map(|c| format!("{} = EXCLUDED.{}", c, c))
                            .collect()
                    };
                    sql.push_str(&update_cols.join(", "));
                }
            }
        }

        // Add RETURNING clause if requested
        if self.returning {
            sql.push_str(" RETURNING *");
        }

        (sql, values)
    }

    /// Execute the INSERT and return the inserted ID.
    pub async fn execute<C: Connection>(
        self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<i64, sqlmodel_core::Error> {
        let (sql, params) = self.build();
        conn.insert(cx, &sql, &params).await
    }

    /// Execute the INSERT with RETURNING and get the inserted row.
    ///
    /// This automatically adds RETURNING * and returns the full row.
    pub async fn execute_returning<C: Connection>(
        mut self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Option<Row>, sqlmodel_core::Error> {
        self.returning = true;
        let (sql, params) = self.build();
        conn.query_one(cx, &sql, &params).await
    }
}

/// Bulk INSERT query builder.
///
/// # Example
///
/// ```ignore
/// let heroes = vec![hero1, hero2, hero3];
/// let ids = insert_many!(heroes)
///     .execute(cx, &conn).await?;
/// ```
#[derive(Debug)]
pub struct InsertManyBuilder<'a, M: Model> {
    models: &'a [M],
    returning: bool,
    on_conflict: Option<OnConflict>,
}

impl<'a, M: Model> InsertManyBuilder<'a, M> {
    /// Create a new bulk INSERT builder for the given model instances.
    pub fn new(models: &'a [M]) -> Self {
        Self {
            models,
            returning: false,
            on_conflict: None,
        }
    }

    /// Add RETURNING * clause to return the inserted rows.
    pub fn returning(mut self) -> Self {
        self.returning = true;
        self
    }

    /// Handle conflicts by doing nothing.
    pub fn on_conflict_do_nothing(mut self) -> Self {
        self.on_conflict = Some(OnConflict::DoNothing);
        self
    }

    /// Handle conflicts by updating specified columns.
    pub fn on_conflict_do_update(mut self, columns: &[&str]) -> Self {
        self.on_conflict = Some(OnConflict::DoUpdate {
            columns: columns.iter().map(|s| s.to_string()).collect(),
            target: Vec::new(),
        });
        self
    }

    /// Build the bulk INSERT SQL and parameters with default dialect.
    pub fn build(&self) -> (String, Vec<Value>) {
        self.build_with_dialect(Dialect::default())
    }

    /// Build the bulk INSERT SQL and parameters with specific dialect.
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        if self.models.is_empty() {
            return (String::new(), Vec::new());
        }

        let fields = M::fields();
        let first_row = self.models[0].to_row();

        // Determine which columns to insert (filter auto-increment nulls)
        let insert_columns: Vec<_> = first_row
            .iter()
            .filter(|(name, value)| {
                let field = fields.iter().find(|f| f.name == *name);
                if let Some(f) = field {
                    if f.auto_increment && matches!(value, Value::Null) {
                        return false;
                    }
                }
                true
            })
            .map(|(name, _)| *name)
            .collect();

        let mut all_values = Vec::new();
        let mut value_groups = Vec::new();

        for model in self.models {
            let row = model.to_row();
            let values: Vec<_> = insert_columns
                .iter()
                .map(|col| {
                    row.iter()
                        .find(|(name, _)| name == col)
                        .map(|(_, v)| v.clone())
                        .unwrap_or(Value::Null)
                })
                .collect();

            let start = all_values.len() + 1;
            let placeholders: Vec<_> = (start..start + values.len())
                .map(|i| dialect.placeholder(i))
                .collect();
            value_groups.push(format!("({})", placeholders.join(", ")));
            all_values.extend(values);
        }

        let mut sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            M::TABLE_NAME,
            insert_columns.join(", "),
            value_groups.join(", ")
        );

        // Add ON CONFLICT clause if specified
        if let Some(on_conflict) = &self.on_conflict {
            match on_conflict {
                OnConflict::DoNothing => {
                    sql.push_str(" ON CONFLICT DO NOTHING");
                }
                OnConflict::DoUpdate { columns, target } => {
                    sql.push_str(" ON CONFLICT");

                    if !target.is_empty() {
                        sql.push_str(" (");
                        sql.push_str(&target.join(", "));
                        sql.push_str(")");
                    } else if !M::PRIMARY_KEY.is_empty() {
                        sql.push_str(" (");
                        sql.push_str(&M::PRIMARY_KEY.join(", "));
                        sql.push_str(")");
                    }

                    sql.push_str(" DO UPDATE SET ");

                    let update_cols: Vec<String> = if columns.is_empty() {
                        insert_columns
                            .iter()
                            .filter(|name| !M::PRIMARY_KEY.contains(name))
                            .map(|name| format!("{} = EXCLUDED.{}", name, name))
                            .collect()
                    } else {
                        columns
                            .iter()
                            .map(|c| format!("{} = EXCLUDED.{}", c, c))
                            .collect()
                    };
                    sql.push_str(&update_cols.join(", "));
                }
            }
        }

        if self.returning {
            sql.push_str(" RETURNING *");
        }

        (sql, all_values)
    }

    /// Execute the bulk INSERT and return rows affected.
    pub async fn execute<C: Connection>(
        self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<u64, sqlmodel_core::Error> {
        if self.models.is_empty() {
            return Outcome::Ok(0);
        }
        let (sql, params) = self.build();
        conn.execute(cx, &sql, &params).await
    }

    /// Execute the bulk INSERT with RETURNING and get the inserted rows.
    pub async fn execute_returning<C: Connection>(
        mut self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Vec<Row>, sqlmodel_core::Error> {
        if self.models.is_empty() {
            return Outcome::Ok(Vec::new());
        }
        self.returning = true;
        let (sql, params) = self.build();
        conn.query(cx, &sql, &params).await
    }
}

/// A column-value pair for explicit UPDATE SET operations.
#[derive(Debug, Clone)]
pub struct SetClause {
    column: String,
    value: Value,
}

/// UPDATE query builder.
///
/// # Example
///
/// ```ignore
/// // Update a model instance (uses primary key for WHERE)
/// update!(hero).execute(cx, &conn).await?;
///
/// // Update with explicit SET
/// UpdateBuilder::<Hero>::empty()
///     .set("age", 26)
///     .set("name", "New Name")
///     .filter(Expr::col("id").eq(42))
///     .execute(cx, &conn).await?;
///
/// // Update with RETURNING
/// let row = update!(hero).returning().execute_returning(cx, &conn).await?;
/// ```
#[derive(Debug)]
pub struct UpdateBuilder<'a, M: Model> {
    model: Option<&'a M>,
    where_clause: Option<Where>,
    set_fields: Option<Vec<&'static str>>,
    explicit_sets: Vec<SetClause>,
    returning: bool,
}

impl<'a, M: Model> UpdateBuilder<'a, M> {
    /// Create a new UPDATE builder for the given model instance.
    pub fn new(model: &'a M) -> Self {
        Self {
            model: Some(model),
            where_clause: None,
            set_fields: None,
            explicit_sets: Vec::new(),
            returning: false,
        }
    }

    /// Create an empty UPDATE builder for explicit SET operations.
    ///
    /// Use this when you want to update specific columns without a model instance.
    pub fn empty() -> Self {
        Self {
            model: None,
            where_clause: None,
            set_fields: None,
            explicit_sets: Vec::new(),
            returning: false,
        }
    }

    /// Set a column to a specific value.
    ///
    /// This can be used with or without a model instance.
    /// When used with a model, these explicit sets override the model values.
    pub fn set<V: Into<Value>>(mut self, column: &str, value: V) -> Self {
        self.explicit_sets.push(SetClause {
            column: column.to_string(),
            value: value.into(),
        });
        self
    }

    /// Only update specific fields from the model.
    pub fn set_only(mut self, fields: &[&'static str]) -> Self {
        self.set_fields = Some(fields.to_vec());
        self
    }

    /// Add a WHERE condition (defaults to primary key match).
    pub fn filter(mut self, expr: Expr) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => existing.and(expr),
            None => Where::new(expr),
        });
        self
    }

    /// Add RETURNING * clause to return the updated row(s).
    pub fn returning(mut self) -> Self {
        self.returning = true;
        self
    }

    /// Build the UPDATE SQL and parameters with default dialect (Postgres).
    pub fn build(&self) -> (String, Vec<Value>) {
        self.build_with_dialect(Dialect::default())
    }

    /// Build the UPDATE SQL and parameters with specific dialect.
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let pk = M::PRIMARY_KEY;
        let mut params = Vec::new();
        let mut set_clauses = Vec::new();

        // First, add explicit SET clauses
        for set in &self.explicit_sets {
            set_clauses.push(format!(
                "{} = {}",
                set.column,
                dialect.placeholder(params.len() + 1)
            ));
            params.push(set.value.clone());
        }

        // Then, add model fields if we have a model
        if let Some(model) = &self.model {
            let row = model.to_row();

            // Determine which fields to update
            let update_fields: Vec<_> = row
                .iter()
                .filter(|(name, _)| {
                    // Skip primary key fields
                    if pk.contains(name) {
                        return false;
                    }
                    // Skip columns that have explicit sets
                    if self.explicit_sets.iter().any(|s| s.column == *name) {
                        return false;
                    }
                    // If set_only specified, only include those fields
                    if let Some(fields) = &self.set_fields {
                        return fields.contains(name);
                    }
                    true
                })
                .collect();

            for (name, value) in update_fields {
                set_clauses.push(format!(
                    "{} = {}",
                    name,
                    dialect.placeholder(params.len() + 1)
                ));
                params.push(value.clone());
            }
        }

        if set_clauses.is_empty() {
            // Nothing to update - return empty SQL
            return (String::new(), Vec::new());
        }

        let mut sql = format!("UPDATE {} SET {}", M::TABLE_NAME, set_clauses.join(", "));

        // Add WHERE clause
        if let Some(where_clause) = &self.where_clause {
            let (where_sql, where_params) = where_clause.build_with_dialect(dialect, params.len());
            sql.push_str(" WHERE ");
            sql.push_str(&where_sql);
            params.extend(where_params);
        } else if let Some(model) = &self.model {
            // Default to primary key match
            let pk_values = model.primary_key_value();
            let pk_conditions: Vec<_> = pk
                .iter()
                .zip(pk_values.iter())
                .enumerate()
                .map(|(i, (col, _))| {
                    format!("{} = {}", col, dialect.placeholder(params.len() + i + 1))
                })
                .collect();

            if !pk_conditions.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&pk_conditions.join(" AND "));
                params.extend(pk_values);
            }
        }

        // Add RETURNING clause if requested
        if self.returning {
            sql.push_str(" RETURNING *");
        }

        (sql, params)
    }

    /// Execute the UPDATE and return rows affected.
    pub async fn execute<C: Connection>(
        self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<u64, sqlmodel_core::Error> {
        let (sql, params) = self.build();
        if sql.is_empty() {
            return Outcome::Ok(0);
        }
        conn.execute(cx, &sql, &params).await
    }

    /// Execute the UPDATE with RETURNING and get the updated rows.
    pub async fn execute_returning<C: Connection>(
        mut self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Vec<Row>, sqlmodel_core::Error> {
        self.returning = true;
        let (sql, params) = self.build();
        if sql.is_empty() {
            return Outcome::Ok(Vec::new());
        }
        conn.query(cx, &sql, &params).await
    }
}

/// DELETE query builder.
///
/// # Example
///
/// ```ignore
/// // Delete by filter
/// delete!(Hero)
///     .filter(Expr::col("age").lt(18))
///     .execute(cx, &conn).await?;
///
/// // Delete a specific model instance
/// DeleteBuilder::from_model(&hero)
///     .execute(cx, &conn).await?;
///
/// // Delete with RETURNING
/// let rows = delete!(Hero)
///     .filter(Expr::col("status").eq("inactive"))
///     .returning()
///     .execute_returning(cx, &conn).await?;
/// ```
#[derive(Debug)]
pub struct DeleteBuilder<'a, M: Model> {
    model: Option<&'a M>,
    where_clause: Option<Where>,
    returning: bool,
    _marker: PhantomData<M>,
}

impl<'a, M: Model> DeleteBuilder<'a, M> {
    /// Create a new DELETE builder for the model type.
    pub fn new() -> Self {
        Self {
            model: None,
            where_clause: None,
            returning: false,
            _marker: PhantomData,
        }
    }

    /// Create a DELETE builder for a specific model instance.
    ///
    /// This automatically adds a WHERE clause matching the primary key.
    pub fn from_model(model: &'a M) -> Self {
        Self {
            model: Some(model),
            where_clause: None,
            returning: false,
            _marker: PhantomData,
        }
    }

    /// Add a WHERE condition.
    pub fn filter(mut self, expr: Expr) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => existing.and(expr),
            None => Where::new(expr),
        });
        self
    }

    /// Add RETURNING * clause to return the deleted row(s).
    pub fn returning(mut self) -> Self {
        self.returning = true;
        self
    }

    /// Build the DELETE SQL and parameters with default dialect (Postgres).
    pub fn build(&self) -> (String, Vec<Value>) {
        self.build_with_dialect(Dialect::default())
    }

    /// Build the DELETE SQL and parameters with specific dialect.
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let mut sql = format!("DELETE FROM {}", M::TABLE_NAME);
        let mut params = Vec::new();

        if let Some(where_clause) = &self.where_clause {
            let (where_sql, where_params) = where_clause.build_with_dialect(dialect, 0);
            sql.push_str(" WHERE ");
            sql.push_str(&where_sql);
            params = where_params;
        } else if let Some(model) = &self.model {
            // Delete by primary key
            let pk = M::PRIMARY_KEY;
            let pk_values = model.primary_key_value();
            let pk_conditions: Vec<_> = pk
                .iter()
                .zip(pk_values.iter())
                .enumerate()
                .map(|(i, (col, _))| format!("{} = {}", col, dialect.placeholder(i + 1)))
                .collect();

            if !pk_conditions.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&pk_conditions.join(" AND "));
                params.extend(pk_values);
            }
        }

        // Add RETURNING clause if requested
        if self.returning {
            sql.push_str(" RETURNING *");
        }

        (sql, params)
    }

    /// Execute the DELETE and return rows affected.
    pub async fn execute<C: Connection>(
        self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<u64, sqlmodel_core::Error> {
        let (sql, params) = self.build();
        conn.execute(cx, &sql, &params).await
    }

    /// Execute the DELETE with RETURNING and get the deleted rows.
    pub async fn execute_returning<C: Connection>(
        mut self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Vec<Row>, sqlmodel_core::Error> {
        self.returning = true;
        let (sql, params) = self.build();
        conn.query(cx, &sql, &params).await
    }
}

impl<M: Model> Default for DeleteBuilder<'_, M> {
    fn default() -> Self {
        Self::new()
    }
}

/// Query builder for raw SQL with type-safe parameter binding.
#[derive(Debug)]
pub struct QueryBuilder {
    sql: String,
    params: Vec<Value>,
}

impl QueryBuilder {
    /// Create a new query builder with the given SQL.
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    /// Bind a parameter value.
    pub fn bind(mut self, value: impl Into<Value>) -> Self {
        self.params.push(value.into());
        self
    }

    /// Bind multiple parameter values.
    pub fn bind_all(mut self, values: impl IntoIterator<Item = Value>) -> Self {
        self.params.extend(values);
        self
    }

    /// Get the SQL and parameters.
    pub fn build(self) -> (String, Vec<Value>) {
        (self.sql, self.params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Dialect;
    use sqlmodel_core::field::FieldInfo;
    use sqlmodel_core::types::SqlType;

    // Mock model for testing
    struct TestHero {
        id: Option<i64>,
        name: String,
        age: i32,
    }

    impl Model for TestHero {
        const TABLE_NAME: &'static str = "heroes";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt)
                    .primary_key(true)
                    .auto_increment(true)
                    .nullable(true),
                FieldInfo::new("name", "name", SqlType::Text),
                FieldInfo::new("age", "age", SqlType::Integer),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![
                ("id", self.id.map(Value::BigInt).unwrap_or(Value::Null)),
                ("name", Value::Text(self.name.clone())),
                ("age", Value::Int(self.age)),
            ]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            unimplemented!()
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![self.id.map(Value::BigInt).unwrap_or(Value::Null)]
        }

        fn is_new(&self) -> bool {
            self.id.is_none()
        }
    }

    #[test]
    fn test_insert_basic() {
        let hero = TestHero {
            id: None,
            name: "Spider-Man".to_string(),
            age: 25,
        };
        let (sql, params) = InsertBuilder::new(&hero).build();

        assert_eq!(sql, "INSERT INTO heroes (name, age) VALUES ($1, $2)");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_insert_returning() {
        let hero = TestHero {
            id: None,
            name: "Spider-Man".to_string(),
            age: 25,
        };
        let (sql, _) = InsertBuilder::new(&hero).returning().build();

        assert!(sql.ends_with(" RETURNING *"));
    }

    #[test]
    fn test_insert_on_conflict_do_nothing() {
        let hero = TestHero {
            id: None,
            name: "Spider-Man".to_string(),
            age: 25,
        };
        let (sql, _) = InsertBuilder::new(&hero).on_conflict_do_nothing().build();

        assert!(sql.contains("ON CONFLICT DO NOTHING"));
    }

    #[test]
    fn test_insert_on_conflict_do_update() {
        let hero = TestHero {
            id: None,
            name: "Spider-Man".to_string(),
            age: 25,
        };
        let (sql, _) = InsertBuilder::new(&hero)
            .on_conflict_do_update(&["name", "age"])
            .build();

        assert!(sql.contains("ON CONFLICT (id) DO UPDATE SET"));
        assert!(sql.contains("name = EXCLUDED.name"));
        assert!(sql.contains("age = EXCLUDED.age"));
    }

    #[test]
    fn test_insert_many() {
        let heroes = vec![
            TestHero {
                id: None,
                name: "Spider-Man".to_string(),
                age: 25,
            },
            TestHero {
                id: None,
                name: "Iron Man".to_string(),
                age: 45,
            },
        ];
        let (sql, params) = InsertManyBuilder::new(&heroes).build();

        assert!(sql.starts_with("INSERT INTO heroes (name, age) VALUES"));
        assert!(sql.contains("($1, $2), ($3, $4)"));
        assert_eq!(params.len(), 4);
    }

    #[test]
    fn test_update_basic() {
        let hero = TestHero {
            id: Some(1),
            name: "Spider-Man".to_string(),
            age: 26,
        };
        let (sql, params) = UpdateBuilder::new(&hero).build();

        assert!(sql.starts_with("UPDATE heroes SET"));
        assert!(sql.contains("WHERE id = "));
        assert!(params.len() >= 2); // At least name, age, and id
    }

    #[test]
    fn test_update_explicit_set() {
        let (sql, params) = UpdateBuilder::<TestHero>::empty()
            .set("age", 30)
            .filter(Expr::col("id").eq(1))
            .build_with_dialect(Dialect::Postgres);

        assert_eq!(sql, "UPDATE heroes SET age = $1 WHERE \"id\" = $2");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_update_returning() {
        let hero = TestHero {
            id: Some(1),
            name: "Spider-Man".to_string(),
            age: 26,
        };
        let (sql, _) = UpdateBuilder::new(&hero).returning().build();

        assert!(sql.ends_with(" RETURNING *"));
    }

    #[test]
    fn test_delete_basic() {
        let (sql, _) = DeleteBuilder::<TestHero>::new()
            .filter(Expr::col("age").lt(18))
            .build_with_dialect(Dialect::Postgres);

        assert_eq!(sql, "DELETE FROM heroes WHERE \"age\" < $1");
    }

    #[test]
    fn test_delete_from_model() {
        let hero = TestHero {
            id: Some(42),
            name: "Spider-Man".to_string(),
            age: 25,
        };
        let (sql, params) = DeleteBuilder::from_model(&hero).build();

        assert!(sql.contains("WHERE id = $1"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_delete_returning() {
        let (sql, _) = DeleteBuilder::<TestHero>::new()
            .filter(Expr::col("status").eq("inactive"))
            .returning()
            .build_with_dialect(Dialect::Postgres);

        assert!(sql.ends_with(" RETURNING *"));
    }

    #[test]
    fn test_dialect_sqlite() {
        let hero = TestHero {
            id: None,
            name: "Spider-Man".to_string(),
            age: 25,
        };
        let (sql, _) = InsertBuilder::new(&hero).build_with_dialect(Dialect::Sqlite);

        assert!(sql.contains("?1"));
        assert!(sql.contains("?2"));
    }

    #[test]
    fn test_dialect_mysql() {
        let hero = TestHero {
            id: None,
            name: "Spider-Man".to_string(),
            age: 25,
        };
        let (sql, _) = InsertBuilder::new(&hero).build_with_dialect(Dialect::Mysql);

        // MySQL uses ? without numbers
        assert!(sql.contains("?"));
        assert!(!sql.contains("$1"));
    }
}
