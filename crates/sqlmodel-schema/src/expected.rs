//! Expected schema extraction from Model definitions.
//!
//! This module provides utilities to extract the "expected" database schema
//! from Rust Model definitions, which can then be compared against the
//! actual database schema obtained via introspection.

use crate::introspect::{
    ColumnInfo, DatabaseSchema, Dialect, ForeignKeyInfo, IndexInfo, ParsedSqlType, TableInfo,
    UniqueConstraintInfo,
};
use sqlmodel_core::{FieldInfo, Model};

// ============================================================================
// Extension Trait for Model
// ============================================================================

/// Extension trait that adds schema extraction to Model types.
///
/// This trait is automatically implemented for all types that implement `Model`.
/// It provides the `table_schema()` method to extract the expected table schema
/// from the Model's metadata.
///
/// # Example
///
/// ```ignore
/// use sqlmodel::Model;
/// use sqlmodel_schema::expected::ModelSchema;
///
/// #[derive(Model)]
/// #[sqlmodel(table = "heroes")]
/// struct Hero {
///     #[sqlmodel(primary_key, auto_increment)]
///     id: Option<i64>,
///     name: String,
/// }
///
/// // Extract the expected schema
/// let schema = Hero::table_schema();
/// assert_eq!(schema.name, "heroes");
/// ```
pub trait ModelSchema: Model {
    /// Get the expected table schema for this model.
    fn table_schema() -> TableInfo {
        table_schema_from_model::<Self>()
    }
}

// Blanket implementation for all Model types
impl<M: Model> ModelSchema for M {}

// ============================================================================
// Schema Extraction Functions
// ============================================================================

/// Extract a TableInfo from a Model type.
pub fn table_schema_from_model<M: Model>() -> TableInfo {
    table_schema_from_fields(M::TABLE_NAME, M::fields(), M::PRIMARY_KEY)
}

/// Convert field metadata to a TableInfo.
///
/// This is the core conversion function that transforms the compile-time
/// FieldInfo array into a runtime TableInfo structure compatible with
/// database introspection.
pub fn table_schema_from_fields(
    table_name: &str,
    fields: &[FieldInfo],
    primary_key_cols: &[&str],
) -> TableInfo {
    let mut columns = Vec::with_capacity(fields.len());
    let mut foreign_keys = Vec::new();
    let mut unique_constraints = Vec::new();
    let mut indexes = Vec::new();

    for field in fields {
        // Convert FieldInfo to ColumnInfo
        let sql_type = field.effective_sql_type();
        columns.push(ColumnInfo {
            name: field.column_name.to_string(),
            sql_type: sql_type.clone(),
            parsed_type: ParsedSqlType::parse(&sql_type),
            nullable: field.nullable,
            default: field.default.map(String::from),
            primary_key: field.primary_key,
            auto_increment: field.auto_increment,
            comment: None,
        });

        // Extract foreign key if present
        if let Some(fk_ref) = field.foreign_key {
            if let Some((ref_table, ref_col)) = parse_fk_reference(fk_ref) {
                foreign_keys.push(ForeignKeyInfo {
                    name: Some(format!("fk_{}_{}", table_name, field.column_name)),
                    column: field.column_name.to_string(),
                    foreign_table: ref_table,
                    foreign_column: ref_col,
                    on_delete: field.on_delete.map(|a| a.as_sql().to_string()),
                    on_update: field.on_update.map(|a| a.as_sql().to_string()),
                });
            }
        }

        // Extract unique constraint if present (and not part of PK)
        if field.unique && !field.primary_key {
            unique_constraints.push(UniqueConstraintInfo {
                name: Some(format!("uk_{}", field.column_name)),
                columns: vec![field.column_name.to_string()],
            });
        }

        // Extract index if present
        if let Some(idx_name) = field.index {
            indexes.push(IndexInfo {
                name: idx_name.to_string(),
                columns: vec![field.column_name.to_string()],
                unique: false,
                index_type: None,
                primary: false,
            });
        }
    }

    TableInfo {
        name: table_name.to_string(),
        columns,
        primary_key: primary_key_cols.iter().map(|s| s.to_string()).collect(),
        foreign_keys,
        unique_constraints,
        check_constraints: Vec::new(),
        indexes,
        comment: None,
    }
}

/// Parse a foreign key reference string (e.g., "users.id") into (table, column).
fn parse_fk_reference(reference: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = reference.split('.').collect();
    if parts.len() == 2 {
        Some((parts[0].to_string(), parts[1].to_string()))
    } else {
        None
    }
}

// ============================================================================
// Schema Aggregation
// ============================================================================

/// Build a DatabaseSchema from a single Model.
///
/// # Example
///
/// ```ignore
/// let schema = expected_schema::<Hero>(Dialect::Sqlite);
/// ```
pub fn expected_schema<M: Model>(dialect: Dialect) -> DatabaseSchema {
    let mut schema = DatabaseSchema::new(dialect);
    let table_info = table_schema_from_model::<M>();
    schema.tables.insert(table_info.name.clone(), table_info);
    schema
}

/// Trait for tuples of Models to aggregate their schemas.
///
/// This allows building a complete expected schema from multiple models.
pub trait ModelTuple {
    /// Get all table schemas from this tuple of models.
    fn all_table_schemas() -> Vec<TableInfo>;

    /// Build a complete database schema from all models in this tuple.
    fn database_schema(dialect: Dialect) -> DatabaseSchema {
        let mut schema = DatabaseSchema::new(dialect);
        for table in Self::all_table_schemas() {
            schema.tables.insert(table.name.clone(), table);
        }
        schema
    }
}

// Implement for single model
impl<A: Model> ModelTuple for (A,) {
    fn all_table_schemas() -> Vec<TableInfo> {
        vec![table_schema_from_model::<A>()]
    }
}

// Implement for 2-tuple
impl<A: Model, B: Model> ModelTuple for (A, B) {
    fn all_table_schemas() -> Vec<TableInfo> {
        vec![
            table_schema_from_model::<A>(),
            table_schema_from_model::<B>(),
        ]
    }
}

// Implement for 3-tuple
impl<A: Model, B: Model, C: Model> ModelTuple for (A, B, C) {
    fn all_table_schemas() -> Vec<TableInfo> {
        vec![
            table_schema_from_model::<A>(),
            table_schema_from_model::<B>(),
            table_schema_from_model::<C>(),
        ]
    }
}

// Implement for 4-tuple
impl<A: Model, B: Model, C: Model, D: Model> ModelTuple for (A, B, C, D) {
    fn all_table_schemas() -> Vec<TableInfo> {
        vec![
            table_schema_from_model::<A>(),
            table_schema_from_model::<B>(),
            table_schema_from_model::<C>(),
            table_schema_from_model::<D>(),
        ]
    }
}

// Implement for 5-tuple
impl<A: Model, B: Model, C: Model, D: Model, E: Model> ModelTuple for (A, B, C, D, E) {
    fn all_table_schemas() -> Vec<TableInfo> {
        vec![
            table_schema_from_model::<A>(),
            table_schema_from_model::<B>(),
            table_schema_from_model::<C>(),
            table_schema_from_model::<D>(),
            table_schema_from_model::<E>(),
        ]
    }
}

// Implement for 6-tuple
impl<A: Model, B: Model, C: Model, D: Model, E: Model, F: Model> ModelTuple for (A, B, C, D, E, F) {
    fn all_table_schemas() -> Vec<TableInfo> {
        vec![
            table_schema_from_model::<A>(),
            table_schema_from_model::<B>(),
            table_schema_from_model::<C>(),
            table_schema_from_model::<D>(),
            table_schema_from_model::<E>(),
            table_schema_from_model::<F>(),
        ]
    }
}

// Implement for 7-tuple
impl<A: Model, B: Model, C: Model, D: Model, E: Model, F: Model, G: Model> ModelTuple
    for (A, B, C, D, E, F, G)
{
    fn all_table_schemas() -> Vec<TableInfo> {
        vec![
            table_schema_from_model::<A>(),
            table_schema_from_model::<B>(),
            table_schema_from_model::<C>(),
            table_schema_from_model::<D>(),
            table_schema_from_model::<E>(),
            table_schema_from_model::<F>(),
            table_schema_from_model::<G>(),
        ]
    }
}

// Implement for 8-tuple
impl<A: Model, B: Model, C: Model, D: Model, E: Model, F: Model, G: Model, H: Model> ModelTuple
    for (A, B, C, D, E, F, G, H)
{
    fn all_table_schemas() -> Vec<TableInfo> {
        vec![
            table_schema_from_model::<A>(),
            table_schema_from_model::<B>(),
            table_schema_from_model::<C>(),
            table_schema_from_model::<D>(),
            table_schema_from_model::<E>(),
            table_schema_from_model::<F>(),
            table_schema_from_model::<G>(),
            table_schema_from_model::<H>(),
        ]
    }
}

// ============================================================================
// Type Normalization
// ============================================================================

/// Normalize a SQL type for comparison across dialects.
///
/// This handles common type aliases and dialect-specific variations
/// to enable meaningful comparison between expected and actual schemas.
pub fn normalize_sql_type(sql_type: &str, dialect: Dialect) -> String {
    let upper = sql_type.to_uppercase();

    match dialect {
        Dialect::Sqlite => {
            // SQLite type affinity normalization
            if upper.contains("INT") {
                "INTEGER".to_string()
            } else if upper.contains("CHAR") || upper.contains("TEXT") || upper.contains("CLOB") {
                "TEXT".to_string()
            } else if upper.contains("REAL") || upper.contains("FLOAT") || upper.contains("DOUB") {
                "REAL".to_string()
            } else if upper.contains("BLOB") || upper.is_empty() {
                "BLOB".to_string()
            } else {
                // Numeric affinity for anything else
                upper
            }
        }
        Dialect::Postgres => {
            // PostgreSQL type normalizations
            match upper.as_str() {
                "INT" | "INT4" => "INTEGER".to_string(),
                "INT8" => "BIGINT".to_string(),
                "INT2" => "SMALLINT".to_string(),
                "FLOAT4" => "REAL".to_string(),
                "FLOAT8" => "DOUBLE PRECISION".to_string(),
                "BOOL" => "BOOLEAN".to_string(),
                "SERIAL" => "INTEGER".to_string(), // Serial is INTEGER with sequence
                "BIGSERIAL" => "BIGINT".to_string(),
                "SMALLSERIAL" => "SMALLINT".to_string(),
                _ => upper,
            }
        }
        Dialect::Mysql => {
            // MySQL type normalizations
            match upper.as_str() {
                "INTEGER" => "INT".to_string(),
                "BOOL" | "BOOLEAN" => "TINYINT".to_string(),
                _ => upper,
            }
        }
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use sqlmodel_core::{ReferentialAction, Row, SqlType, Value};

    // Test model
    struct TestHero;

    impl Model for TestHero {
        const TABLE_NAME: &'static str = "heroes";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo {
                    name: "id",
                    column_name: "id",
                    sql_type: SqlType::BigInt,
                    sql_type_override: None,
                    precision: None,
                    scale: None,
                    nullable: true,
                    primary_key: true,
                    auto_increment: true,
                    unique: false,
                    default: None,
                    foreign_key: None,
                    on_delete: None,
                    on_update: None,
                    index: None,
                    alias: None,
                    validation_alias: None,
                    serialization_alias: None,
                    computed: false,
                },
                FieldInfo {
                    name: "name",
                    column_name: "name",
                    sql_type: SqlType::Text,
                    sql_type_override: Some("VARCHAR(100)"),
                    precision: None,
                    scale: None,
                    nullable: false,
                    primary_key: false,
                    auto_increment: false,
                    unique: true,
                    default: None,
                    foreign_key: None,
                    on_delete: None,
                    on_update: None,
                    index: None,
                    alias: None,
                    validation_alias: None,
                    serialization_alias: None,
                    computed: false,
                },
                FieldInfo {
                    name: "age",
                    column_name: "age",
                    sql_type: SqlType::Integer,
                    sql_type_override: None,
                    precision: None,
                    scale: None,
                    nullable: true,
                    primary_key: false,
                    auto_increment: false,
                    unique: false,
                    default: None,
                    foreign_key: None,
                    on_delete: None,
                    on_update: None,
                    index: Some("idx_heroes_age"),
                    alias: None,
                    validation_alias: None,
                    serialization_alias: None,
                    computed: false,
                },
                FieldInfo {
                    name: "team_id",
                    column_name: "team_id",
                    sql_type: SqlType::BigInt,
                    sql_type_override: None,
                    precision: None,
                    scale: None,
                    nullable: true,
                    primary_key: false,
                    auto_increment: false,
                    unique: false,
                    default: None,
                    foreign_key: Some("teams.id"),
                    on_delete: Some(ReferentialAction::Cascade),
                    on_update: None,
                    index: None,
                    alias: None,
                    validation_alias: None,
                    serialization_alias: None,
                    computed: false,
                },
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(TestHero)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_model_schema_table_name() {
        let schema = TestHero::table_schema();
        assert_eq!(schema.name, "heroes");
    }

    #[test]
    fn test_model_schema_columns() {
        let schema = TestHero::table_schema();
        assert_eq!(schema.columns.len(), 4);

        let id_col = schema.column("id").unwrap();
        assert_eq!(id_col.sql_type, "BIGINT");
        assert!(id_col.primary_key);
        assert!(id_col.auto_increment);

        let name_col = schema.column("name").unwrap();
        assert_eq!(name_col.sql_type, "VARCHAR(100)");
        assert!(!name_col.nullable);
    }

    #[test]
    fn test_model_schema_primary_key() {
        let schema = TestHero::table_schema();
        assert_eq!(schema.primary_key, vec!["id"]);
    }

    #[test]
    fn test_model_schema_foreign_keys() {
        let schema = TestHero::table_schema();
        assert_eq!(schema.foreign_keys.len(), 1);

        let fk = &schema.foreign_keys[0];
        assert_eq!(fk.column, "team_id");
        assert_eq!(fk.foreign_table, "teams");
        assert_eq!(fk.foreign_column, "id");
        assert_eq!(fk.on_delete, Some("CASCADE".to_string()));
    }

    #[test]
    fn test_model_schema_unique_constraints() {
        let schema = TestHero::table_schema();
        assert_eq!(schema.unique_constraints.len(), 1);

        let uk = &schema.unique_constraints[0];
        assert_eq!(uk.columns, vec!["name"]);
    }

    #[test]
    fn test_model_schema_indexes() {
        let schema = TestHero::table_schema();
        assert_eq!(schema.indexes.len(), 1);

        let idx = &schema.indexes[0];
        assert_eq!(idx.name, "idx_heroes_age");
        assert_eq!(idx.columns, vec!["age"]);
        assert!(!idx.unique);
    }

    #[test]
    fn test_expected_schema() {
        let schema = expected_schema::<TestHero>(Dialect::Sqlite);
        assert_eq!(schema.dialect, Dialect::Sqlite);
        assert!(schema.table("heroes").is_some());
    }

    #[test]
    fn test_model_tuple_two() {
        struct TestTeam;

        impl Model for TestTeam {
            const TABLE_NAME: &'static str = "teams";
            const PRIMARY_KEY: &'static [&'static str] = &["id"];

            fn fields() -> &'static [FieldInfo] {
                static FIELDS: &[FieldInfo] = &[FieldInfo {
                    name: "id",
                    column_name: "id",
                    sql_type: SqlType::BigInt,
                    sql_type_override: None,
                    precision: None,
                    scale: None,
                    nullable: true,
                    primary_key: true,
                    auto_increment: true,
                    unique: false,
                    default: None,
                    foreign_key: None,
                    on_delete: None,
                    on_update: None,
                    index: None,
                    alias: None,
                    validation_alias: None,
                    serialization_alias: None,
                    computed: false,
                }];
                FIELDS
            }

            fn to_row(&self) -> Vec<(&'static str, Value)> {
                vec![]
            }

            fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
                Ok(TestTeam)
            }

            fn primary_key_value(&self) -> Vec<Value> {
                vec![]
            }

            fn is_new(&self) -> bool {
                true
            }
        }

        let schema = <(TestHero, TestTeam)>::database_schema(Dialect::Postgres);
        assert_eq!(schema.tables.len(), 2);
        assert!(schema.table("heroes").is_some());
        assert!(schema.table("teams").is_some());
    }

    #[test]
    fn test_normalize_sql_type_sqlite() {
        assert_eq!(normalize_sql_type("INTEGER", Dialect::Sqlite), "INTEGER");
        assert_eq!(normalize_sql_type("INT", Dialect::Sqlite), "INTEGER");
        assert_eq!(normalize_sql_type("BIGINT", Dialect::Sqlite), "INTEGER");
        assert_eq!(normalize_sql_type("VARCHAR(100)", Dialect::Sqlite), "TEXT");
        assert_eq!(normalize_sql_type("TEXT", Dialect::Sqlite), "TEXT");
        assert_eq!(normalize_sql_type("REAL", Dialect::Sqlite), "REAL");
        assert_eq!(normalize_sql_type("FLOAT", Dialect::Sqlite), "REAL");
    }

    #[test]
    fn test_normalize_sql_type_postgres() {
        assert_eq!(normalize_sql_type("INT", Dialect::Postgres), "INTEGER");
        assert_eq!(normalize_sql_type("INT4", Dialect::Postgres), "INTEGER");
        assert_eq!(normalize_sql_type("INT8", Dialect::Postgres), "BIGINT");
        assert_eq!(
            normalize_sql_type("FLOAT8", Dialect::Postgres),
            "DOUBLE PRECISION"
        );
        assert_eq!(normalize_sql_type("BOOL", Dialect::Postgres), "BOOLEAN");
        assert_eq!(normalize_sql_type("SERIAL", Dialect::Postgres), "INTEGER");
    }

    #[test]
    fn test_normalize_sql_type_mysql() {
        assert_eq!(normalize_sql_type("INTEGER", Dialect::Mysql), "INT");
        assert_eq!(normalize_sql_type("BOOLEAN", Dialect::Mysql), "TINYINT");
        assert_eq!(normalize_sql_type("BOOL", Dialect::Mysql), "TINYINT");
    }

    #[test]
    fn test_parse_fk_reference() {
        assert_eq!(
            parse_fk_reference("users.id"),
            Some(("users".to_string(), "id".to_string()))
        );
        assert_eq!(
            parse_fk_reference("teams.team_id"),
            Some(("teams".to_string(), "team_id".to_string()))
        );
        assert_eq!(parse_fk_reference("invalid"), None);
        assert_eq!(parse_fk_reference("too.many.parts"), None);
    }
}
