//! CREATE TABLE statement builder.

use sqlmodel_core::{FieldInfo, Model};
use std::marker::PhantomData;

/// Builder for CREATE TABLE statements.
#[derive(Debug)]
pub struct CreateTable<M: Model> {
    if_not_exists: bool,
    _marker: PhantomData<M>,
}

impl<M: Model> CreateTable<M> {
    /// Create a new CREATE TABLE builder.
    pub fn new() -> Self {
        Self {
            if_not_exists: false,
            _marker: PhantomData,
        }
    }

    /// Add IF NOT EXISTS clause.
    pub fn if_not_exists(mut self) -> Self {
        self.if_not_exists = true;
        self
    }

    /// Build the CREATE TABLE SQL.
    pub fn build(&self) -> String {
        let mut sql = String::from("CREATE TABLE ");

        if self.if_not_exists {
            sql.push_str("IF NOT EXISTS ");
        }

        sql.push_str(&format!("\"{}\"", M::TABLE_NAME));
        sql.push_str(" (\n");

        let fields = M::fields();
        let mut column_defs = Vec::new();
        let mut constraints = Vec::new();

        for field in fields {
            column_defs.push(self.column_definition(field));

            // Collect constraints
            if field.unique && !field.primary_key {
                constraints.push(format!(
                    "CONSTRAINT \"uk_{}\" UNIQUE (\"{}\")",
                    field.column_name, field.column_name
                ));
            }

            if let Some(fk) = field.foreign_key {
                let parts: Vec<&str> = fk.split('.').collect();
                if parts.len() == 2 {
                    constraints.push(format!(
                        "CONSTRAINT \"fk_{}_{}\" FOREIGN KEY (\"{}\") REFERENCES \"{}\"(\"{}\")",
                        M::TABLE_NAME,
                        field.column_name,
                        field.column_name,
                        parts[0],
                        parts[1]
                    ));
                }
            }
        }

        // Add primary key constraint
        let pk_cols = M::PRIMARY_KEY;
        if !pk_cols.is_empty() {
            let quoted_pk: Vec<String> = pk_cols.iter().map(|c| format!("\"{}\"", c)).collect();
            constraints.insert(0, format!("PRIMARY KEY ({})", quoted_pk.join(", ")));
        }

        // Combine column definitions and constraints
        let all_parts: Vec<_> = column_defs.into_iter().chain(constraints).collect();

        sql.push_str(&all_parts.join(",\n  "));
        sql.push_str("\n)");

        sql
    }

    fn column_definition(&self, field: &FieldInfo) -> String {
        let mut def = format!("  \"{}\" {}", field.column_name, field.sql_type.sql_name());

        if !field.nullable && !field.auto_increment {
            def.push_str(" NOT NULL");
        }

        if field.auto_increment {
            // Use AUTOINCREMENT for SQLite, SERIAL/GENERATED for PostgreSQL
            // For now, use a simple approach
            def.push_str(" AUTOINCREMENT");
        }

        if let Some(default) = field.default {
            def.push_str(" DEFAULT ");
            def.push_str(default);
        }

        def
    }
}

impl<M: Model> Default for CreateTable<M> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlmodel_core::{FieldInfo, Row, SqlType, Value};

    // Test model for CREATE TABLE generation
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
                    nullable: true,
                    primary_key: true,
                    auto_increment: true,
                    unique: false,
                    default: None,
                    foreign_key: None,
                    index: None,
                },
                FieldInfo {
                    name: "name",
                    column_name: "name",
                    sql_type: SqlType::Text,
                    nullable: false,
                    primary_key: false,
                    auto_increment: false,
                    unique: true,
                    default: None,
                    foreign_key: None,
                    index: None,
                },
                FieldInfo {
                    name: "age",
                    column_name: "age",
                    sql_type: SqlType::Integer,
                    nullable: true,
                    primary_key: false,
                    auto_increment: false,
                    unique: false,
                    default: None,
                    foreign_key: None,
                    index: None,
                },
                FieldInfo {
                    name: "team_id",
                    column_name: "team_id",
                    sql_type: SqlType::BigInt,
                    nullable: true,
                    primary_key: false,
                    auto_increment: false,
                    unique: false,
                    default: None,
                    foreign_key: Some("teams.id"),
                    index: None,
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
    fn test_create_table_basic() {
        let sql = CreateTable::<TestHero>::new().build();
        assert!(sql.starts_with("CREATE TABLE \"heroes\""));
        assert!(sql.contains("\"id\" BIGINT"));
        assert!(sql.contains("\"name\" TEXT NOT NULL"));
        assert!(sql.contains("\"age\" INTEGER"));
        assert!(sql.contains("\"team_id\" BIGINT"));
    }

    #[test]
    fn test_create_table_if_not_exists() {
        let sql = CreateTable::<TestHero>::new().if_not_exists().build();
        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS \"heroes\""));
    }

    #[test]
    fn test_create_table_primary_key() {
        let sql = CreateTable::<TestHero>::new().build();
        assert!(sql.contains("PRIMARY KEY (\"id\")"));
    }

    #[test]
    fn test_create_table_unique_constraint() {
        let sql = CreateTable::<TestHero>::new().build();
        assert!(sql.contains("CONSTRAINT \"uk_name\" UNIQUE (\"name\")"));
    }

    #[test]
    fn test_create_table_foreign_key() {
        let sql = CreateTable::<TestHero>::new().build();
        assert!(sql.contains("FOREIGN KEY (\"team_id\") REFERENCES \"teams\"(\"id\")"));
    }

    #[test]
    fn test_create_table_auto_increment() {
        let sql = CreateTable::<TestHero>::new().build();
        assert!(sql.contains("AUTOINCREMENT"));
    }

    #[test]
    fn test_schema_builder_single_table() {
        let statements = SchemaBuilder::new().create_table::<TestHero>().build();
        assert_eq!(statements.len(), 1);
        assert!(statements[0].contains("CREATE TABLE IF NOT EXISTS \"heroes\""));
    }

    #[test]
    fn test_schema_builder_with_index() {
        let statements = SchemaBuilder::new()
            .create_table::<TestHero>()
            .create_index("idx_hero_name", "heroes", &["name"], false)
            .build();
        assert_eq!(statements.len(), 2);
        assert!(
            statements[1]
                .contains("CREATE INDEX IF NOT EXISTS \"idx_hero_name\" ON \"heroes\" (\"name\")")
        );
    }

    #[test]
    fn test_schema_builder_unique_index() {
        let statements = SchemaBuilder::new()
            .create_index("idx_hero_email", "heroes", &["email"], true)
            .build();
        assert!(statements[0].contains("CREATE UNIQUE INDEX"));
    }

    #[test]
    fn test_schema_builder_raw_sql() {
        let statements = SchemaBuilder::new()
            .raw("ALTER TABLE heroes ADD COLUMN power TEXT")
            .build();
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0], "ALTER TABLE heroes ADD COLUMN power TEXT");
    }

    #[test]
    fn test_schema_builder_multi_column_index() {
        let statements = SchemaBuilder::new()
            .create_index("idx_hero_name_age", "heroes", &["name", "age"], false)
            .build();
        assert!(statements[0].contains("ON \"heroes\" (\"name\", \"age\")"));
    }

    // Test model with default values
    struct TestWithDefault;

    impl Model for TestWithDefault {
        const TABLE_NAME: &'static str = "settings";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo {
                    name: "id",
                    column_name: "id",
                    sql_type: SqlType::Integer,
                    nullable: false,
                    primary_key: true,
                    auto_increment: false,
                    unique: false,
                    default: None,
                    foreign_key: None,
                    index: None,
                },
                FieldInfo {
                    name: "is_active",
                    column_name: "is_active",
                    sql_type: SqlType::Boolean,
                    nullable: false,
                    primary_key: false,
                    auto_increment: false,
                    unique: false,
                    default: Some("true"),
                    foreign_key: None,
                    index: None,
                },
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(TestWithDefault)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_create_table_default_value() {
        let sql = CreateTable::<TestWithDefault>::new().build();
        assert!(sql.contains("\"is_active\" BOOLEAN NOT NULL DEFAULT true"));
    }
}

/// Builder for multiple schema operations.
#[derive(Debug, Default)]
pub struct SchemaBuilder {
    statements: Vec<String>,
}

impl SchemaBuilder {
    /// Create a new schema builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a CREATE TABLE statement.
    pub fn create_table<M: Model>(mut self) -> Self {
        self.statements
            .push(CreateTable::<M>::new().if_not_exists().build());
        self
    }

    /// Add a raw SQL statement.
    pub fn raw(mut self, sql: impl Into<String>) -> Self {
        self.statements.push(sql.into());
        self
    }

    /// Add an index creation statement.
    pub fn create_index(mut self, name: &str, table: &str, columns: &[&str], unique: bool) -> Self {
        let unique_str = if unique { "UNIQUE " } else { "" };
        let quoted_cols: Vec<String> = columns.iter().map(|c| format!("\"{}\"", c)).collect();
        self.statements.push(format!(
            "CREATE {}INDEX IF NOT EXISTS \"{}\" ON \"{}\" ({})",
            unique_str,
            name,
            table,
            quoted_cols.join(", ")
        ));
        self
    }

    /// Get all SQL statements.
    pub fn build(self) -> Vec<String> {
        self.statements
    }
}
