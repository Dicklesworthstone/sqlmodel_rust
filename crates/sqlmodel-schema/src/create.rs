//! CREATE TABLE statement builder.

use sqlmodel_core::{FieldInfo, Model, quote_ident};
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

        sql.push_str(&quote_ident(M::TABLE_NAME));
        sql.push_str(" (\n");

        let fields = M::fields();
        let mut column_defs = Vec::new();
        let mut constraints = Vec::new();

        for field in fields {
            column_defs.push(self.column_definition(field));

            // Collect constraints
            if field.unique && !field.primary_key {
                let constraint_name = format!("uk_{}", field.column_name);
                let constraint = format!(
                    "CONSTRAINT {} UNIQUE ({})",
                    quote_ident(&constraint_name),
                    quote_ident(field.column_name)
                );
                constraints.push(constraint);
            }

            if let Some(fk) = field.foreign_key {
                let parts: Vec<&str> = fk.split('.').collect();
                if parts.len() == 2 {
                    let constraint_name = format!("fk_{}_{}", M::TABLE_NAME, field.column_name);
                    let mut fk_sql = format!(
                        "CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({})",
                        quote_ident(&constraint_name),
                        quote_ident(field.column_name),
                        quote_ident(parts[0]),
                        quote_ident(parts[1])
                    );

                    // Add ON DELETE action if specified
                    if let Some(on_delete) = field.on_delete {
                        fk_sql.push_str(" ON DELETE ");
                        fk_sql.push_str(on_delete.as_sql());
                    }

                    // Add ON UPDATE action if specified
                    if let Some(on_update) = field.on_update {
                        fk_sql.push_str(" ON UPDATE ");
                        fk_sql.push_str(on_update.as_sql());
                    }

                    constraints.push(fk_sql);
                }
            }
        }

        // Add primary key constraint
        let pk_cols = M::PRIMARY_KEY;
        if !pk_cols.is_empty() {
            let quoted_pk: Vec<String> = pk_cols.iter().map(|c| quote_ident(c)).collect();
            let mut constraint = String::new();
            constraint.push_str("PRIMARY KEY (");
            constraint.push_str(&quoted_pk.join(", "));
            constraint.push(')');
            constraints.insert(0, constraint);
        }

        // Combine column definitions and constraints
        let all_parts: Vec<_> = column_defs.into_iter().chain(constraints).collect();

        sql.push_str(&all_parts.join(",\n  "));
        sql.push_str("\n)");

        sql
    }

    fn column_definition(&self, field: &FieldInfo) -> String {
        let sql_type = field.effective_sql_type();
        let mut def = String::from("  ");
        def.push_str(&quote_ident(field.column_name));
        def.push(' ');
        def.push_str(&sql_type);

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
                FieldInfo::new("id", "id", SqlType::BigInt)
                    .nullable(true)
                    .primary_key(true)
                    .auto_increment(true),
                FieldInfo::new("name", "name", SqlType::Text).unique(true),
                FieldInfo::new("age", "age", SqlType::Integer).nullable(true),
                FieldInfo::new("team_id", "team_id", SqlType::BigInt)
                    .nullable(true)
                    .foreign_key("teams.id"),
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
                FieldInfo::new("id", "id", SqlType::Integer).primary_key(true),
                FieldInfo::new("is_active", "is_active", SqlType::Boolean).default("true"),
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

    // Test model with ON DELETE CASCADE
    struct TestWithOnDelete;

    impl Model for TestWithOnDelete {
        const TABLE_NAME: &'static str = "comments";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            use sqlmodel_core::ReferentialAction;
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt)
                    .nullable(true)
                    .primary_key(true)
                    .auto_increment(true),
                FieldInfo::new("post_id", "post_id", SqlType::BigInt)
                    .foreign_key("posts.id")
                    .on_delete(ReferentialAction::Cascade)
                    .on_update(ReferentialAction::NoAction),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(TestWithOnDelete)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_create_table_on_delete_cascade() {
        let sql = CreateTable::<TestWithOnDelete>::new().build();
        assert!(sql.contains("FOREIGN KEY (\"post_id\") REFERENCES \"posts\"(\"id\") ON DELETE CASCADE ON UPDATE NO ACTION"));
    }

    #[test]
    fn test_referential_action_as_sql() {
        use sqlmodel_core::ReferentialAction;
        assert_eq!(ReferentialAction::NoAction.as_sql(), "NO ACTION");
        assert_eq!(ReferentialAction::Restrict.as_sql(), "RESTRICT");
        assert_eq!(ReferentialAction::Cascade.as_sql(), "CASCADE");
        assert_eq!(ReferentialAction::SetNull.as_sql(), "SET NULL");
        assert_eq!(ReferentialAction::SetDefault.as_sql(), "SET DEFAULT");
    }

    #[test]
    fn test_referential_action_from_str() {
        use sqlmodel_core::ReferentialAction;
        assert_eq!(
            ReferentialAction::from_str("CASCADE"),
            Some(ReferentialAction::Cascade)
        );
        assert_eq!(
            ReferentialAction::from_str("cascade"),
            Some(ReferentialAction::Cascade)
        );
        assert_eq!(
            ReferentialAction::from_str("SET NULL"),
            Some(ReferentialAction::SetNull)
        );
        assert_eq!(
            ReferentialAction::from_str("SETNULL"),
            Some(ReferentialAction::SetNull)
        );
        assert_eq!(ReferentialAction::from_str("invalid"), None);
    }

    #[derive(sqlmodel_macros::Model)]
    struct TestDerivedSqlTypeOverride {
        #[sqlmodel(primary_key)]
        id: i64,

        #[sqlmodel(sql_type = "TIMESTAMP WITH TIME ZONE")]
        created_at: String,
    }

    #[test]
    fn test_create_table_sql_type_attribute_preserves_raw_string() {
        let sql = CreateTable::<TestDerivedSqlTypeOverride>::new().build();
        assert!(sql.contains("\"created_at\" TIMESTAMP WITH TIME ZONE NOT NULL"));
    }

    // Test model with sql_type_override
    struct TestWithSqlTypeOverride;

    impl Model for TestWithSqlTypeOverride {
        const TABLE_NAME: &'static str = "products";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt)
                    .nullable(true)
                    .primary_key(true)
                    .auto_increment(true),
                FieldInfo::new("price", "price", SqlType::Real).sql_type_override("DECIMAL(10,2)"),
                FieldInfo::new("sku", "sku", SqlType::Text)
                    .sql_type_override("VARCHAR(50)")
                    .unique(true),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(TestWithSqlTypeOverride)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_create_table_sql_type_override() {
        let sql = CreateTable::<TestWithSqlTypeOverride>::new().build();
        // Override types should be used instead of base types
        assert!(sql.contains("\"price\" DECIMAL(10,2) NOT NULL"));
        assert!(sql.contains("\"sku\" VARCHAR(50) NOT NULL"));
        // Non-overridden types use sql_type.sql_name()
        assert!(sql.contains("\"id\" BIGINT"));
    }

    #[test]
    fn test_field_info_effective_sql_type() {
        let field_no_override = FieldInfo::new("col", "col", SqlType::Integer);
        assert_eq!(field_no_override.effective_sql_type(), "INTEGER");

        let field_with_override =
            FieldInfo::new("col", "col", SqlType::Text).sql_type_override("VARCHAR(255)");
        assert_eq!(field_with_override.effective_sql_type(), "VARCHAR(255)");
    }

    #[test]
    fn test_quote_ident_escapes_embedded_quotes() {
        // Simple identifier - no escaping needed
        assert_eq!(quote_ident("simple"), "\"simple\"");

        // Identifier with embedded quote - must be doubled
        assert_eq!(quote_ident("with\"quote"), "\"with\"\"quote\"");

        // Identifier with multiple quotes
        assert_eq!(quote_ident("a\"b\"c"), "\"a\"\"b\"\"c\"");

        // Already-doubled quotes stay doubled-doubled
        assert_eq!(quote_ident("test\"\"name"), "\"test\"\"\"\"name\"");
    }

    #[test]
    fn test_schema_builder_index_with_special_chars() {
        let statements = SchemaBuilder::new()
            .create_index("idx\"test", "my\"table", &["col\"name"], false)
            .build();
        // Verify quotes are escaped (doubled)
        assert!(statements[0].contains("\"idx\"\"test\""));
        assert!(statements[0].contains("\"my\"\"table\""));
        assert!(statements[0].contains("\"col\"\"name\""));
    }

    // ================================================================================
    // DDL Identifier Quoting Integration Tests
    // ================================================================================

    // Test model with SQL keyword table name
    struct TestOrderTable;

    impl Model for TestOrderTable {
        const TABLE_NAME: &'static str = "order"; // SQL keyword!
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt)
                    .nullable(true)
                    .primary_key(true),
                FieldInfo::new("select", "select", SqlType::Text), // SQL keyword column!
                FieldInfo::new("from", "from", SqlType::Text),     // SQL keyword column!
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(TestOrderTable)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_create_table_with_keyword_table_name() {
        let sql = CreateTable::<TestOrderTable>::new().build();
        // Table name "order" must be quoted
        assert!(sql.contains("CREATE TABLE \"order\""));
        // Column names that are keywords must be quoted
        assert!(sql.contains("\"select\" TEXT NOT NULL"));
        assert!(sql.contains("\"from\" TEXT NOT NULL"));
        assert!(sql.contains("\"id\" BIGINT"));
        assert!(sql.contains("PRIMARY KEY (\"id\")"));
    }

    #[test]
    fn test_schema_builder_with_keyword_table_name() {
        let statements = SchemaBuilder::new()
            .create_table::<TestOrderTable>()
            .build();
        assert!(statements[0].contains("CREATE TABLE IF NOT EXISTS \"order\""));
        assert!(statements[0].contains("\"select\" TEXT NOT NULL"));
    }

    #[test]
    fn test_create_index_with_keyword_names() {
        let statements = SchemaBuilder::new()
            .create_index("idx_order_select", "order", &["select", "from"], false)
            .build();
        // All identifiers must be quoted
        assert!(statements[0].contains("\"idx_order_select\""));
        assert!(statements[0].contains("ON \"order\""));
        assert!(statements[0].contains("(\"select\", \"from\")"));
    }

    // Test model with embedded quotes in table/column names
    struct TestQuotedNames;

    impl Model for TestQuotedNames {
        const TABLE_NAME: &'static str = "my\"table"; // Embedded quote!
        const PRIMARY_KEY: &'static [&'static str] = &["pk"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("pk", "pk", SqlType::BigInt).primary_key(true),
                FieldInfo::new("data\"col", "data\"col", SqlType::Text), // Embedded quote!
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(TestQuotedNames)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_create_table_with_embedded_quotes() {
        let sql = CreateTable::<TestQuotedNames>::new().build();
        // Embedded quotes must be doubled
        assert!(sql.contains("CREATE TABLE \"my\"\"table\""));
        assert!(sql.contains("\"data\"\"col\" TEXT NOT NULL"));
        // Primary key also needs quote escaping
        assert!(sql.contains("PRIMARY KEY (\"pk\")"));
    }

    // Test model with unicode characters
    struct TestUnicodeTable;

    impl Model for TestUnicodeTable {
        const TABLE_NAME: &'static str = "用户表"; // Chinese "user table"
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                FieldInfo::new("名前", "名前", SqlType::Text), // Japanese "name"
                FieldInfo::new("émoji_🦀", "émoji_🦀", SqlType::Text), // Emoji in column name
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(TestUnicodeTable)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_create_table_with_unicode_names() {
        let sql = CreateTable::<TestUnicodeTable>::new().build();
        // Unicode should be preserved and quoted
        assert!(sql.contains("CREATE TABLE \"用户表\""));
        assert!(sql.contains("\"名前\" TEXT NOT NULL"));
        assert!(sql.contains("\"émoji_🦀\" TEXT NOT NULL"));
    }

    // Test model with spaces in names
    struct TestSpacedNames;

    impl Model for TestSpacedNames {
        const TABLE_NAME: &'static str = "my table";
        const PRIMARY_KEY: &'static [&'static str] = &["my id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("my id", "my id", SqlType::BigInt).primary_key(true),
                FieldInfo::new("full name", "full name", SqlType::Text),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(TestSpacedNames)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_create_table_with_spaces_in_names() {
        let sql = CreateTable::<TestSpacedNames>::new().build();
        // Spaces must be preserved within quotes
        assert!(sql.contains("CREATE TABLE \"my table\""));
        assert!(sql.contains("\"my id\" BIGINT"));
        assert!(sql.contains("\"full name\" TEXT NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (\"my id\")"));
    }

    // Test foreign key with keyword table reference
    struct TestFkToKeyword;

    impl Model for TestFkToKeyword {
        const TABLE_NAME: &'static str = "user_orders";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt)
                    .nullable(true)
                    .primary_key(true),
                FieldInfo::new("order_id", "order_id", SqlType::BigInt)
                    .nullable(true)
                    .foreign_key("order.id"), // FK to keyword table!
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(TestFkToKeyword)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_foreign_key_to_keyword_table() {
        let sql = CreateTable::<TestFkToKeyword>::new().build();
        // FK reference must quote the keyword table name
        assert!(sql.contains("FOREIGN KEY (\"order_id\") REFERENCES \"order\"(\"id\")"));
    }

    // Test unique constraint with keyword column name
    struct TestUniqueKeyword;

    impl Model for TestUniqueKeyword {
        const TABLE_NAME: &'static str = "items";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                FieldInfo::new("index", "index", SqlType::Integer).unique(true), // keyword!
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(TestUniqueKeyword)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_unique_constraint_with_keyword_column() {
        let sql = CreateTable::<TestUniqueKeyword>::new().build();
        // Unique constraint with keyword column name
        assert!(sql.contains("CONSTRAINT \"uk_index\" UNIQUE (\"index\")"));
        assert!(sql.contains("\"index\" INTEGER NOT NULL"));
    }

    // Edge cases: empty string, single quote, backslash
    #[test]
    fn test_quote_ident_edge_cases() {
        // Empty string
        assert_eq!(quote_ident(""), "\"\"");

        // Single character
        assert_eq!(quote_ident("x"), "\"x\"");

        // Just a quote
        assert_eq!(quote_ident("\""), "\"\"\"\"");

        // Backslash (should pass through)
        assert_eq!(quote_ident("back\\slash"), "\"back\\slash\"");

        // Multiple consecutive quotes
        assert_eq!(quote_ident("\"\"\""), "\"\"\"\"\"\"\"\"");

        // Mixed quotes and other chars
        assert_eq!(quote_ident("a\"b\"c\"d"), "\"a\"\"b\"\"c\"\"d\"");
    }

    // Test that all SQL keywords are properly quoted
    #[test]
    fn test_various_sql_keywords_as_identifiers() {
        // All these are SQL reserved words
        let keywords = [
            "select",
            "from",
            "where",
            "order",
            "group",
            "by",
            "having",
            "insert",
            "update",
            "delete",
            "create",
            "drop",
            "table",
            "index",
            "primary",
            "foreign",
            "key",
            "references",
            "constraint",
            "unique",
            "not",
            "null",
            "default",
            "and",
            "or",
            "in",
            "between",
            "like",
            "is",
            "as",
            "join",
            "inner",
            "outer",
            "left",
            "right",
            "on",
            "into",
            "values",
            "set",
            "limit",
            "offset",
            "asc",
            "desc",
            "user",
            "database",
        ];

        for keyword in keywords {
            let quoted = quote_ident(keyword);
            // Must be quoted with double quotes
            assert!(
                quoted.starts_with('"') && quoted.ends_with('"'),
                "Keyword '{}' not properly quoted: {}",
                keyword,
                quoted
            );
            // Content should be the keyword itself
            assert_eq!(
                &quoted[1..quoted.len() - 1],
                keyword,
                "Keyword '{}' mangled in quoting",
                keyword
            );
        }
    }

    // SchemaBuilder edge cases
    #[test]
    fn test_schema_builder_create_index_with_keywords() {
        let stmts = SchemaBuilder::new()
            .create_index("idx_user_select", "user", &["select"], true)
            .build();
        assert!(stmts[0].contains("CREATE UNIQUE INDEX IF NOT EXISTS \"idx_user_select\""));
        assert!(stmts[0].contains("ON \"user\" (\"select\")"));
    }

    #[test]
    fn test_schema_builder_multi_column_index_with_quotes() {
        let stmts = SchemaBuilder::new()
            .create_index("idx\"multi", "tbl\"name", &["col\"a", "col\"b"], false)
            .build();
        assert!(stmts[0].contains("\"idx\"\"multi\""));
        assert!(stmts[0].contains("ON \"tbl\"\"name\""));
        assert!(stmts[0].contains("(\"col\"\"a\", \"col\"\"b\")"));
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
        let quoted_cols: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
        let stmt = format!(
            "CREATE {}INDEX IF NOT EXISTS {} ON {} ({})",
            unique_str,
            quote_ident(name),
            quote_ident(table),
            quoted_cols.join(", ")
        );
        self.statements.push(stmt);
        self
    }

    /// Get all SQL statements.
    pub fn build(self) -> Vec<String> {
        self.statements
    }
}
