//! CREATE TABLE statement builder.

#[cfg(test)]
use sqlmodel_core::quote_ident;
use sqlmodel_core::{Dialect, FieldInfo, InheritanceStrategy, Model};
use std::marker::PhantomData;

/// Builder for CREATE TABLE statements.
///
/// Output is generated for a [`Dialect`]: identifier quoting and the spelling of
/// an auto-increment primary key differ per engine. The default is SQLite (the
/// historical behaviour of this builder); call [`Self::dialect`] with
/// `conn.dialect()` for PostgreSQL or MySQL.
#[derive(Debug)]
pub struct CreateTable<M: Model> {
    if_not_exists: bool,
    dialect: Dialect,
    _marker: PhantomData<M>,
}

impl<M: Model> CreateTable<M> {
    /// Create a new CREATE TABLE builder (SQLite dialect).
    pub fn new() -> Self {
        Self {
            if_not_exists: false,
            dialect: Dialect::Sqlite,
            _marker: PhantomData,
        }
    }

    /// Generate SQL for `dialect` (identifier quoting, auto-increment form).
    pub fn dialect(mut self, dialect: Dialect) -> Self {
        self.dialect = dialect;
        self
    }

    /// Add IF NOT EXISTS clause.
    pub fn if_not_exists(mut self) -> Self {
        self.if_not_exists = true;
        self
    }

    /// Build the CREATE TABLE SQL.
    ///
    /// # Inheritance Handling
    ///
    /// - **Single Table Inheritance (child)**: Returns empty string (child uses parent's table)
    /// - **Joined Table Inheritance (child)**: Adds FK constraint to parent table
    /// - **Concrete Table Inheritance**: Each model gets independent table (normal behavior)
    pub fn build(&self) -> String {
        let inheritance = M::inheritance();
        let q = |name: &str| self.dialect.quote_identifier(name);

        // Single table inheritance: child models don't create their own table
        // They share the parent's table and are distinguished by the discriminator column
        if inheritance.strategy == InheritanceStrategy::None
            && inheritance.parent.is_some()
            && inheritance.discriminator_value.is_some()
        {
            tracing::debug!(
                target: "sqlmodel_schema::inheritance",
                model = %M::TABLE_NAME,
                parent = ?inheritance.parent,
                "skipping table creation for STI child model"
            );
            // This is a single table inheritance child - no table to create
            // Child-specific columns are handled by higher-level schema planning (e.g. SchemaBuilder)
            return String::new();
        }

        let mut sql = String::from("CREATE TABLE ");

        if self.if_not_exists {
            sql.push_str("IF NOT EXISTS ");
        }

        sql.push_str(&q(M::TABLE_NAME));
        sql.push_str(" (\n");

        let fields = M::fields();
        let mut column_defs = Vec::new();
        let mut constraints = Vec::new();

        // SQLite auto-increment requires `INTEGER PRIMARY KEY` on the column itself.
        // When we detect a single-column PK marked `auto_increment`, we embed the PK
        // constraint in the column definition and skip the table-level PK clause.
        let embedded_autoinc_pk: Option<&str> = {
            let pk_cols = M::PRIMARY_KEY;
            if pk_cols.len() == 1 {
                let pk = pk_cols[0];
                let has_autoinc_pk = fields
                    .iter()
                    .any(|f| f.column_name == pk && f.primary_key && f.auto_increment);
                if has_autoinc_pk { Some(pk) } else { None }
            } else {
                None
            }
        };

        for field in fields {
            let embed_pk = embedded_autoinc_pk.is_some_and(|col| {
                col == field.column_name && field.primary_key && field.auto_increment
            });
            column_defs.push(self.column_definition(field, embed_pk));

            // Collect constraints
            if field.unique && !field.primary_key {
                let constraint_name = format!("uk_{}_{}", M::TABLE_NAME, field.column_name);
                let constraint = format!(
                    "CONSTRAINT {} UNIQUE ({})",
                    q(&constraint_name),
                    q(field.column_name)
                );
                constraints.push(constraint);
            }

            if let Some(fk) = field.foreign_key {
                let parts: Vec<&str> = fk.split('.').collect();
                if parts.len() == 2 {
                    let constraint_name = format!("fk_{}_{}", M::TABLE_NAME, field.column_name);
                    let mut fk_sql = format!(
                        "CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({})",
                        q(&constraint_name),
                        q(field.column_name),
                        q(parts[0]),
                        q(parts[1])
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

        // For joined table inheritance child models, add FK to parent table
        if inheritance.strategy == InheritanceStrategy::Joined
            && let Some(parent_table) = inheritance.parent
        {
            // In joined inheritance, the child's primary key columns are also a foreign key
            // to the parent table's primary key columns (same column names).
            let pk_cols = M::PRIMARY_KEY;
            if !pk_cols.is_empty() {
                let quoted_child_cols: Vec<String> = pk_cols.iter().map(|c| q(c)).collect();
                let quoted_parent_cols = quoted_child_cols.clone();
                let constraint_name = format!("fk_{}_parent", M::TABLE_NAME);
                let fk_sql = format!(
                    "CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({}) ON DELETE CASCADE",
                    q(&constraint_name),
                    quoted_child_cols.join(", "),
                    q(parent_table),
                    quoted_parent_cols.join(", ")
                );
                tracing::debug!(
                    target: "sqlmodel_schema::inheritance",
                    model = %M::TABLE_NAME,
                    parent = %parent_table,
                    fk_constraint = %constraint_name,
                    "added joined inheritance foreign key to parent primary key"
                );
                constraints.push(fk_sql);
            }
        }

        // Add primary key constraint (unless embedded for SQLite-style auto-increment single PK).
        let pk_cols = M::PRIMARY_KEY;
        if !pk_cols.is_empty() {
            let embedded = embedded_autoinc_pk.is_some_and(|pk| pk_cols == [pk]);
            if !embedded {
                let quoted_pk: Vec<String> = pk_cols.iter().map(|c| q(c)).collect();
                let mut constraint = String::new();
                constraint.push_str("PRIMARY KEY (");
                constraint.push_str(&quoted_pk.join(", "));
                constraint.push(')');
                constraints.insert(0, constraint);
            }
        }

        // Combine column definitions and constraints
        let all_parts: Vec<_> = column_defs.into_iter().chain(constraints).collect();

        sql.push_str(&all_parts.join(",\n  "));
        sql.push_str("\n)");

        sql
    }

    /// Check if this model should skip table creation.
    ///
    /// Returns true for single table inheritance child models, which
    /// share their parent's table rather than having their own.
    pub fn should_skip_table_creation() -> bool {
        let inheritance = M::inheritance();
        // Single table inheritance child: has parent + discriminator_value but no explicit strategy
        inheritance.strategy == InheritanceStrategy::None
            && inheritance.parent.is_some()
            && inheritance.discriminator_value.is_some()
    }

    fn column_definition(&self, field: &FieldInfo, embed_primary_key: bool) -> String {
        let mut def = String::from("  ");
        def.push_str(&self.dialect.quote_identifier(field.column_name));
        def.push(' ');

        if embed_primary_key {
            // A single-column auto-increment primary key; every engine spells it
            // differently, and getting this wrong silently yields a key that never
            // auto-assigns (which is what happened on PostgreSQL and MySQL before the
            // builder knew its dialect).
            let sql_type = field.effective_sql_type_for(self.dialect);
            match self.dialect {
                // Only `INTEGER PRIMARY KEY` is rowid-backed and auto-assigning in SQLite.
                Dialect::Sqlite => def.push_str("INTEGER PRIMARY KEY"),
                Dialect::Postgres => {
                    def.push_str(&sql_type);
                    def.push_str(" PRIMARY KEY");
                    // A user-specified SERIAL/BIGSERIAL already auto-assigns.
                    if !sql_type.to_ascii_uppercase().contains("SERIAL") {
                        def.push_str(" GENERATED BY DEFAULT AS IDENTITY");
                    }
                }
                Dialect::Mysql => {
                    def.push_str(&sql_type);
                    def.push_str(" NOT NULL AUTO_INCREMENT PRIMARY KEY");
                }
            }
        } else {
            def.push_str(&field.effective_sql_type_for(self.dialect));
            if !field.nullable && !field.auto_increment {
                def.push_str(" NOT NULL");
            }
        }

        if let Some(default) = field.default {
            def.push_str(" DEFAULT ");
            def.push_str(default);
        }

        // Declared CHECK constraints (`#[sqlmodel(check = "...")]` /
        // `sa_column(check = ...)`) were carried in the metadata but never
        // rendered until 2026-09. Every dialect accepts them inline.
        for constraint in field.column_constraints {
            let expression = constraint.trim();
            let expression = expression
                .strip_prefix("CHECK")
                .or_else(|| expression.strip_prefix("check"))
                .map_or(expression, str::trim);
            let expression = expression
                .strip_prefix('(')
                .and_then(|e| e.strip_suffix(')'))
                .unwrap_or(expression);
            def.push_str(" CHECK (");
            def.push_str(expression);
            def.push(')');
        }

        // MySQL takes the column comment inline; PostgreSQL needs a separate
        // `COMMENT ON COLUMN` (emitted by `SchemaBuilder`); SQLite has none.
        if self.dialect == Dialect::Mysql
            && let Some(comment) = field.column_comment
        {
            def.push_str(" COMMENT '");
            def.push_str(&comment.replace('\'', "''"));
            def.push('\'');
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
    fn create_table_is_dialect_aware() {
        let mysql = CreateTable::<TestHero>::new()
            .dialect(Dialect::Mysql)
            .build();
        assert!(mysql.contains("CREATE TABLE `heroes`"), "{mysql}");
        assert!(
            mysql.contains("`id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY"),
            "{mysql}"
        );
        assert!(!mysql.contains('"'), "{mysql}");

        let pg = CreateTable::<TestHero>::new()
            .dialect(Dialect::Postgres)
            .build();
        assert!(
            pg.contains("\"id\" BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY"),
            "{pg}"
        );

        // Default (SQLite) output is unchanged: rowid-backed INTEGER PRIMARY KEY.
        let sqlite = CreateTable::<TestHero>::new().build();
        assert!(sqlite.contains("\"id\" INTEGER PRIMARY KEY"), "{sqlite}");

        let idx = SchemaBuilder::new()
            .dialect(Dialect::Mysql)
            .create_index("ix_heroes_name", "heroes", &["name"], false)
            .build();
        assert_eq!(idx[0], "CREATE INDEX `ix_heroes_name` ON `heroes` (`name`)");
    }

    #[test]
    fn test_create_table_basic() {
        let sql = CreateTable::<TestHero>::new().build();
        assert!(sql.starts_with("CREATE TABLE \"heroes\""));
        assert!(sql.contains("\"id\" INTEGER PRIMARY KEY"));
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
        assert!(sql.contains("\"id\" INTEGER PRIMARY KEY"));
    }

    #[test]
    fn test_create_table_unique_constraint() {
        let sql = CreateTable::<TestHero>::new().build();
        assert!(sql.contains("CONSTRAINT \"uk_heroes_name\" UNIQUE (\"name\")"));
    }

    #[test]
    fn test_create_table_foreign_key() {
        let sql = CreateTable::<TestHero>::new().build();
        assert!(sql.contains("FOREIGN KEY (\"team_id\") REFERENCES \"teams\"(\"id\")"));
    }

    #[test]
    fn test_create_table_auto_increment() {
        let sql = CreateTable::<TestHero>::new().build();
        assert!(sql.contains("\"id\" INTEGER PRIMARY KEY"));
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

    /// CHECK constraints and column comments declared on a model were carried
    /// in `FieldInfo` but never rendered until 2026-09.
    struct TestWithCheckAndComment;

    impl Model for TestWithCheckAndComment {
        const TABLE_NAME: &'static str = "people";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::Integer).primary_key(true),
                FieldInfo::new("age", "age", SqlType::Integer)
                    .column_constraints(&["age >= 0", "CHECK (age <= 150)"])
                    .column_comment("years, it's an int")
                    .index("idx_people_age"),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(TestWithCheckAndComment)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    #[test]
    fn check_constraints_and_comments_reach_the_ddl() {
        let sqlite = CreateTable::<TestWithCheckAndComment>::new()
            .dialect(Dialect::Sqlite)
            .build();
        assert!(
            sqlite.contains("\"age\" INTEGER NOT NULL CHECK (age >= 0) CHECK (age <= 150)"),
            "{sqlite}"
        );
        assert!(
            !sqlite.contains("COMMENT"),
            "SQLite has no column comments: {sqlite}"
        );

        let mysql = CreateTable::<TestWithCheckAndComment>::new()
            .dialect(Dialect::Mysql)
            .build();
        assert!(
            mysql.contains("CHECK (age >= 0) CHECK (age <= 150) COMMENT 'years, it''s an int'"),
            "{mysql}"
        );

        let statements = SchemaBuilder::new()
            .dialect(Dialect::Postgres)
            .create_table::<TestWithCheckAndComment>()
            .build();
        assert!(
            statements[0].contains("CHECK (age >= 0)"),
            "{}",
            statements[0]
        );
        assert!(!statements[0].contains("COMMENT"), "{}", statements[0]);
        assert_eq!(
            statements[1],
            "COMMENT ON COLUMN \"people\".\"age\" IS 'years, it''s an int'"
        );
        // `index = "..."` on a field was silently dropped by `create_table` until
        // 2026-09; callers had to repeat it through `create_index` by hand.
        assert_eq!(
            statements[2],
            "CREATE INDEX IF NOT EXISTS \"idx_people_age\" ON \"people\" (\"age\")"
        );
        assert_eq!(statements.len(), 3);

        let mysql_statements = SchemaBuilder::new()
            .dialect(Dialect::Mysql)
            .create_table::<TestWithCheckAndComment>()
            .build();
        assert_eq!(
            mysql_statements[1],
            "CREATE INDEX `idx_people_age` ON `people` (`age`)"
        );
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
        // Auto-increment single PK embeds as INTEGER PRIMARY KEY (SQLite compat)
        assert!(sql.contains("\"id\" INTEGER PRIMARY KEY"));
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
        assert!(sql.contains("CONSTRAINT \"uk_items_index\" UNIQUE (\"index\")"));
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

    // ================================================================================
    // Table Inheritance Schema Generation Tests
    // ================================================================================

    use sqlmodel_core::InheritanceInfo;

    // Single Table Inheritance Base Model
    struct SingleTableBase;

    impl Model for SingleTableBase {
        const TABLE_NAME: &'static str = "employees";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                FieldInfo::new("name", "name", SqlType::Text),
                FieldInfo::new("type_", "type_", SqlType::Text), // Discriminator column
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(SingleTableBase)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }

        fn inheritance() -> InheritanceInfo {
            InheritanceInfo {
                strategy: sqlmodel_core::InheritanceStrategy::Single,
                parent: None,
                parent_fields_fn: None,
                discriminator_column: Some("type_"),
                discriminator_value: None,
            }
        }
    }

    // Single Table Inheritance Child Model (should not create table)
    struct SingleTableChild;

    impl Model for SingleTableChild {
        // Single-table inheritance child shares the parent's physical table.
        const TABLE_NAME: &'static str = "employees";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                FieldInfo::new("department", "department", SqlType::Text),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(SingleTableChild)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }

        fn inheritance() -> InheritanceInfo {
            // Child model: has parent and discriminator_value but strategy is None
            // (inherits from parent's strategy implicitly)
            InheritanceInfo {
                strategy: sqlmodel_core::InheritanceStrategy::None,
                parent: Some("employees"),
                parent_fields_fn: None,
                discriminator_column: Some("type_"),
                discriminator_value: Some("manager"),
            }
        }
    }

    // Joined Table Inheritance Base Model
    struct JoinedTableBase;

    impl Model for JoinedTableBase {
        const TABLE_NAME: &'static str = "persons";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                FieldInfo::new("name", "name", SqlType::Text),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(JoinedTableBase)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }

        fn inheritance() -> InheritanceInfo {
            InheritanceInfo {
                strategy: sqlmodel_core::InheritanceStrategy::Joined,
                parent: None,
                parent_fields_fn: None,
                discriminator_column: None,
                discriminator_value: None,
            }
        }
    }

    // Joined Table Inheritance Child Model (has FK to parent)
    struct JoinedTableChild;

    impl Model for JoinedTableChild {
        const TABLE_NAME: &'static str = "students";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                FieldInfo::new("grade", "grade", SqlType::Text),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(JoinedTableChild)
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }

        fn inheritance() -> InheritanceInfo {
            InheritanceInfo {
                strategy: sqlmodel_core::InheritanceStrategy::Joined,
                parent: Some("persons"),
                parent_fields_fn: None,
                discriminator_column: None,
                discriminator_value: None,
            }
        }
    }

    #[test]
    fn test_single_table_inheritance_base_creates_table() {
        let sql = CreateTable::<SingleTableBase>::new().build();
        assert!(sql.contains("CREATE TABLE \"employees\""));
        assert!(sql.contains("\"type_\" TEXT NOT NULL")); // Discriminator column
    }

    #[test]
    fn test_single_table_inheritance_child_skips_table_creation() {
        // Child model should not create its own table
        let sql = CreateTable::<SingleTableChild>::new().build();
        assert!(
            sql.is_empty(),
            "Single table inheritance child should not create a table"
        );
    }

    #[test]
    fn test_single_table_inheritance_child_should_skip() {
        assert!(
            CreateTable::<SingleTableChild>::should_skip_table_creation(),
            "should_skip_table_creation should return true for STI child"
        );
        assert!(
            !CreateTable::<SingleTableBase>::should_skip_table_creation(),
            "should_skip_table_creation should return false for STI base"
        );
    }

    #[test]
    fn test_joined_table_inheritance_base_creates_table() {
        let sql = CreateTable::<JoinedTableBase>::new().build();
        assert!(sql.contains("CREATE TABLE \"persons\""));
        assert!(sql.contains("\"id\" BIGINT"));
        assert!(sql.contains("\"name\" TEXT NOT NULL"));
    }

    #[test]
    fn test_joined_table_inheritance_child_creates_table_with_fk() {
        let sql = CreateTable::<JoinedTableChild>::new().build();
        assert!(sql.contains("CREATE TABLE \"students\""));
        assert!(sql.contains("\"id\" BIGINT"));
        assert!(sql.contains("\"grade\" TEXT NOT NULL"));
        // Should have FK to parent table
        assert!(
            sql.contains("FOREIGN KEY (\"id\") REFERENCES \"persons\"(\"id\") ON DELETE CASCADE"),
            "Joined table child should have FK to parent: {}",
            sql
        );
    }

    #[test]
    fn test_schema_builder_applies_sti_child_columns() {
        let statements = SchemaBuilder::new()
            .create_table::<SingleTableBase>()
            .create_table::<SingleTableChild>() // Adds child-specific columns via ALTER TABLE
            .build();

        // Base creates the table, child adds the extra column(s).
        assert_eq!(
            statements.len(),
            2,
            "STI child should contribute ALTER TABLE statements"
        );
        assert!(statements[0].contains("CREATE TABLE IF NOT EXISTS \"employees\""));
        assert!(statements[1].contains("ALTER TABLE \"employees\" ADD COLUMN \"department\""));
    }

    #[test]
    fn test_schema_builder_creates_both_joined_tables() {
        let statements = SchemaBuilder::new()
            .create_table::<JoinedTableBase>()
            .create_table::<JoinedTableChild>()
            .build();

        // Both tables should be created
        assert_eq!(
            statements.len(),
            2,
            "Both joined inheritance tables should be created"
        );
        assert!(statements[0].contains("CREATE TABLE IF NOT EXISTS \"persons\""));
        assert!(statements[1].contains("CREATE TABLE IF NOT EXISTS \"students\""));
        assert!(statements[1].contains("FOREIGN KEY"));
    }
}

/// Builder for multiple schema operations.
///
/// Statements are generated for a [`Dialect`] (identifier quoting and the
/// auto-increment primary-key form differ per engine). The default is SQLite,
/// which also matches the double-quoted output this builder produced before it
/// became dialect-aware; pass `conn.dialect()` for anything else.
#[derive(Debug)]
pub struct SchemaBuilder {
    statements: Vec<String>,
    dialect: Dialect,
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self {
            statements: Vec::new(),
            dialect: Dialect::Sqlite,
        }
    }
}

impl SchemaBuilder {
    /// Create a new schema builder (SQLite dialect; see [`Self::dialect`]).
    pub fn new() -> Self {
        Self::default()
    }

    /// Generate statements for `dialect` (identifier quoting, auto-increment form,
    /// `IF NOT EXISTS` support on indexes).
    pub fn dialect(mut self, dialect: Dialect) -> Self {
        self.dialect = dialect;
        self
    }

    /// Add a CREATE TABLE statement.
    ///
    /// For single table inheritance child models, this emits `ALTER TABLE .. ADD COLUMN ..`
    /// statements for the child-specific fields, since the child's logical table is the
    /// parent's physical table.
    pub fn create_table<M: Model>(mut self) -> Self {
        if CreateTable::<M>::should_skip_table_creation() {
            tracing::debug!(
                target: "sqlmodel_schema::inheritance",
                model = %M::TABLE_NAME,
                "skipping physical table creation in SchemaBuilder for STI child"
            );
            let inheritance = M::inheritance();
            let Some(parent_table) = inheritance.parent else {
                return self;
            };

            let pk_cols = M::PRIMARY_KEY;
            // Columns the base model already declares live on the base table; a child
            // that redeclares them (the natural way to give a child model access to
            // inherited columns such as `name`) must not try to ADD them again.
            let parent_columns: &[FieldInfo] = inheritance.parent_fields_fn.map_or(&[], |f| f());
            for field in M::fields() {
                // Avoid trying to re-add PK columns that are expected to be on the base table.
                if field.primary_key || pk_cols.contains(&field.column_name) {
                    continue;
                }
                if parent_columns
                    .iter()
                    .any(|p| p.column_name == field.column_name)
                {
                    continue;
                }
                self.statements
                    .push(alter_table_add_column(self.dialect, parent_table, field));
            }
            return self;
        }

        self.statements.push(
            CreateTable::<M>::new()
                .dialect(self.dialect)
                .if_not_exists()
                .build(),
        );
        // PostgreSQL column comments are separate statements (MySQL takes
        // them inline in the column definition; SQLite has none).
        if self.dialect == Dialect::Postgres {
            for field in M::fields() {
                if let Some(comment) = field.column_comment {
                    self.statements.push(format!(
                        "COMMENT ON COLUMN {}.{} IS '{}'",
                        self.dialect.quote_identifier(M::TABLE_NAME),
                        self.dialect.quote_identifier(field.column_name),
                        comment.replace('\'', "''")
                    ));
                }
            }
        }
        // `#[sqlmodel(index = "name")]` declares a single-column index; it is
        // part of the model's schema (the differ expects it), so create it here
        // rather than leaving every caller to repeat `create_index`.
        for field in M::fields() {
            if let Some(index_name) = field.index {
                self = self.create_index(index_name, M::TABLE_NAME, &[field.column_name], false);
            }
        }
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
        // MySQL has no `CREATE INDEX IF NOT EXISTS`.
        let if_not_exists = if matches!(self.dialect, Dialect::Mysql) {
            ""
        } else {
            "IF NOT EXISTS "
        };
        let quoted_cols: Vec<String> = columns
            .iter()
            .map(|c| self.dialect.quote_identifier(c))
            .collect();
        let stmt = format!(
            "CREATE {}INDEX {}{} ON {} ({})",
            unique_str,
            if_not_exists,
            self.dialect.quote_identifier(name),
            self.dialect.quote_identifier(table),
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

fn alter_table_add_column(dialect: Dialect, table: &str, field: &FieldInfo) -> String {
    let sql_type = field.effective_sql_type_for(dialect);
    let mut stmt = format!(
        "ALTER TABLE {} ADD COLUMN {} {}",
        dialect.quote_identifier(table),
        dialect.quote_identifier(field.column_name),
        sql_type
    );

    if !field.nullable && !field.auto_increment {
        stmt.push_str(" NOT NULL");
    }

    if let Some(default) = field.default {
        stmt.push_str(" DEFAULT ");
        stmt.push_str(default);
    }

    for constraint in field.column_constraints {
        let expression = constraint.trim();
        let expression = expression
            .strip_prefix("CHECK")
            .or_else(|| expression.strip_prefix("check"))
            .map_or(expression, str::trim);
        let expression = expression
            .strip_prefix('(')
            .and_then(|e| e.strip_suffix(')'))
            .unwrap_or(expression);
        stmt.push_str(" CHECK (");
        stmt.push_str(expression);
        stmt.push(')');
    }

    stmt
}
