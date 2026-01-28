//! Field and column definitions.

use crate::types::SqlType;

/// Referential action for foreign key constraints (ON DELETE / ON UPDATE).
///
/// These define what happens to referencing rows when the referenced row is
/// deleted or updated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReferentialAction {
    /// No action - raise error if any references exist.
    /// This is the default and most restrictive option.
    #[default]
    NoAction,
    /// Restrict - same as NO ACTION (alias for compatibility).
    Restrict,
    /// Cascade - automatically delete/update referencing rows.
    Cascade,
    /// Set null - set referencing columns to NULL.
    SetNull,
    /// Set default - set referencing columns to their default values.
    SetDefault,
}

impl ReferentialAction {
    /// Get the SQL representation of this action.
    #[must_use]
    pub const fn as_sql(&self) -> &'static str {
        match self {
            ReferentialAction::NoAction => "NO ACTION",
            ReferentialAction::Restrict => "RESTRICT",
            ReferentialAction::Cascade => "CASCADE",
            ReferentialAction::SetNull => "SET NULL",
            ReferentialAction::SetDefault => "SET DEFAULT",
        }
    }

    /// Parse a referential action from a string (case-insensitive).
    ///
    /// Returns `None` if the string is not a recognized action.
    #[must_use]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "NO ACTION" | "NOACTION" | "NO_ACTION" => Some(ReferentialAction::NoAction),
            "RESTRICT" => Some(ReferentialAction::Restrict),
            "CASCADE" => Some(ReferentialAction::Cascade),
            "SET NULL" | "SETNULL" | "SET_NULL" => Some(ReferentialAction::SetNull),
            "SET DEFAULT" | "SETDEFAULT" | "SET_DEFAULT" => Some(ReferentialAction::SetDefault),
            _ => None,
        }
    }
}

/// Metadata about a model field/column.
#[derive(Debug, Clone)]
pub struct FieldInfo {
    /// Rust field name
    pub name: &'static str,
    /// Database column name (may differ from field name)
    pub column_name: &'static str,
    /// SQL type for this field
    pub sql_type: SqlType,
    /// Explicit SQL type override string (e.g., "VARCHAR(255)", "DECIMAL(10,2)")
    /// When set, this takes precedence over `sql_type` in DDL generation.
    pub sql_type_override: Option<&'static str>,
    /// Precision for DECIMAL/NUMERIC types (total digits)
    pub precision: Option<u8>,
    /// Scale for DECIMAL/NUMERIC types (digits after decimal point)
    pub scale: Option<u8>,
    /// Whether this field is nullable
    pub nullable: bool,
    /// Whether this is a primary key
    pub primary_key: bool,
    /// Whether this field auto-increments
    pub auto_increment: bool,
    /// Whether this field has a unique constraint
    pub unique: bool,
    /// Default value expression (SQL)
    pub default: Option<&'static str>,
    /// Foreign key reference (table.column)
    pub foreign_key: Option<&'static str>,
    /// Referential action for ON DELETE (only valid with foreign_key)
    pub on_delete: Option<ReferentialAction>,
    /// Referential action for ON UPDATE (only valid with foreign_key)
    pub on_update: Option<ReferentialAction>,
    /// Index name if indexed
    pub index: Option<&'static str>,
    /// Alias for both input and output (like serde rename).
    /// When set, this name is used instead of `name` for serialization/deserialization.
    pub alias: Option<&'static str>,
    /// Alias used only during deserialization/validation (input-only).
    /// Accepts this name as an alternative to `name` or `alias` during parsing.
    pub validation_alias: Option<&'static str>,
    /// Alias used only during serialization (output-only).
    /// Overrides `alias` when outputting the field name.
    pub serialization_alias: Option<&'static str>,
    /// Whether this is a computed field (not stored in database).
    /// Computed fields are excluded from database operations but included
    /// in serialization (model_dump) unless exclude_computed_fields is set.
    pub computed: bool,
    /// Whether to exclude this field from serialization (model_dump).
    /// When true, the field will never appear in serialized output.
    pub exclude: bool,
    /// Schema title for JSON Schema generation.
    /// Used as the "title" property in the generated JSON Schema.
    pub title: Option<&'static str>,
    /// Schema description for JSON Schema generation.
    /// Used as the "description" property in the generated JSON Schema.
    pub description: Option<&'static str>,
    /// Extra JSON Schema properties (as JSON string, merged into schema).
    /// The string should be valid JSON that will be merged into the field's schema.
    pub schema_extra: Option<&'static str>,
    /// JSON representation of the field's default value (for exclude_defaults).
    /// When set, model_dump with exclude_defaults=true will compare the current
    /// value against this and exclude the field if they match.
    pub default_json: Option<&'static str>,
    /// Whether this field has a default value (used for exclude_unset tracking).
    /// Fields with defaults can be distinguished from fields that were explicitly set.
    pub has_default: bool,
    /// Whether this field is constant (immutable after creation).
    /// Const fields cannot be modified after initial construction.
    /// This is enforced at validation/session level, not compile-time.
    pub const_field: bool,
    /// Additional SQL constraints for DDL generation (e.g., CHECK constraints).
    /// Each string is a SQL constraint expression that will be added to the column definition.
    pub column_constraints: &'static [&'static str],
    /// SQL comment for the column (used in DDL generation).
    /// This maps to the COMMENT ON COLUMN or inline COMMENT clause depending on the database.
    pub column_comment: Option<&'static str>,
    /// Extra metadata as JSON string (for custom extensions/info).
    /// This can be used to store additional information that doesn't fit in other fields.
    pub column_info: Option<&'static str>,
}

impl FieldInfo {
    /// Create a new field info with minimal required data.
    pub const fn new(name: &'static str, column_name: &'static str, sql_type: SqlType) -> Self {
        Self {
            name,
            column_name,
            sql_type,
            sql_type_override: None,
            precision: None,
            scale: None,
            nullable: false,
            primary_key: false,
            auto_increment: false,
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
            exclude: false,
            title: None,
            description: None,
            schema_extra: None,
            default_json: None,
            has_default: false,
            const_field: false,
            column_constraints: &[],
            column_comment: None,
            column_info: None,
        }
    }

    /// Set the database column name.
    pub const fn column(mut self, name: &'static str) -> Self {
        self.column_name = name;
        self
    }

    /// Set explicit SQL type override.
    ///
    /// When set, this string will be used directly in DDL generation instead
    /// of the `sql_type.sql_name()`. Use this for database-specific types like
    /// `VARCHAR(255)`, `DECIMAL(10,2)`, `TINYINT UNSIGNED`, etc.
    pub const fn sql_type_override(mut self, type_str: &'static str) -> Self {
        self.sql_type_override = Some(type_str);
        self
    }

    /// Set SQL type override from optional.
    pub const fn sql_type_override_opt(mut self, type_str: Option<&'static str>) -> Self {
        self.sql_type_override = type_str;
        self
    }

    /// Set precision for DECIMAL/NUMERIC types.
    ///
    /// Precision is the total number of digits (before and after decimal point).
    /// Typical range: 1-38, depends on database.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // DECIMAL(10, 2) - 10 total digits, 2 after decimal
    /// FieldInfo::new("price", "price", SqlType::Decimal { precision: 10, scale: 2 })
    ///     .precision(10)
    ///     .scale(2)
    /// ```
    pub const fn precision(mut self, value: u8) -> Self {
        self.precision = Some(value);
        self
    }

    /// Set precision from optional.
    pub const fn precision_opt(mut self, value: Option<u8>) -> Self {
        self.precision = value;
        self
    }

    /// Set scale for DECIMAL/NUMERIC types.
    ///
    /// Scale is the number of digits after the decimal point.
    /// Must be less than or equal to precision.
    pub const fn scale(mut self, value: u8) -> Self {
        self.scale = Some(value);
        self
    }

    /// Set scale from optional.
    pub const fn scale_opt(mut self, value: Option<u8>) -> Self {
        self.scale = value;
        self
    }

    /// Set both precision and scale for DECIMAL/NUMERIC types.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // DECIMAL(10, 2) for currency
    /// FieldInfo::new("price", "price", SqlType::Decimal { precision: 10, scale: 2 })
    ///     .decimal_precision(10, 2)
    /// ```
    pub const fn decimal_precision(mut self, precision: u8, scale: u8) -> Self {
        self.precision = Some(precision);
        self.scale = Some(scale);
        self
    }

    /// Get the effective SQL type name for DDL generation.
    ///
    /// Priority:
    /// 1. `sql_type_override` if set
    /// 2. For DECIMAL/NUMERIC: uses `precision` and `scale` fields if set
    /// 3. Falls back to `sql_type.sql_name()`
    #[must_use]
    pub fn effective_sql_type(&self) -> String {
        // sql_type_override takes highest precedence
        if let Some(override_str) = self.sql_type_override {
            return override_str.to_string();
        }

        // For Decimal/Numeric types, use precision/scale fields if available
        match self.sql_type {
            SqlType::Decimal { .. } | SqlType::Numeric { .. } => {
                if let (Some(p), Some(s)) = (self.precision, self.scale) {
                    let type_name = match self.sql_type {
                        SqlType::Decimal { .. } => "DECIMAL",
                        SqlType::Numeric { .. } => "NUMERIC",
                        _ => unreachable!(),
                    };
                    return format!("{}({}, {})", type_name, p, s);
                }
            }
            _ => {}
        }

        // Fall back to sql_type's own name generation
        self.sql_type.sql_name()
    }

    /// Set nullable flag.
    pub const fn nullable(mut self, value: bool) -> Self {
        self.nullable = value;
        self
    }

    /// Set primary key flag.
    pub const fn primary_key(mut self, value: bool) -> Self {
        self.primary_key = value;
        self
    }

    /// Set auto-increment flag.
    pub const fn auto_increment(mut self, value: bool) -> Self {
        self.auto_increment = value;
        self
    }

    /// Set unique flag.
    pub const fn unique(mut self, value: bool) -> Self {
        self.unique = value;
        self
    }

    /// Set default value.
    pub const fn default(mut self, expr: &'static str) -> Self {
        self.default = Some(expr);
        self
    }

    /// Set default value from optional.
    pub const fn default_opt(mut self, expr: Option<&'static str>) -> Self {
        self.default = expr;
        self
    }

    /// Set foreign key reference.
    pub const fn foreign_key(mut self, reference: &'static str) -> Self {
        self.foreign_key = Some(reference);
        self
    }

    /// Set foreign key reference from optional.
    pub const fn foreign_key_opt(mut self, reference: Option<&'static str>) -> Self {
        self.foreign_key = reference;
        self
    }

    /// Set ON DELETE action for foreign key.
    ///
    /// This is only meaningful when `foreign_key` is also set.
    pub const fn on_delete(mut self, action: ReferentialAction) -> Self {
        self.on_delete = Some(action);
        self
    }

    /// Set ON DELETE action from optional.
    pub const fn on_delete_opt(mut self, action: Option<ReferentialAction>) -> Self {
        self.on_delete = action;
        self
    }

    /// Set ON UPDATE action for foreign key.
    ///
    /// This is only meaningful when `foreign_key` is also set.
    pub const fn on_update(mut self, action: ReferentialAction) -> Self {
        self.on_update = Some(action);
        self
    }

    /// Set ON UPDATE action from optional.
    pub const fn on_update_opt(mut self, action: Option<ReferentialAction>) -> Self {
        self.on_update = action;
        self
    }

    /// Set index name.
    pub const fn index(mut self, name: &'static str) -> Self {
        self.index = Some(name);
        self
    }

    /// Set index name from optional.
    pub const fn index_opt(mut self, name: Option<&'static str>) -> Self {
        self.index = name;
        self
    }

    /// Set alias for both input and output.
    ///
    /// When set, this name is used instead of the field name for both
    /// serialization and deserialization.
    pub const fn alias(mut self, name: &'static str) -> Self {
        self.alias = Some(name);
        self
    }

    /// Set alias from optional.
    pub const fn alias_opt(mut self, name: Option<&'static str>) -> Self {
        self.alias = name;
        self
    }

    /// Set validation alias (input-only).
    ///
    /// This name is accepted as an alternative during deserialization,
    /// in addition to the field name and regular alias.
    pub const fn validation_alias(mut self, name: &'static str) -> Self {
        self.validation_alias = Some(name);
        self
    }

    /// Set validation alias from optional.
    pub const fn validation_alias_opt(mut self, name: Option<&'static str>) -> Self {
        self.validation_alias = name;
        self
    }

    /// Set serialization alias (output-only).
    ///
    /// This name is used instead of the field name or regular alias
    /// when serializing the field.
    pub const fn serialization_alias(mut self, name: &'static str) -> Self {
        self.serialization_alias = Some(name);
        self
    }

    /// Set serialization alias from optional.
    pub const fn serialization_alias_opt(mut self, name: Option<&'static str>) -> Self {
        self.serialization_alias = name;
        self
    }

    /// Mark this field as computed (not stored in database).
    ///
    /// Computed fields are:
    /// - Excluded from database operations (INSERT, UPDATE, SELECT)
    /// - Initialized with Default::default() when loading from database
    /// - Included in serialization (model_dump) unless exclude_computed_fields is set
    ///
    /// Use this for fields whose value is derived from other fields at access time.
    pub const fn computed(mut self, value: bool) -> Self {
        self.computed = value;
        self
    }

    /// Mark this field as excluded from serialization (model_dump).
    ///
    /// Excluded fields will never appear in serialized output, regardless
    /// of other serialization settings. This is useful for sensitive data
    /// like passwords or internal fields.
    ///
    /// # Example
    ///
    /// ```ignore
    /// #[derive(Model)]
    /// struct User {
    ///     id: i32,
    ///     #[sqlmodel(exclude)]
    ///     password_hash: String,  // Never serialized
    /// }
    /// ```
    pub const fn exclude(mut self, value: bool) -> Self {
        self.exclude = value;
        self
    }

    /// Set the schema title for JSON Schema generation.
    ///
    /// The title appears as the "title" property in the field's JSON Schema.
    pub const fn title(mut self, value: &'static str) -> Self {
        self.title = Some(value);
        self
    }

    /// Set the schema title from optional.
    pub const fn title_opt(mut self, value: Option<&'static str>) -> Self {
        self.title = value;
        self
    }

    /// Set the schema description for JSON Schema generation.
    ///
    /// The description appears as the "description" property in the field's JSON Schema.
    pub const fn description(mut self, value: &'static str) -> Self {
        self.description = Some(value);
        self
    }

    /// Set the schema description from optional.
    pub const fn description_opt(mut self, value: Option<&'static str>) -> Self {
        self.description = value;
        self
    }

    /// Set extra JSON Schema properties (as JSON string).
    ///
    /// The string should be valid JSON that will be merged into the field's schema.
    /// For example: `{"examples": ["John Doe"], "minLength": 1}`
    pub const fn schema_extra(mut self, value: &'static str) -> Self {
        self.schema_extra = Some(value);
        self
    }

    /// Set schema_extra from optional.
    pub const fn schema_extra_opt(mut self, value: Option<&'static str>) -> Self {
        self.schema_extra = value;
        self
    }

    /// Set the JSON representation of the field's default value.
    ///
    /// This is used by model_dump with exclude_defaults=true to compare
    /// the current value against the default.
    ///
    /// # Example
    ///
    /// ```ignore
    /// FieldInfo::new("count", "count", SqlType::Integer)
    ///     .default_json("0")
    ///     .has_default(true)
    /// ```
    pub const fn default_json(mut self, value: &'static str) -> Self {
        self.default_json = Some(value);
        self.has_default = true;
        self
    }

    /// Set default_json from optional.
    pub const fn default_json_opt(mut self, value: Option<&'static str>) -> Self {
        self.default_json = value;
        if value.is_some() {
            self.has_default = true;
        }
        self
    }

    /// Mark whether this field has a default value.
    ///
    /// Used for exclude_unset tracking - fields with defaults may not need
    /// to be explicitly set.
    pub const fn has_default(mut self, value: bool) -> Self {
        self.has_default = value;
        self
    }

    /// Mark this field as constant (immutable after creation).
    ///
    /// Const fields cannot be modified after the model is initially created.
    /// This is useful for version numbers, creation timestamps, or other
    /// immutable identifiers.
    ///
    /// # Example
    ///
    /// ```ignore
    /// FieldInfo::new("version", "version", SqlType::Text)
    ///     .const_field(true)
    /// ```
    pub const fn const_field(mut self, value: bool) -> Self {
        self.const_field = value;
        self
    }

    /// Set additional SQL constraints for DDL generation.
    ///
    /// These constraints are added to the column definition.
    /// Common uses include CHECK constraints.
    ///
    /// # Example
    ///
    /// ```ignore
    /// FieldInfo::new("age", "age", SqlType::Integer)
    ///     .column_constraints(&["CHECK(age >= 0)", "CHECK(age <= 150)"])
    /// ```
    pub const fn column_constraints(mut self, constraints: &'static [&'static str]) -> Self {
        self.column_constraints = constraints;
        self
    }

    /// Set a SQL comment for the column.
    ///
    /// The comment will be included in DDL generation.
    pub const fn column_comment(mut self, comment: &'static str) -> Self {
        self.column_comment = Some(comment);
        self
    }

    /// Set column comment from optional.
    pub const fn column_comment_opt(mut self, comment: Option<&'static str>) -> Self {
        self.column_comment = comment;
        self
    }

    /// Set extra metadata as JSON string.
    ///
    /// This can be used for custom extensions or information
    /// that doesn't fit in other fields.
    pub const fn column_info(mut self, info: &'static str) -> Self {
        self.column_info = Some(info);
        self
    }

    /// Set column info from optional.
    pub const fn column_info_opt(mut self, info: Option<&'static str>) -> Self {
        self.column_info = info;
        self
    }

    /// Get the name to use when serializing (output).
    ///
    /// Priority: serialization_alias > alias > name
    #[must_use]
    pub const fn output_name(&self) -> &'static str {
        if let Some(ser_alias) = self.serialization_alias {
            ser_alias
        } else if let Some(alias) = self.alias {
            alias
        } else {
            self.name
        }
    }

    /// Check if a given name matches this field for input (deserialization).
    ///
    /// Matches: name, alias, or validation_alias
    #[must_use]
    pub fn matches_input_name(&self, input: &str) -> bool {
        if input == self.name {
            return true;
        }
        if let Some(alias) = self.alias {
            if input == alias {
                return true;
            }
        }
        if let Some(val_alias) = self.validation_alias {
            if input == val_alias {
                return true;
            }
        }
        false
    }

    /// Check if this field has any alias configuration.
    #[must_use]
    pub const fn has_alias(&self) -> bool {
        self.alias.is_some()
            || self.validation_alias.is_some()
            || self.serialization_alias.is_some()
    }
}

/// A column reference used in queries.
#[derive(Debug, Clone)]
pub struct Column {
    /// Table name (optional, for joins)
    pub table: Option<String>,
    /// Column name
    pub name: String,
    /// Alias (AS name)
    pub alias: Option<String>,
}

impl Column {
    /// Create a new column reference.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            table: None,
            name: name.into(),
            alias: None,
        }
    }

    /// Create a column reference with table prefix.
    pub fn qualified(table: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            table: Some(table.into()),
            name: name.into(),
            alias: None,
        }
    }

    /// Set an alias for this column.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Generate SQL for this column reference.
    pub fn to_sql(&self) -> String {
        let mut sql = if let Some(table) = &self.table {
            format!("{}.{}", table, self.name)
        } else {
            self.name.clone()
        };

        if let Some(alias) = &self.alias {
            sql.push_str(" AS ");
            sql.push_str(alias);
        }

        sql
    }
}

/// A field reference for type-safe column access.
///
/// This is used by generated code to provide compile-time
/// checked column references.
#[derive(Debug, Clone, Copy)]
pub struct Field<T> {
    /// The column name
    pub name: &'static str,
    /// Phantom data for the field type
    _marker: std::marker::PhantomData<T>,
}

impl<T> Field<T> {
    /// Create a new typed field reference.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqlType;

    #[test]
    fn test_field_info_new() {
        let field = FieldInfo::new(
            "price",
            "price",
            SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        );
        assert_eq!(field.name, "price");
        assert_eq!(field.column_name, "price");
        assert!(field.precision.is_none());
        assert!(field.scale.is_none());
    }

    #[test]
    fn test_field_info_precision_scale() {
        let field = FieldInfo::new(
            "amount",
            "amount",
            SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .precision(12)
        .scale(4);
        assert_eq!(field.precision, Some(12));
        assert_eq!(field.scale, Some(4));
    }

    #[test]
    fn test_field_info_decimal_precision() {
        let field = FieldInfo::new(
            "total",
            "total",
            SqlType::Numeric {
                precision: 10,
                scale: 2,
            },
        )
        .decimal_precision(18, 6);
        assert_eq!(field.precision, Some(18));
        assert_eq!(field.scale, Some(6));
    }

    #[test]
    fn test_effective_sql_type_override_takes_precedence() {
        let field = FieldInfo::new(
            "amount",
            "amount",
            SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .sql_type_override("MONEY")
        .precision(18)
        .scale(4);
        // Override should take precedence over precision/scale
        assert_eq!(field.effective_sql_type(), "MONEY");
    }

    #[test]
    fn test_effective_sql_type_uses_precision_scale() {
        let field = FieldInfo::new(
            "price",
            "price",
            SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .precision(15)
        .scale(3);
        assert_eq!(field.effective_sql_type(), "DECIMAL(15, 3)");
    }

    #[test]
    fn test_effective_sql_type_numeric_uses_precision_scale() {
        let field = FieldInfo::new(
            "value",
            "value",
            SqlType::Numeric {
                precision: 10,
                scale: 2,
            },
        )
        .precision(20)
        .scale(8);
        assert_eq!(field.effective_sql_type(), "NUMERIC(20, 8)");
    }

    #[test]
    fn test_effective_sql_type_fallback_to_sql_type() {
        let field = FieldInfo::new("count", "count", SqlType::BigInt);
        assert_eq!(field.effective_sql_type(), "BIGINT");
    }

    #[test]
    fn test_effective_sql_type_decimal_without_precision_scale() {
        // When precision/scale not set on FieldInfo, use SqlType's values
        let field = FieldInfo::new(
            "amount",
            "amount",
            SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        );
        // Falls back to sql_type.sql_name() which should generate "DECIMAL(10, 2)"
        assert_eq!(field.effective_sql_type(), "DECIMAL(10, 2)");
    }

    #[test]
    fn test_precision_opt() {
        let field = FieldInfo::new(
            "test",
            "test",
            SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .precision_opt(Some(16));
        assert_eq!(field.precision, Some(16));

        let field2 = FieldInfo::new(
            "test2",
            "test2",
            SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .precision_opt(None);
        assert_eq!(field2.precision, None);
    }

    #[test]
    fn test_scale_opt() {
        let field = FieldInfo::new(
            "test",
            "test",
            SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .scale_opt(Some(5));
        assert_eq!(field.scale, Some(5));

        let field2 = FieldInfo::new(
            "test2",
            "test2",
            SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .scale_opt(None);
        assert_eq!(field2.scale, None);
    }

    // ========================================================================
    // Field alias tests
    // ========================================================================

    #[test]
    fn test_field_info_alias() {
        let field = FieldInfo::new("name", "name", SqlType::Text).alias("userName");
        assert_eq!(field.alias, Some("userName"));
        assert!(field.validation_alias.is_none());
        assert!(field.serialization_alias.is_none());
    }

    #[test]
    fn test_field_info_validation_alias() {
        let field = FieldInfo::new("name", "name", SqlType::Text).validation_alias("user_name");
        assert!(field.alias.is_none());
        assert_eq!(field.validation_alias, Some("user_name"));
        assert!(field.serialization_alias.is_none());
    }

    #[test]
    fn test_field_info_serialization_alias() {
        let field = FieldInfo::new("name", "name", SqlType::Text).serialization_alias("user-name");
        assert!(field.alias.is_none());
        assert!(field.validation_alias.is_none());
        assert_eq!(field.serialization_alias, Some("user-name"));
    }

    #[test]
    fn test_field_info_all_aliases() {
        let field = FieldInfo::new("name", "name", SqlType::Text)
            .alias("nm")
            .validation_alias("input_name")
            .serialization_alias("outputName");

        assert_eq!(field.alias, Some("nm"));
        assert_eq!(field.validation_alias, Some("input_name"));
        assert_eq!(field.serialization_alias, Some("outputName"));
    }

    #[test]
    fn test_field_info_alias_opt() {
        let field1 = FieldInfo::new("name", "name", SqlType::Text).alias_opt(Some("userName"));
        assert_eq!(field1.alias, Some("userName"));

        let field2 = FieldInfo::new("name", "name", SqlType::Text).alias_opt(None);
        assert!(field2.alias.is_none());
    }

    #[test]
    fn test_field_info_validation_alias_opt() {
        let field1 =
            FieldInfo::new("name", "name", SqlType::Text).validation_alias_opt(Some("user_name"));
        assert_eq!(field1.validation_alias, Some("user_name"));

        let field2 = FieldInfo::new("name", "name", SqlType::Text).validation_alias_opt(None);
        assert!(field2.validation_alias.is_none());
    }

    #[test]
    fn test_field_info_serialization_alias_opt() {
        let field1 = FieldInfo::new("name", "name", SqlType::Text)
            .serialization_alias_opt(Some("user-name"));
        assert_eq!(field1.serialization_alias, Some("user-name"));

        let field2 = FieldInfo::new("name", "name", SqlType::Text).serialization_alias_opt(None);
        assert!(field2.serialization_alias.is_none());
    }

    #[test]
    fn test_field_info_output_name() {
        // No aliases - uses field name
        let field1 = FieldInfo::new("name", "name", SqlType::Text);
        assert_eq!(field1.output_name(), "name");

        // Only alias - uses alias
        let field2 = FieldInfo::new("name", "name", SqlType::Text).alias("nm");
        assert_eq!(field2.output_name(), "nm");

        // serialization_alias takes precedence
        let field3 = FieldInfo::new("name", "name", SqlType::Text)
            .alias("nm")
            .serialization_alias("outputName");
        assert_eq!(field3.output_name(), "outputName");

        // Only serialization_alias
        let field4 = FieldInfo::new("name", "name", SqlType::Text).serialization_alias("userName");
        assert_eq!(field4.output_name(), "userName");
    }

    #[test]
    fn test_field_info_matches_input_name() {
        // No aliases - only matches field name
        let field1 = FieldInfo::new("name", "name", SqlType::Text);
        assert!(field1.matches_input_name("name"));
        assert!(!field1.matches_input_name("userName"));

        // With alias - matches both
        let field2 = FieldInfo::new("name", "name", SqlType::Text).alias("nm");
        assert!(field2.matches_input_name("name"));
        assert!(field2.matches_input_name("nm"));
        assert!(!field2.matches_input_name("userName"));

        // With validation_alias - matches both
        let field3 = FieldInfo::new("name", "name", SqlType::Text).validation_alias("user_name");
        assert!(field3.matches_input_name("name"));
        assert!(field3.matches_input_name("user_name"));
        assert!(!field3.matches_input_name("userName"));

        // With both - matches all three
        let field4 = FieldInfo::new("name", "name", SqlType::Text)
            .alias("nm")
            .validation_alias("user_name");
        assert!(field4.matches_input_name("name"));
        assert!(field4.matches_input_name("nm"));
        assert!(field4.matches_input_name("user_name"));
    }

    #[test]
    fn test_field_info_has_alias() {
        let field1 = FieldInfo::new("name", "name", SqlType::Text);
        assert!(!field1.has_alias());

        let field2 = FieldInfo::new("name", "name", SqlType::Text).alias("nm");
        assert!(field2.has_alias());

        let field3 = FieldInfo::new("name", "name", SqlType::Text).validation_alias("user_name");
        assert!(field3.has_alias());

        let field4 = FieldInfo::new("name", "name", SqlType::Text).serialization_alias("userName");
        assert!(field4.has_alias());

        let field5 = FieldInfo::new("name", "name", SqlType::Text)
            .alias("nm")
            .validation_alias("user_name")
            .serialization_alias("userName");
        assert!(field5.has_alias());
    }

    #[test]
    fn test_field_info_exclude() {
        // Default: not excluded
        let field1 = FieldInfo::new("name", "name", SqlType::Text);
        assert!(!field1.exclude);

        // Excluded field
        let field2 = FieldInfo::new("password", "password", SqlType::Text).exclude(true);
        assert!(field2.exclude);

        // Exclude can be explicitly set to false
        let field3 = FieldInfo::new("email", "email", SqlType::Text).exclude(false);
        assert!(!field3.exclude);
    }

    #[test]
    fn test_field_info_exclude_combined_with_other_attrs() {
        // Exclude can be combined with other attributes
        let field = FieldInfo::new("secret", "secret", SqlType::Text)
            .exclude(true)
            .nullable(true)
            .alias("hidden_value");

        assert!(field.exclude);
        assert!(field.nullable);
        assert_eq!(field.alias, Some("hidden_value"));
    }

    #[test]
    fn test_field_info_title() {
        let field1 = FieldInfo::new("name", "name", SqlType::Text);
        assert_eq!(field1.title, None);

        let field2 = FieldInfo::new("name", "name", SqlType::Text).title("User Name");
        assert_eq!(field2.title, Some("User Name"));
    }

    #[test]
    fn test_field_info_description() {
        let field1 = FieldInfo::new("name", "name", SqlType::Text);
        assert_eq!(field1.description, None);

        let field2 =
            FieldInfo::new("name", "name", SqlType::Text).description("The full name of the user");
        assert_eq!(field2.description, Some("The full name of the user"));
    }

    #[test]
    fn test_field_info_schema_extra() {
        let field1 = FieldInfo::new("name", "name", SqlType::Text);
        assert_eq!(field1.schema_extra, None);

        let field2 =
            FieldInfo::new("name", "name", SqlType::Text).schema_extra(r#"{"examples": ["John"]}"#);
        assert_eq!(field2.schema_extra, Some(r#"{"examples": ["John"]}"#));
    }

    #[test]
    fn test_field_info_all_schema_metadata() {
        let field = FieldInfo::new("name", "name", SqlType::Text)
            .title("User Name")
            .description("The full name of the user")
            .schema_extra(r#"{"examples": ["John Doe"]}"#);

        assert_eq!(field.title, Some("User Name"));
        assert_eq!(field.description, Some("The full name of the user"));
        assert_eq!(field.schema_extra, Some(r#"{"examples": ["John Doe"]}"#));
    }

    #[test]
    fn test_field_info_const_field() {
        // Default should be false
        let field1 = FieldInfo::new("version", "version", SqlType::Text);
        assert!(!field1.const_field);

        // Explicitly set to true
        let field2 = FieldInfo::new("version", "version", SqlType::Text).const_field(true);
        assert!(field2.const_field);

        // Can combine with other attributes
        let field3 = FieldInfo::new("version", "version", SqlType::Text)
            .const_field(true)
            .default("'1.0.0'");
        assert!(field3.const_field);
        assert_eq!(field3.default, Some("'1.0.0'"));
    }

    #[test]
    fn test_field_info_column_constraints() {
        // Default should be empty
        let field1 = FieldInfo::new("price", "price", SqlType::Integer);
        assert!(field1.column_constraints.is_empty());

        // With constraints
        static CONSTRAINTS: &[&str] = &["CHECK(price > 0)", "CHECK(price < 1000)"];
        let field2 =
            FieldInfo::new("price", "price", SqlType::Integer).column_constraints(CONSTRAINTS);
        assert_eq!(field2.column_constraints.len(), 2);
        assert_eq!(field2.column_constraints[0], "CHECK(price > 0)");
    }

    #[test]
    fn test_field_info_column_comment() {
        // Default should be None
        let field1 = FieldInfo::new("name", "name", SqlType::Text);
        assert_eq!(field1.column_comment, None);

        // With comment
        let field2 =
            FieldInfo::new("name", "name", SqlType::Text).column_comment("User's display name");
        assert_eq!(field2.column_comment, Some("User's display name"));
    }

    #[test]
    fn test_field_info_column_info() {
        // Default should be None
        let field1 = FieldInfo::new("email", "email", SqlType::Text);
        assert_eq!(field1.column_info, None);

        // With info
        let field2 =
            FieldInfo::new("email", "email", SqlType::Text).column_info(r#"{"deprecated": true}"#);
        assert_eq!(field2.column_info, Some(r#"{"deprecated": true}"#));
    }
}
