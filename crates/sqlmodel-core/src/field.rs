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
}

impl FieldInfo {
    /// Create a new field info with minimal required data.
    pub const fn new(name: &'static str, column_name: &'static str, sql_type: SqlType) -> Self {
        Self {
            name,
            column_name,
            sql_type,
            sql_type_override: None,
            nullable: false,
            primary_key: false,
            auto_increment: false,
            unique: false,
            default: None,
            foreign_key: None,
            on_delete: None,
            on_update: None,
            index: None,
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

    /// Get the effective SQL type name for DDL generation.
    ///
    /// Returns `sql_type_override` if set, otherwise falls back to `sql_type.sql_name()`.
    #[must_use]
    pub fn effective_sql_type(&self) -> String {
        self.sql_type_override
            .map_or_else(|| self.sql_type.sql_name(), str::to_string)
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
