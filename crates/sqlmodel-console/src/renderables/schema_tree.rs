//! Schema tree visualization for database structure display.
//!
//! Displays database schema as a tree view for understanding table structure.
//!
//! # Example
//!
//! ```rust
//! use sqlmodel_console::renderables::{SchemaTree, SchemaTreeConfig};
//! use sqlmodel_schema::{TableInfo, ColumnInfo};
//!
//! let table = TableInfo {
//!     name: "heroes".to_string(),
//!     columns: vec![
//!         ColumnInfo {
//!             name: "id".to_string(),
//!             sql_type: "INTEGER".to_string(),
//!             nullable: false,
//!             default: None,
//!             primary_key: true,
//!             auto_increment: true,
//!         },
//!         ColumnInfo {
//!             name: "name".to_string(),
//!             sql_type: "TEXT".to_string(),
//!             nullable: false,
//!             default: None,
//!             primary_key: false,
//!             auto_increment: false,
//!         },
//!     ],
//!     primary_key: vec!["id".to_string()],
//!     foreign_keys: vec![],
//!     indexes: vec![],
//! };
//!
//! let tree = SchemaTree::new(&[table]);
//! println!("{}", tree.render_plain());
//! ```

use crate::theme::Theme;

/// Configuration for schema tree rendering.
#[derive(Debug, Clone)]
pub struct SchemaTreeConfig {
    /// Show column types
    pub show_types: bool,
    /// Show constraints (nullable, default, auto_increment)
    pub show_constraints: bool,
    /// Show indexes
    pub show_indexes: bool,
    /// Show foreign keys
    pub show_foreign_keys: bool,
    /// Theme for styled output
    pub theme: Option<Theme>,
    /// Use Unicode box drawing characters
    pub use_unicode: bool,
}

impl Default for SchemaTreeConfig {
    fn default() -> Self {
        Self {
            show_types: true,
            show_constraints: true,
            show_indexes: true,
            show_foreign_keys: true,
            theme: None,
            use_unicode: true,
        }
    }
}

impl SchemaTreeConfig {
    /// Create a new config with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to show column types.
    #[must_use]
    pub fn show_types(mut self, show: bool) -> Self {
        self.show_types = show;
        self
    }

    /// Set whether to show constraints.
    #[must_use]
    pub fn show_constraints(mut self, show: bool) -> Self {
        self.show_constraints = show;
        self
    }

    /// Set whether to show indexes.
    #[must_use]
    pub fn show_indexes(mut self, show: bool) -> Self {
        self.show_indexes = show;
        self
    }

    /// Set whether to show foreign keys.
    #[must_use]
    pub fn show_foreign_keys(mut self, show: bool) -> Self {
        self.show_foreign_keys = show;
        self
    }

    /// Set the theme for styled output.
    #[must_use]
    pub fn theme(mut self, theme: Theme) -> Self {
        self.theme = Some(theme);
        self
    }

    /// Use ASCII characters instead of Unicode.
    #[must_use]
    pub fn ascii(mut self) -> Self {
        self.use_unicode = false;
        self
    }

    /// Use Unicode box drawing characters.
    #[must_use]
    pub fn unicode(mut self) -> Self {
        self.use_unicode = true;
        self
    }
}

/// Simplified table info for rendering (avoids dependency on sqlmodel-schema).
#[derive(Debug, Clone)]
pub struct TableData {
    /// Table name
    pub name: String,
    /// Columns
    pub columns: Vec<ColumnData>,
    /// Primary key column names
    pub primary_key: Vec<String>,
    /// Foreign keys
    pub foreign_keys: Vec<ForeignKeyData>,
    /// Indexes
    pub indexes: Vec<IndexData>,
}

/// Simplified column info for rendering.
#[derive(Debug, Clone)]
pub struct ColumnData {
    /// Column name
    pub name: String,
    /// SQL type
    pub sql_type: String,
    /// Whether nullable
    pub nullable: bool,
    /// Default value
    pub default: Option<String>,
    /// Is primary key
    pub primary_key: bool,
    /// Auto increment
    pub auto_increment: bool,
}

/// Simplified foreign key info for rendering.
#[derive(Debug, Clone)]
pub struct ForeignKeyData {
    /// Constraint name
    pub name: Option<String>,
    /// Local column
    pub column: String,
    /// Foreign table
    pub foreign_table: String,
    /// Foreign column
    pub foreign_column: String,
    /// ON DELETE action
    pub on_delete: Option<String>,
    /// ON UPDATE action
    pub on_update: Option<String>,
}

/// Simplified index info for rendering.
#[derive(Debug, Clone)]
pub struct IndexData {
    /// Index name
    pub name: String,
    /// Columns
    pub columns: Vec<String>,
    /// Is unique
    pub unique: bool,
}

/// Schema tree view for visualizing database structure.
///
/// Displays database tables and their columns as an ASCII/Unicode tree.
#[derive(Debug, Clone)]
pub struct SchemaTree {
    /// Tables to display
    tables: Vec<TableData>,
    /// Configuration
    config: SchemaTreeConfig,
}

impl SchemaTree {
    /// Create a new schema tree from table data.
    #[must_use]
    pub fn new(tables: &[TableData]) -> Self {
        Self {
            tables: tables.to_vec(),
            config: SchemaTreeConfig::default(),
        }
    }

    /// Create an empty schema tree.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            tables: Vec::new(),
            config: SchemaTreeConfig::default(),
        }
    }

    /// Add a table to the schema tree.
    #[must_use]
    pub fn add_table(mut self, table: TableData) -> Self {
        self.tables.push(table);
        self
    }

    /// Set the configuration.
    #[must_use]
    pub fn config(mut self, config: SchemaTreeConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the theme for styled output.
    #[must_use]
    pub fn theme(mut self, theme: Theme) -> Self {
        self.config.theme = Some(theme);
        self
    }

    /// Use ASCII characters instead of Unicode.
    #[must_use]
    pub fn ascii(mut self) -> Self {
        self.config.use_unicode = false;
        self
    }

    /// Use Unicode box drawing characters.
    #[must_use]
    pub fn unicode(mut self) -> Self {
        self.config.use_unicode = true;
        self
    }

    /// Get tree drawing characters.
    fn chars(&self) -> (&'static str, &'static str, &'static str, &'static str) {
        if self.config.use_unicode {
            ("├── ", "└── ", "│   ", "    ")
        } else {
            ("+-- ", "\\-- ", "|   ", "    ")
        }
    }

    /// Render the schema as plain text.
    #[must_use]
    pub fn render_plain(&self) -> String {
        if self.tables.is_empty() {
            return "Schema: (empty)".to_string();
        }

        let mut lines = Vec::new();
        lines.push("Schema".to_string());

        let table_count = self.tables.len();
        for (i, table) in self.tables.iter().enumerate() {
            let is_last = i == table_count - 1;
            self.render_table_plain(table, "", is_last, &mut lines);
        }

        lines.join("\n")
    }

    /// Render a table in plain text.
    fn render_table_plain(
        &self,
        table: &TableData,
        prefix: &str,
        is_last: bool,
        lines: &mut Vec<String>,
    ) {
        let (branch, last_branch, vertical, space) = self.chars();
        let connector = if is_last { last_branch } else { branch };

        // Table name with icon
        let pk_info = if table.primary_key.is_empty() {
            String::new()
        } else {
            format!(" [PK: {}]", table.primary_key.join(", "))
        };
        lines.push(format!("{prefix}{connector}Table: {}{pk_info}", table.name));

        let child_prefix = if is_last {
            format!("{prefix}{space}")
        } else {
            format!("{prefix}{vertical}")
        };

        // Calculate total children for proper connectors
        #[allow(clippy::type_complexity)]
        let mut children: Vec<(&str, Box<dyn Fn(&str, bool, &mut Vec<String>) + '_>)> = Vec::new();

        // Columns section
        if !table.columns.is_empty() {
            let columns = table.columns.clone();
            children.push((
                "Columns",
                Box::new(move |prefix, is_last, lines| {
                    self.render_columns_plain(&columns, prefix, is_last, lines);
                }),
            ));
        }

        // Indexes section
        if self.config.show_indexes && !table.indexes.is_empty() {
            let indexes = table.indexes.clone();
            children.push((
                "Indexes",
                Box::new(move |prefix, is_last, lines| {
                    self.render_indexes_plain(&indexes, prefix, is_last, lines);
                }),
            ));
        }

        // Foreign keys section
        if self.config.show_foreign_keys && !table.foreign_keys.is_empty() {
            let fks = table.foreign_keys.clone();
            children.push((
                "Foreign Keys",
                Box::new(move |prefix, is_last, lines| {
                    self.render_fks_plain(&fks, prefix, is_last, lines);
                }),
            ));
        }

        // Render all sections
        let child_count = children.len();
        for (i, (label, render_fn)) in children.into_iter().enumerate() {
            let is_last_child = i == child_count - 1;
            let section_connector = if is_last_child { last_branch } else { branch };
            lines.push(format!("{child_prefix}{section_connector}{label}"));

            let section_prefix = if is_last_child {
                format!("{child_prefix}{space}")
            } else {
                format!("{child_prefix}{vertical}")
            };

            render_fn(&section_prefix, true, lines);
        }
    }

    /// Render columns in plain text.
    fn render_columns_plain(
        &self,
        columns: &[ColumnData],
        prefix: &str,
        _is_last: bool,
        lines: &mut Vec<String>,
    ) {
        let (branch, last_branch, _, _) = self.chars();

        let col_count = columns.len();
        for (i, col) in columns.iter().enumerate() {
            let is_last_col = i == col_count - 1;
            let connector = if is_last_col { last_branch } else { branch };

            let mut parts = vec![col.name.clone()];

            if self.config.show_types {
                parts.push(col.sql_type.clone());
            }

            if self.config.show_constraints {
                let mut constraints: Vec<String> = Vec::new();
                if col.primary_key {
                    constraints.push("PK".into());
                }
                if col.auto_increment {
                    constraints.push("AUTO".into());
                }
                if !col.nullable {
                    constraints.push("NOT NULL".into());
                }
                if let Some(ref default) = col.default {
                    constraints.push(format!("DEFAULT {default}"));
                }
                if !constraints.is_empty() {
                    parts.push(format!("[{}]", constraints.join(", ")));
                }
            }

            lines.push(format!("{prefix}{connector}{}", parts.join(" ")));
        }
    }

    /// Render indexes in plain text.
    fn render_indexes_plain(
        &self,
        indexes: &[IndexData],
        prefix: &str,
        _is_last: bool,
        lines: &mut Vec<String>,
    ) {
        let (branch, last_branch, _, _) = self.chars();

        let idx_count = indexes.len();
        for (i, idx) in indexes.iter().enumerate() {
            let is_last_idx = i == idx_count - 1;
            let connector = if is_last_idx { last_branch } else { branch };

            let unique_marker = if idx.unique { "UNIQUE " } else { "" };
            lines.push(format!(
                "{prefix}{connector}{unique_marker}{} ({})",
                idx.name,
                idx.columns.join(", ")
            ));
        }
    }

    /// Render foreign keys in plain text.
    fn render_fks_plain(
        &self,
        fks: &[ForeignKeyData],
        prefix: &str,
        _is_last: bool,
        lines: &mut Vec<String>,
    ) {
        let (branch, last_branch, _, _) = self.chars();

        let fk_count = fks.len();
        for (i, fk) in fks.iter().enumerate() {
            let is_last_fk = i == fk_count - 1;
            let connector = if is_last_fk { last_branch } else { branch };

            let name = fk.name.as_deref().unwrap_or("(unnamed)");
            let mut parts = vec![format!(
                "{}: {} -> {}.{}",
                name, fk.column, fk.foreign_table, fk.foreign_column
            )];

            if let Some(ref on_delete) = fk.on_delete {
                parts.push(format!("ON DELETE {on_delete}"));
            }
            if let Some(ref on_update) = fk.on_update {
                parts.push(format!("ON UPDATE {on_update}"));
            }

            lines.push(format!("{prefix}{connector}{}", parts.join(" ")));
        }
    }

    /// Render the schema as styled text with ANSI colors.
    #[must_use]
    pub fn render_styled(&self) -> String {
        let theme = self.config.theme.clone().unwrap_or_default();

        if self.tables.is_empty() {
            let dim = theme.dim.color_code();
            let reset = "\x1b[0m";
            return format!("{dim}Schema: (empty){reset}");
        }

        let mut lines = Vec::new();
        let keyword_color = theme.sql_keyword.color_code();
        let reset = "\x1b[0m";
        lines.push(format!("{keyword_color}Schema{reset}"));

        let table_count = self.tables.len();
        for (i, table) in self.tables.iter().enumerate() {
            let is_last = i == table_count - 1;
            self.render_table_styled(table, "", is_last, &mut lines, &theme);
        }

        lines.join("\n")
    }

    /// Render a table with styling.
    fn render_table_styled(
        &self,
        table: &TableData,
        prefix: &str,
        is_last: bool,
        lines: &mut Vec<String>,
        theme: &Theme,
    ) {
        let (branch, last_branch, vertical, space) = self.chars();
        let connector = if is_last { last_branch } else { branch };

        let reset = "\x1b[0m";
        let dim = theme.dim.color_code();
        let table_color = theme.sql_keyword.color_code();
        let name_color = theme.sql_identifier.color_code();
        let pk_color = theme.dim.color_code();

        // Table name
        let pk_info = if table.primary_key.is_empty() {
            String::new()
        } else {
            format!(" {pk_color}[PK: {}]{reset}", table.primary_key.join(", "))
        };
        lines.push(format!(
            "{dim}{prefix}{connector}{reset}{table_color}Table:{reset} {name_color}{}{reset}{pk_info}",
            table.name
        ));

        let child_prefix = if is_last {
            format!("{prefix}{space}")
        } else {
            format!("{prefix}{vertical}")
        };

        // Sections
        #[allow(clippy::type_complexity)]
        let mut sections: Vec<(&str, Box<dyn Fn(&str, bool, &mut Vec<String>, &Theme) + '_>)> =
            Vec::new();

        if !table.columns.is_empty() {
            let columns = table.columns.clone();
            sections.push((
                "Columns",
                Box::new(move |prefix, is_last, lines, theme| {
                    self.render_columns_styled(&columns, prefix, is_last, lines, theme);
                }),
            ));
        }

        if self.config.show_indexes && !table.indexes.is_empty() {
            let indexes = table.indexes.clone();
            sections.push((
                "Indexes",
                Box::new(move |prefix, is_last, lines, theme| {
                    self.render_indexes_styled(&indexes, prefix, is_last, lines, theme);
                }),
            ));
        }

        if self.config.show_foreign_keys && !table.foreign_keys.is_empty() {
            let fks = table.foreign_keys.clone();
            sections.push((
                "Foreign Keys",
                Box::new(move |prefix, is_last, lines, theme| {
                    self.render_fks_styled(&fks, prefix, is_last, lines, theme);
                }),
            ));
        }

        let section_count = sections.len();
        for (i, (label, render_fn)) in sections.into_iter().enumerate() {
            let is_last_section = i == section_count - 1;
            let section_connector = if is_last_section { last_branch } else { branch };
            let header_color = theme.sql_keyword.color_code();
            lines.push(format!(
                "{dim}{child_prefix}{section_connector}{reset}{header_color}{label}{reset}"
            ));

            let section_prefix = if is_last_section {
                format!("{child_prefix}{space}")
            } else {
                format!("{child_prefix}{vertical}")
            };

            render_fn(&section_prefix, true, lines, theme);
        }
    }

    /// Render columns with styling.
    fn render_columns_styled(
        &self,
        columns: &[ColumnData],
        prefix: &str,
        _is_last: bool,
        lines: &mut Vec<String>,
        theme: &Theme,
    ) {
        let (branch, last_branch, _, _) = self.chars();
        let reset = "\x1b[0m";
        let dim = theme.dim.color_code();
        let name_color = theme.sql_identifier.color_code();
        let type_color = theme.sql_keyword.color_code();
        let constraint_color = theme.dim.color_code();

        let col_count = columns.len();
        for (i, col) in columns.iter().enumerate() {
            let is_last_col = i == col_count - 1;
            let connector = if is_last_col { last_branch } else { branch };

            let mut line = format!(
                "{dim}{prefix}{connector}{reset}{name_color}{}{reset}",
                col.name
            );

            if self.config.show_types {
                line.push_str(&format!(" {type_color}{}{reset}", col.sql_type));
            }

            if self.config.show_constraints {
                let mut constraints: Vec<String> = Vec::new();
                if col.primary_key {
                    constraints.push("PK".into());
                }
                if col.auto_increment {
                    constraints.push("AUTO".into());
                }
                if !col.nullable {
                    constraints.push("NOT NULL".into());
                }
                if let Some(ref default) = col.default {
                    constraints.push(format!("DEFAULT {default}"));
                }
                if !constraints.is_empty() {
                    line.push_str(&format!(
                        " {constraint_color}[{}]{reset}",
                        constraints.join(", ")
                    ));
                }
            }

            lines.push(line);
        }
    }

    /// Render indexes with styling.
    fn render_indexes_styled(
        &self,
        indexes: &[IndexData],
        prefix: &str,
        _is_last: bool,
        lines: &mut Vec<String>,
        theme: &Theme,
    ) {
        let (branch, last_branch, _, _) = self.chars();
        let reset = "\x1b[0m";
        let dim = theme.dim.color_code();
        let name_color = theme.sql_identifier.color_code();
        let keyword_color = theme.sql_keyword.color_code();

        let idx_count = indexes.len();
        for (i, idx) in indexes.iter().enumerate() {
            let is_last_idx = i == idx_count - 1;
            let connector = if is_last_idx { last_branch } else { branch };

            let unique_marker = if idx.unique {
                format!("{keyword_color}UNIQUE {reset}")
            } else {
                String::new()
            };

            lines.push(format!(
                "{dim}{prefix}{connector}{reset}{unique_marker}{name_color}{}{reset} ({dim}{}{reset})",
                idx.name,
                idx.columns.join(", ")
            ));
        }
    }

    /// Render foreign keys with styling.
    fn render_fks_styled(
        &self,
        fks: &[ForeignKeyData],
        prefix: &str,
        _is_last: bool,
        lines: &mut Vec<String>,
        theme: &Theme,
    ) {
        let (branch, last_branch, _, _) = self.chars();
        let reset = "\x1b[0m";
        let dim = theme.dim.color_code();
        let name_color = theme.sql_identifier.color_code();
        let ref_color = theme.string_value.color_code();

        let fk_count = fks.len();
        for (i, fk) in fks.iter().enumerate() {
            let is_last_fk = i == fk_count - 1;
            let connector = if is_last_fk { last_branch } else { branch };

            let name = fk.name.as_deref().unwrap_or("(unnamed)");

            let mut line = format!(
                "{dim}{prefix}{connector}{reset}{name_color}{name}{reset}: {dim}{}{reset} -> {ref_color}{}.{}{reset}",
                fk.column, fk.foreign_table, fk.foreign_column
            );

            if let Some(ref on_delete) = fk.on_delete {
                line.push_str(&format!(" {dim}ON DELETE {on_delete}{reset}"));
            }
            if let Some(ref on_update) = fk.on_update {
                line.push_str(&format!(" {dim}ON UPDATE {on_update}{reset}"));
            }

            lines.push(line);
        }
    }

    /// Render as JSON-serializable structure.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        let tables: Vec<serde_json::Value> = self.tables.iter().map(Self::table_to_json).collect();

        serde_json::json!({
            "schema": {
                "tables": tables
            }
        })
    }

    /// Convert a table to JSON.
    fn table_to_json(table: &TableData) -> serde_json::Value {
        let columns: Vec<serde_json::Value> = table
            .columns
            .iter()
            .map(|col| {
                serde_json::json!({
                    "name": col.name,
                    "type": col.sql_type,
                    "nullable": col.nullable,
                    "default": col.default,
                    "primary_key": col.primary_key,
                    "auto_increment": col.auto_increment,
                })
            })
            .collect();

        let indexes: Vec<serde_json::Value> = table
            .indexes
            .iter()
            .map(|idx| {
                serde_json::json!({
                    "name": idx.name,
                    "columns": idx.columns,
                    "unique": idx.unique,
                })
            })
            .collect();

        let foreign_keys: Vec<serde_json::Value> = table
            .foreign_keys
            .iter()
            .map(|fk| {
                serde_json::json!({
                    "name": fk.name,
                    "column": fk.column,
                    "foreign_table": fk.foreign_table,
                    "foreign_column": fk.foreign_column,
                    "on_delete": fk.on_delete,
                    "on_update": fk.on_update,
                })
            })
            .collect();

        serde_json::json!({
            "name": table.name,
            "columns": columns,
            "primary_key": table.primary_key,
            "indexes": indexes,
            "foreign_keys": foreign_keys,
        })
    }
}

impl Default for SchemaTree {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_column(name: &str, sql_type: &str, primary_key: bool) -> ColumnData {
        ColumnData {
            name: name.to_string(),
            sql_type: sql_type.to_string(),
            nullable: !primary_key,
            default: None,
            primary_key,
            auto_increment: primary_key,
        }
    }

    fn sample_table() -> TableData {
        TableData {
            name: "heroes".to_string(),
            columns: vec![
                sample_column("id", "INTEGER", true),
                sample_column("name", "TEXT", false),
                sample_column("secret_name", "TEXT", false),
            ],
            primary_key: vec!["id".to_string()],
            foreign_keys: vec![],
            indexes: vec![],
        }
    }

    fn sample_table_with_fk() -> TableData {
        TableData {
            name: "team_members".to_string(),
            columns: vec![
                sample_column("id", "INTEGER", true),
                sample_column("hero_id", "INTEGER", false),
                sample_column("team_id", "INTEGER", false),
            ],
            primary_key: vec!["id".to_string()],
            foreign_keys: vec![
                ForeignKeyData {
                    name: Some("fk_hero".to_string()),
                    column: "hero_id".to_string(),
                    foreign_table: "heroes".to_string(),
                    foreign_column: "id".to_string(),
                    on_delete: Some("CASCADE".to_string()),
                    on_update: None,
                },
                ForeignKeyData {
                    name: Some("fk_team".to_string()),
                    column: "team_id".to_string(),
                    foreign_table: "teams".to_string(),
                    foreign_column: "id".to_string(),
                    on_delete: Some("SET NULL".to_string()),
                    on_update: Some("CASCADE".to_string()),
                },
            ],
            indexes: vec![IndexData {
                name: "idx_hero_team".to_string(),
                columns: vec!["hero_id".to_string(), "team_id".to_string()],
                unique: true,
            }],
        }
    }

    #[test]
    fn test_empty_schema() {
        let tree = SchemaTree::empty();
        let output = tree.render_plain();
        assert_eq!(output, "Schema: (empty)");
    }

    #[test]
    fn test_schema_tree_new() {
        let tree = SchemaTree::new(&[sample_table()]);
        let output = tree.render_plain();
        assert!(output.contains("Schema"));
        assert!(output.contains("Table: heroes"));
    }

    #[test]
    fn test_schema_tree_columns() {
        let tree = SchemaTree::new(&[sample_table()]);
        let output = tree.render_plain();
        assert!(output.contains("Columns"));
        assert!(output.contains("id INTEGER"));
        assert!(output.contains("name TEXT"));
        assert!(output.contains("secret_name TEXT"));
    }

    #[test]
    fn test_schema_tree_primary_key() {
        let tree = SchemaTree::new(&[sample_table()]);
        let output = tree.render_plain();
        assert!(output.contains("[PK: id]"));
        assert!(output.contains("[PK, AUTO, NOT NULL]"));
    }

    #[test]
    fn test_schema_tree_indexes() {
        let tree = SchemaTree::new(&[sample_table_with_fk()]);
        let output = tree.render_plain();
        assert!(output.contains("Indexes"));
        assert!(output.contains("UNIQUE idx_hero_team"));
        assert!(output.contains("hero_id, team_id"));
    }

    #[test]
    fn test_schema_tree_foreign_keys() {
        let tree = SchemaTree::new(&[sample_table_with_fk()]);
        let output = tree.render_plain();
        assert!(output.contains("Foreign Keys"));
        assert!(output.contains("fk_hero: hero_id -> heroes.id"));
        assert!(output.contains("ON DELETE CASCADE"));
        assert!(output.contains("fk_team: team_id -> teams.id"));
        assert!(output.contains("ON UPDATE CASCADE"));
    }

    #[test]
    fn test_schema_tree_unicode() {
        let tree = SchemaTree::new(&[sample_table()]).unicode();
        let output = tree.render_plain();
        assert!(output.contains("├── ") || output.contains("└── "));
    }

    #[test]
    fn test_schema_tree_ascii() {
        let tree = SchemaTree::new(&[sample_table()]).ascii();
        let output = tree.render_plain();
        assert!(output.contains("+-- ") || output.contains("\\-- "));
    }

    #[test]
    fn test_schema_tree_styled_contains_ansi() {
        let tree = SchemaTree::new(&[sample_table()]);
        let styled = tree.render_styled();
        assert!(styled.contains('\x1b'));
    }

    #[test]
    fn test_schema_tree_config_no_types() {
        let config = SchemaTreeConfig::new().show_types(false);
        let tree = SchemaTree::new(&[sample_table()]).config(config);
        let output = tree.render_plain();
        assert!(output.contains("id"));
        assert!(!output.contains("INTEGER"));
    }

    #[test]
    fn test_schema_tree_config_no_constraints() {
        let config = SchemaTreeConfig::new().show_constraints(false);
        let tree = SchemaTree::new(&[sample_table()]).config(config);
        let output = tree.render_plain();
        assert!(!output.contains("[PK"));
        assert!(!output.contains("NOT NULL"));
    }

    #[test]
    fn test_schema_tree_config_no_indexes() {
        let config = SchemaTreeConfig::new().show_indexes(false);
        let tree = SchemaTree::new(&[sample_table_with_fk()]).config(config);
        let output = tree.render_plain();
        assert!(!output.contains("Indexes"));
    }

    #[test]
    fn test_schema_tree_config_no_fks() {
        let config = SchemaTreeConfig::new().show_foreign_keys(false);
        let tree = SchemaTree::new(&[sample_table_with_fk()]).config(config);
        let output = tree.render_plain();
        assert!(!output.contains("Foreign Keys"));
    }

    #[test]
    fn test_schema_tree_to_json() {
        let tree = SchemaTree::new(&[sample_table()]);
        let json = tree.to_json();
        assert!(json["schema"]["tables"].is_array());
        assert_eq!(json["schema"]["tables"][0]["name"], "heroes");
        assert!(json["schema"]["tables"][0]["columns"].is_array());
    }

    #[test]
    fn test_schema_tree_multiple_tables() {
        let tree = SchemaTree::new(&[sample_table(), sample_table_with_fk()]);
        let output = tree.render_plain();
        assert!(output.contains("Table: heroes"));
        assert!(output.contains("Table: team_members"));
    }

    #[test]
    fn test_schema_tree_add_table() {
        let tree = SchemaTree::empty().add_table(sample_table());
        let output = tree.render_plain();
        assert!(output.contains("Table: heroes"));
    }

    #[test]
    fn test_default() {
        let tree = SchemaTree::default();
        let output = tree.render_plain();
        assert!(output.contains("(empty)"));
    }

    #[test]
    fn test_column_with_default() {
        let mut table = sample_table();
        table.columns.push(ColumnData {
            name: "status".to_string(),
            sql_type: "TEXT".to_string(),
            nullable: true,
            default: Some("'active'".to_string()),
            primary_key: false,
            auto_increment: false,
        });

        let tree = SchemaTree::new(&[table]);
        let output = tree.render_plain();
        assert!(output.contains("DEFAULT 'active'"));
    }
}
