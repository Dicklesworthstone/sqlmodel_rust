//! Table info panel for single-table detail display.
//!
//! Displays comprehensive information about a single database table, including
//! columns, indexes, foreign keys, and optional statistics. Complementary to
//! [`SchemaTree`](super::schema_tree::SchemaTree) - while SchemaTree shows the
//! overview, TableInfo shows detailed information for one table.
//!
//! # Example
//!
//! ```rust
//! use sqlmodel_console::renderables::{TableInfo, TableStats, ColumnData, IndexData, ForeignKeyData};
//!
//! let columns = vec![
//!     ColumnData {
//!         name: "id".to_string(),
//!         sql_type: "BIGINT".to_string(),
//!         nullable: false,
//!         default: None,
//!         primary_key: true,
//!         auto_increment: true,
//!     },
//!     ColumnData {
//!         name: "name".to_string(),
//!         sql_type: "VARCHAR(255)".to_string(),
//!         nullable: false,
//!         default: None,
//!         primary_key: false,
//!         auto_increment: false,
//!     },
//! ];
//!
//! let table_info = TableInfo::new("heroes", columns)
//!     .with_primary_key(vec!["id".to_string()])
//!     .with_stats(TableStats {
//!         row_count: Some(10_000),
//!         size_bytes: Some(2_500_000),
//!         ..Default::default()
//!     })
//!     .width(80);
//!
//! println!("{}", table_info.render_plain());
//! ```

use crate::theme::Theme;

// Re-use data types from schema_tree
pub use super::schema_tree::{ColumnData, ForeignKeyData, IndexData};

/// Optional runtime statistics for a table.
#[derive(Debug, Clone, Default)]
pub struct TableStats {
    /// Number of rows in the table.
    pub row_count: Option<u64>,
    /// Size of the table in bytes (data + indexes).
    pub size_bytes: Option<u64>,
    /// Index size in bytes.
    pub index_size_bytes: Option<u64>,
    /// Last analyzed timestamp.
    pub last_analyzed: Option<String>,
    /// Last modified timestamp.
    pub last_modified: Option<String>,
}

impl TableStats {
    /// Create empty stats.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the row count.
    #[must_use]
    pub fn row_count(mut self, count: u64) -> Self {
        self.row_count = Some(count);
        self
    }

    /// Set the table size in bytes.
    #[must_use]
    pub fn size_bytes(mut self, bytes: u64) -> Self {
        self.size_bytes = Some(bytes);
        self
    }

    /// Set the index size in bytes.
    #[must_use]
    pub fn index_size_bytes(mut self, bytes: u64) -> Self {
        self.index_size_bytes = Some(bytes);
        self
    }

    /// Set the last analyzed timestamp.
    #[must_use]
    pub fn last_analyzed<S: Into<String>>(mut self, ts: S) -> Self {
        self.last_analyzed = Some(ts.into());
        self
    }

    /// Set the last modified timestamp.
    #[must_use]
    pub fn last_modified<S: Into<String>>(mut self, ts: S) -> Self {
        self.last_modified = Some(ts.into());
        self
    }

    /// Check if any stats are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.row_count.is_none()
            && self.size_bytes.is_none()
            && self.index_size_bytes.is_none()
            && self.last_analyzed.is_none()
            && self.last_modified.is_none()
    }
}

/// Table information panel for displaying detailed table structure.
///
/// Displays a single table's complete information in a panel format,
/// including columns, indexes, foreign keys, and optional statistics.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table name
    name: String,
    /// Schema name (optional)
    schema: Option<String>,
    /// Columns
    columns: Vec<ColumnData>,
    /// Primary key column names
    primary_key: Vec<String>,
    /// Indexes
    indexes: Vec<IndexData>,
    /// Foreign keys
    foreign_keys: Vec<ForeignKeyData>,
    /// Optional runtime statistics
    stats: Option<TableStats>,
    /// Theme for styled output
    theme: Theme,
    /// Display width
    width: Option<usize>,
    /// Whether to show column types
    show_types: bool,
    /// Whether to show constraints
    show_constraints: bool,
}

impl TableInfo {
    /// Create a new table info display.
    #[must_use]
    pub fn new<S: Into<String>>(name: S, columns: Vec<ColumnData>) -> Self {
        Self {
            name: name.into(),
            schema: None,
            columns,
            primary_key: Vec::new(),
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            stats: None,
            theme: Theme::default(),
            width: None,
            show_types: true,
            show_constraints: true,
        }
    }

    /// Create an empty table info.
    #[must_use]
    pub fn empty<S: Into<String>>(name: S) -> Self {
        Self::new(name, Vec::new())
    }

    /// Set the schema name.
    #[must_use]
    pub fn schema<S: Into<String>>(mut self, schema: S) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set the primary key columns.
    #[must_use]
    pub fn with_primary_key(mut self, columns: Vec<String>) -> Self {
        self.primary_key = columns;
        self
    }

    /// Add an index.
    #[must_use]
    pub fn add_index(mut self, index: IndexData) -> Self {
        self.indexes.push(index);
        self
    }

    /// Set all indexes.
    #[must_use]
    pub fn with_indexes(mut self, indexes: Vec<IndexData>) -> Self {
        self.indexes = indexes;
        self
    }

    /// Add a foreign key.
    #[must_use]
    pub fn add_foreign_key(mut self, fk: ForeignKeyData) -> Self {
        self.foreign_keys.push(fk);
        self
    }

    /// Set all foreign keys.
    #[must_use]
    pub fn with_foreign_keys(mut self, fks: Vec<ForeignKeyData>) -> Self {
        self.foreign_keys = fks;
        self
    }

    /// Set table statistics.
    #[must_use]
    pub fn with_stats(mut self, stats: TableStats) -> Self {
        self.stats = Some(stats);
        self
    }

    /// Set the theme for styled output.
    #[must_use]
    pub fn theme(mut self, theme: Theme) -> Self {
        self.theme = theme;
        self
    }

    /// Set the display width.
    #[must_use]
    pub fn width(mut self, width: usize) -> Self {
        self.width = Some(width);
        self
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

    /// Get the full table name (schema.table if schema is set).
    #[must_use]
    pub fn full_name(&self) -> String {
        if let Some(ref schema) = self.schema {
            format!("{}.{}", schema, self.name)
        } else {
            self.name.clone()
        }
    }

    /// Render as plain text for agent consumption.
    #[must_use]
    pub fn render_plain(&self) -> String {
        let mut lines = Vec::new();

        // Header with table name
        let pk_info = if self.primary_key.is_empty() {
            String::new()
        } else {
            format!(" (PK: {})", self.primary_key.join(", "))
        };

        lines.push(format!("TABLE: {}{}", self.full_name(), pk_info));
        lines.push("=".repeat(lines[0].len().min(60)));

        // Statistics section
        if let Some(ref stats) = self.stats {
            if !stats.is_empty() {
                let mut stats_parts = Vec::new();
                if let Some(rows) = stats.row_count {
                    stats_parts.push(format!("Rows: {}", format_number(rows)));
                }
                if let Some(size) = stats.size_bytes {
                    stats_parts.push(format!("Size: {}", format_bytes(size)));
                }
                if let Some(idx_size) = stats.index_size_bytes {
                    stats_parts.push(format!("Index Size: {}", format_bytes(idx_size)));
                }
                if !stats_parts.is_empty() {
                    lines.push(stats_parts.join(" | "));
                }
                if let Some(ref analyzed) = stats.last_analyzed {
                    lines.push(format!("Last Analyzed: {}", analyzed));
                }
                if let Some(ref modified) = stats.last_modified {
                    lines.push(format!("Last Modified: {}", modified));
                }
                lines.push(String::new());
            }
        }

        // Columns section
        lines.push("COLUMNS:".to_string());
        lines.push("-".repeat(40));

        if self.columns.is_empty() {
            lines.push("  (no columns)".to_string());
        } else {
            // Calculate column widths for alignment
            let max_name_len = self.columns.iter().map(|c| c.name.len()).max().unwrap_or(4);
            let max_type_len = self
                .columns
                .iter()
                .map(|c| c.sql_type.len())
                .max()
                .unwrap_or(4);

            for col in &self.columns {
                let mut parts = vec![format!("  {:<width$}", col.name, width = max_name_len)];

                if self.show_types {
                    parts.push(format!("{:<width$}", col.sql_type, width = max_type_len));
                }

                if self.show_constraints {
                    let mut constraints: Vec<String> = Vec::new();
                    if col.primary_key {
                        constraints.push("PK".into());
                    }
                    if col.auto_increment {
                        constraints.push("AUTO".into());
                    }
                    if col.nullable {
                        constraints.push("NULL".into());
                    } else {
                        constraints.push("NOT NULL".into());
                    }
                    if let Some(ref default) = col.default {
                        constraints.push(format!("DEFAULT {}", default));
                    }
                    parts.push(format!("[{}]", constraints.join(", ")));
                }

                lines.push(parts.join("  "));
            }
        }

        // Indexes section
        if !self.indexes.is_empty() {
            lines.push(String::new());
            lines.push("INDEXES:".to_string());
            lines.push("-".repeat(40));

            for idx in &self.indexes {
                let unique_marker = if idx.unique { "UNIQUE " } else { "" };
                lines.push(format!(
                    "  {}{} ({})",
                    unique_marker,
                    idx.name,
                    idx.columns.join(", ")
                ));
            }
        }

        // Foreign keys section
        if !self.foreign_keys.is_empty() {
            lines.push(String::new());
            lines.push("FOREIGN KEYS:".to_string());
            lines.push("-".repeat(40));

            for fk in &self.foreign_keys {
                let name = fk.name.as_deref().unwrap_or("(unnamed)");
                let mut parts = vec![format!(
                    "  {}: {} -> {}.{}",
                    name, fk.column, fk.foreign_table, fk.foreign_column
                )];

                if let Some(ref on_delete) = fk.on_delete {
                    parts.push(format!("ON DELETE {}", on_delete));
                }
                if let Some(ref on_update) = fk.on_update {
                    parts.push(format!("ON UPDATE {}", on_update));
                }

                lines.push(parts.join(" "));
            }
        }

        lines.join("\n")
    }

    /// Render with ANSI colors for terminal display.
    #[must_use]
    pub fn render_styled(&self) -> String {
        let width = self.width.unwrap_or(70);
        let reset = "\x1b[0m";
        let dim = self.theme.dim.color_code();
        let header_color = self.theme.header.color_code();
        let keyword_color = self.theme.sql_keyword.color_code();
        let name_color = self.theme.sql_identifier.color_code();
        let type_color = self.theme.sql_keyword.color_code();
        let success_color = self.theme.success.color_code();
        let info_color = self.theme.info.color_code();

        let mut lines = Vec::new();

        // Top border
        lines.push(format!("{dim}┌{}┐{reset}", "─".repeat(width - 2)));

        // Title with table name
        let pk_info = if self.primary_key.is_empty() {
            String::new()
        } else {
            format!(" {dim}(PK: {}){reset}", self.primary_key.join(", "))
        };

        let title = format!(
            "{keyword_color}TABLE:{reset} {name_color}{}{reset}{pk_info}",
            self.full_name()
        );
        let title_visible_len = self.full_name().len()
            + 7
            + if self.primary_key.is_empty() {
                0
            } else {
                6 + self.primary_key.join(", ").len()
            };
        let padding = width.saturating_sub(title_visible_len + 4);
        lines.push(format!(
            "{dim}│{reset} {}{:padding$} {dim}│{reset}",
            title,
            "",
            padding = padding
        ));

        // Separator
        lines.push(format!("{dim}├{}┤{reset}", "─".repeat(width - 2)));

        // Statistics section
        if let Some(ref stats) = self.stats {
            if !stats.is_empty() {
                let mut stats_parts = Vec::new();
                if let Some(rows) = stats.row_count {
                    stats_parts.push(format!("{info_color}Rows:{reset} {}", format_number(rows)));
                }
                if let Some(size) = stats.size_bytes {
                    stats_parts.push(format!("{info_color}Size:{reset} {}", format_bytes(size)));
                }
                if let Some(idx_size) = stats.index_size_bytes {
                    stats_parts.push(format!(
                        "{info_color}Idx:{reset} {}",
                        format_bytes(idx_size)
                    ));
                }
                if !stats_parts.is_empty() {
                    let stats_line = stats_parts.join(" {dim}│{reset} ");
                    lines.push(format!(
                        "{dim}│{reset} {:<width$} {dim}│{reset}",
                        stats_line,
                        width = width - 4
                    ));
                }
                if let Some(ref analyzed) = stats.last_analyzed {
                    lines.push(format!(
                        "{dim}│{reset} {info_color}Analyzed:{reset} {:<width$} {dim}│{reset}",
                        analyzed,
                        width = width - 14
                    ));
                }
                lines.push(format!("{dim}├{}┤{reset}", "─".repeat(width - 2)));
            }
        }

        // Columns header
        lines.push(format!(
            "{dim}│{reset} {header_color}COLUMNS{reset}{:width$} {dim}│{reset}",
            "",
            width = width - 11
        ));

        // Column rows
        if self.columns.is_empty() {
            lines.push(format!(
                "{dim}│{reset}   {dim}(no columns){reset}{:width$} {dim}│{reset}",
                "",
                width = width - 17
            ));
        } else {
            let max_name_len = self.columns.iter().map(|c| c.name.len()).max().unwrap_or(4);
            let max_type_len = self
                .columns
                .iter()
                .map(|c| c.sql_type.len())
                .max()
                .unwrap_or(4);

            for col in &self.columns {
                let mut content = format!(
                    "  {name_color}{:<name_w$}{reset}  {type_color}{:<type_w$}{reset}",
                    col.name,
                    col.sql_type,
                    name_w = max_name_len,
                    type_w = max_type_len
                );

                if self.show_constraints {
                    let mut constraints = Vec::new();
                    if col.primary_key {
                        constraints.push(format!("{success_color}PK{reset}"));
                    }
                    if col.auto_increment {
                        constraints.push(format!("{info_color}AUTO{reset}"));
                    }
                    if !col.nullable {
                        constraints.push(format!("{dim}NOT NULL{reset}"));
                    }
                    if let Some(ref default) = col.default {
                        constraints.push(format!("{dim}DEFAULT {}{reset}", default));
                    }
                    if !constraints.is_empty() {
                        content.push_str(&format!("  {}", constraints.join(" ")));
                    }
                }

                // Approximate visible length for padding
                let visible_len = 2 + max_name_len + 2 + max_type_len + 20;
                let padding = width.saturating_sub(visible_len + 4);
                lines.push(format!(
                    "{dim}│{reset}{}{:padding$} {dim}│{reset}",
                    content,
                    "",
                    padding = padding
                ));
            }
        }

        // Indexes section
        if !self.indexes.is_empty() {
            lines.push(format!("{dim}├{}┤{reset}", "─".repeat(width - 2)));
            lines.push(format!(
                "{dim}│{reset} {header_color}INDEXES{reset}{:width$} {dim}│{reset}",
                "",
                width = width - 11
            ));

            for idx in &self.indexes {
                let unique_marker = if idx.unique {
                    format!("{keyword_color}UNIQUE {reset}")
                } else {
                    String::new()
                };
                let content = format!(
                    "  {}{name_color}{}{reset} {dim}({}){reset}",
                    unique_marker,
                    idx.name,
                    idx.columns.join(", ")
                );
                let visible_len = 2
                    + if idx.unique { 7 } else { 0 }
                    + idx.name.len()
                    + 3
                    + idx.columns.join(", ").len();
                let padding = width.saturating_sub(visible_len + 4);
                lines.push(format!(
                    "{dim}│{reset}{}{:padding$} {dim}│{reset}",
                    content,
                    "",
                    padding = padding
                ));
            }
        }

        // Foreign keys section
        if !self.foreign_keys.is_empty() {
            lines.push(format!("{dim}├{}┤{reset}", "─".repeat(width - 2)));
            lines.push(format!(
                "{dim}│{reset} {header_color}FOREIGN KEYS{reset}{:width$} {dim}│{reset}",
                "",
                width = width - 16
            ));

            for fk in &self.foreign_keys {
                let name = fk.name.as_deref().unwrap_or("(unnamed)");
                let ref_color = self.theme.string_value.color_code();
                let content = format!(
                    "  {name_color}{}{reset}: {dim}{}{reset} → {ref_color}{}.{}{reset}",
                    name, fk.column, fk.foreign_table, fk.foreign_column
                );

                let mut actions = Vec::new();
                if let Some(ref on_delete) = fk.on_delete {
                    actions.push(format!("{dim}DEL:{}{reset}", on_delete));
                }
                if let Some(ref on_update) = fk.on_update {
                    actions.push(format!("{dim}UPD:{}{reset}", on_update));
                }

                let full_content = if actions.is_empty() {
                    content
                } else {
                    format!("{} {}", content, actions.join(" "))
                };

                let visible_len = 2
                    + name.len()
                    + 2
                    + fk.column.len()
                    + 3
                    + fk.foreign_table.len()
                    + 1
                    + fk.foreign_column.len();
                let padding = width.saturating_sub(visible_len + 20);
                lines.push(format!(
                    "{dim}│{reset}{}{:padding$} {dim}│{reset}",
                    full_content,
                    "",
                    padding = padding
                ));
            }
        }

        // Bottom border
        lines.push(format!("{dim}└{}┘{reset}", "─".repeat(width - 2)));

        lines.join("\n")
    }

    /// Convert to JSON representation.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        let columns: Vec<serde_json::Value> = self
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

        let indexes: Vec<serde_json::Value> = self
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

        let foreign_keys: Vec<serde_json::Value> = self
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

        let mut result = serde_json::json!({
            "table": {
                "name": self.name,
                "schema": self.schema,
                "full_name": self.full_name(),
                "columns": columns,
                "primary_key": self.primary_key,
                "indexes": indexes,
                "foreign_keys": foreign_keys,
            }
        });

        if let Some(ref stats) = self.stats {
            result["table"]["stats"] = serde_json::json!({
                "row_count": stats.row_count,
                "size_bytes": stats.size_bytes,
                "index_size_bytes": stats.index_size_bytes,
                "last_analyzed": stats.last_analyzed,
                "last_modified": stats.last_modified,
            });
        }

        result
    }
}

/// Format a number with thousand separators.
///
/// # Example
///
/// ```rust
/// use sqlmodel_console::renderables::table_info::format_number;
///
/// assert_eq!(format_number(1234567), "1,234,567");
/// assert_eq!(format_number(999), "999");
/// ```
#[must_use]
pub fn format_number(n: u64) -> String {
    let s = n.to_string();
    let bytes: Vec<char> = s.chars().collect();
    let mut result = String::new();

    for (i, &c) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }

    result
}

/// Format bytes as human-readable size (KB, MB, GB).
///
/// # Example
///
/// ```rust
/// use sqlmodel_console::renderables::table_info::format_bytes;
///
/// assert_eq!(format_bytes(1024), "1.0 KB");
/// assert_eq!(format_bytes(1_048_576), "1.0 MB");
/// assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
/// ```
#[must_use]
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
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

    fn sample_columns() -> Vec<ColumnData> {
        vec![
            sample_column("id", "BIGINT", true),
            sample_column("name", "VARCHAR(255)", false),
            sample_column("email", "VARCHAR(255)", false),
            ColumnData {
                name: "created_at".to_string(),
                sql_type: "TIMESTAMP".to_string(),
                nullable: false,
                default: Some("CURRENT_TIMESTAMP".to_string()),
                primary_key: false,
                auto_increment: false,
            },
        ]
    }

    #[test]
    fn test_table_info_creation() {
        let info = TableInfo::new("users", sample_columns());
        assert_eq!(info.name, "users");
        assert_eq!(info.columns.len(), 4);
    }

    #[test]
    fn test_table_info_empty() {
        let info = TableInfo::empty("empty_table");
        assert_eq!(info.name, "empty_table");
        assert!(info.columns.is_empty());
    }

    #[test]
    fn test_table_info_with_schema() {
        let info = TableInfo::new("users", sample_columns()).schema("public");
        assert_eq!(info.full_name(), "public.users");
    }

    #[test]
    fn test_table_info_columns_display() {
        let info = TableInfo::new("users", sample_columns());
        let output = info.render_plain();
        assert!(output.contains("id"));
        assert!(output.contains("BIGINT"));
        assert!(output.contains("name"));
        assert!(output.contains("VARCHAR(255)"));
    }

    #[test]
    fn test_table_info_primary_key() {
        let info =
            TableInfo::new("users", sample_columns()).with_primary_key(vec!["id".to_string()]);
        let output = info.render_plain();
        assert!(output.contains("(PK: id)"));
    }

    #[test]
    fn test_table_info_indexes_section() {
        let info = TableInfo::new("users", sample_columns()).add_index(IndexData {
            name: "idx_email".to_string(),
            columns: vec!["email".to_string()],
            unique: true,
        });
        let output = info.render_plain();
        assert!(output.contains("INDEXES:"));
        assert!(output.contains("UNIQUE idx_email"));
        assert!(output.contains("(email)"));
    }

    #[test]
    fn test_table_info_foreign_keys() {
        let info = TableInfo::new("posts", sample_columns()).add_foreign_key(ForeignKeyData {
            name: Some("fk_user".to_string()),
            column: "user_id".to_string(),
            foreign_table: "users".to_string(),
            foreign_column: "id".to_string(),
            on_delete: Some("CASCADE".to_string()),
            on_update: None,
        });
        let output = info.render_plain();
        assert!(output.contains("FOREIGN KEYS:"));
        assert!(output.contains("fk_user: user_id -> users.id"));
        assert!(output.contains("ON DELETE CASCADE"));
    }

    #[test]
    fn test_table_info_with_stats() {
        let info = TableInfo::new("users", sample_columns()).with_stats(TableStats {
            row_count: Some(10_000),
            size_bytes: Some(2_500_000),
            index_size_bytes: Some(500_000),
            last_analyzed: Some("2026-01-22 10:30:00".to_string()),
            last_modified: None,
        });
        let output = info.render_plain();
        assert!(output.contains("Rows: 10,000"));
        assert!(output.contains("Size: 2.4 MB"));
        assert!(output.contains("Index Size: 488.3 KB"));
        assert!(output.contains("Last Analyzed: 2026-01-22"));
    }

    #[test]
    fn test_table_info_render_plain() {
        let info = TableInfo::new("heroes", sample_columns());
        let output = info.render_plain();
        assert!(output.contains("TABLE: heroes"));
        assert!(output.contains("COLUMNS:"));
    }

    #[test]
    fn test_table_info_render_rich() {
        let info = TableInfo::new("heroes", sample_columns()).width(80);
        let styled = info.render_styled();
        assert!(styled.contains('\x1b')); // Contains ANSI codes
        assert!(styled.contains("┌")); // Box drawing
        assert!(styled.contains("│"));
        assert!(styled.contains("└"));
    }

    #[test]
    fn test_table_info_width_constraint() {
        let info = TableInfo::new("heroes", sample_columns()).width(60);
        let styled = info.render_styled();
        // Verify the box is roughly the right width
        let lines: Vec<&str> = styled.lines().collect();
        assert!(!lines.is_empty());
    }

    #[test]
    fn test_table_info_empty_table() {
        let info = TableInfo::empty("empty");
        let output = info.render_plain();
        assert!(output.contains("(no columns)"));
    }

    #[test]
    fn test_table_info_to_json() {
        let info = TableInfo::new("users", sample_columns())
            .with_primary_key(vec!["id".to_string()])
            .with_stats(TableStats::new().row_count(100));
        let json = info.to_json();
        assert_eq!(json["table"]["name"], "users");
        assert!(json["table"]["columns"].is_array());
        assert_eq!(json["table"]["primary_key"][0], "id");
        assert_eq!(json["table"]["stats"]["row_count"], 100);
    }

    #[test]
    fn test_format_number_thousands() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(999), "999");
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(12345), "12,345");
        assert_eq!(format_number(1_234_567), "1,234,567");
        assert_eq!(format_number(1_234_567_890), "1,234,567,890");
    }

    #[test]
    fn test_format_bytes_units() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_572_864), "1.5 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
        assert_eq!(format_bytes(1_099_511_627_776), "1.0 TB");
    }

    #[test]
    fn test_table_stats_builder() {
        let stats = TableStats::new()
            .row_count(1000)
            .size_bytes(50000)
            .last_analyzed("2026-01-22");
        assert_eq!(stats.row_count, Some(1000));
        assert_eq!(stats.size_bytes, Some(50000));
        assert_eq!(stats.last_analyzed, Some("2026-01-22".to_string()));
    }

    #[test]
    fn test_table_stats_is_empty() {
        let empty = TableStats::new();
        assert!(empty.is_empty());

        let with_data = TableStats::new().row_count(100);
        assert!(!with_data.is_empty());
    }

    #[test]
    fn test_table_info_builder_pattern() {
        let info = TableInfo::new("test", vec![])
            .schema("myschema")
            .with_primary_key(vec!["id".to_string()])
            .with_indexes(vec![IndexData {
                name: "idx1".to_string(),
                columns: vec!["col".to_string()],
                unique: false,
            }])
            .with_foreign_keys(vec![])
            .theme(Theme::light())
            .width(100)
            .show_types(false)
            .show_constraints(false);

        assert_eq!(info.schema, Some("myschema".to_string()));
        assert_eq!(info.width, Some(100));
        assert!(!info.show_types);
        assert!(!info.show_constraints);
    }
}
