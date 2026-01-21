//! Query result table renderable for beautiful result display.
//!
//! Provides a table specifically designed for displaying query results with
//! rich formatting in styled mode and multiple plain text formats for agent
//! compatibility.
//!
//! # Example
//!
//! ```rust
//! use sqlmodel_console::renderables::{QueryResultTable, PlainFormat};
//!
//! let table = QueryResultTable::new()
//!     .title("Query Results")
//!     .columns(vec!["id", "name", "email"])
//!     .row(vec!["1", "Alice", "alice@example.com"])
//!     .row(vec!["2", "Bob", "bob@example.com"])
//!     .timing_ms(12.34)
//!     .max_width(80);
//!
//! // Plain mode output (pipe-delimited)
//! println!("{}", table.render_plain());
//!
//! // Or use a different format
//! println!("{}", table.render_plain_format(PlainFormat::Csv));
//! ```

use crate::theme::Theme;
use std::time::Duration;

/// Plain text output format for query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PlainFormat {
    /// Pipe-delimited format: `id|name|email` (default)
    #[default]
    Pipe,
    /// CSV format with proper quoting
    Csv,
    /// JSON Lines format (one JSON object per row)
    JsonLines,
    /// JSON Array format (single array of objects)
    JsonArray,
}

/// SQL value type for cell coloring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ValueType {
    /// NULL value (gray, italic)
    Null,
    /// Boolean value (yellow)
    Boolean,
    /// Integer value (cyan)
    Integer,
    /// Float value (cyan)
    Float,
    /// String value (green)
    #[default]
    String,
    /// Date value (magenta)
    Date,
    /// Time value (magenta)
    Time,
    /// Timestamp value (magenta)
    Timestamp,
    /// Binary/blob value (orange)
    Binary,
    /// JSON value (purple)
    Json,
    /// UUID value (orange)
    Uuid,
}

impl ValueType {
    /// Infer value type from a string value.
    #[must_use]
    pub fn infer(value: &str) -> Self {
        let trimmed = value.trim();

        // Check for NULL
        if trimmed.eq_ignore_ascii_case("null") || trimmed.eq_ignore_ascii_case("<null>") {
            return Self::Null;
        }

        // Check for boolean
        if trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("false") {
            return Self::Boolean;
        }

        // Check for binary blob marker
        if trimmed.starts_with("[BLOB:") || trimmed.starts_with("<binary:") {
            return Self::Binary;
        }

        // Check for JSON (starts with { or [)
        if (trimmed.starts_with('{') && trimmed.ends_with('}'))
            || (trimmed.starts_with('[') && trimmed.ends_with(']'))
        {
            return Self::Json;
        }

        // Check for UUID pattern (8-4-4-4-12)
        if trimmed.len() == 36 && trimmed.chars().filter(|c| *c == '-').count() == 4 {
            let parts: Vec<&str> = trimmed.split('-').collect();
            if parts.len() == 5
                && parts[0].len() == 8
                && parts[1].len() == 4
                && parts[2].len() == 4
                && parts[3].len() == 4
                && parts[4].len() == 12
                && parts.iter().all(|p| p.chars().all(|c| c.is_ascii_hexdigit()))
            {
                return Self::Uuid;
            }
        }

        // Check for date pattern (YYYY-MM-DD)
        if trimmed.len() == 10 && trimmed.chars().filter(|c| *c == '-').count() == 2 {
            if let Some(year) = trimmed.get(0..4) {
                if year.parse::<u32>().is_ok() {
                    return Self::Date;
                }
            }
        }

        // Check for timestamp pattern (contains 'T' or date-like with time)
        if trimmed.contains('T') && trimmed.len() >= 19 {
            return Self::Timestamp;
        }
        if trimmed.len() >= 19 && trimmed.contains(' ') && trimmed.contains(':') {
            return Self::Timestamp;
        }

        // Check for time pattern (HH:MM:SS)
        if trimmed.len() >= 8 && trimmed.contains(':') && !trimmed.contains('-') {
            let parts: Vec<&str> = trimmed.split(':').collect();
            if parts.len() >= 2 && parts.iter().all(|p| p.parse::<u32>().is_ok() || p.contains('.')) {
                return Self::Time;
            }
        }

        // Check for integer
        if trimmed.parse::<i64>().is_ok() {
            return Self::Integer;
        }

        // Check for float
        if trimmed.parse::<f64>().is_ok() {
            return Self::Float;
        }

        // Default to string
        Self::String
    }

    /// Get the ANSI color code for this value type from theme.
    #[must_use]
    pub fn color_code(&self, theme: &Theme) -> String {
        match self {
            Self::Null => theme.null_value.color_code(),
            Self::Boolean => theme.bool_value.color_code(),
            Self::Integer | Self::Float => theme.number_value.color_code(),
            Self::String => theme.string_value.color_code(),
            Self::Date | Self::Time | Self::Timestamp => theme.date_value.color_code(),
            Self::Binary => theme.binary_value.color_code(),
            Self::Json => theme.json_value.color_code(),
            Self::Uuid => theme.uuid_value.color_code(),
        }
    }
}

/// A cell in the query result table.
#[derive(Debug, Clone)]
pub struct Cell {
    /// The display value.
    pub value: String,
    /// The inferred or explicit value type.
    pub value_type: ValueType,
}

impl Cell {
    /// Create a cell with automatic type inference.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        let value = value.into();
        let value_type = ValueType::infer(&value);
        Self { value, value_type }
    }

    /// Create a cell with explicit type.
    #[must_use]
    pub fn with_type(value: impl Into<String>, value_type: ValueType) -> Self {
        Self {
            value: value.into(),
            value_type,
        }
    }

    /// Create a NULL cell.
    #[must_use]
    pub fn null() -> Self {
        Self {
            value: "NULL".to_string(),
            value_type: ValueType::Null,
        }
    }
}

/// A table for displaying query results.
///
/// Provides rich formatting for query result sets including type-based
/// coloring, auto-sized columns, and multiple output formats.
#[derive(Debug, Clone)]
pub struct QueryResultTable {
    /// Optional title for the table
    title: Option<String>,
    /// Column names
    columns: Vec<String>,
    /// Row data (each row is a vector of cells)
    rows: Vec<Vec<Cell>>,
    /// Query execution time in milliseconds
    timing_ms: Option<f64>,
    /// Maximum table width (for wrapping/truncation)
    max_width: Option<usize>,
    /// Maximum rows to display (rest shown as "... and N more")
    max_rows: Option<usize>,
    /// Show row numbers
    show_row_numbers: bool,
    /// Theme for styled output
    theme: Option<Theme>,
    /// Plain format for non-styled output
    plain_format: PlainFormat,
}

impl QueryResultTable {
    /// Create a new empty query result table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            title: None,
            columns: Vec::new(),
            rows: Vec::new(),
            timing_ms: None,
            max_width: None,
            max_rows: None,
            show_row_numbers: false,
            theme: None,
            plain_format: PlainFormat::Pipe,
        }
    }

    /// Set the table title.
    #[must_use]
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Set the column names.
    #[must_use]
    pub fn columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Add a row of string values (types inferred).
    #[must_use]
    pub fn row(mut self, values: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let cells: Vec<Cell> = values.into_iter().map(|v| Cell::new(v)).collect();
        self.rows.push(cells);
        self
    }

    /// Add a row of cells (with explicit types).
    #[must_use]
    pub fn row_cells(mut self, cells: Vec<Cell>) -> Self {
        self.rows.push(cells);
        self
    }

    /// Add multiple rows at once.
    #[must_use]
    pub fn rows(mut self, rows: impl IntoIterator<Item = impl IntoIterator<Item = impl Into<String>>>) -> Self {
        for row in rows {
            let cells: Vec<Cell> = row.into_iter().map(|v| Cell::new(v)).collect();
            self.rows.push(cells);
        }
        self
    }

    /// Set the query timing in milliseconds.
    #[must_use]
    pub fn timing_ms(mut self, ms: f64) -> Self {
        self.timing_ms = Some(ms);
        self
    }

    /// Set the query timing from a Duration.
    #[must_use]
    pub fn timing(mut self, duration: Duration) -> Self {
        self.timing_ms = Some(duration.as_secs_f64() * 1000.0);
        self
    }

    /// Set maximum table width.
    #[must_use]
    pub fn max_width(mut self, width: usize) -> Self {
        self.max_width = Some(width);
        self
    }

    /// Set maximum rows to display.
    #[must_use]
    pub fn max_rows(mut self, max: usize) -> Self {
        self.max_rows = Some(max);
        self
    }

    /// Enable row numbers.
    #[must_use]
    pub fn with_row_numbers(mut self) -> Self {
        self.show_row_numbers = true;
        self
    }

    /// Set the theme for styled output.
    #[must_use]
    pub fn theme(mut self, theme: Theme) -> Self {
        self.theme = Some(theme);
        self
    }

    /// Set the plain format for non-styled output.
    #[must_use]
    pub fn plain_format(mut self, format: PlainFormat) -> Self {
        self.plain_format = format;
        self
    }

    /// Get the number of rows.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get the number of columns.
    #[must_use]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Calculate column widths based on content.
    fn calculate_column_widths(&self) -> Vec<usize> {
        let mut widths: Vec<usize> = self
            .columns
            .iter()
            .map(|c| c.chars().count())
            .collect();

        // Consider row number column if enabled
        if self.show_row_numbers {
            let row_num_width = self.rows.len().to_string().len().max(1);
            widths.insert(0, row_num_width);
        }

        // Find max width for each column from data
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                let col_idx = if self.show_row_numbers { i + 1 } else { i };
                if col_idx < widths.len() {
                    widths[col_idx] = widths[col_idx].max(cell.value.chars().count());
                }
            }
        }

        // Apply max width constraint if set
        if let Some(max_width) = self.max_width {
            let total_padding = (widths.len() * 3) + 1; // | + space before/after each col
            let available = max_width.saturating_sub(total_padding);
            let per_col_max = available / widths.len().max(1);

            for w in &mut widths {
                *w = (*w).min(per_col_max.max(3)); // At least 3 chars
            }
        }

        widths
    }

    /// Truncate a value to fit within width, adding "..." if needed.
    fn truncate_value(value: &str, width: usize) -> String {
        if value.chars().count() <= width {
            value.to_string()
        } else if width <= 3 {
            value.chars().take(width).collect()
        } else {
            let truncated: String = value.chars().take(width - 3).collect();
            format!("{truncated}...")
        }
    }

    /// Render as plain text using the configured format.
    #[must_use]
    pub fn render_plain(&self) -> String {
        self.render_plain_format(self.plain_format)
    }

    /// Render as plain text using a specific format.
    #[must_use]
    pub fn render_plain_format(&self, format: PlainFormat) -> String {
        match format {
            PlainFormat::Pipe => self.render_pipe(),
            PlainFormat::Csv => self.render_csv(),
            PlainFormat::JsonLines => self.render_json_lines(),
            PlainFormat::JsonArray => self.render_json_array(),
        }
    }

    /// Render as pipe-delimited format.
    fn render_pipe(&self) -> String {
        let mut lines = Vec::new();

        // Optional timing header
        if let Some(ms) = self.timing_ms {
            lines.push(format!("# {} rows in {:.2}ms", self.rows.len(), ms));
        }

        // Header row
        let mut header = self.columns.join("|");
        if self.show_row_numbers {
            header = format!("#|{header}");
        }
        lines.push(header);

        // Determine display rows
        let display_rows = self.max_rows.unwrap_or(self.rows.len());
        let truncated = self.rows.len() > display_rows;

        // Data rows
        for (idx, row) in self.rows.iter().take(display_rows).enumerate() {
            let values: Vec<&str> = row.iter().map(|c| c.value.as_str()).collect();
            let mut line = values.join("|");
            if self.show_row_numbers {
                line = format!("{}|{line}", idx + 1);
            }
            lines.push(line);
        }

        // Truncation indicator
        if truncated {
            lines.push(format!("... and {} more rows", self.rows.len() - display_rows));
        }

        lines.join("\n")
    }

    /// Render as CSV format.
    fn render_csv(&self) -> String {
        let mut lines = Vec::new();

        // Header row
        let header: Vec<String> = self.columns.iter().map(|c| Self::csv_escape(c)).collect();
        lines.push(header.join(","));

        // Determine display rows
        let display_rows = self.max_rows.unwrap_or(self.rows.len());

        // Data rows
        for row in self.rows.iter().take(display_rows) {
            let values: Vec<String> = row.iter().map(|c| Self::csv_escape(&c.value)).collect();
            lines.push(values.join(","));
        }

        lines.join("\n")
    }

    /// Escape a value for CSV output.
    fn csv_escape(value: &str) -> String {
        if value.contains(',') || value.contains('"') || value.contains('\n') {
            let escaped = value.replace('"', "\"\"");
            format!("\"{escaped}\"")
        } else {
            value.to_string()
        }
    }

    /// Render as JSON Lines format.
    fn render_json_lines(&self) -> String {
        let display_rows = self.max_rows.unwrap_or(self.rows.len());

        self.rows
            .iter()
            .take(display_rows)
            .map(|row| {
                let obj: serde_json::Map<String, serde_json::Value> = self
                    .columns
                    .iter()
                    .zip(row.iter())
                    .map(|(col, cell)| {
                        let value = match cell.value_type {
                            ValueType::Null => serde_json::Value::Null,
                            ValueType::Boolean => {
                                serde_json::Value::Bool(cell.value.eq_ignore_ascii_case("true"))
                            }
                            ValueType::Integer => {
                                if let Ok(n) = cell.value.parse::<i64>() {
                                    serde_json::Value::Number(n.into())
                                } else {
                                    serde_json::Value::String(cell.value.clone())
                                }
                            }
                            ValueType::Float => {
                                if let Ok(n) = cell.value.parse::<f64>() {
                                    serde_json::Number::from_f64(n)
                                        .map_or_else(|| serde_json::Value::String(cell.value.clone()), serde_json::Value::Number)
                                } else {
                                    serde_json::Value::String(cell.value.clone())
                                }
                            }
                            _ => serde_json::Value::String(cell.value.clone()),
                        };
                        (col.clone(), value)
                    })
                    .collect();
                serde_json::to_string(&obj).unwrap_or_else(|_| "{}".to_string())
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Render as JSON Array format.
    fn render_json_array(&self) -> String {
        let display_rows = self.max_rows.unwrap_or(self.rows.len());

        let array: Vec<serde_json::Map<String, serde_json::Value>> = self
            .rows
            .iter()
            .take(display_rows)
            .map(|row| {
                self.columns
                    .iter()
                    .zip(row.iter())
                    .map(|(col, cell)| {
                        let value = match cell.value_type {
                            ValueType::Null => serde_json::Value::Null,
                            ValueType::Boolean => {
                                serde_json::Value::Bool(cell.value.eq_ignore_ascii_case("true"))
                            }
                            ValueType::Integer => {
                                if let Ok(n) = cell.value.parse::<i64>() {
                                    serde_json::Value::Number(n.into())
                                } else {
                                    serde_json::Value::String(cell.value.clone())
                                }
                            }
                            ValueType::Float => {
                                if let Ok(n) = cell.value.parse::<f64>() {
                                    serde_json::Number::from_f64(n)
                                        .map_or_else(|| serde_json::Value::String(cell.value.clone()), serde_json::Value::Number)
                                } else {
                                    serde_json::Value::String(cell.value.clone())
                                }
                            }
                            _ => serde_json::Value::String(cell.value.clone()),
                        };
                        (col.clone(), value)
                    })
                    .collect()
            })
            .collect();

        serde_json::to_string_pretty(&array).unwrap_or_else(|_| "[]".to_string())
    }

    /// Render as styled text with ANSI colors and box drawing.
    #[must_use]
    pub fn render_styled(&self) -> String {
        let theme = self.theme.clone().unwrap_or_default();
        let widths = self.calculate_column_widths();

        let border_color = theme.border.color_code();
        let header_color = theme.header.color_code();
        let dim = theme.dim.color_code();
        let reset = "\x1b[0m";

        let mut lines = Vec::new();

        // Calculate total width
        let total_width: usize = widths.iter().sum::<usize>() + (widths.len() * 3) + 1;

        // Title bar
        if let Some(ref title) = self.title {
            let timing_str = self.timing_ms.map_or(String::new(), |ms| {
                format!(" • {} rows in {:.2}ms", self.rows.len(), ms)
            });
            let full_title = format!(" {title}{timing_str} ");
            let title_len = full_title.chars().count();
            let left_pad = (total_width.saturating_sub(2).saturating_sub(title_len)) / 2;
            let right_pad = total_width.saturating_sub(2).saturating_sub(title_len).saturating_sub(left_pad);

            lines.push(format!(
                "{border_color}╭{}{}{}╮{reset}",
                "─".repeat(left_pad),
                full_title,
                "─".repeat(right_pad)
            ));
        } else if let Some(ms) = self.timing_ms {
            let timing_str = format!(" {} rows in {:.2}ms ", self.rows.len(), ms);
            let timing_len = timing_str.chars().count();
            let left_pad = (total_width.saturating_sub(2).saturating_sub(timing_len)) / 2;
            let right_pad = total_width.saturating_sub(2).saturating_sub(timing_len).saturating_sub(left_pad);

            lines.push(format!(
                "{border_color}╭{}{}{}╮{reset}",
                "─".repeat(left_pad),
                timing_str,
                "─".repeat(right_pad)
            ));
        } else {
            lines.push(format!(
                "{border_color}╭{}╮{reset}",
                "─".repeat(total_width - 2)
            ));
        }

        // Header row
        let mut header_cells = Vec::new();
        if self.show_row_numbers {
            header_cells.push(format!("{dim}{:>width$}{reset}", "#", width = widths[0]));
        }
        for (i, col) in self.columns.iter().enumerate() {
            let col_idx = if self.show_row_numbers { i + 1 } else { i };
            let width = widths.get(col_idx).copied().unwrap_or(10);
            let truncated = Self::truncate_value(col, width);
            header_cells.push(format!("{header_color}{:width$}{reset}", truncated, width = width));
        }
        lines.push(format!(
            "{border_color}│{reset} {} {border_color}│{reset}",
            header_cells.join(&format!(" {border_color}│{reset} "))
        ));

        // Header separator
        let separators: Vec<String> = widths.iter().map(|w| "─".repeat(*w)).collect();
        lines.push(format!(
            "{border_color}├─{}─┤{reset}",
            separators.join("─┼─")
        ));

        // Determine display rows
        let display_rows = self.max_rows.unwrap_or(self.rows.len());
        let truncated = self.rows.len() > display_rows;

        // Data rows
        for (idx, row) in self.rows.iter().take(display_rows).enumerate() {
            let mut cells = Vec::new();

            if self.show_row_numbers {
                let row_num_width = widths[0];
                cells.push(format!("{dim}{:>width$}{reset}", idx + 1, width = row_num_width));
            }

            for (i, cell) in row.iter().enumerate() {
                let col_idx = if self.show_row_numbers { i + 1 } else { i };
                let width = widths.get(col_idx).copied().unwrap_or(10);
                let truncated_val = Self::truncate_value(&cell.value, width);
                let color = cell.value_type.color_code(&theme);

                // Right-align numbers, left-align everything else
                let formatted = match cell.value_type {
                    ValueType::Integer | ValueType::Float => {
                        format!("{color}{:>width$}{reset}", truncated_val, width = width)
                    }
                    ValueType::Null => {
                        format!("{color}\x1b[3m{:^width$}\x1b[23m{reset}", truncated_val, width = width)
                    }
                    _ => {
                        format!("{color}{:width$}{reset}", truncated_val, width = width)
                    }
                };
                cells.push(formatted);
            }

            lines.push(format!(
                "{border_color}│{reset} {} {border_color}│{reset}",
                cells.join(&format!(" {border_color}│{reset} "))
            ));
        }

        // Truncation indicator
        if truncated {
            let more_text = format!("... and {} more rows", self.rows.len() - display_rows);
            let padding = total_width.saturating_sub(4).saturating_sub(more_text.len());
            lines.push(format!(
                "{border_color}│{reset} {dim}{more_text}{:padding$}{reset} {border_color}│{reset}",
                "",
                padding = padding
            ));
        }

        // Bottom border
        lines.push(format!(
            "{border_color}╰{}╯{reset}",
            "─".repeat(total_width - 2)
        ));

        lines.join("\n")
    }

    /// Render as JSON-serializable structure.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        let rows: Vec<serde_json::Value> = self
            .rows
            .iter()
            .map(|row| {
                let obj: serde_json::Map<String, serde_json::Value> = self
                    .columns
                    .iter()
                    .zip(row.iter())
                    .map(|(col, cell)| {
                        let value = match cell.value_type {
                            ValueType::Null => serde_json::Value::Null,
                            ValueType::Boolean => {
                                serde_json::Value::Bool(cell.value.eq_ignore_ascii_case("true"))
                            }
                            ValueType::Integer => {
                                if let Ok(n) = cell.value.parse::<i64>() {
                                    serde_json::Value::Number(n.into())
                                } else {
                                    serde_json::Value::String(cell.value.clone())
                                }
                            }
                            ValueType::Float => {
                                if let Ok(n) = cell.value.parse::<f64>() {
                                    serde_json::Number::from_f64(n)
                                        .map_or_else(|| serde_json::Value::String(cell.value.clone()), serde_json::Value::Number)
                                } else {
                                    serde_json::Value::String(cell.value.clone())
                                }
                            }
                            _ => serde_json::Value::String(cell.value.clone()),
                        };
                        (col.clone(), value)
                    })
                    .collect();
                serde_json::Value::Object(obj)
            })
            .collect();

        serde_json::json!({
            "columns": self.columns,
            "rows": rows,
            "row_count": self.rows.len(),
            "timing_ms": self.timing_ms,
        })
    }
}

impl Default for QueryResultTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_result_table_new() {
        let table = QueryResultTable::new();
        assert_eq!(table.row_count(), 0);
        assert_eq!(table.column_count(), 0);
    }

    #[test]
    fn test_query_result_table_basic() {
        let table = QueryResultTable::new()
            .columns(vec!["id", "name"])
            .row(vec!["1", "Alice"])
            .row(vec!["2", "Bob"]);

        assert_eq!(table.row_count(), 2);
        assert_eq!(table.column_count(), 2);
    }

    #[test]
    fn test_value_type_inference_null() {
        assert_eq!(ValueType::infer("null"), ValueType::Null);
        assert_eq!(ValueType::infer("NULL"), ValueType::Null);
        assert_eq!(ValueType::infer("<null>"), ValueType::Null);
    }

    #[test]
    fn test_value_type_inference_boolean() {
        assert_eq!(ValueType::infer("true"), ValueType::Boolean);
        assert_eq!(ValueType::infer("false"), ValueType::Boolean);
        assert_eq!(ValueType::infer("TRUE"), ValueType::Boolean);
    }

    #[test]
    fn test_value_type_inference_integer() {
        assert_eq!(ValueType::infer("42"), ValueType::Integer);
        assert_eq!(ValueType::infer("-123"), ValueType::Integer);
        assert_eq!(ValueType::infer("0"), ValueType::Integer);
    }

    #[test]
    fn test_value_type_inference_float() {
        assert_eq!(ValueType::infer("3.14"), ValueType::Float);
        assert_eq!(ValueType::infer("-2.5"), ValueType::Float);
        assert_eq!(ValueType::infer("1.0e10"), ValueType::Float);
    }

    #[test]
    fn test_value_type_inference_date() {
        assert_eq!(ValueType::infer("2024-01-15"), ValueType::Date);
    }

    #[test]
    fn test_value_type_inference_timestamp() {
        assert_eq!(ValueType::infer("2024-01-15T10:30:00"), ValueType::Timestamp);
        assert_eq!(ValueType::infer("2024-01-15 10:30:00"), ValueType::Timestamp);
    }

    #[test]
    fn test_value_type_inference_time() {
        assert_eq!(ValueType::infer("10:30:00"), ValueType::Time);
        assert_eq!(ValueType::infer("10:30:00.123"), ValueType::Time);
    }

    #[test]
    fn test_value_type_inference_uuid() {
        assert_eq!(
            ValueType::infer("550e8400-e29b-41d4-a716-446655440000"),
            ValueType::Uuid
        );
    }

    #[test]
    fn test_value_type_inference_json() {
        assert_eq!(ValueType::infer("{\"key\": \"value\"}"), ValueType::Json);
        assert_eq!(ValueType::infer("[1, 2, 3]"), ValueType::Json);
    }

    #[test]
    fn test_value_type_inference_binary() {
        assert_eq!(ValueType::infer("[BLOB: 1024 bytes]"), ValueType::Binary);
    }

    #[test]
    fn test_value_type_inference_string() {
        assert_eq!(ValueType::infer("hello"), ValueType::String);
        assert_eq!(ValueType::infer("alice@example.com"), ValueType::String);
    }

    #[test]
    fn test_render_pipe_basic() {
        let table = QueryResultTable::new()
            .columns(vec!["id", "name"])
            .row(vec!["1", "Alice"])
            .row(vec!["2", "Bob"]);

        let output = table.render_plain();
        assert!(output.contains("id|name"));
        assert!(output.contains("1|Alice"));
        assert!(output.contains("2|Bob"));
    }

    #[test]
    fn test_render_pipe_with_timing() {
        let table = QueryResultTable::new()
            .columns(vec!["id"])
            .row(vec!["1"])
            .timing_ms(12.34);

        let output = table.render_plain();
        assert!(output.contains("# 1 rows in 12.34ms"));
    }

    #[test]
    fn test_render_pipe_with_row_numbers() {
        let table = QueryResultTable::new()
            .columns(vec!["name"])
            .row(vec!["Alice"])
            .row(vec!["Bob"])
            .with_row_numbers();

        let output = table.render_plain();
        assert!(output.contains("#|name"));
        assert!(output.contains("1|Alice"));
        assert!(output.contains("2|Bob"));
    }

    #[test]
    fn test_render_csv_basic() {
        let table = QueryResultTable::new()
            .columns(vec!["id", "name"])
            .row(vec!["1", "Alice"]);

        let output = table.render_plain_format(PlainFormat::Csv);
        assert!(output.contains("id,name"));
        assert!(output.contains("1,Alice"));
    }

    #[test]
    fn test_render_csv_escaping() {
        let table = QueryResultTable::new()
            .columns(vec!["text"])
            .row(vec!["hello, world"]);

        let output = table.render_plain_format(PlainFormat::Csv);
        assert!(output.contains("\"hello, world\""));
    }

    #[test]
    fn test_render_json_lines() {
        let table = QueryResultTable::new()
            .columns(vec!["id", "name"])
            .row(vec!["1", "Alice"]);

        let output = table.render_plain_format(PlainFormat::JsonLines);
        assert!(output.contains("\"id\":1"));
        assert!(output.contains("\"name\":\"Alice\""));
    }

    #[test]
    fn test_render_json_array() {
        let table = QueryResultTable::new()
            .columns(vec!["id"])
            .row(vec!["1"])
            .row(vec!["2"]);

        let output = table.render_plain_format(PlainFormat::JsonArray);
        assert!(output.starts_with('['));
        assert!(output.ends_with(']'));
    }

    #[test]
    fn test_max_rows_truncation() {
        let table = QueryResultTable::new()
            .columns(vec!["id"])
            .row(vec!["1"])
            .row(vec!["2"])
            .row(vec!["3"])
            .row(vec!["4"])
            .row(vec!["5"])
            .max_rows(3);

        let output = table.render_plain();
        assert!(output.contains("... and 2 more rows"));
    }

    #[test]
    fn test_cell_new() {
        let cell = Cell::new("42");
        assert_eq!(cell.value, "42");
        assert_eq!(cell.value_type, ValueType::Integer);
    }

    #[test]
    fn test_cell_with_type() {
        let cell = Cell::with_type("hello", ValueType::String);
        assert_eq!(cell.value, "hello");
        assert_eq!(cell.value_type, ValueType::String);
    }

    #[test]
    fn test_cell_null() {
        let cell = Cell::null();
        assert_eq!(cell.value, "NULL");
        assert_eq!(cell.value_type, ValueType::Null);
    }

    #[test]
    fn test_truncate_value_short() {
        assert_eq!(QueryResultTable::truncate_value("abc", 10), "abc");
    }

    #[test]
    fn test_truncate_value_long() {
        assert_eq!(QueryResultTable::truncate_value("hello world", 8), "hello...");
    }

    #[test]
    fn test_truncate_value_exact() {
        assert_eq!(QueryResultTable::truncate_value("hello", 5), "hello");
    }

    #[test]
    fn test_to_json() {
        let table = QueryResultTable::new()
            .columns(vec!["id", "name"])
            .row(vec!["1", "Alice"])
            .timing_ms(10.0);

        let json = table.to_json();
        assert_eq!(json["row_count"], 1);
        assert_eq!(json["timing_ms"], 10.0);
        assert!(json["columns"].is_array());
        assert!(json["rows"].is_array());
    }

    #[test]
    fn test_render_styled_contains_box() {
        let table = QueryResultTable::new()
            .columns(vec!["id"])
            .row(vec!["1"]);

        let styled = table.render_styled();
        assert!(styled.contains("╭"));
        assert!(styled.contains("╯"));
        assert!(styled.contains("│"));
    }

    #[test]
    fn test_render_styled_with_title() {
        let table = QueryResultTable::new()
            .title("Test Results")
            .columns(vec!["id"])
            .row(vec!["1"]);

        let styled = table.render_styled();
        assert!(styled.contains("Test Results"));
    }

    #[test]
    fn test_builder_chain() {
        let table = QueryResultTable::new()
            .title("My Table")
            .columns(vec!["a", "b"])
            .row(vec!["1", "2"])
            .timing_ms(5.0)
            .max_width(80)
            .max_rows(100)
            .with_row_numbers()
            .theme(Theme::dark())
            .plain_format(PlainFormat::Csv);

        assert_eq!(table.row_count(), 1);
        assert_eq!(table.column_count(), 2);
    }

    #[test]
    fn test_null_values_in_json() {
        let table = QueryResultTable::new()
            .columns(vec!["value"])
            .row(vec!["null"]);

        let json = table.to_json();
        let rows = json["rows"].as_array().unwrap();
        assert!(rows[0]["value"].is_null());
    }

    #[test]
    fn test_boolean_values_in_json() {
        let table = QueryResultTable::new()
            .columns(vec!["flag"])
            .row(vec!["true"]);

        let json = table.to_json();
        let rows = json["rows"].as_array().unwrap();
        assert_eq!(rows[0]["flag"], true);
    }

    #[test]
    fn test_integer_values_in_json() {
        let table = QueryResultTable::new()
            .columns(vec!["count"])
            .row(vec!["42"]);

        let json = table.to_json();
        let rows = json["rows"].as_array().unwrap();
        assert_eq!(rows[0]["count"], 42);
    }
}
