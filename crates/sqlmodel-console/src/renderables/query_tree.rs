//! Query tree visualization for query structure display.
//!
//! Displays query structure as a tree view for understanding complex queries.
//!
//! # Example
//!
//! ```rust
//! use sqlmodel_console::renderables::QueryTreeView;
//!
//! let tree = QueryTreeView::new("SELECT from heroes")
//!     .add_child("Columns", vec!["id", "name", "secret_name"])
//!     .add_node("WHERE", "age > 18")
//!     .add_node("ORDER BY", "name ASC")
//!     .add_node("LIMIT", "10");
//!
//! println!("{}", tree.render_plain());
//! println!("{}", tree.render_styled());
//! ```

use crate::theme::Theme;

/// A node in the query tree.
#[derive(Debug, Clone)]
pub struct TreeNode {
    /// The label for this node (e.g., "WHERE", "ORDER BY")
    pub label: String,
    /// The value or description
    pub value: Option<String>,
    /// Child nodes
    pub children: Vec<TreeNode>,
}

impl TreeNode {
    /// Create a new tree node with a label.
    #[must_use]
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            value: None,
            children: Vec::new(),
        }
    }

    /// Create a new tree node with label and value.
    #[must_use]
    pub fn with_value(label: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            value: Some(value.into()),
            children: Vec::new(),
        }
    }

    /// Add a child node.
    #[must_use]
    pub fn add_child(mut self, child: TreeNode) -> Self {
        self.children.push(child);
        self
    }

    /// Add multiple children from strings.
    #[must_use]
    pub fn add_items(mut self, items: impl IntoIterator<Item = impl Into<String>>) -> Self {
        for item in items {
            self.children.push(TreeNode::new(item));
        }
        self
    }
}

/// Query tree view for visualizing query structure.
///
/// Displays SQL query structure as an ASCII/Unicode tree.
#[derive(Debug, Clone)]
pub struct QueryTreeView {
    /// Root node (query type, e.g., "SELECT from heroes")
    root: TreeNode,
    /// Theme for styled output
    theme: Option<Theme>,
    /// Use Unicode box drawing characters
    use_unicode: bool,
}

impl QueryTreeView {
    /// Create a new query tree with a root label.
    #[must_use]
    pub fn new(root_label: impl Into<String>) -> Self {
        Self {
            root: TreeNode::new(root_label),
            theme: None,
            use_unicode: true,
        }
    }

    /// Add a simple node with label and optional value.
    #[must_use]
    pub fn add_node(mut self, label: impl Into<String>, value: impl Into<String>) -> Self {
        self.root.children.push(TreeNode::with_value(label, value));
        self
    }

    /// Add a node with children (for lists like columns).
    #[must_use]
    pub fn add_child(
        mut self,
        label: impl Into<String>,
        items: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let mut node = TreeNode::new(label);
        for item in items {
            node.children.push(TreeNode::new(item));
        }
        self.root.children.push(node);
        self
    }

    /// Add a pre-built tree node.
    #[must_use]
    pub fn add_tree_node(mut self, node: TreeNode) -> Self {
        self.root.children.push(node);
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

    /// Get tree drawing characters.
    fn chars(&self) -> (&'static str, &'static str, &'static str, &'static str) {
        if self.use_unicode {
            ("├── ", "└── ", "│   ", "    ")
        } else {
            ("+-- ", "\\-- ", "|   ", "    ")
        }
    }

    /// Render the tree as plain text.
    #[must_use]
    pub fn render_plain(&self) -> String {
        let mut lines = Vec::new();
        self.render_node_plain(&self.root, "", true, 0, &mut lines);
        lines.join("\n")
    }

    /// Recursively render a node in plain text.
    fn render_node_plain(
        &self,
        node: &TreeNode,
        prefix: &str,
        is_last: bool,
        depth: usize,
        lines: &mut Vec<String>,
    ) {
        let (branch, last_branch, vertical, space) = self.chars();

        // Root node handling (depth 0)
        if depth == 0 {
            let root_line = if let Some(ref value) = node.value {
                format!("{}: {}", node.label, value)
            } else {
                node.label.clone()
            };
            lines.push(root_line);
        } else {
            let connector = if is_last { last_branch } else { branch };
            let line = if let Some(ref value) = node.value {
                format!("{}{}{}: {}", prefix, connector, node.label, value)
            } else {
                format!("{}{}{}", prefix, connector, node.label)
            };
            lines.push(line);
        }

        // Child nodes
        let child_prefix = if depth == 0 {
            String::new()
        } else if is_last {
            format!("{}{}", prefix, space)
        } else {
            format!("{}{}", prefix, vertical)
        };

        let child_count = node.children.len();
        for (i, child) in node.children.iter().enumerate() {
            let is_last_child = i == child_count - 1;
            self.render_node_plain(child, &child_prefix, is_last_child, depth + 1, lines);
        }
    }

    /// Render the tree as styled text with ANSI colors.
    #[must_use]
    pub fn render_styled(&self) -> String {
        let theme = self.theme.clone().unwrap_or_default();
        let mut lines = Vec::new();
        self.render_node_styled(&self.root, "", true, 0, &mut lines, &theme);
        lines.join("\n")
    }

    /// Recursively render a node with styling.
    fn render_node_styled(
        &self,
        node: &TreeNode,
        prefix: &str,
        is_last: bool,
        depth: usize,
        lines: &mut Vec<String>,
        theme: &Theme,
    ) {
        let (branch, last_branch, vertical, space) = self.chars();
        let reset = "\x1b[0m";
        let dim = theme.dim.color_code();
        let keyword_color = theme.sql_keyword.color_code();
        let value_color = theme.string_value.color_code();

        // Root node handling (depth 0)
        if depth == 0 {
            let root_line = if let Some(ref value) = node.value {
                format!(
                    "{keyword_color}{}{reset}: {value_color}{}{reset}",
                    node.label, value
                )
            } else {
                format!("{keyword_color}{}{reset}", node.label)
            };
            lines.push(root_line);
        } else {
            let connector = if is_last { last_branch } else { branch };
            let line = if let Some(ref value) = node.value {
                format!(
                    "{dim}{prefix}{connector}{reset}{keyword_color}{}{reset}: {value_color}{}{reset}",
                    node.label, value
                )
            } else {
                format!("{dim}{prefix}{connector}{reset}{}", node.label)
            };
            lines.push(line);
        }

        // Child nodes
        let child_prefix = if depth == 0 {
            String::new()
        } else if is_last {
            format!("{}{}", prefix, space)
        } else {
            format!("{}{}", prefix, vertical)
        };

        let child_count = node.children.len();
        for (i, child) in node.children.iter().enumerate() {
            let is_last_child = i == child_count - 1;
            self.render_node_styled(child, &child_prefix, is_last_child, depth + 1, lines, theme);
        }
    }

    /// Render as JSON-serializable structure.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        Self::node_to_json(&self.root)
    }

    /// Convert a node to JSON.
    fn node_to_json(node: &TreeNode) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "label".to_string(),
            serde_json::Value::String(node.label.clone()),
        );

        if let Some(ref value) = node.value {
            obj.insert(
                "value".to_string(),
                serde_json::Value::String(value.clone()),
            );
        }

        if !node.children.is_empty() {
            let children: Vec<serde_json::Value> =
                node.children.iter().map(Self::node_to_json).collect();
            obj.insert("children".to_string(), serde_json::Value::Array(children));
        }

        serde_json::Value::Object(obj)
    }
}

impl Default for QueryTreeView {
    fn default() -> Self {
        Self::new("Query")
    }
}

/// Helper to build a SELECT query tree.
#[derive(Debug, Default)]
pub struct SelectTreeBuilder {
    table: Option<String>,
    columns: Vec<String>,
    where_clause: Option<String>,
    order_by: Option<String>,
    limit: Option<String>,
    offset: Option<String>,
    joins: Vec<(String, String)>,
    group_by: Option<String>,
    having: Option<String>,
}

impl SelectTreeBuilder {
    /// Create a new SELECT tree builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the table name.
    #[must_use]
    pub fn table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    /// Set the columns.
    #[must_use]
    pub fn columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Set the WHERE clause.
    #[must_use]
    pub fn where_clause(mut self, clause: impl Into<String>) -> Self {
        self.where_clause = Some(clause.into());
        self
    }

    /// Set the ORDER BY clause.
    #[must_use]
    pub fn order_by(mut self, order: impl Into<String>) -> Self {
        self.order_by = Some(order.into());
        self
    }

    /// Set the LIMIT.
    #[must_use]
    pub fn limit(mut self, limit: impl Into<String>) -> Self {
        self.limit = Some(limit.into());
        self
    }

    /// Set the OFFSET.
    #[must_use]
    pub fn offset(mut self, offset: impl Into<String>) -> Self {
        self.offset = Some(offset.into());
        self
    }

    /// Add a JOIN.
    #[must_use]
    pub fn join(mut self, join_type: impl Into<String>, condition: impl Into<String>) -> Self {
        self.joins.push((join_type.into(), condition.into()));
        self
    }

    /// Set the GROUP BY clause.
    #[must_use]
    pub fn group_by(mut self, clause: impl Into<String>) -> Self {
        self.group_by = Some(clause.into());
        self
    }

    /// Set the HAVING clause.
    #[must_use]
    pub fn having(mut self, clause: impl Into<String>) -> Self {
        self.having = Some(clause.into());
        self
    }

    /// Build the query tree view.
    #[must_use]
    pub fn build(self) -> QueryTreeView {
        let root_label = format!("SELECT from {}", self.table.as_deref().unwrap_or("?"));
        let mut tree = QueryTreeView::new(root_label);

        // Columns
        if !self.columns.is_empty() {
            tree = tree.add_child("Columns", self.columns);
        }

        // JOINs
        for (join_type, condition) in self.joins {
            tree = tree.add_node(join_type, condition);
        }

        // WHERE
        if let Some(where_clause) = self.where_clause {
            tree = tree.add_node("WHERE", where_clause);
        }

        // GROUP BY
        if let Some(group_by) = self.group_by {
            tree = tree.add_node("GROUP BY", group_by);
        }

        // HAVING
        if let Some(having) = self.having {
            tree = tree.add_node("HAVING", having);
        }

        // ORDER BY
        if let Some(order_by) = self.order_by {
            tree = tree.add_node("ORDER BY", order_by);
        }

        // LIMIT
        if let Some(limit) = self.limit {
            tree = tree.add_node("LIMIT", limit);
        }

        // OFFSET
        if let Some(offset) = self.offset {
            tree = tree.add_node("OFFSET", offset);
        }

        tree
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_tree_new() {
        let tree = QueryTreeView::new("SELECT from users");
        let output = tree.render_plain();
        assert!(output.contains("SELECT from users"));
    }

    #[test]
    fn test_query_tree_with_node() {
        let tree = QueryTreeView::new("SELECT from users").add_node("WHERE", "id = 1");

        let output = tree.render_plain();
        assert!(output.contains("WHERE: id = 1"));
    }

    #[test]
    fn test_query_tree_with_children() {
        let tree = QueryTreeView::new("SELECT from users")
            .add_child("Columns", vec!["id", "name", "email"]);

        let output = tree.render_plain();
        assert!(output.contains("Columns"));
        assert!(output.contains("id"));
        assert!(output.contains("name"));
        assert!(output.contains("email"));
    }

    #[test]
    fn test_query_tree_unicode_chars() {
        let tree = QueryTreeView::new("Query")
            .add_node("Child", "value")
            .unicode();

        let output = tree.render_plain();
        assert!(output.contains("└── ") || output.contains("├── "));
    }

    #[test]
    fn test_query_tree_ascii_chars() {
        let tree = QueryTreeView::new("Query")
            .add_node("Child", "value")
            .ascii();

        let output = tree.render_plain();
        assert!(output.contains("\\-- ") || output.contains("+-- "));
    }

    #[test]
    fn test_query_tree_styled_contains_ansi() {
        let tree = QueryTreeView::new("SELECT from users").add_node("WHERE", "id = 1");

        let styled = tree.render_styled();
        assert!(styled.contains('\x1b'));
    }

    #[test]
    fn test_query_tree_to_json() {
        let tree = QueryTreeView::new("SELECT from users").add_node("WHERE", "id = 1");

        let json = tree.to_json();
        assert_eq!(json["label"], "SELECT from users");
        assert!(json["children"].is_array());
    }

    #[test]
    fn test_select_tree_builder() {
        let tree = SelectTreeBuilder::new()
            .table("heroes")
            .columns(vec!["id", "name", "secret_name"])
            .where_clause("age > 18")
            .order_by("name ASC")
            .limit("10")
            .build();

        let output = tree.render_plain();
        assert!(output.contains("SELECT from heroes"));
        assert!(output.contains("Columns"));
        assert!(output.contains("WHERE: age > 18"));
        assert!(output.contains("ORDER BY: name ASC"));
        assert!(output.contains("LIMIT: 10"));
    }

    #[test]
    fn test_select_tree_builder_with_join() {
        let tree = SelectTreeBuilder::new()
            .table("heroes")
            .join("LEFT JOIN teams", "heroes.team_id = teams.id")
            .build();

        let output = tree.render_plain();
        assert!(output.contains("LEFT JOIN teams"));
        assert!(output.contains("heroes.team_id = teams.id"));
    }

    #[test]
    fn test_tree_node_new() {
        let node = TreeNode::new("label");
        assert_eq!(node.label, "label");
        assert!(node.value.is_none());
    }

    #[test]
    fn test_tree_node_with_value() {
        let node = TreeNode::with_value("WHERE", "id = 1");
        assert_eq!(node.label, "WHERE");
        assert_eq!(node.value, Some("id = 1".to_string()));
    }

    #[test]
    fn test_tree_node_add_child() {
        let node = TreeNode::new("parent").add_child(TreeNode::new("child"));
        assert_eq!(node.children.len(), 1);
    }

    #[test]
    fn test_tree_node_add_items() {
        let node = TreeNode::new("Columns").add_items(vec!["a", "b", "c"]);
        assert_eq!(node.children.len(), 3);
    }

    #[test]
    fn test_nested_tree() {
        let tree = QueryTreeView::new("Root").add_tree_node(
            TreeNode::new("Level 1")
                .add_child(TreeNode::new("Level 2").add_child(TreeNode::new("Level 3"))),
        );

        let output = tree.render_plain();
        assert!(output.contains("Root"));
        assert!(output.contains("Level 1"));
        assert!(output.contains("Level 2"));
        assert!(output.contains("Level 3"));
    }

    #[test]
    fn test_select_builder_group_having() {
        let tree = SelectTreeBuilder::new()
            .table("orders")
            .columns(vec!["user_id", "COUNT(*)"])
            .group_by("user_id")
            .having("COUNT(*) > 5")
            .build();

        let output = tree.render_plain();
        assert!(output.contains("GROUP BY: user_id"));
        assert!(output.contains("HAVING: COUNT(*) > 5"));
    }

    #[test]
    fn test_default() {
        let tree = QueryTreeView::default();
        let output = tree.render_plain();
        assert!(output.contains("Query"));
    }
}
