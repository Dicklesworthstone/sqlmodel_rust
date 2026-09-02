//! JOIN clause types.

use crate::expr::{Dialect, Expr, adjust_placeholder_indices};
use crate::subquery::SelectQuery;
use sqlmodel_core::Value;

/// A JOIN clause.
#[derive(Debug, Clone)]
pub struct Join {
    /// Type of join
    pub join_type: JoinType,
    /// Table to join (table name or subquery SQL)
    pub table: String,
    /// Optional table alias
    pub alias: Option<String>,
    /// ON condition
    pub on: Expr,
    /// Whether this is a LATERAL join (subquery can reference outer query columns).
    ///
    /// Supported by PostgreSQL and MySQL 8.0+. Not supported by SQLite.
    pub lateral: bool,
    /// Whether the table field contains a subquery (wrapped in parentheses).
    pub is_subquery: bool,
    /// Parameters from a subquery table expression.
    pub subquery_params: Vec<Value>,
    /// Deferred subquery builder (dialect-aware).
    pub subquery: Option<Box<SelectQuery>>,
}

/// Types of SQL joins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

impl JoinType {
    /// Get the SQL keyword for this join type.
    pub const fn as_str(&self) -> &'static str {
        match self {
            JoinType::Inner => "INNER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
            JoinType::Full => "FULL JOIN",
            JoinType::Cross => "CROSS JOIN",
        }
    }
}

impl Join {
    /// Create an INNER JOIN.
    pub fn inner(table: impl Into<String>, on: Expr) -> Self {
        Self {
            join_type: JoinType::Inner,
            table: table.into(),
            alias: None,
            on,
            lateral: false,
            is_subquery: false,
            subquery_params: Vec::new(),
            subquery: None,
        }
    }

    /// Create a LEFT JOIN.
    pub fn left(table: impl Into<String>, on: Expr) -> Self {
        Self {
            join_type: JoinType::Left,
            table: table.into(),
            alias: None,
            on,
            lateral: false,
            is_subquery: false,
            subquery_params: Vec::new(),
            subquery: None,
        }
    }

    /// Create a RIGHT JOIN.
    pub fn right(table: impl Into<String>, on: Expr) -> Self {
        Self {
            join_type: JoinType::Right,
            table: table.into(),
            alias: None,
            on,
            lateral: false,
            is_subquery: false,
            subquery_params: Vec::new(),
            subquery: None,
        }
    }

    /// Create a FULL OUTER JOIN.
    pub fn full(table: impl Into<String>, on: Expr) -> Self {
        Self {
            join_type: JoinType::Full,
            table: table.into(),
            alias: None,
            on,
            lateral: false,
            is_subquery: false,
            subquery_params: Vec::new(),
            subquery: None,
        }
    }

    /// Create a CROSS JOIN (no ON condition needed, but we require one for uniformity).
    pub fn cross(table: impl Into<String>) -> Self {
        Self {
            join_type: JoinType::Cross,
            table: table.into(),
            alias: None,
            on: Expr::raw("TRUE"), // Dummy condition for cross join
            lateral: false,
            is_subquery: false,
            subquery_params: Vec::new(),
            subquery: None,
        }
    }

    /// Create a LATERAL JOIN with a subquery.
    ///
    /// A LATERAL subquery can reference columns from preceding FROM items.
    /// Supported by PostgreSQL (9.3+) and MySQL (8.0.14+). Not supported by SQLite.
    ///
    /// # Arguments
    ///
    /// * `join_type` - The join type (typically `JoinType::Inner` or `JoinType::Left`)
    /// * `subquery_sql` - The subquery SQL (without parentheses)
    /// * `alias` - Required alias for the lateral subquery
    /// * `on` - ON condition (use `Expr::raw("TRUE")` for implicit join)
    /// * `params` - Parameters for the subquery
    pub fn lateral(
        join_type: JoinType,
        subquery_sql: impl Into<String>,
        alias: impl Into<String>,
        on: Expr,
        params: Vec<Value>,
    ) -> Self {
        Self {
            join_type,
            table: subquery_sql.into(),
            alias: Some(alias.into()),
            on,
            lateral: true,
            is_subquery: true,
            subquery_params: params,
            subquery: None,
        }
    }

    /// Create a LATERAL JOIN with a deferred subquery builder.
    pub fn lateral_query(
        join_type: JoinType,
        subquery: SelectQuery,
        alias: impl Into<String>,
        on: Expr,
    ) -> Self {
        Self {
            join_type,
            table: String::new(),
            alias: Some(alias.into()),
            on,
            lateral: true,
            is_subquery: true,
            subquery_params: Vec::new(),
            subquery: Some(Box::new(subquery)),
        }
    }

    /// Create a LEFT JOIN LATERAL (most common form).
    ///
    /// Shorthand for `Join::lateral(JoinType::Left, ...)`.
    pub fn left_lateral(
        subquery_sql: impl Into<String>,
        alias: impl Into<String>,
        on: Expr,
        params: Vec<Value>,
    ) -> Self {
        Self::lateral(JoinType::Left, subquery_sql, alias, on, params)
    }

    /// Create an INNER JOIN LATERAL.
    pub fn inner_lateral(
        subquery_sql: impl Into<String>,
        alias: impl Into<String>,
        on: Expr,
        params: Vec<Value>,
    ) -> Self {
        Self::lateral(JoinType::Inner, subquery_sql, alias, on, params)
    }

    /// Create a CROSS JOIN LATERAL (no ON condition).
    pub fn cross_lateral(
        subquery_sql: impl Into<String>,
        alias: impl Into<String>,
        params: Vec<Value>,
    ) -> Self {
        Self {
            join_type: JoinType::Cross,
            table: subquery_sql.into(),
            alias: Some(alias.into()),
            on: Expr::raw("TRUE"),
            lateral: true,
            is_subquery: true,
            subquery_params: params,
            subquery: None,
        }
    }

    /// Set an alias for the joined table.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Mark this join as LATERAL.
    pub fn set_lateral(mut self) -> Self {
        self.lateral = true;
        self
    }

    /// Generate SQL for this JOIN clause and collect parameters.
    ///
    /// Returns a tuple of (sql, params) since the ON condition may contain
    /// literal values that need to be bound as parameters.
    pub fn to_sql(&self) -> (String, Vec<Value>) {
        let mut params = Vec::new();
        let sql = self.build_sql(Dialect::default(), &mut params, 0);
        (sql, params)
    }

    /// Generate SQL for this JOIN clause with a specific dialect.
    pub fn to_sql_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let mut params = Vec::new();
        let sql = self.build_sql(dialect, &mut params, 0);
        (sql, params)
    }

    /// Generate SQL and collect parameters.
    pub fn build(&self, params: &mut Vec<Value>, offset: usize) -> String {
        self.build_sql(Dialect::default(), params, offset)
    }

    /// Generate SQL and collect parameters with a specific dialect.
    pub fn build_with_dialect(
        &self,
        dialect: Dialect,
        params: &mut Vec<Value>,
        offset: usize,
    ) -> String {
        self.build_sql(dialect, params, offset)
    }

    fn build_sql(&self, dialect: Dialect, params: &mut Vec<Value>, offset: usize) -> String {
        let lateral_keyword = if self.lateral { " LATERAL" } else { "" };

        let (table_ref, subquery_params) = if let Some(subquery) = &self.subquery {
            let start_idx = offset + params.len();
            let (subquery_sql, subquery_params) = subquery.build_with_dialect(dialect);
            let adjusted_subquery = if subquery_params.is_empty() {
                subquery_sql
            } else {
                adjust_placeholder_indices(&subquery_sql, start_idx, dialect)
            };
            (format!("({})", adjusted_subquery), subquery_params)
        } else if self.is_subquery {
            let start_idx = offset + params.len();
            let adjusted_subquery = if self.subquery_params.is_empty() {
                self.table.clone()
            } else {
                adjust_placeholder_indices(&self.table, start_idx, dialect)
            };
            (
                format!("({})", adjusted_subquery),
                self.subquery_params.clone(),
            )
        } else {
            // A plain (possibly schema-qualified) table name: quote it for the
            // dialect so reserved words such as `order` or `user` work.
            (dialect.quote_table(&self.table), Vec::new())
        };

        let mut sql = format!(
            " {}{}{}",
            self.join_type.as_str(),
            lateral_keyword,
            if table_ref.is_empty() {
                String::new()
            } else {
                format!(" {}", table_ref)
            }
        );

        // Add subquery params before ON condition params
        params.extend(subquery_params);

        if let Some(alias) = &self.alias {
            sql.push_str(" AS ");
            sql.push_str(alias);
        }

        if self.join_type != JoinType::Cross {
            let on_sql = self.on.build_with_dialect(dialect, params, offset);
            sql.push_str(" ON ");
            sql.push_str(&on_sql);
        }

        sql
    }
}
