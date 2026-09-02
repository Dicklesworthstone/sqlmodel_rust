//! Dialect-aware subquery builders.

use crate::clause::{Limit, Offset, OrderBy, Where};
use crate::expr::Dialect;
use crate::join::Join;
use sqlmodel_core::Value;

/// Non-generic SELECT representation for subqueries.
///
/// This is used to defer SQL generation until a specific dialect is known.
#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct SelectQuery {
    /// Table name for FROM clause
    pub table: String,
    /// Columns to select (empty = all)
    pub columns: Vec<String>,
    /// WHERE clause conditions
    pub where_clause: Option<Where>,
    /// ORDER BY clauses
    pub order_by: Vec<OrderBy>,
    /// JOIN clauses
    pub joins: Vec<Join>,
    /// LIMIT clause
    pub limit: Option<Limit>,
    /// OFFSET clause
    pub offset: Option<Offset>,
    /// GROUP BY columns
    pub group_by: Vec<String>,
    /// HAVING clause
    pub having: Option<Where>,
    /// DISTINCT flag
    pub distinct: bool,
    /// FOR UPDATE flag
    pub for_update: bool,
}

impl SelectQuery {
    /// Build the SQL query and parameters with a specific dialect.
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let mut sql = String::new();
        let mut params = Vec::new();

        // SELECT
        sql.push_str("SELECT ");
        if self.distinct {
            sql.push_str("DISTINCT ");
        }

        if self.columns.is_empty() {
            sql.push('*');
        } else {
            sql.push_str(&self.columns.join(", "));
        }

        // FROM
        sql.push_str(" FROM ");
        sql.push_str(&dialect.quote_table(&self.table));

        // JOINs
        for join in &self.joins {
            sql.push_str(&join.build_with_dialect(dialect, &mut params, 0));
        }

        // WHERE
        if let Some(where_clause) = &self.where_clause {
            let (where_sql, where_params) = where_clause.build_with_dialect(dialect, params.len());
            sql.push_str(" WHERE ");
            sql.push_str(&where_sql);
            params.extend(where_params);
        }

        // GROUP BY
        if !self.group_by.is_empty() {
            sql.push_str(" GROUP BY ");
            sql.push_str(&self.group_by.join(", "));
        }

        // HAVING
        if let Some(having) = &self.having {
            let (having_sql, having_params) = having.build_with_dialect(dialect, params.len());
            sql.push_str(" HAVING ");
            sql.push_str(&having_sql);
            params.extend(having_params);
        }

        // ORDER BY
        if !self.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            let order_strs: Vec<_> = self
                .order_by
                .iter()
                .map(|o| o.build(dialect, &mut params, 0))
                .collect();
            sql.push_str(&order_strs.join(", "));
        }

        // LIMIT
        if let Some(Limit(n)) = self.limit {
            sql.push_str(&format!(" LIMIT {}", n));
        }

        // OFFSET
        if let Some(Offset(n)) = self.offset {
            sql.push_str(&format!(" OFFSET {}", n));
        }

        // FOR UPDATE
        if self.for_update {
            sql.push_str(" FOR UPDATE");
        }

        (sql, params)
    }

    /// Build an optimized EXISTS subquery (SELECT 1 instead of SELECT *).
    pub fn build_exists_subquery_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let mut sql = String::new();
        let mut params = Vec::new();

        // SELECT 1 for optimal EXISTS performance
        sql.push_str("SELECT 1 FROM ");
        sql.push_str(&dialect.quote_table(&self.table));

        // JOINs (if any)
        for join in &self.joins {
            sql.push_str(&join.build_with_dialect(dialect, &mut params, 0));
        }

        // WHERE
        if let Some(where_clause) = &self.where_clause {
            let (where_sql, where_params) = where_clause.build_with_dialect(dialect, params.len());
            sql.push_str(" WHERE ");
            sql.push_str(&where_sql);
            params.extend(where_params);
        }

        // GROUP BY (rare in EXISTS but supported)
        if !self.group_by.is_empty() {
            sql.push_str(" GROUP BY ");
            sql.push_str(&self.group_by.join(", "));
        }

        // HAVING (rare in EXISTS but supported)
        if let Some(having) = &self.having {
            let (having_sql, having_params) = having.build_with_dialect(dialect, params.len());
            sql.push_str(" HAVING ");
            sql.push_str(&having_sql);
            params.extend(having_params);
        }

        // Note: ORDER BY, LIMIT, OFFSET are omitted in EXISTS subquery as they have no effect

        (sql, params)
    }
}
