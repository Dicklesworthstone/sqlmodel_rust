//! JOIN clause types.

use crate::expr::Expr;
use sqlmodel_core::Value;

/// A JOIN clause.
#[derive(Debug, Clone)]
pub struct Join {
    /// Type of join
    pub join_type: JoinType,
    /// Table to join
    pub table: String,
    /// Optional table alias
    pub alias: Option<String>,
    /// ON condition
    pub on: Expr,
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
        }
    }

    /// Create a LEFT JOIN.
    pub fn left(table: impl Into<String>, on: Expr) -> Self {
        Self {
            join_type: JoinType::Left,
            table: table.into(),
            alias: None,
            on,
        }
    }

    /// Create a RIGHT JOIN.
    pub fn right(table: impl Into<String>, on: Expr) -> Self {
        Self {
            join_type: JoinType::Right,
            table: table.into(),
            alias: None,
            on,
        }
    }

    /// Create a FULL OUTER JOIN.
    pub fn full(table: impl Into<String>, on: Expr) -> Self {
        Self {
            join_type: JoinType::Full,
            table: table.into(),
            alias: None,
            on,
        }
    }

    /// Create a CROSS JOIN (no ON condition needed, but we require one for uniformity).
    pub fn cross(table: impl Into<String>) -> Self {
        Self {
            join_type: JoinType::Cross,
            table: table.into(),
            alias: None,
            on: Expr::raw("TRUE"), // Dummy condition for cross join
        }
    }

    /// Set an alias for the joined table.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Generate SQL for this JOIN clause and collect parameters.
    ///
    /// Returns a tuple of (sql, params) since the ON condition may contain
    /// literal values that need to be bound as parameters.
    pub fn to_sql(&self) -> (String, Vec<Value>) {
        let mut params = Vec::new();
        let mut sql = format!(" {} {}", self.join_type.as_str(), self.table);

        if let Some(alias) = &self.alias {
            sql.push_str(" AS ");
            sql.push_str(alias);
        }

        if self.join_type != JoinType::Cross {
            let on_sql = self.on.build(&mut params, 0);
            sql.push_str(" ON ");
            sql.push_str(&on_sql);
        }

        (sql, params)
    }

    /// Generate SQL and collect parameters.
    pub fn build(&self, params: &mut Vec<Value>, offset: usize) -> String {
        let mut sql = format!(" {} {}", self.join_type.as_str(), self.table);

        if let Some(alias) = &self.alias {
            sql.push_str(" AS ");
            sql.push_str(alias);
        }

        if self.join_type != JoinType::Cross {
            let on_sql = self.on.build(params, offset);
            sql.push_str(" ON ");
            sql.push_str(&on_sql);
        }

        sql
    }
}
