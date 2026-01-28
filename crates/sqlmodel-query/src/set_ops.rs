//! Set operations for combining query results.
//!
//! Provides UNION, UNION ALL, INTERSECT, INTERSECT ALL, EXCEPT, and EXCEPT ALL
//! operations for combining multiple SELECT queries.
//!
//! # Example
//!
//! ```ignore
//! use sqlmodel_query::{select, union, union_all, SetOperation};
//!
//! // UNION - removes duplicates
//! let admins = select!(User).filter(Expr::col("role").eq("admin"));
//! let managers = select!(User).filter(Expr::col("role").eq("manager"));
//! let query = admins.union(managers);
//!
//! // UNION ALL - keeps duplicates
//! let query = union_all([query1, query2, query3]);
//!
//! // With ORDER BY on final result
//! let query = select!(User)
//!     .filter(Expr::col("active").eq(true))
//!     .union(select!(User).filter(Expr::col("premium").eq(true)))
//!     .order_by(Expr::col("name").asc());
//! ```

use crate::clause::OrderBy;
use crate::expr::Dialect;
use sqlmodel_core::Value;

/// Type of set operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpType {
    /// UNION - combines results, removes duplicates
    Union,
    /// UNION ALL - combines results, keeps duplicates
    UnionAll,
    /// INTERSECT - returns common rows, removes duplicates
    Intersect,
    /// INTERSECT ALL - returns common rows, keeps duplicates
    IntersectAll,
    /// EXCEPT - returns rows in first query not in second, removes duplicates
    Except,
    /// EXCEPT ALL - returns rows in first query not in second, keeps duplicates
    ExceptAll,
}

impl SetOpType {
    /// Get the SQL keyword for this set operation.
    pub const fn as_sql(&self) -> &'static str {
        match self {
            SetOpType::Union => "UNION",
            SetOpType::UnionAll => "UNION ALL",
            SetOpType::Intersect => "INTERSECT",
            SetOpType::IntersectAll => "INTERSECT ALL",
            SetOpType::Except => "EXCEPT",
            SetOpType::ExceptAll => "EXCEPT ALL",
        }
    }
}

/// A set operation combining multiple queries.
#[derive(Debug, Clone)]
pub struct SetOperation {
    /// The queries to combine (in order)
    queries: Vec<(String, Vec<Value>)>,
    /// The type of set operation between consecutive queries
    op_types: Vec<SetOpType>,
    /// Optional ORDER BY on the final result
    order_by: Vec<OrderBy>,
    /// Optional LIMIT on the final result
    limit: Option<u64>,
    /// Optional OFFSET on the final result
    offset: Option<u64>,
}

impl SetOperation {
    /// Create a new set operation from a single query.
    pub fn new(query_sql: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            queries: vec![(query_sql.into(), params)],
            op_types: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    /// Add a UNION operation with another query.
    pub fn union(self, query_sql: impl Into<String>, params: Vec<Value>) -> Self {
        self.add_op(SetOpType::Union, query_sql, params)
    }

    /// Add a UNION ALL operation with another query.
    pub fn union_all(self, query_sql: impl Into<String>, params: Vec<Value>) -> Self {
        self.add_op(SetOpType::UnionAll, query_sql, params)
    }

    /// Add an INTERSECT operation with another query.
    pub fn intersect(self, query_sql: impl Into<String>, params: Vec<Value>) -> Self {
        self.add_op(SetOpType::Intersect, query_sql, params)
    }

    /// Add an INTERSECT ALL operation with another query.
    pub fn intersect_all(self, query_sql: impl Into<String>, params: Vec<Value>) -> Self {
        self.add_op(SetOpType::IntersectAll, query_sql, params)
    }

    /// Add an EXCEPT operation with another query.
    pub fn except(self, query_sql: impl Into<String>, params: Vec<Value>) -> Self {
        self.add_op(SetOpType::Except, query_sql, params)
    }

    /// Add an EXCEPT ALL operation with another query.
    pub fn except_all(self, query_sql: impl Into<String>, params: Vec<Value>) -> Self {
        self.add_op(SetOpType::ExceptAll, query_sql, params)
    }

    fn add_op(mut self, op: SetOpType, query_sql: impl Into<String>, params: Vec<Value>) -> Self {
        self.op_types.push(op);
        self.queries.push((query_sql.into(), params));
        self
    }

    /// Add ORDER BY to the final result.
    pub fn order_by(mut self, order: OrderBy) -> Self {
        self.order_by.push(order);
        self
    }

    /// Add multiple ORDER BY clauses.
    pub fn order_by_many(mut self, orders: Vec<OrderBy>) -> Self {
        self.order_by.extend(orders);
        self
    }

    /// Set LIMIT on the final result.
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set OFFSET on the final result.
    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Build the SQL query with default dialect (PostgreSQL).
    pub fn build(&self) -> (String, Vec<Value>) {
        self.build_with_dialect(Dialect::Postgres)
    }

    /// Build the SQL query with a specific dialect.
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let mut sql = String::new();
        let mut params = Vec::new();

        // Build each query with set operations between them
        for (i, (query_sql, query_params)) in self.queries.iter().enumerate() {
            if i > 0 {
                // Add the set operation before this query
                let op = &self.op_types[i - 1];
                sql.push(' ');
                sql.push_str(op.as_sql());
                sql.push(' ');
            }

            // Wrap each query in parentheses for clarity
            sql.push('(');
            sql.push_str(query_sql);
            sql.push(')');

            params.extend(query_params.clone());
        }

        // ORDER BY on final result
        if !self.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            let order_strs: Vec<String> = self
                .order_by
                .iter()
                .map(|o| {
                    let expr_sql = o.expr.build_with_dialect(dialect, &mut params, 0);
                    let dir = match o.direction {
                        crate::clause::OrderDirection::Asc => "ASC",
                        crate::clause::OrderDirection::Desc => "DESC",
                    };
                    let nulls = match o.nulls {
                        Some(crate::clause::NullsOrder::First) => " NULLS FIRST",
                        Some(crate::clause::NullsOrder::Last) => " NULLS LAST",
                        None => "",
                    };
                    format!("{expr_sql} {dir}{nulls}")
                })
                .collect();
            sql.push_str(&order_strs.join(", "));
        }

        // LIMIT
        if let Some(limit) = self.limit {
            sql.push_str(" LIMIT ");
            sql.push_str(&limit.to_string());
        }

        // OFFSET
        if let Some(offset) = self.offset {
            sql.push_str(" OFFSET ");
            sql.push_str(&offset.to_string());
        }

        (sql, params)
    }
}

/// Create a UNION of multiple queries.
///
/// # Example
///
/// ```ignore
/// let query = union([
///     ("SELECT * FROM users WHERE role = 'admin'", vec![]),
///     ("SELECT * FROM users WHERE role = 'manager'", vec![]),
/// ]);
/// ```
pub fn union<I, S>(queries: I) -> SetOperation
where
    I: IntoIterator<Item = (S, Vec<Value>)>,
    S: Into<String>,
{
    combine_queries(SetOpType::Union, queries)
}

/// Create a UNION ALL of multiple queries.
///
/// # Example
///
/// ```ignore
/// let query = union_all([
///     ("SELECT id FROM table1", vec![]),
///     ("SELECT id FROM table2", vec![]),
///     ("SELECT id FROM table3", vec![]),
/// ]);
/// ```
pub fn union_all<I, S>(queries: I) -> SetOperation
where
    I: IntoIterator<Item = (S, Vec<Value>)>,
    S: Into<String>,
{
    combine_queries(SetOpType::UnionAll, queries)
}

/// Create an INTERSECT of multiple queries.
pub fn intersect<I, S>(queries: I) -> SetOperation
where
    I: IntoIterator<Item = (S, Vec<Value>)>,
    S: Into<String>,
{
    combine_queries(SetOpType::Intersect, queries)
}

/// Create an INTERSECT ALL of multiple queries.
pub fn intersect_all<I, S>(queries: I) -> SetOperation
where
    I: IntoIterator<Item = (S, Vec<Value>)>,
    S: Into<String>,
{
    combine_queries(SetOpType::IntersectAll, queries)
}

/// Create an EXCEPT of multiple queries.
pub fn except<I, S>(queries: I) -> SetOperation
where
    I: IntoIterator<Item = (S, Vec<Value>)>,
    S: Into<String>,
{
    combine_queries(SetOpType::Except, queries)
}

/// Create an EXCEPT ALL of multiple queries.
pub fn except_all<I, S>(queries: I) -> SetOperation
where
    I: IntoIterator<Item = (S, Vec<Value>)>,
    S: Into<String>,
{
    combine_queries(SetOpType::ExceptAll, queries)
}

fn combine_queries<I, S>(op: SetOpType, queries: I) -> SetOperation
where
    I: IntoIterator<Item = (S, Vec<Value>)>,
    S: Into<String>,
{
    let mut iter = queries.into_iter();

    // Get the first query
    let (first_sql, first_params) = iter
        .next()
        .expect("set operation requires at least one query");

    let mut result = SetOperation::new(first_sql, first_params);

    // Add remaining queries with the set operation
    for (sql, params) in iter {
        result = result.add_op(op, sql, params);
    }

    result
}

// ==================== Tests ====================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Expr;

    #[test]
    fn test_union_basic() {
        let query = SetOperation::new("SELECT * FROM users WHERE role = 'admin'", vec![])
            .union("SELECT * FROM users WHERE role = 'manager'", vec![]);

        let (sql, params) = query.build();
        assert_eq!(
            sql,
            "(SELECT * FROM users WHERE role = 'admin') UNION (SELECT * FROM users WHERE role = 'manager')"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_union_all_basic() {
        let query = SetOperation::new("SELECT id FROM table1", vec![])
            .union_all("SELECT id FROM table2", vec![]);

        let (sql, _) = query.build();
        assert_eq!(
            sql,
            "(SELECT id FROM table1) UNION ALL (SELECT id FROM table2)"
        );
    }

    #[test]
    fn test_union_with_params() {
        let query = SetOperation::new(
            "SELECT * FROM users WHERE role = $1",
            vec![Value::Text("admin".to_string())],
        )
        .union(
            "SELECT * FROM users WHERE role = $2",
            vec![Value::Text("manager".to_string())],
        );

        let (sql, params) = query.build();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0], Value::Text("admin".to_string()));
        assert_eq!(params[1], Value::Text("manager".to_string()));
        assert!(sql.contains("$1"));
        assert!(sql.contains("$2"));
    }

    #[test]
    fn test_union_function() {
        let query = union([
            ("SELECT * FROM admins", vec![]),
            ("SELECT * FROM managers", vec![]),
            ("SELECT * FROM employees", vec![]),
        ]);

        let (sql, _) = query.build();
        assert!(sql.contains("UNION"));
        assert!(!sql.contains("UNION ALL"));
        assert!(sql.contains("admins"));
        assert!(sql.contains("managers"));
        assert!(sql.contains("employees"));
    }

    #[test]
    fn test_union_all_function() {
        let query = union_all([
            ("SELECT 1", vec![]),
            ("SELECT 2", vec![]),
            ("SELECT 3", vec![]),
        ]);

        let (sql, _) = query.build();
        // Should have two UNION ALL operations
        assert_eq!(sql.matches("UNION ALL").count(), 2);
    }

    #[test]
    fn test_union_with_order_by() {
        let query = SetOperation::new("SELECT name FROM users WHERE active = true", vec![])
            .union("SELECT name FROM users WHERE premium = true", vec![])
            .order_by(Expr::col("name").asc());

        let (sql, _) = query.build();
        assert!(sql.ends_with("ORDER BY \"name\" ASC"));
    }

    #[test]
    fn test_union_with_limit_offset() {
        let query = SetOperation::new("SELECT * FROM t1", vec![])
            .union("SELECT * FROM t2", vec![])
            .limit(10)
            .offset(5);

        let (sql, _) = query.build();
        assert!(sql.ends_with("LIMIT 10 OFFSET 5"));
    }

    #[test]
    fn test_intersect() {
        let query = SetOperation::new("SELECT id FROM users WHERE active = true", vec![])
            .intersect("SELECT id FROM users WHERE premium = true", vec![]);

        let (sql, _) = query.build();
        assert!(sql.contains("INTERSECT"));
        assert!(!sql.contains("INTERSECT ALL"));
    }

    #[test]
    fn test_intersect_all() {
        let query = intersect_all([("SELECT id FROM t1", vec![]), ("SELECT id FROM t2", vec![])]);

        let (sql, _) = query.build();
        assert!(sql.contains("INTERSECT ALL"));
    }

    #[test]
    fn test_except() {
        let query = SetOperation::new("SELECT id FROM all_users", vec![])
            .except("SELECT id FROM banned_users", vec![]);

        let (sql, _) = query.build();
        assert!(sql.contains("EXCEPT"));
        assert!(!sql.contains("EXCEPT ALL"));
    }

    #[test]
    fn test_except_all() {
        let query = except_all([("SELECT id FROM t1", vec![]), ("SELECT id FROM t2", vec![])]);

        let (sql, _) = query.build();
        assert!(sql.contains("EXCEPT ALL"));
    }

    #[test]
    fn test_chained_operations() {
        let query = SetOperation::new("SELECT id FROM t1", vec![])
            .union("SELECT id FROM t2", vec![])
            .union_all("SELECT id FROM t3", vec![]);

        let (sql, _) = query.build();
        // First should be UNION, second should be UNION ALL
        let union_pos = sql.find("UNION").unwrap();
        let union_all_pos = sql.find("UNION ALL").unwrap();
        assert!(union_pos < union_all_pos);
    }

    #[test]
    fn test_complex_query() {
        let query = SetOperation::new(
            "SELECT name, email FROM users WHERE role = $1",
            vec![Value::Text("admin".to_string())],
        )
        .union_all(
            "SELECT name, email FROM users WHERE department = $2",
            vec![Value::Text("engineering".to_string())],
        )
        .order_by(Expr::col("name").asc())
        .order_by(Expr::col("email").desc())
        .limit(100)
        .offset(0);

        let (sql, params) = query.build();

        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("LIMIT 100"));
        assert!(sql.contains("OFFSET 0"));
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_set_op_type_sql() {
        assert_eq!(SetOpType::Union.as_sql(), "UNION");
        assert_eq!(SetOpType::UnionAll.as_sql(), "UNION ALL");
        assert_eq!(SetOpType::Intersect.as_sql(), "INTERSECT");
        assert_eq!(SetOpType::IntersectAll.as_sql(), "INTERSECT ALL");
        assert_eq!(SetOpType::Except.as_sql(), "EXCEPT");
        assert_eq!(SetOpType::ExceptAll.as_sql(), "EXCEPT ALL");
    }
}
