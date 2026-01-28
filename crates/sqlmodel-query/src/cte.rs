//! Common Table Expressions (CTEs) for SQL queries.
//!
//! Provides support for WITH clauses including recursive CTEs.
//!
//! # Example
//!
//! ```ignore
//! use sqlmodel_query::{Cte, CteRef, select};
//!
//! // Basic CTE
//! let active_users = Cte::new("active_users")
//!     .query(select!(User).filter(Expr::col("active").eq(true)));
//!
//! // Query using the CTE
//! let query = select_from_cte(&active_users)
//!     .columns(&["name", "email"]);
//!
//! // Recursive CTE for hierarchical data
//! let hierarchy = Cte::recursive("hierarchy")
//!     .columns(&["id", "name", "manager_id", "level"])
//!     .initial(
//!         select!(Employee)
//!             .filter(Expr::col("manager_id").is_null())
//!     )
//!     .recursive_term(|cte| {
//!         select!(Employee)
//!             .join_on("hierarchy", Expr::col("manager_id").eq(cte.col("id")))
//!     });
//! ```

use crate::expr::{Dialect, Expr};
use sqlmodel_core::Value;

/// A Common Table Expression (WITH clause).
#[derive(Debug, Clone)]
pub struct Cte {
    /// Name of the CTE
    name: String,
    /// Column aliases (optional)
    columns: Vec<String>,
    /// Whether this is a recursive CTE
    recursive: bool,
    /// The SQL query for the CTE (pre-built)
    query_sql: String,
    /// Parameters for the CTE query
    query_params: Vec<Value>,
    /// For recursive CTEs: the UNION part
    union_sql: Option<String>,
    /// Parameters for the UNION part
    union_params: Vec<Value>,
}

impl Cte {
    /// Create a new non-recursive CTE.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the CTE (used to reference it in the main query)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let recent_orders = Cte::new("recent_orders")
    ///     .as_select("SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '7 days'");
    /// ```
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            recursive: false,
            query_sql: String::new(),
            query_params: Vec::new(),
            union_sql: None,
            union_params: Vec::new(),
        }
    }

    /// Create a new recursive CTE.
    ///
    /// Recursive CTEs require an initial (anchor) term and a recursive term
    /// joined with UNION ALL.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Traverse an employee hierarchy
    /// let hierarchy = Cte::recursive("org_chart")
    ///     .columns(&["id", "name", "level"])
    ///     .as_select("SELECT id, name, 0 FROM employees WHERE manager_id IS NULL")
    ///     .union_all("SELECT e.id, e.name, h.level + 1 FROM employees e JOIN org_chart h ON e.manager_id = h.id");
    /// ```
    pub fn recursive(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            recursive: true,
            query_sql: String::new(),
            query_params: Vec::new(),
            union_sql: None,
            union_params: Vec::new(),
        }
    }

    /// Specify column aliases for the CTE.
    ///
    /// # Example
    ///
    /// ```ignore
    /// Cte::new("totals")
    ///     .columns(&["category", "total_amount"])
    ///     .as_select("SELECT category, SUM(amount) FROM orders GROUP BY category");
    /// ```
    pub fn columns(mut self, cols: &[&str]) -> Self {
        self.columns = cols.iter().map(|&s| s.to_string()).collect();
        self
    }

    /// Set the CTE query from a raw SQL string.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query for the CTE
    pub fn as_select(mut self, sql: impl Into<String>) -> Self {
        self.query_sql = sql.into();
        self
    }

    /// Set the CTE query from SQL with parameters.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query for the CTE
    /// * `params` - Parameters to bind
    pub fn as_select_with_params(mut self, sql: impl Into<String>, params: Vec<Value>) -> Self {
        self.query_sql = sql.into();
        self.query_params = params;
        self
    }

    /// Add a UNION ALL clause for recursive CTEs.
    ///
    /// # Arguments
    ///
    /// * `sql` - The recursive term SQL
    pub fn union_all(mut self, sql: impl Into<String>) -> Self {
        self.union_sql = Some(sql.into());
        self
    }

    /// Add a UNION ALL clause with parameters.
    pub fn union_all_with_params(mut self, sql: impl Into<String>, params: Vec<Value>) -> Self {
        self.union_sql = Some(sql.into());
        self.union_params = params;
        self
    }

    /// Get the name of this CTE.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Check if this is a recursive CTE.
    pub fn is_recursive(&self) -> bool {
        self.recursive
    }

    /// Create a reference to this CTE for use in queries.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let cte = Cte::new("active_users").as_select("...");
    /// let cte_ref = cte.as_ref();
    ///
    /// // Use in expressions
    /// let expr = cte_ref.col("name").eq("Alice");
    /// ```
    pub fn as_ref(&self) -> CteRef {
        CteRef {
            name: self.name.clone(),
        }
    }

    /// Build the CTE definition SQL.
    ///
    /// Returns the SQL for use in a WITH clause and the parameters.
    pub fn build(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let mut sql = String::new();
        let mut params = Vec::new();

        // CTE name and optional column list
        sql.push_str(&dialect.quote_identifier(&self.name));

        if !self.columns.is_empty() {
            sql.push_str(" (");
            let quoted_cols: Vec<_> = self
                .columns
                .iter()
                .map(|c| dialect.quote_identifier(c))
                .collect();
            sql.push_str(&quoted_cols.join(", "));
            sql.push(')');
        }

        sql.push_str(" AS (");

        // Main query
        sql.push_str(&self.query_sql);
        params.extend(self.query_params.clone());

        // UNION ALL for recursive CTEs
        if let Some(union) = &self.union_sql {
            sql.push_str(" UNION ALL ");
            sql.push_str(union);
            params.extend(self.union_params.clone());
        }

        sql.push(')');

        (sql, params)
    }
}

/// A reference to a CTE for use in expressions.
#[derive(Debug, Clone)]
pub struct CteRef {
    name: String,
}

impl CteRef {
    /// Create a new CTE reference.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Reference a column in this CTE.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let cte_ref = CteRef::new("active_users");
    /// let expr = cte_ref.col("email").like("%@example.com");
    /// ```
    pub fn col(&self, column: impl Into<String>) -> Expr {
        Expr::qualified(&self.name, column)
    }

    /// Get the CTE name for use in FROM clauses.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A query with one or more CTEs.
#[derive(Debug, Clone)]
pub struct WithQuery {
    /// List of CTEs in order of definition
    ctes: Vec<Cte>,
    /// The main query SQL
    main_sql: String,
    /// Parameters for the main query
    main_params: Vec<Value>,
}

impl WithQuery {
    /// Create a new query with CTEs.
    pub fn new() -> Self {
        Self {
            ctes: Vec::new(),
            main_sql: String::new(),
            main_params: Vec::new(),
        }
    }

    /// Add a CTE to this query.
    ///
    /// CTEs are added in order and can reference previously defined CTEs.
    pub fn with_cte(mut self, cte: Cte) -> Self {
        self.ctes.push(cte);
        self
    }

    /// Add multiple CTEs to this query.
    pub fn with_ctes(mut self, ctes: Vec<Cte>) -> Self {
        self.ctes.extend(ctes);
        self
    }

    /// Set the main query.
    pub fn select(mut self, sql: impl Into<String>) -> Self {
        self.main_sql = sql.into();
        self
    }

    /// Set the main query with parameters.
    pub fn select_with_params(mut self, sql: impl Into<String>, params: Vec<Value>) -> Self {
        self.main_sql = sql.into();
        self.main_params = params;
        self
    }

    /// Build the complete SQL with WITH clause.
    pub fn build(&self) -> (String, Vec<Value>) {
        self.build_with_dialect(Dialect::Postgres)
    }

    /// Build the complete SQL with a specific dialect.
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let mut sql = String::new();
        let mut params = Vec::new();

        if !self.ctes.is_empty() {
            // Check if any CTE is recursive
            let has_recursive = self.ctes.iter().any(|c| c.recursive);

            if has_recursive {
                sql.push_str("WITH RECURSIVE ");
            } else {
                sql.push_str("WITH ");
            }

            // Build each CTE
            let cte_sqls: Vec<String> = self
                .ctes
                .iter()
                .map(|cte| {
                    let (cte_sql, cte_params) = cte.build(dialect);
                    params.extend(cte_params);
                    cte_sql
                })
                .collect();

            sql.push_str(&cte_sqls.join(", "));
            sql.push(' ');
        }

        // Main query
        sql.push_str(&self.main_sql);
        params.extend(self.main_params.clone());

        (sql, params)
    }
}

impl Default for WithQuery {
    fn default() -> Self {
        Self::new()
    }
}

// ==================== Tests ====================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_cte() {
        let cte = Cte::new("active_users").as_select("SELECT * FROM users WHERE active = true");

        let (sql, params) = cte.build(Dialect::Postgres);
        assert_eq!(
            sql,
            "\"active_users\" AS (SELECT * FROM users WHERE active = true)"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_cte_with_columns() {
        let cte = Cte::new("user_totals")
            .columns(&["user_id", "total"])
            .as_select("SELECT user_id, SUM(amount) FROM orders GROUP BY user_id");

        let (sql, params) = cte.build(Dialect::Postgres);
        assert_eq!(
            sql,
            "\"user_totals\" (\"user_id\", \"total\") AS (SELECT user_id, SUM(amount) FROM orders GROUP BY user_id)"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_cte_with_params() {
        let cte = Cte::new("recent_orders").as_select_with_params(
            "SELECT * FROM orders WHERE amount > $1",
            vec![Value::Int(100)],
        );

        let (sql, params) = cte.build(Dialect::Postgres);
        assert_eq!(
            sql,
            "\"recent_orders\" AS (SELECT * FROM orders WHERE amount > $1)"
        );
        assert_eq!(params, vec![Value::Int(100)]);
    }

    #[test]
    fn test_recursive_cte() {
        let cte = Cte::recursive("hierarchy")
            .columns(&["id", "name", "level"])
            .as_select("SELECT id, name, 0 FROM employees WHERE manager_id IS NULL")
            .union_all("SELECT e.id, e.name, h.level + 1 FROM employees e JOIN hierarchy h ON e.manager_id = h.id");

        let (sql, _) = cte.build(Dialect::Postgres);
        assert!(sql.contains("UNION ALL"));
        assert!(cte.is_recursive());
    }

    #[test]
    fn test_cte_ref_column() {
        let cte_ref = CteRef::new("my_cte");
        let expr = cte_ref.col("name");

        let mut params = Vec::new();
        let sql = expr.build(&mut params, 0);
        assert_eq!(sql, "\"my_cte\".\"name\"");
    }

    #[test]
    fn test_with_query_single_cte() {
        let cte = Cte::new("active_users").as_select("SELECT * FROM users WHERE active = true");

        let query = WithQuery::new()
            .with_cte(cte)
            .select("SELECT * FROM active_users");

        let (sql, params) = query.build();
        assert_eq!(
            sql,
            "WITH \"active_users\" AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_with_query_multiple_ctes() {
        let cte1 = Cte::new("active_users").as_select("SELECT * FROM users WHERE active = true");

        let cte2 = Cte::new("user_orders")
            .as_select("SELECT u.id, COUNT(*) as order_count FROM active_users u JOIN orders o ON u.id = o.user_id GROUP BY u.id");

        let query = WithQuery::new()
            .with_cte(cte1)
            .with_cte(cte2)
            .select("SELECT * FROM user_orders WHERE order_count > 5");

        let (sql, _) = query.build();
        assert!(sql.starts_with("WITH "));
        assert!(sql.contains("\"active_users\" AS"));
        assert!(sql.contains("\"user_orders\" AS"));
    }

    #[test]
    fn test_with_query_recursive() {
        let cte = Cte::recursive("numbers")
            .columns(&["n"])
            .as_select("SELECT 1")
            .union_all("SELECT n + 1 FROM numbers WHERE n < 10");

        let query = WithQuery::new()
            .with_cte(cte)
            .select("SELECT * FROM numbers");

        let (sql, _) = query.build();
        assert!(sql.starts_with("WITH RECURSIVE "));
    }

    #[test]
    fn test_cte_mysql_dialect() {
        let cte = Cte::new("temp")
            .columns(&["col1", "col2"])
            .as_select("SELECT a, b FROM t");

        let (sql, _) = cte.build(Dialect::Mysql);
        assert_eq!(sql, "`temp` (`col1`, `col2`) AS (SELECT a, b FROM t)");
    }

    #[test]
    fn test_cte_sqlite_dialect() {
        let cte = Cte::new("temp").as_select("SELECT 1");

        let (sql, _) = cte.build(Dialect::Sqlite);
        assert_eq!(sql, "\"temp\" AS (SELECT 1)");
    }

    #[test]
    fn test_with_query_params_aggregation() {
        let cte = Cte::new("filtered")
            .as_select_with_params("SELECT * FROM items WHERE price > $1", vec![Value::Int(50)]);

        let query = WithQuery::new().with_cte(cte).select_with_params(
            "SELECT * FROM filtered WHERE category = $2",
            vec![Value::Text("electronics".to_string())],
        );

        let (sql, params) = query.build();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0], Value::Int(50));
        assert_eq!(params[1], Value::Text("electronics".to_string()));
        assert!(sql.contains("$1"));
        assert!(sql.contains("$2"));
    }

    #[test]
    fn test_recursive_cte_hierarchy_example() {
        // Classic organizational hierarchy example
        let cte = Cte::recursive("org_chart")
            .columns(&["id", "name", "manager_id", "level"])
            .as_select("SELECT id, name, manager_id, 0 AS level FROM employees WHERE manager_id IS NULL")
            .union_all("SELECT e.id, e.name, e.manager_id, oc.level + 1 FROM employees e INNER JOIN org_chart oc ON e.manager_id = oc.id");

        let query = WithQuery::new()
            .with_cte(cte)
            .select("SELECT * FROM org_chart ORDER BY level, name");

        let (sql, _) = query.build();

        assert!(sql.starts_with("WITH RECURSIVE "));
        assert!(sql.contains("\"org_chart\""));
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("ORDER BY level, name"));
    }

    #[test]
    fn test_cte_chained_references() {
        // CTE that references another CTE
        let cte1 =
            Cte::new("base_data").as_select("SELECT id, value FROM raw_data WHERE valid = true");

        let cte2 = Cte::new("aggregated")
            .as_select("SELECT COUNT(*) as cnt, SUM(value) as total FROM base_data");

        let query = WithQuery::new()
            .with_cte(cte1)
            .with_cte(cte2)
            .select("SELECT * FROM aggregated");

        let (sql, _) = query.build();

        // Verify both CTEs are present and in order
        let base_pos = sql.find("\"base_data\"").unwrap();
        let agg_pos = sql.find("\"aggregated\"").unwrap();
        assert!(
            base_pos < agg_pos,
            "base_data should come before aggregated"
        );
    }
}
