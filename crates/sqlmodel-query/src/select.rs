//! SELECT query builder.

use crate::clause::{Limit, Offset, OrderBy, Where};
use crate::eager::{
    EagerLoader, IncludePath, build_aliased_column_parts, build_join_clause, find_relationship,
};
use crate::expr::{Dialect, Expr};
use crate::join::Join;
use crate::subquery::SelectQuery;
use asupersync::{Cx, Outcome};
use sqlmodel_core::{Connection, Model, RelationshipKind, Value};
use std::marker::PhantomData;

type ParentFieldsFn = fn() -> &'static [sqlmodel_core::FieldInfo];

fn sti_discriminator_filter<M: Model>() -> Option<Expr> {
    let inh = M::inheritance();
    match (inh.discriminator_column, inh.discriminator_value) {
        (Some(col), Some(val)) => Some(Expr::qualified(M::TABLE_NAME, col).eq(val)),
        _ => None,
    }
}

fn joined_inheritance_parent<M: Model>() -> Option<(&'static str, ParentFieldsFn)> {
    let inh = M::inheritance();
    if inh.strategy != sqlmodel_core::InheritanceStrategy::Joined {
        return None;
    }
    let parent = inh.parent?;
    let parent_fields_fn = inh.parent_fields_fn?;
    Some((parent, parent_fields_fn))
}

fn joined_inheritance_join<M: Model>() -> Option<Join> {
    let (parent_table, _parent_fields_fn) = joined_inheritance_parent::<M>()?;

    // Join child's PK columns to the parent's PK columns (same names).
    let pks = M::PRIMARY_KEY;
    if pks.is_empty() {
        return None;
    }

    let mut on = Expr::qualified(M::TABLE_NAME, pks[0]).eq(Expr::qualified(parent_table, pks[0]));
    for pk in &pks[1..] {
        on = on.and(Expr::qualified(M::TABLE_NAME, *pk).eq(Expr::qualified(parent_table, *pk)));
    }

    Some(Join::inner(parent_table, on))
}

fn joined_inheritance_select_columns<M: Model>() -> Option<Vec<String>> {
    let (parent_table, parent_fields_fn) = joined_inheritance_parent::<M>()?;

    let child_cols: Vec<&str> = M::fields().iter().map(|f| f.column_name).collect();
    let parent_cols: Vec<&str> = parent_fields_fn().iter().map(|f| f.column_name).collect();

    let mut parts = Vec::new();
    parts.extend(build_aliased_column_parts(M::TABLE_NAME, &child_cols));
    parts.extend(build_aliased_column_parts(parent_table, &parent_cols));
    Some(parts)
}

/// Information about a JOIN for eager loading.
///
/// Used internally to track which relationships are being eagerly loaded
/// and how to hydrate them from the query results.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields used for full hydration (future implementation)
struct EagerJoinInfo {
    /// Name of the relationship field.
    relationship_name: &'static str,
    /// Table name of the related model.
    related_table: &'static str,
    /// Kind of relationship.
    kind: RelationshipKind,
    /// Nested relationships to load.
    nested: Vec<IncludePath>,
}

/// A SELECT query builder.
///
/// Provides a fluent API for building SELECT queries with
/// type-safe column references and conditions.
#[derive(Debug, Clone)]
pub struct Select<M: Model> {
    /// Columns to select (empty = all)
    columns: Vec<String>,
    /// WHERE clause conditions
    where_clause: Option<Where>,
    /// ORDER BY clauses
    order_by: Vec<OrderBy>,
    /// JOIN clauses
    joins: Vec<Join>,
    /// LIMIT clause
    limit: Option<Limit>,
    /// OFFSET clause
    offset: Option<Offset>,
    /// GROUP BY columns
    group_by: Vec<String>,
    /// HAVING clause
    having: Option<Where>,
    /// DISTINCT flag
    distinct: bool,
    /// FOR UPDATE flag
    for_update: bool,
    /// Eager loading configuration
    eager_loader: Option<EagerLoader<M>>,
    /// Model type marker
    _marker: PhantomData<M>,
}

impl<M: Model> Select<M> {
    /// Create a new SELECT query for the model's table.
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            where_clause: None,
            order_by: Vec::new(),
            joins: Vec::new(),
            limit: None,
            offset: None,
            group_by: Vec::new(),
            having: None,
            distinct: false,
            for_update: false,
            eager_loader: None,
            _marker: PhantomData,
        }
    }

    /// Select specific columns.
    pub fn columns(mut self, cols: &[&str]) -> Self {
        self.columns = cols.iter().map(|&s| s.to_string()).collect();
        self
    }

    /// Add a WHERE condition.
    pub fn filter(mut self, expr: Expr) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => existing.and(expr),
            None => Where::new(expr),
        });
        self
    }

    /// Add an OR WHERE condition.
    pub fn or_filter(mut self, expr: Expr) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => existing.or(expr),
            None => Where::new(expr),
        });
        self
    }

    /// Add ORDER BY clause.
    pub fn order_by(mut self, order: OrderBy) -> Self {
        self.order_by.push(order);
        self
    }

    /// Add a JOIN clause.
    pub fn join(mut self, join: Join) -> Self {
        self.joins.push(join);
        self
    }

    /// Set LIMIT.
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(Limit(n));
        self
    }

    /// Set OFFSET.
    pub fn offset(mut self, n: u64) -> Self {
        self.offset = Some(Offset(n));
        self
    }

    /// Add GROUP BY columns.
    pub fn group_by(mut self, cols: &[&str]) -> Self {
        self.group_by.extend(cols.iter().map(|&s| s.to_string()));
        self
    }

    /// Add HAVING condition.
    pub fn having(mut self, expr: Expr) -> Self {
        self.having = Some(match self.having {
            Some(existing) => existing.and(expr),
            None => Where::new(expr),
        });
        self
    }

    /// Make this a DISTINCT query.
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Add FOR UPDATE lock.
    pub fn for_update(mut self) -> Self {
        self.for_update = true;
        self
    }

    /// Configure eager loading for relationships.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let heroes = select!(Hero)
    ///     .eager(EagerLoader::new().include("team"))
    ///     .all_eager(cx, &conn)
    ///     .await?;
    /// ```
    pub fn eager(mut self, loader: EagerLoader<M>) -> Self {
        self.eager_loader = Some(loader);
        self
    }

    /// Build SQL for eager loading with JOINs using a specific dialect.
    ///
    /// Generates SELECT with aliased columns and LEFT JOINs for included relationships.
    #[tracing::instrument(level = "trace", skip(self))]
    fn build_eager_with_dialect(
        &self,
        dialect: Dialect,
    ) -> (String, Vec<Value>, Vec<EagerJoinInfo>) {
        let mut sql = String::new();
        let mut params = Vec::new();
        let mut join_info = Vec::new();
        let mut where_clause = self.where_clause.clone();
        let mut joins = self.joins.clone();

        // Single-table inheritance child models should be implicitly filtered by their discriminator.
        if let Some(expr) = sti_discriminator_filter::<M>() {
            where_clause = Some(match where_clause {
                Some(existing) => existing.and(expr),
                None => Where::new(expr),
            });
        }

        if let Some(join) = joined_inheritance_join::<M>() {
            joins.insert(0, join);
        }

        // Collect parent table columns (database column names).
        let parent_cols: Vec<&str> = M::fields().iter().map(|f| f.column_name).collect();

        // Start with SELECT DISTINCT to avoid duplicates from JOINs
        sql.push_str("SELECT ");
        if self.distinct {
            sql.push_str("DISTINCT ");
        }

        // Build column list with model's table aliased
        let mut col_parts = Vec::new();
        for col in &parent_cols {
            col_parts.push(format!(
                "{}.{} AS {}__{}",
                M::TABLE_NAME,
                col,
                M::TABLE_NAME,
                col
            ));
        }

        // For joined inheritance, also project parent-table columns so `#[sqlmodel(parent)]`
        // hydration can build the embedded parent model from `row.subset_by_prefix(parent_table)`.
        if let Some((parent_table, parent_fields_fn)) = joined_inheritance_parent::<M>() {
            let parent_cols: Vec<&str> = parent_fields_fn().iter().map(|f| f.column_name).collect();
            col_parts.extend(build_aliased_column_parts(parent_table, &parent_cols));
        }

        // Add columns for each eagerly loaded relationship
        if let Some(loader) = &self.eager_loader {
            for include in loader.includes() {
                if let Some(rel) = find_relationship::<M>(include.relationship) {
                    join_info.push(EagerJoinInfo {
                        relationship_name: include.relationship,
                        related_table: rel.related_table,
                        kind: rel.kind,
                        nested: include.nested.clone(),
                    });

                    // Add aliased columns for related table so callers can use
                    // `row.subset_by_prefix(rel.related_table)` deterministically.
                    let related_cols: Vec<&str> = (rel.related_fields_fn)()
                        .iter()
                        .map(|f| f.column_name)
                        .collect();
                    col_parts.extend(build_aliased_column_parts(rel.related_table, &related_cols));
                }
            }
        }

        sql.push_str(&col_parts.join(", "));

        // FROM
        sql.push_str(" FROM ");
        sql.push_str(M::TABLE_NAME);

        // Add JOINs for eager loading
        if let Some(loader) = &self.eager_loader {
            for include in loader.includes() {
                if let Some(rel) = find_relationship::<M>(include.relationship) {
                    let (join_sql, join_params) =
                        build_join_clause(M::TABLE_NAME, rel, params.len());
                    sql.push_str(&join_sql);
                    params.extend(join_params);
                }
            }
        }

        // Additional explicit JOINs (plus joined-inheritance join if present)
        for join in &joins {
            sql.push_str(&join.build_with_dialect(dialect, &mut params, 0));
        }

        // WHERE
        if let Some(where_clause) = &where_clause {
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

        (sql, params, join_info)
    }

    /// Execute the query with eager loading and return hydrated models.
    ///
    /// This method fetches the parent models along with their eagerly loaded
    /// relationships in a single query using JOINs. Results are deduplicated
    /// by primary key to handle one-to-many JOINs.
    ///
    /// # Note
    ///
    /// Currently, this method parses parent models from aliased columns and
    /// deduplicates by primary key. Full hydration of `Related<T>` and
    /// `RelatedMany<T>` fields requires macro support and is tracked
    /// separately. The JOIN query is still valuable as it:
    /// - Fetches all data in a single query (avoiding N+1)
    /// - Returns related data that can be accessed via `row.subset_by_prefix()`
    ///
    /// # Example
    ///
    /// ```ignore
    /// let heroes = select!(Hero)
    ///     .eager(EagerLoader::new().include("team"))
    ///     .all_eager(cx, &conn)
    ///     .await?;
    /// ```
    #[tracing::instrument(level = "debug", skip(self, cx, conn))]
    pub async fn all_eager<C: Connection>(
        self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Vec<M>, sqlmodel_core::Error> {
        // If no eager loading configured, fall back to regular all()
        if !self.eager_loader.as_ref().is_some_and(|e| e.has_includes()) {
            tracing::trace!("No eager loading configured, falling back to regular all()");
            return self.all(cx, conn).await;
        }

        let (sql, params, join_info) = self.build_eager_with_dialect(conn.dialect());

        tracing::debug!(
            table = M::TABLE_NAME,
            includes = join_info.len(),
            "Executing eager loading query"
        );
        tracing::trace!(sql = %sql, "Eager SQL");

        let rows = conn.query(cx, &sql, &params).await;

        rows.and_then(|rows| {
            tracing::debug!(row_count = rows.len(), "Processing eager query results");

            // Use a map to deduplicate by primary key (JOINs can duplicate parent rows)
            let mut seen_pks = std::collections::HashSet::new();
            let mut models = Vec::with_capacity(rows.len());

            for row in &rows {
                // Extract parent columns using table prefix
                let parent_row = row.subset_by_prefix(M::TABLE_NAME);

                // Skip if we can't parse (shouldn't happen with well-formed query)
                if parent_row.is_empty() {
                    tracing::warn!(
                        table = M::TABLE_NAME,
                        "Row has no columns with parent table prefix"
                    );
                    // Fall back to trying the row as-is (backwards compatibility)
                    match M::from_row(row) {
                        Ok(model) => {
                            models.push(model);
                        }
                        Err(e) => {
                            tracing::debug!(error = %e, "Failed to parse model from row");
                            return Outcome::Err(e);
                        }
                    }
                    continue;
                }

                // Parse the parent model from extracted columns
                match M::from_row(&parent_row) {
                    Ok(model) => {
                        // Deduplicate by primary key
                        let pk = model.primary_key_value();
                        let pk_hash = {
                            use std::hash::{Hash, Hasher};
                            let mut hasher = std::collections::hash_map::DefaultHasher::new();
                            // Hash the debug representation as a simple PK identifier
                            format!("{:?}", pk).hash(&mut hasher);
                            hasher.finish()
                        };

                        if seen_pks.insert(pk_hash) {
                            models.push(model);
                        }
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "Failed to parse model from prefixed row");
                        return Outcome::Err(e);
                    }
                }
            }

            tracing::debug!(
                unique_models = models.len(),
                "Eager loading complete (deduplicated)"
            );
            Outcome::Ok(models)
        })
    }

    /// Build the SQL query and parameters.
    pub fn build(&self) -> (String, Vec<Value>) {
        self.build_with_dialect(Dialect::default())
    }

    /// Build the SQL query and parameters with a specific dialect.
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let mut sql = String::new();
        let mut params = Vec::new();
        let mut where_clause = self.where_clause.clone();
        let mut joins = self.joins.clone();

        // Single-table inheritance child models should be implicitly filtered by their discriminator.
        if let Some(expr) = sti_discriminator_filter::<M>() {
            where_clause = Some(match where_clause {
                Some(existing) => existing.and(expr),
                None => Where::new(expr),
            });
        }

        if let Some(join) = joined_inheritance_join::<M>() {
            joins.insert(0, join);
        }

        // SELECT
        sql.push_str("SELECT ");
        if self.distinct {
            sql.push_str("DISTINCT ");
        }

        if let Some(cols) = joined_inheritance_select_columns::<M>() {
            sql.push_str(&cols.join(", "));
        } else if self.columns.is_empty() {
            sql.push('*');
        } else {
            sql.push_str(&self.columns.join(", "));
        }

        // FROM
        sql.push_str(" FROM ");
        sql.push_str(M::TABLE_NAME);

        // JOINs
        for join in &joins {
            sql.push_str(&join.build_with_dialect(dialect, &mut params, 0));
        }

        // WHERE
        if let Some(where_clause) = &where_clause {
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

    /// Convert this SELECT query to an EXISTS expression.
    ///
    /// Creates an `Expr::Exists` that can be used in WHERE clauses of other queries.
    /// For performance, the SELECT is automatically optimized to `SELECT 1` when
    /// generating the EXISTS subquery.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Find customers who have at least one order
    /// let has_orders = Select::<Order>::new()
    ///     .filter(Expr::raw("orders.customer_id = customers.id"))
    ///     .into_exists();
    ///
    /// let customers = Select::<Customer>::new()
    ///     .filter(has_orders)
    ///     .all(cx, &conn)
    ///     .await?;
    ///
    /// // Generates: SELECT * FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)
    /// ```
    pub fn into_exists(self) -> Expr {
        Expr::exists_query(self.into_query())
    }

    /// Convert this SELECT query to an EXISTS expression using a specific dialect.
    ///
    /// Use this when embedding the EXISTS in a query for a non-default dialect.
    pub fn into_exists_with_dialect(self, dialect: Dialect) -> Expr {
        let (sql, params) = self.build_exists_subquery_with_dialect(dialect);
        Expr::exists(sql, params)
    }

    /// Convert this SELECT query to a NOT EXISTS expression.
    ///
    /// Creates an `Expr::Exists` (negated) that can be used in WHERE clauses.
    /// For performance, the SELECT is automatically optimized to `SELECT 1`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Find customers with no orders
    /// let has_no_orders = Select::<Order>::new()
    ///     .filter(Expr::raw("orders.customer_id = customers.id"))
    ///     .into_not_exists();
    ///
    /// let customers = Select::<Customer>::new()
    ///     .filter(has_no_orders)
    ///     .all(cx, &conn)
    ///     .await?;
    ///
    /// // Generates: SELECT * FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)
    /// ```
    pub fn into_not_exists(self) -> Expr {
        Expr::not_exists_query(self.into_query())
    }

    /// Convert this SELECT query to a NOT EXISTS expression using a specific dialect.
    pub fn into_not_exists_with_dialect(self, dialect: Dialect) -> Expr {
        let (sql, params) = self.build_exists_subquery_with_dialect(dialect);
        Expr::not_exists(sql, params)
    }

    /// Convert this SELECT into a LATERAL JOIN.
    ///
    /// Creates a `Join` with `lateral: true` that can be added to another query.
    /// The subquery can reference columns from the outer query.
    ///
    /// Supported by PostgreSQL (9.3+) and MySQL (8.0.14+). Not supported by SQLite.
    ///
    /// # Arguments
    ///
    /// * `alias` - Required alias for the lateral subquery
    /// * `join_type` - The join type (typically `Left` or `Inner`)
    /// * `on` - ON condition (use `Expr::raw("TRUE")` for implicit TRUE)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get top 3 recent orders per customer
    /// let recent_orders = Select::<Order>::new()
    ///     .filter(Expr::raw("orders.customer_id = customers.id"))
    ///     .order_by(OrderBy::desc("date"))
    ///     .limit(3)
    ///     .into_lateral_join("recent_orders", JoinType::Left, Expr::raw("TRUE"));
    ///
    /// let query = Select::<Customer>::new().join(recent_orders);
    /// ```
    pub fn into_lateral_join(
        self,
        alias: impl Into<String>,
        join_type: crate::JoinType,
        on: Expr,
    ) -> crate::Join {
        crate::Join::lateral_query(join_type, self.into_query(), alias, on)
    }

    /// Convert this SELECT into a LATERAL JOIN using a specific dialect.
    pub fn into_lateral_join_with_dialect(
        self,
        alias: impl Into<String>,
        join_type: crate::JoinType,
        on: Expr,
        dialect: Dialect,
    ) -> crate::Join {
        let (sql, params) = self.into_query().build_with_dialect(dialect);
        crate::Join::lateral(join_type, sql, alias, on, params)
    }

    /// Build an optimized EXISTS subquery (SELECT 1 instead of SELECT *).
    fn into_query(self) -> SelectQuery {
        let Select {
            columns,
            where_clause,
            order_by,
            joins,
            limit,
            offset,
            group_by,
            having,
            distinct,
            for_update,
            eager_loader: _,
            _marker: _,
        } = self;

        let mut where_clause = where_clause;
        if let Some(expr) = sti_discriminator_filter::<M>() {
            where_clause = Some(match where_clause {
                Some(existing) => existing.and(expr),
                None => Where::new(expr),
            });
        }

        let mut joins = joins;
        if let Some(join) = joined_inheritance_join::<M>() {
            joins.insert(0, join);
        }

        SelectQuery {
            table: M::TABLE_NAME.to_string(),
            columns,
            where_clause,
            order_by,
            joins,
            limit,
            offset,
            group_by,
            having,
            distinct,
            for_update,
        }
    }

    fn build_exists_subquery_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let mut sql = String::new();
        let mut params = Vec::new();
        let mut where_clause = self.where_clause.clone();
        let mut joins = self.joins.clone();

        if let Some(expr) = sti_discriminator_filter::<M>() {
            where_clause = Some(match where_clause {
                Some(existing) => existing.and(expr),
                None => Where::new(expr),
            });
        }

        if let Some(join) = joined_inheritance_join::<M>() {
            joins.insert(0, join);
        }

        // SELECT 1 for optimal EXISTS performance
        sql.push_str("SELECT 1 FROM ");
        sql.push_str(M::TABLE_NAME);

        // JOINs (if any)
        for join in &joins {
            sql.push_str(&join.build_with_dialect(dialect, &mut params, 0));
        }

        // WHERE
        if let Some(where_clause) = &where_clause {
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

    /// Execute the query and return all matching rows as models.
    pub async fn all<C: Connection>(
        self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Vec<M>, sqlmodel_core::Error> {
        let (sql, params) = self.build_with_dialect(conn.dialect());
        let rows = conn.query(cx, &sql, &params).await;

        rows.and_then(|rows| {
            let mut models = Vec::with_capacity(rows.len());
            for row in &rows {
                match M::from_row(row) {
                    Ok(model) => models.push(model),
                    Err(e) => return Outcome::Err(e),
                }
            }
            Outcome::Ok(models)
        })
    }

    /// Execute the query and return the first matching row.
    pub async fn first<C: Connection>(
        self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Option<M>, sqlmodel_core::Error> {
        let query = self.limit(1);
        let (sql, params) = query.build_with_dialect(conn.dialect());
        let row = conn.query_one(cx, &sql, &params).await;

        row.and_then(|opt_row| match opt_row {
            Some(row) => match M::from_row(&row) {
                Ok(model) => Outcome::Ok(Some(model)),
                Err(e) => Outcome::Err(e),
            },
            None => Outcome::Ok(None),
        })
    }

    /// Execute the query and return exactly one row, or error.
    pub async fn one<C: Connection>(self, cx: &Cx, conn: &C) -> Outcome<M, sqlmodel_core::Error> {
        match self.first(cx, conn).await {
            Outcome::Ok(Some(model)) => Outcome::Ok(model),
            Outcome::Ok(None) => Outcome::Err(sqlmodel_core::Error::Custom(
                "Expected one row, found none".to_string(),
            )),
            Outcome::Err(e) => Outcome::Err(e),
            Outcome::Cancelled(r) => Outcome::Cancelled(r),
            Outcome::Panicked(p) => Outcome::Panicked(p),
        }
    }

    /// Execute the query and return the count of matching rows.
    pub async fn count<C: Connection>(
        self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<u64, sqlmodel_core::Error> {
        let mut count_query = self;
        count_query.columns = vec!["COUNT(*) as count".to_string()];
        count_query.order_by.clear();
        count_query.limit = None;
        count_query.offset = None;

        let (sql, params) = count_query.build_with_dialect(conn.dialect());
        let row = conn.query_one(cx, &sql, &params).await;

        row.and_then(|opt_row| match opt_row {
            Some(row) => match row.get_named::<i64>("count") {
                Ok(count) => Outcome::Ok(count as u64),
                Err(e) => Outcome::Err(e),
            },
            None => Outcome::Ok(0),
        })
    }

    /// Check if any rows match the query.
    pub async fn exists<C: Connection>(
        self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<bool, sqlmodel_core::Error> {
        let count = self.count(cx, conn).await;
        count.map(|n| n > 0)
    }
}

impl<M: Model> Default for Select<M> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JoinType;
    use sqlmodel_core::{
        Error, FieldInfo, InheritanceInfo, InheritanceStrategy, Result, Row, Value,
    };

    #[derive(Debug, Clone)]
    struct Hero;

    impl Model for Hero {
        const TABLE_NAME: &'static str = "heroes";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            &[]
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            Vec::new()
        }

        fn from_row(_row: &Row) -> Result<Self> {
            Err(Error::Custom("not used in tests".to_string()))
        }

        fn primary_key_value(&self) -> Vec<Value> {
            Vec::new()
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    #[derive(Debug, Clone)]
    struct StiManager;

    impl Model for StiManager {
        // STI child shares the physical table.
        const TABLE_NAME: &'static str = "employees";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            &[]
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            Vec::new()
        }

        fn from_row(_row: &Row) -> Result<Self> {
            Err(Error::Custom("not used in tests".to_string()))
        }

        fn primary_key_value(&self) -> Vec<Value> {
            Vec::new()
        }

        fn is_new(&self) -> bool {
            true
        }

        fn inheritance() -> InheritanceInfo {
            InheritanceInfo {
                strategy: InheritanceStrategy::None,
                parent: Some("employees"),
                parent_fields_fn: None,
                discriminator_column: Some("type_"),
                discriminator_value: Some("manager"),
            }
        }
    }

    #[test]
    fn build_collects_params_across_joins_where_having() {
        let query = Select::<Hero>::new()
            .join(Join::inner(
                "teams",
                Expr::qualified("teams", "active").eq(true),
            ))
            .filter(Expr::col("age").gt(18))
            .group_by(&["team_id"])
            .having(Expr::col("count").gt(1));

        let (sql, params) = query.build();

        assert_eq!(
            sql,
            "SELECT * FROM heroes INNER JOIN teams ON \"teams\".\"active\" = $1 WHERE \"age\" > $2 GROUP BY team_id HAVING \"count\" > $3"
        );
        assert_eq!(
            params,
            vec![Value::Bool(true), Value::Int(18), Value::Int(1)]
        );
    }

    #[test]
    fn test_select_all_columns() {
        let query = Select::<Hero>::new();
        let (sql, params) = query.build();

        assert_eq!(sql, "SELECT * FROM heroes");
        assert!(params.is_empty());
    }

    #[test]
    fn test_sti_child_select_adds_discriminator_filter() {
        let query = Select::<StiManager>::new();
        let (sql, params) = query.build();
        assert_eq!(
            sql,
            "SELECT * FROM employees WHERE \"employees\".\"type_\" = $1"
        );
        assert_eq!(params, vec![Value::Text("manager".to_string())]);
    }

    #[test]
    fn test_sti_child_select_ands_discriminator_with_user_filter() {
        let query = Select::<StiManager>::new().filter(Expr::col("active").eq(true));
        let (sql, params) = query.build();
        assert_eq!(
            sql,
            "SELECT * FROM employees WHERE \"active\" = $1 AND \"employees\".\"type_\" = $2"
        );
        assert_eq!(
            params,
            vec![Value::Bool(true), Value::Text("manager".to_string())]
        );
    }

    #[derive(Debug, Clone)]
    struct JoinedParent;

    impl Model for JoinedParent {
        const TABLE_NAME: &'static str = "persons";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", sqlmodel_core::SqlType::BigInt).primary_key(true),
                FieldInfo::new("name", "name", sqlmodel_core::SqlType::Text),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            Vec::new()
        }

        fn from_row(_row: &Row) -> Result<Self> {
            Err(Error::Custom("not used in tests".to_string()))
        }

        fn primary_key_value(&self) -> Vec<Value> {
            Vec::new()
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    #[derive(Debug, Clone)]
    struct JoinedChild;

    impl Model for JoinedChild {
        const TABLE_NAME: &'static str = "employees";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", sqlmodel_core::SqlType::BigInt).primary_key(true),
                FieldInfo::new("dept", "department", sqlmodel_core::SqlType::Text),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            Vec::new()
        }

        fn from_row(_row: &Row) -> Result<Self> {
            Err(Error::Custom("not used in tests".to_string()))
        }

        fn primary_key_value(&self) -> Vec<Value> {
            Vec::new()
        }

        fn is_new(&self) -> bool {
            true
        }

        fn inheritance() -> InheritanceInfo {
            InheritanceInfo {
                strategy: InheritanceStrategy::Joined,
                parent: Some("persons"),
                parent_fields_fn: Some(<JoinedParent as Model>::fields),
                discriminator_column: None,
                discriminator_value: None,
            }
        }
    }

    #[test]
    fn test_joined_inheritance_child_select_projects_parent_and_joins() {
        let query = Select::<JoinedChild>::new();
        let (sql, params) = query.build();

        assert!(params.is_empty());
        assert!(sql.starts_with("SELECT "));
        assert!(sql.contains("employees.id AS employees__id"));
        assert!(sql.contains("employees.department AS employees__department"));
        assert!(sql.contains("persons.id AS persons__id"));
        assert!(sql.contains("persons.name AS persons__name"));
        assert!(sql.contains(
            "FROM employees INNER JOIN persons ON \"employees\".\"id\" = \"persons\".\"id\""
        ));
    }

    #[test]
    fn test_select_specific_columns() {
        let query = Select::<Hero>::new().columns(&["id", "name", "power"]);
        let (sql, params) = query.build();

        assert_eq!(sql, "SELECT id, name, power FROM heroes");
        assert!(params.is_empty());
    }

    #[test]
    fn test_select_distinct() {
        let query = Select::<Hero>::new().columns(&["team_id"]).distinct();
        let (sql, params) = query.build();

        assert_eq!(sql, "SELECT DISTINCT team_id FROM heroes");
        assert!(params.is_empty());
    }

    #[test]
    fn test_select_with_simple_filter() {
        let query = Select::<Hero>::new().filter(Expr::col("active").eq(true));
        let (sql, params) = query.build();

        assert_eq!(sql, "SELECT * FROM heroes WHERE \"active\" = $1");
        assert_eq!(params, vec![Value::Bool(true)]);
    }

    #[test]
    fn test_select_with_multiple_and_filters() {
        let query = Select::<Hero>::new()
            .filter(Expr::col("active").eq(true))
            .filter(Expr::col("age").gt(18));
        let (sql, params) = query.build();

        assert_eq!(
            sql,
            "SELECT * FROM heroes WHERE \"active\" = $1 AND \"age\" > $2"
        );
        assert_eq!(params, vec![Value::Bool(true), Value::Int(18)]);
    }

    #[test]
    fn test_select_with_or_filter() {
        let query = Select::<Hero>::new()
            .filter(Expr::col("role").eq("warrior"))
            .or_filter(Expr::col("role").eq("mage"));
        let (sql, params) = query.build();

        assert_eq!(
            sql,
            "SELECT * FROM heroes WHERE \"role\" = $1 OR \"role\" = $2"
        );
        assert_eq!(
            params,
            vec![
                Value::Text("warrior".to_string()),
                Value::Text("mage".to_string())
            ]
        );
    }

    #[test]
    fn test_select_with_order_by_asc() {
        let query = Select::<Hero>::new().order_by(OrderBy::asc(Expr::col("name")));
        let (sql, params) = query.build();

        assert_eq!(sql, "SELECT * FROM heroes ORDER BY \"name\" ASC");
        assert!(params.is_empty());
    }

    #[test]
    fn test_select_with_order_by_desc() {
        let query = Select::<Hero>::new().order_by(OrderBy::desc(Expr::col("created_at")));
        let (sql, params) = query.build();

        assert_eq!(sql, "SELECT * FROM heroes ORDER BY \"created_at\" DESC");
        assert!(params.is_empty());
    }

    #[test]
    fn test_select_with_multiple_order_by() {
        let query = Select::<Hero>::new()
            .order_by(OrderBy::asc(Expr::col("team_id")))
            .order_by(OrderBy::asc(Expr::col("name")));
        let (sql, params) = query.build();

        assert_eq!(
            sql,
            "SELECT * FROM heroes ORDER BY \"team_id\" ASC, \"name\" ASC"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_select_with_limit() {
        let query = Select::<Hero>::new().limit(10);
        let (sql, params) = query.build();

        assert_eq!(sql, "SELECT * FROM heroes LIMIT 10");
        assert!(params.is_empty());
    }

    #[test]
    fn test_select_with_offset() {
        let query = Select::<Hero>::new().offset(20);
        let (sql, params) = query.build();

        assert_eq!(sql, "SELECT * FROM heroes OFFSET 20");
        assert!(params.is_empty());
    }

    #[test]
    fn test_select_with_limit_and_offset() {
        let query = Select::<Hero>::new().limit(10).offset(20);
        let (sql, params) = query.build();

        assert_eq!(sql, "SELECT * FROM heroes LIMIT 10 OFFSET 20");
        assert!(params.is_empty());
    }

    #[test]
    fn test_select_with_group_by() {
        let query = Select::<Hero>::new()
            .columns(&["team_id", "COUNT(*) as count"])
            .group_by(&["team_id"]);
        let (sql, params) = query.build();

        assert_eq!(
            sql,
            "SELECT team_id, COUNT(*) as count FROM heroes GROUP BY team_id"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_select_with_multiple_group_by() {
        let query = Select::<Hero>::new()
            .columns(&["team_id", "role", "COUNT(*) as count"])
            .group_by(&["team_id", "role"]);
        let (sql, params) = query.build();

        assert_eq!(
            sql,
            "SELECT team_id, role, COUNT(*) as count FROM heroes GROUP BY team_id, role"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_select_with_for_update() {
        let query = Select::<Hero>::new()
            .filter(Expr::col("id").eq(1))
            .for_update();
        let (sql, params) = query.build();

        assert_eq!(sql, "SELECT * FROM heroes WHERE \"id\" = $1 FOR UPDATE");
        assert_eq!(params, vec![Value::Int(1)]);
    }

    #[test]
    fn test_select_inner_join() {
        let query = Select::<Hero>::new().join(Join::inner(
            "teams",
            Expr::qualified("heroes", "team_id").eq(Expr::qualified("teams", "id")),
        ));
        let (sql, _) = query.build();

        assert!(sql.contains("INNER JOIN teams ON"));
    }

    #[test]
    fn test_select_left_join() {
        let query = Select::<Hero>::new().join(Join::left(
            "teams",
            Expr::qualified("heroes", "team_id").eq(Expr::qualified("teams", "id")),
        ));
        let (sql, _) = query.build();

        assert!(sql.contains("LEFT JOIN teams ON"));
    }

    #[test]
    fn test_select_right_join() {
        let query = Select::<Hero>::new().join(Join::right(
            "teams",
            Expr::qualified("heroes", "team_id").eq(Expr::qualified("teams", "id")),
        ));
        let (sql, _) = query.build();

        assert!(sql.contains("RIGHT JOIN teams ON"));
    }

    #[test]
    fn test_select_multiple_joins() {
        let query = Select::<Hero>::new()
            .join(Join::inner(
                "teams",
                Expr::qualified("heroes", "team_id").eq(Expr::qualified("teams", "id")),
            ))
            .join(Join::left(
                "powers",
                Expr::qualified("heroes", "id").eq(Expr::qualified("powers", "hero_id")),
            ));
        let (sql, _) = query.build();

        assert!(sql.contains("INNER JOIN teams ON"));
        assert!(sql.contains("LEFT JOIN powers ON"));
    }

    #[test]
    fn test_select_complex_query() {
        let query = Select::<Hero>::new()
            .columns(&["heroes.id", "heroes.name", "teams.name as team_name"])
            .distinct()
            .join(Join::inner(
                "teams",
                Expr::qualified("heroes", "team_id").eq(Expr::qualified("teams", "id")),
            ))
            .filter(Expr::col("active").eq(true))
            .filter(Expr::col("level").gt(10))
            .group_by(&["heroes.id", "heroes.name", "teams.name"])
            .having(Expr::col("score").gt(100))
            .order_by(OrderBy::desc(Expr::col("level")))
            .limit(50)
            .offset(0);
        let (sql, params) = query.build();

        assert!(sql.starts_with(
            "SELECT DISTINCT heroes.id, heroes.name, teams.name as team_name FROM heroes"
        ));
        assert!(sql.contains("INNER JOIN teams ON"));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("HAVING"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("LIMIT 50"));
        assert!(sql.contains("OFFSET 0"));

        // Params: true (active), 10 (level), 100 (score)
        // Note: join condition uses column comparison, not value param
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_select_default() {
        let query = Select::<Hero>::default();
        let (sql, _) = query.build();
        assert_eq!(sql, "SELECT * FROM heroes");
    }

    #[test]
    fn test_select_clone() {
        let query = Select::<Hero>::new()
            .filter(Expr::col("id").eq(1))
            .limit(10);
        let cloned = query.clone();

        let (sql1, params1) = query.build();
        let (sql2, params2) = cloned.build();

        assert_eq!(sql1, sql2);
        assert_eq!(params1, params2);
    }

    // ========================================================================
    // Eager Loading Tests
    // ========================================================================

    use sqlmodel_core::RelationshipInfo;

    /// A test team model for eager loading column projection.
    #[derive(Debug, Clone)]
    struct EagerTeam;

    impl Model for EagerTeam {
        const TABLE_NAME: &'static str = "teams";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", sqlmodel_core::SqlType::BigInt),
                FieldInfo::new("name", "name", sqlmodel_core::SqlType::Text),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            Vec::new()
        }

        fn from_row(_row: &Row) -> Result<Self> {
            Err(Error::Custom("not used in tests".to_string()))
        }

        fn primary_key_value(&self) -> Vec<Value> {
            Vec::new()
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    /// A test hero model with relationships defined.
    #[derive(Debug, Clone)]
    struct EagerHero;

    impl Model for EagerHero {
        const TABLE_NAME: &'static str = "heroes";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];
        const RELATIONSHIPS: &'static [RelationshipInfo] =
            &[
                RelationshipInfo::new("team", "teams", RelationshipKind::ManyToOne)
                    .related_fields(EagerTeam::fields)
                    .local_key("team_id"),
            ];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", sqlmodel_core::SqlType::BigInt),
                FieldInfo::new("name", "name", sqlmodel_core::SqlType::Text),
                FieldInfo::new("team_id", "team_id", sqlmodel_core::SqlType::BigInt),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            Vec::new()
        }

        fn from_row(_row: &Row) -> Result<Self> {
            Err(Error::Custom("not used in tests".to_string()))
        }

        fn primary_key_value(&self) -> Vec<Value> {
            Vec::new()
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_select_with_eager_loader() {
        let loader = EagerLoader::<EagerHero>::new().include("team");
        let query = Select::<EagerHero>::new().eager(loader);

        // Verify eager_loader is set
        assert!(query.eager_loader.is_some());
        assert!(query.eager_loader.as_ref().unwrap().has_includes());
    }

    #[test]
    fn test_select_eager_generates_join() {
        let loader = EagerLoader::<EagerHero>::new().include("team");
        let query = Select::<EagerHero>::new().eager(loader);

        let (sql, params, join_info) = query.build_eager_with_dialect(Dialect::default());

        // Should have LEFT JOIN for team relationship
        assert!(sql.contains("LEFT JOIN teams"));
        assert!(sql.contains("heroes.team_id = teams.id"));

        // Should have aliased columns for parent table
        assert!(sql.contains("heroes.id AS heroes__id"));
        assert!(sql.contains("heroes.name AS heroes__name"));
        assert!(sql.contains("heroes.team_id AS heroes__team_id"));

        // Should have aliased columns for related table (so subset_by_prefix works)
        assert!(sql.contains("teams.id AS teams__id"));
        assert!(sql.contains("teams.name AS teams__name"));

        // Should have join info
        assert_eq!(join_info.len(), 1);
        assert!(params.is_empty());
    }

    #[test]
    fn test_select_eager_with_filter() {
        let loader = EagerLoader::<EagerHero>::new().include("team");
        let query = Select::<EagerHero>::new()
            .eager(loader)
            .filter(Expr::col("active").eq(true));

        let (sql, params, _) = query.build_eager_with_dialect(Dialect::default());

        assert!(sql.contains("LEFT JOIN teams"));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("\"active\" = $1"));
        assert_eq!(params, vec![Value::Bool(true)]);
    }

    #[test]
    fn test_select_eager_with_order_and_limit() {
        let loader = EagerLoader::<EagerHero>::new().include("team");
        let query = Select::<EagerHero>::new()
            .eager(loader)
            .order_by(OrderBy::asc(Expr::col("name")))
            .limit(10)
            .offset(5);

        let (sql, _, _) = query.build_eager_with_dialect(Dialect::default());

        assert!(sql.contains("LEFT JOIN teams"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("OFFSET 5"));
    }

    #[test]
    fn test_select_eager_no_includes_fallback() {
        // Eager loader with no includes
        let loader = EagerLoader::<EagerHero>::new();
        let query = Select::<EagerHero>::new().eager(loader);

        // all_eager should fall back to regular all() when no includes
        // We can't test async execution here, but we can verify the state
        assert!(query.eager_loader.is_some());
        assert!(!query.eager_loader.as_ref().unwrap().has_includes());
    }

    #[test]
    fn test_select_eager_distinct() {
        let loader = EagerLoader::<EagerHero>::new().include("team");
        let query = Select::<EagerHero>::new().eager(loader).distinct();

        let (sql, _, _) = query.build_eager_with_dialect(Dialect::default());

        assert!(sql.starts_with("SELECT DISTINCT"));
    }

    // ==================== EXISTS Tests ====================

    #[test]
    fn test_select_into_exists() {
        // Convert a SELECT query into an EXISTS expression
        let exists_expr = Select::<Hero>::new()
            .filter(Expr::raw("orders.customer_id = customers.id"))
            .into_exists();

        let mut params = Vec::new();
        let sql = exists_expr.build(&mut params, 0);

        // Should generate EXISTS (SELECT 1 FROM heroes WHERE ...)
        assert_eq!(
            sql,
            "EXISTS (SELECT 1 FROM heroes WHERE orders.customer_id = customers.id)"
        );
    }

    #[test]
    fn test_select_into_not_exists() {
        // Convert a SELECT query into a NOT EXISTS expression
        let not_exists_expr = Select::<Hero>::new()
            .filter(Expr::raw("orders.customer_id = customers.id"))
            .into_not_exists();

        let mut params = Vec::new();
        let sql = not_exists_expr.build(&mut params, 0);

        assert_eq!(
            sql,
            "NOT EXISTS (SELECT 1 FROM heroes WHERE orders.customer_id = customers.id)"
        );
    }

    #[test]
    fn test_select_into_exists_with_params() {
        // EXISTS subquery with bound parameters
        let exists_expr = Select::<Hero>::new()
            .filter(Expr::col("status").eq("active"))
            .into_exists();

        let mut params = Vec::new();
        let sql = exists_expr.build(&mut params, 0);

        assert_eq!(sql, "EXISTS (SELECT 1 FROM heroes WHERE \"status\" = $1)");
        assert_eq!(params.len(), 1);
        assert_eq!(params[0], Value::Text("active".to_string()));
    }

    #[test]
    fn test_select_into_exists_propagates_dialect_mysql() {
        let exists_expr = Select::<Hero>::new()
            .filter(Expr::col("status").eq("active"))
            .into_exists();

        let mut params = Vec::new();
        let sql = exists_expr.build_with_dialect(Dialect::Mysql, &mut params, 0);

        assert_eq!(sql, "EXISTS (SELECT 1 FROM heroes WHERE `status` = ?)");
        assert_eq!(params, vec![Value::Text("active".to_string())]);
    }

    #[test]
    fn test_select_into_exists_with_join() {
        // EXISTS subquery with JOIN
        let exists_expr = Select::<Hero>::new()
            .join(Join::inner(
                "teams",
                Expr::qualified("heroes", "team_id").eq(Expr::qualified("teams", "id")),
            ))
            .filter(Expr::col("active").eq(true))
            .into_exists();

        let mut params = Vec::new();
        let sql = exists_expr.build(&mut params, 0);

        assert!(sql.starts_with("EXISTS (SELECT 1 FROM heroes"));
        assert!(sql.contains("INNER JOIN teams ON"));
        assert!(sql.contains("WHERE"));
    }

    #[test]
    fn test_select_into_exists_omits_order_by_limit() {
        // ORDER BY, LIMIT, OFFSET should be omitted from EXISTS subquery
        // as they have no effect and add unnecessary overhead
        let exists_expr = Select::<Hero>::new()
            .filter(Expr::col("active").eq(true))
            .order_by(OrderBy::asc(Expr::col("name")))
            .limit(10)
            .offset(5)
            .into_exists();

        let mut params = Vec::new();
        let sql = exists_expr.build(&mut params, 0);

        // Should NOT contain ORDER BY, LIMIT, OFFSET
        assert!(!sql.contains("ORDER BY"));
        assert!(!sql.contains("LIMIT"));
        assert!(!sql.contains("OFFSET"));
        assert_eq!(sql, "EXISTS (SELECT 1 FROM heroes WHERE \"active\" = $1)");
    }

    #[test]
    fn test_exists_in_outer_query() {
        // Use EXISTS expression in a WHERE clause of another query
        let has_heroes = Select::<Hero>::new()
            .filter(Expr::raw("heroes.team_id = teams.id"))
            .into_exists();

        let query = Select::<EagerTeam>::new().filter(Expr::col("active").eq(true).and(has_heroes));
        let (sql, params) = query.build_with_dialect(Dialect::default());

        assert_eq!(
            sql,
            "SELECT * FROM teams WHERE \"active\" = $1 AND EXISTS (SELECT 1 FROM heroes WHERE heroes.team_id = teams.id)"
        );
        assert_eq!(params, vec![Value::Bool(true)]);
    }

    #[test]
    fn test_lateral_join_propagates_dialect_sqlite() {
        let lateral = Select::<Hero>::new()
            .filter(Expr::col("status").eq("active"))
            .into_lateral_join("recent", JoinType::Left, Expr::raw("TRUE"));

        let query = Select::<Hero>::new()
            .filter(Expr::col("active").eq(true))
            .join(lateral);

        let (sql, params) = query.build_with_dialect(Dialect::Sqlite);

        assert!(sql.contains(
            "LEFT JOIN LATERAL (SELECT * FROM heroes WHERE \"status\" = ?1) AS recent ON TRUE"
        ));
        assert!(sql.contains("WHERE \"active\" = ?2"));
        assert_eq!(params.len(), 2);
        assert_eq!(params[0], Value::Text("active".to_string()));
        assert_eq!(params[1], Value::Bool(true));
    }
}
