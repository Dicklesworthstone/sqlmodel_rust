//! Flush operation ordering and batching for SQLModel Session.
//!
//! This module handles writing pending changes to the database in the correct order:
//! - DELETE child-first (to respect FK constraints)
//! - INSERT parent-first (to respect FK constraints)
//! - UPDATE any order (no circular FK assumed)
//!
//! Operations are batched by table for performance.

use crate::ObjectKey;
use asupersync::{Cx, Outcome};
use sqlmodel_core::{Connection, Error, Model, Value, quote_ident};
use std::collections::HashMap;

/// A pending database operation.
#[derive(Debug, Clone)]
pub enum PendingOp {
    /// Insert a new row.
    Insert {
        /// Object key for identity map.
        key: ObjectKey,
        /// Table name.
        table: &'static str,
        /// Column names.
        columns: Vec<&'static str>,
        /// Values to insert.
        values: Vec<Value>,
    },
    /// Update an existing row.
    Update {
        /// Object key for identity map.
        key: ObjectKey,
        /// Table name.
        table: &'static str,
        /// Primary key column names.
        pk_columns: Vec<&'static str>,
        /// Primary key values.
        pk_values: Vec<Value>,
        /// Columns to update (only dirty ones).
        set_columns: Vec<&'static str>,
        /// New values for dirty columns.
        set_values: Vec<Value>,
    },
    /// Delete an existing row.
    Delete {
        /// Object key for identity map.
        key: ObjectKey,
        /// Table name.
        table: &'static str,
        /// Primary key column names.
        pk_columns: Vec<&'static str>,
        /// Primary key values.
        pk_values: Vec<Value>,
    },
}

/// A pending link table operation (for many-to-many relationships).
#[derive(Debug, Clone)]
pub enum LinkTableOp {
    /// Insert a link (relationship).
    Link {
        /// Link table name.
        table: String,
        /// Local (parent) column names.
        local_columns: Vec<String>,
        /// Local (parent) PK values (must match local_columns).
        local_values: Vec<Value>,
        /// Remote (child) column names.
        remote_columns: Vec<String>,
        /// Remote (child) PK values (must match remote_columns).
        remote_values: Vec<Value>,
    },
    /// Delete a link (relationship).
    Unlink {
        /// Link table name.
        table: String,
        /// Local (parent) column names.
        local_columns: Vec<String>,
        /// Local (parent) PK values (must match local_columns).
        local_values: Vec<Value>,
        /// Remote (child) column names.
        remote_columns: Vec<String>,
        /// Remote (child) PK values (must match remote_columns).
        remote_values: Vec<Value>,
    },
}

impl LinkTableOp {
    /// Create a link operation.
    pub fn link(
        table: impl Into<String>,
        local_column: impl Into<String>,
        local_value: Value,
        remote_column: impl Into<String>,
        remote_value: Value,
    ) -> Self {
        Self::link_multi(
            table,
            vec![local_column.into()],
            vec![local_value],
            vec![remote_column.into()],
            vec![remote_value],
        )
    }

    /// Create an unlink operation.
    pub fn unlink(
        table: impl Into<String>,
        local_column: impl Into<String>,
        local_value: Value,
        remote_column: impl Into<String>,
        remote_value: Value,
    ) -> Self {
        Self::unlink_multi(
            table,
            vec![local_column.into()],
            vec![local_value],
            vec![remote_column.into()],
            vec![remote_value],
        )
    }

    /// Create a link operation for composite keys.
    pub fn link_multi(
        table: impl Into<String>,
        local_columns: Vec<String>,
        local_values: Vec<Value>,
        remote_columns: Vec<String>,
        remote_values: Vec<Value>,
    ) -> Self {
        Self::Link {
            table: table.into(),
            local_columns,
            local_values,
            remote_columns,
            remote_values,
        }
    }

    /// Create an unlink operation for composite keys.
    pub fn unlink_multi(
        table: impl Into<String>,
        local_columns: Vec<String>,
        local_values: Vec<Value>,
        remote_columns: Vec<String>,
        remote_values: Vec<Value>,
    ) -> Self {
        Self::Unlink {
            table: table.into(),
            local_columns,
            local_values,
            remote_columns,
            remote_values,
        }
    }

    /// Get the table name.
    pub fn table(&self) -> &str {
        match self {
            LinkTableOp::Link { table, .. } => table,
            LinkTableOp::Unlink { table, .. } => table,
        }
    }

    /// Check if this is a link (insert) operation.
    pub fn is_link(&self) -> bool {
        matches!(self, LinkTableOp::Link { .. })
    }

    /// Check if this is an unlink (delete) operation.
    pub fn is_unlink(&self) -> bool {
        matches!(self, LinkTableOp::Unlink { .. })
    }

    /// Generate the SQL that would be executed for this operation.
    ///
    /// Useful for testing and debugging.
    pub fn to_sql(&self) -> String {
        match self {
            LinkTableOp::Link {
                table,
                local_columns,
                remote_columns,
                ..
            } => format!(
                "INSERT INTO {} ({}) VALUES ({})",
                quote_ident(table),
                local_columns
                    .iter()
                    .chain(remote_columns.iter())
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", "),
                (1..=(local_columns.len() + remote_columns.len()))
                    .map(|i| format!("${}", i))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            LinkTableOp::Unlink {
                table,
                local_columns,
                remote_columns,
                ..
            } => format!(
                "DELETE FROM {} WHERE {}",
                quote_ident(table),
                local_columns
                    .iter()
                    .chain(remote_columns.iter())
                    .enumerate()
                    .map(|(i, c)| format!("{} = ${}", quote_ident(c), i + 1))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
        }
    }

    /// Execute this link table operation.
    #[tracing::instrument(level = "debug", skip(cx, conn))]
    pub async fn execute<C: Connection>(&self, cx: &Cx, conn: &C) -> Outcome<(), Error> {
        let dialect = conn.dialect();
        match self {
            LinkTableOp::Link {
                table,
                local_columns,
                local_values,
                remote_columns,
                remote_values,
            } => {
                if local_columns.len() != local_values.len()
                    || remote_columns.len() != remote_values.len()
                {
                    return Outcome::Err(Error::Custom(
                        "link op columns/values length mismatch".to_string(),
                    ));
                }

                let mut params: Vec<Value> =
                    Vec::with_capacity(local_values.len() + remote_values.len());
                params.extend(local_values.iter().cloned());
                params.extend(remote_values.iter().cloned());

                let col_list = local_columns
                    .iter()
                    .chain(remote_columns.iter())
                    .map(|c| dialect.quote_identifier(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                let placeholders = (1..=params.len())
                    .map(|i| dialect.placeholder(i))
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    dialect.quote_identifier(table),
                    col_list,
                    placeholders
                );
                tracing::trace!(sql = %sql, "Executing link INSERT");
                conn.execute(cx, &sql, &params).await.map(|_| ())
            }
            LinkTableOp::Unlink {
                table,
                local_columns,
                local_values,
                remote_columns,
                remote_values,
            } => {
                if local_columns.len() != local_values.len()
                    || remote_columns.len() != remote_values.len()
                {
                    return Outcome::Err(Error::Custom(
                        "unlink op columns/values length mismatch".to_string(),
                    ));
                }

                let mut params: Vec<Value> =
                    Vec::with_capacity(local_values.len() + remote_values.len());
                params.extend(local_values.iter().cloned());
                params.extend(remote_values.iter().cloned());

                let where_clause = local_columns
                    .iter()
                    .chain(remote_columns.iter())
                    .enumerate()
                    .map(|(i, c)| {
                        format!(
                            "{} = {}",
                            dialect.quote_identifier(c),
                            dialect.placeholder(i + 1)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" AND ");
                let sql = format!(
                    "DELETE FROM {} WHERE {}",
                    dialect.quote_identifier(table),
                    where_clause
                );
                tracing::trace!(sql = %sql, "Executing link DELETE");
                conn.execute(cx, &sql, &params).await.map(|_| ())
            }
        }
    }
}

/// Execute a batch of link table operations.
#[tracing::instrument(level = "debug", skip(cx, conn, ops))]
pub async fn execute_link_table_ops<C: Connection>(
    cx: &Cx,
    conn: &C,
    ops: &[LinkTableOp],
) -> Outcome<usize, Error> {
    if ops.is_empty() {
        return Outcome::Ok(0);
    }

    tracing::info!(count = ops.len(), "Executing link table operations");

    let mut count = 0;
    for op in ops {
        match op.execute(cx, conn).await {
            Outcome::Ok(()) => count += 1,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        }
    }

    tracing::debug!(executed = count, "Link table operations complete");
    Outcome::Ok(count)
}

impl PendingOp {
    /// Get the table name for this operation.
    pub fn table(&self) -> &'static str {
        match self {
            PendingOp::Insert { table, .. } => table,
            PendingOp::Update { table, .. } => table,
            PendingOp::Delete { table, .. } => table,
        }
    }

    /// Get the object key for this operation.
    pub fn key(&self) -> ObjectKey {
        match self {
            PendingOp::Insert { key, .. } => *key,
            PendingOp::Update { key, .. } => *key,
            PendingOp::Delete { key, .. } => *key,
        }
    }

    /// Check if this is an insert operation.
    pub fn is_insert(&self) -> bool {
        matches!(self, PendingOp::Insert { .. })
    }

    /// Check if this is an update operation.
    pub fn is_update(&self) -> bool {
        matches!(self, PendingOp::Update { .. })
    }

    /// Check if this is a delete operation.
    pub fn is_delete(&self) -> bool {
        matches!(self, PendingOp::Delete { .. })
    }

    /// Generate the SQL that would be executed for this operation.
    ///
    /// This is useful for testing and debugging. For INSERT, this generates
    /// a single-row insert. For DELETE/UPDATE, the SQL matches what would be
    /// executed for a single operation.
    ///
    /// Returns a descriptive error string for invalid operations (e.g., empty
    /// pk_columns for DELETE/UPDATE, empty set_columns for UPDATE).
    pub fn to_sql(&self) -> String {
        match self {
            PendingOp::Insert {
                table,
                columns,
                values,
                ..
            } => {
                if columns.is_empty() {
                    return format!(
                        "-- ERROR: INSERT INTO {} with no columns",
                        quote_ident(table)
                    );
                }
                let col_list: String = columns
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                let placeholders: Vec<String> =
                    (1..=values.len()).map(|i| format!("${}", i)).collect();
                format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    quote_ident(table),
                    col_list,
                    placeholders.join(", ")
                )
            }
            PendingOp::Delete {
                table, pk_columns, ..
            } => {
                if pk_columns.is_empty() {
                    return format!(
                        "-- ERROR: DELETE FROM {} with no pk_columns",
                        quote_ident(table)
                    );
                }
                if pk_columns.len() == 1 {
                    format!(
                        "DELETE FROM {} WHERE {} IN ($1)",
                        quote_ident(table),
                        quote_ident(pk_columns[0])
                    )
                } else {
                    let where_clause: String = pk_columns
                        .iter()
                        .enumerate()
                        .map(|(i, col)| format!("{} = ${}", quote_ident(col), i + 1))
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    format!("DELETE FROM {} WHERE {}", quote_ident(table), where_clause)
                }
            }
            PendingOp::Update {
                table,
                pk_columns,
                set_columns,
                ..
            } => {
                if pk_columns.is_empty() {
                    return format!("-- ERROR: UPDATE {} with no pk_columns", quote_ident(table));
                }
                if set_columns.is_empty() {
                    return format!(
                        "-- ERROR: UPDATE {} with no set_columns",
                        quote_ident(table)
                    );
                }
                let mut param_idx = 1;
                let set_clause: String = set_columns
                    .iter()
                    .map(|col| {
                        let s = format!("{} = ${}", quote_ident(col), param_idx);
                        param_idx += 1;
                        s
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let where_clause: String = pk_columns
                    .iter()
                    .map(|col| {
                        let s = format!("{} = ${}", quote_ident(col), param_idx);
                        param_idx += 1;
                        s
                    })
                    .collect::<Vec<_>>()
                    .join(" AND ");
                format!(
                    "UPDATE {} SET {} WHERE {}",
                    quote_ident(table),
                    set_clause,
                    where_clause
                )
            }
        }
    }
}

/// Builds a dependency graph and orders operations for flush.
///
/// Uses table foreign key relationships to determine correct ordering:
/// - Parents must be inserted before children
/// - Children must be deleted before parents
#[derive(Debug, Default)]
pub struct FlushOrderer {
    /// Table -> tables it depends on (has FK to).
    dependencies: HashMap<&'static str, Vec<&'static str>>,
}

impl FlushOrderer {
    /// Create a new flush orderer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a model type's dependencies.
    ///
    /// Extracts foreign key relationships from the model's field metadata.
    pub fn register_model<T: Model>(&mut self) {
        let table = T::TABLE_NAME;
        let deps: Vec<&'static str> = T::fields()
            .iter()
            .filter_map(|f| f.foreign_key)
            .filter_map(|fk| fk.split('.').next())
            .collect();
        self.dependencies.insert(table, deps);
    }

    /// Register a table's dependencies directly.
    pub fn register_table(&mut self, table: &'static str, depends_on: Vec<&'static str>) {
        self.dependencies.insert(table, depends_on);
    }

    /// Get the dependency count for a table.
    fn dependency_count(&self, table: &str) -> usize {
        self.dependencies.get(table).map_or(0, Vec::len)
    }

    /// Order operations into a flush plan.
    ///
    /// Returns operations grouped and sorted:
    /// - Deletes: child-first (more dependencies = delete first)
    /// - Inserts: parent-first (fewer dependencies = insert first)
    /// - Updates: any order
    pub fn order(&self, ops: Vec<PendingOp>) -> FlushPlan {
        let mut deletes = Vec::new();
        let mut inserts = Vec::new();
        let mut updates = Vec::new();

        for op in ops {
            match op {
                PendingOp::Delete { .. } => deletes.push(op),
                PendingOp::Insert { .. } => inserts.push(op),
                PendingOp::Update { .. } => updates.push(op),
            }
        }

        // Sort deletes: children first (more deps = delete first)
        deletes.sort_by(|a, b| {
            let a_deps = self.dependency_count(a.table());
            let b_deps = self.dependency_count(b.table());
            b_deps.cmp(&a_deps)
        });

        // Sort inserts: parents first (fewer deps = insert first)
        inserts.sort_by(|a, b| {
            let a_deps = self.dependency_count(a.table());
            let b_deps = self.dependency_count(b.table());
            a_deps.cmp(&b_deps)
        });

        FlushPlan {
            deletes,
            inserts,
            updates,
        }
    }
}

/// A plan for executing flush operations.
#[derive(Debug, Default)]
pub struct FlushPlan {
    /// Delete operations (ordered child-first).
    pub deletes: Vec<PendingOp>,
    /// Insert operations (ordered parent-first).
    pub inserts: Vec<PendingOp>,
    /// Update operations (any order).
    pub updates: Vec<PendingOp>,
}

impl FlushPlan {
    /// Create an empty flush plan.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if the plan has any operations.
    pub fn is_empty(&self) -> bool {
        self.deletes.is_empty() && self.inserts.is_empty() && self.updates.is_empty()
    }

    /// Total number of operations in the plan.
    pub fn len(&self) -> usize {
        self.deletes.len() + self.inserts.len() + self.updates.len()
    }

    /// Execute the flush plan against the database.
    #[tracing::instrument(level = "info", skip(self, cx, conn))]
    pub async fn execute<C: Connection>(&self, cx: &Cx, conn: &C) -> Outcome<FlushResult, Error> {
        tracing::info!(
            deletes = self.deletes.len(),
            inserts = self.inserts.len(),
            updates = self.updates.len(),
            "Executing flush plan"
        );

        let start = std::time::Instant::now();
        let mut result = FlushResult::default();

        // 1. Execute deletes (batched by table)
        for batch in Self::batch_by_table(&self.deletes) {
            match Self::execute_delete_batch(cx, conn, &batch).await {
                Outcome::Ok(count) => result.deleted += count,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            }
        }

        // 2. Execute inserts (batched by table)
        for batch in Self::batch_by_table(&self.inserts) {
            match Self::execute_insert_batch(cx, conn, &batch).await {
                Outcome::Ok(count) => result.inserted += count,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            }
        }

        // 3. Execute updates (one at a time - different columns may be dirty)
        for op in &self.updates {
            match Self::execute_update(cx, conn, op).await {
                Outcome::Ok(()) => result.updated += 1,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            }
        }

        tracing::info!(
            elapsed_ms = start.elapsed().as_millis(),
            inserted = result.inserted,
            updated = result.updated,
            deleted = result.deleted,
            "Flush complete"
        );

        Outcome::Ok(result)
    }

    /// Group operations by table name.
    fn batch_by_table(ops: &[PendingOp]) -> Vec<Vec<&PendingOp>> {
        if ops.is_empty() {
            return Vec::new();
        }

        let mut batches: Vec<Vec<&PendingOp>> = Vec::new();
        let mut current_table: Option<&'static str> = None;
        let mut current_batch: Vec<&PendingOp> = Vec::new();

        for op in ops {
            let table = op.table();
            if current_table == Some(table) {
                current_batch.push(op);
            } else {
                if !current_batch.is_empty() {
                    batches.push(current_batch);
                }
                current_batch = vec![op];
                current_table = Some(table);
            }
        }

        if !current_batch.is_empty() {
            batches.push(current_batch);
        }

        batches
    }

    #[allow(clippy::result_large_err)]
    fn build_insert_batch_sql(
        dialect: sqlmodel_core::Dialect,
        ops: &[&PendingOp],
    ) -> Result<(String, Vec<Value>), Error> {
        let table = ops[0].table();
        let PendingOp::Insert { columns, .. } = ops[0] else {
            return Err(Error::Custom("expected insert operation".to_string()));
        };

        let col_list: String = columns
            .iter()
            .map(|c| dialect.quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");

        let mut sql = format!(
            "INSERT INTO {} ({}) VALUES ",
            dialect.quote_identifier(table),
            col_list
        );
        let mut params: Vec<Value> = Vec::new();
        let mut param_idx = 1;

        for (i, op) in ops.iter().enumerate() {
            let PendingOp::Insert {
                columns: row_columns,
                values,
                ..
            } = op
            else {
                return Err(Error::Custom(
                    "mixed operation kinds in insert batch".to_string(),
                ));
            };

            if row_columns != columns {
                return Err(Error::Custom(format!(
                    "inconsistent insert columns in flush batch for table {table}"
                )));
            }
            if values.len() != columns.len() {
                return Err(Error::Custom(format!(
                    "insert column/value length mismatch for table {table}: {} columns vs {} values",
                    columns.len(),
                    values.len()
                )));
            }

            if i > 0 {
                sql.push_str(", ");
            }
            let placeholders: Vec<String> = (0..values.len())
                .map(|_| {
                    let p = dialect.placeholder(param_idx);
                    param_idx += 1;
                    p
                })
                .collect();
            sql.push('(');
            sql.push_str(&placeholders.join(", "));
            sql.push(')');
            params.extend(values.iter().cloned());
        }

        Ok((sql, params))
    }

    #[allow(clippy::result_large_err)]
    fn build_delete_batch_sql(
        dialect: sqlmodel_core::Dialect,
        ops: &[&PendingOp],
    ) -> Result<Option<(String, Vec<Value>, usize)>, Error> {
        let table = ops[0].table();
        let PendingOp::Delete { pk_columns, .. } = ops[0] else {
            return Err(Error::Custom("expected delete operation".to_string()));
        };

        // No PK means no safe WHERE clause.
        if pk_columns.is_empty() {
            tracing::warn!(
                table = table,
                count = ops.len(),
                "Skipping DELETE batch for table without primary key - cannot identify rows"
            );
            return Ok(None);
        }

        if pk_columns.len() == 1 {
            let pk_col = pk_columns[0];
            let mut params: Vec<Value> = Vec::new();
            let placeholders: Vec<String> = ops
                .iter()
                .filter_map(|op| {
                    if let PendingOp::Delete {
                        pk_columns: row_pk_columns,
                        pk_values,
                        ..
                    } = op
                    {
                        if row_pk_columns != pk_columns {
                            return None;
                        }
                        if pk_values.len() != 1 {
                            return None;
                        }
                        params.push(pk_values[0].clone());
                        return Some(dialect.placeholder(params.len()));
                    }
                    None
                })
                .collect();

            if placeholders.is_empty() {
                return Ok(None);
            }

            let actual_count = params.len();
            let sql = format!(
                "DELETE FROM {} WHERE {} IN ({})",
                dialect.quote_identifier(table),
                dialect.quote_identifier(pk_col),
                placeholders.join(", ")
            );
            return Ok(Some((sql, params, actual_count)));
        }

        Err(Error::Custom(
            "composite delete batch must be handled per-row".to_string(),
        ))
    }

    #[allow(clippy::result_large_err)]
    fn build_update_sql(
        dialect: sqlmodel_core::Dialect,
        op: &PendingOp,
    ) -> Result<Option<(String, Vec<Value>)>, Error> {
        let PendingOp::Update {
            table,
            pk_columns,
            pk_values,
            set_columns,
            set_values,
            ..
        } = op
        else {
            return Ok(None);
        };

        // No PK means no safe WHERE clause.
        if pk_columns.is_empty() || pk_values.is_empty() {
            tracing::warn!(
                table = *table,
                "Skipping UPDATE for row without primary key - cannot identify row"
            );
            return Ok(None);
        }
        if set_columns.is_empty() {
            return Ok(None);
        }

        if pk_columns.len() != pk_values.len() {
            return Err(Error::Custom(format!(
                "update primary key column/value length mismatch for table {table}: {} columns vs {} values",
                pk_columns.len(),
                pk_values.len()
            )));
        }
        if set_columns.len() != set_values.len() {
            return Err(Error::Custom(format!(
                "update set column/value length mismatch for table {table}: {} columns vs {} values",
                set_columns.len(),
                set_values.len()
            )));
        }

        let mut param_idx = 1;
        let set_clause: String = set_columns
            .iter()
            .map(|col| {
                let clause = format!(
                    "{} = {}",
                    dialect.quote_identifier(col),
                    dialect.placeholder(param_idx)
                );
                param_idx += 1;
                clause
            })
            .collect::<Vec<_>>()
            .join(", ");

        let where_clause: String = pk_columns
            .iter()
            .map(|col| {
                let clause = format!(
                    "{} = {}",
                    dialect.quote_identifier(col),
                    dialect.placeholder(param_idx)
                );
                param_idx += 1;
                clause
            })
            .collect::<Vec<_>>()
            .join(" AND ");

        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            dialect.quote_identifier(table),
            set_clause,
            where_clause
        );

        let mut params: Vec<Value> = set_values.clone();
        params.extend(pk_values.iter().cloned());

        Ok(Some((sql, params)))
    }

    /// Execute a batch of insert operations.
    #[tracing::instrument(level = "debug", skip(cx, conn, ops))]
    async fn execute_insert_batch<C: Connection>(
        cx: &Cx,
        conn: &C,
        ops: &[&PendingOp],
    ) -> Outcome<usize, Error> {
        if ops.is_empty() {
            return Outcome::Ok(0);
        }

        let table = ops[0].table();
        let PendingOp::Insert { .. } = ops[0] else {
            return Outcome::Ok(0);
        };

        tracing::debug!(table = table, count = ops.len(), "Executing insert batch");
        let dialect = conn.dialect();
        let (sql, params) = match Self::build_insert_batch_sql(dialect, ops) {
            Ok(v) => v,
            Err(e) => return Outcome::Err(e),
        };

        match conn.execute(cx, &sql, &params).await {
            Outcome::Ok(_) => Outcome::Ok(ops.len()),
            Outcome::Err(e) => Outcome::Err(e),
            Outcome::Cancelled(r) => Outcome::Cancelled(r),
            Outcome::Panicked(p) => Outcome::Panicked(p),
        }
    }

    /// Execute a batch of delete operations.
    #[tracing::instrument(level = "debug", skip(cx, conn, ops))]
    async fn execute_delete_batch<C: Connection>(
        cx: &Cx,
        conn: &C,
        ops: &[&PendingOp],
    ) -> Outcome<usize, Error> {
        if ops.is_empty() {
            return Outcome::Ok(0);
        }

        let table = ops[0].table();
        let PendingOp::Delete { pk_columns, .. } = ops[0] else {
            return Outcome::Ok(0);
        };

        // Skip if no primary key columns - cannot safely DELETE without WHERE clause
        if pk_columns.is_empty() {
            tracing::warn!(
                table = table,
                count = ops.len(),
                "Skipping DELETE batch for table without primary key - cannot identify rows"
            );
            return Outcome::Ok(0);
        }

        tracing::debug!(table = table, count = ops.len(), "Executing delete batch");
        let dialect = conn.dialect();

        // For simple single-column PK, use IN clause
        // DELETE FROM table WHERE pk IN ($1, $2, $3, ...)
        if pk_columns.len() == 1 {
            let (sql, params, actual_count) = match Self::build_delete_batch_sql(dialect, ops) {
                Ok(Some(v)) => v,
                Ok(None) => return Outcome::Ok(0),
                Err(e) => return Outcome::Err(e),
            };

            match conn.execute(cx, &sql, &params).await {
                // Return actual count of items in IN clause, not ops.len()
                // (some ops may have been filtered out due to empty pk_values)
                Outcome::Ok(_) => Outcome::Ok(actual_count),
                Outcome::Err(e) => Outcome::Err(e),
                Outcome::Cancelled(r) => Outcome::Cancelled(r),
                Outcome::Panicked(p) => Outcome::Panicked(p),
            }
        } else {
            // Composite PK: execute individual deletes
            let mut deleted = 0;
            for op in ops {
                if let PendingOp::Delete {
                    pk_columns,
                    pk_values,
                    ..
                } = op
                {
                    // Skip if pk_values is empty - would cause parameter mismatch
                    if pk_values.is_empty() {
                        tracing::warn!(
                            table = table,
                            "Skipping DELETE for row with empty primary key values"
                        );
                        continue;
                    }
                    if pk_values.len() != pk_columns.len() {
                        return Outcome::Err(Error::Custom(format!(
                            "delete primary key column/value length mismatch for table {table}: {} columns vs {} values",
                            pk_columns.len(),
                            pk_values.len()
                        )));
                    }

                    let where_clause: String = pk_columns
                        .iter()
                        .enumerate()
                        .map(|(i, col)| {
                            format!(
                                "{} = {}",
                                dialect.quote_identifier(col),
                                dialect.placeholder(i + 1)
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(" AND ");

                    let sql = format!(
                        "DELETE FROM {} WHERE {}",
                        dialect.quote_identifier(table),
                        where_clause
                    );

                    match conn.execute(cx, &sql, pk_values).await {
                        Outcome::Ok(_) => deleted += 1,
                        Outcome::Err(e) => return Outcome::Err(e),
                        Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                        Outcome::Panicked(p) => return Outcome::Panicked(p),
                    }
                }
            }
            Outcome::Ok(deleted)
        }
    }

    /// Execute a single update operation.
    #[tracing::instrument(level = "debug", skip(cx, conn, op))]
    async fn execute_update<C: Connection>(
        cx: &Cx,
        conn: &C,
        op: &PendingOp,
    ) -> Outcome<(), Error> {
        let PendingOp::Update { table, .. } = op else {
            return Outcome::Ok(());
        };

        tracing::debug!(table = *table, "Executing update");
        let dialect = conn.dialect();
        let (sql, params) = match Self::build_update_sql(dialect, op) {
            Ok(Some(v)) => v,
            Ok(None) => return Outcome::Ok(()),
            Err(e) => return Outcome::Err(e),
        };

        match conn.execute(cx, &sql, &params).await {
            Outcome::Ok(_) => Outcome::Ok(()),
            Outcome::Err(e) => Outcome::Err(e),
            Outcome::Cancelled(r) => Outcome::Cancelled(r),
            Outcome::Panicked(p) => Outcome::Panicked(p),
        }
    }
}

/// Result of a flush operation.
#[derive(Debug, Default, Clone, Copy)]
pub struct FlushResult {
    /// Number of rows inserted.
    pub inserted: usize,
    /// Number of rows updated.
    pub updated: usize,
    /// Number of rows deleted.
    pub deleted: usize,
}

impl FlushResult {
    /// Create a new empty result.
    pub fn new() -> Self {
        Self::default()
    }

    /// Total number of operations performed.
    pub fn total(&self) -> usize {
        self.inserted + self.updated + self.deleted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlmodel_core::{FieldInfo, Row};
    use std::any::TypeId;

    // Mock models for testing
    struct Team;
    struct Hero;

    impl Model for Team {
        const TABLE_NAME: &'static str = "teams";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: [FieldInfo; 1] =
                [FieldInfo::new("id", "id", sqlmodel_core::SqlType::BigInt)
                    .primary_key(true)
                    .auto_increment(true)];
            &FIELDS
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn from_row(_row: &Row) -> Result<Self, sqlmodel_core::Error> {
            Ok(Team)
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    impl Model for Hero {
        const TABLE_NAME: &'static str = "heroes";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: [FieldInfo; 2] = [
                FieldInfo::new("id", "id", sqlmodel_core::SqlType::BigInt)
                    .primary_key(true)
                    .auto_increment(true),
                FieldInfo::new("team_id", "team_id", sqlmodel_core::SqlType::BigInt)
                    .nullable(true)
                    .foreign_key("teams.id"),
            ];
            &FIELDS
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn from_row(_row: &Row) -> Result<Self, sqlmodel_core::Error> {
            Ok(Hero)
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    fn make_insert(table: &'static str, pk: i64) -> PendingOp {
        PendingOp::Insert {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: pk as u64,
            },
            table,
            columns: vec!["id", "name"],
            values: vec![Value::BigInt(pk), Value::Text("Test".to_string())],
        }
    }

    fn make_delete(table: &'static str, pk: i64) -> PendingOp {
        PendingOp::Delete {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: pk as u64,
            },
            table,
            pk_columns: vec!["id"],
            pk_values: vec![Value::BigInt(pk)],
        }
    }

    fn make_update(table: &'static str, pk: i64) -> PendingOp {
        PendingOp::Update {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: pk as u64,
            },
            table,
            pk_columns: vec!["id"],
            pk_values: vec![Value::BigInt(pk)],
            set_columns: vec!["name"],
            set_values: vec![Value::Text("Updated".to_string())],
        }
    }

    #[test]
    fn test_pending_op_table_accessor() {
        let insert = make_insert("teams", 1);
        assert_eq!(insert.table(), "teams");

        let delete = make_delete("heroes", 2);
        assert_eq!(delete.table(), "heroes");

        let update = make_update("teams", 3);
        assert_eq!(update.table(), "teams");
    }

    #[test]
    fn test_pending_op_type_checks() {
        let insert = make_insert("teams", 1);
        assert!(insert.is_insert());
        assert!(!insert.is_update());
        assert!(!insert.is_delete());

        let update = make_update("teams", 1);
        assert!(update.is_update());
        assert!(!update.is_insert());
        assert!(!update.is_delete());

        let delete = make_delete("teams", 1);
        assert!(delete.is_delete());
        assert!(!delete.is_insert());
        assert!(!delete.is_update());
    }

    #[test]
    fn test_orderer_simple_no_deps() {
        let orderer = FlushOrderer::new();
        let ops = vec![
            make_insert("teams", 1),
            make_insert("teams", 2),
            make_delete("teams", 3),
        ];

        let plan = orderer.order(ops);
        assert_eq!(plan.inserts.len(), 2);
        assert_eq!(plan.deletes.len(), 1);
        assert_eq!(plan.updates.len(), 0);
    }

    #[test]
    fn test_orderer_parent_child_inserts() {
        let mut orderer = FlushOrderer::new();
        orderer.register_model::<Team>();
        orderer.register_model::<Hero>();

        // Add child insert first, then parent
        let ops = vec![
            make_insert("heroes", 1), // Has FK to teams
            make_insert("teams", 1),  // No FK
        ];

        let plan = orderer.order(ops);

        // Teams should be first (fewer deps)
        assert_eq!(plan.inserts[0].table(), "teams");
        assert_eq!(plan.inserts[1].table(), "heroes");
    }

    #[test]
    fn test_orderer_parent_child_deletes() {
        let mut orderer = FlushOrderer::new();
        orderer.register_model::<Team>();
        orderer.register_model::<Hero>();

        // Add parent delete first, then child
        let ops = vec![
            make_delete("teams", 1),  // No FK
            make_delete("heroes", 1), // Has FK to teams
        ];

        let plan = orderer.order(ops);

        // Heroes should be first (more deps = delete first)
        assert_eq!(plan.deletes[0].table(), "heroes");
        assert_eq!(plan.deletes[1].table(), "teams");
    }

    #[test]
    fn test_batch_by_table_groups_correctly() {
        let ops = vec![
            make_insert("teams", 1),
            make_insert("teams", 2),
            make_insert("heroes", 1),
            make_insert("heroes", 2),
            make_insert("teams", 3),
        ];

        let batches = FlushPlan::batch_by_table(&ops);

        // Should group consecutive same-table ops
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].len(), 2); // teams 1, 2
        assert_eq!(batches[1].len(), 2); // heroes 1, 2
        assert_eq!(batches[2].len(), 1); // teams 3
    }

    #[test]
    fn test_batch_empty_returns_empty() {
        let ops: Vec<PendingOp> = vec![];
        let batches = FlushPlan::batch_by_table(&ops);
        assert!(batches.is_empty());
    }

    #[test]
    fn test_flush_plan_is_empty() {
        let plan = FlushPlan::new();
        assert!(plan.is_empty());
        assert_eq!(plan.len(), 0);
    }

    #[test]
    fn test_flush_plan_len() {
        let plan = FlushPlan {
            deletes: vec![make_delete("teams", 1)],
            inserts: vec![make_insert("teams", 1), make_insert("teams", 2)],
            updates: vec![make_update("teams", 1)],
        };
        assert!(!plan.is_empty());
        assert_eq!(plan.len(), 4);
    }

    #[test]
    fn test_flush_result_total() {
        let result = FlushResult {
            inserted: 5,
            updated: 3,
            deleted: 2,
        };
        assert_eq!(result.total(), 10);
    }

    #[test]
    fn test_flush_result_default() {
        let result = FlushResult::new();
        assert_eq!(result.inserted, 0);
        assert_eq!(result.updated, 0);
        assert_eq!(result.deleted, 0);
        assert_eq!(result.total(), 0);
    }

    // ========================================================================
    // Link Table Operation Tests
    // ========================================================================

    #[test]
    fn test_link_table_op_link_constructor() {
        let op = LinkTableOp::link(
            "hero_powers".to_string(),
            "hero_id".to_string(),
            Value::BigInt(1),
            "power_id".to_string(),
            Value::BigInt(5),
        );

        match op {
            LinkTableOp::Link {
                table,
                local_columns,
                local_values,
                remote_columns,
                remote_values,
            } => {
                assert_eq!(table, "hero_powers");
                assert_eq!(local_columns, vec!["hero_id".to_string()]);
                assert_eq!(local_values, vec![Value::BigInt(1)]);
                assert_eq!(remote_columns, vec!["power_id".to_string()]);
                assert_eq!(remote_values, vec![Value::BigInt(5)]);
            }
            LinkTableOp::Unlink { .. } => std::panic::panic_any("Expected Link variant"),
        }
    }

    #[test]
    fn test_link_table_op_unlink_constructor() {
        let op = LinkTableOp::unlink(
            "hero_powers".to_string(),
            "hero_id".to_string(),
            Value::BigInt(1),
            "power_id".to_string(),
            Value::BigInt(5),
        );

        match op {
            LinkTableOp::Unlink {
                table,
                local_columns,
                local_values,
                remote_columns,
                remote_values,
            } => {
                assert_eq!(table, "hero_powers");
                assert_eq!(local_columns, vec!["hero_id".to_string()]);
                assert_eq!(local_values, vec![Value::BigInt(1)]);
                assert_eq!(remote_columns, vec!["power_id".to_string()]);
                assert_eq!(remote_values, vec![Value::BigInt(5)]);
            }
            LinkTableOp::Link { .. } => std::panic::panic_any("Expected Unlink variant"),
        }
    }

    #[test]
    fn test_link_table_op_is_link() {
        let link = LinkTableOp::link(
            "t".to_string(),
            "a".to_string(),
            Value::BigInt(1),
            "b".to_string(),
            Value::BigInt(2),
        );
        let unlink = LinkTableOp::unlink(
            "t".to_string(),
            "a".to_string(),
            Value::BigInt(1),
            "b".to_string(),
            Value::BigInt(2),
        );

        assert!(matches!(link, LinkTableOp::Link { .. }));
        assert!(matches!(unlink, LinkTableOp::Unlink { .. }));
    }

    #[test]
    fn test_link_table_op_debug_format() {
        let link = LinkTableOp::link(
            "hero_powers".to_string(),
            "hero_id".to_string(),
            Value::BigInt(1),
            "power_id".to_string(),
            Value::BigInt(5),
        );
        let debug_str = format!("{:?}", link);
        assert!(debug_str.contains("Link"));
        assert!(debug_str.contains("hero_powers"));
    }

    #[test]
    fn test_link_table_op_clone() {
        let op = LinkTableOp::link(
            "hero_powers".to_string(),
            "hero_id".to_string(),
            Value::BigInt(1),
            "power_id".to_string(),
            Value::BigInt(5),
        );
        let cloned = op.clone();

        match (op, cloned) {
            (
                LinkTableOp::Link {
                    table: t1,
                    local_values: lv1,
                    remote_values: rv1,
                    ..
                },
                LinkTableOp::Link {
                    table: t2,
                    local_values: lv2,
                    remote_values: rv2,
                    ..
                },
            ) => {
                assert_eq!(t1, t2);
                assert_eq!(lv1, lv2);
                assert_eq!(rv1, rv2);
            }
            _ => std::panic::panic_any("Clone should preserve variant"),
        }
    }

    #[test]
    fn test_link_table_ops_empty_vec() {
        // Test that an empty ops vec handles correctly
        let ops: Vec<LinkTableOp> = vec![];
        assert!(ops.is_empty());
    }

    #[test]
    fn test_link_table_ops_multiple_operations() {
        let ops = [
            LinkTableOp::link(
                "hero_powers".to_string(),
                "hero_id".to_string(),
                Value::BigInt(1),
                "power_id".to_string(),
                Value::BigInt(1),
            ),
            LinkTableOp::link(
                "hero_powers".to_string(),
                "hero_id".to_string(),
                Value::BigInt(1),
                "power_id".to_string(),
                Value::BigInt(2),
            ),
            LinkTableOp::unlink(
                "hero_powers".to_string(),
                "hero_id".to_string(),
                Value::BigInt(1),
                "power_id".to_string(),
                Value::BigInt(3),
            ),
        ];

        let links: Vec<_> = ops
            .iter()
            .filter(|o| matches!(o, LinkTableOp::Link { .. }))
            .collect();
        let unlinks: Vec<_> = ops
            .iter()
            .filter(|o| matches!(o, LinkTableOp::Unlink { .. }))
            .collect();

        assert_eq!(links.len(), 2);
        assert_eq!(unlinks.len(), 1);
    }

    #[test]
    fn test_link_table_op_with_different_value_types() {
        // Test with string values
        let op_str = LinkTableOp::link(
            "tag_items".to_string(),
            "tag_id".to_string(),
            Value::Text("tag-uuid-123".to_string()),
            "item_id".to_string(),
            Value::Text("item-uuid-456".to_string()),
        );

        match op_str {
            LinkTableOp::Link {
                local_values,
                remote_values,
                ..
            } => {
                assert!(matches!(local_values.first(), Some(Value::Text(_))));
                assert!(matches!(remote_values.first(), Some(Value::Text(_))));
            }
            LinkTableOp::Unlink { .. } => std::panic::panic_any("Expected Link"),
        }

        // Test with integer values
        let op_int = LinkTableOp::link(
            "user_roles".to_string(),
            "user_id".to_string(),
            Value::Int(42),
            "role_id".to_string(),
            Value::Int(7),
        );

        match op_int {
            LinkTableOp::Link {
                local_values,
                remote_values,
                ..
            } => {
                assert!(matches!(local_values.first(), Some(Value::Int(_))));
                assert!(matches!(remote_values.first(), Some(Value::Int(_))));
            }
            LinkTableOp::Unlink { .. } => std::panic::panic_any("Expected Link"),
        }
    }

    // ================================================================================
    // DML Identifier Quoting Integration Tests
    // ================================================================================

    // Helper to create PendingOp::Insert with custom names
    fn make_custom_insert(table: &'static str, columns: Vec<&'static str>, pk: i64) -> PendingOp {
        PendingOp::Insert {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: pk as u64,
            },
            table,
            columns,
            values: vec![Value::BigInt(pk), Value::Text("Test".to_string())],
        }
    }

    // Helper to create PendingOp::Delete with custom pk columns
    fn make_custom_delete(
        table: &'static str,
        pk_columns: Vec<&'static str>,
        pk: i64,
    ) -> PendingOp {
        PendingOp::Delete {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: pk as u64,
            },
            table,
            pk_columns,
            pk_values: vec![Value::BigInt(pk)],
        }
    }

    // Helper to create PendingOp::Update with custom column names
    fn make_custom_update(
        table: &'static str,
        pk_columns: Vec<&'static str>,
        set_columns: Vec<&'static str>,
        pk: i64,
    ) -> PendingOp {
        PendingOp::Update {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: pk as u64,
            },
            table,
            pk_columns,
            pk_values: vec![Value::BigInt(pk)],
            set_columns,
            set_values: vec![Value::Text("Updated".to_string())],
        }
    }

    // ------ LinkTableOp SQL Generation Tests ------

    #[test]
    fn test_link_table_op_to_sql_simple() {
        let op = LinkTableOp::link(
            "hero_powers".to_string(),
            "hero_id".to_string(),
            Value::BigInt(1),
            "power_id".to_string(),
            Value::BigInt(5),
        );
        let sql = op.to_sql();
        assert_eq!(
            sql,
            "INSERT INTO \"hero_powers\" (\"hero_id\", \"power_id\") VALUES ($1, $2)"
        );
    }

    #[test]
    fn test_link_table_op_to_sql_with_keywords() {
        let op = LinkTableOp::link(
            "order".to_string(),  // SQL keyword table
            "select".to_string(), // SQL keyword column
            Value::BigInt(1),
            "from".to_string(), // SQL keyword column
            Value::BigInt(2),
        );
        let sql = op.to_sql();
        assert_eq!(
            sql,
            "INSERT INTO \"order\" (\"select\", \"from\") VALUES ($1, $2)"
        );
    }

    #[test]
    fn test_link_table_op_to_sql_with_embedded_quotes() {
        let op = LinkTableOp::link(
            "my\"table".to_string(),
            "col\"a".to_string(),
            Value::BigInt(1),
            "col\"b".to_string(),
            Value::BigInt(2),
        );
        let sql = op.to_sql();
        assert_eq!(
            sql,
            "INSERT INTO \"my\"\"table\" (\"col\"\"a\", \"col\"\"b\") VALUES ($1, $2)"
        );
    }

    #[test]
    fn test_link_table_op_unlink_to_sql_with_keywords() {
        let op = LinkTableOp::unlink(
            "user".to_string(),
            "index".to_string(),
            Value::BigInt(1),
            "key".to_string(),
            Value::BigInt(2),
        );
        let sql = op.to_sql();
        assert_eq!(
            sql,
            "DELETE FROM \"user\" WHERE \"index\" = $1 AND \"key\" = $2"
        );
    }

    #[test]
    fn test_link_table_op_to_sql_with_unicode() {
        let op = LinkTableOp::link(
            "用户表".to_string(),
            "用户id".to_string(),
            Value::BigInt(1),
            "角色id".to_string(),
            Value::BigInt(2),
        );
        let sql = op.to_sql();
        assert_eq!(
            sql,
            "INSERT INTO \"用户表\" (\"用户id\", \"角色id\") VALUES ($1, $2)"
        );
    }

    #[test]
    fn test_link_table_op_to_sql_with_spaces() {
        let op = LinkTableOp::link(
            "link table".to_string(),
            "local id".to_string(),
            Value::BigInt(1),
            "remote id".to_string(),
            Value::BigInt(2),
        );
        let sql = op.to_sql();
        assert_eq!(
            sql,
            "INSERT INTO \"link table\" (\"local id\", \"remote id\") VALUES ($1, $2)"
        );
    }

    // ------ PendingOp::Insert SQL Generation Tests ------

    #[test]
    fn test_pending_op_insert_to_sql_simple() {
        let op = make_insert("teams", 1);
        let sql = op.to_sql();
        assert!(sql.starts_with("INSERT INTO \"teams\""));
        assert!(sql.contains("(\"id\", \"name\")"));
        assert!(sql.contains("VALUES ($1, $2)"));
    }

    #[test]
    fn test_pending_op_insert_to_sql_with_keyword_table() {
        let op = make_custom_insert("order", vec!["id", "select"], 1);
        let sql = op.to_sql();
        assert_eq!(
            sql,
            "INSERT INTO \"order\" (\"id\", \"select\") VALUES ($1, $2)"
        );
    }

    #[test]
    fn test_pending_op_insert_to_sql_with_quoted_names() {
        let op = make_custom_insert("my\"table", vec!["pk\"id", "data\"col"], 1);
        let sql = op.to_sql();
        assert_eq!(
            sql,
            "INSERT INTO \"my\"\"table\" (\"pk\"\"id\", \"data\"\"col\") VALUES ($1, $2)"
        );
    }

    // ------ PendingOp::Delete SQL Generation Tests ------

    #[test]
    fn test_pending_op_delete_to_sql_single_pk() {
        let op = make_delete("teams", 1);
        let sql = op.to_sql();
        assert_eq!(sql, "DELETE FROM \"teams\" WHERE \"id\" IN ($1)");
    }

    #[test]
    fn test_pending_op_delete_to_sql_with_keyword_table() {
        let op = make_custom_delete("order", vec!["id"], 1);
        let sql = op.to_sql();
        assert_eq!(sql, "DELETE FROM \"order\" WHERE \"id\" IN ($1)");
    }

    #[test]
    fn test_pending_op_delete_to_sql_composite_pk() {
        let op = PendingOp::Delete {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: 1,
            },
            table: "order_items",
            pk_columns: vec!["order_id", "item_id"],
            pk_values: vec![Value::BigInt(1), Value::BigInt(2)],
        };
        let sql = op.to_sql();
        assert_eq!(
            sql,
            "DELETE FROM \"order_items\" WHERE \"order_id\" = $1 AND \"item_id\" = $2"
        );
    }

    #[test]
    fn test_pending_op_delete_to_sql_with_keyword_pk_columns() {
        let op = PendingOp::Delete {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: 1,
            },
            table: "user",
            pk_columns: vec!["select", "from"],
            pk_values: vec![Value::BigInt(1), Value::BigInt(2)],
        };
        let sql = op.to_sql();
        assert_eq!(
            sql,
            "DELETE FROM \"user\" WHERE \"select\" = $1 AND \"from\" = $2"
        );
    }

    // ------ PendingOp::Update SQL Generation Tests ------

    #[test]
    fn test_pending_op_update_to_sql_simple() {
        let op = make_update("teams", 1);
        let sql = op.to_sql();
        assert_eq!(sql, "UPDATE \"teams\" SET \"name\" = $1 WHERE \"id\" = $2");
    }

    #[test]
    fn test_pending_op_update_to_sql_with_keyword_names() {
        let op = make_custom_update("order", vec!["id"], vec!["select", "from"], 1);
        let sql = op.to_sql();
        assert_eq!(
            sql,
            "UPDATE \"order\" SET \"select\" = $1, \"from\" = $2 WHERE \"id\" = $3"
        );
    }

    #[test]
    fn test_pending_op_update_to_sql_with_quoted_names() {
        let op = make_custom_update("my\"table", vec!["pk\"id"], vec!["data\"col"], 1);
        let sql = op.to_sql();
        assert_eq!(
            sql,
            "UPDATE \"my\"\"table\" SET \"data\"\"col\" = $1 WHERE \"pk\"\"id\" = $2"
        );
    }

    #[test]
    fn test_pending_op_update_to_sql_composite_pk() {
        let op = PendingOp::Update {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: 1,
            },
            table: "order_items",
            pk_columns: vec!["order_id", "item_id"],
            pk_values: vec![Value::BigInt(1), Value::BigInt(2)],
            set_columns: vec!["quantity"],
            set_values: vec![Value::Int(5)],
        };
        let sql = op.to_sql();
        assert_eq!(
            sql,
            "UPDATE \"order_items\" SET \"quantity\" = $1 WHERE \"order_id\" = $2 AND \"item_id\" = $3"
        );
    }

    // ------ SQL Injection Neutralization Tests ------

    #[test]
    fn test_link_op_sql_injection_neutralized() {
        // Attempt SQL injection through table name
        let op = LinkTableOp::link(
            "links\"; DROP TABLE users; --".to_string(),
            "col1".to_string(),
            Value::BigInt(1),
            "col2".to_string(),
            Value::BigInt(2),
        );
        let sql = op.to_sql();
        // The embedded quote should be doubled, keeping everything as an identifier
        assert!(sql.contains("\"links\"\"; DROP TABLE users; --\""));
        // Count quotes - injection is neutralized
        assert!(sql.starts_with("INSERT INTO \""));
    }

    #[test]
    fn test_pending_op_insert_sql_injection_neutralized() {
        let op = make_custom_insert("users\"; DROP TABLE secrets; --", vec!["id", "name"], 1);
        let sql = op.to_sql();
        // Injection attempt should be contained within quotes
        assert!(sql.contains("\"users\"\"; DROP TABLE secrets; --\""));
        assert!(sql.starts_with("INSERT INTO \""));
    }

    #[test]
    fn test_pending_op_update_sql_injection_neutralized() {
        let op = make_custom_update("data", vec!["id"], vec!["col\"; DROP TABLE data; --"], 1);
        let sql = op.to_sql();
        // The malicious column name is safely quoted
        assert!(sql.contains("\"col\"\"; DROP TABLE data; --\""));
    }

    // ------ Edge Cases ------

    #[test]
    fn test_pending_op_insert_many_columns() {
        let op = PendingOp::Insert {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: 1,
            },
            table: "wide_table",
            columns: vec!["a", "b", "c", "d", "e"],
            values: vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
                Value::Int(4),
                Value::Int(5),
            ],
        };
        let sql = op.to_sql();
        assert_eq!(
            sql,
            "INSERT INTO \"wide_table\" (\"a\", \"b\", \"c\", \"d\", \"e\") VALUES ($1, $2, $3, $4, $5)"
        );
    }

    #[test]
    fn test_pending_op_update_many_set_columns() {
        let op = PendingOp::Update {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: 1,
            },
            table: "items",
            pk_columns: vec!["id"],
            pk_values: vec![Value::BigInt(1)],
            set_columns: vec!["a", "b", "c"],
            set_values: vec![Value::Int(1), Value::Int(2), Value::Int(3)],
        };
        let sql = op.to_sql();
        assert_eq!(
            sql,
            "UPDATE \"items\" SET \"a\" = $1, \"b\" = $2, \"c\" = $3 WHERE \"id\" = $4"
        );
    }

    #[test]
    fn test_link_table_op_empty_strings() {
        // Edge case: empty identifiers (unusual but should still be quoted)
        let op = LinkTableOp::link(
            String::new(),
            String::new(),
            Value::BigInt(1),
            String::new(),
            Value::BigInt(2),
        );
        let sql = op.to_sql();
        assert_eq!(sql, "INSERT INTO \"\" (\"\", \"\") VALUES ($1, $2)");
    }

    // ------ Invalid Operation Edge Cases ------

    #[test]
    fn test_pending_op_delete_empty_pk_columns() {
        let op = PendingOp::Delete {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: 1,
            },
            table: "orphan_table",
            pk_columns: vec![], // No PK columns - invalid!
            pk_values: vec![],
        };
        let sql = op.to_sql();
        // Should return error indicator, not invalid SQL
        assert!(sql.starts_with("-- ERROR:"));
        assert!(sql.contains("DELETE"));
        assert!(sql.contains("no pk_columns"));
    }

    #[test]
    fn test_pending_op_update_empty_pk_columns() {
        let op = PendingOp::Update {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: 1,
            },
            table: "orphan_table",
            pk_columns: vec![], // No PK columns - invalid!
            pk_values: vec![],
            set_columns: vec!["name"],
            set_values: vec![Value::Text("test".to_string())],
        };
        let sql = op.to_sql();
        // Should return error indicator, not invalid SQL
        assert!(sql.starts_with("-- ERROR:"));
        assert!(sql.contains("UPDATE"));
        assert!(sql.contains("no pk_columns"));
    }

    #[test]
    fn test_pending_op_update_empty_set_columns() {
        let op = PendingOp::Update {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: 1,
            },
            table: "nothing_to_update",
            pk_columns: vec!["id"],
            pk_values: vec![Value::BigInt(1)],
            set_columns: vec![], // No columns to set - invalid!
            set_values: vec![],
        };
        let sql = op.to_sql();
        // Should return error indicator, not invalid SQL
        assert!(sql.starts_with("-- ERROR:"));
        assert!(sql.contains("UPDATE"));
        assert!(sql.contains("no set_columns"));
    }

    #[test]
    fn test_pending_op_insert_empty_columns() {
        let op = PendingOp::Insert {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: 1,
            },
            table: "empty_insert",
            columns: vec![], // No columns - invalid!
            values: vec![],
        };
        let sql = op.to_sql();
        // Should return error indicator, not invalid SQL
        assert!(sql.starts_with("-- ERROR:"));
        assert!(sql.contains("INSERT"));
        assert!(sql.contains("no columns"));
    }

    #[test]
    fn test_build_insert_batch_sql_mysql_dialect() {
        let ops = [make_insert("teams", 1), make_insert("teams", 2)];
        let refs: Vec<&PendingOp> = ops.iter().collect();
        let (sql, params) = FlushPlan::build_insert_batch_sql(sqlmodel_core::Dialect::Mysql, &refs)
            .expect("build insert batch sql");

        assert_eq!(
            sql,
            "INSERT INTO `teams` (`id`, `name`) VALUES (?, ?), (?, ?)"
        );
        assert_eq!(params.len(), 4);
    }

    #[test]
    fn test_build_delete_batch_sql_sqlite_dialect() {
        let ops = [make_delete("heroes", 1), make_delete("heroes", 2)];
        let refs: Vec<&PendingOp> = ops.iter().collect();
        let built = FlushPlan::build_delete_batch_sql(sqlmodel_core::Dialect::Sqlite, &refs)
            .expect("build delete batch sql")
            .expect("non-empty delete sql");

        assert_eq!(built.0, "DELETE FROM \"heroes\" WHERE \"id\" IN (?1, ?2)");
        assert_eq!(built.1.len(), 2);
        assert_eq!(built.2, 2);
    }

    #[test]
    fn test_build_update_sql_mysql_dialect() {
        let op = make_update("teams", 42);
        let (sql, params) = FlushPlan::build_update_sql(sqlmodel_core::Dialect::Mysql, &op)
            .expect("build update sql")
            .expect("non-empty update sql");

        assert_eq!(sql, "UPDATE `teams` SET `name` = ? WHERE `id` = ?");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_build_update_sql_rejects_set_mismatch() {
        let op = PendingOp::Update {
            key: ObjectKey {
                type_id: TypeId::of::<()>(),
                pk_hash: 1,
            },
            table: "teams",
            pk_columns: vec!["id"],
            pk_values: vec![Value::BigInt(1)],
            set_columns: vec!["name", "active"],
            set_values: vec![Value::Text("A".to_string())],
        };

        let err = FlushPlan::build_update_sql(sqlmodel_core::Dialect::Postgres, &op)
            .expect_err("expected set mismatch error");
        assert!(
            err.to_string()
                .contains("update set column/value length mismatch")
        );
    }
}
