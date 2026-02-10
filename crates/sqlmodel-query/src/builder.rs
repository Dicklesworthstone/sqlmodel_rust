//! Query builders for INSERT, UPDATE, DELETE operations.
//!
//! This module provides fluent builders for CRUD operations with support for:
//! - RETURNING clause (PostgreSQL)
//! - Bulk inserts
//! - UPSERT (ON CONFLICT)
//! - Explicit column SET for updates
//! - Model-based deletes

use crate::clause::Where;
use crate::expr::{Dialect, Expr};
use asupersync::{Cx, Outcome};
use sqlmodel_core::{
    Connection, FieldInfo, InheritanceStrategy, Model, Row, TransactionOps, Value,
};
use std::marker::PhantomData;

fn is_joined_inheritance_child<M: Model>() -> bool {
    let inh = M::inheritance();
    inh.strategy == InheritanceStrategy::Joined && inh.parent.is_some()
}

fn build_insert_sql_for_table(
    dialect: Dialect,
    table: &str,
    fields: &[FieldInfo],
    row: &[(&'static str, Value)],
    returning: Option<&str>,
) -> (String, Vec<Value>) {
    let insert_fields: Vec<_> = row
        .iter()
        .map(|(name, value)| {
            let field = fields.iter().find(|f| f.column_name == *name);
            if let Some(f) = field {
                if f.auto_increment && matches!(value, Value::Null) {
                    return (*name, Value::Default);
                }
            }
            (*name, value.clone())
        })
        .collect();

    let mut columns = Vec::new();
    let mut placeholders = Vec::new();
    let mut params = Vec::new();

    for (name, value) in insert_fields {
        if matches!(value, Value::Default) && dialect == Dialect::Sqlite {
            // SQLite doesn't allow DEFAULT in VALUES; omit the column to trigger defaults.
            continue;
        }

        columns.push(name);

        if matches!(value, Value::Default) {
            placeholders.push("DEFAULT".to_string());
        } else {
            params.push(value);
            placeholders.push(dialect.placeholder(params.len()));
        }
    }

    let mut sql = if columns.is_empty() {
        format!("INSERT INTO {} DEFAULT VALUES", table)
    } else {
        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table,
            columns.join(", "),
            placeholders.join(", ")
        )
    };

    if let Some(ret) = returning {
        sql.push_str(" RETURNING ");
        sql.push_str(ret);
    }

    (sql, params)
}

fn build_update_sql_for_table(
    dialect: Dialect,
    table: &str,
    pk_cols: &[&'static str],
    pk_vals: &[Value],
    set_pairs: &[(&'static str, Value)],
) -> (String, Vec<Value>) {
    let mut params = Vec::new();
    let mut set_clauses = Vec::new();
    for (col, value) in set_pairs {
        set_clauses.push(format!(
            "{} = {}",
            col,
            dialect.placeholder(params.len() + 1)
        ));
        params.push(value.clone());
    }

    if set_clauses.is_empty() {
        return (String::new(), Vec::new());
    }

    let mut sql = format!("UPDATE {} SET {}", table, set_clauses.join(", "));
    if !pk_cols.is_empty() && pk_cols.len() == pk_vals.len() {
        let where_parts: Vec<String> = pk_cols
            .iter()
            .enumerate()
            .map(|(i, col)| format!("{} = {}", col, dialect.placeholder(params.len() + i + 1)))
            .collect();
        sql.push_str(" WHERE ");
        sql.push_str(&where_parts.join(" AND "));
        params.extend_from_slice(pk_vals);
    }

    (sql, params)
}

fn build_delete_sql_for_table(
    dialect: Dialect,
    table: &str,
    pk_cols: &[&'static str],
    pk_vals: &[Value],
) -> (String, Vec<Value>) {
    let mut sql = format!("DELETE FROM {}", table);
    let mut params = Vec::new();
    if !pk_cols.is_empty() && pk_cols.len() == pk_vals.len() {
        let where_parts: Vec<String> = pk_cols
            .iter()
            .enumerate()
            .map(|(i, col)| format!("{} = {}", col, dialect.placeholder(i + 1)))
            .collect();
        sql.push_str(" WHERE ");
        sql.push_str(&where_parts.join(" AND "));
        params.extend_from_slice(pk_vals);
    }
    (sql, params)
}

fn extract_single_pk_i64(pk_vals: &[Value]) -> Option<i64> {
    if pk_vals.len() != 1 {
        return None;
    }
    match &pk_vals[0] {
        Value::BigInt(v) => Some(*v),
        Value::Int(v) => Some(i64::from(*v)),
        _ => None,
    }
}

async fn tx_rollback_best_effort<Tx: TransactionOps>(tx: Tx, cx: &Cx) {
    let _ = tx.rollback(cx).await;
}

/// Conflict resolution strategy for INSERT operations.
///
/// Used with PostgreSQL's ON CONFLICT clause for UPSERT operations.
#[derive(Debug, Clone)]
pub enum OnConflict {
    /// Do nothing on conflict (INSERT ... ON CONFLICT DO NOTHING)
    DoNothing,
    /// Update specified columns on conflict (INSERT ... ON CONFLICT DO UPDATE SET ...)
    DoUpdate {
        /// The columns to update. If empty, all non-primary-key columns are updated.
        columns: Vec<String>,
        /// The conflict target (column names). If empty, uses primary key.
        target: Vec<String>,
    },
}

/// INSERT query builder.
///
/// # Example
///
/// ```ignore
/// // Simple insert
/// let id = insert!(hero).execute(cx, &conn).await?;
///
/// // Insert with RETURNING
/// let row = insert!(hero).returning().execute_returning(cx, &conn).await?;
///
/// // Insert with UPSERT
/// let id = insert!(hero)
///     .on_conflict_do_nothing()
///     .execute(cx, &conn).await?;
/// ```
#[derive(Debug)]
pub struct InsertBuilder<'a, M: Model> {
    model: &'a M,
    returning: bool,
    on_conflict: Option<OnConflict>,
}

impl<'a, M: Model> InsertBuilder<'a, M> {
    /// Create a new INSERT builder for the given model instance.
    pub fn new(model: &'a M) -> Self {
        Self {
            model,
            returning: false,
            on_conflict: None,
        }
    }

    /// Add RETURNING * clause to return the inserted row.
    ///
    /// Use with `execute_returning()` to get the inserted row.
    pub fn returning(mut self) -> Self {
        self.returning = true;
        self
    }

    /// Handle conflicts by doing nothing (PostgreSQL ON CONFLICT DO NOTHING).
    ///
    /// This allows the insert to silently succeed even if it would violate
    /// a unique constraint.
    pub fn on_conflict_do_nothing(mut self) -> Self {
        self.on_conflict = Some(OnConflict::DoNothing);
        self
    }

    /// Handle conflicts by updating specified columns (UPSERT).
    ///
    /// If `columns` is empty, all non-primary-key columns are updated.
    /// The conflict target defaults to the primary key.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Update name and age on conflict
    /// insert!(hero)
    ///     .on_conflict_do_update(&["name", "age"])
    ///     .execute(cx, &conn).await?;
    /// ```
    pub fn on_conflict_do_update(mut self, columns: &[&str]) -> Self {
        self.on_conflict = Some(OnConflict::DoUpdate {
            columns: columns.iter().map(|s| s.to_string()).collect(),
            target: Vec::new(), // Default to primary key
        });
        self
    }

    /// Handle conflicts by updating columns with a specific conflict target.
    ///
    /// # Arguments
    ///
    /// * `target` - The columns that form the unique constraint to match
    /// * `columns` - The columns to update on conflict
    pub fn on_conflict_target_do_update(mut self, target: &[&str], columns: &[&str]) -> Self {
        self.on_conflict = Some(OnConflict::DoUpdate {
            columns: columns.iter().map(|s| s.to_string()).collect(),
            target: target.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    /// Build the INSERT SQL and parameters with default dialect (Postgres).
    pub fn build(&self) -> (String, Vec<Value>) {
        self.build_with_dialect(Dialect::default())
    }

    /// Build the INSERT SQL and parameters with specific dialect.
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let row = self.model.to_row();
        let fields = M::fields();

        let insert_fields: Vec<_> = row
            .iter()
            .map(|(name, value)| {
                let field = fields.iter().find(|f| f.column_name == *name);
                if let Some(f) = field {
                    if f.auto_increment && matches!(value, Value::Null) {
                        return (*name, Value::Default);
                    }
                }
                (*name, value.clone())
            })
            .collect();

        let mut columns = Vec::new();
        let mut placeholders = Vec::new();
        let mut params = Vec::new();

        for (name, value) in insert_fields {
            if matches!(value, Value::Default) && dialect == Dialect::Sqlite {
                // SQLite doesn't allow DEFAULT in VALUES; omit the column to trigger defaults.
                continue;
            }

            columns.push(name);

            if matches!(value, Value::Default) {
                placeholders.push("DEFAULT".to_string());
            } else {
                params.push(value);
                placeholders.push(dialect.placeholder(params.len()));
            }
        }

        let mut sql = if columns.is_empty() {
            format!("INSERT INTO {} DEFAULT VALUES", M::TABLE_NAME)
        } else {
            format!(
                "INSERT INTO {} ({}) VALUES ({})",
                M::TABLE_NAME,
                columns.join(", "),
                placeholders.join(", ")
            )
        };

        // Add ON CONFLICT clause if specified
        if let Some(on_conflict) = &self.on_conflict {
            match on_conflict {
                OnConflict::DoNothing => {
                    sql.push_str(" ON CONFLICT DO NOTHING");
                }
                OnConflict::DoUpdate {
                    columns: update_cols,
                    target,
                } => {
                    sql.push_str(" ON CONFLICT");

                    // Add target if specified, otherwise use primary key
                    // PostgreSQL requires a conflict target for DO UPDATE
                    let has_target = if !target.is_empty() {
                        sql.push_str(" (");
                        sql.push_str(&target.join(", "));
                        sql.push(')');
                        true
                    } else if !M::PRIMARY_KEY.is_empty() {
                        sql.push_str(" (");
                        sql.push_str(&M::PRIMARY_KEY.join(", "));
                        sql.push(')');
                        true
                    } else {
                        // No conflict target available - fall back to DO NOTHING
                        // since PostgreSQL requires a target for DO UPDATE
                        tracing::warn!(
                            table = M::TABLE_NAME,
                            "ON CONFLICT DO UPDATE requires a conflict target (primary key or explicit target). \
                             Falling back to DO NOTHING since no target is available."
                        );
                        sql.push_str(" DO NOTHING");
                        false
                    };

                    // Only add DO UPDATE SET if we have a valid conflict target
                    if has_target {
                        // If columns is empty, update all non-PK columns
                        let update_set: Vec<String> = if update_cols.is_empty() {
                            columns // Use full column list from explicit insert
                                .iter()
                                .filter(|name| !M::PRIMARY_KEY.contains(name)) // Don't update PK
                                .map(|name| format!("{} = EXCLUDED.{}", name, name))
                                .collect()
                        } else {
                            update_cols
                                .iter()
                                .map(|c| format!("{} = EXCLUDED.{}", c, c))
                                .collect()
                        };

                        if update_set.is_empty() {
                            tracing::warn!(
                                table = M::TABLE_NAME,
                                "ON CONFLICT DO UPDATE has no updatable columns. Falling back to DO NOTHING."
                            );
                            sql.push_str(" DO NOTHING");
                        } else {
                            sql.push_str(" DO UPDATE SET ");
                            sql.push_str(&update_set.join(", "));
                        }
                    }
                }
            }
        }

        // Add RETURNING clause if requested
        if self.returning {
            sql.push_str(" RETURNING *");
        }

        (sql, params)
    }

    /// Execute the INSERT and return the inserted ID.
    pub async fn execute<C: Connection>(
        self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<i64, sqlmodel_core::Error> {
        if is_joined_inheritance_child::<M>() {
            if self.on_conflict.is_some() {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance insert does not support ON CONFLICT yet (bd-2bht follow-ups may extend this)"
                        .to_string(),
                ));
            }

            let dialect = conn.dialect();
            let inh = M::inheritance();
            let Some(parent_table) = inh.parent else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance child missing parent table metadata".to_string(),
                ));
            };
            let Some(parent_fields_fn) = inh.parent_fields_fn else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance child missing parent_fields_fn metadata".to_string(),
                ));
            };
            let parent_fields = parent_fields_fn();

            let Some(parent_row) = self.model.joined_parent_row() else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance child missing joined_parent_row() implementation"
                        .to_string(),
                ));
            };

            let pk_vals = self.model.primary_key_value();
            let pk_col = M::PRIMARY_KEY.first().copied();
            let needs_generated_id = pk_col.is_some()
                && pk_vals.len() == 1
                && parent_fields
                    .iter()
                    .find(|f| f.column_name == pk_col.unwrap_or("") && f.primary_key)
                    .is_some_and(|f| f.auto_increment)
                && pk_vals[0].is_null();

            let tx_out = conn.begin(cx).await;
            let tx = match tx_out {
                Outcome::Ok(t) => t,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };

            // 1) Insert base row (parent table), possibly retrieving the generated PK.
            let mut inserted_id: Option<i64> = None;
            if dialect == Dialect::Postgres {
                let Some(pk_col) = pk_col else {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Err(sqlmodel_core::Error::Custom(
                        "joined-table inheritance insert requires a primary key column".to_string(),
                    ));
                };

                let (sql, params) = build_insert_sql_for_table(
                    dialect,
                    parent_table,
                    parent_fields,
                    &parent_row,
                    Some(pk_col),
                );
                match tx.query_one(cx, &sql, &params).await {
                    Outcome::Ok(Some(row)) => match row.get_as::<i64>(0) {
                        Ok(v) => inserted_id = Some(v),
                        Err(e) => {
                            tx_rollback_best_effort(tx, cx).await;
                            return Outcome::Err(e);
                        }
                    },
                    Outcome::Ok(None) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Err(sqlmodel_core::Error::Custom(
                            "base insert returned no row".to_string(),
                        ));
                    }
                    Outcome::Err(e) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Err(e);
                    }
                    Outcome::Cancelled(r) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Cancelled(r);
                    }
                    Outcome::Panicked(p) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Panicked(p);
                    }
                }
            } else {
                let (sql, params) = build_insert_sql_for_table(
                    dialect,
                    parent_table,
                    parent_fields,
                    &parent_row,
                    None,
                );
                match tx.execute(cx, &sql, &params).await {
                    Outcome::Ok(_) => {}
                    Outcome::Err(e) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Err(e);
                    }
                    Outcome::Cancelled(r) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Cancelled(r);
                    }
                    Outcome::Panicked(p) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Panicked(p);
                    }
                }

                if needs_generated_id {
                    let id_sql = match dialect {
                        Dialect::Sqlite => "SELECT last_insert_rowid()",
                        Dialect::Mysql => "SELECT LAST_INSERT_ID()",
                        Dialect::Postgres => unreachable!(),
                    };
                    match tx.query_one(cx, id_sql, &[]).await {
                        Outcome::Ok(Some(row)) => match row.get_as::<i64>(0) {
                            Ok(v) => inserted_id = Some(v),
                            Err(e) => {
                                tx_rollback_best_effort(tx, cx).await;
                                return Outcome::Err(e);
                            }
                        },
                        Outcome::Ok(None) => {
                            tx_rollback_best_effort(tx, cx).await;
                            return Outcome::Err(sqlmodel_core::Error::Custom(
                                "failed to fetch last insert id".to_string(),
                            ));
                        }
                        Outcome::Err(e) => {
                            tx_rollback_best_effort(tx, cx).await;
                            return Outcome::Err(e);
                        }
                        Outcome::Cancelled(r) => {
                            tx_rollback_best_effort(tx, cx).await;
                            return Outcome::Cancelled(r);
                        }
                        Outcome::Panicked(p) => {
                            tx_rollback_best_effort(tx, cx).await;
                            return Outcome::Panicked(p);
                        }
                    }
                }
            }

            // 2) Insert child row (child table), patching PK if it was generated by base insert.
            let mut child_row = self.model.to_row();
            if let (Some(pk_col), Some(id)) = (pk_col, inserted_id) {
                if M::PRIMARY_KEY.len() != 1 {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Err(sqlmodel_core::Error::Custom(
                        "joined-table inheritance auto-increment insert currently requires a single-column primary key"
                            .to_string(),
                    ));
                }

                for (name, value) in &mut child_row {
                    if *name == pk_col && value.is_null() {
                        *value = Value::BigInt(id);
                    }
                }
            }

            let (child_sql, child_params) =
                build_insert_sql_for_table(dialect, M::TABLE_NAME, M::fields(), &child_row, None);
            match tx.execute(cx, &child_sql, &child_params).await {
                Outcome::Ok(_) => {}
                Outcome::Err(e) => {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Err(e);
                }
                Outcome::Cancelled(r) => {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Cancelled(r);
                }
                Outcome::Panicked(p) => {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Panicked(p);
                }
            }

            match tx.commit(cx).await {
                Outcome::Ok(()) => {}
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            }

            let id = inserted_id
                .or_else(|| extract_single_pk_i64(&pk_vals))
                .unwrap_or(0);
            return Outcome::Ok(id);
        }

        let (sql, params) = self.build_with_dialect(conn.dialect());
        conn.insert(cx, &sql, &params).await
    }

    /// Execute the INSERT with RETURNING and get the inserted row.
    ///
    /// This automatically adds RETURNING * and returns the full row.
    pub async fn execute_returning<C: Connection>(
        mut self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Option<Row>, sqlmodel_core::Error> {
        self.returning = true;
        if is_joined_inheritance_child::<M>() {
            if self.on_conflict.is_some() {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance insert does not support ON CONFLICT yet (bd-2bht follow-ups may extend this)"
                        .to_string(),
                ));
            }

            let dialect = conn.dialect();
            let inh = M::inheritance();
            let Some(parent_table) = inh.parent else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance child missing parent table metadata".to_string(),
                ));
            };
            let Some(parent_fields_fn) = inh.parent_fields_fn else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance child missing parent_fields_fn metadata".to_string(),
                ));
            };
            let parent_fields = parent_fields_fn();

            let Some(parent_row) = self.model.joined_parent_row() else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance child missing joined_parent_row() implementation"
                        .to_string(),
                ));
            };

            let pk_vals = self.model.primary_key_value();
            let pk_col = M::PRIMARY_KEY.first().copied();
            let needs_generated_id = pk_col.is_some()
                && pk_vals.len() == 1
                && parent_fields
                    .iter()
                    .find(|f| f.column_name == pk_col.unwrap_or("") && f.primary_key)
                    .is_some_and(|f| f.auto_increment)
                && pk_vals[0].is_null();

            let tx_out = conn.begin(cx).await;
            let tx = match tx_out {
                Outcome::Ok(t) => t,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };

            let mut inserted_id: Option<i64> = None;
            if dialect == Dialect::Postgres {
                let Some(pk_col) = pk_col else {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Err(sqlmodel_core::Error::Custom(
                        "joined-table inheritance insert requires a primary key column".to_string(),
                    ));
                };

                let (sql, params) = build_insert_sql_for_table(
                    dialect,
                    parent_table,
                    parent_fields,
                    &parent_row,
                    Some(pk_col),
                );
                match tx.query_one(cx, &sql, &params).await {
                    Outcome::Ok(Some(row)) => match row.get_as::<i64>(0) {
                        Ok(v) => inserted_id = Some(v),
                        Err(e) => {
                            tx_rollback_best_effort(tx, cx).await;
                            return Outcome::Err(e);
                        }
                    },
                    Outcome::Ok(None) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Err(sqlmodel_core::Error::Custom(
                            "base insert returned no row".to_string(),
                        ));
                    }
                    Outcome::Err(e) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Err(e);
                    }
                    Outcome::Cancelled(r) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Cancelled(r);
                    }
                    Outcome::Panicked(p) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Panicked(p);
                    }
                }
            } else {
                let (sql, params) = build_insert_sql_for_table(
                    dialect,
                    parent_table,
                    parent_fields,
                    &parent_row,
                    None,
                );
                match tx.execute(cx, &sql, &params).await {
                    Outcome::Ok(_) => {}
                    Outcome::Err(e) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Err(e);
                    }
                    Outcome::Cancelled(r) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Cancelled(r);
                    }
                    Outcome::Panicked(p) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Panicked(p);
                    }
                }

                if needs_generated_id {
                    let id_sql = match dialect {
                        Dialect::Sqlite => "SELECT last_insert_rowid()",
                        Dialect::Mysql => "SELECT LAST_INSERT_ID()",
                        Dialect::Postgres => unreachable!(),
                    };
                    match tx.query_one(cx, id_sql, &[]).await {
                        Outcome::Ok(Some(row)) => match row.get_as::<i64>(0) {
                            Ok(v) => inserted_id = Some(v),
                            Err(e) => {
                                tx_rollback_best_effort(tx, cx).await;
                                return Outcome::Err(e);
                            }
                        },
                        Outcome::Ok(None) => {
                            tx_rollback_best_effort(tx, cx).await;
                            return Outcome::Err(sqlmodel_core::Error::Custom(
                                "failed to fetch last insert id".to_string(),
                            ));
                        }
                        Outcome::Err(e) => {
                            tx_rollback_best_effort(tx, cx).await;
                            return Outcome::Err(e);
                        }
                        Outcome::Cancelled(r) => {
                            tx_rollback_best_effort(tx, cx).await;
                            return Outcome::Cancelled(r);
                        }
                        Outcome::Panicked(p) => {
                            tx_rollback_best_effort(tx, cx).await;
                            return Outcome::Panicked(p);
                        }
                    }
                }
            }

            let mut child_row = self.model.to_row();
            if let (Some(pk_col), Some(id)) = (pk_col, inserted_id) {
                if M::PRIMARY_KEY.len() != 1 {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Err(sqlmodel_core::Error::Custom(
                        "joined-table inheritance auto-increment insert currently requires a single-column primary key"
                            .to_string(),
                    ));
                }

                for (name, value) in &mut child_row {
                    if *name == pk_col && value.is_null() {
                        *value = Value::BigInt(id);
                    }
                }
            }

            let (child_sql, child_params) = build_insert_sql_for_table(
                dialect,
                M::TABLE_NAME,
                M::fields(),
                &child_row,
                Some("*"),
            );
            let row_out = match tx.query_one(cx, &child_sql, &child_params).await {
                Outcome::Ok(row) => Outcome::Ok(row),
                Outcome::Err(e) => {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Err(e);
                }
                Outcome::Cancelled(r) => {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Cancelled(r);
                }
                Outcome::Panicked(p) => {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Panicked(p);
                }
            };

            match tx.commit(cx).await {
                Outcome::Ok(()) => {}
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            }

            return row_out;
        }

        let (sql, params) = self.build_with_dialect(conn.dialect());
        conn.query_one(cx, &sql, &params).await
    }
}

/// Bulk INSERT query builder.
///
/// # Example
///
/// ```ignore
/// let heroes = vec![hero1, hero2, hero3];
/// let ids = insert_many!(heroes)
///     .execute(cx, &conn).await?;
/// ```
#[derive(Debug)]
pub struct InsertManyBuilder<'a, M: Model> {
    models: &'a [M],
    returning: bool,
    on_conflict: Option<OnConflict>,
}

impl<'a, M: Model> InsertManyBuilder<'a, M> {
    /// Create a new bulk INSERT builder for the given model instances.
    pub fn new(models: &'a [M]) -> Self {
        Self {
            models,
            returning: false,
            on_conflict: None,
        }
    }

    /// Add RETURNING * clause to return the inserted rows.
    pub fn returning(mut self) -> Self {
        self.returning = true;
        self
    }

    /// Handle conflicts by doing nothing.
    pub fn on_conflict_do_nothing(mut self) -> Self {
        self.on_conflict = Some(OnConflict::DoNothing);
        self
    }

    /// Handle conflicts by updating specified columns.
    pub fn on_conflict_do_update(mut self, columns: &[&str]) -> Self {
        self.on_conflict = Some(OnConflict::DoUpdate {
            columns: columns.iter().map(|s| s.to_string()).collect(),
            target: Vec::new(),
        });
        self
    }

    /// Build the bulk INSERT SQL and parameters with default dialect.
    pub fn build(&self) -> (String, Vec<Value>) {
        self.build_with_dialect(Dialect::default())
    }

    /// Build the bulk INSERT SQL and parameters with specific dialect.
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let batches = self.build_batches_with_dialect(dialect);
        match batches.len() {
            0 => (String::new(), Vec::new()),
            1 => batches.into_iter().next().unwrap(),
            _ => {
                tracing::warn!(
                    table = M::TABLE_NAME,
                    "Bulk insert requires multiple statements for this dialect. \
                     Use build_batches_with_dialect or execute() instead of build_with_dialect."
                );
                (String::new(), Vec::new())
            }
        }
    }

    /// Build bulk INSERT statements for the given dialect.
    ///
    /// SQLite requires column omission when defaults are used, which can
    /// produce multiple statements to preserve correct semantics.
    pub fn build_batches_with_dialect(&self, dialect: Dialect) -> Vec<(String, Vec<Value>)> {
        enum Batch {
            Values {
                columns: Vec<&'static str>,
                rows: Vec<Vec<Value>>,
            },
            DefaultValues,
        }

        if self.models.is_empty() {
            return Vec::new();
        }

        if dialect != Dialect::Sqlite {
            return vec![self.build_single_with_dialect(dialect)];
        }

        let fields = M::fields();
        let rows: Vec<Vec<(&'static str, Value)>> =
            self.models.iter().map(|model| model.to_row()).collect();

        // Determine which columns to insert (preserve field order)
        let insert_columns: Vec<_> = fields
            .iter()
            .filter_map(|field| {
                if field.auto_increment {
                    return Some(field.column_name);
                }
                let has_value = rows.iter().any(|row| {
                    row.iter()
                        .find(|(name, _)| name == &field.column_name)
                        .is_some_and(|(_, v)| !matches!(v, Value::Null))
                });
                if has_value {
                    Some(field.column_name)
                } else {
                    None
                }
            })
            .collect();

        let mut batches: Vec<Batch> = Vec::new();

        for row in &rows {
            let mut columns_for_row = Vec::new();
            let mut values_for_row = Vec::new();

            for col in &insert_columns {
                let mut val = row
                    .iter()
                    .find(|(name, _)| name == col)
                    .map_or(Value::Null, |(_, v)| v.clone());

                // Map Null auto-increment fields to DEFAULT
                if let Some(f) = fields.iter().find(|f| f.column_name == *col) {
                    if f.auto_increment && matches!(val, Value::Null) {
                        val = Value::Default;
                    }
                }

                if matches!(val, Value::Default) {
                    continue;
                }

                columns_for_row.push(*col);
                values_for_row.push(val);
            }

            if columns_for_row.is_empty() {
                batches.push(Batch::DefaultValues);
                continue;
            }

            match batches.last_mut() {
                Some(Batch::Values { columns, rows }) if *columns == columns_for_row => {
                    rows.push(values_for_row);
                }
                _ => batches.push(Batch::Values {
                    columns: columns_for_row,
                    rows: vec![values_for_row],
                }),
            }
        }

        let mut statements = Vec::new();

        for batch in batches {
            match batch {
                Batch::DefaultValues => {
                    let mut sql = format!("INSERT INTO {} DEFAULT VALUES", M::TABLE_NAME);
                    self.append_on_conflict(&mut sql, &[]);
                    self.append_returning(&mut sql);
                    statements.push((sql, Vec::new()));
                }
                Batch::Values { columns, rows } => {
                    let (sql, params) = self.build_values_batch_sql(dialect, &columns, &rows);
                    statements.push((sql, params));
                }
            }
        }

        statements
    }

    fn build_single_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let fields = M::fields();
        let rows: Vec<Vec<(&'static str, Value)>> =
            self.models.iter().map(|model| model.to_row()).collect();

        // Determine which columns to insert
        // Always include auto-increment fields, include non-null values seen in any row.
        let insert_columns: Vec<_> = fields
            .iter()
            .filter_map(|field| {
                if field.auto_increment {
                    return Some(field.column_name);
                }
                let has_value = rows.iter().any(|row| {
                    row.iter()
                        .find(|(name, _)| name == &field.column_name)
                        .is_some_and(|(_, v)| !matches!(v, Value::Null))
                });
                if has_value {
                    Some(field.column_name)
                } else {
                    None
                }
            })
            .collect();

        let mut all_values = Vec::new();
        let mut value_groups = Vec::new();

        for row in &rows {
            let values: Vec<_> = insert_columns
                .iter()
                .map(|col| {
                    let val = row
                        .iter()
                        .find(|(name, _)| name == col)
                        .map_or(Value::Null, |(_, v)| v.clone());

                    // Map Null auto-increment fields to DEFAULT
                    let field = fields.iter().find(|f| f.column_name == *col);
                    if let Some(f) = field {
                        if f.auto_increment && matches!(val, Value::Null) {
                            return Value::Default;
                        }
                    }
                    val
                })
                .collect();

            let mut placeholders = Vec::new();
            for v in &values {
                if matches!(v, Value::Default) {
                    placeholders.push("DEFAULT".to_string());
                } else {
                    all_values.push(v.clone());
                    placeholders.push(dialect.placeholder(all_values.len()));
                }
            }

            value_groups.push(format!("({})", placeholders.join(", ")));
        }

        let mut sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            M::TABLE_NAME,
            insert_columns.join(", "),
            value_groups.join(", ")
        );

        self.append_on_conflict(&mut sql, &insert_columns);
        self.append_returning(&mut sql);

        (sql, all_values)
    }

    fn build_values_batch_sql(
        &self,
        dialect: Dialect,
        columns: &[&'static str],
        rows: &[Vec<Value>],
    ) -> (String, Vec<Value>) {
        let mut params = Vec::new();
        let mut value_groups = Vec::new();

        for row in rows {
            let mut placeholders = Vec::new();
            for value in row {
                if matches!(value, Value::Default) {
                    placeholders.push("DEFAULT".to_string());
                } else {
                    params.push(value.clone());
                    placeholders.push(dialect.placeholder(params.len()));
                }
            }
            value_groups.push(format!("({})", placeholders.join(", ")));
        }

        let mut sql = if columns.is_empty() {
            format!("INSERT INTO {} DEFAULT VALUES", M::TABLE_NAME)
        } else {
            format!(
                "INSERT INTO {} ({}) VALUES {}",
                M::TABLE_NAME,
                columns.join(", "),
                value_groups.join(", ")
            )
        };

        self.append_on_conflict(&mut sql, columns);
        self.append_returning(&mut sql);

        (sql, params)
    }

    fn append_on_conflict(&self, sql: &mut String, insert_columns: &[&'static str]) {
        if let Some(on_conflict) = &self.on_conflict {
            match on_conflict {
                OnConflict::DoNothing => {
                    sql.push_str(" ON CONFLICT DO NOTHING");
                }
                OnConflict::DoUpdate { columns, target } => {
                    sql.push_str(" ON CONFLICT");

                    // PostgreSQL requires a conflict target for DO UPDATE
                    let has_target = if !target.is_empty() {
                        sql.push_str(" (");
                        sql.push_str(&target.join(", "));
                        sql.push(')');
                        true
                    } else if !M::PRIMARY_KEY.is_empty() {
                        sql.push_str(" (");
                        sql.push_str(&M::PRIMARY_KEY.join(", "));
                        sql.push(')');
                        true
                    } else {
                        // No conflict target available - fall back to DO NOTHING
                        tracing::warn!(
                            table = M::TABLE_NAME,
                            "ON CONFLICT DO UPDATE requires a conflict target (primary key or explicit target). \
                             Falling back to DO NOTHING since no target is available."
                        );
                        sql.push_str(" DO NOTHING");
                        false
                    };

                    // Only add DO UPDATE SET if we have a valid conflict target
                    if has_target {
                        let update_cols: Vec<String> = if columns.is_empty() {
                            insert_columns
                                .iter()
                                .filter(|name| !M::PRIMARY_KEY.contains(name))
                                .map(|name| format!("{} = EXCLUDED.{}", name, name))
                                .collect()
                        } else {
                            columns
                                .iter()
                                .map(|c| format!("{} = EXCLUDED.{}", c, c))
                                .collect()
                        };

                        if update_cols.is_empty() {
                            tracing::warn!(
                                table = M::TABLE_NAME,
                                "ON CONFLICT DO UPDATE has no updatable columns. Falling back to DO NOTHING."
                            );
                            sql.push_str(" DO NOTHING");
                        } else {
                            sql.push_str(" DO UPDATE SET ");
                            sql.push_str(&update_cols.join(", "));
                        }
                    }
                }
            }
        }
    }

    fn append_returning(&self, sql: &mut String) {
        if self.returning {
            sql.push_str(" RETURNING *");
        }
    }

    /// Execute the bulk INSERT and return rows affected.
    pub async fn execute<C: Connection>(
        self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<u64, sqlmodel_core::Error> {
        let batches = self.build_batches_with_dialect(conn.dialect());
        if batches.is_empty() {
            return Outcome::Ok(0);
        }

        if batches.len() == 1 {
            let (sql, params) = &batches[0];
            return conn.execute(cx, sql, params).await;
        }

        let outcome = conn.batch(cx, &batches).await;
        outcome.map(|counts| counts.into_iter().sum())
    }

    /// Execute the bulk INSERT with RETURNING and get the inserted rows.
    pub async fn execute_returning<C: Connection>(
        mut self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Vec<Row>, sqlmodel_core::Error> {
        self.returning = true;
        let batches = self.build_batches_with_dialect(conn.dialect());
        if batches.is_empty() {
            return Outcome::Ok(Vec::new());
        }

        let mut all_rows = Vec::new();
        for (sql, params) in batches {
            match conn.query(cx, &sql, &params).await {
                Outcome::Ok(mut rows) => all_rows.append(&mut rows),
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            }
        }

        Outcome::Ok(all_rows)
    }
}

/// A column-value pair for explicit UPDATE SET operations.
#[derive(Debug, Clone)]
pub struct SetClause {
    column: String,
    value: Value,
}

/// UPDATE query builder.
///
/// # Example
///
/// ```ignore
/// // Update a model instance (uses primary key for WHERE)
/// update!(hero).execute(cx, &conn).await?;
///
/// // Update with explicit SET
/// UpdateBuilder::<Hero>::empty()
///     .set("age", 26)
///     .set("name", "New Name")
///     .filter(Expr::col("id").eq(42))
///     .execute(cx, &conn).await?;
///
/// // Update with RETURNING
/// let row = update!(hero).returning().execute_returning(cx, &conn).await?;
/// ```
#[derive(Debug)]
pub struct UpdateBuilder<'a, M: Model> {
    model: Option<&'a M>,
    where_clause: Option<Where>,
    set_fields: Option<Vec<&'static str>>,
    explicit_sets: Vec<SetClause>,
    returning: bool,
}

impl<'a, M: Model> UpdateBuilder<'a, M> {
    /// Create a new UPDATE builder for the given model instance.
    pub fn new(model: &'a M) -> Self {
        Self {
            model: Some(model),
            where_clause: None,
            set_fields: None,
            explicit_sets: Vec::new(),
            returning: false,
        }
    }

    /// Create an empty UPDATE builder for explicit SET operations.
    ///
    /// Use this when you want to update specific columns without a model instance.
    pub fn empty() -> Self {
        Self {
            model: None,
            where_clause: None,
            set_fields: None,
            explicit_sets: Vec::new(),
            returning: false,
        }
    }

    /// Set a column to a specific value.
    ///
    /// This can be used with or without a model instance.
    /// When used with a model, these explicit sets override the model values.
    pub fn set<V: Into<Value>>(mut self, column: &str, value: V) -> Self {
        self.explicit_sets.push(SetClause {
            column: column.to_string(),
            value: value.into(),
        });
        self
    }

    /// Only update specific fields from the model.
    pub fn set_only(mut self, fields: &[&'static str]) -> Self {
        self.set_fields = Some(fields.to_vec());
        self
    }

    /// Add a WHERE condition (defaults to primary key match).
    pub fn filter(mut self, expr: Expr) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => existing.and(expr),
            None => Where::new(expr),
        });
        self
    }

    /// Add RETURNING * clause to return the updated row(s).
    pub fn returning(mut self) -> Self {
        self.returning = true;
        self
    }

    /// Build the UPDATE SQL and parameters with default dialect (Postgres).
    pub fn build(&self) -> (String, Vec<Value>) {
        self.build_with_dialect(Dialect::default())
    }

    /// Build the UPDATE SQL and parameters with specific dialect.
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let pk = M::PRIMARY_KEY;
        let mut params = Vec::new();
        let mut set_clauses = Vec::new();

        // First, add explicit SET clauses
        for set in &self.explicit_sets {
            set_clauses.push(format!(
                "{} = {}",
                set.column,
                dialect.placeholder(params.len() + 1)
            ));
            params.push(set.value.clone());
        }

        // Then, add model fields if we have a model
        if let Some(model) = &self.model {
            let row = model.to_row();

            // Determine which fields to update
            let update_fields: Vec<_> = row
                .iter()
                .filter(|(name, _)| {
                    // Skip primary key fields
                    if pk.contains(name) {
                        return false;
                    }
                    // Skip columns that have explicit sets
                    if self.explicit_sets.iter().any(|s| s.column == *name) {
                        return false;
                    }
                    // If set_only specified, only include those fields
                    if let Some(fields) = &self.set_fields {
                        return fields.contains(name);
                    }
                    true
                })
                .collect();

            for (name, value) in update_fields {
                set_clauses.push(format!(
                    "{} = {}",
                    name,
                    dialect.placeholder(params.len() + 1)
                ));
                params.push(value.clone());
            }
        }

        if set_clauses.is_empty() {
            // Nothing to update - return empty SQL
            return (String::new(), Vec::new());
        }

        let mut sql = format!("UPDATE {} SET {}", M::TABLE_NAME, set_clauses.join(", "));

        // Add WHERE clause
        if let Some(where_clause) = &self.where_clause {
            let (where_sql, where_params) = where_clause.build_with_dialect(dialect, params.len());
            sql.push_str(" WHERE ");
            sql.push_str(&where_sql);
            params.extend(where_params);
        } else if let Some(model) = &self.model {
            // Default to primary key match
            let pk_values = model.primary_key_value();
            let pk_conditions: Vec<_> = pk
                .iter()
                .zip(pk_values.iter())
                .enumerate()
                .map(|(i, (col, _))| {
                    format!("{} = {}", col, dialect.placeholder(params.len() + i + 1))
                })
                .collect();

            if !pk_conditions.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&pk_conditions.join(" AND "));
                params.extend(pk_values);
            }
        }

        // Add RETURNING clause if requested
        if self.returning {
            sql.push_str(" RETURNING *");
        }

        (sql, params)
    }

    /// Execute the UPDATE and return rows affected.
    pub async fn execute<C: Connection>(
        self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<u64, sqlmodel_core::Error> {
        if self.model.is_some() && is_joined_inheritance_child::<M>() {
            if self.where_clause.is_some() || !self.explicit_sets.is_empty() {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance update currently supports only model-based updates (no explicit WHERE/SET yet)"
                        .to_string(),
                ));
            }

            let dialect = conn.dialect();
            let Some(model) = self.model else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "update called without model".to_string(),
                ));
            };
            let inh = M::inheritance();
            let Some(parent_table) = inh.parent else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance child missing parent table metadata".to_string(),
                ));
            };
            let Some(parent_fields_fn) = inh.parent_fields_fn else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance child missing parent_fields_fn metadata".to_string(),
                ));
            };
            let parent_fields = parent_fields_fn();
            let Some(parent_row) = model.joined_parent_row() else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance child missing joined_parent_row() implementation"
                        .to_string(),
                ));
            };

            let pk_cols = M::PRIMARY_KEY;
            let pk_vals = model.primary_key_value();

            let tx_out = conn.begin(cx).await;
            let tx = match tx_out {
                Outcome::Ok(t) => t,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };

            let mut total = 0_u64;

            // Parent update (base table).
            let mut parent_sets: Vec<(&'static str, Value)> = Vec::new();
            for f in parent_fields {
                if f.primary_key || pk_cols.contains(&f.column_name) {
                    continue;
                }
                if let Some((_, v)) = parent_row.iter().find(|(k, _)| *k == f.column_name) {
                    parent_sets.push((f.column_name, v.clone()));
                }
            }
            let (parent_sql, parent_params) =
                build_update_sql_for_table(dialect, parent_table, pk_cols, &pk_vals, &parent_sets);
            if !parent_sql.is_empty() {
                match tx.execute(cx, &parent_sql, &parent_params).await {
                    Outcome::Ok(n) => total = total.saturating_add(n),
                    Outcome::Err(e) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Err(e);
                    }
                    Outcome::Cancelled(r) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Cancelled(r);
                    }
                    Outcome::Panicked(p) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Panicked(p);
                    }
                }
            }

            // Child update (child table).
            let row = model.to_row();
            let mut child_sets: Vec<(&'static str, Value)> = Vec::new();
            for (name, value) in row {
                if pk_cols.contains(&name) {
                    continue;
                }
                if let Some(fields) = &self.set_fields {
                    if !fields.contains(&name) {
                        continue;
                    }
                }
                child_sets.push((name, value));
            }
            let (child_sql, child_params) =
                build_update_sql_for_table(dialect, M::TABLE_NAME, pk_cols, &pk_vals, &child_sets);
            if !child_sql.is_empty() {
                match tx.execute(cx, &child_sql, &child_params).await {
                    Outcome::Ok(n) => total = total.saturating_add(n),
                    Outcome::Err(e) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Err(e);
                    }
                    Outcome::Cancelled(r) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Cancelled(r);
                    }
                    Outcome::Panicked(p) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Panicked(p);
                    }
                }
            }

            match tx.commit(cx).await {
                Outcome::Ok(()) => Outcome::Ok(total),
                Outcome::Err(e) => Outcome::Err(e),
                Outcome::Cancelled(r) => Outcome::Cancelled(r),
                Outcome::Panicked(p) => Outcome::Panicked(p),
            }
        } else {
            let (sql, params) = self.build_with_dialect(conn.dialect());
            if sql.is_empty() {
                return Outcome::Ok(0);
            }
            conn.execute(cx, &sql, &params).await
        }
    }

    /// Execute the UPDATE with RETURNING and get the updated rows.
    pub async fn execute_returning<C: Connection>(
        mut self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Vec<Row>, sqlmodel_core::Error> {
        self.returning = true;
        if self.model.is_some() && is_joined_inheritance_child::<M>() {
            if self.where_clause.is_some() || !self.explicit_sets.is_empty() {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance update_returning currently supports only model-based updates (no explicit WHERE/SET yet)"
                        .to_string(),
                ));
            }

            let dialect = conn.dialect();
            let Some(model) = self.model else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "update_returning called without model".to_string(),
                ));
            };
            let inh = M::inheritance();
            let Some(parent_table) = inh.parent else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance child missing parent table metadata".to_string(),
                ));
            };
            let Some(parent_fields_fn) = inh.parent_fields_fn else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance child missing parent_fields_fn metadata".to_string(),
                ));
            };
            let parent_fields = parent_fields_fn();
            let Some(parent_row) = model.joined_parent_row() else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance child missing joined_parent_row() implementation"
                        .to_string(),
                ));
            };

            let pk_cols = M::PRIMARY_KEY;
            let pk_vals = model.primary_key_value();

            let tx_out = conn.begin(cx).await;
            let tx = match tx_out {
                Outcome::Ok(t) => t,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };

            // Parent update (base table) - execute only.
            let mut parent_sets: Vec<(&'static str, Value)> = Vec::new();
            for f in parent_fields {
                if f.primary_key || pk_cols.contains(&f.column_name) {
                    continue;
                }
                if let Some((_, v)) = parent_row.iter().find(|(k, _)| *k == f.column_name) {
                    parent_sets.push((f.column_name, v.clone()));
                }
            }
            let (parent_sql, parent_params) =
                build_update_sql_for_table(dialect, parent_table, pk_cols, &pk_vals, &parent_sets);
            if !parent_sql.is_empty() {
                match tx.execute(cx, &parent_sql, &parent_params).await {
                    Outcome::Ok(_) => {}
                    Outcome::Err(e) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Err(e);
                    }
                    Outcome::Cancelled(r) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Cancelled(r);
                    }
                    Outcome::Panicked(p) => {
                        tx_rollback_best_effort(tx, cx).await;
                        return Outcome::Panicked(p);
                    }
                }
            }

            // Child update with RETURNING *.
            let row = model.to_row();
            let mut child_sets: Vec<(&'static str, Value)> = Vec::new();
            for (name, value) in row {
                if pk_cols.contains(&name) {
                    continue;
                }
                if let Some(fields) = &self.set_fields {
                    if !fields.contains(&name) {
                        continue;
                    }
                }
                child_sets.push((name, value));
            }
            let (mut child_sql, child_params) =
                build_update_sql_for_table(dialect, M::TABLE_NAME, pk_cols, &pk_vals, &child_sets);
            if child_sql.is_empty() {
                tx_rollback_best_effort(tx, cx).await;
                return Outcome::Ok(Vec::new());
            }
            child_sql.push_str(" RETURNING *");

            let rows = match tx.query(cx, &child_sql, &child_params).await {
                Outcome::Ok(rows) => rows,
                Outcome::Err(e) => {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Err(e);
                }
                Outcome::Cancelled(r) => {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Cancelled(r);
                }
                Outcome::Panicked(p) => {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Panicked(p);
                }
            };

            match tx.commit(cx).await {
                Outcome::Ok(()) => Outcome::Ok(rows),
                Outcome::Err(e) => Outcome::Err(e),
                Outcome::Cancelled(r) => Outcome::Cancelled(r),
                Outcome::Panicked(p) => Outcome::Panicked(p),
            }
        } else {
            let (sql, params) = self.build_with_dialect(conn.dialect());
            if sql.is_empty() {
                return Outcome::Ok(Vec::new());
            }
            conn.query(cx, &sql, &params).await
        }
    }
}

/// DELETE query builder.
///
/// # Example
///
/// ```ignore
/// // Delete by filter
/// delete!(Hero)
///     .filter(Expr::col("age").lt(18))
///     .execute(cx, &conn).await?;
///
/// // Delete a specific model instance
/// DeleteBuilder::from_model(&hero)
///     .execute(cx, &conn).await?;
///
/// // Delete with RETURNING
/// let rows = delete!(Hero)
///     .filter(Expr::col("status").eq("inactive"))
///     .returning()
///     .execute_returning(cx, &conn).await?;
/// ```
#[derive(Debug)]
pub struct DeleteBuilder<'a, M: Model> {
    model: Option<&'a M>,
    where_clause: Option<Where>,
    returning: bool,
    _marker: PhantomData<M>,
}

impl<'a, M: Model> DeleteBuilder<'a, M> {
    /// Create a new DELETE builder for the model type.
    pub fn new() -> Self {
        Self {
            model: None,
            where_clause: None,
            returning: false,
            _marker: PhantomData,
        }
    }

    /// Create a DELETE builder for a specific model instance.
    ///
    /// This automatically adds a WHERE clause matching the primary key.
    pub fn from_model(model: &'a M) -> Self {
        Self {
            model: Some(model),
            where_clause: None,
            returning: false,
            _marker: PhantomData,
        }
    }

    /// Add a WHERE condition.
    pub fn filter(mut self, expr: Expr) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => existing.and(expr),
            None => Where::new(expr),
        });
        self
    }

    /// Add RETURNING * clause to return the deleted row(s).
    pub fn returning(mut self) -> Self {
        self.returning = true;
        self
    }

    /// Build the DELETE SQL and parameters with default dialect (Postgres).
    pub fn build(&self) -> (String, Vec<Value>) {
        self.build_with_dialect(Dialect::default())
    }

    /// Build the DELETE SQL and parameters with specific dialect.
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let mut sql = format!("DELETE FROM {}", M::TABLE_NAME);
        let mut params = Vec::new();

        if let Some(where_clause) = &self.where_clause {
            let (where_sql, where_params) = where_clause.build_with_dialect(dialect, 0);
            sql.push_str(" WHERE ");
            sql.push_str(&where_sql);
            params = where_params;
        } else if let Some(model) = &self.model {
            // Delete by primary key
            let pk = M::PRIMARY_KEY;
            let pk_values = model.primary_key_value();
            let pk_conditions: Vec<_> = pk
                .iter()
                .zip(pk_values.iter())
                .enumerate()
                .map(|(i, (col, _))| format!("{} = {}", col, dialect.placeholder(i + 1)))
                .collect();

            if !pk_conditions.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&pk_conditions.join(" AND "));
                params.extend(pk_values);
            }
        }

        // Add RETURNING clause if requested
        if self.returning {
            sql.push_str(" RETURNING *");
        }

        (sql, params)
    }

    /// Execute the DELETE and return rows affected.
    pub async fn execute<C: Connection>(
        self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<u64, sqlmodel_core::Error> {
        if self.model.is_some() && is_joined_inheritance_child::<M>() {
            if self.where_clause.is_some() {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance delete currently supports only DeleteBuilder::from_model (no explicit WHERE yet)"
                        .to_string(),
                ));
            }

            let dialect = conn.dialect();
            let Some(model) = self.model else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "delete called without model".to_string(),
                ));
            };
            let inh = M::inheritance();
            let Some(parent_table) = inh.parent else {
                return Outcome::Err(sqlmodel_core::Error::Custom(
                    "joined-table inheritance child missing parent table metadata".to_string(),
                ));
            };

            let pk_cols = M::PRIMARY_KEY;
            let pk_vals = model.primary_key_value();

            let tx_out = conn.begin(cx).await;
            let tx = match tx_out {
                Outcome::Ok(t) => t,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };

            let (child_sql, child_params) =
                build_delete_sql_for_table(dialect, M::TABLE_NAME, pk_cols, &pk_vals);
            let (parent_sql, parent_params) =
                build_delete_sql_for_table(dialect, parent_table, pk_cols, &pk_vals);

            let mut total = 0_u64;

            match tx.execute(cx, &child_sql, &child_params).await {
                Outcome::Ok(n) => total = total.saturating_add(n),
                Outcome::Err(e) => {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Err(e);
                }
                Outcome::Cancelled(r) => {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Cancelled(r);
                }
                Outcome::Panicked(p) => {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Panicked(p);
                }
            }

            match tx.execute(cx, &parent_sql, &parent_params).await {
                Outcome::Ok(n) => total = total.saturating_add(n),
                Outcome::Err(e) => {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Err(e);
                }
                Outcome::Cancelled(r) => {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Cancelled(r);
                }
                Outcome::Panicked(p) => {
                    tx_rollback_best_effort(tx, cx).await;
                    return Outcome::Panicked(p);
                }
            }

            match tx.commit(cx).await {
                Outcome::Ok(()) => Outcome::Ok(total),
                Outcome::Err(e) => Outcome::Err(e),
                Outcome::Cancelled(r) => Outcome::Cancelled(r),
                Outcome::Panicked(p) => Outcome::Panicked(p),
            }
        } else {
            let (sql, params) = self.build_with_dialect(conn.dialect());
            conn.execute(cx, &sql, &params).await
        }
    }

    /// Execute the DELETE with RETURNING and get the deleted rows.
    pub async fn execute_returning<C: Connection>(
        mut self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Vec<Row>, sqlmodel_core::Error> {
        self.returning = true;
        let (sql, params) = self.build_with_dialect(conn.dialect());
        conn.query(cx, &sql, &params).await
    }
}

impl<M: Model> Default for DeleteBuilder<'_, M> {
    fn default() -> Self {
        Self::new()
    }
}

/// Query builder for raw SQL with type-safe parameter binding.
#[derive(Debug)]
pub struct QueryBuilder {
    sql: String,
    params: Vec<Value>,
}

impl QueryBuilder {
    /// Create a new query builder with the given SQL.
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    /// Bind a parameter value.
    pub fn bind(mut self, value: impl Into<Value>) -> Self {
        self.params.push(value.into());
        self
    }

    /// Bind multiple parameter values.
    pub fn bind_all(mut self, values: impl IntoIterator<Item = Value>) -> Self {
        self.params.extend(values);
        self
    }

    /// Get the SQL and parameters.
    pub fn build(self) -> (String, Vec<Value>) {
        (self.sql, self.params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Dialect;
    use sqlmodel_core::field::FieldInfo;
    use sqlmodel_core::types::SqlType;

    // Mock model for testing
    struct TestHero {
        id: Option<i64>,
        name: String,
        age: i32,
    }

    impl Model for TestHero {
        const TABLE_NAME: &'static str = "heroes";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt)
                    .primary_key(true)
                    .auto_increment(true)
                    .nullable(true),
                FieldInfo::new("name", "name", SqlType::Text),
                FieldInfo::new("age", "age", SqlType::Integer),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![
                ("id", self.id.map_or(Value::Null, Value::BigInt)),
                ("name", Value::Text(self.name.clone())),
                ("age", Value::Int(self.age)),
            ]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Err(sqlmodel_core::Error::Custom(
                "from_row not used in tests".to_string(),
            ))
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![self.id.map_or(Value::Null, Value::BigInt)]
        }

        fn is_new(&self) -> bool {
            self.id.is_none()
        }
    }

    struct TestOnlyId {
        id: Option<i64>,
    }

    impl Model for TestOnlyId {
        const TABLE_NAME: &'static str = "only_ids";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[FieldInfo::new("id", "id", SqlType::BigInt)
                .primary_key(true)
                .auto_increment(true)
                .nullable(true)];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![("id", self.id.map_or(Value::Null, Value::BigInt))]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Err(sqlmodel_core::Error::Custom(
                "from_row not used in tests".to_string(),
            ))
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![self.id.map_or(Value::Null, Value::BigInt)]
        }

        fn is_new(&self) -> bool {
            self.id.is_none()
        }
    }

    #[test]
    fn test_insert_basic() {
        let hero = TestHero {
            id: None,
            name: "Spider-Man".to_string(),
            age: 25,
        };
        let (sql, params) = InsertBuilder::new(&hero).build();

        // Auto-increment column with None gets DEFAULT, other columns get placeholders
        assert_eq!(
            sql,
            "INSERT INTO heroes (id, name, age) VALUES (DEFAULT, $1, $2)"
        );
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_insert_returning() {
        let hero = TestHero {
            id: None,
            name: "Spider-Man".to_string(),
            age: 25,
        };
        let (sql, _) = InsertBuilder::new(&hero).returning().build();

        assert!(sql.ends_with(" RETURNING *"));
    }

    #[test]
    fn test_insert_on_conflict_do_nothing() {
        let hero = TestHero {
            id: None,
            name: "Spider-Man".to_string(),
            age: 25,
        };
        let (sql, _) = InsertBuilder::new(&hero).on_conflict_do_nothing().build();

        assert!(sql.contains("ON CONFLICT DO NOTHING"));
    }

    #[test]
    fn test_insert_on_conflict_do_update() {
        let hero = TestHero {
            id: None,
            name: "Spider-Man".to_string(),
            age: 25,
        };
        let (sql, _) = InsertBuilder::new(&hero)
            .on_conflict_do_update(&["name", "age"])
            .build();

        assert!(sql.contains("ON CONFLICT (id) DO UPDATE SET"));
        assert!(sql.contains("name = EXCLUDED.name"));
        assert!(sql.contains("age = EXCLUDED.age"));
    }

    #[test]
    fn test_insert_many() {
        let heroes = vec![
            TestHero {
                id: None,
                name: "Spider-Man".to_string(),
                age: 25,
            },
            TestHero {
                id: None,
                name: "Iron Man".to_string(),
                age: 45,
            },
        ];
        let (sql, params) = InsertManyBuilder::new(&heroes).build();

        // Auto-increment columns with None get DEFAULT, other columns get placeholders
        assert!(sql.starts_with("INSERT INTO heroes (id, name, age) VALUES"));
        assert!(sql.contains("(DEFAULT, $1, $2), (DEFAULT, $3, $4)"));
        assert_eq!(params.len(), 4);
    }

    #[test]
    fn test_insert_sqlite_omits_default_columns() {
        let hero = TestHero {
            id: None,
            name: "Spider-Man".to_string(),
            age: 25,
        };
        let (sql, params) = InsertBuilder::new(&hero).build_with_dialect(Dialect::Sqlite);

        assert_eq!(sql, "INSERT INTO heroes (name, age) VALUES (?1, ?2)");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_insert_sqlite_default_values_only() {
        let model = TestOnlyId { id: None };
        let (sql, params) = InsertBuilder::new(&model).build_with_dialect(Dialect::Sqlite);

        assert_eq!(sql, "INSERT INTO only_ids DEFAULT VALUES");
        assert!(params.is_empty());
    }

    #[test]
    fn test_insert_many_sqlite_omits_auto_increment() {
        let heroes = vec![
            TestHero {
                id: None,
                name: "Spider-Man".to_string(),
                age: 25,
            },
            TestHero {
                id: None,
                name: "Iron Man".to_string(),
                age: 45,
            },
        ];
        let batches = InsertManyBuilder::new(&heroes).build_batches_with_dialect(Dialect::Sqlite);

        assert_eq!(batches.len(), 1);
        let (sql, params) = &batches[0];
        assert!(sql.starts_with("INSERT INTO heroes (name, age) VALUES"));
        assert!(sql.contains("(?1, ?2), (?3, ?4)"));
        assert_eq!(params.len(), 4);
    }

    #[test]
    fn test_insert_many_sqlite_mixed_defaults_split() {
        let heroes = vec![
            TestHero {
                id: Some(1),
                name: "Spider-Man".to_string(),
                age: 25,
            },
            TestHero {
                id: None,
                name: "Iron Man".to_string(),
                age: 45,
            },
        ];
        let batches = InsertManyBuilder::new(&heroes).build_batches_with_dialect(Dialect::Sqlite);

        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches[0].0,
            "INSERT INTO heroes (id, name, age) VALUES (?1, ?2, ?3)"
        );
        assert_eq!(
            batches[1].0,
            "INSERT INTO heroes (name, age) VALUES (?1, ?2)"
        );
        assert_eq!(batches[0].1.len(), 3);
        assert_eq!(batches[1].1.len(), 2);
    }

    #[test]
    fn test_insert_many_sqlite_default_values_only() {
        let rows = vec![TestOnlyId { id: None }, TestOnlyId { id: None }];
        let batches = InsertManyBuilder::new(&rows).build_batches_with_dialect(Dialect::Sqlite);

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].0, "INSERT INTO only_ids DEFAULT VALUES");
        assert_eq!(batches[1].0, "INSERT INTO only_ids DEFAULT VALUES");
        assert!(batches[0].1.is_empty());
        assert!(batches[1].1.is_empty());
    }

    #[test]
    fn test_update_basic() {
        let hero = TestHero {
            id: Some(1),
            name: "Spider-Man".to_string(),
            age: 26,
        };
        let (sql, params) = UpdateBuilder::new(&hero).build();

        assert!(sql.starts_with("UPDATE heroes SET"));
        assert!(sql.contains("WHERE id = "));
        assert!(params.len() >= 2); // At least name, age, and id
    }

    #[test]
    fn test_update_explicit_set() {
        let (sql, params) = UpdateBuilder::<TestHero>::empty()
            .set("age", 30)
            .filter(Expr::col("id").eq(1))
            .build_with_dialect(Dialect::Postgres);

        assert_eq!(sql, "UPDATE heroes SET age = $1 WHERE \"id\" = $2");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_update_returning() {
        let hero = TestHero {
            id: Some(1),
            name: "Spider-Man".to_string(),
            age: 26,
        };
        let (sql, _) = UpdateBuilder::new(&hero).returning().build();

        assert!(sql.ends_with(" RETURNING *"));
    }

    #[test]
    fn test_delete_basic() {
        let (sql, _) = DeleteBuilder::<TestHero>::new()
            .filter(Expr::col("age").lt(18))
            .build_with_dialect(Dialect::Postgres);

        assert_eq!(sql, "DELETE FROM heroes WHERE \"age\" < $1");
    }

    #[test]
    fn test_delete_from_model() {
        let hero = TestHero {
            id: Some(42),
            name: "Spider-Man".to_string(),
            age: 25,
        };
        let (sql, params) = DeleteBuilder::from_model(&hero).build();

        assert!(sql.contains("WHERE id = $1"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_delete_returning() {
        let (sql, _) = DeleteBuilder::<TestHero>::new()
            .filter(Expr::col("status").eq("inactive"))
            .returning()
            .build_with_dialect(Dialect::Postgres);

        assert!(sql.ends_with(" RETURNING *"));
    }

    #[test]
    fn test_dialect_sqlite() {
        let hero = TestHero {
            id: None,
            name: "Spider-Man".to_string(),
            age: 25,
        };
        let (sql, _) = InsertBuilder::new(&hero).build_with_dialect(Dialect::Sqlite);

        assert!(sql.contains("?1"));
        assert!(sql.contains("?2"));
    }

    #[test]
    fn test_dialect_mysql() {
        let hero = TestHero {
            id: None,
            name: "Spider-Man".to_string(),
            age: 25,
        };
        let (sql, _) = InsertBuilder::new(&hero).build_with_dialect(Dialect::Mysql);

        // MySQL uses ? without numbers
        assert!(sql.contains('?'));
        assert!(!sql.contains("$1"));
    }
}
