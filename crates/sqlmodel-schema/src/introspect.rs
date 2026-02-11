//! Database introspection.
//!
//! This module provides comprehensive schema introspection for SQLite, PostgreSQL, and MySQL.
//! It extracts metadata about tables, columns, constraints, and indexes.

use asupersync::{Cx, Outcome};
use sqlmodel_core::{Connection, Error};
use std::collections::HashMap;

#[cfg(test)]
use sqlmodel_core::sanitize_identifier;

// ============================================================================
// Schema Types
// ============================================================================

/// Complete representation of a database schema.
#[derive(Debug, Clone, Default)]
pub struct DatabaseSchema {
    /// All tables in the schema, keyed by table name
    pub tables: HashMap<String, TableInfo>,
    /// Database dialect
    pub dialect: Dialect,
}

impl DatabaseSchema {
    /// Create a new empty schema for the given dialect.
    pub fn new(dialect: Dialect) -> Self {
        Self {
            tables: HashMap::new(),
            dialect,
        }
    }

    /// Get a table by name.
    pub fn table(&self, name: &str) -> Option<&TableInfo> {
        self.tables.get(name)
    }

    /// Get all table names.
    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }
}

/// Parsed SQL type with extracted metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedSqlType {
    /// Base type name (e.g., VARCHAR, INTEGER, DECIMAL)
    pub base_type: String,
    /// Length for character types (e.g., VARCHAR(255) -> 255)
    pub length: Option<u32>,
    /// Precision for numeric types (e.g., DECIMAL(10,2) -> 10)
    pub precision: Option<u32>,
    /// Scale for numeric types (e.g., DECIMAL(10,2) -> 2)
    pub scale: Option<u32>,
    /// Whether the type is unsigned (MySQL)
    pub unsigned: bool,
    /// Whether this is an array type (PostgreSQL)
    pub array: bool,
}

impl ParsedSqlType {
    /// Parse a SQL type string into structured metadata.
    ///
    /// # Examples
    /// - `VARCHAR(255)` -> base_type: "VARCHAR", length: 255
    /// - `DECIMAL(10,2)` -> base_type: "DECIMAL", precision: 10, scale: 2
    /// - `INT UNSIGNED` -> base_type: "INT", unsigned: true
    /// - `TEXT[]` -> base_type: "TEXT", array: true
    pub fn parse(type_str: &str) -> Self {
        let type_str = type_str.trim().to_uppercase();

        // Check for array suffix (PostgreSQL)
        let (type_str, array) = if type_str.ends_with("[]") {
            (type_str.trim_end_matches("[]"), true)
        } else {
            (type_str.as_str(), false)
        };

        // Check for UNSIGNED suffix (MySQL)
        let (type_str, unsigned) = if type_str.ends_with(" UNSIGNED") {
            (type_str.trim_end_matches(" UNSIGNED"), true)
        } else {
            (type_str, false)
        };

        // Parse base type and parameters
        if let Some(paren_start) = type_str.find('(') {
            let base_type = type_str[..paren_start].trim().to_string();
            let params = &type_str[paren_start + 1..type_str.len() - 1]; // Remove ()

            // Check if it's precision,scale or just length
            if params.contains(',') {
                let parts: Vec<&str> = params.split(',').collect();
                let precision = parts.first().and_then(|s| s.trim().parse().ok());
                let scale = parts.get(1).and_then(|s| s.trim().parse().ok());
                Self {
                    base_type,
                    length: None,
                    precision,
                    scale,
                    unsigned,
                    array,
                }
            } else {
                let length = params.trim().parse().ok();
                Self {
                    base_type,
                    length,
                    precision: None,
                    scale: None,
                    unsigned,
                    array,
                }
            }
        } else {
            Self {
                base_type: type_str.to_string(),
                length: None,
                precision: None,
                scale: None,
                unsigned,
                array,
            }
        }
    }

    /// Check if this is a text/string type.
    pub fn is_text(&self) -> bool {
        matches!(
            self.base_type.as_str(),
            "VARCHAR" | "CHAR" | "TEXT" | "CLOB" | "NVARCHAR" | "NCHAR" | "NTEXT"
        )
    }

    /// Check if this is a numeric type.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self.base_type.as_str(),
            "INT"
                | "INTEGER"
                | "BIGINT"
                | "SMALLINT"
                | "TINYINT"
                | "MEDIUMINT"
                | "DECIMAL"
                | "NUMERIC"
                | "FLOAT"
                | "DOUBLE"
                | "REAL"
                | "DOUBLE PRECISION"
        )
    }

    /// Check if this is a date/time type.
    pub fn is_datetime(&self) -> bool {
        matches!(
            self.base_type.as_str(),
            "DATE" | "TIME" | "DATETIME" | "TIMESTAMP" | "TIMESTAMPTZ" | "TIMETZ"
        )
    }
}

/// Unique constraint information.
#[derive(Debug, Clone)]
pub struct UniqueConstraintInfo {
    /// Constraint name
    pub name: Option<String>,
    /// Columns in the constraint
    pub columns: Vec<String>,
}

/// Check constraint information.
#[derive(Debug, Clone)]
pub struct CheckConstraintInfo {
    /// Constraint name
    pub name: Option<String>,
    /// Check expression
    pub expression: String,
}

/// Information about a database table.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table name
    pub name: String,
    /// Columns in the table
    pub columns: Vec<ColumnInfo>,
    /// Primary key column names
    pub primary_key: Vec<String>,
    /// Foreign key constraints
    pub foreign_keys: Vec<ForeignKeyInfo>,
    /// Unique constraints
    pub unique_constraints: Vec<UniqueConstraintInfo>,
    /// Check constraints
    pub check_constraints: Vec<CheckConstraintInfo>,
    /// Indexes on the table
    pub indexes: Vec<IndexInfo>,
    /// Table comment (if any)
    pub comment: Option<String>,
}

impl TableInfo {
    /// Get a column by name.
    pub fn column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Check if this table has a single-column auto-increment primary key.
    pub fn has_auto_pk(&self) -> bool {
        self.primary_key.len() == 1
            && self
                .column(&self.primary_key[0])
                .is_some_and(|c| c.auto_increment)
    }
}

/// Information about a table column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// SQL type as raw string
    pub sql_type: String,
    /// Parsed SQL type with extracted metadata
    pub parsed_type: ParsedSqlType,
    /// Whether the column is nullable
    pub nullable: bool,
    /// Default value expression
    pub default: Option<String>,
    /// Whether this is part of the primary key
    pub primary_key: bool,
    /// Whether this column auto-increments
    pub auto_increment: bool,
    /// Column comment (if any)
    pub comment: Option<String>,
}

/// Information about a foreign key constraint.
#[derive(Debug, Clone)]
pub struct ForeignKeyInfo {
    /// Constraint name
    pub name: Option<String>,
    /// Local column name
    pub column: String,
    /// Referenced table
    pub foreign_table: String,
    /// Referenced column
    pub foreign_column: String,
    /// ON DELETE action
    pub on_delete: Option<String>,
    /// ON UPDATE action
    pub on_update: Option<String>,
}

/// Information about an index.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index name
    pub name: String,
    /// Columns in the index
    pub columns: Vec<String>,
    /// Whether this is a unique index
    pub unique: bool,
    /// Index type (BTREE, HASH, GIN, GIST, etc.)
    pub index_type: Option<String>,
    /// Whether this is a primary key index
    pub primary: bool,
}

/// Database introspector.
pub struct Introspector {
    /// Database type for dialect-specific queries
    dialect: Dialect,
}

/// Supported database dialects.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Dialect {
    /// SQLite
    #[default]
    Sqlite,
    /// PostgreSQL
    Postgres,
    /// MySQL/MariaDB
    Mysql,
}

impl Introspector {
    /// Create a new introspector for the given dialect.
    pub fn new(dialect: Dialect) -> Self {
        Self { dialect }
    }

    /// List all table names in the database.
    pub async fn table_names<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<Vec<String>, Error> {
        let sql = match self.dialect {
            Dialect::Sqlite => {
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            }
            Dialect::Postgres => {
                "SELECT table_name
                                   FROM information_schema.tables
                                   WHERE table_schema = current_schema()
                                     AND table_type = 'BASE TABLE'"
            }
            Dialect::Mysql => "SHOW TABLES",
        };

        let rows = match conn.query(cx, sql, &[]).await {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let names: Vec<String> = rows
            .iter()
            .filter_map(|row| row.get(0).and_then(|v| v.as_str().map(String::from)))
            .collect();

        Outcome::Ok(names)
    }

    /// Get detailed information about a table.
    pub async fn table_info<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<TableInfo, Error> {
        let columns = match self.columns(cx, conn, table_name).await {
            Outcome::Ok(cols) => cols,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let primary_key: Vec<String> = columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name.clone())
            .collect();

        let foreign_keys = match self.foreign_keys(cx, conn, table_name).await {
            Outcome::Ok(fks) => fks,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let indexes = match self.indexes(cx, conn, table_name).await {
            Outcome::Ok(idxs) => idxs,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let unique_constraints = match self.dialect {
            Dialect::Postgres => match self.postgres_unique_constraints(cx, conn, table_name).await
            {
                Outcome::Ok(uks) => uks,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            },
            Dialect::Sqlite | Dialect::Mysql => {
                // For SQLite/MySQL, UNIQUE constraints are represented by UNIQUE indexes.
                // We normalize them into `unique_constraints` and remove them from `indexes`
                // so the diff engine does not try to DROP/CREATE constraint-backed indexes.
                Vec::new()
            }
        };

        // SQLite/MySQL: derive unique_constraints from indexes (unique && !primary).
        // PostgreSQL: unique_constraints already queried from pg_constraint; indexes already
        // exclude constraint-backed indexes (see postgres_indexes()).
        let (unique_constraints, indexes) = match self.dialect {
            Dialect::Sqlite | Dialect::Mysql => {
                let mut uks = Vec::new();
                let mut idxs = Vec::new();
                for idx in indexes {
                    if idx.unique && !idx.primary {
                        uks.push(UniqueConstraintInfo {
                            name: Some(idx.name.clone()),
                            columns: idx.columns.clone(),
                        });
                    } else {
                        idxs.push(idx);
                    }
                }
                (uks, idxs)
            }
            Dialect::Postgres => (unique_constraints, indexes),
        };

        let check_constraints = match self.check_constraints(cx, conn, table_name).await {
            Outcome::Ok(checks) => checks,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let comment = match self.table_comment(cx, conn, table_name).await {
            Outcome::Ok(comment) => comment,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        Outcome::Ok(TableInfo {
            name: table_name.to_string(),
            columns,
            primary_key,
            foreign_keys,
            unique_constraints,
            check_constraints,
            indexes,
            comment,
        })
    }

    async fn postgres_unique_constraints<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<UniqueConstraintInfo>, Error> {
        debug_assert!(self.dialect == Dialect::Postgres);

        let sql = "SELECT
                       c.conname AS constraint_name,
                       a.attname AS column_name,
                       u.ord AS ordinal
                   FROM pg_constraint c
                   JOIN pg_class t ON t.oid = c.conrelid
                   JOIN pg_namespace n ON n.oid = t.relnamespace
                   JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, ord) ON true
                   JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = u.attnum
                   WHERE t.relname = $1
                     AND n.nspname = current_schema()
                     AND c.contype = 'u'
                   ORDER BY c.conname, u.ord";

        let rows = match conn
            .query(
                cx,
                sql,
                &[sqlmodel_core::Value::Text(table_name.to_string())],
            )
            .await
        {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let mut map: HashMap<String, Vec<(i64, String)>> = HashMap::new();
        for row in &rows {
            let Ok(name) = row.get_named::<String>("constraint_name") else {
                continue;
            };
            let Ok(col) = row.get_named::<String>("column_name") else {
                continue;
            };
            let ord = row.get_named::<i64>("ordinal").ok().unwrap_or(0);
            map.entry(name.clone())
                .and_modify(|cols| cols.push((ord, col.clone())))
                .or_insert_with(|| vec![(ord, col)]);
        }

        let mut out = Vec::new();
        for (name, mut cols) in map {
            cols.sort_by_key(|(ord, _)| *ord);
            out.push(UniqueConstraintInfo {
                name: Some(name),
                columns: cols.into_iter().map(|(_, c)| c).collect(),
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));

        Outcome::Ok(out)
    }

    /// Introspect the entire database schema.
    pub async fn introspect_all<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
    ) -> Outcome<DatabaseSchema, Error> {
        let table_names = match self.table_names(cx, conn).await {
            Outcome::Ok(names) => names,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let mut schema = DatabaseSchema::new(self.dialect);

        for name in table_names {
            let info = match self.table_info(cx, conn, &name).await {
                Outcome::Ok(info) => info,
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };
            schema.tables.insert(name, info);
        }

        Outcome::Ok(schema)
    }

    /// Get column information for a table.
    async fn columns<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<ColumnInfo>, Error> {
        match self.dialect {
            Dialect::Sqlite => self.sqlite_columns(cx, conn, table_name).await,
            Dialect::Postgres => self.postgres_columns(cx, conn, table_name).await,
            Dialect::Mysql => self.mysql_columns(cx, conn, table_name).await,
        }
    }

    async fn sqlite_columns<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<ColumnInfo>, Error> {
        let sql = format!("PRAGMA table_info({})", quote_sqlite_identifier(table_name));
        let rows = match conn.query(cx, &sql, &[]).await {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let columns: Vec<ColumnInfo> = rows
            .iter()
            .filter_map(|row| {
                let name = row.get_named::<String>("name").ok()?;
                let sql_type = row.get_named::<String>("type").ok()?;
                let notnull = row.get_named::<i64>("notnull").ok().unwrap_or(0);
                let dflt_value = row.get_named::<String>("dflt_value").ok();
                let pk = row.get_named::<i64>("pk").ok().unwrap_or(0);
                let parsed_type = ParsedSqlType::parse(&sql_type);

                Some(ColumnInfo {
                    name,
                    sql_type,
                    parsed_type,
                    nullable: notnull == 0,
                    default: dflt_value,
                    primary_key: pk > 0,
                    auto_increment: false, // SQLite doesn't report this via PRAGMA
                    comment: None,         // SQLite doesn't support column comments
                })
            })
            .collect();

        Outcome::Ok(columns)
    }

    async fn postgres_columns<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<ColumnInfo>, Error> {
        // Use a more comprehensive query to get full type info
        let sql = "SELECT
                       c.column_name,
                       c.data_type,
                       c.udt_name,
                       c.character_maximum_length,
                       c.numeric_precision,
                       c.numeric_scale,
                       c.is_nullable,
                       c.column_default,
                       COALESCE(d.description, '') as column_comment
                   FROM information_schema.columns c
                   LEFT JOIN pg_catalog.pg_statio_all_tables st
                       ON c.table_schema = st.schemaname AND c.table_name = st.relname
                   LEFT JOIN pg_catalog.pg_description d
                       ON d.objoid = st.relid AND d.objsubid = c.ordinal_position
                   WHERE c.table_name = $1 AND c.table_schema = current_schema()
                   ORDER BY c.ordinal_position";

        let rows = match conn
            .query(
                cx,
                sql,
                &[sqlmodel_core::Value::Text(table_name.to_string())],
            )
            .await
        {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let columns: Vec<ColumnInfo> = rows
            .iter()
            .filter_map(|row| {
                let name = row.get_named::<String>("column_name").ok()?;
                let data_type = row.get_named::<String>("data_type").ok()?;
                let udt_name = row.get_named::<String>("udt_name").ok().unwrap_or_default();
                let char_len = row.get_named::<i64>("character_maximum_length").ok();
                let precision = row.get_named::<i64>("numeric_precision").ok();
                let scale = row.get_named::<i64>("numeric_scale").ok();
                let nullable_str = row.get_named::<String>("is_nullable").ok()?;
                let default = row.get_named::<String>("column_default").ok();
                let comment = row.get_named::<String>("column_comment").ok();

                // Build a complete SQL type string
                let sql_type =
                    build_postgres_type(&data_type, &udt_name, char_len, precision, scale);
                let parsed_type = ParsedSqlType::parse(&sql_type);

                // Check if auto-increment by looking at default (nextval)
                let auto_increment = default.as_ref().is_some_and(|d| d.starts_with("nextval("));

                Some(ColumnInfo {
                    name,
                    sql_type,
                    parsed_type,
                    nullable: nullable_str == "YES",
                    default,
                    primary_key: false, // Determined via separate index query
                    auto_increment,
                    comment: comment.filter(|s| !s.is_empty()),
                })
            })
            .collect();

        Outcome::Ok(columns)
    }

    async fn mysql_columns<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<ColumnInfo>, Error> {
        // Use SHOW FULL COLUMNS to get comments
        let sql = format!(
            "SHOW FULL COLUMNS FROM {}",
            quote_mysql_identifier(table_name)
        );
        let rows = match conn.query(cx, &sql, &[]).await {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let columns: Vec<ColumnInfo> = rows
            .iter()
            .filter_map(|row| {
                let name = row.get_named::<String>("Field").ok()?;
                let sql_type = row.get_named::<String>("Type").ok()?;
                let null = row.get_named::<String>("Null").ok()?;
                let key = row.get_named::<String>("Key").ok()?;
                let default = row.get_named::<String>("Default").ok();
                let extra = row.get_named::<String>("Extra").ok().unwrap_or_default();
                let comment = row.get_named::<String>("Comment").ok();
                let parsed_type = ParsedSqlType::parse(&sql_type);

                Some(ColumnInfo {
                    name,
                    sql_type,
                    parsed_type,
                    nullable: null == "YES",
                    default,
                    primary_key: key == "PRI",
                    auto_increment: extra.contains("auto_increment"),
                    comment: comment.filter(|s| !s.is_empty()),
                })
            })
            .collect();

        Outcome::Ok(columns)
    }

    // ========================================================================
    // Foreign Key Introspection
    // ========================================================================

    async fn check_constraints<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<CheckConstraintInfo>, Error> {
        match self.dialect {
            Dialect::Sqlite => self.sqlite_check_constraints(cx, conn, table_name).await,
            Dialect::Postgres => self.postgres_check_constraints(cx, conn, table_name).await,
            Dialect::Mysql => self.mysql_check_constraints(cx, conn, table_name).await,
        }
    }

    async fn sqlite_check_constraints<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<CheckConstraintInfo>, Error> {
        let sql = "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1";
        let rows = match conn
            .query(
                cx,
                sql,
                &[sqlmodel_core::Value::Text(table_name.to_string())],
            )
            .await
        {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let create_sql = rows.iter().find_map(|row| {
            row.get_named::<String>("sql").ok().or_else(|| {
                row.get(0)
                    .and_then(|value| value.as_str().map(ToString::to_string))
            })
        });

        match create_sql {
            Some(sql) => Outcome::Ok(extract_sqlite_check_constraints(&sql)),
            None => Outcome::Ok(Vec::new()),
        }
    }

    async fn postgres_check_constraints<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<CheckConstraintInfo>, Error> {
        let sql = "SELECT
                       c.conname AS constraint_name,
                       pg_get_constraintdef(c.oid, true) AS constraint_definition
                   FROM pg_constraint c
                   JOIN pg_class t ON t.oid = c.conrelid
                   JOIN pg_namespace n ON n.oid = t.relnamespace
                   WHERE t.relname = $1
                     AND n.nspname = current_schema()
                     AND c.contype = 'c'
                   ORDER BY c.conname";

        let rows = match conn
            .query(
                cx,
                sql,
                &[sqlmodel_core::Value::Text(table_name.to_string())],
            )
            .await
        {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let checks = rows
            .iter()
            .filter_map(|row| {
                let definition = row.get_named::<String>("constraint_definition").ok()?;
                let expression = normalize_check_expression(&definition);
                if expression.is_empty() {
                    return None;
                }
                Some(CheckConstraintInfo {
                    name: row
                        .get_named::<String>("constraint_name")
                        .ok()
                        .filter(|s| !s.is_empty()),
                    expression,
                })
            })
            .collect();

        Outcome::Ok(checks)
    }

    async fn mysql_check_constraints<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<CheckConstraintInfo>, Error> {
        let sql = "SELECT
                 tc.CONSTRAINT_NAME AS constraint_name,
                 cc.CHECK_CLAUSE AS check_clause
             FROM information_schema.TABLE_CONSTRAINTS tc
             JOIN information_schema.CHECK_CONSTRAINTS cc
               ON tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA
              AND tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
             WHERE tc.CONSTRAINT_TYPE = 'CHECK'
               AND tc.TABLE_SCHEMA = DATABASE()
               AND tc.TABLE_NAME = ?
             ORDER BY tc.CONSTRAINT_NAME";

        let rows = match conn
            .query(
                cx,
                sql,
                &[sqlmodel_core::Value::Text(table_name.to_string())],
            )
            .await
        {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let checks = rows
            .iter()
            .filter_map(|row| {
                let definition = row.get_named::<String>("check_clause").ok()?;
                let expression = normalize_check_expression(&definition);
                if expression.is_empty() {
                    return None;
                }
                Some(CheckConstraintInfo {
                    name: row
                        .get_named::<String>("constraint_name")
                        .ok()
                        .filter(|s| !s.is_empty()),
                    expression,
                })
            })
            .collect();

        Outcome::Ok(checks)
    }

    async fn table_comment<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Option<String>, Error> {
        match self.dialect {
            Dialect::Sqlite => Outcome::Ok(None),
            Dialect::Postgres => self.postgres_table_comment(cx, conn, table_name).await,
            Dialect::Mysql => self.mysql_table_comment(cx, conn, table_name).await,
        }
    }

    async fn postgres_table_comment<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Option<String>, Error> {
        let sql = "SELECT
                       COALESCE(obj_description(c.oid, 'pg_class'), '') AS table_comment
                   FROM pg_class c
                   JOIN pg_namespace n ON n.oid = c.relnamespace
                   WHERE c.relname = $1
                     AND n.nspname = current_schema()
                   LIMIT 1";

        let rows = match conn
            .query(
                cx,
                sql,
                &[sqlmodel_core::Value::Text(table_name.to_string())],
            )
            .await
        {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let comment = rows.iter().find_map(|row| {
            row.get_named::<String>("table_comment")
                .ok()
                .filter(|s| !s.is_empty())
        });
        Outcome::Ok(comment)
    }

    async fn mysql_table_comment<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Option<String>, Error> {
        let sql = "SELECT TABLE_COMMENT AS table_comment
             FROM information_schema.TABLES
             WHERE TABLE_SCHEMA = DATABASE()
               AND TABLE_NAME = ?
             LIMIT 1";

        let rows = match conn
            .query(
                cx,
                sql,
                &[sqlmodel_core::Value::Text(table_name.to_string())],
            )
            .await
        {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let comment = rows.iter().find_map(|row| {
            row.get_named::<String>("table_comment")
                .ok()
                .filter(|s| !s.is_empty())
        });
        Outcome::Ok(comment)
    }

    /// Get foreign key constraints for a table.
    async fn foreign_keys<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<ForeignKeyInfo>, Error> {
        match self.dialect {
            Dialect::Sqlite => self.sqlite_foreign_keys(cx, conn, table_name).await,
            Dialect::Postgres => self.postgres_foreign_keys(cx, conn, table_name).await,
            Dialect::Mysql => self.mysql_foreign_keys(cx, conn, table_name).await,
        }
    }

    async fn sqlite_foreign_keys<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<ForeignKeyInfo>, Error> {
        let sql = format!(
            "PRAGMA foreign_key_list({})",
            quote_sqlite_identifier(table_name)
        );
        let rows = match conn.query(cx, &sql, &[]).await {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let fks: Vec<ForeignKeyInfo> = rows
            .iter()
            .filter_map(|row| {
                let table = row.get_named::<String>("table").ok()?;
                let from = row.get_named::<String>("from").ok()?;
                let to = row.get_named::<String>("to").ok()?;
                let on_update = row.get_named::<String>("on_update").ok();
                let on_delete = row.get_named::<String>("on_delete").ok();

                Some(ForeignKeyInfo {
                    name: None, // SQLite doesn't name FK constraints in PRAGMA output
                    column: from,
                    foreign_table: table,
                    foreign_column: to,
                    on_delete: on_delete.filter(|s| s != "NO ACTION"),
                    on_update: on_update.filter(|s| s != "NO ACTION"),
                })
            })
            .collect();

        Outcome::Ok(fks)
    }

    async fn postgres_foreign_keys<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<ForeignKeyInfo>, Error> {
        let sql = "SELECT
                       tc.constraint_name,
                       kcu.column_name,
                       ccu.table_name AS foreign_table_name,
                       ccu.column_name AS foreign_column_name,
                       rc.delete_rule,
                       rc.update_rule
                   FROM information_schema.table_constraints AS tc
                   JOIN information_schema.key_column_usage AS kcu
                       ON tc.constraint_name = kcu.constraint_name
                       AND tc.table_schema = kcu.table_schema
                   JOIN information_schema.constraint_column_usage AS ccu
                       ON ccu.constraint_name = tc.constraint_name
                       AND ccu.table_schema = tc.table_schema
                   JOIN information_schema.referential_constraints AS rc
                       ON rc.constraint_name = tc.constraint_name
                       AND rc.constraint_schema = tc.table_schema
                   WHERE tc.constraint_type = 'FOREIGN KEY'
                       AND tc.table_name = $1
                       AND tc.table_schema = current_schema()";

        let rows = match conn
            .query(
                cx,
                sql,
                &[sqlmodel_core::Value::Text(table_name.to_string())],
            )
            .await
        {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let fks: Vec<ForeignKeyInfo> = rows
            .iter()
            .filter_map(|row| {
                let name = row.get_named::<String>("constraint_name").ok();
                let column = row.get_named::<String>("column_name").ok()?;
                let foreign_table = row.get_named::<String>("foreign_table_name").ok()?;
                let foreign_column = row.get_named::<String>("foreign_column_name").ok()?;
                let on_delete = row.get_named::<String>("delete_rule").ok();
                let on_update = row.get_named::<String>("update_rule").ok();

                Some(ForeignKeyInfo {
                    name,
                    column,
                    foreign_table,
                    foreign_column,
                    on_delete: on_delete.filter(|s| s != "NO ACTION"),
                    on_update: on_update.filter(|s| s != "NO ACTION"),
                })
            })
            .collect();

        Outcome::Ok(fks)
    }

    async fn mysql_foreign_keys<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<ForeignKeyInfo>, Error> {
        let sql = "SELECT
                       kcu.constraint_name,
                       kcu.column_name,
                       kcu.referenced_table_name,
                       kcu.referenced_column_name,
                       rc.delete_rule,
                       rc.update_rule
                   FROM information_schema.key_column_usage AS kcu
                   JOIN information_schema.referential_constraints AS rc
                       ON rc.constraint_name = kcu.constraint_name
                       AND rc.constraint_schema = kcu.constraint_schema
                   WHERE kcu.table_schema = DATABASE()
                       AND kcu.table_name = ?
                       AND kcu.referenced_table_name IS NOT NULL";

        let rows = match conn
            .query(
                cx,
                sql,
                &[sqlmodel_core::Value::Text(table_name.to_string())],
            )
            .await
        {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let fks: Vec<ForeignKeyInfo> = rows
            .iter()
            .filter_map(|row| {
                let name = row.get_named::<String>("constraint_name").ok();
                let column = row.get_named::<String>("column_name").ok()?;
                let foreign_table = row.get_named::<String>("referenced_table_name").ok()?;
                let foreign_column = row.get_named::<String>("referenced_column_name").ok()?;
                let on_delete = row.get_named::<String>("delete_rule").ok();
                let on_update = row.get_named::<String>("update_rule").ok();

                Some(ForeignKeyInfo {
                    name,
                    column,
                    foreign_table,
                    foreign_column,
                    on_delete: on_delete.filter(|s| s != "NO ACTION"),
                    on_update: on_update.filter(|s| s != "NO ACTION"),
                })
            })
            .collect();

        Outcome::Ok(fks)
    }

    // ========================================================================
    // Index Introspection
    // ========================================================================

    /// Get indexes for a table.
    async fn indexes<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<IndexInfo>, Error> {
        match self.dialect {
            Dialect::Sqlite => self.sqlite_indexes(cx, conn, table_name).await,
            Dialect::Postgres => self.postgres_indexes(cx, conn, table_name).await,
            Dialect::Mysql => self.mysql_indexes(cx, conn, table_name).await,
        }
    }

    async fn sqlite_indexes<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<IndexInfo>, Error> {
        let sql = format!("PRAGMA index_list({})", quote_sqlite_identifier(table_name));
        let rows = match conn.query(cx, &sql, &[]).await {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let mut indexes = Vec::new();

        for row in &rows {
            let Ok(name) = row.get_named::<String>("name") else {
                continue;
            };
            let unique = row.get_named::<i64>("unique").ok().unwrap_or(0) == 1;
            let origin = row.get_named::<String>("origin").ok().unwrap_or_default();
            let primary = origin == "pk";

            // Get column info for this index
            let info_sql = format!("PRAGMA index_info({})", quote_sqlite_identifier(&name));
            let info_rows = match conn.query(cx, &info_sql, &[]).await {
                Outcome::Ok(r) => r,
                Outcome::Err(_) => continue,
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            };

            let columns: Vec<String> = info_rows
                .iter()
                .filter_map(|r| r.get_named::<String>("name").ok())
                .collect();

            indexes.push(IndexInfo {
                name,
                columns,
                unique,
                index_type: None, // SQLite doesn't expose index type
                primary,
            });
        }

        Outcome::Ok(indexes)
    }

    async fn postgres_indexes<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<IndexInfo>, Error> {
        // Exclude indexes backing PRIMARY KEY / UNIQUE constraints; those are represented
        // via TableInfo.primary_key and TableInfo.unique_constraints so the diff engine
        // doesn't try to DROP/CREATE constraint-backed indexes.
        let sql = "SELECT
                       i.relname AS index_name,
                       a.attname AS column_name,
                       k.ord AS column_ord,
                       ix.indisunique AS is_unique,
                       ix.indisprimary AS is_primary,
                       am.amname AS index_type
                   FROM pg_class t
                   JOIN pg_namespace n ON n.oid = t.relnamespace
                   JOIN pg_index ix ON t.oid = ix.indrelid
                   JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
                   JOIN pg_class i ON i.oid = ix.indexrelid
                   JOIN pg_am am ON i.relam = am.oid
                   JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
                   WHERE t.relname = $1
                       AND n.nspname = current_schema()
                       AND t.relkind = 'r'
                       AND NOT EXISTS (
                           SELECT 1
                           FROM pg_constraint c
                           WHERE c.conrelid = t.oid
                             AND c.conindid = i.oid
                             AND c.contype IN ('p', 'u')
                       )
                   ORDER BY i.relname, k.ord";

        let rows = match conn
            .query(
                cx,
                sql,
                &[sqlmodel_core::Value::Text(table_name.to_string())],
            )
            .await
        {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        // Group by index name
        let mut index_map: HashMap<String, IndexInfo> = HashMap::new();

        for row in &rows {
            let Ok(name) = row.get_named::<String>("index_name") else {
                continue;
            };
            let Ok(column) = row.get_named::<String>("column_name") else {
                continue;
            };
            let unique = row.get_named::<bool>("is_unique").ok().unwrap_or(false);
            let primary = row.get_named::<bool>("is_primary").ok().unwrap_or(false);
            let index_type = row.get_named::<String>("index_type").ok();

            index_map
                .entry(name.clone())
                .and_modify(|idx| idx.columns.push(column.clone()))
                .or_insert_with(|| IndexInfo {
                    name,
                    columns: vec![column],
                    unique,
                    index_type,
                    primary,
                });
        }

        Outcome::Ok(index_map.into_values().collect())
    }

    async fn mysql_indexes<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        table_name: &str,
    ) -> Outcome<Vec<IndexInfo>, Error> {
        let sql = format!("SHOW INDEX FROM {}", quote_mysql_identifier(table_name));
        let rows = match conn.query(cx, &sql, &[]).await {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        // Group by index name
        let mut index_map: HashMap<String, IndexInfo> = HashMap::new();

        for row in &rows {
            let Ok(name) = row.get_named::<String>("Key_name") else {
                continue;
            };
            let Ok(column) = row.get_named::<String>("Column_name") else {
                continue;
            };
            let non_unique = row.get_named::<i64>("Non_unique").ok().unwrap_or(1);
            let index_type = row.get_named::<String>("Index_type").ok();
            let primary = name == "PRIMARY";

            index_map
                .entry(name.clone())
                .and_modify(|idx| idx.columns.push(column.clone()))
                .or_insert_with(|| IndexInfo {
                    name,
                    columns: vec![column],
                    unique: non_unique == 0,
                    index_type,
                    primary,
                });
        }

        Outcome::Ok(index_map.into_values().collect())
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn quote_sqlite_identifier(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn quote_mysql_identifier(name: &str) -> String {
    let escaped = name.replace('`', "``");
    format!("`{escaped}`")
}

/// Build a complete PostgreSQL type string from information_schema data.
fn build_postgres_type(
    data_type: &str,
    udt_name: &str,
    char_len: Option<i64>,
    precision: Option<i64>,
    scale: Option<i64>,
) -> String {
    // Handle array types
    if data_type == "ARRAY" {
        return format!("{}[]", udt_name.trim_start_matches('_'));
    }

    // For character types with length
    if let Some(len) = char_len {
        return format!("{}({})", data_type.to_uppercase(), len);
    }

    // For numeric types with precision/scale
    if let (Some(p), Some(s)) = (precision, scale) {
        if data_type == "numeric" {
            return format!("NUMERIC({},{})", p, s);
        }
    }

    // Default: just return the data type
    data_type.to_uppercase()
}

fn normalize_check_expression(definition: &str) -> String {
    let trimmed = definition.trim();
    let check_positions = keyword_positions_outside_quotes(trimmed, "CHECK");
    if let Some(check_pos) = check_positions.first().copied() {
        let mut cursor = check_pos + "CHECK".len();
        while cursor < trimmed.len() && trimmed.as_bytes()[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor < trimmed.len()
            && trimmed.as_bytes()[cursor] == b'('
            && let Some((expr, _)) = extract_parenthesized(trimmed, cursor)
        {
            return expr;
        }
    }
    trimmed.to_string()
}

fn extract_sqlite_check_constraints(create_table_sql: &str) -> Vec<CheckConstraintInfo> {
    let Some(definitions) = sqlite_table_definitions(create_table_sql) else {
        return Vec::new();
    };

    let mut checks = Vec::new();
    for definition in split_sqlite_definitions(definitions) {
        let constraint_positions = keyword_positions_outside_quotes(definition, "CONSTRAINT");
        let check_positions = keyword_positions_outside_quotes(definition, "CHECK");

        for check_pos in check_positions {
            let mut cursor = check_pos + "CHECK".len();
            while cursor < definition.len() && definition.as_bytes()[cursor].is_ascii_whitespace() {
                cursor += 1;
            }

            if cursor >= definition.len() || definition.as_bytes()[cursor] != b'(' {
                continue;
            }

            let Some((expression, _end_pos)) = extract_parenthesized(definition, cursor) else {
                continue;
            };

            checks.push(CheckConstraintInfo {
                name: sqlite_constraint_name_for_check(
                    definition,
                    check_pos,
                    &constraint_positions,
                ),
                expression,
            });
        }
    }

    checks
}

fn sqlite_table_definitions(create_table_sql: &str) -> Option<&str> {
    let mut start = None;
    let mut depth = 0usize;

    for (idx, byte) in create_table_sql.as_bytes().iter().copied().enumerate() {
        match byte {
            b'(' => {
                if start.is_none() {
                    start = Some(idx + 1);
                }
                depth += 1;
            }
            b')' if depth > 0 => {
                depth -= 1;
                if depth == 0 {
                    return start.map(|s| &create_table_sql[s..idx]);
                }
            }
            _ => {}
        }
    }

    None
}

fn split_sqlite_definitions(definitions: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let bytes = definitions.as_bytes();
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut i = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick_quote = false;
    let mut bracket_quote = false;

    while i < bytes.len() {
        let b = bytes[i];
        if single_quote {
            if b == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                single_quote = false;
            }
            i += 1;
            continue;
        }
        if double_quote {
            if b == b'"' {
                double_quote = false;
            }
            i += 1;
            continue;
        }
        if backtick_quote {
            if b == b'`' {
                backtick_quote = false;
            }
            i += 1;
            continue;
        }
        if bracket_quote {
            if b == b']' {
                bracket_quote = false;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' => single_quote = true,
            b'"' => double_quote = true,
            b'`' => backtick_quote = true,
            b'[' => bracket_quote = true,
            b'(' => depth += 1,
            b')' if depth > 0 => depth -= 1,
            b',' if depth == 0 => {
                let part = definitions[start..i].trim();
                if !part.is_empty() {
                    parts.push(part);
                }
                start = i + 1;
            }
            _ => {}
        }

        i += 1;
    }

    let tail = definitions[start..].trim();
    if !tail.is_empty() {
        parts.push(tail);
    }

    parts
}

fn keyword_positions_outside_quotes(input: &str, keyword: &str) -> Vec<usize> {
    if keyword.is_empty() || input.len() < keyword.len() {
        return Vec::new();
    }

    let bytes = input.as_bytes();
    let keyword_bytes = keyword.as_bytes();
    let mut positions = Vec::new();
    let mut i = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick_quote = false;
    let mut bracket_quote = false;

    while i < bytes.len() {
        let b = bytes[i];
        if single_quote {
            if b == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                single_quote = false;
            }
            i += 1;
            continue;
        }
        if double_quote {
            if b == b'"' {
                double_quote = false;
            }
            i += 1;
            continue;
        }
        if backtick_quote {
            if b == b'`' {
                backtick_quote = false;
            }
            i += 1;
            continue;
        }
        if bracket_quote {
            if b == b']' {
                bracket_quote = false;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' => {
                single_quote = true;
                i += 1;
                continue;
            }
            b'"' => {
                double_quote = true;
                i += 1;
                continue;
            }
            b'`' => {
                backtick_quote = true;
                i += 1;
                continue;
            }
            b'[' => {
                bracket_quote = true;
                i += 1;
                continue;
            }
            _ => {}
        }

        if i + keyword_bytes.len() <= bytes.len()
            && bytes[i..i + keyword_bytes.len()].eq_ignore_ascii_case(keyword_bytes)
            && (i == 0 || !is_identifier_byte(bytes[i - 1]))
            && (i + keyword_bytes.len() == bytes.len()
                || !is_identifier_byte(bytes[i + keyword_bytes.len()]))
        {
            positions.push(i);
            i += keyword_bytes.len();
            continue;
        }

        i += 1;
    }

    positions
}

fn sqlite_constraint_name_for_check(
    definition: &str,
    check_pos: usize,
    constraint_positions: &[usize],
) -> Option<String> {
    let constraint_pos = constraint_positions
        .iter()
        .copied()
        .rfind(|pos| *pos < check_pos)?;

    let mut cursor = constraint_pos + "CONSTRAINT".len();
    while cursor < definition.len() && definition.as_bytes()[cursor].is_ascii_whitespace() {
        cursor += 1;
    }
    if cursor >= definition.len() {
        return None;
    }

    let (name, _next) = parse_sqlite_identifier_token(definition, cursor)?;
    Some(name)
}

fn parse_sqlite_identifier_token(input: &str, start: usize) -> Option<(String, usize)> {
    let bytes = input.as_bytes();
    let first = *bytes.get(start)?;
    match first {
        b'"' => {
            let mut i = start + 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    let name = input[start + 1..i].replace("\"\"", "\"");
                    return Some((name, i + 1));
                }
                i += 1;
            }
            None
        }
        b'`' => {
            let mut i = start + 1;
            while i < bytes.len() {
                if bytes[i] == b'`' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'`' {
                        i += 2;
                        continue;
                    }
                    let name = input[start + 1..i].replace("``", "`");
                    return Some((name, i + 1));
                }
                i += 1;
            }
            None
        }
        b'[' => {
            let mut i = start + 1;
            while i < bytes.len() {
                if bytes[i] == b']' {
                    let name = input[start + 1..i].to_string();
                    return Some((name, i + 1));
                }
                i += 1;
            }
            None
        }
        _ => {
            let mut i = start;
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i == start {
                None
            } else {
                Some((input[start..i].to_string(), i))
            }
        }
    }
}

fn extract_parenthesized(input: &str, open_paren_pos: usize) -> Option<(String, usize)> {
    let bytes = input.as_bytes();
    if bytes.get(open_paren_pos).copied() != Some(b'(') {
        return None;
    }

    let mut depth = 0usize;
    let mut i = open_paren_pos;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick_quote = false;
    let mut bracket_quote = false;

    while i < bytes.len() {
        let b = bytes[i];
        if single_quote {
            if b == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                single_quote = false;
            }
            i += 1;
            continue;
        }
        if double_quote {
            if b == b'"' {
                double_quote = false;
            }
            i += 1;
            continue;
        }
        if backtick_quote {
            if b == b'`' {
                backtick_quote = false;
            }
            i += 1;
            continue;
        }
        if bracket_quote {
            if b == b']' {
                bracket_quote = false;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' => single_quote = true,
            b'"' => double_quote = true,
            b'`' => backtick_quote = true,
            b'[' => bracket_quote = true,
            b'(' => depth += 1,
            b')' => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                if depth == 0 {
                    let expression = input[open_paren_pos + 1..i].trim().to_string();
                    return Some((expression, i));
                }
            }
            _ => {}
        }
        i += 1;
    }

    None
}

fn is_identifier_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsed_sql_type_varchar() {
        let t = ParsedSqlType::parse("VARCHAR(255)");
        assert_eq!(t.base_type, "VARCHAR");
        assert_eq!(t.length, Some(255));
        assert_eq!(t.precision, None);
        assert_eq!(t.scale, None);
        assert!(!t.unsigned);
        assert!(!t.array);
    }

    #[test]
    fn test_parsed_sql_type_decimal() {
        let t = ParsedSqlType::parse("DECIMAL(10,2)");
        assert_eq!(t.base_type, "DECIMAL");
        assert_eq!(t.length, None);
        assert_eq!(t.precision, Some(10));
        assert_eq!(t.scale, Some(2));
    }

    #[test]
    fn test_parsed_sql_type_unsigned() {
        let t = ParsedSqlType::parse("INT UNSIGNED");
        assert_eq!(t.base_type, "INT");
        assert!(t.unsigned);
    }

    #[test]
    fn test_parsed_sql_type_array() {
        let t = ParsedSqlType::parse("TEXT[]");
        assert_eq!(t.base_type, "TEXT");
        assert!(t.array);
    }

    #[test]
    fn test_parsed_sql_type_simple() {
        let t = ParsedSqlType::parse("INTEGER");
        assert_eq!(t.base_type, "INTEGER");
        assert_eq!(t.length, None);
        assert!(!t.unsigned);
        assert!(!t.array);
    }

    #[test]
    fn test_parsed_sql_type_is_text() {
        assert!(ParsedSqlType::parse("VARCHAR(100)").is_text());
        assert!(ParsedSqlType::parse("TEXT").is_text());
        assert!(ParsedSqlType::parse("CHAR(1)").is_text());
        assert!(!ParsedSqlType::parse("INTEGER").is_text());
    }

    #[test]
    fn test_parsed_sql_type_is_numeric() {
        assert!(ParsedSqlType::parse("INTEGER").is_numeric());
        assert!(ParsedSqlType::parse("BIGINT").is_numeric());
        assert!(ParsedSqlType::parse("DECIMAL(10,2)").is_numeric());
        assert!(!ParsedSqlType::parse("TEXT").is_numeric());
    }

    #[test]
    fn test_parsed_sql_type_is_datetime() {
        assert!(ParsedSqlType::parse("DATE").is_datetime());
        assert!(ParsedSqlType::parse("TIMESTAMP").is_datetime());
        assert!(ParsedSqlType::parse("TIMESTAMPTZ").is_datetime());
        assert!(!ParsedSqlType::parse("TEXT").is_datetime());
    }

    #[test]
    fn test_database_schema_new() {
        let schema = DatabaseSchema::new(Dialect::Postgres);
        assert_eq!(schema.dialect, Dialect::Postgres);
        assert!(schema.tables.is_empty());
    }

    #[test]
    fn test_table_info_column() {
        let table = TableInfo {
            name: "test".to_string(),
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                sql_type: "INTEGER".to_string(),
                parsed_type: ParsedSqlType::parse("INTEGER"),
                nullable: false,
                default: None,
                primary_key: true,
                auto_increment: true,
                comment: None,
            }],
            primary_key: vec!["id".to_string()],
            foreign_keys: Vec::new(),
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            indexes: Vec::new(),
            comment: None,
        };

        assert!(table.column("id").is_some());
        assert!(table.column("nonexistent").is_none());
        assert!(table.has_auto_pk());
    }

    #[test]
    fn test_build_postgres_type_array() {
        let result = build_postgres_type("ARRAY", "_text", None, None, None);
        assert_eq!(result, "text[]");
    }

    #[test]
    fn test_build_postgres_type_varchar() {
        let result = build_postgres_type("character varying", "", Some(100), None, None);
        assert_eq!(result, "CHARACTER VARYING(100)");
    }

    #[test]
    fn test_build_postgres_type_numeric() {
        let result = build_postgres_type("numeric", "", None, Some(10), Some(2));
        assert_eq!(result, "NUMERIC(10,2)");
    }

    #[test]
    fn test_sanitize_identifier_normal() {
        assert_eq!(sanitize_identifier("users"), "users");
        assert_eq!(sanitize_identifier("my_table"), "my_table");
        assert_eq!(sanitize_identifier("Table123"), "Table123");
    }

    #[test]
    fn test_sanitize_identifier_sql_injection() {
        // SQL injection attempts should be sanitized
        assert_eq!(sanitize_identifier("users; DROP TABLE--"), "usersDROPTABLE");
        assert_eq!(sanitize_identifier("table`; malicious"), "tablemalicious");
        assert_eq!(sanitize_identifier("users'--"), "users");
        assert_eq!(
            sanitize_identifier("table\"); DROP TABLE users;--"),
            "tableDROPTABLEusers"
        );
    }

    #[test]
    fn test_sanitize_identifier_special_chars() {
        // Various special characters should be stripped
        assert_eq!(sanitize_identifier("table-name"), "tablename");
        assert_eq!(sanitize_identifier("table.name"), "tablename");
        assert_eq!(sanitize_identifier("table name"), "tablename");
        assert_eq!(sanitize_identifier("table\nname"), "tablename");
    }

    #[test]
    fn test_quote_sqlite_identifier_preserves_special_chars() {
        assert_eq!(quote_sqlite_identifier("my table"), "\"my table\"");
        assert_eq!(quote_sqlite_identifier("my\"table"), "\"my\"\"table\"");
    }

    #[test]
    fn test_quote_mysql_identifier_preserves_special_chars() {
        assert_eq!(quote_mysql_identifier("my-table"), "`my-table`");
        assert_eq!(quote_mysql_identifier("my`table"), "`my``table`");
    }

    #[test]
    fn test_normalize_check_expression_wrapped_check() {
        assert_eq!(
            normalize_check_expression("CHECK ((age >= 0) AND (age <= 150))"),
            "(age >= 0) AND (age <= 150)"
        );
    }

    #[test]
    fn test_normalize_check_expression_raw_clause() {
        assert_eq!(normalize_check_expression("(score > 0)"), "(score > 0)");
    }

    #[test]
    fn test_normalize_check_expression_with_quoted_commas() {
        assert_eq!(
            normalize_check_expression("CHECK (kind IN ('A,B', 'C'))"),
            "kind IN ('A,B', 'C')"
        );
    }

    #[test]
    fn test_extract_sqlite_check_constraints_named_and_unnamed() {
        let sql = r"
            CREATE TABLE heroes (
                id INTEGER PRIMARY KEY,
                age INTEGER,
                CONSTRAINT age_non_negative CHECK (age >= 0),
                CHECK (age <= 150)
            )
        ";

        let checks = extract_sqlite_check_constraints(sql);
        assert_eq!(checks.len(), 2);
        assert_eq!(checks[0].name.as_deref(), Some("age_non_negative"));
        assert_eq!(checks[0].expression, "age >= 0");
        assert_eq!(checks[1].name, None);
        assert_eq!(checks[1].expression, "age <= 150");
    }

    #[test]
    fn test_extract_sqlite_check_constraints_column_level_and_nested() {
        let sql = r"
            CREATE TABLE heroes (
                age INTEGER CONSTRAINT age_positive CHECK (age > 0),
                score INTEGER CHECK ((score >= 0) AND (score <= 100)),
                level INTEGER CHECK (level > 0) CHECK (level < 10)
            )
        ";

        let checks = extract_sqlite_check_constraints(sql);
        assert_eq!(checks.len(), 4);
        assert_eq!(checks[0].name.as_deref(), Some("age_positive"));
        assert_eq!(checks[0].expression, "age > 0");
        assert_eq!(checks[1].name, None);
        assert_eq!(checks[1].expression, "(score >= 0) AND (score <= 100)");
        assert_eq!(checks[2].expression, "level > 0");
        assert_eq!(checks[3].expression, "level < 10");
    }

    #[test]
    fn test_extract_sqlite_check_constraints_handles_quoted_commas() {
        let sql = r"
            CREATE TABLE heroes (
                kind TEXT CHECK (kind IN ('A,B', 'C')),
                note TEXT
            )
        ";

        let checks = extract_sqlite_check_constraints(sql);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].expression, "kind IN ('A,B', 'C')");
    }
}
