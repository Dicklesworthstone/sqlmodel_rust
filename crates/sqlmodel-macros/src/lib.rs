//! Procedural macros for SQLModel Rust.
//!
//! `sqlmodel-macros` is the **compile-time codegen layer**. It turns Rust structs into
//! fully described SQL models by generating static metadata and trait implementations.
//!
//! # Role In The Architecture
//!
//! - **Model metadata**: `#[derive(Model)]` produces a `Model` implementation with
//!   table/column metadata consumed by query, schema, and session layers.
//! - **Validation**: `#[derive(Validate)]` generates field validation glue.
//! - **Schema export**: `#[derive(JsonSchema)]` enables JSON schema generation for
//!   API documentation or tooling.
//!
//! These macros are used by application crates via the `sqlmodel` facade.

use proc_macro::TokenStream;
use syn::ext::IdentExt;

mod infer;
mod parse;
mod validate;
mod validate_derive;

use parse::{InheritanceStrategy, ModelDef, RelationshipKindAttr, parse_model};

/// Derive macro for the `Model` trait.
///
/// This macro generates implementations for:
/// - Table name and primary key metadata
/// - Field information
/// - Row conversion (to_row, from_row)
/// - Primary key access
///
/// # Attributes
///
/// - `#[sqlmodel(table = "name")]` - Override table name (defaults to snake_case struct name)
/// - `#[sqlmodel(primary_key)]` - Mark field as primary key
/// - `#[sqlmodel(auto_increment)]` - Mark field as auto-incrementing
/// - `#[sqlmodel(column = "name")]` - Override column name
/// - `#[sqlmodel(nullable)]` - Mark field as nullable
/// - `#[sqlmodel(unique)]` - Add unique constraint
/// - `#[sqlmodel(default = "expr")]` - Set default SQL expression
/// - `#[sqlmodel(foreign_key = "table.column")]` - Add foreign key reference
/// - `#[sqlmodel(index = "name")]` - Add to named index
/// - `#[sqlmodel(skip)]` - Skip this field in database operations
///
/// # Example
///
/// ```ignore
/// use sqlmodel::Model;
///
/// #[derive(Model)]
/// #[sqlmodel(table = "heroes")]
/// struct Hero {
///     #[sqlmodel(primary_key, auto_increment)]
///     id: Option<i64>,
///
///     #[sqlmodel(unique)]
///     name: String,
///
///     secret_name: String,
///
///     #[sqlmodel(nullable)]
///     age: Option<i32>,
///
///     #[sqlmodel(foreign_key = "teams.id")]
///     team_id: Option<i64>,
/// }
/// ```
#[proc_macro_derive(Model, attributes(sqlmodel))]
pub fn derive_model(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    // Parse the struct and its attributes
    let model = match parse_model(&input) {
        Ok(m) => m,
        Err(e) => return e.to_compile_error().into(),
    };

    // Validate the parsed model
    if let Err(e) = validate::validate_model(&model) {
        return e.to_compile_error().into();
    }

    // Generate the Model implementation
    generate_model_impl(&model).into()
}

/// Generate the Model trait implementation from parsed model definition.
fn generate_model_impl(model: &ModelDef) -> proc_macro2::TokenStream {
    let name = &model.name;
    let table_name_lit = &model.table_name;
    let (impl_generics, ty_generics, where_clause) = model.generics.split_for_impl();

    // If this is a single-table-inheritance child (inherits + discriminator_value),
    // its effective table is the parent table.
    let table_name_ts =
        if model.config.inherits.is_some() && model.config.discriminator_value.is_some() {
            let parent = model
                .config
                .inherits
                .as_deref()
                .expect("inherits checked above");
            let parent_ty_ts: proc_macro2::TokenStream =
                if let Ok(path) = syn::parse_str::<syn::Path>(parent) {
                    quote::quote! { #path }
                } else {
                    let ident = syn::Ident::new(parent, proc_macro2::Span::call_site());
                    quote::quote! { #ident }
                };
            quote::quote! { <#parent_ty_ts as sqlmodel_core::Model>::TABLE_NAME }
        } else {
            quote::quote! { #table_name_lit }
        };

    // Collect primary key field names
    let pk_fields: Vec<&str> = model
        .primary_key_fields()
        .iter()
        .map(|f| f.column_name.as_str())
        .collect();
    let pk_field_names: Vec<_> = pk_fields.clone();

    // If no explicit primary key, default to "id" if present
    let pk_slice = if pk_field_names.is_empty() {
        // Only default to "id" if an "id" field actually exists
        let has_id_field = model.fields.iter().any(|f| f.name == "id" && !f.skip);
        if has_id_field {
            quote::quote! { &["id"] }
        } else {
            quote::quote! { &[] }
        }
    } else {
        quote::quote! { &[#(#pk_field_names),*] }
    };

    // Generate static FieldInfo array for fields()
    let field_infos = generate_field_infos(model);

    // Generate RELATIONSHIPS constant
    let relationships = generate_relationships(model);

    // Generate to_row implementation
    let to_row_body = generate_to_row(model);

    // Generate from_row implementation
    let from_row_body = generate_from_row(model);

    // Generate primary_key_value implementation
    let pk_value_body = generate_primary_key_value(model);

    // Generate is_new implementation
    let is_new_body = generate_is_new(model);

    // Generate model_config implementation
    let model_config_body = generate_model_config(model);

    // Generate inheritance implementation
    let inheritance_body = generate_inheritance(model);

    // Generate shard_key implementation
    let (shard_key_const, shard_key_value_body) = generate_shard_key(model);

    // Generate joined-parent extraction for joined-table inheritance child models.
    let joined_parent_row_body = generate_joined_parent_row(model);

    // Generate Debug impl only if any field has repr=false
    let debug_impl = generate_debug_impl(model);

    // Generate hybrid property expr methods
    let hybrid_impl = generate_hybrid_methods(model);

    quote::quote! {
        impl #impl_generics sqlmodel_core::Model for #name #ty_generics #where_clause {
            const TABLE_NAME: &'static str = #table_name_ts;
            const PRIMARY_KEY: &'static [&'static str] = #pk_slice;
            const RELATIONSHIPS: &'static [sqlmodel_core::RelationshipInfo] = #relationships;
            const SHARD_KEY: Option<&'static str> = #shard_key_const;

            fn fields() -> &'static [sqlmodel_core::FieldInfo] {
                static FIELDS: &[sqlmodel_core::FieldInfo] = &[
                    #field_infos
                ];
                FIELDS
            }

            fn to_row(&self) -> Vec<(&'static str, sqlmodel_core::Value)> {
                #to_row_body
            }

            fn from_row(row: &sqlmodel_core::Row) -> sqlmodel_core::Result<Self> {
                #from_row_body
            }

            fn primary_key_value(&self) -> Vec<sqlmodel_core::Value> {
                #pk_value_body
            }

            fn is_new(&self) -> bool {
                #is_new_body
            }

            fn model_config() -> sqlmodel_core::ModelConfig {
                #model_config_body
            }

            fn inheritance() -> sqlmodel_core::InheritanceInfo {
                #inheritance_body
            }

            fn shard_key_value(&self) -> Option<sqlmodel_core::Value> {
                #shard_key_value_body
            }

            #joined_parent_row_body
        }

        #debug_impl

        #hybrid_impl
    }
}

/// Generate associated functions for hybrid properties.
///
/// For each field with `#[sqlmodel(hybrid, sql = "...")]`, generates
/// a `pub fn {field}_expr() -> sqlmodel_query::Expr` method that returns
/// `Expr::raw(sql)`.
fn generate_hybrid_methods(model: &ModelDef) -> proc_macro2::TokenStream {
    let hybrid_fields: Vec<_> = model
        .fields
        .iter()
        .filter_map(|f| {
            if !f.hybrid {
                return None;
            }
            let sql = f.hybrid_sql.as_deref()?;
            Some((f, sql))
        })
        .collect();

    if hybrid_fields.is_empty() {
        return quote::quote! {};
    }

    let name = &model.name;
    let (impl_generics, ty_generics, where_clause) = model.generics.split_for_impl();

    let methods: Vec<_> = hybrid_fields
        .iter()
        .map(|(field, sql)| {
            let method_name = quote::format_ident!("{}_expr", field.name);
            let doc = format!(
                "SQL expression for the `{}` hybrid property.\n\nGenerates: `{}`",
                field.name, sql
            );
            quote::quote! {
                #[doc = #doc]
                pub fn #method_name() -> sqlmodel_query::Expr {
                    sqlmodel_query::Expr::raw(#sql)
                }
            }
        })
        .collect();

    quote::quote! {
        impl #impl_generics #name #ty_generics #where_clause {
            #(#methods)*
        }
    }
}

fn generate_joined_parent_row(model: &ModelDef) -> proc_macro2::TokenStream {
    let is_joined_child =
        model.config.inheritance == InheritanceStrategy::Joined && model.config.inherits.is_some();
    if !is_joined_child {
        return quote::quote! {};
    }

    let Some(parent_field) = model.fields.iter().find(|f| f.parent) else {
        return quote::quote! {};
    };
    let parent_ident = &parent_field.name;

    quote::quote! {
        fn joined_parent_row(&self) -> Option<Vec<(&'static str, sqlmodel_core::Value)>> {
            Some(self.#parent_ident.to_row())
        }
    }
}

/// Convert a referential action string to the corresponding token.
fn referential_action_ts(action: &str) -> proc_macro2::TokenStream {
    match action.to_uppercase().as_str() {
        "NO ACTION" | "NOACTION" | "NO_ACTION" => {
            quote::quote! { sqlmodel_core::ReferentialAction::NoAction }
        }
        "RESTRICT" => quote::quote! { sqlmodel_core::ReferentialAction::Restrict },
        "CASCADE" => quote::quote! { sqlmodel_core::ReferentialAction::Cascade },
        "SET NULL" | "SETNULL" | "SET_NULL" => {
            quote::quote! { sqlmodel_core::ReferentialAction::SetNull }
        }
        "SET DEFAULT" | "SETDEFAULT" | "SET_DEFAULT" => {
            quote::quote! { sqlmodel_core::ReferentialAction::SetDefault }
        }
        _ => quote::quote! { sqlmodel_core::ReferentialAction::NoAction },
    }
}

/// Generate the static FieldInfo array contents.
fn generate_field_infos(model: &ModelDef) -> proc_macro2::TokenStream {
    let mut field_ts = Vec::new();

    // Use data_fields() to include computed fields in metadata (needed for serialization)
    for field in model.data_fields() {
        let field_ident = field.name.unraw();
        let column_name = &field.column_name;
        let primary_key = field.primary_key;
        let auto_increment = field.auto_increment;

        // Check if sa_column override is present
        let sa_col = field.sa_column.as_ref();

        // Nullable: sa_column.nullable takes precedence over field.nullable
        let nullable = sa_col.and_then(|sc| sc.nullable).unwrap_or(field.nullable);

        // Unique: sa_column.unique takes precedence over field.unique
        let unique = sa_col.and_then(|sc| sc.unique).unwrap_or(field.unique);

        // Determine SQL type: sa_column.sql_type > field.sql_type > inferred
        let effective_sql_type = sa_col
            .and_then(|sc| sc.sql_type.as_ref())
            .or(field.sql_type.as_ref());
        let sql_type_ts = if let Some(sql_type_str) = effective_sql_type {
            // Parse the explicit SQL type attribute string
            infer::parse_sql_type_attr(sql_type_str)
        } else {
            // Infer from Rust type (handles primitives, Option<T>, common library types)
            infer::infer_sql_type(&field.ty)
        };

        // If sql_type attribute was provided, also store the raw string as an override for DDL.
        let sql_type_override_ts = if let Some(sql_type_str) = effective_sql_type {
            quote::quote! { Some(#sql_type_str) }
        } else {
            quote::quote! { None }
        };

        // Default value: sa_column.server_default takes precedence over field.default
        let effective_default = sa_col
            .and_then(|sc| sc.server_default.as_ref())
            .or(field.default.as_ref());
        let default_ts = if let Some(d) = effective_default {
            quote::quote! { Some(#d) }
        } else {
            quote::quote! { None }
        };

        // Foreign key (validation prevents use with sa_column, so field value is always used)
        let fk_ts = if let Some(fk) = &field.foreign_key {
            quote::quote! { Some(#fk) }
        } else {
            quote::quote! { None }
        };

        // Index: sa_column.index takes precedence over field.index
        let effective_index = sa_col
            .and_then(|sc| sc.index.as_ref())
            .or(field.index.as_ref());
        let index_ts = if let Some(idx) = effective_index {
            quote::quote! { Some(#idx) }
        } else {
            quote::quote! { None }
        };

        // ON DELETE action
        let on_delete_ts = if let Some(ref action) = field.on_delete {
            let action_ts = referential_action_ts(action);
            quote::quote! { Some(#action_ts) }
        } else {
            quote::quote! { None }
        };

        // ON UPDATE action
        let on_update_ts = if let Some(ref action) = field.on_update {
            let action_ts = referential_action_ts(action);
            quote::quote! { Some(#action_ts) }
        } else {
            quote::quote! { None }
        };

        // Alias tokens
        let alias_ts = if let Some(ref alias) = field.alias {
            quote::quote! { Some(#alias) }
        } else {
            quote::quote! { None }
        };

        let validation_alias_ts = if let Some(ref val_alias) = field.validation_alias {
            quote::quote! { Some(#val_alias) }
        } else {
            quote::quote! { None }
        };

        let serialization_alias_ts = if let Some(ref ser_alias) = field.serialization_alias {
            quote::quote! { Some(#ser_alias) }
        } else {
            quote::quote! { None }
        };

        let computed = field.computed;
        let exclude = field.exclude;

        // Schema metadata tokens
        let title_ts = if let Some(ref title) = field.title {
            quote::quote! { Some(#title) }
        } else {
            quote::quote! { None }
        };

        let description_ts = if let Some(ref desc) = field.description {
            quote::quote! { Some(#desc) }
        } else {
            quote::quote! { None }
        };

        let schema_extra_ts = if let Some(ref extra) = field.schema_extra {
            quote::quote! { Some(#extra) }
        } else {
            quote::quote! { None }
        };

        // Default JSON for exclude_defaults support
        let default_json_ts = if let Some(ref dj) = field.default_json {
            quote::quote! { Some(#dj) }
        } else {
            quote::quote! { None }
        };

        // Const field
        let const_field = field.const_field;

        // Column constraints: sa_column.check is used if sa_column is present,
        // otherwise field.column_constraints (validation prevents both being set)
        let effective_constraints: Vec<&String> = if let Some(sc) = sa_col {
            sc.check.iter().collect()
        } else {
            field.column_constraints.iter().collect()
        };
        let column_constraints_ts = if effective_constraints.is_empty() {
            quote::quote! { &[] }
        } else {
            quote::quote! { &[#(#effective_constraints),*] }
        };

        // Column comment: sa_column.comment is used if sa_column is present,
        // otherwise field.column_comment (validation prevents both being set)
        let effective_comment = sa_col
            .and_then(|sc| sc.comment.as_ref())
            .or(field.column_comment.as_ref());
        let column_comment_ts = if let Some(comment) = effective_comment {
            quote::quote! { Some(#comment) }
        } else {
            quote::quote! { None }
        };

        // Column info
        let column_info_ts = if let Some(ref info) = field.column_info {
            quote::quote! { Some(#info) }
        } else {
            quote::quote! { None }
        };

        // Hybrid SQL expression
        let hybrid_sql_ts = if let Some(ref sql) = field.hybrid_sql {
            quote::quote! { Some(#sql) }
        } else {
            quote::quote! { None }
        };

        // Discriminator for union types
        let discriminator_ts = if let Some(ref disc) = field.discriminator {
            quote::quote! { Some(#disc) }
        } else {
            quote::quote! { None }
        };

        // Decimal precision (max_digits -> precision, decimal_places -> scale)
        let precision_ts = if let Some(p) = field.max_digits {
            quote::quote! { Some(#p) }
        } else {
            quote::quote! { None }
        };

        let scale_ts = if let Some(s) = field.decimal_places {
            quote::quote! { Some(#s) }
        } else {
            quote::quote! { None }
        };

        field_ts.push(quote::quote! {
            sqlmodel_core::FieldInfo::new(stringify!(#field_ident), #column_name, #sql_type_ts)
                .sql_type_override_opt(#sql_type_override_ts)
                .precision_opt(#precision_ts)
                .scale_opt(#scale_ts)
                .nullable(#nullable)
                .primary_key(#primary_key)
                .auto_increment(#auto_increment)
                .unique(#unique)
                .default_opt(#default_ts)
                .foreign_key_opt(#fk_ts)
                .on_delete_opt(#on_delete_ts)
                .on_update_opt(#on_update_ts)
                .index_opt(#index_ts)
                .alias_opt(#alias_ts)
                .validation_alias_opt(#validation_alias_ts)
                .serialization_alias_opt(#serialization_alias_ts)
                .computed(#computed)
                .exclude(#exclude)
                .title_opt(#title_ts)
                .description_opt(#description_ts)
                .schema_extra_opt(#schema_extra_ts)
                .default_json_opt(#default_json_ts)
                .const_field(#const_field)
                .column_constraints(#column_constraints_ts)
                .column_comment_opt(#column_comment_ts)
                .column_info_opt(#column_info_ts)
                .hybrid_sql_opt(#hybrid_sql_ts)
                .discriminator_opt(#discriminator_ts)
        });
    }

    quote::quote! { #(#field_ts),* }
}

/// Generate the to_row method body.
fn generate_to_row(model: &ModelDef) -> proc_macro2::TokenStream {
    let mut conversions = Vec::new();

    for field in model.select_fields() {
        let field_name = &field.name;
        let column_name = &field.column_name;

        // Convert field to Value
        if parse::is_option_type(&field.ty) {
            conversions.push(quote::quote! {
                (#column_name, match &self.#field_name {
                    Some(v) => sqlmodel_core::Value::from(v.clone()),
                    None => sqlmodel_core::Value::Null,
                })
            });
        } else {
            conversions.push(quote::quote! {
                (#column_name, sqlmodel_core::Value::from(self.#field_name.clone()))
            });
        }
    }

    quote::quote! {
        let mut out = vec![#(#conversions),*];

        // Single-table inheritance child models should always emit their discriminator
        // so inserts/updates can round-trip correctly even if the struct doesn't have a
        // dedicated discriminator field.
        let inh = <Self as sqlmodel_core::Model>::inheritance();
        if let (Some(col), Some(val)) = (inh.discriminator_column, inh.discriminator_value) {
            if !out.iter().any(|(c, _)| *c == col) {
                out.push((col, sqlmodel_core::Value::from(val)));
            }
        }

        out
    }
}

/// Generate the from_row method body.
fn generate_from_row(model: &ModelDef) -> proc_macro2::TokenStream {
    let name = &model.name;
    let mut field_extractions = Vec::new();

    // Support both "plain" rows (SELECT *) and prefixed/aliased rows (e.g. eager loading,
    // joined inheritance) by looking for `table__col` prefixes.
    let row_ident = quote::format_ident!("local_row");

    for field in model.select_fields() {
        let field_name = &field.name;
        let column_name = &field.column_name;

        if parse::is_option_type(&field.ty) {
            // For Option<T> fields, handle NULL gracefully
            field_extractions.push(quote::quote! {
                #field_name: #row_ident.get_named(#column_name).ok()
            });
        } else {
            // For required fields, propagate errors
            field_extractions.push(quote::quote! {
                #field_name: #row_ident.get_named(#column_name)?
            });
        }
    }

    // Handle skipped fields with Default
    let skipped_fields: Vec<_> = model
        .fields
        .iter()
        .filter(|f| f.skip)
        .map(|f| {
            let field_name = &f.name;
            quote::quote! { #field_name: Default::default() }
        })
        .collect();

    // Handle relationship fields with Default (they're not in the DB row)
    let relationship_fields: Vec<_> = model
        .fields
        .iter()
        .filter(|f| f.relationship.is_some())
        .map(|f| {
            let field_name = &f.name;
            quote::quote! { #field_name: Default::default() }
        })
        .collect();

    // Joined-table inheritance parent field hydration.
    let parent_fields: Vec<_> = model
        .fields
        .iter()
        .filter(|f| f.parent)
        .map(|f| {
            let field_name = &f.name;
            let ty = &f.ty;
            quote::quote! {
                #field_name: {
                    let inh = <Self as sqlmodel_core::Model>::inheritance();
                    let parent_table = inh.parent.ok_or_else(|| {
                        sqlmodel_core::Error::Custom(
                            "joined inheritance parent_table missing in inheritance metadata".to_string(),
                        )
                    })?;
                    if !row.has_prefix(parent_table) {
                        return Err(sqlmodel_core::Error::Custom(format!(
                            "expected prefixed parent columns for joined inheritance: {}__*",
                            parent_table
                        )));
                    }
                    let prow = row.subset_by_prefix(parent_table);
                    <#ty as sqlmodel_core::Model>::from_row(&prow)?
                }
            }
        })
        .collect();

    // Handle computed fields with Default (they're not in the DB row)
    let computed_fields: Vec<_> = model
        .computed_fields()
        .iter()
        .map(|f| {
            let field_name = &f.name;
            quote::quote! { #field_name: Default::default() }
        })
        .collect();

    quote::quote! {
        let #row_ident = if row.has_prefix(<Self as sqlmodel_core::Model>::TABLE_NAME) {
            row.subset_by_prefix(<Self as sqlmodel_core::Model>::TABLE_NAME)
        } else {
            row.clone()
        };

        Ok(#name {
            #(#field_extractions,)*
            #(#skipped_fields,)*
            #(#relationship_fields,)*
            #(#parent_fields,)*
            #(#computed_fields,)*
        })
    }
}

/// Generate the primary_key_value method body.
fn generate_primary_key_value(model: &ModelDef) -> proc_macro2::TokenStream {
    let pk_fields = model.primary_key_fields();

    if pk_fields.is_empty() {
        // Try to use "id" field if it exists
        let id_field = model.fields.iter().find(|f| f.name == "id");
        if let Some(field) = id_field {
            let field_name = &field.name;
            if parse::is_option_type(&field.ty) {
                return quote::quote! {
                    match &self.#field_name {
                        Some(v) => vec![sqlmodel_core::Value::from(v.clone())],
                        None => vec![sqlmodel_core::Value::Null],
                    }
                };
            }
            return quote::quote! {
                vec![sqlmodel_core::Value::from(self.#field_name.clone())]
            };
        }
        return quote::quote! { vec![] };
    }

    let mut value_exprs = Vec::new();
    for field in pk_fields {
        let field_name = &field.name;
        if parse::is_option_type(&field.ty) {
            value_exprs.push(quote::quote! {
                match &self.#field_name {
                    Some(v) => sqlmodel_core::Value::from(v.clone()),
                    None => sqlmodel_core::Value::Null,
                }
            });
        } else {
            value_exprs.push(quote::quote! {
                sqlmodel_core::Value::from(self.#field_name.clone())
            });
        }
    }

    quote::quote! {
        vec![#(#value_exprs),*]
    }
}

/// Generate the is_new method body.
fn generate_is_new(model: &ModelDef) -> proc_macro2::TokenStream {
    let pk_fields = model.primary_key_fields();

    // If there's an auto_increment primary key field that is Option<T>,
    // check if it's None
    for field in &pk_fields {
        if field.auto_increment && parse::is_option_type(&field.ty) {
            let field_name = &field.name;
            return quote::quote! {
                self.#field_name.is_none()
            };
        }
    }

    // Otherwise, try "id" field if it exists and is Option<T>
    if let Some(id_field) = model.fields.iter().find(|f| f.name == "id") {
        if parse::is_option_type(&id_field.ty) {
            return quote::quote! {
                self.id.is_none()
            };
        }
    }

    // Default: cannot determine, always return true
    quote::quote! { true }
}

/// Generate the model_config method body.
fn generate_model_config(model: &ModelDef) -> proc_macro2::TokenStream {
    let config = &model.config;

    let table = config.table;
    let from_attributes = config.from_attributes;
    let validate_assignment = config.validate_assignment;
    let strict = config.strict;
    let populate_by_name = config.populate_by_name;
    let use_enum_values = config.use_enum_values;
    let arbitrary_types_allowed = config.arbitrary_types_allowed;
    let defer_build = config.defer_build;
    let revalidate_instances = config.revalidate_instances;

    // Handle extra field behavior
    let extra_ts = match config.extra.as_str() {
        "forbid" => quote::quote! { sqlmodel_core::ExtraFieldsBehavior::Forbid },
        "allow" => quote::quote! { sqlmodel_core::ExtraFieldsBehavior::Allow },
        _ => quote::quote! { sqlmodel_core::ExtraFieldsBehavior::Ignore },
    };

    // Handle optional string fields
    let json_schema_extra_ts = if let Some(ref extra) = config.json_schema_extra {
        quote::quote! { Some(#extra) }
    } else {
        quote::quote! { None }
    };

    let title_ts = if let Some(ref title) = config.title {
        quote::quote! { Some(#title) }
    } else {
        quote::quote! { None }
    };

    quote::quote! {
        sqlmodel_core::ModelConfig {
            table: #table,
            from_attributes: #from_attributes,
            validate_assignment: #validate_assignment,
            extra: #extra_ts,
            strict: #strict,
            populate_by_name: #populate_by_name,
            use_enum_values: #use_enum_values,
            arbitrary_types_allowed: #arbitrary_types_allowed,
            defer_build: #defer_build,
            revalidate_instances: #revalidate_instances,
            json_schema_extra: #json_schema_extra_ts,
            title: #title_ts,
        }
    }
}

/// Generate the inheritance method body.
fn generate_inheritance(model: &ModelDef) -> proc_macro2::TokenStream {
    use crate::parse::InheritanceStrategy;

    let config = &model.config;

    // Determine the inheritance strategy token
    let strategy_ts = match config.inheritance {
        InheritanceStrategy::None => {
            quote::quote! { sqlmodel_core::InheritanceStrategy::None }
        }
        InheritanceStrategy::Single => {
            quote::quote! { sqlmodel_core::InheritanceStrategy::Single }
        }
        InheritanceStrategy::Joined => {
            quote::quote! { sqlmodel_core::InheritanceStrategy::Joined }
        }
        InheritanceStrategy::Concrete => {
            quote::quote! { sqlmodel_core::InheritanceStrategy::Concrete }
        }
    };

    // Helper: interpret `inherits = "..."` as a Rust type path in the current scope.
    // We keep it as a string in parsing so attribute syntax stays simple; here we
    // translate it into type tokens for codegen.
    let parent_ty_ts: Option<proc_macro2::TokenStream> = config.inherits.as_deref().map(|p| {
        if let Ok(path) = syn::parse_str::<syn::Path>(p) {
            quote::quote! { #path }
        } else {
            let ident = syn::Ident::new(p, proc_macro2::Span::call_site());
            quote::quote! { #ident }
        }
    });

    // Store parent table name (not parent Rust type name) in metadata so schema/DDL can be correct.
    let parent_table_ts = if let Some(ref parent_ty) = parent_ty_ts {
        quote::quote! { Some(<#parent_ty as sqlmodel_core::Model>::TABLE_NAME) }
    } else {
        quote::quote! { None }
    };

    let parent_fields_fn_ts = if let Some(ref parent_ty) = parent_ty_ts {
        quote::quote! { Some(<#parent_ty as sqlmodel_core::Model>::fields) }
    } else {
        quote::quote! { None }
    };

    // Handle discriminator column.
    //
    // - Base STI models specify it explicitly: `#[sqlmodel(inheritance="single", discriminator="...")]`
    // - Child STI models inherit it from the parent so query/schema tooling has the column name
    //   available without requiring duplicate annotation.
    let discriminator_column_ts = if let Some(ref column) = config.discriminator_column {
        quote::quote! { Some(#column) }
    } else if config.discriminator_value.is_some() {
        if let Some(parent_ty) = parent_ty_ts.as_ref() {
            quote::quote! { <#parent_ty as sqlmodel_core::Model>::inheritance().discriminator_column }
        } else {
            quote::quote! { None }
        }
    } else {
        quote::quote! { None }
    };

    // Handle discriminator value (for child models)
    let discriminator_value_ts = if let Some(ref value) = config.discriminator_value {
        quote::quote! { Some(#value) }
    } else {
        quote::quote! { None }
    };

    quote::quote! {
        sqlmodel_core::InheritanceInfo {
            strategy: #strategy_ts,
            parent: #parent_table_ts,
            parent_fields_fn: #parent_fields_fn_ts,
            discriminator_column: #discriminator_column_ts,
            discriminator_value: #discriminator_value_ts,
        }
    }
}

/// Generate the shard_key constant and shard_key_value method body.
///
/// Returns a tuple of (const token, method body token) for:
/// - `const SHARD_KEY: Option<&'static str>`
/// - `fn shard_key_value(&self) -> Option<Value>`
fn generate_shard_key(model: &ModelDef) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let config = &model.config;

    if let Some(ref shard_key_name) = config.shard_key {
        // Find the shard key field to get its type info
        let shard_field = model.fields.iter().find(|f| f.name == shard_key_name);

        let const_ts = quote::quote! { Some(#shard_key_name) };

        // Generate the method body based on whether the field exists and its type
        let value_body = if let Some(field) = shard_field {
            let field_ident = &field.name;
            if parse::is_option_type(&field.ty) {
                // Option<T> field: return Some(value) if Some, None if None
                quote::quote! {
                    match &self.#field_ident {
                        Some(v) => Some(sqlmodel_core::Value::from(v.clone())),
                        None => None,
                    }
                }
            } else {
                // Non-optional field: always has a value
                quote::quote! {
                    Some(sqlmodel_core::Value::from(self.#field_ident.clone()))
                }
            }
        } else {
            // Field not found - this is a compile error in validation,
            // but generate safe fallback code
            quote::quote! { None }
        };

        (const_ts, value_body)
    } else {
        // No shard key configured
        let const_ts = quote::quote! { None };
        let value_body = quote::quote! { None };
        (const_ts, value_body)
    }
}

/// Generate a custom Debug implementation if any field has repr=false.
///
/// This generates a Debug impl that excludes fields marked with `repr = false`,
/// which is useful for hiding sensitive data like passwords from debug output.
fn generate_debug_impl(model: &ModelDef) -> proc_macro2::TokenStream {
    // Check if any field has repr=false
    let has_hidden_fields = model.fields.iter().any(|f| !f.repr);

    // Only generate custom Debug if there are hidden fields
    if !has_hidden_fields {
        return quote::quote! {};
    }

    let name = &model.name;
    let (impl_generics, ty_generics, where_clause) = model.generics.split_for_impl();

    // Generate field entries for Debug, excluding fields with repr=false
    let debug_fields: Vec<_> = model
        .fields
        .iter()
        .filter(|f| f.repr) // Only include fields with repr=true
        .map(|f| {
            let field_name = &f.name;
            let field_name_str = field_name.to_string();
            quote::quote! {
                .field(#field_name_str, &self.#field_name)
            }
        })
        .collect();

    let struct_name_str = name.to_string();

    quote::quote! {
        impl #impl_generics ::core::fmt::Debug for #name #ty_generics #where_clause {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.debug_struct(#struct_name_str)
                    #(#debug_fields)*
                    .finish()
            }
        }
    }
}

/// Generate the RELATIONSHIPS constant from relationship fields.
fn generate_relationships(model: &ModelDef) -> proc_macro2::TokenStream {
    fn relationship_inner_model_ty(ty: &syn::Type) -> Option<syn::Type> {
        let syn::Type::Path(tp) = ty else {
            return None;
        };

        let last = tp.path.segments.last()?;
        let ident = last.ident.to_string();
        if ident != "Related" && ident != "RelatedMany" && ident != "Lazy" {
            return None;
        }

        let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
            return None;
        };

        args.args.iter().find_map(|arg| match arg {
            syn::GenericArgument::Type(t) => Some(t.clone()),
            _ => None,
        })
    }

    let relationship_fields = model.relationship_fields();

    if relationship_fields.is_empty() {
        return quote::quote! { &[] };
    }

    let mut relationship_ts = Vec::new();

    for field in relationship_fields {
        let Some(rel) = field.relationship.as_ref() else {
            relationship_ts.push(quote::quote! {
                ::core::compile_error!(
                    "sqlmodel: internal error: relationship field missing parsed relationship metadata"
                )
            });
            continue;
        };
        let field_name = &field.name;
        let related_table = &rel.model;

        let Some(related_ty) = relationship_inner_model_ty(&field.ty) else {
            relationship_ts.push(quote::quote! {
                ::core::compile_error!(
                    "sqlmodel: relationship field type must be Related<T>, RelatedMany<T>, or Lazy<T>"
                )
            });
            continue;
        };

        // Determine RelationshipKind token
        let kind_ts = match rel.kind {
            RelationshipKindAttr::OneToOne => {
                quote::quote! { sqlmodel_core::RelationshipKind::OneToOne }
            }
            RelationshipKindAttr::ManyToOne => {
                quote::quote! { sqlmodel_core::RelationshipKind::ManyToOne }
            }
            RelationshipKindAttr::OneToMany => {
                quote::quote! { sqlmodel_core::RelationshipKind::OneToMany }
            }
            RelationshipKindAttr::ManyToMany => {
                quote::quote! { sqlmodel_core::RelationshipKind::ManyToMany }
            }
        };

        // Build optional method calls
        let local_key_call = if let Some(ref fk) = rel.foreign_key {
            quote::quote! { .local_key(#fk) }
        } else {
            quote::quote! {}
        };

        let remote_key_call = if let Some(ref rk) = rel.remote_key {
            quote::quote! { .remote_key(#rk) }
        } else {
            quote::quote! {}
        };

        let back_populates_call = if let Some(ref bp) = rel.back_populates {
            quote::quote! { .back_populates(#bp) }
        } else {
            quote::quote! {}
        };

        let link_table_call = if let Some(ref lt) = rel.link_table {
            let table = &lt.table;
            let local_col = &lt.local_column;
            let remote_col = &lt.remote_column;
            quote::quote! {
                .link_table(sqlmodel_core::LinkTableInfo::new(#table, #local_col, #remote_col))
            }
        } else {
            quote::quote! {}
        };

        let lazy_val = rel.lazy;
        let cascade_val = rel.cascade_delete;
        let passive_deletes_ts = match rel.passive_deletes {
            crate::parse::PassiveDeletesAttr::Active => {
                quote::quote! { sqlmodel_core::PassiveDeletes::Active }
            }
            crate::parse::PassiveDeletesAttr::Passive => {
                quote::quote! { sqlmodel_core::PassiveDeletes::Passive }
            }
            crate::parse::PassiveDeletesAttr::All => {
                quote::quote! { sqlmodel_core::PassiveDeletes::All }
            }
        };

        // New sa_relationship fields
        let order_by_call = if let Some(ref ob) = rel.order_by {
            quote::quote! { .order_by(#ob) }
        } else {
            quote::quote! {}
        };

        let lazy_strategy_call = if let Some(ref strategy) = rel.lazy_strategy {
            let strategy_ts = match strategy {
                crate::parse::LazyLoadStrategyAttr::Select => {
                    quote::quote! { sqlmodel_core::LazyLoadStrategy::Select }
                }
                crate::parse::LazyLoadStrategyAttr::Joined => {
                    quote::quote! { sqlmodel_core::LazyLoadStrategy::Joined }
                }
                crate::parse::LazyLoadStrategyAttr::Subquery => {
                    quote::quote! { sqlmodel_core::LazyLoadStrategy::Subquery }
                }
                crate::parse::LazyLoadStrategyAttr::Selectin => {
                    quote::quote! { sqlmodel_core::LazyLoadStrategy::Selectin }
                }
                crate::parse::LazyLoadStrategyAttr::Dynamic => {
                    quote::quote! { sqlmodel_core::LazyLoadStrategy::Dynamic }
                }
                crate::parse::LazyLoadStrategyAttr::NoLoad => {
                    quote::quote! { sqlmodel_core::LazyLoadStrategy::NoLoad }
                }
                crate::parse::LazyLoadStrategyAttr::RaiseOnSql => {
                    quote::quote! { sqlmodel_core::LazyLoadStrategy::RaiseOnSql }
                }
                crate::parse::LazyLoadStrategyAttr::WriteOnly => {
                    quote::quote! { sqlmodel_core::LazyLoadStrategy::WriteOnly }
                }
            };
            quote::quote! { .lazy_strategy(#strategy_ts) }
        } else {
            quote::quote! {}
        };

        let cascade_call = if let Some(ref c) = rel.cascade {
            quote::quote! { .cascade(#c) }
        } else {
            quote::quote! {}
        };

        let uselist_call = if let Some(ul) = rel.uselist {
            quote::quote! { .uselist(#ul) }
        } else {
            quote::quote! {}
        };

        relationship_ts.push(quote::quote! {
            sqlmodel_core::RelationshipInfo::new(
                stringify!(#field_name),
                #related_table,
                #kind_ts
            )
            .related_fields(<#related_ty as sqlmodel_core::Model>::fields)
            #local_key_call
            #remote_key_call
            #back_populates_call
            #link_table_call
            .lazy(#lazy_val)
            .cascade_delete(#cascade_val)
            .passive_deletes(#passive_deletes_ts)
            #order_by_call
            #lazy_strategy_call
            #cascade_call
            #uselist_call
        });
    }

    quote::quote! {
        &[#(#relationship_ts),*]
    }
}

/// Derive macro for field validation.
///
/// Generates a `validate()` method that checks field constraints at runtime.
///
/// # Attributes
///
/// - `#[validate(min = N)]` - Minimum value for numbers
/// - `#[validate(max = N)]` - Maximum value for numbers
/// - `#[validate(min_length = N)]` - Minimum length for strings
/// - `#[validate(max_length = N)]` - Maximum length for strings
/// - `#[validate(pattern = "regex")]` - Regex pattern for strings
/// - `#[validate(email)]` - Email format validation
/// - `#[validate(url)]` - URL format validation
/// - `#[validate(required)]` - Mark an Option<T> field as required
/// - `#[validate(custom = "fn_name")]` - Custom validation function
///
/// # Example
///
/// ```ignore
/// use sqlmodel::Validate;
///
/// #[derive(Validate)]
/// struct User {
///     #[validate(min_length = 1, max_length = 100)]
///     name: String,
///
///     #[validate(min = 0, max = 150)]
///     age: i32,
///
///     #[validate(email)]
///     email: String,
///
///     #[validate(required)]
///     team_id: Option<i64>,
/// }
///
/// let user = User {
///     name: "".to_string(),
///     age: 200,
///     email: "invalid".to_string(),
///     team_id: None,
/// };
///
/// // Returns Err with all validation failures
/// let result = user.validate();
/// assert!(result.is_err());
/// ```
#[proc_macro_derive(Validate, attributes(validate))]
pub fn derive_validate(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    // Parse the struct and its validation attributes
    let def = match validate_derive::parse_validate(&input) {
        Ok(d) => d,
        Err(e) => return e.to_compile_error().into(),
    };

    // Generate the validation implementation
    validate_derive::generate_validate_impl(&def).into()
}

/// Derive macro for SQL enum types.
///
/// Generates `SqlEnum` trait implementation, `From<EnumType> for Value`,
/// `TryFrom<Value> for EnumType`, and `Display`/`FromStr` implementations.
///
/// Enum variants are mapped to their snake_case string representations by default.
/// Use `#[sqlmodel(rename = "custom_name")]` on variants to override.
///
/// # Example
///
/// ```ignore
/// #[derive(SqlEnum, Debug, Clone, PartialEq)]
/// enum Status {
///     Active,
///     Inactive,
///     #[sqlmodel(rename = "on_hold")]
///     OnHold,
/// }
/// ```
#[proc_macro_derive(SqlEnum, attributes(sqlmodel))]
pub fn derive_sql_enum(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    match generate_sql_enum_impl(&input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn generate_sql_enum_impl(input: &syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let syn::Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "SqlEnum can only be derived for enums",
        ));
    };

    // Collect variant info
    let mut variant_names = Vec::new();
    let mut variant_strings = Vec::new();

    for variant in &data.variants {
        if !variant.fields.is_empty() {
            return Err(syn::Error::new_spanned(
                variant,
                "SqlEnum variants must be unit variants (no fields)",
            ));
        }

        let ident = &variant.ident;
        variant_names.push(ident.clone());

        // Check for #[sqlmodel(rename = "...")] attribute
        let mut custom_name = None;
        for attr in &variant.attrs {
            if attr.path().is_ident("sqlmodel") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("rename") {
                        let value = meta.value()?;
                        let s: syn::LitStr = value.parse()?;
                        custom_name = Some(s.value());
                    }
                    Ok(())
                })?;
            }
        }

        let sql_str = custom_name.unwrap_or_else(|| to_snake_case(&ident.to_string()));
        variant_strings.push(sql_str);
    }

    let type_name = to_snake_case(&name.to_string());

    // Generate static VARIANTS array
    let variant_str_refs: Vec<_> = variant_strings.iter().map(|s| s.as_str()).collect();

    let to_sql_arms: Vec<_> = variant_names
        .iter()
        .zip(variant_strings.iter())
        .map(|(ident, s)| {
            quote::quote! { #name::#ident => #s }
        })
        .collect();

    let from_sql_arms: Vec<_> = variant_names
        .iter()
        .zip(variant_strings.iter())
        .map(|(ident, s)| {
            quote::quote! { #s => Ok(#name::#ident) }
        })
        .collect();

    // Build the error message listing valid values
    let valid_values: String = variant_strings
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");
    let error_msg = format!(
        "invalid value for {}: expected one of {}",
        name, valid_values
    );

    Ok(quote::quote! {
        impl #impl_generics sqlmodel_core::SqlEnum for #name #ty_generics #where_clause {
            const VARIANTS: &'static [&'static str] = &[#(#variant_str_refs),*];
            const TYPE_NAME: &'static str = #type_name;

            fn to_sql_str(&self) -> &'static str {
                match self {
                    #(#to_sql_arms,)*
                }
            }

            fn from_sql_str(s: &str) -> Result<Self, String> {
                match s {
                    #(#from_sql_arms,)*
                    _ => Err(format!("{}, got '{}'", #error_msg, s)),
                }
            }
        }

        impl #impl_generics From<#name #ty_generics> for sqlmodel_core::Value #where_clause {
            fn from(v: #name #ty_generics) -> Self {
                sqlmodel_core::Value::Text(
                    sqlmodel_core::SqlEnum::to_sql_str(&v).to_string()
                )
            }
        }

        impl #impl_generics From<&#name #ty_generics> for sqlmodel_core::Value #where_clause {
            fn from(v: &#name #ty_generics) -> Self {
                sqlmodel_core::Value::Text(
                    sqlmodel_core::SqlEnum::to_sql_str(v).to_string()
                )
            }
        }

        impl #impl_generics TryFrom<sqlmodel_core::Value> for #name #ty_generics #where_clause {
            type Error = sqlmodel_core::Error;

            fn try_from(value: sqlmodel_core::Value) -> Result<Self, Self::Error> {
                match value {
                    sqlmodel_core::Value::Text(ref s) => {
                        sqlmodel_core::SqlEnum::from_sql_str(s.as_str()).map_err(|e| {
                            sqlmodel_core::Error::Type(sqlmodel_core::error::TypeError {
                                expected: <#name as sqlmodel_core::SqlEnum>::TYPE_NAME,
                                actual: e,
                                column: None,
                                rust_type: None,
                            })
                        })
                    }
                    other => Err(sqlmodel_core::Error::Type(sqlmodel_core::error::TypeError {
                        expected: <#name as sqlmodel_core::SqlEnum>::TYPE_NAME,
                        actual: other.type_name().to_string(),
                        column: None,
                        rust_type: None,
                    })),
                }
            }
        }

        impl #impl_generics ::core::fmt::Display for #name #ty_generics #where_clause {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(sqlmodel_core::SqlEnum::to_sql_str(self))
            }
        }

        impl #impl_generics ::core::str::FromStr for #name #ty_generics #where_clause {
            type Err = String;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                sqlmodel_core::SqlEnum::from_sql_str(s)
            }
        }
    })
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    let chars: Vec<char> = s.chars().collect();
    for (i, &ch) in chars.iter().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                let prev_lower = chars[i - 1].is_lowercase();
                let next_lower = chars.get(i + 1).is_some_and(|c| c.is_lowercase());
                // "FooBar" -> "foo_bar": insert _ when prev is lowercase
                // "HTTPStatus" -> "http_status": insert _ when next is lowercase (acronym boundary)
                if prev_lower || (next_lower && chars[i - 1].is_uppercase()) {
                    result.push('_');
                }
            }
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push(ch);
        }
    }
    result
}

/// Attribute macro for defining SQL functions in handlers.
///
/// # Example
///
/// ```ignore
/// #[sqlmodel::query]
/// async fn get_heroes(cx: &Cx, conn: &impl Connection) -> Vec<Hero> {
///     sqlmodel::select!(Hero).all(cx, conn).await
/// }
/// ```
#[proc_macro_attribute]
pub fn query(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let original = item.clone();

    let func: syn::ItemFn = match syn::parse(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };

    if func.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            func.sig.fn_token,
            "#[sqlmodel::query] requires an async fn",
        )
        .to_compile_error()
        .into();
    }

    let Some(first_arg) = func.sig.inputs.first() else {
        return syn::Error::new_spanned(
            &func.sig.ident,
            "#[sqlmodel::query] requires the first parameter to be `cx: &Cx`",
        )
        .to_compile_error()
        .into();
    };

    let first_ty = match first_arg {
        syn::FnArg::Typed(pat_ty) => &*pat_ty.ty,
        syn::FnArg::Receiver(recv) => {
            return syn::Error::new_spanned(
                recv,
                "#[sqlmodel::query] does not support methods; use a free function",
            )
            .to_compile_error()
            .into();
        }
    };

    if !is_ref_to_cx(first_ty) {
        return syn::Error::new_spanned(
            first_ty,
            "#[sqlmodel::query] requires the first parameter to be `cx: &Cx`",
        )
        .to_compile_error()
        .into();
    }

    original
}

fn is_ref_to_cx(ty: &syn::Type) -> bool {
    let syn::Type::Reference(r) = ty else {
        return false;
    };
    is_cx_path(&r.elem)
}

fn is_cx_path(ty: &syn::Type) -> bool {
    let syn::Type::Path(p) = ty else {
        return false;
    };
    p.path.segments.last().is_some_and(|seg| seg.ident == "Cx")
}
