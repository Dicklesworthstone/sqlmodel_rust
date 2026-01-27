//! Procedural macros for SQLModel Rust.
//!
//! This crate provides derive macros for:
//! - `Model` - ORM-style struct mapping
//! - `Validate` - Field validation
//! - `JsonSchema` - JSON Schema generation (for OpenAPI)

use proc_macro::TokenStream;
use syn::ext::IdentExt;

mod infer;
mod parse;
mod validate;
mod validate_derive;

use parse::{ModelDef, RelationshipKindAttr, parse_model};

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
    let table_name = &model.table_name;
    let (impl_generics, ty_generics, where_clause) = model.generics.split_for_impl();

    // Collect primary key field names
    let pk_fields: Vec<&str> = model
        .primary_key_fields()
        .iter()
        .map(|f| f.column_name.as_str())
        .collect();
    let pk_field_names: Vec<_> = pk_fields.clone();

    // If no explicit primary key, default to "id" if present
    let pk_slice = if pk_field_names.is_empty() {
        quote::quote! { &["id"] }
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

    quote::quote! {
        impl #impl_generics sqlmodel_core::Model for #name #ty_generics #where_clause {
            const TABLE_NAME: &'static str = #table_name;
            const PRIMARY_KEY: &'static [&'static str] = #pk_slice;
            const RELATIONSHIPS: &'static [sqlmodel_core::RelationshipInfo] = #relationships;

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
        }
    }
}

/// Convert a referential action string to the corresponding token.
fn referential_action_token(action: &str) -> proc_macro2::TokenStream {
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
    let mut field_tokens = Vec::new();

    for field in model.select_fields() {
        let field_ident = field.name.unraw();
        let column_name = &field.column_name;
        let nullable = field.nullable;
        let primary_key = field.primary_key;
        let auto_increment = field.auto_increment;
        let unique = field.unique;

        // Determine SQL type: use explicit attribute or infer from Rust type
        let sql_type_token = if let Some(ref sql_type_str) = field.sql_type {
            // Parse the explicit SQL type attribute string
            infer::parse_sql_type_attr(sql_type_str)
        } else {
            // Infer from Rust type (handles primitives, Option<T>, common library types)
            infer::infer_sql_type(&field.ty)
        };

        // If sql_type attribute was provided, also store the raw string as an override for DDL.
        let sql_type_override_token = if let Some(ref sql_type_str) = field.sql_type {
            quote::quote! { Some(#sql_type_str) }
        } else {
            quote::quote! { None }
        };

        // Default value
        let default_token = if let Some(d) = &field.default {
            quote::quote! { Some(#d) }
        } else {
            quote::quote! { None }
        };

        // Foreign key
        let fk_token = if let Some(fk) = &field.foreign_key {
            quote::quote! { Some(#fk) }
        } else {
            quote::quote! { None }
        };

        // Index
        let index_token = if let Some(idx) = &field.index {
            quote::quote! { Some(#idx) }
        } else {
            quote::quote! { None }
        };

        // ON DELETE action
        let on_delete_token = if let Some(ref action) = field.on_delete {
            let action_token = referential_action_token(action);
            quote::quote! { Some(#action_token) }
        } else {
            quote::quote! { None }
        };

        // ON UPDATE action
        let on_update_token = if let Some(ref action) = field.on_update {
            let action_token = referential_action_token(action);
            quote::quote! { Some(#action_token) }
        } else {
            quote::quote! { None }
        };

        field_tokens.push(quote::quote! {
            sqlmodel_core::FieldInfo::new(stringify!(#field_ident), #column_name, #sql_type_token)
                .sql_type_override_opt(#sql_type_override_token)
                .nullable(#nullable)
                .primary_key(#primary_key)
                .auto_increment(#auto_increment)
                .unique(#unique)
                .default_opt(#default_token)
                .foreign_key_opt(#fk_token)
                .on_delete_opt(#on_delete_token)
                .on_update_opt(#on_update_token)
                .index_opt(#index_token)
        });
    }

    quote::quote! { #(#field_tokens),* }
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
        vec![#(#conversions),*]
    }
}

/// Generate the from_row method body.
fn generate_from_row(model: &ModelDef) -> proc_macro2::TokenStream {
    let name = &model.name;
    let mut field_extractions = Vec::new();

    for field in model.select_fields() {
        let field_name = &field.name;
        let column_name = &field.column_name;

        if parse::is_option_type(&field.ty) {
            // For Option<T> fields, handle NULL gracefully
            field_extractions.push(quote::quote! {
                #field_name: row.get_named(#column_name).ok()
            });
        } else {
            // For required fields, propagate errors
            field_extractions.push(quote::quote! {
                #field_name: row.get_named(#column_name)?
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

    quote::quote! {
        Ok(#name {
            #(#field_extractions,)*
            #(#skipped_fields,)*
            #(#relationship_fields,)*
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

/// Generate the RELATIONSHIPS constant from relationship fields.
fn generate_relationships(model: &ModelDef) -> proc_macro2::TokenStream {
    let relationship_fields = model.relationship_fields();

    if relationship_fields.is_empty() {
        return quote::quote! { &[] };
    }

    let mut relationship_tokens = Vec::new();

    for field in relationship_fields {
        let rel = field.relationship.as_ref().unwrap();
        let field_name = &field.name;
        let related_table = &rel.model;

        // Determine RelationshipKind token
        let kind_token = match rel.kind {
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

        relationship_tokens.push(quote::quote! {
            sqlmodel_core::RelationshipInfo::new(
                stringify!(#field_name),
                #related_table,
                #kind_token
            )
            #local_key_call
            #remote_key_call
            #back_populates_call
            #link_table_call
            .lazy(#lazy_val)
            .cascade_delete(#cascade_val)
        });
    }

    quote::quote! {
        &[#(#relationship_tokens),*]
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
    // TODO: Implement query attribute macro
    item
}
