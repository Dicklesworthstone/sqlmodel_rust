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

    // Generate Debug impl only if any field has repr=false
    let debug_impl = generate_debug_impl(model);

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

            fn model_config() -> sqlmodel_core::ModelConfig {
                #model_config_body
            }
        }

        #debug_impl
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

    // Use data_fields() to include computed fields in metadata (needed for serialization)
    for field in model.data_fields() {
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

        // Alias tokens
        let alias_token = if let Some(ref alias) = field.alias {
            quote::quote! { Some(#alias) }
        } else {
            quote::quote! { None }
        };

        let validation_alias_token = if let Some(ref val_alias) = field.validation_alias {
            quote::quote! { Some(#val_alias) }
        } else {
            quote::quote! { None }
        };

        let serialization_alias_token = if let Some(ref ser_alias) = field.serialization_alias {
            quote::quote! { Some(#ser_alias) }
        } else {
            quote::quote! { None }
        };

        let computed = field.computed;
        let exclude = field.exclude;

        // Schema metadata tokens
        let title_token = if let Some(ref title) = field.title {
            quote::quote! { Some(#title) }
        } else {
            quote::quote! { None }
        };

        let description_token = if let Some(ref desc) = field.description {
            quote::quote! { Some(#desc) }
        } else {
            quote::quote! { None }
        };

        let schema_extra_token = if let Some(ref extra) = field.schema_extra {
            quote::quote! { Some(#extra) }
        } else {
            quote::quote! { None }
        };

        // Default JSON for exclude_defaults support
        let default_json_token = if let Some(ref dj) = field.default_json {
            quote::quote! { Some(#dj) }
        } else {
            quote::quote! { None }
        };

        // Const field
        let const_field = field.const_field;

        // Column constraints
        let column_constraints = &field.column_constraints;
        let column_constraints_token = if column_constraints.is_empty() {
            quote::quote! { &[] }
        } else {
            quote::quote! { &[#(#column_constraints),*] }
        };

        // Column comment
        let column_comment_token = if let Some(ref comment) = field.column_comment {
            quote::quote! { Some(#comment) }
        } else {
            quote::quote! { None }
        };

        // Column info
        let column_info_token = if let Some(ref info) = field.column_info {
            quote::quote! { Some(#info) }
        } else {
            quote::quote! { None }
        };

        // Decimal precision (max_digits -> precision, decimal_places -> scale)
        let precision_token = if let Some(p) = field.max_digits {
            quote::quote! { Some(#p) }
        } else {
            quote::quote! { None }
        };

        let scale_token = if let Some(s) = field.decimal_places {
            quote::quote! { Some(#s) }
        } else {
            quote::quote! { None }
        };

        field_tokens.push(quote::quote! {
            sqlmodel_core::FieldInfo::new(stringify!(#field_ident), #column_name, #sql_type_token)
                .sql_type_override_opt(#sql_type_override_token)
                .precision_opt(#precision_token)
                .scale_opt(#scale_token)
                .nullable(#nullable)
                .primary_key(#primary_key)
                .auto_increment(#auto_increment)
                .unique(#unique)
                .default_opt(#default_token)
                .foreign_key_opt(#fk_token)
                .on_delete_opt(#on_delete_token)
                .on_update_opt(#on_update_token)
                .index_opt(#index_token)
                .alias_opt(#alias_token)
                .validation_alias_opt(#validation_alias_token)
                .serialization_alias_opt(#serialization_alias_token)
                .computed(#computed)
                .exclude(#exclude)
                .title_opt(#title_token)
                .description_opt(#description_token)
                .schema_extra_opt(#schema_extra_token)
                .default_json_opt(#default_json_token)
                .const_field(#const_field)
                .column_constraints(#column_constraints_token)
                .column_comment_opt(#column_comment_token)
                .column_info_opt(#column_info_token)
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
        Ok(#name {
            #(#field_extractions,)*
            #(#skipped_fields,)*
            #(#relationship_fields,)*
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
    let extra_token = match config.extra.as_str() {
        "forbid" => quote::quote! { sqlmodel_core::ExtraFieldsBehavior::Forbid },
        "allow" => quote::quote! { sqlmodel_core::ExtraFieldsBehavior::Allow },
        _ => quote::quote! { sqlmodel_core::ExtraFieldsBehavior::Ignore },
    };

    // Handle optional string fields
    let json_schema_extra_token = if let Some(ref extra) = config.json_schema_extra {
        quote::quote! { Some(#extra) }
    } else {
        quote::quote! { None }
    };

    let title_token = if let Some(ref title) = config.title {
        quote::quote! { Some(#title) }
    } else {
        quote::quote! { None }
    };

    quote::quote! {
        sqlmodel_core::ModelConfig {
            table: #table,
            from_attributes: #from_attributes,
            validate_assignment: #validate_assignment,
            extra: #extra_token,
            strict: #strict,
            populate_by_name: #populate_by_name,
            use_enum_values: #use_enum_values,
            arbitrary_types_allowed: #arbitrary_types_allowed,
            defer_build: #defer_build,
            revalidate_instances: #revalidate_instances,
            json_schema_extra: #json_schema_extra_token,
            title: #title_token,
        }
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
    // Stub: query attribute macro is a pass-through placeholder for future SQL validation.
    // When implemented, it will provide compile-time SQL validation and query optimization hints.
    item
}
