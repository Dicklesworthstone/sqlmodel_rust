//! Parsing logic for the Model derive macro.
//!
//! This module extracts struct-level and field-level attributes from the
//! derive input to build `ModelDef` and `FieldDef` structures used for
//! code generation.

use proc_macro2::Span;
use quote::ToTokens;
use syn::{Attribute, Data, DeriveInput, Error, Field, Fields, Generics, Ident, Lit, Result, Type};

/// Model-level configuration parsed from attributes.
#[derive(Debug, Clone, Default)]
pub struct ModelConfigParsed {
    /// Whether this model maps to a database table.
    pub table: bool,
    /// Allow reading data from object attributes (ORM mode).
    pub from_attributes: bool,
    /// Validate field values when they are assigned.
    pub validate_assignment: bool,
    /// How to handle extra fields: "ignore", "forbid", or "allow".
    pub extra: String,
    /// Enable strict type checking during validation.
    pub strict: bool,
    /// Allow populating fields by either name or alias.
    pub populate_by_name: bool,
    /// Use enum values instead of names during serialization.
    pub use_enum_values: bool,
    /// Allow arbitrary types in fields.
    pub arbitrary_types_allowed: bool,
    /// Defer model validation to allow forward references.
    pub defer_build: bool,
    /// Revalidate instances when converting to this model.
    pub revalidate_instances: bool,
    /// Custom JSON schema extra data.
    pub json_schema_extra: Option<String>,
    /// Title for JSON schema generation.
    pub title: Option<String>,
}

/// Parsed model definition from a struct with `#[derive(Model)]`.
#[derive(Debug)]
pub struct ModelDef {
    /// The struct name (e.g., `Hero`).
    pub name: Ident,
    /// The SQL table name (e.g., `"heroes"`).
    pub table_name: String,
    /// Optional table alias for queries (reserved for future use).
    #[allow(dead_code)]
    pub table_alias: Option<String>,
    /// Parsed field definitions.
    pub fields: Vec<FieldDef>,
    /// Generic parameters from the struct.
    pub generics: Generics,
    /// Model-level configuration.
    pub config: ModelConfigParsed,
}

/// Parsed field definition from a struct field.
#[derive(Debug)]
pub struct FieldDef {
    /// The Rust field name (e.g., `secret_name`).
    pub name: Ident,
    /// The SQL column name (e.g., `"secret_name"` or custom override).
    pub column_name: String,
    /// The Rust type of the field.
    pub ty: Type,
    /// Optional SQL type override (e.g., `"VARCHAR(100)"`).
    pub sql_type: Option<String>,
    /// Whether the field allows NULL values.
    pub nullable: bool,
    /// Whether this field is (part of) the primary key.
    pub primary_key: bool,
    /// Whether the field auto-increments.
    pub auto_increment: bool,
    /// Whether the field has a UNIQUE constraint.
    pub unique: bool,
    /// Foreign key reference (e.g., `"teams.id"`).
    pub foreign_key: Option<String>,
    /// ON DELETE referential action (e.g., "CASCADE", "SET NULL").
    pub on_delete: Option<String>,
    /// ON UPDATE referential action (e.g., "CASCADE", "NO ACTION").
    pub on_update: Option<String>,
    /// SQL DEFAULT expression.
    pub default: Option<String>,
    /// Index name if this field is part of an index.
    pub index: Option<String>,
    /// Skip this field entirely in database operations.
    pub skip: bool,
    /// Skip this field in INSERT operations (reserved for future use).
    #[allow(dead_code)]
    pub skip_insert: bool,
    /// Skip this field in UPDATE operations (reserved for future use).
    #[allow(dead_code)]
    pub skip_update: bool,
    /// Relationship definition (if this is a relationship field).
    pub relationship: Option<RelationshipAttr>,
    /// Alias for both input and output (like serde rename).
    pub alias: Option<String>,
    /// Alias used only during deserialization/validation (input-only).
    pub validation_alias: Option<String>,
    /// Alias used only during serialization (output-only).
    pub serialization_alias: Option<String>,
    /// Whether this is a computed field (not stored in database).
    pub computed: bool,
    /// Total number of digits for Decimal/Numeric types (precision).
    /// Maps to DECIMAL(max_digits, decimal_places) in SQL.
    pub max_digits: Option<u8>,
    /// Number of digits after decimal point for Decimal/Numeric types (scale).
    /// Maps to DECIMAL(max_digits, decimal_places) in SQL.
    pub decimal_places: Option<u8>,
    /// Exclude this field from serialization (model_dump).
    pub exclude: bool,
    /// Schema title for JSON Schema generation.
    pub title: Option<String>,
    /// Schema description for JSON Schema generation.
    pub description: Option<String>,
    /// Extra JSON Schema properties (merged into schema).
    pub schema_extra: Option<String>,
    /// JSON representation of the field's default value (for exclude_defaults).
    /// Used by model_dump with exclude_defaults=true to compare current value.
    pub default_json: Option<String>,
    /// Whether to include this field in Debug output (repr).
    /// Defaults to true. Set to false for sensitive fields like passwords.
    pub repr: bool,
    /// Whether this field is constant (immutable after creation).
    /// Const fields cannot be modified after initial construction.
    pub const_field: bool,
    /// Additional SQL constraints for DDL generation (e.g., CHECK constraints).
    pub column_constraints: Vec<String>,
    /// SQL comment for the column.
    pub column_comment: Option<String>,
    /// Extra metadata as JSON string.
    pub column_info: Option<String>,
    /// Complete column specification override (sa_column).
    /// When set, provides full column control and disables other column attributes.
    pub sa_column: Option<SaColumnDef>,
}

/// Parsed relationship attribute from `#[sqlmodel(relationship(...))]`.
#[derive(Debug, Clone)]
pub struct RelationshipAttr {
    /// Related model's table name (e.g., "teams" or the model name).
    pub model: String,
    /// Local FK column (for ManyToOne/OneToOne).
    pub foreign_key: Option<String>,
    /// Remote FK column (for OneToMany).
    pub remote_key: Option<String>,
    /// Link table for ManyToMany relationships.
    pub link_table: Option<LinkTableAttr>,
    /// The field on the related model that points back.
    pub back_populates: Option<String>,
    /// Whether to use lazy loading (simple flag, superseded by lazy_strategy).
    pub lazy: bool,
    /// Cascade delete behavior.
    pub cascade_delete: bool,
    /// Passive deletes: let DB handle cascade deletes instead of ORM.
    pub passive_deletes: PassiveDeletesAttr,
    /// Inferred relationship kind from field type.
    pub kind: RelationshipKindAttr,
    /// Default ordering for related items (e.g., "Child::name" or "name DESC").
    pub order_by: Option<String>,
    /// Loading strategy for the relationship.
    pub lazy_strategy: Option<LazyLoadStrategyAttr>,
    /// Full cascade options string (e.g., "all, delete-orphan").
    pub cascade: Option<String>,
    /// Force list or single (override inference from field type).
    pub uselist: Option<bool>,
}

/// Lazy loading strategy for relationships.
///
/// Maps to SQLAlchemy's relationship lazy parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LazyLoadStrategyAttr {
    /// Load items on first access (default).
    Select,
    /// Eager load via JOIN.
    Joined,
    /// Eager load via separate SELECT with IN clause.
    Subquery,
    /// Load via subquery into parent query.
    Selectin,
    /// Return a query object instead of loading items.
    Dynamic,
    /// Never load (raise error on access).
    NoLoad,
    /// Always raise error on access (for write-only relationships).
    RaiseOnSql,
    /// Write-only collection.
    WriteOnly,
}

/// Link table configuration for many-to-many relationships.
#[derive(Debug, Clone)]
pub struct LinkTableAttr {
    /// The link table name.
    pub table: String,
    /// Column pointing to the local model.
    pub local_column: String,
    /// Column pointing to the remote model.
    pub remote_column: String,
}

/// Relationship kind as detected from field type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipKindAttr {
    /// One-to-one relationship.
    OneToOne,
    /// Many-to-one (foreign key on this model).
    ManyToOne,
    /// One-to-many (foreign key on related model).
    OneToMany,
    /// Many-to-many via link table.
    ManyToMany,
}

/// Passive deletes behavior parsed from attribute.
///
/// Controls whether ORM emits DELETE or relies on DB cascade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PassiveDeletesAttr {
    /// ORM handles deletes (default).
    #[default]
    Active,
    /// Let database handle via ON DELETE CASCADE.
    Passive,
    /// Passive + disable orphan tracking entirely.
    All,
}

/// Complete column specification override via `sa_column(...)`.
///
/// When used, this provides full control over the column definition,
/// replacing individual column attributes like `sql_type`, `nullable`, etc.
///
/// # Example
///
/// ```ignore
/// #[derive(Model)]
/// struct User {
///     #[sqlmodel(sa_column(
///         sql_type = "VARCHAR(50)",
///         check = "status IN ('active', 'inactive', 'pending')",
///         server_default = "'pending'",
///         comment = "User account status"
///     ))]
///     status: String,
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct SaColumnDef {
    /// SQL type override (e.g., "VARCHAR(50)", "DECIMAL(10,2)").
    pub sql_type: Option<String>,
    /// CHECK constraints for the column.
    pub check: Vec<String>,
    /// Server default value expression (e.g., "'pending'", "NOW()").
    pub server_default: Option<String>,
    /// Column comment.
    pub comment: Option<String>,
    /// Whether the column is nullable (overrides type inference).
    pub nullable: Option<bool>,
    /// Whether the column should have a unique constraint.
    pub unique: Option<bool>,
    /// Index name if this column should be indexed.
    pub index: Option<String>,
}

impl ModelDef {
    /// Returns the fields that are part of the primary key.
    pub fn primary_key_fields(&self) -> Vec<&FieldDef> {
        self.fields.iter().filter(|f| f.primary_key).collect()
    }

    /// Returns fields that should be included in INSERT statements (reserved for future use).
    /// Excludes skipped fields, computed fields, and relationship fields.
    #[allow(dead_code)]
    pub fn insert_fields(&self) -> Vec<&FieldDef> {
        self.fields
            .iter()
            .filter(|f| !f.skip && !f.skip_insert && !f.computed && f.relationship.is_none())
            .collect()
    }

    /// Returns fields that should be included in UPDATE statements (reserved for future use).
    /// Excludes skipped fields, computed fields, primary key fields, and relationship fields.
    #[allow(dead_code)]
    pub fn update_fields(&self) -> Vec<&FieldDef> {
        self.fields
            .iter()
            .filter(|f| {
                !f.skip
                    && !f.skip_update
                    && !f.computed
                    && !f.primary_key
                    && f.relationship.is_none()
            })
            .collect()
    }

    /// Returns fields that should be read from the database (SELECT).
    /// Excludes skipped fields, relationship fields, and computed fields (they're not DB columns).
    pub fn select_fields(&self) -> Vec<&FieldDef> {
        self.fields
            .iter()
            .filter(|f| !f.skip && !f.computed && f.relationship.is_none())
            .collect()
    }

    /// Returns all data fields for model metadata (Model::fields()).
    /// Includes computed fields but excludes skipped fields and relationship fields.
    /// This is used for serialization/validation which needs to know about all fields.
    pub fn data_fields(&self) -> Vec<&FieldDef> {
        self.fields
            .iter()
            .filter(|f| !f.skip && f.relationship.is_none())
            .collect()
    }

    /// Returns fields that are relationships (Related<T>, RelatedMany<T>).
    pub fn relationship_fields(&self) -> Vec<&FieldDef> {
        self.fields
            .iter()
            .filter(|f| f.relationship.is_some())
            .collect()
    }

    /// Returns fields that are computed (not stored in database).
    pub fn computed_fields(&self) -> Vec<&FieldDef> {
        self.fields.iter().filter(|f| f.computed).collect()
    }
}

impl FieldDef {
    /// Returns the name to use when serializing this field (output).
    ///
    /// Priority: serialization_alias > alias > field name
    #[allow(dead_code)]
    pub fn output_name(&self) -> &str {
        self.serialization_alias
            .as_deref()
            .or(self.alias.as_deref())
            .unwrap_or_else(|| self.name.to_string().leak())
    }

    /// Returns all names that should be accepted when deserializing (input).
    ///
    /// This includes: field name, alias, and validation_alias.
    #[allow(dead_code)]
    pub fn input_names(&self) -> Vec<&str> {
        let field_name = self.name.to_string();
        let mut names = vec![field_name.leak() as &str];

        if let Some(ref alias) = self.alias {
            if !names.contains(&alias.as_str()) {
                names.push(alias.as_str());
            }
        }

        if let Some(ref val_alias) = self.validation_alias {
            if !names.contains(&val_alias.as_str()) {
                names.push(val_alias.as_str());
            }
        }

        names
    }

    /// Returns true if this field has any alias configuration.
    #[allow(dead_code)]
    pub fn has_alias(&self) -> bool {
        self.alias.is_some()
            || self.validation_alias.is_some()
            || self.serialization_alias.is_some()
    }
}

/// Parse a `DeriveInput` into a `ModelDef`.
///
/// # Errors
///
/// Returns an error if:
/// - The input is not a struct
/// - The struct uses tuple or unit syntax (must have named fields)
/// - Unknown attributes are present
/// - Attribute values are invalid
pub fn parse_model(input: &DeriveInput) -> Result<ModelDef> {
    let name = input.ident.clone();
    let generics = input.generics.clone();

    // Parse struct-level attributes
    let StructAttrs {
        table_name,
        table_alias,
        config,
    } = parse_struct_sqlmodel_attrs(&input.attrs, &name)?;

    // Get struct fields
    let fields = match &input.data {
        Data::Struct(data) => parse_fields(&data.fields)?,
        Data::Enum(_) => {
            return Err(Error::new_spanned(
                input,
                "Model can only be derived for structs, not enums",
            ));
        }
        Data::Union(_) => {
            return Err(Error::new_spanned(
                input,
                "Model can only be derived for structs, not unions",
            ));
        }
    };

    // Validate: at least one field should be a primary key, or warn
    // (we don't error because some use cases may not need a PK)
    let has_pk = fields.iter().any(|f| f.primary_key);
    if !has_pk {
        // Check if there's a field named "id" we could implicitly use
        // For now, just allow it - the generate phase will handle defaults
    }

    Ok(ModelDef {
        name,
        table_name,
        table_alias,
        fields,
        generics,
        config,
    })
}

/// Parsed struct-level attributes result.
struct StructAttrs {
    table_name: String,
    table_alias: Option<String>,
    config: ModelConfigParsed,
}

/// Parse struct-level `#[sqlmodel(...)]` attributes.
///
/// Supported keys:
/// - `table = "name"` (overrides derived table name)
/// - `table_alias = "alias"` (optional table alias)
/// - Model config options (from_attributes, validate_assignment, extra, strict, etc.)
fn parse_struct_sqlmodel_attrs(attrs: &[Attribute], struct_name: &Ident) -> Result<StructAttrs> {
    let mut table_name: Option<String> = None;
    let mut table_alias: Option<String> = None;
    let mut config = ModelConfigParsed::default();

    for attr in attrs {
        if !attr.path().is_ident("sqlmodel") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("table") {
                // Check if it's a flag (no value) or has a value
                if meta.input.peek(syn::Token![=]) {
                    let value: Lit = meta.value()?.parse()?;
                    if let Lit::Str(lit_str) = value {
                        if table_name.is_some() {
                            return Err(Error::new_spanned(
                                meta.path,
                                "duplicate sqlmodel attribute: table",
                            ));
                        }
                        table_name = Some(lit_str.value());
                    } else {
                        return Err(Error::new_spanned(
                            value,
                            "expected string literal for table name",
                        ));
                    }
                } else {
                    // Flag form: #[sqlmodel(table)]
                    config.table = true;
                }
                Ok(())
            } else if meta.path.is_ident("table_alias") {
                if table_alias.is_some() {
                    return Err(Error::new_spanned(
                        meta.path,
                        "duplicate sqlmodel attribute: table_alias",
                    ));
                }

                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    table_alias = Some(lit_str.value());
                    Ok(())
                } else {
                    Err(Error::new_spanned(
                        value,
                        "expected string literal for table_alias",
                    ))
                }
            // Model config options
            } else if meta.path.is_ident("from_attributes") {
                config.from_attributes = true;
                Ok(())
            } else if meta.path.is_ident("validate_assignment") {
                config.validate_assignment = true;
                Ok(())
            } else if meta.path.is_ident("extra") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    let extra_value = lit_str.value().to_lowercase();
                    if !["ignore", "forbid", "allow"].contains(&extra_value.as_str()) {
                        return Err(Error::new_spanned(
                            lit_str,
                            "extra must be one of: 'ignore', 'forbid', 'allow'",
                        ));
                    }
                    config.extra = extra_value;
                    Ok(())
                } else {
                    Err(Error::new_spanned(
                        value,
                        "expected string literal for extra",
                    ))
                }
            } else if meta.path.is_ident("strict") {
                config.strict = true;
                Ok(())
            } else if meta.path.is_ident("populate_by_name") {
                config.populate_by_name = true;
                Ok(())
            } else if meta.path.is_ident("use_enum_values") {
                config.use_enum_values = true;
                Ok(())
            } else if meta.path.is_ident("arbitrary_types_allowed") {
                config.arbitrary_types_allowed = true;
                Ok(())
            } else if meta.path.is_ident("defer_build") {
                config.defer_build = true;
                Ok(())
            } else if meta.path.is_ident("revalidate_instances") {
                config.revalidate_instances = true;
                Ok(())
            } else if meta.path.is_ident("json_schema_extra") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    config.json_schema_extra = Some(lit_str.value());
                    Ok(())
                } else {
                    Err(Error::new_spanned(
                        value,
                        "expected string literal for json_schema_extra",
                    ))
                }
            } else if meta.path.is_ident("title") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    config.title = Some(lit_str.value());
                    Ok(())
                } else {
                    Err(Error::new_spanned(value, "expected string literal for title"))
                }
            } else {
                Err(Error::new_spanned(
                    meta.path,
                    "unknown sqlmodel struct attribute (supported: table, table_alias, from_attributes, \
                     validate_assignment, extra, strict, populate_by_name, use_enum_values, \
                     arbitrary_types_allowed, defer_build, revalidate_instances, json_schema_extra, title)",
                ))
            }
        })?;
    }

    let table_name = table_name.unwrap_or_else(|| derive_table_name(&struct_name.to_string()));
    Ok(StructAttrs {
        table_name,
        table_alias,
        config,
    })
}

/// Derive table name from struct name: convert to snake_case and pluralize.
///
/// Examples:
/// - `Hero` -> `heroes`
/// - `TeamMember` -> `team_members`
/// - `Person` -> `people`
/// - `Category` -> `categories`
fn derive_table_name(struct_name: &str) -> String {
    let snake = to_snake_case(struct_name);
    pluralize(&snake)
}

/// Convert PascalCase to snake_case.
///
/// Examples:
/// - `Hero` -> `hero`
/// - `TeamMember` -> `team_member`
/// - `HTTPServer` -> `http_server`
fn to_snake_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    let chars: Vec<char> = s.chars().collect();

    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                let prev = chars[i - 1];
                let next = chars.get(i + 1).copied();

                // Add underscore if:
                // 1. Previous char was lowercase (transitioning from word to new word)
                // 2. OR this is the start of a word after an acronym
                //    (current is uppercase, next is lowercase, and previous was uppercase)
                let should_underscore = prev.is_lowercase()
                    || (prev.is_uppercase() && next.is_some_and(|n| n.is_lowercase()));

                if should_underscore {
                    result.push('_');
                }
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }

    result
}

/// Simple English pluralization.
///
/// Rules:
/// - Words ending in 's', 'x', 'z', 'ch', 'sh' -> add 'es'
/// - Words ending in 'y' preceded by consonant -> change 'y' to 'ies'
/// - Words ending in 'f' or 'fe' -> change to 'ves'
/// - Special cases: person -> people, child -> children, etc.
/// - Default: add 's'
fn pluralize(word: &str) -> String {
    // Handle special cases first
    match word {
        "person" => return "people".to_string(),
        "child" => return "children".to_string(),
        "man" => return "men".to_string(),
        "woman" => return "women".to_string(),
        "foot" => return "feet".to_string(),
        "tooth" => return "teeth".to_string(),
        "goose" => return "geese".to_string(),
        "mouse" => return "mice".to_string(),
        "datum" => return "data".to_string(),
        "index" => return "indices".to_string(),
        "matrix" => return "matrices".to_string(),
        "vertex" => return "vertices".to_string(),
        "analysis" => return "analyses".to_string(),
        "crisis" => return "crises".to_string(),
        "axis" => return "axes".to_string(),
        _ => {}
    }

    if word.is_empty() {
        return word.to_string();
    }

    // Words ending in 's', 'x', 'ch', 'sh' -> add 'es'
    if word.ends_with('s') || word.ends_with('x') || word.ends_with("ch") || word.ends_with("sh") {
        return format!("{word}es");
    }

    // Words ending in 'z': double the 'z' if preceded by a vowel, then add 'es'
    // e.g., quiz -> quizzes, fez -> fezzes
    if word.ends_with('z') {
        let chars: Vec<char> = word.chars().collect();
        if chars.len() >= 2 {
            let second_last = chars[chars.len() - 2];
            if "aeiou".contains(second_last) {
                // Short vowel before 'z', double the 'z'
                return format!("{word}zes");
            }
        }
        return format!("{word}es");
    }

    // Words ending in 'y' preceded by consonant -> change 'y' to 'ies'
    if let Some(stripped) = word.strip_suffix('y') {
        let chars: Vec<char> = stripped.chars().collect();
        if let Some(&second_last) = chars.last() {
            if !"aeiou".contains(second_last) {
                return format!("{stripped}ies");
            }
        }
        return format!("{word}s");
    }

    // Words ending in 'fe' -> change to 'ves' (check before 'f')
    if let Some(stripped) = word.strip_suffix("fe") {
        return format!("{stripped}ves");
    }

    // Words ending in 'f' -> change to 'ves'
    if let Some(stripped) = word.strip_suffix('f') {
        return format!("{stripped}ves");
    }

    // Words ending in 'o' (after consonant) -> add 'es'
    if word.ends_with('o') {
        let chars: Vec<char> = word.chars().collect();
        if chars.len() >= 2 {
            let second_last = chars[chars.len() - 2];
            if !"aeiou".contains(second_last) {
                // Common exceptions that just add 's'
                let exceptions = ["photo", "piano", "halo", "memo", "pro", "auto"];
                if !exceptions.contains(&word) {
                    return format!("{word}es");
                }
            }
        }
    }

    // Default: add 's'
    format!("{word}s")
}

/// Parse all fields from a struct.
fn parse_fields(fields: &Fields) -> Result<Vec<FieldDef>> {
    match fields {
        Fields::Named(named) => named.named.iter().map(parse_field).collect(),
        Fields::Unnamed(_) => Err(Error::new(
            Span::call_site(),
            "Model requires a struct with named fields, not a tuple struct",
        )),
        Fields::Unit => Err(Error::new(
            Span::call_site(),
            "Model requires a struct with fields, not a unit struct",
        )),
    }
}

/// Parse a single field and its attributes.
fn parse_field(field: &Field) -> Result<FieldDef> {
    let name = field
        .ident
        .clone()
        .ok_or_else(|| Error::new_spanned(field, "expected named field"))?;

    let ty = field.ty.clone();

    // Check if the type is Option<T> to infer nullability
    let nullable = is_option_type(&ty);

    // Parse field attributes
    let attrs = parse_field_attrs(&field.attrs, &name, &ty)?;

    // Column name defaults to field name
    let column_name = attrs.column.unwrap_or_else(|| name.to_string());

    Ok(FieldDef {
        name,
        column_name,
        ty,
        sql_type: attrs.sql_type,
        nullable: attrs.nullable.unwrap_or(nullable),
        primary_key: attrs.primary_key,
        auto_increment: attrs.auto_increment,
        unique: attrs.unique,
        foreign_key: attrs.foreign_key,
        on_delete: attrs.on_delete,
        on_update: attrs.on_update,
        default: attrs.default,
        index: attrs.index,
        skip: attrs.skip,
        skip_insert: attrs.skip_insert,
        skip_update: attrs.skip_update,
        relationship: attrs.relationship,
        alias: attrs.alias,
        validation_alias: attrs.validation_alias,
        serialization_alias: attrs.serialization_alias,
        computed: attrs.computed,
        max_digits: attrs.max_digits,
        decimal_places: attrs.decimal_places,
        exclude: attrs.exclude,
        title: attrs.title,
        description: attrs.description,
        schema_extra: attrs.schema_extra,
        default_json: attrs.default_json,
        repr: attrs.repr.unwrap_or(true), // Default to true if not specified
        const_field: attrs.const_field,
        column_constraints: attrs.column_constraints,
        column_comment: attrs.column_comment,
        column_info: attrs.column_info,
        sa_column: attrs.sa_column,
    })
}

/// Intermediate struct for collecting field attributes.
#[derive(Default)]
struct FieldAttrs {
    column: Option<String>,
    sql_type: Option<String>,
    nullable: Option<bool>,
    primary_key: bool,
    auto_increment: bool,
    unique: bool,
    foreign_key: Option<String>,
    on_delete: Option<String>,
    on_update: Option<String>,
    default: Option<String>,
    index: Option<String>,
    skip: bool,
    skip_insert: bool,
    skip_update: bool,
    relationship: Option<RelationshipAttr>,
    alias: Option<String>,
    validation_alias: Option<String>,
    serialization_alias: Option<String>,
    computed: bool,
    /// Total number of digits for Decimal/Numeric types (precision).
    max_digits: Option<u8>,
    /// Number of digits after decimal point for Decimal/Numeric types (scale).
    decimal_places: Option<u8>,
    /// Exclude this field from serialization.
    exclude: bool,
    /// Schema title for JSON Schema generation.
    title: Option<String>,
    /// Schema description for JSON Schema generation.
    description: Option<String>,
    /// Extra JSON Schema properties (merged into schema).
    schema_extra: Option<String>,
    /// JSON representation of the field's default value (for exclude_defaults).
    default_json: Option<String>,
    /// Whether to include this field in Debug output (repr).
    /// Defaults to true (None means true).
    repr: Option<bool>,
    /// Whether this field is constant (immutable after creation).
    const_field: bool,
    /// Additional SQL constraints for DDL generation.
    column_constraints: Vec<String>,
    /// SQL comment for the column.
    column_comment: Option<String>,
    /// Extra metadata as JSON string.
    column_info: Option<String>,
    /// Complete column specification override (sa_column).
    sa_column: Option<SaColumnDef>,
}

/// Detect the relationship kind from a field's Rust type.
///
/// Returns `Some(kind)` if the type is a recognized relationship wrapper,
/// `None` otherwise.
pub fn detect_relationship_kind(ty: &Type) -> Option<RelationshipKindAttr> {
    let type_str = ty.to_token_stream().to_string();

    // Remove spaces for easier matching
    let normalized = type_str.replace(' ', "");

    if normalized.starts_with("Related<") || normalized.contains("::Related<") {
        // Related<T> is typically ManyToOne (FK on this model)
        Some(RelationshipKindAttr::ManyToOne)
    } else if normalized.starts_with("RelatedMany<") || normalized.contains("::RelatedMany<") {
        // RelatedMany<T> is OneToMany (FK on related model)
        Some(RelationshipKindAttr::OneToMany)
    } else if normalized.starts_with("Lazy<") || normalized.contains("::Lazy<") {
        // Lazy<T> defaults to ManyToOne
        Some(RelationshipKindAttr::ManyToOne)
    } else {
        None
    }
}

/// Parse all `#[sqlmodel(...)]` attributes on a field.
fn parse_field_attrs(
    attrs: &[Attribute],
    field_name: &Ident,
    field_type: &Type,
) -> Result<FieldAttrs> {
    let mut result = FieldAttrs::default();

    for attr in attrs {
        if !attr.path().is_ident("sqlmodel") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            let path = &meta.path;

            if path.is_ident("primary_key") {
                result.primary_key = true;
            } else if path.is_ident("auto_increment") {
                result.auto_increment = true;
            } else if path.is_ident("nullable") {
                result.nullable = Some(true);
            } else if path.is_ident("unique") {
                result.unique = true;
            } else if path.is_ident("skip") {
                result.skip = true;
            } else if path.is_ident("skip_insert") {
                result.skip_insert = true;
            } else if path.is_ident("skip_update") {
                result.skip_update = true;
            } else if path.is_ident("column") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    result.column = Some(lit_str.value());
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for column name",
                    ));
                }
            } else if path.is_ident("sql_type") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    result.sql_type = Some(lit_str.value());
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for sql_type",
                    ));
                }
            } else if path.is_ident("foreign_key") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    let fk = lit_str.value();
                    // Validate format: "table.column"
                    if !fk.contains('.') {
                        return Err(Error::new_spanned(
                            lit_str,
                            "foreign_key must be in format 'table.column'",
                        ));
                    }
                    result.foreign_key = Some(fk);
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for foreign_key",
                    ));
                }
            } else if path.is_ident("default") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    result.default = Some(lit_str.value());
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for default",
                    ));
                }
            } else if path.is_ident("index") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    result.index = Some(lit_str.value());
                } else {
                    return Err(Error::new_spanned(value, "expected string literal for index"));
                }
            } else if path.is_ident("on_delete") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    let action = lit_str.value().to_uppercase();
                    // Validate the referential action
                    let valid = matches!(
                        action.as_str(),
                        "NO ACTION" | "NOACTION" | "NO_ACTION" | "RESTRICT" | "CASCADE"
                            | "SET NULL" | "SETNULL" | "SET_NULL" | "SET DEFAULT"
                            | "SETDEFAULT" | "SET_DEFAULT"
                    );
                    if !valid {
                        return Err(Error::new_spanned(
                            lit_str,
                            "on_delete must be one of: NO ACTION, RESTRICT, CASCADE, SET NULL, SET DEFAULT",
                        ));
                    }
                    result.on_delete = Some(action);
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for on_delete",
                    ));
                }
            } else if path.is_ident("on_update") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    let action = lit_str.value().to_uppercase();
                    // Validate the referential action
                    let valid = matches!(
                        action.as_str(),
                        "NO ACTION" | "NOACTION" | "NO_ACTION" | "RESTRICT" | "CASCADE"
                            | "SET NULL" | "SETNULL" | "SET_NULL" | "SET DEFAULT"
                            | "SETDEFAULT" | "SET_DEFAULT"
                    );
                    if !valid {
                        return Err(Error::new_spanned(
                            lit_str,
                            "on_update must be one of: NO ACTION, RESTRICT, CASCADE, SET NULL, SET DEFAULT",
                        ));
                    }
                    result.on_update = Some(action);
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for on_update",
                    ));
                }
            } else if path.is_ident("relationship") {
                // Parse relationship(...) attribute
                let rel_attr = parse_relationship_content(&meta, field_type)?;
                result.relationship = Some(rel_attr);
            } else if path.is_ident("alias") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    result.alias = Some(lit_str.value());
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for alias",
                    ));
                }
            } else if path.is_ident("validation_alias") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    result.validation_alias = Some(lit_str.value());
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for validation_alias",
                    ));
                }
            } else if path.is_ident("serialization_alias") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    result.serialization_alias = Some(lit_str.value());
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for serialization_alias",
                    ));
                }
            } else if path.is_ident("computed") {
                result.computed = true;
            } else if path.is_ident("exclude") {
                result.exclude = true;
            } else if path.is_ident("title") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    result.title = Some(lit_str.value());
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for title",
                    ));
                }
            } else if path.is_ident("description") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    result.description = Some(lit_str.value());
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for description",
                    ));
                }
            } else if path.is_ident("schema_extra") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    result.schema_extra = Some(lit_str.value());
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for schema_extra",
                    ));
                }
            } else if path.is_ident("max_digits") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Int(lit_int) = value {
                    let digits = lit_int.base10_parse::<u8>().map_err(|_| {
                        Error::new_spanned(&lit_int, "max_digits must be a u8 (0-255)")
                    })?;
                    if digits == 0 {
                        return Err(Error::new_spanned(
                            &lit_int,
                            "max_digits must be greater than 0",
                        ));
                    }
                    result.max_digits = Some(digits);
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected integer literal for max_digits",
                    ));
                }
            } else if path.is_ident("decimal_places") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Int(lit_int) = value {
                    let places = lit_int.base10_parse::<u8>().map_err(|_| {
                        Error::new_spanned(&lit_int, "decimal_places must be a u8 (0-255)")
                    })?;
                    result.decimal_places = Some(places);
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected integer literal for decimal_places",
                    ));
                }
            } else if path.is_ident("default_json") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    result.default_json = Some(lit_str.value());
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for default_json",
                    ));
                }
            } else if path.is_ident("repr") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Bool(lit_bool) = value {
                    result.repr = Some(lit_bool.value);
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected boolean literal for repr",
                    ));
                }
            } else if path.is_ident("const_field") {
                result.const_field = true;
            } else if path.is_ident("column_constraints") {
                // Parse array of constraint strings: column_constraints = ["CHECK(...)", "CHECK(...)"]
                let _eq: syn::Token![=] = meta.input.parse()?;
                let content;
                syn::bracketed!(content in meta.input);
                while !content.is_empty() {
                    let lit: syn::LitStr = content.parse()?;
                    result.column_constraints.push(lit.value());
                    if content.is_empty() {
                        break;
                    }
                    let _: syn::Token![,] = content.parse()?;
                }
            } else if path.is_ident("column_comment") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    result.column_comment = Some(lit_str.value());
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for column_comment",
                    ));
                }
            } else if path.is_ident("column_info") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    result.column_info = Some(lit_str.value());
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for column_info",
                    ));
                }
            } else if path.is_ident("sa_column") {
                // Parse sa_column(...) attribute for full column override
                let sa_col = parse_sa_column_content(&meta)?;
                result.sa_column = Some(sa_col);
            } else {
                // Unknown attribute
                let attr_name = path.to_token_stream().to_string();
                return Err(Error::new_spanned(
                    path,
                    format!(
                        "unknown sqlmodel attribute `{attr_name}`. \
                         Valid attributes are: primary_key, auto_increment, column, nullable, \
                         unique, foreign_key, on_delete, on_update, default, sql_type, index, \
                         skip, skip_insert, skip_update, relationship, alias, validation_alias, \
                         serialization_alias, computed, max_digits, decimal_places, default_json, repr, \
                         const_field, column_constraints, column_comment, column_info, sa_column"
                    ),
                ));
            }

            Ok(())
        })?;
    }

    // Validate attribute combinations
    validate_field_attrs(&result, field_name, field_type)?;

    Ok(result)
}

/// Parse the content of a relationship(...) attribute.
fn parse_relationship_content(
    meta: &syn::meta::ParseNestedMeta<'_>,
    field_type: &Type,
) -> Result<RelationshipAttr> {
    let mut model: Option<String> = None;
    let mut foreign_key: Option<String> = None;
    let mut remote_key: Option<String> = None;
    let mut back_populates: Option<String> = None;
    let mut lazy = false;
    let mut cascade_delete = false;
    let mut passive_deletes = PassiveDeletesAttr::Active;
    let mut link_table: Option<LinkTableAttr> = None;
    let mut one_to_one = false;
    let mut many_to_many = false;
    // New sa_relationship fields
    let mut order_by: Option<String> = None;
    let mut lazy_strategy: Option<LazyLoadStrategyAttr> = None;
    let mut cascade: Option<String> = None;
    let mut uselist: Option<bool> = None;

    meta.parse_nested_meta(|nested| {
        let path = &nested.path;

        if path.is_ident("model") {
            let value: Lit = nested.value()?.parse()?;
            if let Lit::Str(lit_str) = value {
                model = Some(lit_str.value());
            } else {
                return Err(Error::new_spanned(
                    value,
                    "expected string literal for relationship model",
                ));
            }
        } else if path.is_ident("foreign_key") {
            let value: Lit = nested.value()?.parse()?;
            if let Lit::Str(lit_str) = value {
                foreign_key = Some(lit_str.value());
            } else {
                return Err(Error::new_spanned(
                    value,
                    "expected string literal for foreign_key",
                ));
            }
        } else if path.is_ident("remote_key") {
            let value: Lit = nested.value()?.parse()?;
            if let Lit::Str(lit_str) = value {
                remote_key = Some(lit_str.value());
            } else {
                return Err(Error::new_spanned(
                    value,
                    "expected string literal for remote_key",
                ));
            }
        } else if path.is_ident("back_populates") {
            let value: Lit = nested.value()?.parse()?;
            if let Lit::Str(lit_str) = value {
                back_populates = Some(lit_str.value());
            } else {
                return Err(Error::new_spanned(
                    value,
                    "expected string literal for back_populates",
                ));
            }
        } else if path.is_ident("lazy") {
            // Check if it has a value (lazy = true/false) or is just a flag
            if nested.input.peek(syn::Token![=]) {
                let value: Lit = nested.value()?.parse()?;
                if let Lit::Bool(lit_bool) = value {
                    lazy = lit_bool.value();
                } else {
                    return Err(Error::new_spanned(value, "expected boolean for lazy"));
                }
            } else {
                lazy = true;
            }
        } else if path.is_ident("cascade_delete") {
            if nested.input.peek(syn::Token![=]) {
                let value: Lit = nested.value()?.parse()?;
                if let Lit::Bool(lit_bool) = value {
                    cascade_delete = lit_bool.value();
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected boolean for cascade_delete",
                    ));
                }
            } else {
                cascade_delete = true;
            }
        } else if path.is_ident("passive_deletes") {
            // passive_deletes can be:
            // - bare: passive_deletes (equivalent to true)
            // - bool: passive_deletes = true/false
            // - string: passive_deletes = "all"
            if nested.input.peek(syn::Token![=]) {
                let value: Lit = nested.value()?.parse()?;
                match value {
                    Lit::Bool(lit_bool) => {
                        passive_deletes = if lit_bool.value() {
                            PassiveDeletesAttr::Passive
                        } else {
                            PassiveDeletesAttr::Active
                        };
                    }
                    Lit::Str(lit_str) => {
                        let s = lit_str.value();
                        passive_deletes = match s.to_lowercase().as_str() {
                            "all" => PassiveDeletesAttr::All,
                            "true" => PassiveDeletesAttr::Passive,
                            "false" => PassiveDeletesAttr::Active,
                            _ => {
                                return Err(Error::new_spanned(
                                    lit_str,
                                    "expected 'all', 'true', or 'false' for passive_deletes",
                                ));
                            }
                        };
                    }
                    _ => {
                        return Err(Error::new_spanned(
                            value,
                            "expected boolean or 'all' string for passive_deletes",
                        ));
                    }
                }
            } else {
                // Bare passive_deletes is equivalent to passive_deletes = true
                passive_deletes = PassiveDeletesAttr::Passive;
            }
        } else if path.is_ident("one_to_one") {
            one_to_one = true;
        } else if path.is_ident("many_to_many") {
            many_to_many = true;
        } else if path.is_ident("link_table") {
            // Parse link_table(table = "...", local_column = "...", remote_column = "...")
            let mut table: Option<String> = None;
            let mut local_column: Option<String> = None;
            let mut remote_column: Option<String> = None;

            nested.parse_nested_meta(|link_meta| {
                let link_path = &link_meta.path;
                if link_path.is_ident("table") {
                    let value: Lit = link_meta.value()?.parse()?;
                    if let Lit::Str(lit_str) = value {
                        table = Some(lit_str.value());
                    } else {
                        return Err(Error::new_spanned(value, "expected string for table"));
                    }
                } else if link_path.is_ident("local_column") {
                    let value: Lit = link_meta.value()?.parse()?;
                    if let Lit::Str(lit_str) = value {
                        local_column = Some(lit_str.value());
                    } else {
                        return Err(Error::new_spanned(value, "expected string for local_column"));
                    }
                } else if link_path.is_ident("remote_column") {
                    let value: Lit = link_meta.value()?.parse()?;
                    if let Lit::Str(lit_str) = value {
                        remote_column = Some(lit_str.value());
                    } else {
                        return Err(Error::new_spanned(
                            value,
                            "expected string for remote_column",
                        ));
                    }
                } else {
                    return Err(Error::new_spanned(
                        link_path,
                        "unknown link_table attribute (expected: table, local_column, remote_column)",
                    ));
                }
                Ok(())
            })?;

            if let (Some(t), Some(lc), Some(rc)) = (table, local_column, remote_column) {
                link_table = Some(LinkTableAttr {
                    table: t,
                    local_column: lc,
                    remote_column: rc,
                });
            } else {
                return Err(Error::new_spanned(
                    path,
                    "link_table requires table, local_column, and remote_column",
                ));
            }
        } else if path.is_ident("order_by") {
            let value: Lit = nested.value()?.parse()?;
            if let Lit::Str(lit_str) = value {
                order_by = Some(lit_str.value());
            } else {
                return Err(Error::new_spanned(
                    value,
                    "expected string literal for order_by",
                ));
            }
        } else if path.is_ident("lazy_strategy") {
            let value: Lit = nested.value()?.parse()?;
            if let Lit::Str(lit_str) = value {
                let strategy = match lit_str.value().to_lowercase().as_str() {
                    "select" => LazyLoadStrategyAttr::Select,
                    "joined" => LazyLoadStrategyAttr::Joined,
                    "subquery" => LazyLoadStrategyAttr::Subquery,
                    "selectin" => LazyLoadStrategyAttr::Selectin,
                    "dynamic" => LazyLoadStrategyAttr::Dynamic,
                    "noload" | "no_load" => LazyLoadStrategyAttr::NoLoad,
                    "raise_on_sql" | "raiseonsql" => LazyLoadStrategyAttr::RaiseOnSql,
                    "write_only" | "writeonly" => LazyLoadStrategyAttr::WriteOnly,
                    other => {
                        return Err(Error::new_spanned(
                            lit_str,
                            format!(
                                "unknown lazy strategy '{}'. Valid: select, joined, \
                                 subquery, selectin, dynamic, noload, raise_on_sql, write_only",
                                other
                            ),
                        ));
                    }
                };
                lazy_strategy = Some(strategy);
            } else {
                return Err(Error::new_spanned(
                    value,
                    "expected string literal for lazy_strategy",
                ));
            }
        } else if path.is_ident("cascade") {
            let value: Lit = nested.value()?.parse()?;
            if let Lit::Str(lit_str) = value {
                cascade = Some(lit_str.value());
            } else {
                return Err(Error::new_spanned(
                    value,
                    "expected string literal for cascade",
                ));
            }
        } else if path.is_ident("uselist") {
            if nested.input.peek(syn::Token![=]) {
                let value: Lit = nested.value()?.parse()?;
                if let Lit::Bool(lit_bool) = value {
                    uselist = Some(lit_bool.value());
                } else {
                    return Err(Error::new_spanned(value, "expected boolean for uselist"));
                }
            } else {
                uselist = Some(true);
            }
        } else {
            return Err(Error::new_spanned(
                path,
                "unknown relationship attribute. \
                 Valid: model, foreign_key, remote_key, back_populates, lazy, \
                 cascade_delete, passive_deletes, one_to_one, many_to_many, link_table, \
                 order_by, lazy_strategy, cascade, uselist",
            ));
        }

        Ok(())
    })?;

    // Require model attribute
    let model = model.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "relationship attribute requires 'model' parameter",
        )
    })?;

    // Infer relationship kind from field type, then override if explicit
    let mut kind = detect_relationship_kind(field_type).unwrap_or(RelationshipKindAttr::ManyToOne);

    // Override based on explicit flags
    if one_to_one {
        kind = RelationshipKindAttr::OneToOne;
    } else if many_to_many || link_table.is_some() {
        kind = RelationshipKindAttr::ManyToMany;
    }

    Ok(RelationshipAttr {
        model,
        foreign_key,
        remote_key,
        link_table,
        back_populates,
        lazy,
        cascade_delete,
        passive_deletes,
        kind,
        order_by,
        lazy_strategy,
        cascade,
        uselist,
    })
}

/// Parse the content of an sa_column(...) attribute.
///
/// This provides full column specification override, similar to SQLAlchemy's Column().
/// When used, other column-related attributes should be forbidden.
fn parse_sa_column_content(meta: &syn::meta::ParseNestedMeta<'_>) -> Result<SaColumnDef> {
    let mut result = SaColumnDef::default();

    meta.parse_nested_meta(|nested| {
        let path = nested.path.clone();

        if path.is_ident("sql_type") {
            let value: Lit = nested.value()?.parse()?;
            if let Lit::Str(lit_str) = value {
                result.sql_type = Some(lit_str.value());
            } else {
                return Err(Error::new_spanned(
                    value,
                    "expected string literal for sql_type",
                ));
            }
        } else if path.is_ident("check") {
            let value: Lit = nested.value()?.parse()?;
            if let Lit::Str(lit_str) = value {
                result.check.push(lit_str.value());
            } else {
                return Err(Error::new_spanned(
                    value,
                    "expected string literal for check",
                ));
            }
        } else if path.is_ident("server_default") {
            let value: Lit = nested.value()?.parse()?;
            if let Lit::Str(lit_str) = value {
                result.server_default = Some(lit_str.value());
            } else {
                return Err(Error::new_spanned(
                    value,
                    "expected string literal for server_default",
                ));
            }
        } else if path.is_ident("comment") {
            let value: Lit = nested.value()?.parse()?;
            if let Lit::Str(lit_str) = value {
                result.comment = Some(lit_str.value());
            } else {
                return Err(Error::new_spanned(
                    value,
                    "expected string literal for comment",
                ));
            }
        } else if path.is_ident("nullable") {
            // Check if it's a bare flag or has a value
            if nested.input.peek(syn::Token![=]) {
                let value: Lit = nested.value()?.parse()?;
                if let Lit::Bool(lit_bool) = value {
                    result.nullable = Some(lit_bool.value());
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected boolean literal for nullable",
                    ));
                }
            } else {
                result.nullable = Some(true);
            }
        } else if path.is_ident("unique") {
            // Check if it's a bare flag or has a value
            if nested.input.peek(syn::Token![=]) {
                let value: Lit = nested.value()?.parse()?;
                if let Lit::Bool(lit_bool) = value {
                    result.unique = Some(lit_bool.value());
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected boolean literal for unique",
                    ));
                }
            } else {
                result.unique = Some(true);
            }
        } else if path.is_ident("index") {
            // Check if it's a bare flag or has a value
            if nested.input.peek(syn::Token![=]) {
                let value: Lit = nested.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    result.index = Some(lit_str.value());
                } else if let Lit::Bool(lit_bool) = value {
                    if lit_bool.value() {
                        // true means create an index (name auto-generated)
                        result.index = Some(String::new());
                    }
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string or boolean literal for index",
                    ));
                }
            } else {
                // Bare flag means create an index
                result.index = Some(String::new());
            }
        } else {
            return Err(Error::new_spanned(
                path,
                "unknown sa_column attribute. \
                 Valid: sql_type, check, server_default, comment, nullable, unique, index",
            ));
        }

        Ok(())
    })?;

    Ok(result)
}

/// Validate that attribute combinations make sense.
fn validate_field_attrs(attrs: &FieldAttrs, field_name: &Ident, field_type: &Type) -> Result<()> {
    // Cannot use skip with primary_key
    if attrs.skip && attrs.primary_key {
        return Err(Error::new_spanned(
            field_name,
            "cannot use both `skip` and `primary_key` on the same field",
        ));
    }

    // Cannot use skip with skip_insert or skip_update (redundant)
    if attrs.skip && (attrs.skip_insert || attrs.skip_update) {
        return Err(Error::new_spanned(
            field_name,
            "`skip` already excludes the field from all operations; \
             `skip_insert` and `skip_update` are redundant",
        ));
    }

    // Validate relationship attribute is on a relationship type
    if attrs.relationship.is_some() {
        let detected = detect_relationship_kind(field_type);
        if detected.is_none() {
            return Err(Error::new_spanned(
                field_name,
                "relationship attribute can only be used on Related<T>, RelatedMany<T>, or Lazy<T> fields",
            ));
        }
    }

    // auto_increment usually implies primary_key (warn, don't error)
    // We allow it for flexibility, but the generate phase may warn

    // Validate decimal precision constraints
    if let (Some(max_digits), Some(decimal_places)) = (attrs.max_digits, attrs.decimal_places) {
        if decimal_places > max_digits {
            return Err(Error::new_spanned(
                field_name,
                format!(
                    "decimal_places ({}) cannot be greater than max_digits ({})",
                    decimal_places, max_digits
                ),
            ));
        }
    }

    // Warn if max_digits/decimal_places used without a Decimal type
    // (We just validate syntax here; type checking is done elsewhere if needed)

    // Validate sa_column mutual exclusivity
    // When sa_column is used, certain other column-related attributes are forbidden
    if attrs.sa_column.is_some() {
        let mut conflicts = Vec::new();

        if attrs.sql_type.is_some() {
            conflicts.push("sql_type");
        }
        if attrs.nullable.is_some() {
            conflicts.push("nullable");
        }
        if attrs.unique {
            conflicts.push("unique");
        }
        if attrs.index.is_some() {
            conflicts.push("index");
        }
        if attrs.foreign_key.is_some() {
            conflicts.push("foreign_key");
        }
        if attrs.on_delete.is_some() {
            conflicts.push("on_delete");
        }
        if attrs.on_update.is_some() {
            conflicts.push("on_update");
        }
        if attrs.default.is_some() {
            conflicts.push("default");
        }
        if !attrs.column_constraints.is_empty() {
            conflicts.push("column_constraints");
        }
        if attrs.column_comment.is_some() {
            conflicts.push("column_comment");
        }

        if !conflicts.is_empty() {
            return Err(Error::new_spanned(
                field_name,
                format!(
                    "`sa_column` provides full column override and cannot be combined with: {}. \
                     Use sa_column sub-attributes instead (e.g., sa_column(sql_type = \"...\"))",
                    conflicts.join(", ")
                ),
            ));
        }
    }

    Ok(())
}

/// Check if a type is `Option<T>`.
pub fn is_option_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            return segment.ident == "Option";
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("Hero"), "hero");
        assert_eq!(to_snake_case("TeamMember"), "team_member");
        assert_eq!(to_snake_case("HTTPServer"), "http_server");
        assert_eq!(to_snake_case("UserID"), "user_id");
        assert_eq!(to_snake_case("XMLParser"), "xml_parser");
        assert_eq!(to_snake_case("IOError"), "io_error");
    }

    #[test]
    fn test_pluralize() {
        // Regular words
        assert_eq!(pluralize("hero"), "heroes");
        assert_eq!(pluralize("user"), "users");
        assert_eq!(pluralize("team"), "teams");

        // Words ending in s, x, z, ch, sh
        assert_eq!(pluralize("bus"), "buses");
        assert_eq!(pluralize("box"), "boxes");
        assert_eq!(pluralize("quiz"), "quizzes");
        assert_eq!(pluralize("match"), "matches");
        assert_eq!(pluralize("dish"), "dishes");

        // Words ending in y
        assert_eq!(pluralize("category"), "categories");
        assert_eq!(pluralize("baby"), "babies");
        assert_eq!(pluralize("key"), "keys");
        assert_eq!(pluralize("day"), "days");

        // Words ending in f/fe
        assert_eq!(pluralize("leaf"), "leaves");
        assert_eq!(pluralize("wife"), "wives");
        assert_eq!(pluralize("knife"), "knives");

        // Words ending in o
        assert_eq!(pluralize("hero"), "heroes");
        assert_eq!(pluralize("potato"), "potatoes");
        assert_eq!(pluralize("photo"), "photos");
        assert_eq!(pluralize("piano"), "pianos");

        // Special cases
        assert_eq!(pluralize("person"), "people");
        assert_eq!(pluralize("child"), "children");
        assert_eq!(pluralize("mouse"), "mice");
        assert_eq!(pluralize("datum"), "data");
    }

    #[test]
    fn test_derive_table_name() {
        assert_eq!(derive_table_name("Hero"), "heroes");
        assert_eq!(derive_table_name("TeamMember"), "team_members");
        assert_eq!(derive_table_name("Person"), "people");
        assert_eq!(derive_table_name("Category"), "categories");
        assert_eq!(derive_table_name("User"), "users");
    }

    #[test]
    fn test_parse_model_table_override() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(table = "events")]
            struct Event {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        assert_eq!(def.table_name, "events");
        assert_eq!(def.table_alias, None);
    }

    #[test]
    fn test_parse_model_table_and_alias_same_attr() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(table = "events", table_alias = "e")]
            struct Event {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        assert_eq!(def.table_name, "events");
        assert_eq!(def.table_alias.as_deref(), Some("e"));
    }

    #[test]
    fn test_parse_model_unknown_struct_attr_errors() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(not_a_real_key = "x")]
            struct Event {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
            }
        };

        let err = parse_model(&input).unwrap_err();
        assert!(
            err.to_string()
                .contains("unknown sqlmodel struct attribute"),
            "{err}"
        );
    }

    // ========================================================================
    // Relationship attribute parsing tests
    // ========================================================================

    #[test]
    fn test_parse_simple_relationship() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
                #[sqlmodel(relationship(model = "teams"))]
                team: Related<Team>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel_fields = def.relationship_fields();
        assert_eq!(rel_fields.len(), 1);

        let rel = rel_fields[0].relationship.as_ref().unwrap();
        assert_eq!(rel.model, "teams");
        assert_eq!(rel.foreign_key, None);
        assert_eq!(rel.remote_key, None);
        assert_eq!(rel.back_populates, None);
        assert!(!rel.lazy);
        assert!(!rel.cascade_delete);
        assert_eq!(rel.kind, RelationshipKindAttr::ManyToOne);
    }

    #[test]
    fn test_parse_relationship_with_foreign_key() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                team_id: i64,
                #[sqlmodel(relationship(model = "teams", foreign_key = "team_id"))]
                team: Related<Team>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel_fields = def.relationship_fields();
        assert_eq!(rel_fields.len(), 1);

        let rel = rel_fields[0].relationship.as_ref().unwrap();
        assert_eq!(rel.model, "teams");
        assert_eq!(rel.foreign_key, Some("team_id".to_string()));
    }

    #[test]
    fn test_parse_relationship_with_remote_key() {
        let input: DeriveInput = parse_quote! {
            struct Team {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
                #[sqlmodel(relationship(model = "heroes", remote_key = "team_id"))]
                members: RelatedMany<Hero>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel_fields = def.relationship_fields();
        assert_eq!(rel_fields.len(), 1);

        let rel = rel_fields[0].relationship.as_ref().unwrap();
        assert_eq!(rel.model, "heroes");
        assert_eq!(rel.remote_key, Some("team_id".to_string()));
        assert_eq!(rel.kind, RelationshipKindAttr::OneToMany);
    }

    #[test]
    fn test_parse_relationship_with_back_populates() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "teams", back_populates = "members"))]
                team: Related<Team>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel = def.relationship_fields()[0].relationship.as_ref().unwrap();
        assert_eq!(rel.back_populates, Some("members".to_string()));
    }

    #[test]
    fn test_parse_relationship_with_lazy_flag() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "teams", lazy))]
                team: Related<Team>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel = def.relationship_fields()[0].relationship.as_ref().unwrap();
        assert!(rel.lazy);
    }

    #[test]
    fn test_parse_relationship_with_lazy_explicit_value() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "teams", lazy = true))]
                team: Related<Team>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel = def.relationship_fields()[0].relationship.as_ref().unwrap();
        assert!(rel.lazy);

        // Test with false
        let input2: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "teams", lazy = false))]
                team: Related<Team>,
            }
        };

        let def2 = parse_model(&input2).unwrap();
        let rel2 = def2.relationship_fields()[0].relationship.as_ref().unwrap();
        assert!(!rel2.lazy);
    }

    #[test]
    fn test_parse_relationship_with_cascade_delete() {
        let input: DeriveInput = parse_quote! {
            struct Team {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "heroes", cascade_delete))]
                members: RelatedMany<Hero>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel = def.relationship_fields()[0].relationship.as_ref().unwrap();
        assert!(rel.cascade_delete);
    }

    #[test]
    fn test_parse_relationship_with_passive_deletes_bare() {
        // Bare passive_deletes (no value) is equivalent to true
        let input: DeriveInput = parse_quote! {
            struct Parent {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "children", passive_deletes))]
                children: RelatedMany<Child>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel = def.relationship_fields()[0].relationship.as_ref().unwrap();
        assert_eq!(rel.passive_deletes, PassiveDeletesAttr::Passive);
    }

    #[test]
    fn test_parse_relationship_with_passive_deletes_true() {
        let input: DeriveInput = parse_quote! {
            struct Parent {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "children", passive_deletes = true))]
                children: RelatedMany<Child>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel = def.relationship_fields()[0].relationship.as_ref().unwrap();
        assert_eq!(rel.passive_deletes, PassiveDeletesAttr::Passive);
    }

    #[test]
    fn test_parse_relationship_with_passive_deletes_false() {
        let input: DeriveInput = parse_quote! {
            struct Parent {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "children", passive_deletes = false))]
                children: RelatedMany<Child>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel = def.relationship_fields()[0].relationship.as_ref().unwrap();
        assert_eq!(rel.passive_deletes, PassiveDeletesAttr::Active);
    }

    #[test]
    fn test_parse_relationship_with_passive_deletes_all() {
        let input: DeriveInput = parse_quote! {
            struct Parent {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "children", passive_deletes = "all"))]
                children: RelatedMany<Child>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel = def.relationship_fields()[0].relationship.as_ref().unwrap();
        assert_eq!(rel.passive_deletes, PassiveDeletesAttr::All);
    }

    #[test]
    fn test_parse_relationship_passive_deletes_default() {
        // Default should be Active (ORM handles deletes)
        let input: DeriveInput = parse_quote! {
            struct Team {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "heroes"))]
                members: RelatedMany<Hero>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel = def.relationship_fields()[0].relationship.as_ref().unwrap();
        assert_eq!(rel.passive_deletes, PassiveDeletesAttr::Active);
    }

    #[test]
    fn test_parse_relationship_with_passive_deletes_invalid_string() {
        let input: DeriveInput = parse_quote! {
            struct Parent {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "children", passive_deletes = "invalid"))]
                children: RelatedMany<Child>,
            }
        };

        let result = parse_model(&input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("expected 'all', 'true', or 'false'"));
    }

    #[test]
    fn test_parse_relationship_with_passive_deletes_integer_fails() {
        let input: DeriveInput = parse_quote! {
            struct Parent {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "children", passive_deletes = 1))]
                children: RelatedMany<Child>,
            }
        };

        let result = parse_model(&input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("expected boolean or 'all' string"));
    }

    #[test]
    fn test_parse_relationship_with_link_table() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(
                    model = "powers",
                    link_table(
                        table = "hero_powers",
                        local_column = "hero_id",
                        remote_column = "power_id"
                    )
                ))]
                powers: RelatedMany<Power>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel = def.relationship_fields()[0].relationship.as_ref().unwrap();
        assert_eq!(rel.model, "powers");
        assert_eq!(rel.kind, RelationshipKindAttr::ManyToMany);

        let link = rel.link_table.as_ref().unwrap();
        assert_eq!(link.table, "hero_powers");
        assert_eq!(link.local_column, "hero_id");
        assert_eq!(link.remote_column, "power_id");
    }

    #[test]
    fn test_parse_relationship_one_to_one_explicit() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "profiles", one_to_one))]
                profile: Related<Profile>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel = def.relationship_fields()[0].relationship.as_ref().unwrap();
        assert_eq!(rel.kind, RelationshipKindAttr::OneToOne);
    }

    #[test]
    fn test_parse_relationship_many_to_many_explicit() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "powers", many_to_many))]
                powers: RelatedMany<Power>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel = def.relationship_fields()[0].relationship.as_ref().unwrap();
        assert_eq!(rel.kind, RelationshipKindAttr::ManyToMany);
    }

    // ========================================================================
    // Type detection tests
    // ========================================================================

    #[test]
    fn test_detect_related_type() {
        let ty: Type = parse_quote!(Related<Team>);
        let kind = detect_relationship_kind(&ty);
        assert_eq!(kind, Some(RelationshipKindAttr::ManyToOne));
    }

    #[test]
    fn test_detect_related_many_type() {
        let ty: Type = parse_quote!(RelatedMany<Hero>);
        let kind = detect_relationship_kind(&ty);
        assert_eq!(kind, Some(RelationshipKindAttr::OneToMany));
    }

    #[test]
    fn test_detect_lazy_type() {
        let ty: Type = parse_quote!(Lazy<Team>);
        let kind = detect_relationship_kind(&ty);
        assert_eq!(kind, Some(RelationshipKindAttr::ManyToOne));
    }

    #[test]
    fn test_detect_qualified_related_type() {
        let ty: Type = parse_quote!(sqlmodel_core::Related<Team>);
        let kind = detect_relationship_kind(&ty);
        assert_eq!(kind, Some(RelationshipKindAttr::ManyToOne));

        let ty2: Type = parse_quote!(crate::RelatedMany<Hero>);
        let kind2 = detect_relationship_kind(&ty2);
        assert_eq!(kind2, Some(RelationshipKindAttr::OneToMany));
    }

    #[test]
    fn test_detect_non_relationship_type() {
        let ty: Type = parse_quote!(String);
        assert_eq!(detect_relationship_kind(&ty), None);

        let ty2: Type = parse_quote!(i64);
        assert_eq!(detect_relationship_kind(&ty2), None);

        let ty3: Type = parse_quote!(Option<String>);
        assert_eq!(detect_relationship_kind(&ty3), None);

        let ty4: Type = parse_quote!(Vec<Hero>);
        assert_eq!(detect_relationship_kind(&ty4), None);
    }

    // ========================================================================
    // Validation error tests
    // ========================================================================

    #[test]
    fn test_error_relationship_missing_model() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(foreign_key = "team_id"))]
                team: Related<Team>,
            }
        };

        let err = parse_model(&input).unwrap_err();
        assert!(
            err.to_string().contains("requires 'model' parameter"),
            "Expected model required error, got: {err}"
        );
    }

    #[test]
    fn test_error_relationship_on_non_relationship_type() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "teams"))]
                team_id: i64,
            }
        };

        let err = parse_model(&input).unwrap_err();
        assert!(
            err.to_string()
                .contains("can only be used on Related<T>, RelatedMany<T>, or Lazy<T>"),
            "Expected invalid field type error, got: {err}"
        );
    }

    #[test]
    fn test_error_relationship_unknown_attribute() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(model = "teams", unknown_key = "value"))]
                team: Related<Team>,
            }
        };

        let err = parse_model(&input).unwrap_err();
        assert!(
            err.to_string().contains("unknown relationship attribute"),
            "Expected unknown attribute error, got: {err}"
        );
    }

    #[test]
    fn test_error_link_table_incomplete() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(
                    model = "powers",
                    link_table(table = "hero_powers")
                ))]
                powers: RelatedMany<Power>,
            }
        };

        let err = parse_model(&input).unwrap_err();
        assert!(
            err.to_string()
                .contains("requires table, local_column, and remote_column"),
            "Expected incomplete link_table error, got: {err}"
        );
    }

    // ========================================================================
    // Relationship field filtering tests
    // ========================================================================

    #[test]
    fn test_relationship_fields_returns_only_relationships() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
                team_id: i64,
                #[sqlmodel(relationship(model = "teams"))]
                team: Related<Team>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel_fields = def.relationship_fields();
        assert_eq!(rel_fields.len(), 1);
        assert_eq!(rel_fields[0].name.to_string(), "team");
    }

    #[test]
    fn test_select_fields_excludes_relationships() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
                team_id: i64,
                #[sqlmodel(relationship(model = "teams"))]
                team: Related<Team>,
            }
        };

        let def = parse_model(&input).unwrap();
        let select = def.select_fields();

        // Should include id, name, team_id but not team
        assert_eq!(select.len(), 3);
        let names: Vec<_> = select.iter().map(|f| f.name.to_string()).collect();
        assert!(names.contains(&"id".to_string()));
        assert!(names.contains(&"name".to_string()));
        assert!(names.contains(&"team_id".to_string()));
        assert!(!names.contains(&"team".to_string()));
    }

    #[test]
    fn test_insert_fields_excludes_relationships() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
                #[sqlmodel(relationship(model = "teams"))]
                team: Related<Team>,
            }
        };

        let def = parse_model(&input).unwrap();
        let insert = def.insert_fields();

        let names: Vec<_> = insert.iter().map(|f| f.name.to_string()).collect();
        assert!(!names.contains(&"team".to_string()));
    }

    #[test]
    fn test_update_fields_excludes_relationships() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
                #[sqlmodel(relationship(model = "teams"))]
                team: Related<Team>,
            }
        };

        let def = parse_model(&input).unwrap();
        let update = def.update_fields();

        let names: Vec<_> = update.iter().map(|f| f.name.to_string()).collect();
        assert!(!names.contains(&"team".to_string()));
    }

    #[test]
    fn test_multiple_relationships() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
                #[sqlmodel(relationship(model = "teams", foreign_key = "team_id"))]
                team: Related<Team>,
                #[sqlmodel(relationship(model = "powers", remote_key = "hero_id"))]
                powers: RelatedMany<Power>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel_fields = def.relationship_fields();
        assert_eq!(rel_fields.len(), 2);

        // Verify select fields count
        let select = def.select_fields();
        assert_eq!(select.len(), 2); // id, name only
    }

    #[test]
    fn test_relationship_with_all_options() {
        let input: DeriveInput = parse_quote! {
            struct Hero {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(
                    model = "teams",
                    foreign_key = "team_id",
                    back_populates = "members",
                    lazy,
                    cascade_delete
                ))]
                team: Related<Team>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel = def.relationship_fields()[0].relationship.as_ref().unwrap();

        assert_eq!(rel.model, "teams");
        assert_eq!(rel.foreign_key, Some("team_id".to_string()));
        assert_eq!(rel.back_populates, Some("members".to_string()));
        assert!(rel.lazy);
        assert!(rel.cascade_delete);
        assert_eq!(rel.kind, RelationshipKindAttr::ManyToOne);
    }

    // ========================================================================
    // Field alias attribute tests
    // ========================================================================

    #[test]
    fn test_parse_field_alias() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(alias = "userName")]
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert_eq!(name_field.alias, Some("userName".to_string()));
        assert!(name_field.validation_alias.is_none());
        assert!(name_field.serialization_alias.is_none());
    }

    #[test]
    fn test_parse_field_validation_alias() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(validation_alias = "user_name")]
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert!(name_field.alias.is_none());
        assert_eq!(name_field.validation_alias, Some("user_name".to_string()));
        assert!(name_field.serialization_alias.is_none());
    }

    #[test]
    fn test_parse_field_serialization_alias() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(serialization_alias = "user-name")]
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert!(name_field.alias.is_none());
        assert!(name_field.validation_alias.is_none());
        assert_eq!(
            name_field.serialization_alias,
            Some("user-name".to_string())
        );
    }

    #[test]
    fn test_parse_field_all_aliases() {
        // Test that all three aliases can be specified together
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(alias = "nm", validation_alias = "input_name", serialization_alias = "outputName")]
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert_eq!(name_field.alias, Some("nm".to_string()));
        assert_eq!(name_field.validation_alias, Some("input_name".to_string()));
        assert_eq!(
            name_field.serialization_alias,
            Some("outputName".to_string())
        );
    }

    #[test]
    fn test_field_def_output_name() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(serialization_alias = "userName")]
                name: String,
                email: String,
            }
        };

        let def = parse_model(&input).unwrap();

        // Field with serialization_alias should use it for output
        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert_eq!(name_field.output_name(), "userName");

        // Field without any alias should use field name
        let email_field = def.fields.iter().find(|f| f.name == "email").unwrap();
        assert_eq!(email_field.output_name(), "email");
    }

    #[test]
    fn test_field_def_output_name_alias_fallback() {
        // When only alias is set, it should be used for output
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(alias = "nm")]
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert_eq!(name_field.output_name(), "nm");
    }

    #[test]
    fn test_field_def_input_names() {
        // No aliases - only field name
        let input1: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
            }
        };
        let def1 = parse_model(&input1).unwrap();
        let field1 = def1.fields.iter().find(|f| f.name == "name").unwrap();
        assert_eq!(field1.input_names(), vec!["name"]);

        // With validation_alias - accepts both
        let input2: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(validation_alias = "user_name")]
                name: String,
            }
        };
        let def2 = parse_model(&input2).unwrap();
        let field2 = def2.fields.iter().find(|f| f.name == "name").unwrap();
        let names2 = field2.input_names();
        assert!(names2.contains(&"name"));
        assert!(names2.contains(&"user_name"));

        // With alias - accepts both
        let input3: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(alias = "nm")]
                name: String,
            }
        };
        let def3 = parse_model(&input3).unwrap();
        let field3 = def3.fields.iter().find(|f| f.name == "name").unwrap();
        let names3 = field3.input_names();
        assert!(names3.contains(&"name"));
        assert!(names3.contains(&"nm"));
    }

    #[test]
    fn test_field_def_has_alias() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(alias = "nm")]
                name: String,
                email: String,
            }
        };

        let def = parse_model(&input).unwrap();

        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert!(name_field.has_alias());

        let email_field = def.fields.iter().find(|f| f.name == "email").unwrap();
        assert!(!email_field.has_alias());
    }

    #[test]
    fn test_parse_alias_with_special_characters() {
        // Aliases can contain hyphens, underscores, etc.
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(alias = "user-name_v2")]
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert_eq!(name_field.alias, Some("user-name_v2".to_string()));
    }

    // =========================================================================
    // Field Schema Metadata Tests (title, description, schema_extra)
    // =========================================================================

    #[test]
    fn test_parse_field_title() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(title = "User Name")]
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert_eq!(name_field.title, Some("User Name".to_string()));
    }

    #[test]
    fn test_parse_field_description() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(description = "The full name of the user")]
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert_eq!(
            name_field.description,
            Some("The full name of the user".to_string())
        );
    }

    #[test]
    fn test_parse_field_schema_extra() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(schema_extra = r#"{"examples": ["John Doe"]}"#)]
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert_eq!(
            name_field.schema_extra,
            Some(r#"{"examples": ["John Doe"]}"#.to_string())
        );
    }

    #[test]
    fn test_parse_all_schema_metadata_combined() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(
                    title = "User Name",
                    description = "The full name of the user",
                    schema_extra = r#"{"examples": ["John Doe"], "minLength": 1}"#
                )]
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert_eq!(name_field.title, Some("User Name".to_string()));
        assert_eq!(
            name_field.description,
            Some("The full name of the user".to_string())
        );
        assert_eq!(
            name_field.schema_extra,
            Some(r#"{"examples": ["John Doe"], "minLength": 1}"#.to_string())
        );
    }

    // =========================================================================
    // Field Exclude Tests
    // =========================================================================

    #[test]
    fn test_parse_exclude_attribute() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(exclude)]
                password_hash: String,
            }
        };

        let def = parse_model(&input).unwrap();
        let password_field = def
            .fields
            .iter()
            .find(|f| f.name == "password_hash")
            .unwrap();
        assert!(password_field.exclude);

        let id_field = def.fields.iter().find(|f| f.name == "id").unwrap();
        assert!(!id_field.exclude);
    }

    #[test]
    fn test_exclude_combined_with_other_attrs() {
        // exclude can be combined with other attributes
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(exclude, alias = "pwd")]
                password: String,
                #[sqlmodel(exclude, column = "internal_notes")]
                notes: String,
            }
        };

        let def = parse_model(&input).unwrap();

        let password_field = def.fields.iter().find(|f| f.name == "password").unwrap();
        assert!(password_field.exclude);
        assert_eq!(password_field.alias, Some("pwd".to_string()));

        let notes_field = def.fields.iter().find(|f| f.name == "notes").unwrap();
        assert!(notes_field.exclude);
        assert_eq!(notes_field.column_name, "internal_notes");
    }

    // =========================================================================
    // Decimal Precision Tests (max_digits, decimal_places)
    // =========================================================================

    #[test]
    fn test_parse_max_digits() {
        let input: DeriveInput = parse_quote! {
            struct Product {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(max_digits = 10)]
                price: f64,
            }
        };

        let def = parse_model(&input).unwrap();
        let price_field = def.fields.iter().find(|f| f.name == "price").unwrap();
        assert_eq!(price_field.max_digits, Some(10));
        assert_eq!(price_field.decimal_places, None);
    }

    #[test]
    fn test_parse_decimal_places() {
        let input: DeriveInput = parse_quote! {
            struct Product {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(decimal_places = 2)]
                price: f64,
            }
        };

        let def = parse_model(&input).unwrap();
        let price_field = def.fields.iter().find(|f| f.name == "price").unwrap();
        assert_eq!(price_field.max_digits, None);
        assert_eq!(price_field.decimal_places, Some(2));
    }

    #[test]
    fn test_parse_max_digits_and_decimal_places() {
        let input: DeriveInput = parse_quote! {
            struct Product {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(max_digits = 10, decimal_places = 2)]
                price: f64,
            }
        };

        let def = parse_model(&input).unwrap();
        let price_field = def.fields.iter().find(|f| f.name == "price").unwrap();
        assert_eq!(price_field.max_digits, Some(10));
        assert_eq!(price_field.decimal_places, Some(2));
    }

    #[test]
    fn test_decimal_places_exceeds_max_digits_errors() {
        // decimal_places > max_digits should fail
        let input: DeriveInput = parse_quote! {
            struct Product {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(max_digits = 5, decimal_places = 10)]
                price: f64,
            }
        };

        let result = parse_model(&input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("decimal_places") && err.contains("max_digits"));
    }

    #[test]
    fn test_max_digits_zero_errors() {
        // max_digits = 0 should fail
        let input: DeriveInput = parse_quote! {
            struct Product {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(max_digits = 0)]
                price: f64,
            }
        };

        let result = parse_model(&input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("max_digits"));
    }

    #[test]
    fn test_data_fields_includes_computed() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
                #[sqlmodel(computed)]
                full_name: String,
            }
        };

        let def = parse_model(&input).unwrap();

        // select_fields should exclude computed
        assert_eq!(def.select_fields().len(), 2);
        assert!(def.select_fields().iter().all(|f| !f.computed));

        // data_fields should include computed
        assert_eq!(def.data_fields().len(), 3);
        assert!(def.data_fields().iter().any(|f| f.computed));
    }

    // ==================== Model Config Tests ====================

    #[test]
    fn test_model_config_defaults() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        assert!(!def.config.table);
        assert!(!def.config.from_attributes);
        assert!(!def.config.validate_assignment);
        assert_eq!(def.config.extra, "");
        assert!(!def.config.strict);
        assert!(!def.config.populate_by_name);
        assert!(!def.config.use_enum_values);
    }

    #[test]
    fn test_model_config_table_flag() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(table)]
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        assert!(def.config.table);
    }

    #[test]
    fn test_model_config_table_with_name() {
        // table = "custom_name" should set table name, not config.table
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(table = "custom_users")]
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        assert_eq!(def.table_name, "custom_users");
        // config.table remains false because only the flag form sets it
        assert!(!def.config.table);
    }

    #[test]
    fn test_model_config_from_attributes() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(from_attributes)]
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        assert!(def.config.from_attributes);
    }

    #[test]
    fn test_model_config_validate_assignment() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(validate_assignment)]
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        assert!(def.config.validate_assignment);
    }

    #[test]
    fn test_model_config_extra_forbid() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(extra = "forbid")]
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        assert_eq!(def.config.extra, "forbid");
    }

    #[test]
    fn test_model_config_extra_allow() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(extra = "allow")]
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
            }
        };

        let def = parse_model(&input).unwrap();
        assert_eq!(def.config.extra, "allow");
    }

    #[test]
    fn test_model_config_extra_invalid() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(extra = "invalid")]
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
            }
        };

        let result = parse_model(&input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("extra"));
    }

    #[test]
    fn test_model_config_strict() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(strict)]
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
            }
        };

        let def = parse_model(&input).unwrap();
        assert!(def.config.strict);
    }

    #[test]
    fn test_model_config_populate_by_name() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(populate_by_name)]
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
            }
        };

        let def = parse_model(&input).unwrap();
        assert!(def.config.populate_by_name);
    }

    #[test]
    fn test_model_config_use_enum_values() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(use_enum_values)]
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
            }
        };

        let def = parse_model(&input).unwrap();
        assert!(def.config.use_enum_values);
    }

    #[test]
    fn test_model_config_multiple_options() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(table, from_attributes, validate_assignment, extra = "forbid", strict)]
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();
        assert!(def.config.table);
        assert!(def.config.from_attributes);
        assert!(def.config.validate_assignment);
        assert_eq!(def.config.extra, "forbid");
        assert!(def.config.strict);
    }

    #[test]
    fn test_model_config_title() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(title = "User Model")]
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
            }
        };

        let def = parse_model(&input).unwrap();
        assert_eq!(def.config.title, Some("User Model".to_string()));
    }

    #[test]
    fn test_model_config_json_schema_extra() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(json_schema_extra = "{\"key\": \"value\"}")]
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
            }
        };

        let def = parse_model(&input).unwrap();
        assert_eq!(
            def.config.json_schema_extra,
            Some("{\"key\": \"value\"}".to_string())
        );
    }

    #[test]
    fn test_model_config_arbitrary_types_allowed() {
        let input: DeriveInput = parse_quote! {
            #[sqlmodel(arbitrary_types_allowed)]
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
            }
        };

        let def = parse_model(&input).unwrap();
        assert!(def.config.arbitrary_types_allowed);
    }

    // =========================================================================
    // Default JSON Tests (for exclude_defaults support)
    // =========================================================================

    #[test]
    fn test_parse_default_json() {
        let input: DeriveInput = parse_quote! {
            struct Config {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(default_json = "0")]
                count: i32,
                #[sqlmodel(default_json = "false")]
                active: bool,
                #[sqlmodel(default_json = r#""default""#)]
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();

        let count_field = def.fields.iter().find(|f| f.name == "count").unwrap();
        assert_eq!(count_field.default_json, Some("0".to_string()));

        let active_field = def.fields.iter().find(|f| f.name == "active").unwrap();
        assert_eq!(active_field.default_json, Some("false".to_string()));

        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert_eq!(name_field.default_json, Some(r#""default""#.to_string()));

        // Field without default_json should have None
        let id_field = def.fields.iter().find(|f| f.name == "id").unwrap();
        assert_eq!(id_field.default_json, None);
    }

    #[test]
    fn test_default_json_combined_with_other_attrs() {
        let input: DeriveInput = parse_quote! {
            struct Settings {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(default_json = "0", alias = "itemCount")]
                count: i32,
                #[sqlmodel(default_json = "[]", nullable)]
                items: Option<Vec<i32>>,
            }
        };

        let def = parse_model(&input).unwrap();

        let count_field = def.fields.iter().find(|f| f.name == "count").unwrap();
        assert_eq!(count_field.default_json, Some("0".to_string()));
        assert_eq!(count_field.alias, Some("itemCount".to_string()));

        let items_field = def.fields.iter().find(|f| f.name == "items").unwrap();
        assert_eq!(items_field.default_json, Some("[]".to_string()));
        assert!(items_field.nullable);
    }

    // =========================================================================
    // Repr Field Control Tests
    // =========================================================================

    #[test]
    fn test_parse_repr_default_true() {
        // Fields without repr attribute should default to repr=true
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();

        let id_field = def.fields.iter().find(|f| f.name == "id").unwrap();
        assert!(id_field.repr); // Default should be true

        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert!(name_field.repr); // Default should be true
    }

    #[test]
    fn test_parse_repr_false() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                name: String,
                #[sqlmodel(repr = false)]
                password: String,
            }
        };

        let def = parse_model(&input).unwrap();

        let id_field = def.fields.iter().find(|f| f.name == "id").unwrap();
        assert!(id_field.repr); // Default true

        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert!(name_field.repr); // Default true

        let password_field = def.fields.iter().find(|f| f.name == "password").unwrap();
        assert!(!password_field.repr); // Explicitly set to false
    }

    #[test]
    fn test_parse_repr_true_explicit() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(repr = true)]
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();

        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert!(name_field.repr); // Explicitly set to true
    }

    #[test]
    fn test_repr_combined_with_other_attrs() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(repr = false, alias = "pwd", exclude)]
                password: String,
            }
        };

        let def = parse_model(&input).unwrap();

        let password_field = def.fields.iter().find(|f| f.name == "password").unwrap();
        assert!(!password_field.repr);
        assert_eq!(password_field.alias, Some("pwd".to_string()));
        assert!(password_field.exclude);
    }

    // =========================================================================
    // Const Field Tests
    // =========================================================================

    #[test]
    fn test_parse_const_field() {
        let input: DeriveInput = parse_quote! {
            struct Config {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(const_field)]
                version: String,
            }
        };

        let def = parse_model(&input).unwrap();

        let id_field = def.fields.iter().find(|f| f.name == "id").unwrap();
        assert!(!id_field.const_field); // Default should be false

        let version_field = def.fields.iter().find(|f| f.name == "version").unwrap();
        assert!(version_field.const_field);
    }

    #[test]
    fn test_const_field_combined_with_other_attrs() {
        let input: DeriveInput = parse_quote! {
            struct Config {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(const_field, default = "'1.0.0'")]
                version: String,
            }
        };

        let def = parse_model(&input).unwrap();

        let version_field = def.fields.iter().find(|f| f.name == "version").unwrap();
        assert!(version_field.const_field);
        assert_eq!(version_field.default, Some("'1.0.0'".to_string()));
    }

    // =========================================================================
    // Column Constraints and Metadata Tests (sa_column_args, sa_column_kwargs)
    // =========================================================================

    #[test]
    fn test_parse_column_constraints() {
        let input: DeriveInput = parse_quote! {
            struct Product {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(column_constraints = ["CHECK(price > 0)", "CHECK(price < 1000000)"])]
                price: f64,
            }
        };

        let def = parse_model(&input).unwrap();

        let price_field = def.fields.iter().find(|f| f.name == "price").unwrap();
        assert_eq!(price_field.column_constraints.len(), 2);
        assert_eq!(price_field.column_constraints[0], "CHECK(price > 0)");
        assert_eq!(price_field.column_constraints[1], "CHECK(price < 1000000)");
    }

    #[test]
    fn test_parse_column_comment() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(column_comment = "The user's display name")]
                name: String,
            }
        };

        let def = parse_model(&input).unwrap();

        let name_field = def.fields.iter().find(|f| f.name == "name").unwrap();
        assert_eq!(
            name_field.column_comment,
            Some("The user's display name".to_string())
        );
    }

    #[test]
    fn test_parse_column_info() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(column_info = r#"{"deprecated": true, "replacement": "user_email"}"#)]
                email: String,
            }
        };

        let def = parse_model(&input).unwrap();

        let email_field = def.fields.iter().find(|f| f.name == "email").unwrap();
        assert_eq!(
            email_field.column_info,
            Some(r#"{"deprecated": true, "replacement": "user_email"}"#.to_string())
        );
    }

    #[test]
    fn test_column_constraints_combined_with_other_attrs() {
        let input: DeriveInput = parse_quote! {
            struct Product {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(
                    column_constraints = ["CHECK(price > 0)"],
                    column_comment = "Product price in cents",
                    column_info = r#"{"currency": "USD"}"#,
                    nullable
                )]
                price: Option<i32>,
            }
        };

        let def = parse_model(&input).unwrap();

        let price_field = def.fields.iter().find(|f| f.name == "price").unwrap();
        assert_eq!(price_field.column_constraints.len(), 1);
        assert_eq!(price_field.column_constraints[0], "CHECK(price > 0)");
        assert_eq!(
            price_field.column_comment,
            Some("Product price in cents".to_string())
        );
        assert_eq!(
            price_field.column_info,
            Some(r#"{"currency": "USD"}"#.to_string())
        );
        assert!(price_field.nullable);
    }

    // =========================================================================
    // sa_column Tests (Full Column Override)
    // =========================================================================

    #[test]
    fn test_parse_sa_column_basic() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(sa_column(
                    sql_type = "VARCHAR(50)",
                    server_default = "'active'",
                    comment = "User status"
                ))]
                status: String,
            }
        };

        let def = parse_model(&input).unwrap();

        let status_field = def.fields.iter().find(|f| f.name == "status").unwrap();
        let sa_col = status_field.sa_column.as_ref().unwrap();
        assert_eq!(sa_col.sql_type.as_deref(), Some("VARCHAR(50)"));
        assert_eq!(sa_col.server_default.as_deref(), Some("'active'"));
        assert_eq!(sa_col.comment.as_deref(), Some("User status"));
    }

    #[test]
    fn test_parse_sa_column_with_check_constraint() {
        let input: DeriveInput = parse_quote! {
            struct Product {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(sa_column(
                    sql_type = "DECIMAL(10,2)",
                    check = "price > 0",
                    check = "price < 1000000"
                ))]
                price: String,
            }
        };

        let def = parse_model(&input).unwrap();

        let price_field = def.fields.iter().find(|f| f.name == "price").unwrap();
        let sa_col = price_field.sa_column.as_ref().unwrap();
        assert_eq!(sa_col.sql_type.as_deref(), Some("DECIMAL(10,2)"));
        assert_eq!(sa_col.check.len(), 2);
        assert_eq!(sa_col.check[0], "price > 0");
        assert_eq!(sa_col.check[1], "price < 1000000");
    }

    #[test]
    fn test_parse_sa_column_with_nullable_and_unique() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(sa_column(
                    sql_type = "VARCHAR(255)",
                    nullable,
                    unique
                ))]
                email: String,
            }
        };

        let def = parse_model(&input).unwrap();

        let email_field = def.fields.iter().find(|f| f.name == "email").unwrap();
        let sa_col = email_field.sa_column.as_ref().unwrap();
        assert_eq!(sa_col.sql_type.as_deref(), Some("VARCHAR(255)"));
        assert_eq!(sa_col.nullable, Some(true));
        assert_eq!(sa_col.unique, Some(true));
    }

    #[test]
    fn test_parse_sa_column_with_index() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(sa_column(
                    sql_type = "VARCHAR(100)",
                    index = "idx_user_email"
                ))]
                email: String,
            }
        };

        let def = parse_model(&input).unwrap();

        let email_field = def.fields.iter().find(|f| f.name == "email").unwrap();
        let sa_col = email_field.sa_column.as_ref().unwrap();
        assert_eq!(sa_col.index.as_deref(), Some("idx_user_email"));
    }

    #[test]
    fn test_sa_column_mutual_exclusivity_with_sql_type() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(
                    sql_type = "TEXT",
                    sa_column(sql_type = "VARCHAR(50)")
                )]
                status: String,
            }
        };

        let err = parse_model(&input).unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("sa_column") && err_msg.contains("sql_type"),
            "Expected mutual exclusivity error, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_sa_column_mutual_exclusivity_with_default() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(
                    default = "'pending'",
                    sa_column(server_default = "'active'")
                )]
                status: String,
            }
        };

        let err = parse_model(&input).unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("sa_column") && err_msg.contains("default"),
            "Expected mutual exclusivity error, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_sa_column_mutual_exclusivity_with_column_constraints() {
        let input: DeriveInput = parse_quote! {
            struct Product {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(
                    column_constraints = ["CHECK(price > 0)"],
                    sa_column(check = "price > 0")
                )]
                price: i64,
            }
        };

        let err = parse_model(&input).unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("sa_column") && err_msg.contains("column_constraints"),
            "Expected mutual exclusivity error, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_sa_column_unknown_attribute_error() {
        let input: DeriveInput = parse_quote! {
            struct User {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(sa_column(
                    sql_type = "VARCHAR(50)",
                    unknown_attr = "value"
                ))]
                status: String,
            }
        };

        let err = parse_model(&input).unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("unknown sa_column attribute"),
            "Expected unknown attribute error, got: {}",
            err_msg
        );
    }

    // =========================================================================
    // sa_relationship Tests (Relationship Override Attributes)
    // =========================================================================

    #[test]
    fn test_parse_relationship_order_by() {
        let input: DeriveInput = parse_quote! {
            struct Team {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(
                    model = "heroes",
                    order_by = "name ASC"
                ))]
                members: Related<Vec<Hero>>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel_fields = def.relationship_fields();
        assert_eq!(rel_fields.len(), 1);

        let rel = rel_fields[0].relationship.as_ref().unwrap();
        assert_eq!(rel.order_by.as_deref(), Some("name ASC"));
    }

    #[test]
    fn test_parse_relationship_lazy_strategy() {
        let input: DeriveInput = parse_quote! {
            struct Team {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(
                    model = "heroes",
                    lazy_strategy = "selectin"
                ))]
                members: Related<Vec<Hero>>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel_fields = def.relationship_fields();
        let rel = rel_fields[0].relationship.as_ref().unwrap();
        assert_eq!(rel.lazy_strategy, Some(LazyLoadStrategyAttr::Selectin));
    }

    #[test]
    fn test_parse_relationship_cascade() {
        let input: DeriveInput = parse_quote! {
            struct Team {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(
                    model = "heroes",
                    cascade = "all, delete-orphan"
                ))]
                members: Related<Vec<Hero>>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel_fields = def.relationship_fields();
        let rel = rel_fields[0].relationship.as_ref().unwrap();
        assert_eq!(rel.cascade.as_deref(), Some("all, delete-orphan"));
    }

    #[test]
    fn test_parse_relationship_uselist() {
        let input: DeriveInput = parse_quote! {
            struct Team {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(
                    model = "teams",
                    uselist = false
                ))]
                parent: Related<Team>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel_fields = def.relationship_fields();
        let rel = rel_fields[0].relationship.as_ref().unwrap();
        assert_eq!(rel.uselist, Some(false));
    }

    #[test]
    fn test_parse_relationship_all_new_attrs() {
        let input: DeriveInput = parse_quote! {
            struct Parent {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(
                    model = "children",
                    remote_key = "parent_id",
                    order_by = "created_at DESC",
                    lazy_strategy = "dynamic",
                    cascade = "save-update, merge",
                    uselist
                ))]
                children: Related<Vec<Child>>,
            }
        };

        let def = parse_model(&input).unwrap();
        let rel_fields = def.relationship_fields();
        let rel = rel_fields[0].relationship.as_ref().unwrap();

        assert_eq!(rel.model, "children");
        assert_eq!(rel.remote_key.as_deref(), Some("parent_id"));
        assert_eq!(rel.order_by.as_deref(), Some("created_at DESC"));
        assert_eq!(rel.lazy_strategy, Some(LazyLoadStrategyAttr::Dynamic));
        assert_eq!(rel.cascade.as_deref(), Some("save-update, merge"));
        assert_eq!(rel.uselist, Some(true));
    }

    #[test]
    fn test_parse_relationship_invalid_lazy_strategy() {
        let input: DeriveInput = parse_quote! {
            struct Team {
                #[sqlmodel(primary_key)]
                id: i64,
                #[sqlmodel(relationship(
                    model = "heroes",
                    lazy_strategy = "invalid_strategy"
                ))]
                members: Related<Vec<Hero>>,
            }
        };

        let err = parse_model(&input).unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("unknown lazy strategy"),
            "Expected unknown lazy strategy error, got: {}",
            err_msg
        );
    }
}
