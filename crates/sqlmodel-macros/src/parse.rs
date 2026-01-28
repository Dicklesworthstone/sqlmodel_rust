//! Parsing logic for the Model derive macro.
//!
//! This module extracts struct-level and field-level attributes from the
//! derive input to build `ModelDef` and `FieldDef` structures used for
//! code generation.

use proc_macro2::Span;
use quote::ToTokens;
use syn::{Attribute, Data, DeriveInput, Error, Field, Fields, Generics, Ident, Lit, Result, Type};

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
    /// Whether to use lazy loading.
    pub lazy: bool,
    /// Cascade delete behavior.
    pub cascade_delete: bool,
    /// Inferred relationship kind from field type.
    pub kind: RelationshipKindAttr,
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

impl ModelDef {
    /// Returns the fields that are part of the primary key.
    pub fn primary_key_fields(&self) -> Vec<&FieldDef> {
        self.fields.iter().filter(|f| f.primary_key).collect()
    }

    /// Returns fields that should be included in INSERT statements (reserved for future use).
    #[allow(dead_code)]
    pub fn insert_fields(&self) -> Vec<&FieldDef> {
        self.fields
            .iter()
            .filter(|f| !f.skip && !f.skip_insert && f.relationship.is_none())
            .collect()
    }

    /// Returns fields that should be included in UPDATE statements (reserved for future use).
    #[allow(dead_code)]
    pub fn update_fields(&self) -> Vec<&FieldDef> {
        self.fields
            .iter()
            .filter(|f| !f.skip && !f.skip_update && !f.primary_key && f.relationship.is_none())
            .collect()
    }

    /// Returns fields that should be read from the database (SELECT).
    /// Excludes skipped fields and relationship fields (they're not DB columns).
    pub fn select_fields(&self) -> Vec<&FieldDef> {
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
}

impl FieldDef {
    /// Returns the name to use when serializing this field (output).
    ///
    /// Priority: serialization_alias > alias > field name
    pub fn output_name(&self) -> &str {
        self.serialization_alias
            .as_deref()
            .or(self.alias.as_deref())
            .unwrap_or_else(|| self.name.to_string().leak())
    }

    /// Returns all names that should be accepted when deserializing (input).
    ///
    /// This includes: field name, alias, and validation_alias.
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
    let (table_name, table_alias) = parse_struct_sqlmodel_attrs(&input.attrs, &name)?;

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
    })
}

/// Parse struct-level `#[sqlmodel(...)]` attributes.
///
/// Supported keys:
/// - `table = "name"` (overrides derived table name)
/// - `table_alias = "alias"` (optional table alias)
fn parse_struct_sqlmodel_attrs(
    attrs: &[Attribute],
    struct_name: &Ident,
) -> Result<(String, Option<String>)> {
    let mut table_name: Option<String> = None;
    let mut table_alias: Option<String> = None;

    for attr in attrs {
        if !attr.path().is_ident("sqlmodel") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("table") {
                if table_name.is_some() {
                    return Err(Error::new_spanned(
                        meta.path,
                        "duplicate sqlmodel attribute: table",
                    ));
                }

                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    table_name = Some(lit_str.value());
                    Ok(())
                } else {
                    Err(Error::new_spanned(
                        value,
                        "expected string literal for table name",
                    ))
                }
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
            } else {
                Err(Error::new_spanned(
                    meta.path,
                    "unknown sqlmodel struct attribute (supported: table, table_alias)",
                ))
            }
        })?;
    }

    let table_name = table_name.unwrap_or_else(|| derive_table_name(&struct_name.to_string()));
    Ok((table_name, table_alias))
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
                         serialization_alias"
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
    let mut link_table: Option<LinkTableAttr> = None;
    let mut one_to_one = false;
    let mut many_to_many = false;

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
        } else {
            return Err(Error::new_spanned(
                path,
                "unknown relationship attribute. \
                 Valid: model, foreign_key, remote_key, back_populates, lazy, \
                 cascade_delete, one_to_one, many_to_many, link_table",
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
        kind,
    })
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
}
