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
            .filter(|f| !f.skip && !f.skip_insert)
            .collect()
    }

    /// Returns fields that should be included in UPDATE statements (reserved for future use).
    #[allow(dead_code)]
    pub fn update_fields(&self) -> Vec<&FieldDef> {
        self.fields
            .iter()
            .filter(|f| !f.skip && !f.skip_update && !f.primary_key)
            .collect()
    }

    /// Returns fields that should be read from the database (SELECT).
    pub fn select_fields(&self) -> Vec<&FieldDef> {
        self.fields.iter().filter(|f| !f.skip).collect()
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
    let attrs = parse_field_attrs(&field.attrs, &name)?;

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
}

/// Parse all `#[sqlmodel(...)]` attributes on a field.
fn parse_field_attrs(attrs: &[Attribute], field_name: &Ident) -> Result<FieldAttrs> {
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
            } else {
                // Unknown attribute
                let attr_name = path.to_token_stream().to_string();
                return Err(Error::new_spanned(
                    path,
                    format!(
                        "unknown sqlmodel attribute `{attr_name}`. \
                         Valid attributes are: primary_key, auto_increment, column, nullable, \
                         unique, foreign_key, on_delete, on_update, default, sql_type, index, skip, skip_insert, skip_update"
                    ),
                ));
            }

            Ok(())
        })?;
    }

    // Validate attribute combinations
    validate_field_attrs(&result, field_name)?;

    Ok(result)
}

/// Validate that attribute combinations make sense.
fn validate_field_attrs(attrs: &FieldAttrs, field_name: &Ident) -> Result<()> {
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
}
