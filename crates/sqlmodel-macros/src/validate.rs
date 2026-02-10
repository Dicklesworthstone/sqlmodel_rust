//! Compile-time validation for Model derive macro.
//!
//! This module provides comprehensive validation with clear, actionable
//! error messages that point to the correct source locations.

use std::collections::HashSet;

use proc_macro2::Span;
use quote::ToTokens;
use syn::{Error, GenericArgument, PathArguments, Type};

use crate::parse::{FieldDef, ModelDef};

/// Validate a parsed model definition.
///
/// Performs all validations and returns combined errors if any issues are found.
/// This allows reporting multiple problems at once rather than failing on the first.
pub fn validate_model(model: &ModelDef) -> Result<(), Error> {
    let mut errors = Vec::new();

    // Struct-level validations
    validate_has_fields(model, &mut errors);
    validate_table_name(&model.table_name, model.name.span(), &mut errors);
    validate_no_duplicate_columns(model, &mut errors);

    // Field-level validations
    for field in &model.fields {
        validate_field(field, &mut errors);
    }

    // Cross-field validations
    validate_auto_increment_has_pk(model, &mut errors);
    validate_joined_inheritance_parent_field(model, &mut errors);

    // Combine all errors
    if errors.is_empty() {
        Ok(())
    } else {
        let mut combined = errors.remove(0);
        for err in errors {
            combined.combine(err);
        }
        Err(combined)
    }
}

fn validate_joined_inheritance_parent_field(model: &ModelDef, errors: &mut Vec<Error>) {
    // Joined inheritance child: `#[sqlmodel(table, inherits = "...")]` (inferred as joined)
    if !model.config.table {
        return;
    }
    if model.config.inherits.is_none() {
        return;
    }
    if model.config.discriminator_value.is_some() {
        // STI child: not a joined-table inheritance child.
        return;
    }
    if model.config.inheritance != crate::parse::InheritanceStrategy::Joined {
        return;
    }

    let parent_fields: Vec<_> = model.fields.iter().filter(|f| f.parent).collect();
    if parent_fields.len() != 1 {
        errors.push(Error::new(
            model.name.span(),
            "joined-table inheritance child models must include exactly one `#[sqlmodel(parent)]` field to embed the parent model",
        ));
    }
}

/// Validate that the struct has at least one field.
fn validate_has_fields(model: &ModelDef, errors: &mut Vec<Error>) {
    if model.fields.is_empty() {
        errors.push(Error::new(
            model.name.span(),
            "Model struct must have at least one field",
        ));
    }
}

/// Validate that the table name doesn't contain SQL injection characters.
fn validate_table_name(table_name: &str, span: Span, errors: &mut Vec<Error>) {
    // Characters that could be problematic in SQL identifiers
    const DANGEROUS_CHARS: &[char] = &[';', '\'', '"', '`', '-', '/', '*', '\\', '\0', '\n', '\r'];

    for ch in table_name.chars() {
        if DANGEROUS_CHARS.contains(&ch) {
            errors.push(Error::new(
                span,
                format!(
                    "table name contains invalid character '{ch}'; \
                     table names should only contain alphanumeric characters and underscores"
                ),
            ));
            return;
        }
    }

    // Also check for empty or whitespace-only
    if table_name.trim().is_empty() {
        errors.push(Error::new(span, "table name cannot be empty or whitespace"));
    }

    // Check it starts with a letter or underscore
    if let Some(first) = table_name.chars().next() {
        if !first.is_alphabetic() && first != '_' {
            errors.push(Error::new(
                span,
                format!("table name must start with a letter or underscore, got '{first}'"),
            ));
        }
    }
}

/// Validate that no two non-skipped fields map to the same column name.
fn validate_no_duplicate_columns(model: &ModelDef, errors: &mut Vec<Error>) {
    let mut seen_columns: HashSet<&str> = HashSet::new();

    for field in &model.fields {
        if field.skip {
            continue;
        }

        if !seen_columns.insert(&field.column_name) {
            errors.push(Error::new(
                field.name.span(),
                format!(
                    "duplicate column name '{}'; another field already maps to this column",
                    field.column_name
                ),
            ));
        }
    }
}

/// Validate that auto_increment fields are also primary_key.
fn validate_auto_increment_has_pk(model: &ModelDef, errors: &mut Vec<Error>) {
    for field in &model.fields {
        if field.auto_increment && !field.primary_key {
            errors.push(Error::new(
                field.name.span(),
                "auto_increment requires primary_key; add #[sqlmodel(primary_key)] to this field",
            ));
        }
    }
}

/// Validate a single field.
fn validate_field(field: &FieldDef, errors: &mut Vec<Error>) {
    validate_type(&field.ty, field.name.span(), errors);
    validate_skip_conflicts(field, errors);
}

/// Validate that a type is supported.
fn validate_type(ty: &Type, span: Span, errors: &mut Vec<Error>) {
    // Check for nested Option<Option<T>>
    if is_nested_option(ty) {
        errors.push(Error::new(
            span,
            "nested Option<Option<T>> is ambiguous and not supported; \
             use a single Option<T> or a custom type",
        ));
    }

    // Check for reference types
    if is_reference_type(ty) {
        errors.push(Error::new(
            span,
            "reference types (&T) are not supported; use owned types instead",
        ));
    }

    // Check for raw pointers
    if is_raw_pointer(ty) {
        errors.push(Error::new(
            span,
            "raw pointer types (*const T, *mut T) are not supported; use owned types instead",
        ));
    }
}

/// Validate that skip doesn't conflict with other attributes.
fn validate_skip_conflicts(field: &FieldDef, errors: &mut Vec<Error>) {
    // These are already checked in parse.rs, but we do a final check here
    // for any that might have slipped through or been added later

    if field.skip && field.unique {
        errors.push(Error::new(
            field.name.span(),
            "cannot use both #[sqlmodel(skip)] and #[sqlmodel(unique)] on the same field; \
             skipped fields are excluded from database operations",
        ));
    }

    if field.skip && field.foreign_key.is_some() {
        errors.push(Error::new(
            field.name.span(),
            "cannot use both #[sqlmodel(skip)] and #[sqlmodel(foreign_key)] on the same field; \
             skipped fields are excluded from database operations",
        ));
    }

    if field.skip && field.index.is_some() {
        errors.push(Error::new(
            field.name.span(),
            "cannot use both #[sqlmodel(skip)] and #[sqlmodel(index)] on the same field; \
             skipped fields are excluded from database operations",
        ));
    }
}

/// Check if a type is Option<Option<T>> (nested Option).
fn is_nested_option(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            if segment.ident == "Option" {
                if let PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(GenericArgument::Type(Type::Path(inner_path))) = args.args.first() {
                        if let Some(inner_seg) = inner_path.path.segments.last() {
                            return inner_seg.ident == "Option";
                        }
                    }
                }
            }
        }
    }
    false
}

/// Check if a type is a reference (&T or &mut T).
fn is_reference_type(ty: &Type) -> bool {
    matches!(ty, Type::Reference(_))
}

/// Check if a type is a raw pointer (*const T or *mut T).
fn is_raw_pointer(ty: &Type) -> bool {
    matches!(ty, Type::Ptr(_))
}

/// Format a type for error messages.
#[allow(dead_code)]
fn type_to_string(ty: &Type) -> String {
    ty.to_token_stream().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn test_is_nested_option() {
        let ty: Type = parse_quote!(Option<Option<i32>>);
        assert!(is_nested_option(&ty));

        let ty: Type = parse_quote!(Option<i32>);
        assert!(!is_nested_option(&ty));

        let ty: Type = parse_quote!(i32);
        assert!(!is_nested_option(&ty));
    }

    #[test]
    fn test_is_reference_type() {
        let ty: Type = parse_quote!(&str);
        assert!(is_reference_type(&ty));

        let ty: Type = parse_quote!(&'static str);
        assert!(is_reference_type(&ty));

        let ty: Type = parse_quote!(&mut String);
        assert!(is_reference_type(&ty));

        let ty: Type = parse_quote!(String);
        assert!(!is_reference_type(&ty));
    }

    #[test]
    fn test_is_raw_pointer() {
        let ty: Type = parse_quote!(*const i32);
        assert!(is_raw_pointer(&ty));

        let ty: Type = parse_quote!(*mut u8);
        assert!(is_raw_pointer(&ty));

        let ty: Type = parse_quote!(Box<i32>);
        assert!(!is_raw_pointer(&ty));
    }

    #[test]
    fn test_validate_table_name_valid() {
        let mut errors = Vec::new();
        validate_table_name("users", Span::call_site(), &mut errors);
        assert!(errors.is_empty());

        validate_table_name("user_accounts", Span::call_site(), &mut errors);
        assert!(errors.is_empty());

        validate_table_name("_internal", Span::call_site(), &mut errors);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_table_name_invalid() {
        let mut errors = Vec::new();
        validate_table_name("users; DROP TABLE users", Span::call_site(), &mut errors);
        assert!(!errors.is_empty());

        let mut errors = Vec::new();
        validate_table_name("user's", Span::call_site(), &mut errors);
        assert!(!errors.is_empty());

        let mut errors = Vec::new();
        validate_table_name("123users", Span::call_site(), &mut errors);
        assert!(!errors.is_empty());
    }
}
