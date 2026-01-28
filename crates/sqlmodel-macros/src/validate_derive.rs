//! Implementation of the Validate derive macro.
//!
//! This module generates validation logic at compile time based on
//! `#[validate(...)]` field attributes.

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{
    Data, DeriveInput, Error, Field, Fields, GenericArgument, Ident, Lit, PathArguments, Result,
    Type,
};

/// Mode for model-level validators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ValidatorMode {
    /// Run before field validation, can preprocess input.
    Before,
    /// Run after field validation (default).
    #[default]
    After,
}

/// Parsed model-level validator.
#[derive(Debug)]
pub struct ModelValidator {
    /// The function name to call for validation.
    pub function: String,
    /// The mode (before or after field validation).
    pub mode: ValidatorMode,
}

/// Parsed validation definition from a struct with `#[derive(Validate)]`.
#[derive(Debug)]
pub struct ValidateDef {
    /// The struct name.
    pub name: Ident,
    /// Parsed field validation rules.
    pub fields: Vec<ValidateFieldDef>,
    /// Model-level validators.
    pub model_validators: Vec<ModelValidator>,
    /// Generics from the struct.
    pub generics: syn::Generics,
}

/// Parsed validation rules for a single field.
#[derive(Debug)]
pub struct ValidateFieldDef {
    /// The field name.
    pub name: Ident,
    /// The field type.
    pub ty: Type,
    /// Minimum value constraint.
    pub min: Option<f64>,
    /// Maximum value constraint.
    pub max: Option<f64>,
    /// Minimum length for strings.
    pub min_length: Option<usize>,
    /// Maximum length for strings.
    pub max_length: Option<usize>,
    /// Regex pattern for strings.
    pub pattern: Option<String>,
    /// Whether the field is required (non-optional).
    pub required: bool,
    /// Custom validation function name.
    pub custom: Option<String>,
}

/// Parse a `DeriveInput` into a `ValidateDef`.
pub fn parse_validate(input: &DeriveInput) -> Result<ValidateDef> {
    let name = input.ident.clone();
    let generics = input.generics.clone();

    // Parse struct-level attributes for model validators
    let model_validators = parse_model_validators(&input.attrs)?;

    let fields = match &input.data {
        Data::Struct(data) => parse_validate_fields(&data.fields)?,
        Data::Enum(_) => {
            return Err(Error::new_spanned(
                input,
                "Validate can only be derived for structs, not enums",
            ));
        }
        Data::Union(_) => {
            return Err(Error::new_spanned(
                input,
                "Validate can only be derived for structs, not unions",
            ));
        }
    };

    Ok(ValidateDef {
        name,
        fields,
        model_validators,
        generics,
    })
}

/// Parse struct-level `#[validate(...)]` attributes for model validators.
fn parse_model_validators(attrs: &[syn::Attribute]) -> Result<Vec<ModelValidator>> {
    let mut validators = Vec::new();

    for attr in attrs {
        if !attr.path().is_ident("validate") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("model") {
                // Parse: #[validate(model = "fn_name")] or
                // #[validate(model(fn = "fn_name", mode = "after"))]
                if meta.input.peek(syn::Token![=]) {
                    // Simple form: model = "fn_name"
                    let value: Lit = meta.value()?.parse()?;
                    if let Lit::Str(lit_str) = value {
                        validators.push(ModelValidator {
                            function: lit_str.value(),
                            mode: ValidatorMode::After,
                        });
                    } else {
                        return Err(Error::new_spanned(
                            value,
                            "expected string literal for model validator function name",
                        ));
                    }
                } else if meta.input.peek(syn::token::Paren) {
                    // Extended form: model(fn = "fn_name", mode = "after")
                    let mut function: Option<String> = None;
                    let mut mode = ValidatorMode::After;

                    meta.parse_nested_meta(|inner| {
                        if inner.path.is_ident("fn") {
                            let value: Lit = inner.value()?.parse()?;
                            if let Lit::Str(lit_str) = value {
                                function = Some(lit_str.value());
                            } else {
                                return Err(Error::new_spanned(
                                    value,
                                    "expected string literal for fn",
                                ));
                            }
                        } else if inner.path.is_ident("mode") {
                            let value: Lit = inner.value()?.parse()?;
                            if let Lit::Str(lit_str) = value {
                                mode = match lit_str.value().as_str() {
                                    "before" => ValidatorMode::Before,
                                    "after" => ValidatorMode::After,
                                    other => {
                                        return Err(Error::new_spanned(
                                            lit_str,
                                            format!(
                                                "invalid mode '{other}', expected 'before' or 'after'"
                                            ),
                                        ))
                                    }
                                };
                            } else {
                                return Err(Error::new_spanned(
                                    value,
                                    "expected string literal for mode",
                                ));
                            }
                        } else {
                            return Err(Error::new_spanned(
                                inner.path,
                                "unknown model validator attribute, expected 'fn' or 'mode'",
                            ));
                        }
                        Ok(())
                    })?;

                    let function = function.ok_or_else(|| {
                        Error::new(
                            proc_macro2::Span::call_site(),
                            "model validator requires 'fn' attribute",
                        )
                    })?;

                    validators.push(ModelValidator { function, mode });
                } else {
                    return Err(Error::new_spanned(
                        meta.path,
                        "expected '=' or '(...)' after 'model'",
                    ));
                }
                Ok(())
            } else {
                Err(Error::new_spanned(
                    meta.path,
                    "unknown struct-level validate attribute, expected 'model'",
                ))
            }
        })?;
    }

    Ok(validators)
}

/// Parse all fields from a struct for validation.
fn parse_validate_fields(fields: &Fields) -> Result<Vec<ValidateFieldDef>> {
    match fields {
        Fields::Named(named) => named.named.iter().map(parse_validate_field).collect(),
        Fields::Unnamed(_) => Err(Error::new_spanned(
            fields,
            "Validate requires a struct with named fields",
        )),
        Fields::Unit => Ok(Vec::new()),
    }
}

/// Parse a single field and its validation attributes.
fn parse_validate_field(field: &Field) -> Result<ValidateFieldDef> {
    let name = field
        .ident
        .clone()
        .ok_or_else(|| Error::new_spanned(field, "expected named field"))?;

    let ty = field.ty.clone();
    let is_optional = is_option_type(&ty);

    let mut min = None;
    let mut max = None;
    let mut min_length = None;
    let mut max_length = None;
    let mut pattern = None;
    let mut required = false;
    let mut custom = None;

    // Parse #[validate(...)] attributes
    for attr in &field.attrs {
        if !attr.path().is_ident("validate") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            let path = &meta.path;

            if path.is_ident("min") {
                let value: Lit = meta.value()?.parse()?;
                min = Some(parse_numeric_lit(&value)?);
            } else if path.is_ident("max") {
                let value: Lit = meta.value()?.parse()?;
                max = Some(parse_numeric_lit(&value)?);
            } else if path.is_ident("min_length") {
                let value: Lit = meta.value()?.parse()?;
                min_length = Some(parse_usize_lit(&value)?);
            } else if path.is_ident("max_length") {
                let value: Lit = meta.value()?.parse()?;
                max_length = Some(parse_usize_lit(&value)?);
            } else if path.is_ident("pattern") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    let pattern_str = lit_str.value();
                    // Validate regex at compile time
                    if let Err(e) = regex::Regex::new(&pattern_str) {
                        return Err(Error::new_spanned(
                            lit_str,
                            format!("invalid regex pattern: {e}"),
                        ));
                    }
                    pattern = Some(pattern_str);
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for pattern",
                    ));
                }
            } else if path.is_ident("required") {
                required = true;
            } else if path.is_ident("custom") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(lit_str) = value {
                    custom = Some(lit_str.value());
                } else {
                    return Err(Error::new_spanned(
                        value,
                        "expected string literal for custom function name",
                    ));
                }
            } else if path.is_ident("email") {
                // Email validation is a common pattern
                pattern = Some(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$".to_string());
            } else if path.is_ident("url") {
                // URL validation pattern (simplified)
                pattern = Some(r"^https?://[^\s/$.?#].[^\s]*$".to_string());
            } else {
                let attr_name = path.to_token_stream().to_string();
                return Err(Error::new_spanned(
                    path,
                    format!(
                        "unknown validate attribute `{attr_name}`. \
                         Valid attributes are: min, max, min_length, max_length, pattern, \
                         required, custom, email, url"
                    ),
                ));
            }

            Ok(())
        })?;
    }

    // If field is not optional and has validation rules, imply required
    if !is_optional
        && (min.is_some()
            || max.is_some()
            || min_length.is_some()
            || max_length.is_some()
            || pattern.is_some())
    {
        // Non-optional fields with constraints are implicitly required
    }

    Ok(ValidateFieldDef {
        name,
        ty,
        min,
        max,
        min_length,
        max_length,
        pattern,
        required,
        custom,
    })
}

/// Parse a numeric literal to f64.
fn parse_numeric_lit(lit: &Lit) -> Result<f64> {
    match lit {
        Lit::Int(int_lit) => int_lit
            .base10_parse::<i64>()
            .map(|v| v as f64)
            .map_err(|e| Error::new_spanned(lit, format!("invalid integer: {e}"))),
        Lit::Float(float_lit) => float_lit
            .base10_parse::<f64>()
            .map_err(|e| Error::new_spanned(lit, format!("invalid float: {e}"))),
        _ => Err(Error::new_spanned(lit, "expected numeric literal")),
    }
}

/// Parse a numeric literal to usize.
fn parse_usize_lit(lit: &Lit) -> Result<usize> {
    match lit {
        Lit::Int(int_lit) => int_lit
            .base10_parse::<usize>()
            .map_err(|e| Error::new_spanned(lit, format!("invalid integer: {e}"))),
        _ => Err(Error::new_spanned(lit, "expected integer literal")),
    }
}

/// Check if a type is `Option<T>`.
fn is_option_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            return segment.ident == "Option";
        }
    }
    false
}

/// Extract the inner type from `Option<T>`.
#[allow(dead_code)]
fn extract_option_inner(ty: &Type) -> Option<&Type> {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            if segment.ident == "Option" {
                if let PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(GenericArgument::Type(inner)) = args.args.first() {
                        return Some(inner);
                    }
                }
            }
        }
    }
    None
}

/// Generate the Validate trait implementation.
pub fn generate_validate_impl(def: &ValidateDef) -> TokenStream {
    let name = &def.name;
    let (impl_generics, ty_generics, where_clause) = def.generics.split_for_impl();

    // Generate validation code for each field
    let field_validations: Vec<TokenStream> = def
        .fields
        .iter()
        .filter(|f| has_validation(f))
        .map(generate_field_validation)
        .collect();

    // Generate model validator calls (before)
    let before_validators: Vec<TokenStream> = def
        .model_validators
        .iter()
        .filter(|v| v.mode == ValidatorMode::Before)
        .map(|v| {
            let fn_name = syn::Ident::new(&v.function, proc_macro2::Span::call_site());
            quote! {
                if let Err(msg) = self.#fn_name() {
                    errors.add_model_error(msg);
                }
            }
        })
        .collect();

    // Generate model validator calls (after)
    let after_validators: Vec<TokenStream> = def
        .model_validators
        .iter()
        .filter(|v| v.mode == ValidatorMode::After)
        .map(|v| {
            let fn_name = syn::Ident::new(&v.function, proc_macro2::Span::call_site());
            quote! {
                if let Err(msg) = self.#fn_name() {
                    errors.add_model_error(msg);
                }
            }
        })
        .collect();

    // If no validations and no model validators, generate a trivial impl
    if field_validations.is_empty() && def.model_validators.is_empty() {
        return quote! {
            impl #impl_generics #name #ty_generics #where_clause {
                /// Validate this model's fields.
                ///
                /// Returns `Ok(())` if all validations pass, or `Err(ValidationError)`
                /// with details about which fields failed.
                pub fn validate(&self) -> std::result::Result<(), sqlmodel_core::ValidationError> {
                    Ok(())
                }
            }
        };
    }

    quote! {
        impl #impl_generics #name #ty_generics #where_clause {
            /// Validate this model's fields and model-level constraints.
            ///
            /// Returns `Ok(())` if all validations pass, or `Err(ValidationError)`
            /// with details about which fields or model constraints failed.
            ///
            /// Validation order:
            /// 1. Model validators with mode="before"
            /// 2. Field-level validations
            /// 3. Model validators with mode="after" (default)
            pub fn validate(&self) -> std::result::Result<(), sqlmodel_core::ValidationError> {
                let mut errors = sqlmodel_core::ValidationError::new();

                // 1. Before validators (run before field validation)
                #(#before_validators)*

                // 2. Field validations
                #(#field_validations)*

                // 3. After validators (run after field validation, default mode)
                #(#after_validators)*

                errors.into_result()
            }
        }
    }
}

/// Check if a field has any validation rules.
fn has_validation(field: &ValidateFieldDef) -> bool {
    field.min.is_some()
        || field.max.is_some()
        || field.min_length.is_some()
        || field.max_length.is_some()
        || field.pattern.is_some()
        || field.required
        || field.custom.is_some()
}

/// Generate validation code for a single field.
fn generate_field_validation(field: &ValidateFieldDef) -> TokenStream {
    let field_name = &field.name;
    let field_name_str = field_name.to_string();
    let is_optional = is_option_type(&field.ty);

    let mut checks = Vec::new();

    // Required check for optional fields marked as required
    if field.required && is_optional {
        checks.push(quote! {
            if self.#field_name.is_none() {
                errors.add_required(#field_name_str);
            }
        });
    }

    // Min value check
    if let Some(min) = field.min {
        if is_optional {
            checks.push(quote! {
                if let Some(ref value) = self.#field_name {
                    if (*value as f64) < #min {
                        errors.add_min(#field_name_str, #min, *value);
                    }
                }
            });
        } else {
            checks.push(quote! {
                if (self.#field_name as f64) < #min {
                    errors.add_min(#field_name_str, #min, self.#field_name);
                }
            });
        }
    }

    // Max value check
    if let Some(max) = field.max {
        if is_optional {
            checks.push(quote! {
                if let Some(ref value) = self.#field_name {
                    if (*value as f64) > #max {
                        errors.add_max(#field_name_str, #max, *value);
                    }
                }
            });
        } else {
            checks.push(quote! {
                if (self.#field_name as f64) > #max {
                    errors.add_max(#field_name_str, #max, self.#field_name);
                }
            });
        }
    }

    // Min length check (for String/str types)
    if let Some(min_len) = field.min_length {
        if is_optional {
            checks.push(quote! {
                if let Some(ref value) = self.#field_name {
                    let len = value.len();
                    if len < #min_len {
                        errors.add_min_length(#field_name_str, #min_len, len);
                    }
                }
            });
        } else {
            checks.push(quote! {
                {
                    let len = self.#field_name.len();
                    if len < #min_len {
                        errors.add_min_length(#field_name_str, #min_len, len);
                    }
                }
            });
        }
    }

    // Max length check (for String/str types)
    if let Some(max_len) = field.max_length {
        if is_optional {
            checks.push(quote! {
                if let Some(ref value) = self.#field_name {
                    let len = value.len();
                    if len > #max_len {
                        errors.add_max_length(#field_name_str, #max_len, len);
                    }
                }
            });
        } else {
            checks.push(quote! {
                {
                    let len = self.#field_name.len();
                    if len > #max_len {
                        errors.add_max_length(#field_name_str, #max_len, len);
                    }
                }
            });
        }
    }

    // Pattern check (regex)
    // Uses sqlmodel_core::validate::matches_pattern for full regex support
    if let Some(ref pattern) = field.pattern {
        if is_optional {
            checks.push(quote! {
                if let Some(ref value) = self.#field_name {
                    if !sqlmodel_core::validate::matches_pattern(value.as_ref(), #pattern) {
                        errors.add_pattern(#field_name_str, #pattern);
                    }
                }
            });
        } else {
            checks.push(quote! {
                if !sqlmodel_core::validate::matches_pattern(self.#field_name.as_ref(), #pattern) {
                    errors.add_pattern(#field_name_str, #pattern);
                }
            });
        }
    }

    // Custom validation function
    if let Some(ref custom_fn) = field.custom {
        let custom_fn_ident = syn::Ident::new(custom_fn, field_name.span());
        if is_optional {
            checks.push(quote! {
                if let Some(ref value) = self.#field_name {
                    if let Err(msg) = self.#custom_fn_ident(value) {
                        errors.add_custom(#field_name_str, msg);
                    }
                }
            });
        } else {
            checks.push(quote! {
                if let Err(msg) = self.#custom_fn_ident(&self.#field_name) {
                    errors.add_custom(#field_name_str, msg);
                }
            });
        }
    }

    quote! {
        #(#checks)*
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn test_is_option_type() {
        let ty: Type = parse_quote!(Option<String>);
        assert!(is_option_type(&ty));

        let ty: Type = parse_quote!(String);
        assert!(!is_option_type(&ty));
    }

    #[test]
    fn test_has_validation() {
        let field = ValidateFieldDef {
            name: syn::Ident::new("test", proc_macro2::Span::call_site()),
            ty: parse_quote!(String),
            min: None,
            max: None,
            min_length: Some(1),
            max_length: None,
            pattern: None,
            required: false,
            custom: None,
        };
        assert!(has_validation(&field));

        let field = ValidateFieldDef {
            name: syn::Ident::new("test", proc_macro2::Span::call_site()),
            ty: parse_quote!(String),
            min: None,
            max: None,
            min_length: None,
            max_length: None,
            pattern: None,
            required: false,
            custom: None,
        };
        assert!(!has_validation(&field));
    }

    // ==================== Model Validator Tests ====================

    #[test]
    fn test_parse_model_validator_simple() {
        let input: syn::DeriveInput = parse_quote! {
            #[validate(model = "validate_passwords")]
            struct User {
                password: String,
                confirm_password: String,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.model_validators.len(), 1);
        assert_eq!(def.model_validators[0].function, "validate_passwords");
        assert_eq!(def.model_validators[0].mode, ValidatorMode::After);
    }

    #[test]
    fn test_parse_model_validator_with_mode_after() {
        let input: syn::DeriveInput = parse_quote! {
            #[validate(model(fn = "check_dates", mode = "after"))]
            struct Event {
                start_date: String,
                end_date: String,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.model_validators.len(), 1);
        assert_eq!(def.model_validators[0].function, "check_dates");
        assert_eq!(def.model_validators[0].mode, ValidatorMode::After);
    }

    #[test]
    fn test_parse_model_validator_with_mode_before() {
        let input: syn::DeriveInput = parse_quote! {
            #[validate(model(fn = "preprocess_data", mode = "before"))]
            struct Data {
                value: String,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.model_validators.len(), 1);
        assert_eq!(def.model_validators[0].function, "preprocess_data");
        assert_eq!(def.model_validators[0].mode, ValidatorMode::Before);
    }

    #[test]
    fn test_parse_multiple_model_validators() {
        let input: syn::DeriveInput = parse_quote! {
            #[validate(model = "validate_first")]
            #[validate(model(fn = "validate_second", mode = "before"))]
            struct Complex {
                field: String,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.model_validators.len(), 2);

        assert_eq!(def.model_validators[0].function, "validate_first");
        assert_eq!(def.model_validators[0].mode, ValidatorMode::After);

        assert_eq!(def.model_validators[1].function, "validate_second");
        assert_eq!(def.model_validators[1].mode, ValidatorMode::Before);
    }

    #[test]
    fn test_parse_model_validator_invalid_mode() {
        let input: syn::DeriveInput = parse_quote! {
            #[validate(model(fn = "validate_fn", mode = "invalid"))]
            struct Data {
                field: String,
            }
        };

        let result = parse_validate(&input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid mode"));
    }

    #[test]
    fn test_validator_mode_default() {
        let mode = ValidatorMode::default();
        assert_eq!(mode, ValidatorMode::After);
    }

    #[test]
    fn test_model_validator_with_field_validators() {
        let input: syn::DeriveInput = parse_quote! {
            #[validate(model = "validate_passwords")]
            struct User {
                #[validate(min_length = 8)]
                password: String,
                #[validate(min_length = 8)]
                confirm_password: String,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.model_validators.len(), 1);
        assert_eq!(def.fields.len(), 2);
        assert!(def.fields[0].min_length.is_some());
        assert!(def.fields[1].min_length.is_some());
    }
}
