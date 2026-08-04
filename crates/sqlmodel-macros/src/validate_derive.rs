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
    /// Value must be a multiple of this number.
    pub multiple_of: Option<f64>,
    /// Minimum number of items in a collection.
    pub min_items: Option<usize>,
    /// Maximum number of items in a collection.
    pub max_items: Option<usize>,
    /// Whether items in a collection must be unique.
    pub unique_items: bool,
    /// Whether to validate as a credit card number (Luhn check).
    pub credit_card: bool,
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
    let mut multiple_of = None;
    let mut min_items = None;
    let mut max_items = None;
    let mut unique_items = false;
    let mut credit_card = false;

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
            } else if path.is_ident("uuid") {
                // UUID format validation (RFC 4122)
                // Matches: 8-4-4-4-12 hex digits with version 1-5 and variant 8-b
                pattern = Some(
                    r"(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
                        .to_string(),
                );
            } else if path.is_ident("ipv4") {
                // IPv4 address validation
                // Matches: 0.0.0.0 to 255.255.255.255
                pattern = Some(
                    r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
                        .to_string(),
                );
            } else if path.is_ident("ipv6") {
                // IPv6 address validation (simplified - covers most common formats)
                // Full form: 8 groups of 4 hex digits separated by colons
                // Also handles :: abbreviation for consecutive zeros
                pattern = Some(
                    r"(?i)^(?:(?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}|(?:[0-9a-f]{1,4}:){1,7}:|(?:[0-9a-f]{1,4}:){1,6}:[0-9a-f]{1,4}|(?:[0-9a-f]{1,4}:){1,5}(?::[0-9a-f]{1,4}){1,2}|(?:[0-9a-f]{1,4}:){1,4}(?::[0-9a-f]{1,4}){1,3}|(?:[0-9a-f]{1,4}:){1,3}(?::[0-9a-f]{1,4}){1,4}|(?:[0-9a-f]{1,4}:){1,2}(?::[0-9a-f]{1,4}){1,5}|[0-9a-f]{1,4}:(?::[0-9a-f]{1,4}){1,6}|:(?::[0-9a-f]{1,4}){1,7}|::)$"
                        .to_string(),
                );
            } else if path.is_ident("mac_address") {
                // MAC address validation (colon or hyphen separated)
                // Matches: XX:XX:XX:XX:XX:XX or XX-XX-XX-XX-XX-XX
                pattern = Some(
                    r"(?i)^(?:[0-9a-f]{2}[:-]){5}[0-9a-f]{2}$".to_string(),
                );
            } else if path.is_ident("slug") {
                // URL-safe slug validation
                // Lowercase alphanumeric with hyphens, no leading/trailing hyphens
                pattern = Some(r"^[a-z0-9]+(?:-[a-z0-9]+)*$".to_string());
            } else if path.is_ident("hex_color") {
                // Hex color validation (#RGB or #RRGGBB)
                pattern = Some(r"(?i)^#(?:[0-9a-f]{3}|[0-9a-f]{6})$".to_string());
            } else if path.is_ident("phone") {
                // Phone number validation (E.164 format)
                // Optional + followed by 1-15 digits, can't start with 0
                pattern = Some(r"^\+?[1-9]\d{1,14}$".to_string());
            } else if path.is_ident("credit_card") {
                // Credit card validation requires Luhn algorithm check
                // This flag triggers runtime validation, not just regex
                credit_card = true;
            } else if path.is_ident("multiple_of") {
                let value: Lit = meta.value()?.parse()?;
                let divisor = parse_numeric_lit(&value)?;
                if divisor == 0.0 {
                    return Err(Error::new_spanned(value, "multiple_of cannot be zero"));
                }
                multiple_of = Some(divisor);
            } else if path.is_ident("min_items") {
                let value: Lit = meta.value()?.parse()?;
                min_items = Some(parse_usize_lit(&value)?);
            } else if path.is_ident("max_items") {
                let value: Lit = meta.value()?.parse()?;
                max_items = Some(parse_usize_lit(&value)?);
            } else if path.is_ident("unique_items") {
                unique_items = true;
            } else {
                let attr_name = path.to_token_stream().to_string();
                return Err(Error::new_spanned(
                    path,
                    format!(
                        "unknown validate attribute `{attr_name}`. \
                         Valid attributes are: min, max, min_length, max_length, pattern, \
                         required, custom, email, url, uuid, ipv4, ipv6, mac_address, slug, \
                         hex_color, phone, credit_card, multiple_of, min_items, max_items, unique_items"
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
        multiple_of,
        min_items,
        max_items,
        unique_items,
        credit_card,
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
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident == "Option";
    }
    false
}

/// Extract the inner type from `Option<T>`.
#[allow(dead_code)]
fn extract_option_inner(ty: &Type) -> Option<&Type> {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
        && segment.ident == "Option"
        && let PathArguments::AngleBracketed(args) = &segment.arguments
        && let Some(GenericArgument::Type(inner)) = args.args.first()
    {
        return Some(inner);
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
        || field.multiple_of.is_some()
        || field.min_items.is_some()
        || field.max_items.is_some()
        || field.unique_items
        || field.credit_card
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

    // Multiple of check (value % divisor must equal 0)
    if let Some(divisor) = field.multiple_of {
        if is_optional {
            checks.push(quote! {
                if let Some(ref value) = self.#field_name {
                    let value_f64 = *value as f64;
                    let remainder = value_f64 % #divisor;
                    // Account for floating point imprecision
                    if remainder.abs() > 1e-9 && (#divisor - remainder.abs()).abs() > 1e-9 {
                        errors.add_multiple_of(#field_name_str, #divisor, *value);
                    }
                }
            });
        } else {
            checks.push(quote! {
                {
                    let value_f64 = self.#field_name as f64;
                    let remainder = value_f64 % #divisor;
                    // Account for floating point imprecision
                    if remainder.abs() > 1e-9 && (#divisor - remainder.abs()).abs() > 1e-9 {
                        errors.add_multiple_of(#field_name_str, #divisor, self.#field_name);
                    }
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

    // Min items check (for collections)
    if let Some(min_items) = field.min_items {
        if is_optional {
            checks.push(quote! {
                if let Some(ref value) = self.#field_name {
                    let len = value.len();
                    if len < #min_items {
                        errors.add_min_items(#field_name_str, #min_items, len);
                    }
                }
            });
        } else {
            checks.push(quote! {
                {
                    let len = self.#field_name.len();
                    if len < #min_items {
                        errors.add_min_items(#field_name_str, #min_items, len);
                    }
                }
            });
        }
    }

    // Max items check (for collections)
    if let Some(max_items) = field.max_items {
        if is_optional {
            checks.push(quote! {
                if let Some(ref value) = self.#field_name {
                    let len = value.len();
                    if len > #max_items {
                        errors.add_max_items(#field_name_str, #max_items, len);
                    }
                }
            });
        } else {
            checks.push(quote! {
                {
                    let len = self.#field_name.len();
                    if len > #max_items {
                        errors.add_max_items(#field_name_str, #max_items, len);
                    }
                }
            });
        }
    }

    // Unique items check (for collections with Eq + Hash items)
    if field.unique_items {
        if is_optional {
            checks.push(quote! {
                if let Some(ref value) = self.#field_name {
                    let len = value.len();
                    let unique_len = value.iter().collect::<std::collections::HashSet<_>>().len();
                    if len != unique_len {
                        errors.add_unique_items(#field_name_str, len - unique_len);
                    }
                }
            });
        } else {
            checks.push(quote! {
                {
                    let len = self.#field_name.len();
                    let unique_len = self.#field_name.iter().collect::<std::collections::HashSet<_>>().len();
                    if len != unique_len {
                        errors.add_unique_items(#field_name_str, len - unique_len);
                    }
                }
            });
        }
    }

    // Credit card validation (Luhn algorithm check)
    if field.credit_card {
        if is_optional {
            checks.push(quote! {
                if let Some(ref value) = self.#field_name {
                    if !sqlmodel_core::validate::is_valid_credit_card(value.as_ref()) {
                        errors.add_credit_card(#field_name_str);
                    }
                }
            });
        } else {
            checks.push(quote! {
                if !sqlmodel_core::validate::is_valid_credit_card(self.#field_name.as_ref()) {
                    errors.add_credit_card(#field_name_str);
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
            multiple_of: None,
            min_items: None,
            max_items: None,
            unique_items: false,
            credit_card: false,
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
            multiple_of: None,
            min_items: None,
            max_items: None,
            unique_items: false,
            credit_card: false,
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

    // ========================================================================
    // Multiple Of Validation Tests
    // ========================================================================

    #[test]
    fn test_parse_multiple_of_integer() {
        let input: syn::DeriveInput = parse_quote! {
            struct Product {
                #[validate(multiple_of = 5)]
                quantity: i32,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        assert_eq!(def.fields[0].multiple_of, Some(5.0));
    }

    #[test]
    fn test_parse_multiple_of_float() {
        let input: syn::DeriveInput = parse_quote! {
            struct Product {
                #[validate(multiple_of = 0.01)]
                price: f64,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        assert_eq!(def.fields[0].multiple_of, Some(0.01));
    }

    #[test]
    fn test_parse_multiple_of_combined_with_min_max() {
        let input: syn::DeriveInput = parse_quote! {
            struct Product {
                #[validate(min = 0, max = 100, multiple_of = 5)]
                quantity: i32,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        assert_eq!(def.fields[0].min, Some(0.0));
        assert_eq!(def.fields[0].max, Some(100.0));
        assert_eq!(def.fields[0].multiple_of, Some(5.0));
    }

    #[test]
    fn test_parse_multiple_of_zero_fails() {
        // multiple_of = 0 should be rejected
        let input: syn::DeriveInput = parse_quote! {
            struct Invalid {
                #[validate(multiple_of = 0)]
                value: i32,
            }
        };

        let result = parse_validate(&input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cannot be zero"));
    }

    #[test]
    fn test_has_validation_includes_multiple_of() {
        let field_with_multiple_of = ValidateFieldDef {
            name: syn::Ident::new("test", proc_macro2::Span::call_site()),
            ty: syn::parse_quote!(i32),
            min: None,
            max: None,
            min_length: None,
            max_length: None,
            pattern: None,
            required: false,
            custom: None,
            multiple_of: Some(5.0),
            min_items: None,
            max_items: None,
            unique_items: false,
            credit_card: false,
        };
        assert!(has_validation(&field_with_multiple_of));

        let field_without_validation = ValidateFieldDef {
            name: syn::Ident::new("test", proc_macro2::Span::call_site()),
            ty: syn::parse_quote!(i32),
            min: None,
            max: None,
            min_length: None,
            max_length: None,
            pattern: None,
            required: false,
            custom: None,
            multiple_of: None,
            min_items: None,
            max_items: None,
            unique_items: false,
            credit_card: false,
        };
        assert!(!has_validation(&field_without_validation));
    }

    // ========================================================================
    // Collection Validators Tests
    // ========================================================================

    #[test]
    fn test_parse_min_items() {
        let input: syn::DeriveInput = parse_quote! {
            struct Order {
                #[validate(min_items = 1)]
                items: Vec<String>,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        assert_eq!(def.fields[0].min_items, Some(1));
    }

    #[test]
    fn test_parse_max_items() {
        let input: syn::DeriveInput = parse_quote! {
            struct Order {
                #[validate(max_items = 100)]
                items: Vec<String>,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        assert_eq!(def.fields[0].max_items, Some(100));
    }

    #[test]
    fn test_parse_unique_items() {
        let input: syn::DeriveInput = parse_quote! {
            struct Order {
                #[validate(unique_items)]
                items: Vec<String>,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        assert!(def.fields[0].unique_items);
    }

    #[test]
    fn test_parse_all_collection_validators() {
        let input: syn::DeriveInput = parse_quote! {
            struct Order {
                #[validate(min_items = 1, max_items = 100, unique_items)]
                items: Vec<String>,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        assert_eq!(def.fields[0].min_items, Some(1));
        assert_eq!(def.fields[0].max_items, Some(100));
        assert!(def.fields[0].unique_items);
    }

    #[test]
    fn test_has_validation_includes_collection_validators() {
        let field_with_min_items = ValidateFieldDef {
            name: syn::Ident::new("test", proc_macro2::Span::call_site()),
            ty: syn::parse_quote!(Vec<i32>),
            min: None,
            max: None,
            min_length: None,
            max_length: None,
            pattern: None,
            required: false,
            custom: None,
            multiple_of: None,
            min_items: Some(1),
            max_items: None,
            unique_items: false,
            credit_card: false,
        };
        assert!(has_validation(&field_with_min_items));

        let field_with_unique = ValidateFieldDef {
            name: syn::Ident::new("test", proc_macro2::Span::call_site()),
            ty: syn::parse_quote!(Vec<i32>),
            min: None,
            max: None,
            min_length: None,
            max_length: None,
            pattern: None,
            required: false,
            custom: None,
            multiple_of: None,
            min_items: None,
            max_items: None,
            unique_items: true,
            credit_card: false,
        };
        assert!(has_validation(&field_with_unique));
    }

    // ========================================================================
    // Built-in Validators Tests (uuid, ipv4, ipv6, mac_address, slug, etc.)
    // ========================================================================

    #[test]
    fn test_parse_uuid_validator() {
        let input: syn::DeriveInput = parse_quote! {
            struct Device {
                #[validate(uuid)]
                device_id: String,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        assert!(def.fields[0].pattern.is_some());
        let pattern = def.fields[0].pattern.as_ref().unwrap();
        // Pattern should match valid UUIDs
        let re = regex::Regex::new(pattern).unwrap();
        assert!(re.is_match("550e8400-e29b-41d4-a716-446655440000"));
        assert!(!re.is_match("invalid-uuid"));
    }

    #[test]
    fn test_parse_ipv4_validator() {
        let input: syn::DeriveInput = parse_quote! {
            struct Server {
                #[validate(ipv4)]
                ip_address: String,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        assert!(def.fields[0].pattern.is_some());
        let pattern = def.fields[0].pattern.as_ref().unwrap();
        let re = regex::Regex::new(pattern).unwrap();
        assert!(re.is_match("192.168.1.1"));
        assert!(re.is_match("0.0.0.0"));
        assert!(re.is_match("255.255.255.255"));
        assert!(!re.is_match("256.1.1.1"));
        assert!(!re.is_match("192.168.1"));
    }

    #[test]
    fn test_parse_ipv6_validator() {
        let input: syn::DeriveInput = parse_quote! {
            struct Server {
                #[validate(ipv6)]
                ip_address: String,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        assert!(def.fields[0].pattern.is_some());
        let pattern = def.fields[0].pattern.as_ref().unwrap();
        let re = regex::Regex::new(pattern).unwrap();
        assert!(re.is_match("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
        assert!(re.is_match("::1"));
        assert!(re.is_match("::"));
        assert!(!re.is_match("192.168.1.1"));
    }

    #[test]
    fn test_parse_mac_address_validator() {
        let input: syn::DeriveInput = parse_quote! {
            struct Device {
                #[validate(mac_address)]
                mac: String,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        assert!(def.fields[0].pattern.is_some());
        let pattern = def.fields[0].pattern.as_ref().unwrap();
        let re = regex::Regex::new(pattern).unwrap();
        assert!(re.is_match("00:1A:2B:3C:4D:5E"));
        assert!(re.is_match("00-1A-2B-3C-4D-5E"));
        assert!(!re.is_match("00:1A:2B:3C:4D"));
        assert!(!re.is_match("invalid"));
    }

    #[test]
    fn test_parse_slug_validator() {
        let input: syn::DeriveInput = parse_quote! {
            struct Post {
                #[validate(slug)]
                url_slug: String,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        assert!(def.fields[0].pattern.is_some());
        let pattern = def.fields[0].pattern.as_ref().unwrap();
        let re = regex::Regex::new(pattern).unwrap();
        assert!(re.is_match("hello-world"));
        assert!(re.is_match("post123"));
        assert!(re.is_match("a"));
        assert!(!re.is_match("Hello-World")); // uppercase not allowed
        assert!(!re.is_match("-hello")); // can't start with hyphen
        assert!(!re.is_match("hello-")); // can't end with hyphen
    }

    #[test]
    fn test_parse_hex_color_validator() {
        let input: syn::DeriveInput = parse_quote! {
            struct Theme {
                #[validate(hex_color)]
                primary_color: String,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        assert!(def.fields[0].pattern.is_some());
        let pattern = def.fields[0].pattern.as_ref().unwrap();
        let re = regex::Regex::new(pattern).unwrap();
        assert!(re.is_match("#fff"));
        assert!(re.is_match("#FFF"));
        assert!(re.is_match("#ffffff"));
        assert!(re.is_match("#FF00FF"));
        assert!(!re.is_match("fff"));
        assert!(!re.is_match("#ffff"));
        assert!(!re.is_match("#gggggg"));
    }

    #[test]
    fn test_parse_phone_validator() {
        let input: syn::DeriveInput = parse_quote! {
            struct Contact {
                #[validate(phone)]
                phone_number: String,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        assert!(def.fields[0].pattern.is_some());
        let pattern = def.fields[0].pattern.as_ref().unwrap();
        let re = regex::Regex::new(pattern).unwrap();
        assert!(re.is_match("+12025551234"));
        assert!(re.is_match("12025551234"));
        assert!(!re.is_match("0123456789")); // can't start with 0
        assert!(!re.is_match("+0123456789")); // can't start with 0 after +
    }

    #[test]
    fn test_parse_credit_card_validator() {
        let input: syn::DeriveInput = parse_quote! {
            struct Payment {
                #[validate(credit_card)]
                card_number: String,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        // credit_card uses runtime validation, not regex
        assert!(def.fields[0].pattern.is_none());
        assert!(def.fields[0].credit_card);
    }

    #[test]
    fn test_has_validation_includes_credit_card() {
        let field_with_credit_card = ValidateFieldDef {
            name: syn::Ident::new("test", proc_macro2::Span::call_site()),
            ty: syn::parse_quote!(String),
            min: None,
            max: None,
            min_length: None,
            max_length: None,
            pattern: None,
            required: false,
            custom: None,
            multiple_of: None,
            min_items: None,
            max_items: None,
            unique_items: false,
            credit_card: true,
        };
        assert!(has_validation(&field_with_credit_card));
    }

    #[test]
    fn test_parse_multiple_builtin_validators() {
        let input: syn::DeriveInput = parse_quote! {
            struct Network {
                #[validate(uuid)]
                device_id: String,
                #[validate(ipv4)]
                ipv4_addr: String,
                #[validate(mac_address)]
                mac: String,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 3);
        assert!(def.fields[0].pattern.is_some()); // uuid
        assert!(def.fields[1].pattern.is_some()); // ipv4
        assert!(def.fields[2].pattern.is_some()); // mac_address
    }

    #[test]
    fn test_builtin_validator_with_other_constraints() {
        let input: syn::DeriveInput = parse_quote! {
            struct User {
                #[validate(uuid, min_length = 36, max_length = 36)]
                user_id: String,
            }
        };

        let def = parse_validate(&input).unwrap();
        assert_eq!(def.fields.len(), 1);
        assert!(def.fields[0].pattern.is_some()); // uuid pattern
        assert_eq!(def.fields[0].min_length, Some(36));
        assert_eq!(def.fields[0].max_length, Some(36));
    }
}
