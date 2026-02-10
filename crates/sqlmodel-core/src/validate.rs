//! Runtime validation helpers for SQLModel.
//!
//! This module provides validation functions that can be called from
//! generated validation code (via the `#[derive(Validate)]` macro).
//!
//! It also provides `model_validate()` functionality for creating and
//! validating models from various input types (similar to Pydantic).

use std::collections::HashMap;
use std::sync::OnceLock;

use regex::Regex;
use serde::de::DeserializeOwned;

use crate::Value;
use crate::error::{ValidationError, ValidationErrorKind};

/// Thread-safe regex cache for compiled patterns.
///
/// This avoids recompiling regex patterns on every validation call.
/// Patterns are compiled lazily on first use and cached for the lifetime
/// of the program.
struct RegexCache {
    cache: std::sync::RwLock<std::collections::HashMap<String, Regex>>,
}

impl RegexCache {
    fn new() -> Self {
        Self {
            cache: std::sync::RwLock::new(std::collections::HashMap::new()),
        }
    }

    fn get_or_compile(&self, pattern: &str) -> Result<Regex, regex::Error> {
        // Fast path: check if already cached
        // Use unwrap_or_else to recover from poisoned lock (another thread panicked)
        {
            let cache = self.cache.read().unwrap_or_else(|e| e.into_inner());
            if let Some(regex) = cache.get(pattern) {
                return Ok(regex.clone());
            }
        }

        // Slow path: compile and cache
        let regex = Regex::new(pattern)?;
        {
            let mut cache = self.cache.write().unwrap_or_else(|e| e.into_inner());
            cache.insert(pattern.to_string(), regex.clone());
        }
        Ok(regex)
    }
}

/// Global regex cache singleton.
fn regex_cache() -> &'static RegexCache {
    static CACHE: OnceLock<RegexCache> = OnceLock::new();
    CACHE.get_or_init(RegexCache::new)
}

/// Check if a string matches a regex pattern.
///
/// This function is designed to be called from generated validation code.
/// It caches compiled regex patterns for efficiency.
///
/// # Arguments
///
/// * `value` - The string to validate
/// * `pattern` - The regex pattern to match against
///
/// # Returns
///
/// `true` if the value matches the pattern, `false` otherwise.
/// Returns `false` if the pattern is invalid (logs a warning).
///
/// # Example
///
/// ```ignore
/// use sqlmodel_core::validate::matches_pattern;
///
/// assert!(matches_pattern("test@example.com", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"));
/// assert!(!matches_pattern("invalid", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"));
/// ```
pub fn matches_pattern(value: &str, pattern: &str) -> bool {
    match regex_cache().get_or_compile(pattern) {
        Ok(regex) => regex.is_match(value),
        Err(e) => {
            // Log the error but don't panic - validation should be resilient
            tracing::warn!(
                pattern = pattern,
                error = %e,
                "Invalid regex pattern in validation, treating as non-match"
            );
            false
        }
    }
}

/// Validate a regex pattern at compile time (for use in proc macros).
///
/// Returns an error message if the pattern is invalid, None if valid.
pub fn validate_pattern(pattern: &str) -> Option<String> {
    match Regex::new(pattern) {
        Ok(_) => None,
        Err(e) => Some(format!("invalid regex pattern: {e}")),
    }
}

// ============================================================================
// Built-in Validators
// ============================================================================

/// Validate a credit card number using the Luhn algorithm.
///
/// The Luhn algorithm (also known as the "modulus 10" algorithm) is a simple
/// checksum formula used to validate identification numbers such as credit card
/// numbers, IMEI numbers, and others.
///
/// # Algorithm
///
/// 1. Starting from the rightmost digit (check digit) and moving left,
///    double the value of every second digit.
/// 2. If the result of doubling is greater than 9, subtract 9.
/// 3. Sum all the digits.
/// 4. The total modulo 10 must equal 0.
///
/// # Arguments
///
/// * `value` - The credit card number as a string (may contain spaces or hyphens)
///
/// # Returns
///
/// `true` if the number is valid according to the Luhn algorithm, `false` otherwise.
///
/// # Example
///
/// ```ignore
/// use sqlmodel_core::validate::is_valid_credit_card;
///
/// assert!(is_valid_credit_card("4539578763621486"));  // Valid Visa
/// assert!(is_valid_credit_card("4539-5787-6362-1486")); // With dashes
/// assert!(is_valid_credit_card("4539 5787 6362 1486")); // With spaces
/// assert!(!is_valid_credit_card("1234567890123456")); // Invalid
/// ```
pub fn is_valid_credit_card(value: &str) -> bool {
    // Remove all non-digit characters (spaces, hyphens, etc.)
    let digits: Vec<u32> = value
        .chars()
        .filter(|c| c.is_ascii_digit())
        .filter_map(|c| c.to_digit(10))
        .collect();

    // Credit card numbers are typically 13-19 digits
    if digits.len() < 13 || digits.len() > 19 {
        return false;
    }

    // Luhn algorithm
    let mut sum = 0u32;
    let len = digits.len();

    for (i, &digit) in digits.iter().enumerate() {
        // Count from right: rightmost is position 1 (odd)
        // We double every second digit starting from the second-to-last
        let position_from_right = len - i;
        let is_double_position = position_from_right % 2 == 0;

        let value = if is_double_position {
            let doubled = digit * 2;
            if doubled > 9 { doubled - 9 } else { doubled }
        } else {
            digit
        };

        sum += value;
    }

    sum % 10 == 0
}

// ============================================================================
// Model Validation (model_validate)
// ============================================================================

/// Input types for model_validate().
///
/// Supports creating models from various input formats.
#[derive(Debug, Clone)]
pub enum ValidateInput {
    /// A HashMap of field names to values.
    Dict(HashMap<String, Value>),
    /// A JSON string to parse.
    Json(String),
    /// A serde_json::Value for direct deserialization.
    JsonValue(serde_json::Value),
}

impl From<HashMap<String, Value>> for ValidateInput {
    fn from(map: HashMap<String, Value>) -> Self {
        ValidateInput::Dict(map)
    }
}

impl From<String> for ValidateInput {
    fn from(json: String) -> Self {
        ValidateInput::Json(json)
    }
}

impl From<&str> for ValidateInput {
    fn from(json: &str) -> Self {
        ValidateInput::Json(json.to_string())
    }
}

impl From<serde_json::Value> for ValidateInput {
    fn from(value: serde_json::Value) -> Self {
        ValidateInput::JsonValue(value)
    }
}

/// Options for model_validate().
///
/// Controls the validation behavior.
#[derive(Debug, Clone, Default)]
pub struct ValidateOptions {
    /// If true, use strict type coercion (no implicit conversions).
    pub strict: bool,
    /// If true, read from object attributes (ORM mode).
    /// Currently unused - reserved for future from_attributes support.
    pub from_attributes: bool,
    /// Optional context dictionary passed to custom validators.
    pub context: Option<HashMap<String, serde_json::Value>>,
    /// Additional values to merge into the result after parsing.
    pub update: Option<HashMap<String, serde_json::Value>>,
}

impl ValidateOptions {
    /// Create new default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable strict mode (no implicit type conversions).
    pub fn strict(mut self) -> Self {
        self.strict = true;
        self
    }

    /// Enable from_attributes mode (read from object attributes).
    pub fn from_attributes(mut self) -> Self {
        self.from_attributes = true;
        self
    }

    /// Set context for custom validators.
    pub fn with_context(mut self, context: HashMap<String, serde_json::Value>) -> Self {
        self.context = Some(context);
        self
    }

    /// Set values to merge into result.
    pub fn with_update(mut self, update: HashMap<String, serde_json::Value>) -> Self {
        self.update = Some(update);
        self
    }
}

/// Result type for model_validate operations.
pub type ValidateResult<T> = std::result::Result<T, ValidationError>;

/// Trait for models that support model_validate().
///
/// This is typically implemented via derive macro or blanket impl
/// for models that implement Deserialize.
pub trait ModelValidate: Sized {
    /// Create and validate a model from input.
    ///
    /// # Arguments
    ///
    /// * `input` - The input to validate (Dict, Json, or JsonValue)
    /// * `options` - Validation options
    ///
    /// # Returns
    ///
    /// The validated model or validation errors.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use sqlmodel_core::validate::{ModelValidate, ValidateInput, ValidateOptions};
    ///
    /// let user = User::model_validate(
    ///     r#"{"name": "Alice", "age": 30}"#,
    ///     ValidateOptions::default()
    /// )?;
    /// ```
    fn model_validate(
        input: impl Into<ValidateInput>,
        options: ValidateOptions,
    ) -> ValidateResult<Self>;

    /// Create and validate a model from JSON string with default options.
    fn model_validate_json(json: &str) -> ValidateResult<Self> {
        Self::model_validate(json, ValidateOptions::default())
    }

    /// Create and validate a model from a HashMap with default options.
    fn model_validate_dict(dict: HashMap<String, Value>) -> ValidateResult<Self> {
        Self::model_validate(dict, ValidateOptions::default())
    }
}

/// Blanket implementation of ModelValidate for types that implement DeserializeOwned.
///
/// This provides model_validate() for any model that can be deserialized from JSON.
impl<T: DeserializeOwned> ModelValidate for T {
    fn model_validate(
        input: impl Into<ValidateInput>,
        options: ValidateOptions,
    ) -> ValidateResult<Self> {
        let input = input.into();

        // Convert input to serde_json::Value
        let mut json_value = match input {
            ValidateInput::Dict(dict) => {
                // Convert HashMap<String, Value> to serde_json::Value
                let map: serde_json::Map<String, serde_json::Value> = dict
                    .into_iter()
                    .map(|(k, v)| (k, value_to_json(v)))
                    .collect();
                serde_json::Value::Object(map)
            }
            ValidateInput::Json(json_str) => serde_json::from_str(&json_str).map_err(|e| {
                let mut err = ValidationError::new();
                err.add(
                    "_json",
                    ValidationErrorKind::Custom,
                    format!("Invalid JSON: {e}"),
                );
                err
            })?,
            ValidateInput::JsonValue(value) => value,
        };

        // Apply update values if provided
        if let Some(update) = options.update {
            if let serde_json::Value::Object(ref mut map) = json_value {
                for (key, value) in update {
                    map.insert(key, value);
                }
            }
        }

        // Deserialize with appropriate strictness
        if options.strict {
            // In strict mode, we use serde's strict deserialization
            // (default behavior - no implicit conversions)
            serde_json::from_value(json_value).map_err(|e| {
                let mut err = ValidationError::new();
                err.add(
                    "_model",
                    ValidationErrorKind::Custom,
                    format!("Validation failed: {e}"),
                );
                err
            })
        } else {
            // Non-strict mode - same for now, but could add coercion logic
            serde_json::from_value(json_value).map_err(|e| {
                let mut err = ValidationError::new();
                err.add(
                    "_model",
                    ValidationErrorKind::Custom,
                    format!("Validation failed: {e}"),
                );
                err
            })
        }
    }
}

// ============================================================================
// Model Dump (model_dump)
// ============================================================================

/// Output mode for model_dump().
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DumpMode {
    /// JSON-compatible types (strings, numbers, booleans, null)
    #[default]
    Json,
    /// Rust native types (preserves Value variants)
    Python,
}

/// Options for model_dump() and model_dump_json().
///
/// Controls the serialization behavior.
#[derive(Debug, Clone, Default)]
pub struct DumpOptions {
    /// Output mode: Json or Python (Rust native).
    ///
    /// In Python/Pydantic, `mode='json'` produces JSON-compatible values while
    /// `mode='python'` preserves native Python types (e.g., datetime objects).
    ///
    /// In this crate, `model_dump()`/`sql_model_dump()` return `serde_json::Value`,
    /// so only JSON output is supported. Non-JSON modes are rejected at runtime.
    pub mode: DumpMode,
    /// Only include these fields (if Some)
    pub include: Option<std::collections::HashSet<String>>,
    /// Exclude these fields
    pub exclude: Option<std::collections::HashSet<String>>,
    /// Use field aliases in output.
    ///
    /// When true, `sql_model_dump()` will rename fields to their
    /// `serialization_alias` (or `alias` as fallback) in the output.
    pub by_alias: bool,
    /// Exclude fields that were not explicitly set.
    ///
    /// In Pydantic, this depends on tracking which fields were explicitly
    /// provided at construction time (distinct from `exclude_defaults`).
    ///
    /// Rust structs do not retain "field set" metadata by default, so this
    /// option is rejected at runtime to avoid silently producing incorrect output.
    ///
    /// Pydantic semantics: If a field has a default value and the user explicitly
    /// sets it to that default, it should still be included (it was "set").
    /// With `exclude_defaults`, it would be excluded regardless of whether
    /// it was explicitly provided.
    pub exclude_unset: bool,
    /// Exclude fields with default values
    pub exclude_defaults: bool,
    /// Exclude fields with None/null values
    pub exclude_none: bool,
    /// Exclude computed fields (for future computed_field support)
    pub exclude_computed_fields: bool,
    /// Enable round-trip mode (preserves types for re-parsing).
    ///
    /// Pydantic can alter serialization to ensure a dump can be fed back into
    /// validation and reproduce the same model. This crate does not currently
    /// implement a tagged encoding for round-trip fidelity, so this option is
    /// rejected at runtime.
    pub round_trip: bool,
    /// Indentation for JSON output (None = compact, Some(n) = n spaces)
    pub indent: Option<usize>,
}

impl DumpOptions {
    /// Create new default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set output mode to JSON.
    pub fn json(mut self) -> Self {
        self.mode = DumpMode::Json;
        self
    }

    /// Set output mode to Python (Rust native).
    pub fn python(mut self) -> Self {
        self.mode = DumpMode::Python;
        self
    }

    /// Set fields to include.
    pub fn include(mut self, fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.include = Some(fields.into_iter().map(Into::into).collect());
        self
    }

    /// Set fields to exclude.
    pub fn exclude(mut self, fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.exclude = Some(fields.into_iter().map(Into::into).collect());
        self
    }

    /// Enable by_alias mode.
    pub fn by_alias(mut self) -> Self {
        self.by_alias = true;
        self
    }

    /// Enable exclude_unset mode.
    pub fn exclude_unset(mut self) -> Self {
        self.exclude_unset = true;
        self
    }

    /// Enable exclude_defaults mode.
    pub fn exclude_defaults(mut self) -> Self {
        self.exclude_defaults = true;
        self
    }

    /// Enable exclude_none mode.
    pub fn exclude_none(mut self) -> Self {
        self.exclude_none = true;
        self
    }

    /// Enable exclude_computed_fields mode.
    pub fn exclude_computed_fields(mut self) -> Self {
        self.exclude_computed_fields = true;
        self
    }

    /// Enable round_trip mode.
    pub fn round_trip(mut self) -> Self {
        self.round_trip = true;
        self
    }

    /// Set indentation for JSON output.
    ///
    /// When set, JSON output will be pretty-printed with the specified number
    /// of spaces for indentation. When None (default), JSON is compact.
    pub fn indent(mut self, spaces: usize) -> Self {
        self.indent = Some(spaces);
        self
    }
}

/// Result type for model_dump operations.
pub type DumpResult = std::result::Result<serde_json::Value, serde_json::Error>;

pub(crate) fn dump_options_unsupported(msg: impl Into<String>) -> serde_json::Error {
    serde_json::Error::io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        msg.into(),
    ))
}

/// Trait for models that support model_dump().
///
/// This is typically implemented via blanket impl for models that implement Serialize.
pub trait ModelDump {
    /// Serialize a model to a JSON value.
    ///
    /// # Arguments
    ///
    /// * `options` - Dump options controlling serialization behavior
    ///
    /// # Returns
    ///
    /// A serde_json::Value representing the serialized model.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use sqlmodel_core::validate::{ModelDump, DumpOptions};
    ///
    /// let json = user.model_dump(DumpOptions::default())?;
    /// ```
    fn model_dump(&self, options: DumpOptions) -> DumpResult;

    /// Serialize a model to a JSON string with default options.
    fn model_dump_json(&self) -> std::result::Result<String, serde_json::Error> {
        let value = self.model_dump(DumpOptions::default())?;
        serde_json::to_string(&value)
    }

    /// Serialize a model to a pretty-printed JSON string.
    fn model_dump_json_pretty(&self) -> std::result::Result<String, serde_json::Error> {
        let value = self.model_dump(DumpOptions::default())?;
        serde_json::to_string_pretty(&value)
    }

    /// Serialize a model to a JSON string with full options support.
    ///
    /// This method supports all DumpOptions including the `indent` option:
    /// - `indent: None` - compact JSON output
    /// - `indent: Some(n)` - pretty-printed with n spaces indentation
    ///
    /// # Example
    ///
    /// ```ignore
    /// use sqlmodel_core::validate::{ModelDump, DumpOptions};
    ///
    /// // Compact JSON with exclusions
    /// let json = user.model_dump_json_with_options(
    ///     DumpOptions::default().exclude(["password"])
    /// )?;
    ///
    /// // Pretty-printed with 4-space indent
    /// let json = user.model_dump_json_with_options(
    ///     DumpOptions::default().indent(4)
    /// )?;
    /// ```
    fn model_dump_json_with_options(
        &self,
        options: DumpOptions,
    ) -> std::result::Result<String, serde_json::Error> {
        let value = self.model_dump(DumpOptions {
            indent: None, // Don't pass indent to model_dump (it returns Value, not String)
            ..options.clone()
        })?;

        match options.indent {
            Some(spaces) => {
                let indent_bytes = " ".repeat(spaces).into_bytes();
                let formatter = serde_json::ser::PrettyFormatter::with_indent(&indent_bytes);
                let mut writer = Vec::new();
                let mut ser = serde_json::Serializer::with_formatter(&mut writer, formatter);
                serde::Serialize::serialize(&value, &mut ser)?;
                // serde_json always produces valid UTF-8, but propagate error instead of panicking
                String::from_utf8(writer).map_err(|e| {
                    serde_json::Error::io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("UTF-8 encoding error: {e}"),
                    ))
                })
            }
            None => serde_json::to_string(&value),
        }
    }
}

/// Blanket implementation of ModelDump for types that implement Serialize.
impl<T: serde::Serialize> ModelDump for T {
    fn model_dump(&self, options: DumpOptions) -> DumpResult {
        if options.mode != DumpMode::Json {
            return Err(dump_options_unsupported(
                "DumpOptions.mode != Json is not supported (output is serde_json::Value)",
            ));
        }
        if options.exclude_unset {
            return Err(dump_options_unsupported(
                "DumpOptions.exclude_unset requires fields_set tracking; use SqlModelValidate::sql_model_validate_tracked(...) or the tracked!(Type { .. }) macro",
            ));
        }
        if options.round_trip {
            return Err(dump_options_unsupported(
                "DumpOptions.round_trip is not supported (tagged round-trip encoding is not available)",
            ));
        }
        if options.by_alias || options.exclude_defaults || options.exclude_computed_fields {
            return Err(dump_options_unsupported(
                "DumpOptions.by_alias/exclude_defaults/exclude_computed_fields require SqlModelDump",
            ));
        }

        // First, serialize to JSON value
        let mut value = serde_json::to_value(self)?;

        // Apply options
        if let serde_json::Value::Object(ref mut map) = value {
            // Apply include filter
            if let Some(ref include) = options.include {
                map.retain(|k, _| include.contains(k));
            }

            // Apply exclude filter
            if let Some(ref exclude) = options.exclude {
                map.retain(|k, _| !exclude.contains(k));
            }

            // Apply exclude_none filter
            if options.exclude_none {
                map.retain(|_, v| !v.is_null());
            }

            // Note: This is the generic ModelDump implementation for Serialize types.
            // Model-aware transforms are implemented by SqlModelDump and rejected above.
        }

        Ok(value)
    }
}

/// Convert a Value to serde_json::Value.
fn value_to_json(value: Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::TinyInt(i) => serde_json::Value::Number(i.into()),
        Value::SmallInt(i) => serde_json::Value::Number(i.into()),
        Value::Int(i) => serde_json::Value::Number(i.into()),
        Value::BigInt(i) => serde_json::Value::Number(i.into()),
        Value::Float(f) => serde_json::Number::from_f64(f64::from(f))
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::Double(f) => serde_json::Number::from_f64(f)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::Decimal(s) => serde_json::Value::String(s),
        Value::Text(s) => serde_json::Value::String(s),
        Value::Bytes(b) => {
            // Encode bytes as hex string
            use std::fmt::Write;
            let hex = b
                .iter()
                .fold(String::with_capacity(b.len() * 2), |mut acc, byte| {
                    let _ = write!(acc, "{byte:02x}");
                    acc
                });
            serde_json::Value::String(hex)
        }
        // Date is i32 (days since epoch) - convert to number
        Value::Date(d) => serde_json::Value::Number(d.into()),
        // Time is i64 (microseconds since midnight)
        Value::Time(t) => serde_json::Value::Number(t.into()),
        // Timestamp is i64 (microseconds since epoch)
        Value::Timestamp(ts) => serde_json::Value::Number(ts.into()),
        // TimestampTz is i64 (microseconds since epoch, UTC)
        Value::TimestampTz(ts) => serde_json::Value::Number(ts.into()),
        // UUID is [u8; 16] - format as UUID string with dashes
        Value::Uuid(u) => {
            use std::fmt::Write;
            let hex = u.iter().fold(String::with_capacity(32), |mut acc, b| {
                let _ = write!(acc, "{b:02x}");
                acc
            });
            // Format as UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
            let formatted = format!(
                "{}-{}-{}-{}-{}",
                &hex[0..8],
                &hex[8..12],
                &hex[12..16],
                &hex[16..20],
                &hex[20..32]
            );
            serde_json::Value::String(formatted)
        }
        Value::Json(j) => j,
        Value::Array(arr) => serde_json::Value::Array(arr.into_iter().map(value_to_json).collect()),
        Value::Default => serde_json::Value::Null,
    }
}

// ============================================================================
// Alias-Aware Validation and Serialization
// ============================================================================

use crate::Model;

/// Apply validation aliases to JSON input.
///
/// This transforms input keys that match validation_alias or alias to their
/// corresponding field names, enabling deserialization to work correctly.
///
/// # Arguments
///
/// * `json` - The JSON value to transform (modified in place)
/// * `fields` - The field metadata containing alias information
pub fn apply_validation_aliases(json: &mut serde_json::Value, fields: &[crate::FieldInfo]) {
    if let serde_json::Value::Object(map) = json {
        // Build a mapping from alias -> field_name
        let mut alias_map: HashMap<&str, &str> = HashMap::new();
        for field in fields {
            // validation_alias takes precedence for input
            if let Some(alias) = field.validation_alias {
                alias_map.insert(alias, field.name);
            }
            // Regular alias also works for input
            if let Some(alias) = field.alias {
                alias_map.entry(alias).or_insert(field.name);
            }
        }

        // Collect keys that need to be renamed
        let renames: Vec<(String, &str)> = map
            .keys()
            .filter_map(|k| alias_map.get(k.as_str()).map(|v| (k.clone(), *v)))
            .collect();

        // Apply renames
        for (old_key, new_key) in renames {
            if let Some(value) = map.remove(&old_key) {
                // Only insert if the target key doesn't already exist
                map.entry(new_key.to_string()).or_insert(value);
            }
        }
    }
}

/// Apply serialization aliases to JSON output.
///
/// This transforms output keys from field names to their serialization_alias
/// or alias, enabling proper JSON output format.
///
/// # Arguments
///
/// * `json` - The JSON value to transform (modified in place)
/// * `fields` - The field metadata containing alias information
pub fn apply_serialization_aliases(json: &mut serde_json::Value, fields: &[crate::FieldInfo]) {
    if let serde_json::Value::Object(map) = json {
        // Build a mapping from field_name -> output_alias
        let mut alias_map: HashMap<&str, &str> = HashMap::new();
        for field in fields {
            // serialization_alias takes precedence for output
            if let Some(alias) = field.serialization_alias {
                alias_map.insert(field.name, alias);
            } else if let Some(alias) = field.alias {
                // Regular alias is fallback for output
                alias_map.insert(field.name, alias);
            }
        }

        // Collect keys that need to be renamed
        let renames: Vec<(String, &str)> = map
            .keys()
            .filter_map(|k| alias_map.get(k.as_str()).map(|v| (k.clone(), *v)))
            .collect();

        // Apply renames
        for (old_key, new_key) in renames {
            if let Some(value) = map.remove(&old_key) {
                map.insert(new_key.to_string(), value);
            }
        }
    }
}

/// Model-aware validation that supports field aliases.
///
/// Unlike the generic `ModelValidate`, this trait uses the `Model::fields()`
/// metadata to transform aliased input keys to their actual field names
/// before deserialization.
///
/// # Example
///
/// ```ignore
/// #[derive(Model, Serialize, Deserialize)]
/// struct User {
///     #[sqlmodel(validation_alias = "userName")]
///     name: String,
/// }
///
/// // Input with alias key works
/// let user = User::sql_model_validate(r#"{"userName": "Alice"}"#)?;
/// assert_eq!(user.name, "Alice");
/// ```
pub trait SqlModelValidate: Model + DeserializeOwned + Sized {
    /// Create and validate a model from input, applying validation aliases.
    fn sql_model_validate(
        input: impl Into<ValidateInput>,
        options: ValidateOptions,
    ) -> ValidateResult<Self> {
        let input = input.into();

        // Convert input to serde_json::Value
        let mut json_value = match input {
            ValidateInput::Dict(dict) => {
                let map: serde_json::Map<String, serde_json::Value> = dict
                    .into_iter()
                    .map(|(k, v)| (k, value_to_json(v)))
                    .collect();
                serde_json::Value::Object(map)
            }
            ValidateInput::Json(json_str) => serde_json::from_str(&json_str).map_err(|e| {
                let mut err = ValidationError::new();
                err.add(
                    "_json",
                    ValidationErrorKind::Custom,
                    format!("Invalid JSON: {e}"),
                );
                err
            })?,
            ValidateInput::JsonValue(value) => value,
        };

        // Apply validation aliases before deserialization
        apply_validation_aliases(&mut json_value, Self::fields());

        // Apply update values if provided
        if let Some(update) = options.update {
            if let serde_json::Value::Object(ref mut map) = json_value {
                for (key, value) in update {
                    map.insert(key, value);
                }
            }
        }

        // Deserialize
        serde_json::from_value(json_value).map_err(|e| {
            let mut err = ValidationError::new();
            err.add(
                "_model",
                ValidationErrorKind::Custom,
                format!("Validation failed: {e}"),
            );
            err
        })
    }

    /// Create and validate a model from input, also tracking which fields were explicitly set.
    ///
    /// This enables Pydantic-compatible `exclude_unset` behavior when dumping via `TrackedModel`.
    fn sql_model_validate_tracked(
        input: impl Into<ValidateInput>,
        options: ValidateOptions,
    ) -> ValidateResult<crate::TrackedModel<Self>> {
        let input = input.into();

        let mut json_value = match input {
            ValidateInput::Dict(dict) => {
                let map: serde_json::Map<String, serde_json::Value> = dict
                    .into_iter()
                    .map(|(k, v)| (k, value_to_json(v)))
                    .collect();
                serde_json::Value::Object(map)
            }
            ValidateInput::Json(json_str) => serde_json::from_str(&json_str).map_err(|e| {
                let mut err = ValidationError::new();
                err.add(
                    "_json",
                    ValidationErrorKind::Custom,
                    format!("Invalid JSON: {e}"),
                );
                err
            })?,
            ValidateInput::JsonValue(value) => value,
        };

        apply_validation_aliases(&mut json_value, Self::fields());

        if let Some(update) = options.update {
            if let serde_json::Value::Object(ref mut map) = json_value {
                for (key, value) in update {
                    map.insert(key, value);
                }
            }
        }

        // Compute fields_set from the (post-alias) object keys.
        let mut fields_set = crate::FieldsSet::empty(Self::fields().len());
        if let serde_json::Value::Object(ref map) = json_value {
            for (idx, field) in Self::fields().iter().enumerate() {
                if map.contains_key(field.name) {
                    fields_set.set(idx);
                }
            }
        }

        let model = serde_json::from_value(json_value).map_err(|e| {
            let mut err = ValidationError::new();
            err.add(
                "_model",
                ValidationErrorKind::Custom,
                format!("Validation failed: {e}"),
            );
            err
        })?;

        Ok(crate::TrackedModel::new(model, fields_set))
    }

    /// Create and validate a model from JSON string with default options.
    fn sql_model_validate_json(json: &str) -> ValidateResult<Self> {
        Self::sql_model_validate(json, ValidateOptions::default())
    }

    /// Create and validate a model from a HashMap with default options.
    fn sql_model_validate_dict(dict: HashMap<String, Value>) -> ValidateResult<Self> {
        Self::sql_model_validate(dict, ValidateOptions::default())
    }
}

/// Blanket implementation for all Model types that implement DeserializeOwned.
impl<T: Model + DeserializeOwned> SqlModelValidate for T {}

/// Model-aware dump that supports field aliases and computed field exclusion.
///
/// Unlike the generic `ModelDump`, this trait uses the `Model::fields()`
/// metadata to transform field names to their serialization aliases
/// in the output and to handle computed fields properly.
///
/// # Example
///
/// ```ignore
/// #[derive(Model, Serialize, Deserialize)]
/// struct User {
///     #[sqlmodel(serialization_alias = "userName")]
///     name: String,
///     #[sqlmodel(computed)]
///     full_name: String, // Derived field, not in DB
/// }
///
/// let user = User { name: "Alice".to_string(), full_name: "Alice Smith".to_string() };
/// let json = user.sql_model_dump(DumpOptions::default().by_alias())?;
/// assert_eq!(json["userName"], "Alice");
///
/// // Exclude computed fields
/// let json = user.sql_model_dump(DumpOptions::default().exclude_computed_fields())?;
/// assert!(json.get("full_name").is_none());
/// ```
pub trait SqlModelDump: Model + serde::Serialize {
    /// Serialize a model to a JSON value, optionally applying aliases.
    fn sql_model_dump(&self, options: DumpOptions) -> DumpResult {
        if options.mode != DumpMode::Json {
            return Err(dump_options_unsupported(
                "DumpOptions.mode != Json is not supported (output is serde_json::Value)",
            ));
        }
        if options.exclude_unset {
            return Err(dump_options_unsupported(
                "DumpOptions.exclude_unset requires fields_set tracking; use SqlModelValidate::sql_model_validate_tracked(...) or the tracked!(Type { .. }) macro",
            ));
        }
        if options.round_trip {
            return Err(dump_options_unsupported(
                "DumpOptions.round_trip is not supported (tagged round-trip encoding is not available)",
            ));
        }

        // First, serialize to JSON value
        let mut value = serde_json::to_value(self)?;

        // Apply options that work on original field names BEFORE alias renaming
        if let serde_json::Value::Object(ref mut map) = value {
            // Always honor per-field exclude flag (Pydantic Field(exclude=True) semantics).
            for field in Self::fields() {
                if field.exclude {
                    map.remove(field.name);
                }
            }

            // Exclude computed fields if requested (must happen before alias renaming)
            if options.exclude_computed_fields {
                let computed_field_names: std::collections::HashSet<&str> = Self::fields()
                    .iter()
                    .filter(|f| f.computed)
                    .map(|f| f.name)
                    .collect();
                map.retain(|k, _| !computed_field_names.contains(k.as_str()));
            }

            // Exclude fields with default values if requested
            if options.exclude_defaults {
                for field in Self::fields() {
                    if let Some(default_json) = field.default_json {
                        if let Some(current_value) = map.get(field.name) {
                            // Parse the default JSON and compare
                            if let Ok(default_value) =
                                serde_json::from_str::<serde_json::Value>(default_json)
                            {
                                if current_value == &default_value {
                                    map.remove(field.name);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Apply serialization aliases if by_alias is set
        if options.by_alias {
            apply_serialization_aliases(&mut value, Self::fields());
        }

        // Apply remaining options (include/exclude work on the final key names)
        if let serde_json::Value::Object(ref mut map) = value {
            // Apply include filter
            if let Some(ref include) = options.include {
                map.retain(|k, _| include.contains(k));
            }

            // Apply exclude filter
            if let Some(ref exclude) = options.exclude {
                map.retain(|k, _| !exclude.contains(k));
            }

            // Apply exclude_none filter
            if options.exclude_none {
                map.retain(|_, v| !v.is_null());
            }
        }

        Ok(value)
    }

    /// Serialize a model to a JSON string with default options.
    fn sql_model_dump_json(&self) -> std::result::Result<String, serde_json::Error> {
        let value = self.sql_model_dump(DumpOptions::default())?;
        serde_json::to_string(&value)
    }

    /// Serialize a model to a pretty-printed JSON string.
    fn sql_model_dump_json_pretty(&self) -> std::result::Result<String, serde_json::Error> {
        let value = self.sql_model_dump(DumpOptions::default())?;
        serde_json::to_string_pretty(&value)
    }

    /// Serialize with aliases to a JSON string.
    fn sql_model_dump_json_by_alias(&self) -> std::result::Result<String, serde_json::Error> {
        let value = self.sql_model_dump(DumpOptions::default().by_alias())?;
        serde_json::to_string(&value)
    }

    /// Serialize a model to a JSON string with full options support.
    ///
    /// This method supports all DumpOptions including the `indent` option:
    /// - `indent: None` - compact JSON output
    /// - `indent: Some(n)` - pretty-printed with n spaces indentation
    ///
    /// Compared to `model_dump_json_with_options`, this method also applies
    /// Model-specific transformations like serialization aliases.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use sqlmodel_core::validate::{SqlModelDump, DumpOptions};
    ///
    /// // With aliases and 2-space indent
    /// let json = user.sql_model_dump_json_with_options(
    ///     DumpOptions::default().by_alias().indent(2)
    /// )?;
    /// ```
    fn sql_model_dump_json_with_options(
        &self,
        options: DumpOptions,
    ) -> std::result::Result<String, serde_json::Error> {
        let value = self.sql_model_dump(DumpOptions {
            indent: None, // Don't pass indent to sql_model_dump (it returns Value, not String)
            ..options.clone()
        })?;

        match options.indent {
            Some(spaces) => {
                let indent_bytes = " ".repeat(spaces).into_bytes();
                let formatter = serde_json::ser::PrettyFormatter::with_indent(&indent_bytes);
                let mut writer = Vec::new();
                let mut ser = serde_json::Serializer::with_formatter(&mut writer, formatter);
                serde::Serialize::serialize(&value, &mut ser)?;
                // serde_json always produces valid UTF-8, but propagate error instead of panicking
                String::from_utf8(writer).map_err(|e| {
                    serde_json::Error::io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("UTF-8 encoding error: {e}"),
                    ))
                })
            }
            None => serde_json::to_string(&value),
        }
    }
}

/// Blanket implementation for all Model types that implement Serialize.
impl<T: Model + serde::Serialize> SqlModelDump for T {}

// ============================================================================
// Model Update (sqlmodel_update)
// ============================================================================

/// Input types for sqlmodel_update().
///
/// Supports updating models from various input formats.
#[derive(Debug, Clone)]
pub enum UpdateInput {
    /// A HashMap of field names to JSON values.
    Dict(HashMap<String, serde_json::Value>),
    /// A serde_json::Value for direct updating.
    JsonValue(serde_json::Value),
}

impl From<HashMap<String, serde_json::Value>> for UpdateInput {
    fn from(map: HashMap<String, serde_json::Value>) -> Self {
        UpdateInput::Dict(map)
    }
}

impl From<serde_json::Value> for UpdateInput {
    fn from(value: serde_json::Value) -> Self {
        UpdateInput::JsonValue(value)
    }
}

impl From<HashMap<String, Value>> for UpdateInput {
    fn from(map: HashMap<String, Value>) -> Self {
        let json_map: HashMap<String, serde_json::Value> = map
            .into_iter()
            .map(|(k, v)| (k, value_to_json(v)))
            .collect();
        UpdateInput::Dict(json_map)
    }
}

/// Options for sqlmodel_update().
#[derive(Debug, Clone, Default)]
pub struct UpdateOptions {
    /// Only update these fields (if Some). Other fields in the source are ignored.
    pub update_fields: Option<std::collections::HashSet<String>>,
}

impl UpdateOptions {
    /// Create new default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set fields to update.
    pub fn update_fields(mut self, fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.update_fields = Some(fields.into_iter().map(Into::into).collect());
        self
    }
}

/// Trait for models that support sqlmodel_update().
///
/// This enables updating a model instance from a dictionary or another model's values.
///
/// # Example
///
/// ```ignore
/// use sqlmodel_core::validate::{SqlModelUpdate, UpdateInput, UpdateOptions};
///
/// let mut user = User { id: 1, name: "Alice".to_string(), age: 30 };
///
/// // Update from a HashMap
/// user.sqlmodel_update(
///     HashMap::from([("name".to_string(), serde_json::json!("Bob"))]),
///     UpdateOptions::default()
/// )?;
/// assert_eq!(user.name, "Bob");
///
/// // Update only specific fields
/// user.sqlmodel_update(
///     HashMap::from([
///         ("name".to_string(), serde_json::json!("Carol")),
///         ("age".to_string(), serde_json::json!(25))
///     ]),
///     UpdateOptions::default().update_fields(["name"])
/// )?;
/// assert_eq!(user.name, "Carol");
/// assert_eq!(user.age, 30); // age was not updated
/// ```
pub trait SqlModelUpdate: Model + serde::Serialize + DeserializeOwned {
    /// Update a model instance from input.
    ///
    /// This method merges values from the input into the current model.
    /// Only fields present in the input (and allowed by `update_fields` option)
    /// are updated.
    ///
    /// # Arguments
    ///
    /// * `input` - The source of update values (Dict or JsonValue)
    /// * `options` - Update options controlling which fields to update
    ///
    /// # Returns
    ///
    /// Ok(()) if the update succeeds, or a validation error if the resulting
    /// model fails validation.
    fn sqlmodel_update(
        &mut self,
        input: impl Into<UpdateInput>,
        options: UpdateOptions,
    ) -> ValidateResult<()> {
        let input = input.into();

        // Convert input to a map
        let update_map = match input {
            UpdateInput::Dict(map) => map,
            UpdateInput::JsonValue(value) => {
                if let serde_json::Value::Object(map) = value {
                    map.into_iter().collect()
                } else {
                    let mut err = ValidationError::new();
                    err.add(
                        "_update",
                        ValidationErrorKind::Custom,
                        "Update input must be an object".to_string(),
                    );
                    return Err(err);
                }
            }
        };

        // Serialize current model to JSON
        let mut current = serde_json::to_value(&*self).map_err(|e| {
            let mut err = ValidationError::new();
            err.add(
                "_model",
                ValidationErrorKind::Custom,
                format!("Failed to serialize model: {e}"),
            );
            err
        })?;

        // Get valid field names from model metadata
        let valid_fields: std::collections::HashSet<&str> =
            Self::fields().iter().map(|f| f.name).collect();

        // Update the current JSON with new values
        if let serde_json::Value::Object(ref mut current_map) = current {
            for (key, value) in update_map {
                // Check if field is valid
                if !valid_fields.contains(key.as_str()) {
                    let mut err = ValidationError::new();
                    err.add(
                        &key,
                        ValidationErrorKind::Custom,
                        format!("Unknown field: {key}"),
                    );
                    return Err(err);
                }

                // Check if field is allowed by update_fields option
                if let Some(ref allowed) = options.update_fields {
                    if !allowed.contains(&key) {
                        continue; // Skip fields not in update_fields
                    }
                }

                // Update the field
                current_map.insert(key, value);
            }
        }

        // Deserialize back to model (this also validates)
        let updated: Self = serde_json::from_value(current).map_err(|e| {
            let mut err = ValidationError::new();
            err.add(
                "_model",
                ValidationErrorKind::Custom,
                format!("Update failed validation: {e}"),
            );
            err
        })?;

        // Replace self with the updated model
        *self = updated;

        Ok(())
    }

    /// Update a model instance from a HashMap with default options.
    fn sqlmodel_update_dict(
        &mut self,
        dict: HashMap<String, serde_json::Value>,
    ) -> ValidateResult<()> {
        self.sqlmodel_update(dict, UpdateOptions::default())
    }

    /// Copy non-None/non-null values from another model into this one.
    ///
    /// This is useful for partial updates where you have a "patch" model
    /// with only the fields that should be updated (non-None values).
    ///
    /// # Arguments
    ///
    /// * `source` - The source model to copy values from
    /// * `options` - Update options controlling which fields to update
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut user = User { id: 1, name: "Alice".to_string(), age: Some(30) };
    /// let patch = User { id: 0, name: "Bob".to_string(), age: None };
    ///
    /// // Only update name, skip None age
    /// user.sqlmodel_update_from(&patch, UpdateOptions::default())?;
    /// assert_eq!(user.name, "Bob");
    /// assert_eq!(user.age, Some(30)); // Unchanged because patch.age is None
    /// ```
    fn sqlmodel_update_from(&mut self, source: &Self, options: UpdateOptions) -> ValidateResult<()>
    where
        Self: Sized,
    {
        // Serialize source to JSON
        let source_json = serde_json::to_value(source).map_err(|e| {
            let mut err = ValidationError::new();
            err.add(
                "_source",
                ValidationErrorKind::Custom,
                format!("Failed to serialize source model: {e}"),
            );
            err
        })?;

        // Filter out null values (None fields)
        let update_map: HashMap<String, serde_json::Value> =
            if let serde_json::Value::Object(map) = source_json {
                map.into_iter().filter(|(_, v)| !v.is_null()).collect()
            } else {
                let mut err = ValidationError::new();
                err.add(
                    "_source",
                    ValidationErrorKind::Custom,
                    "Source model must serialize to an object".to_string(),
                );
                return Err(err);
            };

        self.sqlmodel_update(update_map, options)
    }
}

/// Blanket implementation for all Model types that implement Serialize + DeserializeOwned.
impl<T: Model + serde::Serialize + DeserializeOwned> SqlModelUpdate for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn test_matches_email_pattern() {
        let email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$";

        assert!(matches_pattern("test@example.com", email_pattern));
        assert!(matches_pattern("user.name+tag@domain.org", email_pattern));
        assert!(!matches_pattern("invalid", email_pattern));
        assert!(!matches_pattern("@example.com", email_pattern));
        assert!(!matches_pattern("test@", email_pattern));
    }

    #[test]
    fn test_matches_url_pattern() {
        let url_pattern = r"^https?://[^\s/$.?#].[^\s]*$";

        assert!(matches_pattern("https://example.com", url_pattern));
        assert!(matches_pattern("http://example.com/path", url_pattern));
        assert!(!matches_pattern("ftp://example.com", url_pattern));
        assert!(!matches_pattern("not a url", url_pattern));
    }

    #[test]
    fn test_matches_phone_pattern() {
        let phone_pattern = r"^\+?[1-9]\d{1,14}$";

        assert!(matches_pattern("+12025551234", phone_pattern));
        assert!(matches_pattern("12025551234", phone_pattern));
        assert!(!matches_pattern("0123456789", phone_pattern)); // Can't start with 0
        assert!(!matches_pattern("abc", phone_pattern));
    }

    #[test]
    fn test_matches_uuid_pattern() {
        let uuid_pattern =
            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$";

        assert!(matches_pattern(
            "550e8400-e29b-41d4-a716-446655440000",
            uuid_pattern
        ));
        assert!(matches_pattern(
            "550E8400-E29B-41D4-A716-446655440000",
            uuid_pattern
        ));
        assert!(!matches_pattern("invalid-uuid", uuid_pattern));
        assert!(!matches_pattern(
            "550e8400e29b41d4a716446655440000",
            uuid_pattern
        ));
    }

    #[test]
    fn test_matches_alphanumeric_pattern() {
        let alphanumeric_pattern = r"^[a-zA-Z0-9]+$";

        assert!(matches_pattern("abc123", alphanumeric_pattern));
        assert!(matches_pattern("ABC", alphanumeric_pattern));
        assert!(matches_pattern("123", alphanumeric_pattern));
        assert!(!matches_pattern("abc-123", alphanumeric_pattern));
        assert!(!matches_pattern("hello world", alphanumeric_pattern));
    }

    #[test]
    fn test_invalid_pattern_returns_false() {
        // Invalid regex pattern (unclosed bracket)
        let invalid_pattern = r"[unclosed";
        assert!(!matches_pattern("anything", invalid_pattern));
    }

    #[test]
    fn test_validate_pattern_valid() {
        assert!(validate_pattern(r"^[a-z]+$").is_none());
        assert!(validate_pattern(r"^\d{4}-\d{2}-\d{2}$").is_none());
    }

    #[test]
    fn test_validate_pattern_invalid() {
        let result = validate_pattern(r"[unclosed");
        assert!(result.is_some());
        assert!(result.unwrap().contains("invalid regex pattern"));
    }

    #[test]
    fn test_regex_caching() {
        let pattern = r"^test\d+$";

        // First call compiles the regex
        assert!(matches_pattern("test123", pattern));

        // Second call should use cached regex
        assert!(matches_pattern("test456", pattern));
        assert!(!matches_pattern("invalid", pattern));
    }

    #[test]
    fn test_empty_string() {
        let pattern = r"^.+$"; // At least one character
        assert!(!matches_pattern("", pattern));

        let empty_allowed = r"^.*$"; // Zero or more characters
        assert!(matches_pattern("", empty_allowed));
    }

    #[test]
    fn test_special_characters() {
        let pattern = r"^[a-z]+$";
        assert!(!matches_pattern("hello<script>", pattern));
        assert!(!matches_pattern("test'; DROP TABLE users;--", pattern));
    }

    // =========================================================================
    // model_validate tests
    // =========================================================================

    #[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
    struct TestUser {
        name: String,
        age: i32,
        #[serde(default)]
        active: bool,
    }

    #[test]
    fn test_model_validate_from_json() {
        let json = r#"{"name": "Alice", "age": 30}"#;
        let user: TestUser = TestUser::model_validate_json(json).unwrap();
        assert_eq!(user.name, "Alice");
        assert_eq!(user.age, 30);
        assert!(!user.active); // default
    }

    #[test]
    fn test_model_validate_from_json_value() {
        let json_value = serde_json::json!({"name": "Bob", "age": 25, "active": true});
        let user: TestUser =
            TestUser::model_validate(json_value, ValidateOptions::default()).unwrap();
        assert_eq!(user.name, "Bob");
        assert_eq!(user.age, 25);
        assert!(user.active);
    }

    #[test]
    fn test_model_validate_from_dict() {
        let mut dict = HashMap::new();
        dict.insert("name".to_string(), Value::Text("Charlie".to_string()));
        dict.insert("age".to_string(), Value::Int(35));
        dict.insert("active".to_string(), Value::Bool(true));

        let user: TestUser = TestUser::model_validate_dict(dict).unwrap();
        assert_eq!(user.name, "Charlie");
        assert_eq!(user.age, 35);
        assert!(user.active);
    }

    #[test]
    fn test_model_validate_invalid_json() {
        let json = r#"{"name": "Invalid"}"#; // missing required 'age' field
        let result: ValidateResult<TestUser> = TestUser::model_validate_json(json);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(!err.is_empty());
    }

    #[test]
    fn test_model_validate_malformed_json() {
        let json = r#"{"name": "Alice", age: 30}"#; // invalid JSON syntax
        let result: ValidateResult<TestUser> = TestUser::model_validate_json(json);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.errors
                .iter()
                .any(|e| e.message.contains("Invalid JSON"))
        );
    }

    #[test]
    fn test_model_validate_with_update() {
        let json = r#"{"name": "Original", "age": 20}"#;
        let mut update = HashMap::new();
        update.insert("name".to_string(), serde_json::json!("Updated"));

        let options = ValidateOptions::new().with_update(update);
        let user: TestUser = TestUser::model_validate(json, options).unwrap();
        assert_eq!(user.name, "Updated"); // overridden by update
        assert_eq!(user.age, 20);
    }

    #[test]
    fn test_model_validate_strict_mode() {
        let json = r#"{"name": "Alice", "age": 30}"#;
        let options = ValidateOptions::new().strict();
        let user: TestUser = TestUser::model_validate(json, options).unwrap();
        assert_eq!(user.name, "Alice");
        assert_eq!(user.age, 30);
    }

    #[test]
    fn test_validate_options_builder() {
        let mut context = HashMap::new();
        context.insert("key".to_string(), serde_json::json!("value"));

        let options = ValidateOptions::new()
            .strict()
            .from_attributes()
            .with_context(context.clone());

        assert!(options.strict);
        assert!(options.from_attributes);
        assert!(options.context.is_some());
        assert_eq!(
            options.context.unwrap().get("key"),
            Some(&serde_json::json!("value"))
        );
    }

    #[test]
    fn test_validate_input_from_conversions() {
        // From String
        let input: ValidateInput = "{}".to_string().into();
        assert!(matches!(input, ValidateInput::Json(_)));

        // From &str
        let input: ValidateInput = "{}".into();
        assert!(matches!(input, ValidateInput::Json(_)));

        // From serde_json::Value
        let input: ValidateInput = serde_json::json!({}).into();
        assert!(matches!(input, ValidateInput::JsonValue(_)));

        // From HashMap
        let map: HashMap<String, Value> = HashMap::new();
        let input: ValidateInput = map.into();
        assert!(matches!(input, ValidateInput::Dict(_)));
    }

    #[test]
    fn test_value_to_json_conversions() {
        assert_eq!(value_to_json(Value::Null), serde_json::Value::Null);
        assert_eq!(value_to_json(Value::Bool(true)), serde_json::json!(true));
        assert_eq!(value_to_json(Value::Int(42)), serde_json::json!(42));
        assert_eq!(value_to_json(Value::BigInt(100)), serde_json::json!(100));
        assert_eq!(
            value_to_json(Value::Text("hello".to_string())),
            serde_json::json!("hello")
        );
        // UUID is [u8; 16]
        let uuid_bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        assert_eq!(
            value_to_json(Value::Uuid(uuid_bytes)),
            serde_json::json!("550e8400-e29b-41d4-a716-446655440000")
        );

        // Array conversion
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        assert_eq!(
            value_to_json(Value::Array(arr)),
            serde_json::json!([1, 2, 3])
        );
    }

    // =========================================================================
    // model_dump tests
    // =========================================================================

    #[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
    struct TestProduct {
        name: String,
        price: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    }

    #[test]
    fn test_model_dump_default() {
        let product = TestProduct {
            name: "Widget".to_string(),
            price: 19.99,
            description: Some("A useful widget".to_string()),
        };
        let json = product.model_dump(DumpOptions::default()).unwrap();
        assert_eq!(json["name"], "Widget");
        assert_eq!(json["price"], 19.99);
        assert_eq!(json["description"], "A useful widget");
    }

    #[test]
    fn test_model_dump_json() {
        let product = TestProduct {
            name: "Gadget".to_string(),
            price: 29.99,
            description: None,
        };
        let json_str = product.model_dump_json().unwrap();
        assert!(json_str.contains("Gadget"));
        assert!(json_str.contains("29.99"));
    }

    #[test]
    fn test_model_dump_json_pretty() {
        let product = TestProduct {
            name: "Gadget".to_string(),
            price: 29.99,
            description: None,
        };
        let json_str = product.model_dump_json_pretty().unwrap();
        // Pretty print should have newlines
        assert!(json_str.contains('\n'));
        assert!(json_str.contains("Gadget"));
    }

    #[test]
    fn test_model_dump_json_with_options_compact() {
        let product = TestProduct {
            name: "Widget".to_string(),
            price: 19.99,
            description: Some("A widget".to_string()),
        };

        // Compact JSON (no indent)
        let json_str = product
            .model_dump_json_with_options(DumpOptions::default())
            .unwrap();
        assert!(!json_str.contains('\n')); // No newlines in compact mode
        assert!(json_str.contains("Widget"));
        assert!(json_str.contains("19.99"));
    }

    #[test]
    fn test_model_dump_json_with_options_indent() {
        let product = TestProduct {
            name: "Widget".to_string(),
            price: 19.99,
            description: Some("A widget".to_string()),
        };

        // 2-space indentation
        let json_str = product
            .model_dump_json_with_options(DumpOptions::default().indent(2))
            .unwrap();
        assert!(json_str.contains('\n')); // Has newlines
        assert!(json_str.contains("  \"name\"")); // 2-space indent
        assert!(json_str.contains("Widget"));

        // 4-space indentation
        let json_str = product
            .model_dump_json_with_options(DumpOptions::default().indent(4))
            .unwrap();
        assert!(json_str.contains("    \"name\"")); // 4-space indent
    }

    #[test]
    fn test_model_dump_json_with_options_combined() {
        let product = TestProduct {
            name: "Widget".to_string(),
            price: 19.99,
            description: Some("A widget".to_string()),
        };

        // Combine indent with exclude
        let json_str = product
            .model_dump_json_with_options(DumpOptions::default().exclude(["price"]).indent(2))
            .unwrap();
        assert!(json_str.contains('\n')); // Has newlines
        assert!(json_str.contains("Widget"));
        assert!(!json_str.contains("19.99")); // price is excluded
    }

    #[test]
    fn test_dump_options_indent_builder() {
        let options = DumpOptions::new().indent(4);
        assert_eq!(options.indent, Some(4));

        // Can combine with other options
        let options2 = DumpOptions::new()
            .indent(2)
            .by_alias()
            .exclude(["password"]);
        assert_eq!(options2.indent, Some(2));
        assert!(options2.by_alias);
        assert!(options2.exclude.unwrap().contains("password"));
    }

    #[test]
    fn test_model_dump_include() {
        let product = TestProduct {
            name: "Widget".to_string(),
            price: 19.99,
            description: Some("A widget".to_string()),
        };
        let options = DumpOptions::new().include(["name"]);
        let json = product.model_dump(options).unwrap();
        assert!(json.get("name").is_some());
        assert!(json.get("price").is_none());
        assert!(json.get("description").is_none());
    }

    #[test]
    fn test_model_dump_exclude() {
        let product = TestProduct {
            name: "Widget".to_string(),
            price: 19.99,
            description: Some("A widget".to_string()),
        };
        let options = DumpOptions::new().exclude(["description"]);
        let json = product.model_dump(options).unwrap();
        assert!(json.get("name").is_some());
        assert!(json.get("price").is_some());
        assert!(json.get("description").is_none());
    }

    #[test]
    fn test_model_dump_exclude_none() {
        let product = TestProduct {
            name: "Widget".to_string(),
            price: 19.99,
            description: None,
        };
        // Note: serde skip_serializing_if already handles this
        // But we can still test the exclude_none flag
        let options = DumpOptions::new().exclude_none();
        let json = product.model_dump(options).unwrap();
        assert!(json.get("name").is_some());
        // description would be None, but serde already skips it
    }

    #[test]
    fn test_dump_options_builder() {
        let options = DumpOptions::new()
            .json()
            .include(["name", "age"])
            .exclude(["password"])
            .by_alias()
            .exclude_none()
            .exclude_defaults()
            .round_trip();

        assert_eq!(options.mode, DumpMode::Json);
        assert!(options.include.is_some());
        assert!(options.exclude.is_some());
        assert!(options.by_alias);
        assert!(options.exclude_none);
        assert!(options.exclude_defaults);
        assert!(options.round_trip);
    }

    #[test]
    fn test_dump_mode_default() {
        assert_eq!(DumpMode::default(), DumpMode::Json);
    }

    #[test]
    fn test_model_dump_include_exclude_combined() {
        let user = TestUser {
            name: "Alice".to_string(),
            age: 30,
            active: true,
        };
        // Include name and age, but exclude age
        let options = DumpOptions::new().include(["name", "age"]).exclude(["age"]);
        let json = user.model_dump(options).unwrap();
        // Include is applied first, then exclude
        assert!(json.get("name").is_some());
        assert!(json.get("age").is_none());
        assert!(json.get("active").is_none());
    }

    // ========================================================================
    // Alias Tests
    // ========================================================================

    use crate::{FieldInfo, Row, SqlType};

    /// Test model with aliases for validation and serialization tests.
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestAliasedUser {
        id: i64,
        name: String,
        email: String,
    }

    impl Model for TestAliasedUser {
        const TABLE_NAME: &'static str = "users";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                FieldInfo::new("name", "name", SqlType::Text)
                    .validation_alias("userName")
                    .serialization_alias("displayName"),
                FieldInfo::new("email", "email", SqlType::Text).alias("emailAddress"), // Both input and output
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![
                ("id", Value::BigInt(self.id)),
                ("name", Value::Text(self.name.clone())),
                ("email", Value::Text(self.email.clone())),
            ]
        }

        fn from_row(row: &Row) -> crate::Result<Self> {
            Ok(Self {
                id: row.get_named("id")?,
                name: row.get_named("name")?,
                email: row.get_named("email")?,
            })
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![Value::BigInt(self.id)]
        }

        fn is_new(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_apply_validation_aliases() {
        let fields = TestAliasedUser::fields();

        // Test with validation_alias
        let mut json = serde_json::json!({
            "id": 1,
            "userName": "Alice",
            "email": "alice@example.com"
        });
        apply_validation_aliases(&mut json, fields);

        // userName should be renamed to name
        assert_eq!(json["name"], "Alice");
        assert!(json.get("userName").is_none());

        // Test with regular alias
        let mut json2 = serde_json::json!({
            "id": 1,
            "name": "Bob",
            "emailAddress": "bob@example.com"
        });
        apply_validation_aliases(&mut json2, fields);

        // emailAddress should be renamed to email
        assert_eq!(json2["email"], "bob@example.com");
        assert!(json2.get("emailAddress").is_none());
    }

    #[test]
    fn test_apply_serialization_aliases() {
        let fields = TestAliasedUser::fields();

        let mut json = serde_json::json!({
            "id": 1,
            "name": "Alice",
            "email": "alice@example.com"
        });
        apply_serialization_aliases(&mut json, fields);

        // name should be renamed to displayName (serialization_alias)
        assert_eq!(json["displayName"], "Alice");
        assert!(json.get("name").is_none());

        // email should be renamed to emailAddress (regular alias)
        assert_eq!(json["emailAddress"], "alice@example.com");
        assert!(json.get("email").is_none());
    }

    #[test]
    fn test_sql_model_validate_with_validation_alias() {
        // Use validation_alias in input
        let json = r#"{"id": 1, "userName": "Alice", "email": "alice@example.com"}"#;
        let user: TestAliasedUser = TestAliasedUser::sql_model_validate_json(json).unwrap();

        assert_eq!(user.id, 1);
        assert_eq!(user.name, "Alice");
        assert_eq!(user.email, "alice@example.com");
    }

    #[test]
    fn test_sql_model_validate_with_regular_alias() {
        // Use regular alias in input
        let json = r#"{"id": 1, "name": "Bob", "emailAddress": "bob@example.com"}"#;
        let user: TestAliasedUser = TestAliasedUser::sql_model_validate_json(json).unwrap();

        assert_eq!(user.id, 1);
        assert_eq!(user.name, "Bob");
        assert_eq!(user.email, "bob@example.com");
    }

    #[test]
    fn test_sql_model_validate_with_field_name() {
        // Use actual field name (should still work)
        let json = r#"{"id": 1, "name": "Charlie", "email": "charlie@example.com"}"#;
        let user: TestAliasedUser = TestAliasedUser::sql_model_validate_json(json).unwrap();

        assert_eq!(user.id, 1);
        assert_eq!(user.name, "Charlie");
        assert_eq!(user.email, "charlie@example.com");
    }

    #[test]
    fn test_sql_model_dump_by_alias() {
        let user = TestAliasedUser {
            id: 1,
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
        };

        let json = user
            .sql_model_dump(DumpOptions::default().by_alias())
            .unwrap();

        // name should be serialized as displayName
        assert_eq!(json["displayName"], "Alice");
        assert!(json.get("name").is_none());

        // email should be serialized as emailAddress
        assert_eq!(json["emailAddress"], "alice@example.com");
        assert!(json.get("email").is_none());
    }

    #[test]
    fn test_sql_model_dump_without_alias() {
        let user = TestAliasedUser {
            id: 1,
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
        };

        // Without by_alias, use original field names
        let json = user.sql_model_dump(DumpOptions::default()).unwrap();

        assert_eq!(json["name"], "Alice");
        assert_eq!(json["email"], "alice@example.com");
        assert!(json.get("displayName").is_none());
        assert!(json.get("emailAddress").is_none());
    }

    #[test]
    fn test_alias_does_not_overwrite_existing() {
        let fields = TestAliasedUser::fields();

        // If both alias and field name are present, field name wins
        let mut json = serde_json::json!({
            "id": 1,
            "name": "FieldName",
            "userName": "AliasName",
            "email": "test@example.com"
        });
        apply_validation_aliases(&mut json, fields);

        // Original "name" field should be preserved
        assert_eq!(json["name"], "FieldName");
        // userName should be removed (but couldn't insert because "name" exists)
        assert!(json.get("userName").is_none());
    }

    // ========================================================================
    // Computed Field Tests
    // ========================================================================

    /// Test model with computed fields.
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestUserWithComputed {
        id: i64,
        first_name: String,
        last_name: String,
        #[serde(default)]
        full_name: String, // Computed field - derived from first_name + last_name
    }

    impl Model for TestUserWithComputed {
        const TABLE_NAME: &'static str = "users";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                FieldInfo::new("first_name", "first_name", SqlType::Text),
                FieldInfo::new("last_name", "last_name", SqlType::Text),
                FieldInfo::new("full_name", "full_name", SqlType::Text).computed(true),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            // Computed field is NOT included in DB operations
            vec![
                ("id", Value::BigInt(self.id)),
                ("first_name", Value::Text(self.first_name.clone())),
                ("last_name", Value::Text(self.last_name.clone())),
            ]
        }

        fn from_row(row: &Row) -> crate::Result<Self> {
            Ok(Self {
                id: row.get_named("id")?,
                first_name: row.get_named("first_name")?,
                last_name: row.get_named("last_name")?,
                // Computed field initialized with Default (empty string)
                full_name: String::new(),
            })
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![Value::BigInt(self.id)]
        }

        fn is_new(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_computed_field_included_by_default() {
        let user = TestUserWithComputed {
            id: 1,
            first_name: "John".to_string(),
            last_name: "Doe".to_string(),
            full_name: "John Doe".to_string(),
        };

        // By default, computed fields ARE included in model_dump
        let json = user.sql_model_dump(DumpOptions::default()).unwrap();

        assert_eq!(json["id"], 1);
        assert_eq!(json["first_name"], "John");
        assert_eq!(json["last_name"], "Doe");
        assert_eq!(json["full_name"], "John Doe"); // Computed field is present
    }

    #[test]
    fn test_computed_field_excluded_with_option() {
        let user = TestUserWithComputed {
            id: 1,
            first_name: "John".to_string(),
            last_name: "Doe".to_string(),
            full_name: "John Doe".to_string(),
        };

        // With exclude_computed_fields, computed fields are excluded
        let json = user
            .sql_model_dump(DumpOptions::default().exclude_computed_fields())
            .unwrap();

        assert_eq!(json["id"], 1);
        assert_eq!(json["first_name"], "John");
        assert_eq!(json["last_name"], "Doe");
        assert!(json.get("full_name").is_none()); // Computed field is excluded
    }

    #[test]
    fn test_computed_field_not_in_to_row() {
        let user = TestUserWithComputed {
            id: 1,
            first_name: "Jane".to_string(),
            last_name: "Smith".to_string(),
            full_name: "Jane Smith".to_string(),
        };

        // to_row() should not include computed field (for DB INSERT/UPDATE)
        let row = user.to_row();

        // Should have 3 fields: id, first_name, last_name
        assert_eq!(row.len(), 3);
        let field_names: Vec<&str> = row.iter().map(|(name, _)| *name).collect();
        assert!(field_names.contains(&"id"));
        assert!(field_names.contains(&"first_name"));
        assert!(field_names.contains(&"last_name"));
        assert!(!field_names.contains(&"full_name")); // Computed field NOT in row
    }

    #[test]
    fn test_computed_field_select_fields_excludes() {
        let fields = TestUserWithComputed::fields();

        // Check that computed field is marked
        let computed: Vec<&FieldInfo> = fields.iter().filter(|f| f.computed).collect();
        assert_eq!(computed.len(), 1);
        assert_eq!(computed[0].name, "full_name");

        // Non-computed fields
        let non_computed: Vec<&FieldInfo> = fields.iter().filter(|f| !f.computed).collect();
        assert_eq!(non_computed.len(), 3);
    }

    #[test]
    fn test_computed_field_with_other_dump_options() {
        let user = TestUserWithComputed {
            id: 1,
            first_name: "John".to_string(),
            last_name: "Doe".to_string(),
            full_name: "John Doe".to_string(),
        };

        // Combine exclude_computed_fields with include filter
        let json = user
            .sql_model_dump(DumpOptions::default().exclude_computed_fields().include([
                "id",
                "first_name",
                "full_name",
            ]))
            .unwrap();

        // full_name is excluded because it's computed, even though in include list
        // (exclude_computed_fields is applied before include filter)
        assert!(json.get("id").is_some());
        assert!(json.get("first_name").is_some());
        assert!(json.get("full_name").is_none()); // Excluded as computed
        assert!(json.get("last_name").is_none()); // Not in include list
    }

    #[test]
    fn test_dump_options_exclude_computed_fields_builder() {
        let options = DumpOptions::new().exclude_computed_fields();
        assert!(options.exclude_computed_fields);

        // Can combine with other options
        let options2 = DumpOptions::new()
            .exclude_computed_fields()
            .by_alias()
            .exclude_none();
        assert!(options2.exclude_computed_fields);
        assert!(options2.by_alias);
        assert!(options2.exclude_none);
    }

    /// Test model with both computed fields AND serialization aliases.
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestUserWithComputedAndAlias {
        id: i64,
        first_name: String,
        #[serde(default)]
        display_name: String, // Computed field that also has an alias
    }

    impl Model for TestUserWithComputedAndAlias {
        const TABLE_NAME: &'static str = "users";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                FieldInfo::new("first_name", "first_name", SqlType::Text)
                    .serialization_alias("firstName"),
                FieldInfo::new("display_name", "display_name", SqlType::Text)
                    .computed(true)
                    .serialization_alias("displayName"),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![
                ("id", Value::BigInt(self.id)),
                ("first_name", Value::Text(self.first_name.clone())),
            ]
        }

        fn from_row(row: &Row) -> crate::Result<Self> {
            Ok(Self {
                id: row.get_named("id")?,
                first_name: row.get_named("first_name")?,
                display_name: String::new(),
            })
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![Value::BigInt(self.id)]
        }

        fn is_new(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_exclude_computed_with_by_alias() {
        // This test verifies that computed field exclusion works correctly
        // even when combined with by_alias (which renames keys)
        let user = TestUserWithComputedAndAlias {
            id: 1,
            first_name: "John".to_string(),
            display_name: "John Doe".to_string(),
        };

        // Test with by_alias only - computed field should still appear (aliased)
        let json = user
            .sql_model_dump(DumpOptions::default().by_alias())
            .unwrap();
        assert_eq!(json["firstName"], "John"); // first_name aliased
        assert_eq!(json["displayName"], "John Doe"); // display_name aliased (computed but not excluded)
        assert!(json.get("first_name").is_none()); // Original name should not exist
        assert!(json.get("display_name").is_none()); // Original name should not exist

        // Test with exclude_computed_fields only - computed field should be excluded
        let json = user
            .sql_model_dump(DumpOptions::default().exclude_computed_fields())
            .unwrap();
        assert_eq!(json["first_name"], "John");
        assert!(json.get("display_name").is_none()); // Computed field excluded

        // Test with BOTH by_alias AND exclude_computed_fields
        // This was buggy before the fix - computed field wasn't excluded
        // because exclusion happened after aliasing
        let json = user
            .sql_model_dump(DumpOptions::default().by_alias().exclude_computed_fields())
            .unwrap();
        assert_eq!(json["firstName"], "John"); // first_name aliased
        assert!(json.get("displayName").is_none()); // Computed field excluded (even though aliased)
        assert!(json.get("display_name").is_none()); // Original name doesn't exist either
    }

    // ========================================================================
    // Exclude Defaults Tests
    // ========================================================================

    /// Test model with default values for exclude_defaults testing.
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestModelWithDefaults {
        id: i64,
        name: String,
        count: i32,    // default: 0
        active: bool,  // default: false
        score: f64,    // default: 0.0
        label: String, // default: "default"
    }

    impl Model for TestModelWithDefaults {
        const TABLE_NAME: &'static str = "test_defaults";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                FieldInfo::new("name", "name", SqlType::Text),
                FieldInfo::new("count", "count", SqlType::Integer).default_json("0"),
                FieldInfo::new("active", "active", SqlType::Boolean).default_json("false"),
                FieldInfo::new("score", "score", SqlType::Double).default_json("0.0"),
                FieldInfo::new("label", "label", SqlType::Text).default_json("\"default\""),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![
                ("id", Value::BigInt(self.id)),
                ("name", Value::Text(self.name.clone())),
                ("count", Value::Int(self.count)),
                ("active", Value::Bool(self.active)),
                ("score", Value::Double(self.score)),
                ("label", Value::Text(self.label.clone())),
            ]
        }

        fn from_row(row: &Row) -> crate::Result<Self> {
            Ok(Self {
                id: row.get_named("id")?,
                name: row.get_named("name")?,
                count: row.get_named("count")?,
                active: row.get_named("active")?,
                score: row.get_named("score")?,
                label: row.get_named("label")?,
            })
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![Value::BigInt(self.id)]
        }

        fn is_new(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_exclude_defaults_all_at_default() {
        let model = TestModelWithDefaults {
            id: 1,
            name: "Test".to_string(),
            count: 0,                     // at default
            active: false,                // at default
            score: 0.0,                   // at default
            label: "default".to_string(), // at default
        };

        let json = model
            .sql_model_dump(DumpOptions::default().exclude_defaults())
            .unwrap();

        // id and name have no default_json, so they're always included
        assert!(json.get("id").is_some());
        assert!(json.get("name").is_some());

        // Fields at default value should be excluded
        assert!(json.get("count").is_none());
        assert!(json.get("active").is_none());
        assert!(json.get("score").is_none());
        assert!(json.get("label").is_none());
    }

    #[test]
    fn test_exclude_defaults_none_at_default() {
        let model = TestModelWithDefaults {
            id: 1,
            name: "Test".to_string(),
            count: 42,                   // not at default
            active: true,                // not at default
            score: 3.5,                  // not at default
            label: "custom".to_string(), // not at default
        };

        let json = model
            .sql_model_dump(DumpOptions::default().exclude_defaults())
            .unwrap();

        // All fields should be present since none are at defaults
        assert!(json.get("id").is_some());
        assert!(json.get("name").is_some());
        assert!(json.get("count").is_some());
        assert!(json.get("active").is_some());
        assert!(json.get("score").is_some());
        assert!(json.get("label").is_some());

        // Verify values
        assert_eq!(json["count"], 42);
        assert_eq!(json["active"], true);
        assert_eq!(json["score"], 3.5);
        assert_eq!(json["label"], "custom");
    }

    #[test]
    fn test_exclude_defaults_mixed() {
        let model = TestModelWithDefaults {
            id: 1,
            name: "Test".to_string(),
            count: 0,                    // at default
            active: true,                // not at default
            score: 0.0,                  // at default
            label: "custom".to_string(), // not at default
        };

        let json = model
            .sql_model_dump(DumpOptions::default().exclude_defaults())
            .unwrap();

        assert!(json.get("id").is_some());
        assert!(json.get("name").is_some());

        // At default - excluded
        assert!(json.get("count").is_none());
        assert!(json.get("score").is_none());

        // Not at default - included
        assert!(json.get("active").is_some());
        assert!(json.get("label").is_some());
        assert_eq!(json["active"], true);
        assert_eq!(json["label"], "custom");
    }

    #[test]
    fn test_exclude_defaults_without_flag() {
        let model = TestModelWithDefaults {
            id: 1,
            name: "Test".to_string(),
            count: 0,                     // at default
            active: false,                // at default
            score: 0.0,                   // at default
            label: "default".to_string(), // at default
        };

        // Without exclude_defaults, all fields should be included
        let json = model.sql_model_dump(DumpOptions::default()).unwrap();

        assert!(json.get("id").is_some());
        assert!(json.get("name").is_some());
        assert!(json.get("count").is_some());
        assert!(json.get("active").is_some());
        assert!(json.get("score").is_some());
        assert!(json.get("label").is_some());
    }

    #[test]
    fn test_exclude_defaults_with_by_alias() {
        // Test that exclude_defaults works correctly with by_alias

        /// Model with defaults and aliases
        #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
        struct TestAliasWithDefaults {
            id: i64,
            count: i32,
        }

        impl Model for TestAliasWithDefaults {
            const TABLE_NAME: &'static str = "test";
            const PRIMARY_KEY: &'static [&'static str] = &["id"];

            fn fields() -> &'static [FieldInfo] {
                static FIELDS: &[FieldInfo] = &[
                    FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                    FieldInfo::new("count", "count", SqlType::Integer)
                        .default_json("0")
                        .serialization_alias("itemCount"),
                ];
                FIELDS
            }

            fn to_row(&self) -> Vec<(&'static str, Value)> {
                vec![
                    ("id", Value::BigInt(self.id)),
                    ("count", Value::Int(self.count)),
                ]
            }

            fn from_row(row: &Row) -> crate::Result<Self> {
                Ok(Self {
                    id: row.get_named("id")?,
                    count: row.get_named("count")?,
                })
            }

            fn primary_key_value(&self) -> Vec<Value> {
                vec![Value::BigInt(self.id)]
            }

            fn is_new(&self) -> bool {
                false
            }
        }

        // At default value
        let model_at_default = TestAliasWithDefaults { id: 1, count: 0 };
        let json = model_at_default
            .sql_model_dump(DumpOptions::default().exclude_defaults().by_alias())
            .unwrap();

        // count is at default (0), so neither count nor itemCount should appear
        assert!(json.get("count").is_none());
        assert!(json.get("itemCount").is_none());

        // Not at default
        let model_not_at_default = TestAliasWithDefaults { id: 1, count: 5 };
        let json = model_not_at_default
            .sql_model_dump(DumpOptions::default().exclude_defaults().by_alias())
            .unwrap();

        // count is not at default, should appear with alias
        assert!(json.get("count").is_none()); // Original name not present
        assert_eq!(json["itemCount"], 5); // Alias is present
    }

    #[test]
    fn test_field_info_default_json() {
        // Test the FieldInfo builder methods for default_json
        let field1 = FieldInfo::new("count", "count", SqlType::Integer).default_json("0");
        assert_eq!(field1.default_json, Some("0"));
        assert!(field1.has_default);

        let field2 =
            FieldInfo::new("name", "name", SqlType::Text).default_json_opt(Some("\"hello\""));
        assert_eq!(field2.default_json, Some("\"hello\""));
        assert!(field2.has_default);

        let field3 = FieldInfo::new("name", "name", SqlType::Text).default_json_opt(None);
        assert_eq!(field3.default_json, None);
        assert!(!field3.has_default);

        let field4 = FieldInfo::new("flag", "flag", SqlType::Boolean).has_default(true);
        assert!(field4.has_default);
        assert_eq!(field4.default_json, None); // has_default alone doesn't set default_json
    }

    // ==================== SqlModelUpdate Tests ====================

    #[test]
    fn test_sqlmodel_update_from_dict() {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct TestUser {
            id: i64,
            name: String,
            age: i32,
        }

        impl Model for TestUser {
            const TABLE_NAME: &'static str = "users";
            const PRIMARY_KEY: &'static [&'static str] = &["id"];

            fn fields() -> &'static [FieldInfo] {
                static FIELDS: &[FieldInfo] = &[
                    FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                    FieldInfo::new("name", "name", SqlType::Text),
                    FieldInfo::new("age", "age", SqlType::Integer),
                ];
                FIELDS
            }

            fn to_row(&self) -> Vec<(&'static str, Value)> {
                vec![
                    ("id", Value::BigInt(self.id)),
                    ("name", Value::Text(self.name.clone())),
                    ("age", Value::Int(self.age)),
                ]
            }

            fn from_row(row: &Row) -> crate::Result<Self> {
                Ok(Self {
                    id: row.get_named("id")?,
                    name: row.get_named("name")?,
                    age: row.get_named("age")?,
                })
            }

            fn primary_key_value(&self) -> Vec<Value> {
                vec![Value::BigInt(self.id)]
            }

            fn is_new(&self) -> bool {
                false
            }
        }

        let mut user = TestUser {
            id: 1,
            name: "Alice".to_string(),
            age: 30,
        };

        // Update name only
        let update = HashMap::from([("name".to_string(), serde_json::json!("Bob"))]);
        user.sqlmodel_update(update, UpdateOptions::default())
            .unwrap();

        assert_eq!(user.name, "Bob");
        assert_eq!(user.age, 30); // Unchanged
    }

    #[test]
    fn test_sqlmodel_update_with_update_fields_filter() {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct TestUser {
            id: i64,
            name: String,
            age: i32,
        }

        impl Model for TestUser {
            const TABLE_NAME: &'static str = "users";
            const PRIMARY_KEY: &'static [&'static str] = &["id"];

            fn fields() -> &'static [FieldInfo] {
                static FIELDS: &[FieldInfo] = &[
                    FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                    FieldInfo::new("name", "name", SqlType::Text),
                    FieldInfo::new("age", "age", SqlType::Integer),
                ];
                FIELDS
            }

            fn to_row(&self) -> Vec<(&'static str, Value)> {
                vec![
                    ("id", Value::BigInt(self.id)),
                    ("name", Value::Text(self.name.clone())),
                    ("age", Value::Int(self.age)),
                ]
            }

            fn from_row(row: &Row) -> crate::Result<Self> {
                Ok(Self {
                    id: row.get_named("id")?,
                    name: row.get_named("name")?,
                    age: row.get_named("age")?,
                })
            }

            fn primary_key_value(&self) -> Vec<Value> {
                vec![Value::BigInt(self.id)]
            }

            fn is_new(&self) -> bool {
                false
            }
        }

        let mut user = TestUser {
            id: 1,
            name: "Alice".to_string(),
            age: 30,
        };

        // Try to update both name and age, but only allow name
        let update = HashMap::from([
            ("name".to_string(), serde_json::json!("Bob")),
            ("age".to_string(), serde_json::json!(25)),
        ]);
        user.sqlmodel_update(update, UpdateOptions::default().update_fields(["name"]))
            .unwrap();

        assert_eq!(user.name, "Bob"); // Updated
        assert_eq!(user.age, 30); // Not updated because not in update_fields
    }

    #[test]
    fn test_sqlmodel_update_invalid_field_error() {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct TestUser {
            id: i64,
            name: String,
        }

        impl Model for TestUser {
            const TABLE_NAME: &'static str = "users";
            const PRIMARY_KEY: &'static [&'static str] = &["id"];

            fn fields() -> &'static [FieldInfo] {
                static FIELDS: &[FieldInfo] = &[
                    FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                    FieldInfo::new("name", "name", SqlType::Text),
                ];
                FIELDS
            }

            fn to_row(&self) -> Vec<(&'static str, Value)> {
                vec![
                    ("id", Value::BigInt(self.id)),
                    ("name", Value::Text(self.name.clone())),
                ]
            }

            fn from_row(row: &Row) -> crate::Result<Self> {
                Ok(Self {
                    id: row.get_named("id")?,
                    name: row.get_named("name")?,
                })
            }

            fn primary_key_value(&self) -> Vec<Value> {
                vec![Value::BigInt(self.id)]
            }

            fn is_new(&self) -> bool {
                false
            }
        }

        let mut user = TestUser {
            id: 1,
            name: "Alice".to_string(),
        };

        // Try to update an invalid field
        let update = HashMap::from([("invalid_field".to_string(), serde_json::json!("value"))]);
        let result = user.sqlmodel_update(update, UpdateOptions::default());

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.errors.iter().any(|e| e.field == "invalid_field"));
    }

    #[test]
    fn test_sqlmodel_update_from_model() {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct TestUser {
            id: i64,
            name: String,
            email: Option<String>,
        }

        impl Model for TestUser {
            const TABLE_NAME: &'static str = "users";
            const PRIMARY_KEY: &'static [&'static str] = &["id"];

            fn fields() -> &'static [FieldInfo] {
                static FIELDS: &[FieldInfo] = &[
                    FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                    FieldInfo::new("name", "name", SqlType::Text),
                    FieldInfo::new("email", "email", SqlType::Text).nullable(true),
                ];
                FIELDS
            }

            fn to_row(&self) -> Vec<(&'static str, Value)> {
                vec![
                    ("id", Value::BigInt(self.id)),
                    ("name", Value::Text(self.name.clone())),
                    ("email", self.email.clone().map_or(Value::Null, Value::Text)),
                ]
            }

            fn from_row(row: &Row) -> crate::Result<Self> {
                Ok(Self {
                    id: row.get_named("id")?,
                    name: row.get_named("name")?,
                    email: row.get_named("email").ok(),
                })
            }

            fn primary_key_value(&self) -> Vec<Value> {
                vec![Value::BigInt(self.id)]
            }

            fn is_new(&self) -> bool {
                false
            }
        }

        let mut user = TestUser {
            id: 1,
            name: "Alice".to_string(),
            email: Some("alice@example.com".to_string()),
        };

        // Patch with only name set (email is None)
        let patch = TestUser {
            id: 0, // Will be ignored since we're copying non-null values
            name: "Bob".to_string(),
            email: None, // Should not overwrite existing email
        };

        user.sqlmodel_update_from(&patch, UpdateOptions::default())
            .unwrap();

        assert_eq!(user.name, "Bob"); // Updated
        assert_eq!(user.email, Some("alice@example.com".to_string())); // Not updated (patch.email was None)
    }

    #[test]
    fn test_sqlmodel_update_dict_convenience() {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct TestItem {
            id: i64,
            count: i32,
        }

        impl Model for TestItem {
            const TABLE_NAME: &'static str = "items";
            const PRIMARY_KEY: &'static [&'static str] = &["id"];

            fn fields() -> &'static [FieldInfo] {
                static FIELDS: &[FieldInfo] = &[
                    FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                    FieldInfo::new("count", "count", SqlType::Integer),
                ];
                FIELDS
            }

            fn to_row(&self) -> Vec<(&'static str, Value)> {
                vec![
                    ("id", Value::BigInt(self.id)),
                    ("count", Value::Int(self.count)),
                ]
            }

            fn from_row(row: &Row) -> crate::Result<Self> {
                Ok(Self {
                    id: row.get_named("id")?,
                    count: row.get_named("count")?,
                })
            }

            fn primary_key_value(&self) -> Vec<Value> {
                vec![Value::BigInt(self.id)]
            }

            fn is_new(&self) -> bool {
                false
            }
        }

        let mut item = TestItem { id: 1, count: 10 };

        // Use the convenience method
        item.sqlmodel_update_dict(HashMap::from([(
            "count".to_string(),
            serde_json::json!(20),
        )]))
        .unwrap();

        assert_eq!(item.count, 20);
    }

    // ========================================================================
    // Credit Card Validation Tests (Luhn Algorithm)
    // ========================================================================

    #[test]
    fn test_credit_card_valid_visa() {
        // Valid Visa test number
        assert!(is_valid_credit_card("4539578763621486"));
    }

    #[test]
    fn test_credit_card_valid_mastercard() {
        // Valid Mastercard test number
        assert!(is_valid_credit_card("5425233430109903"));
    }

    #[test]
    fn test_credit_card_valid_amex() {
        // Valid American Express test number
        assert!(is_valid_credit_card("374245455400126"));
    }

    #[test]
    fn test_credit_card_with_spaces() {
        // Spaces should be stripped
        assert!(is_valid_credit_card("4539 5787 6362 1486"));
    }

    #[test]
    fn test_credit_card_with_dashes() {
        // Dashes should be stripped
        assert!(is_valid_credit_card("4539-5787-6362-1486"));
    }

    #[test]
    fn test_credit_card_invalid_luhn() {
        // Invalid Luhn checksum
        assert!(!is_valid_credit_card("1234567890123456"));
    }

    #[test]
    fn test_credit_card_too_short() {
        // Less than 13 digits
        assert!(!is_valid_credit_card("123456789012"));
    }

    #[test]
    fn test_credit_card_too_long() {
        // More than 19 digits
        assert!(!is_valid_credit_card("12345678901234567890"));
    }

    #[test]
    fn test_credit_card_empty() {
        assert!(!is_valid_credit_card(""));
    }

    #[test]
    fn test_credit_card_non_numeric() {
        // Contains letters
        assert!(!is_valid_credit_card("453957876362abcd"));
    }

    #[test]
    fn test_credit_card_all_zeros() {
        // All zeros - 16 digits, technically passes Luhn (sum=0, 0%10=0)
        // but not a realistic card number
        assert!(is_valid_credit_card("0000000000000000"));
    }

    #[test]
    fn test_credit_card_valid_discover() {
        // Valid Discover test number
        assert!(is_valid_credit_card("6011111111111117"));
    }

    // =========================================================================
    // Nested Model Tests
    // =========================================================================

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Address {
        street: String,
        city: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        zip: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Person {
        name: String,
        age: i32,
        address: Address,
        #[serde(skip_serializing_if = "Option::is_none")]
        spouse: Option<Box<Person>>,
    }

    #[test]
    fn test_nested_model_dump_basic() {
        let person = Person {
            name: "Alice".to_string(),
            age: 30,
            address: Address {
                street: "123 Main St".to_string(),
                city: "Springfield".to_string(),
                zip: Some("12345".to_string()),
            },
            spouse: None,
        };

        let json = person.model_dump(DumpOptions::default()).unwrap();
        assert_eq!(json["name"], "Alice");
        assert_eq!(json["age"], 30);
        assert_eq!(json["address"]["street"], "123 Main St");
        assert_eq!(json["address"]["city"], "Springfield");
        assert_eq!(json["address"]["zip"], "12345");
    }

    #[test]
    fn test_nested_model_dump_exclude_top_level() {
        let person = Person {
            name: "Alice".to_string(),
            age: 30,
            address: Address {
                street: "123 Main St".to_string(),
                city: "Springfield".to_string(),
                zip: Some("12345".to_string()),
            },
            spouse: None,
        };

        // Exclude only applies to top-level fields
        let json = person
            .model_dump(DumpOptions::default().exclude(["age"]))
            .unwrap();
        assert!(json.get("name").is_some());
        assert!(json.get("age").is_none());
        assert!(json.get("address").is_some()); // Still present
        // Nested fields are NOT affected by top-level exclude
        assert_eq!(json["address"]["city"], "Springfield");
    }

    #[test]
    fn test_nested_model_dump_exclude_nested_limitation() {
        // NOTE: This test documents a LIMITATION.
        // In Pydantic, you can exclude nested fields with dot notation: exclude={"address.zip"}
        // Our current implementation only supports top-level field exclusion.
        let person = Person {
            name: "Alice".to_string(),
            age: 30,
            address: Address {
                street: "123 Main St".to_string(),
                city: "Springfield".to_string(),
                zip: Some("12345".to_string()),
            },
            spouse: None,
        };

        // Trying to exclude "address.zip" won't work - it treats it as a top-level field name
        let json = person
            .model_dump(DumpOptions::default().exclude(["address.zip"]))
            .unwrap();
        // address.zip is still present because we don't support nested path exclusion
        assert_eq!(json["address"]["zip"], "12345");
    }

    #[test]
    fn test_deeply_nested_model_dump() {
        let person = Person {
            name: "Alice".to_string(),
            age: 30,
            address: Address {
                street: "123 Main St".to_string(),
                city: "Springfield".to_string(),
                zip: None,
            },
            spouse: Some(Box::new(Person {
                name: "Bob".to_string(),
                age: 32,
                address: Address {
                    street: "456 Oak Ave".to_string(),
                    city: "Springfield".to_string(),
                    zip: Some("12346".to_string()),
                },
                spouse: None,
            })),
        };

        let json = person.model_dump(DumpOptions::default()).unwrap();
        assert_eq!(json["name"], "Alice");
        assert_eq!(json["spouse"]["name"], "Bob");
        assert_eq!(json["spouse"]["address"]["street"], "456 Oak Ave");
    }

    #[test]
    fn test_nested_model_exclude_none() {
        let person = Person {
            name: "Alice".to_string(),
            age: 30,
            address: Address {
                street: "123 Main St".to_string(),
                city: "Springfield".to_string(),
                zip: None, // Will be skipped by serde skip_serializing_if
            },
            spouse: None, // Will be skipped by serde skip_serializing_if
        };

        let json = person
            .model_dump(DumpOptions::default().exclude_none())
            .unwrap();
        assert!(json.get("name").is_some());
        // spouse is None and serde skips it, so it's not in the output
        assert!(json.get("spouse").is_none());
        // Note: exclude_none only affects top-level nulls in model_dump
        // Nested nulls are handled by serde's skip_serializing_if
    }
}
