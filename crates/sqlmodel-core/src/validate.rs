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

use crate::error::{ValidationError, ValidationErrorKind};
use crate::Value;

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
            ValidateInput::Json(json_str) => {
                serde_json::from_str(&json_str).map_err(|e| {
                    let mut err = ValidationError::new();
                    err.add("_json", ValidationErrorKind::Custom, format!("Invalid JSON: {e}"));
                    err
                })?
            }
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
            let hex = b.iter().fold(String::new(), |mut acc, byte| {
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
        Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(value_to_json).collect())
        }
        Value::Default => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(err.errors.iter().any(|e| e.message.contains("Invalid JSON")));
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
}
