//! Runtime validation helpers for SQLModel.
//!
//! This module provides validation functions that can be called from
//! generated validation code (via the `#[derive(Validate)]` macro).

use std::sync::OnceLock;

use regex::Regex;

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
        {
            let cache = self.cache.read().unwrap();
            if let Some(regex) = cache.get(pattern) {
                return Ok(regex.clone());
            }
        }

        // Slow path: compile and cache
        let regex = Regex::new(pattern)?;
        {
            let mut cache = self.cache.write().unwrap();
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
}
