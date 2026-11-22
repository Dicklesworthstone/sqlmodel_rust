//! SQL type inference from Rust types.
//!
//! This module provides functions to infer SQL types from Rust types
//! used in Model struct fields.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{GenericArgument, PathArguments, Type};

/// Infer the SQL type from a Rust type, returning a TokenStream that
/// constructs the appropriate SqlType variant.
///
/// This handles:
/// - Primitive types (i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, bool)
/// - String types (String, &str, char)
/// - Binary types (Vec<u8>)
/// - `Option<T>` (unwraps to inner type)
/// - Common library types (chrono, uuid, etc.) when detected
pub fn infer_sql_type(ty: &Type, core_crate: &TokenStream) -> TokenStream {
    // First, unwrap Option<T> to get the inner type
    let inner_ty = unwrap_option_type(ty);

    // Get the type as a string for matching
    let type_str = type_to_string(inner_ty);

    // Match against known types
    match type_str.as_str() {
        // Boolean
        "bool" => quote! { #core_crate::SqlType::Boolean },

        // Integer types
        "i8" => quote! { #core_crate::SqlType::TinyInt },
        "i16" => quote! { #core_crate::SqlType::SmallInt },
        "i32" => quote! { #core_crate::SqlType::Integer },
        "i64" => quote! { #core_crate::SqlType::BigInt },

        // Unsigned integers (map to next larger signed type to avoid overflow)
        "u8" => quote! { #core_crate::SqlType::SmallInt },
        "u16" => quote! { #core_crate::SqlType::Integer },
        "u32" => quote! { #core_crate::SqlType::BigInt },
        "u64" => quote! { #core_crate::SqlType::BigInt }, // May overflow, but best effort

        // Floating point
        "f32" => quote! { #core_crate::SqlType::Real },
        "f64" => quote! { #core_crate::SqlType::Double },

        // String types
        "String" | "&str" | "str" => quote! { #core_crate::SqlType::Text },
        "char" => quote! { #core_crate::SqlType::Char(1) },

        // Binary types
        "Vec<u8>" | "&[u8]" | "[u8]" => quote! { #core_crate::SqlType::Blob },

        // UUID types (various paths)
        "Uuid" | "uuid::Uuid" => quote! { #core_crate::SqlType::Uuid },

        // Chrono date/time types
        "NaiveDate" | "chrono::NaiveDate" => quote! { #core_crate::SqlType::Date },
        "NaiveTime" | "chrono::NaiveTime" => quote! { #core_crate::SqlType::Time },
        "NaiveDateTime" | "chrono::NaiveDateTime" => quote! { #core_crate::SqlType::DateTime },
        "DateTime<Utc>" | "chrono::DateTime<Utc>" | "DateTime<chrono::Utc>" => {
            quote! { #core_crate::SqlType::TimestampTz }
        }
        "DateTime<Local>" | "chrono::DateTime<Local>" | "DateTime<chrono::Local>" => {
            quote! { #core_crate::SqlType::TimestampTz }
        }

        // Time crate date/time types
        "time::Date" => quote! { #core_crate::SqlType::Date },
        "time::Time" => quote! { #core_crate::SqlType::Time },
        "time::PrimitiveDateTime" | "PrimitiveDateTime" => {
            quote! { #core_crate::SqlType::DateTime }
        }
        "time::OffsetDateTime" | "OffsetDateTime" => {
            quote! { #core_crate::SqlType::TimestampTz }
        }

        // JSON types
        "serde_json::Value" | "Value" => quote! { #core_crate::SqlType::Json },

        // Decimal types
        "rust_decimal::Decimal" | "Decimal" => {
            quote! { #core_crate::SqlType::Numeric { precision: 38, scale: 18 } }
        }

        // Bytes crate
        "bytes::Bytes" | "Bytes" | "bytes::BytesMut" | "BytesMut" => {
            quote! { #core_crate::SqlType::Blob }
        }

        // Default: Text (most permissive fallback)
        _ => quote! { #core_crate::SqlType::Text },
    }
}

/// Parse an explicit sql_type attribute string into a SqlType TokenStream.
///
/// Supports common SQL type names:
/// - INTEGER, INT, BIGINT, SMALLINT, TINYINT
/// - REAL, FLOAT, DOUBLE, DOUBLE PRECISION
/// - NUMERIC(p,s), DECIMAL(p,s)
/// - BOOLEAN, BOOL
/// - CHAR(n), VARCHAR(n), TEXT
/// - BINARY(n), VARBINARY(n), BLOB, BYTEA
/// - DATE, TIME, DATETIME, TIMESTAMP, TIMESTAMPTZ
/// - UUID
/// - JSON, JSONB
pub fn parse_sql_type_attr(sql_type: &str, core_crate: &TokenStream) -> TokenStream {
    let sql_type_upper = sql_type.to_uppercase();
    let trimmed = sql_type_upper.trim();

    // Handle parameterized types first
    if let Some(rest) = trimmed.strip_prefix("VARCHAR(")
        && let Some(len_str) = rest.strip_suffix(')')
        && let Ok(len) = len_str.trim().parse::<u32>()
    {
        return quote! { #core_crate::SqlType::VarChar(#len) };
    }

    if let Some(rest) = trimmed.strip_prefix("CHAR(")
        && let Some(len_str) = rest.strip_suffix(')')
        && let Ok(len) = len_str.trim().parse::<u32>()
    {
        return quote! { #core_crate::SqlType::Char(#len) };
    }

    if let Some(rest) = trimmed.strip_prefix("NUMERIC(")
        && let Some(params_str) = rest.strip_suffix(')')
        && let Some((p_str, s_str)) = params_str.split_once(',')
        && let (Ok(p), Ok(s)) = (p_str.trim().parse::<u8>(), s_str.trim().parse::<u8>())
    {
        return quote! { #core_crate::SqlType::Numeric { precision: #p, scale: #s } };
    }

    if let Some(rest) = trimmed.strip_prefix("DECIMAL(")
        && let Some(params_str) = rest.strip_suffix(')')
        && let Some((p_str, s_str)) = params_str.split_once(',')
        && let (Ok(p), Ok(s)) = (p_str.trim().parse::<u8>(), s_str.trim().parse::<u8>())
    {
        return quote! { #core_crate::SqlType::Decimal { precision: #p, scale: #s } };
    }

    if let Some(rest) = trimmed.strip_prefix("BINARY(")
        && let Some(len_str) = rest.strip_suffix(')')
        && let Ok(len) = len_str.trim().parse::<u32>()
    {
        return quote! { #core_crate::SqlType::Binary(#len) };
    }

    if let Some(rest) = trimmed.strip_prefix("VARBINARY(")
        && let Some(len_str) = rest.strip_suffix(')')
        && let Ok(len) = len_str.trim().parse::<u32>()
    {
        return quote! { #core_crate::SqlType::VarBinary(#len) };
    }

    // Handle simple type names
    match trimmed {
        // Integer types
        "TINYINT" => quote! { #core_crate::SqlType::TinyInt },
        "SMALLINT" | "INT2" => quote! { #core_crate::SqlType::SmallInt },
        "INTEGER" | "INT" | "INT4" => quote! { #core_crate::SqlType::Integer },
        "BIGINT" | "INT8" => quote! { #core_crate::SqlType::BigInt },

        // Floating point
        "REAL" | "FLOAT4" => quote! { #core_crate::SqlType::Real },
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" | "FLOAT" => {
            quote! { #core_crate::SqlType::Double }
        }

        // Fixed precision (default precision)
        "NUMERIC" => quote! { #core_crate::SqlType::Numeric { precision: 38, scale: 18 } },
        "DECIMAL" => quote! { #core_crate::SqlType::Decimal { precision: 38, scale: 18 } },

        // Boolean
        "BOOLEAN" | "BOOL" => quote! { #core_crate::SqlType::Boolean },

        // String types
        "TEXT" => quote! { #core_crate::SqlType::Text },
        "VARCHAR" => quote! { #core_crate::SqlType::VarChar(255) }, // Default length
        "CHAR" => quote! { #core_crate::SqlType::Char(1) },

        // Binary types
        "BLOB" | "BYTEA" => quote! { #core_crate::SqlType::Blob },
        "BINARY" => quote! { #core_crate::SqlType::Binary(255) },
        "VARBINARY" => quote! { #core_crate::SqlType::VarBinary(255) },

        // Date/time types
        "DATE" => quote! { #core_crate::SqlType::Date },
        "TIME" => quote! { #core_crate::SqlType::Time },
        "DATETIME" => quote! { #core_crate::SqlType::DateTime },
        "TIMESTAMP" => quote! { #core_crate::SqlType::Timestamp },
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
            quote! { #core_crate::SqlType::TimestampTz }
        }

        // UUID
        "UUID" => quote! { #core_crate::SqlType::Uuid },

        // JSON
        "JSON" => quote! { #core_crate::SqlType::Json },
        "JSONB" => quote! { #core_crate::SqlType::JsonB },

        // Unknown: use custom type
        _ => {
            let custom = sql_type; // Use original case for custom types
            quote! { #core_crate::SqlType::Custom(#custom) }
        }
    }
}

/// Unwrap `Option<T>` to get the inner type, or return the original type.
fn unwrap_option_type(ty: &Type) -> &Type {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
        && segment.ident == "Option"
        && let PathArguments::AngleBracketed(args) = &segment.arguments
        && let Some(GenericArgument::Type(inner)) = args.args.first()
    {
        return inner;
    }
    ty
}

/// Convert a Type to a simplified string representation for matching.
fn type_to_string(ty: &Type) -> String {
    use quote::ToTokens;
    ty.to_token_stream().to_string().replace(' ', "")
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn test_infer_primitives() {
        let core = quote! { ::sqlmodel_core };
        let ty: Type = parse_quote!(i32);
        let result = infer_sql_type(&ty, &core).to_string();
        assert!(result.contains("Integer"));

        let ty: Type = parse_quote!(i64);
        let result = infer_sql_type(&ty, &core).to_string();
        assert!(result.contains("BigInt"));

        let ty: Type = parse_quote!(bool);
        let result = infer_sql_type(&ty, &core).to_string();
        assert!(result.contains("Boolean"));
    }

    #[test]
    fn test_infer_string() {
        let core = quote! { ::sqlmodel_core };
        let ty: Type = parse_quote!(String);
        let result = infer_sql_type(&ty, &core).to_string();
        assert!(result.contains("Text"));
    }

    #[test]
    fn test_infer_option() {
        let core = quote! { ::sqlmodel_core };
        let ty: Type = parse_quote!(Option<i32>);
        let result = infer_sql_type(&ty, &core).to_string();
        assert!(result.contains("Integer"));
    }

    #[test]
    fn test_parse_sql_type_varchar() {
        let core = quote! { ::sqlmodel_core };
        let result = parse_sql_type_attr("VARCHAR(100)", &core).to_string();
        assert!(result.contains("VarChar"));
        assert!(result.contains("100"));
    }

    #[test]
    fn test_parse_sql_type_numeric() {
        let core = quote! { ::sqlmodel_core };
        let result = parse_sql_type_attr("NUMERIC(10, 2)", &core).to_string();
        assert!(result.contains("Numeric"));
        assert!(result.contains("10"));
        assert!(result.contains('2'));
    }
}
