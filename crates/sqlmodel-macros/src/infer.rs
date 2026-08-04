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
pub fn infer_sql_type(ty: &Type) -> TokenStream {
    // First, unwrap Option<T> to get the inner type
    let inner_ty = unwrap_option_type(ty);

    // Get the type as a string for matching
    let type_str = type_to_string(inner_ty);

    // Match against known types
    match type_str.as_str() {
        // Boolean
        "bool" => quote! { sqlmodel_core::SqlType::Boolean },

        // Integer types
        "i8" => quote! { sqlmodel_core::SqlType::TinyInt },
        "i16" => quote! { sqlmodel_core::SqlType::SmallInt },
        "i32" => quote! { sqlmodel_core::SqlType::Integer },
        "i64" => quote! { sqlmodel_core::SqlType::BigInt },

        // Unsigned integers (map to next larger signed type to avoid overflow)
        "u8" => quote! { sqlmodel_core::SqlType::SmallInt },
        "u16" => quote! { sqlmodel_core::SqlType::Integer },
        "u32" => quote! { sqlmodel_core::SqlType::BigInt },
        "u64" => quote! { sqlmodel_core::SqlType::BigInt }, // May overflow, but best effort

        // Floating point
        "f32" => quote! { sqlmodel_core::SqlType::Real },
        "f64" => quote! { sqlmodel_core::SqlType::Double },

        // String types
        "String" | "&str" | "str" => quote! { sqlmodel_core::SqlType::Text },
        "char" => quote! { sqlmodel_core::SqlType::Char(1) },

        // Binary types
        "Vec<u8>" | "&[u8]" | "[u8]" => quote! { sqlmodel_core::SqlType::Blob },

        // UUID types (various paths)
        "Uuid" | "uuid::Uuid" => quote! { sqlmodel_core::SqlType::Uuid },

        // Chrono date/time types
        "NaiveDate" | "chrono::NaiveDate" => quote! { sqlmodel_core::SqlType::Date },
        "NaiveTime" | "chrono::NaiveTime" => quote! { sqlmodel_core::SqlType::Time },
        "NaiveDateTime" | "chrono::NaiveDateTime" => quote! { sqlmodel_core::SqlType::DateTime },
        "DateTime<Utc>" | "chrono::DateTime<Utc>" | "DateTime<chrono::Utc>" => {
            quote! { sqlmodel_core::SqlType::TimestampTz }
        }
        "DateTime<Local>" | "chrono::DateTime<Local>" | "DateTime<chrono::Local>" => {
            quote! { sqlmodel_core::SqlType::TimestampTz }
        }

        // Time crate date/time types
        "time::Date" => quote! { sqlmodel_core::SqlType::Date },
        "time::Time" => quote! { sqlmodel_core::SqlType::Time },
        "time::PrimitiveDateTime" | "PrimitiveDateTime" => {
            quote! { sqlmodel_core::SqlType::DateTime }
        }
        "time::OffsetDateTime" | "OffsetDateTime" => {
            quote! { sqlmodel_core::SqlType::TimestampTz }
        }

        // JSON types
        "serde_json::Value" | "Value" => quote! { sqlmodel_core::SqlType::Json },

        // Decimal types
        "rust_decimal::Decimal" | "Decimal" => {
            quote! { sqlmodel_core::SqlType::Numeric { precision: 38, scale: 18 } }
        }

        // Bytes crate
        "bytes::Bytes" | "Bytes" | "bytes::BytesMut" | "BytesMut" => {
            quote! { sqlmodel_core::SqlType::Blob }
        }

        // Default: Text (most permissive fallback)
        _ => quote! { sqlmodel_core::SqlType::Text },
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
pub fn parse_sql_type_attr(sql_type: &str) -> TokenStream {
    let sql_type_upper = sql_type.to_uppercase();
    let trimmed = sql_type_upper.trim();

    // Handle parameterized types first
    if let Some(rest) = trimmed.strip_prefix("VARCHAR(")
        && let Some(len_str) = rest.strip_suffix(')')
        && let Ok(len) = len_str.trim().parse::<u32>()
    {
        return quote! { sqlmodel_core::SqlType::VarChar(#len) };
    }

    if let Some(rest) = trimmed.strip_prefix("CHAR(")
        && let Some(len_str) = rest.strip_suffix(')')
        && let Ok(len) = len_str.trim().parse::<u32>()
    {
        return quote! { sqlmodel_core::SqlType::Char(#len) };
    }

    if let Some(rest) = trimmed.strip_prefix("NUMERIC(")
        && let Some(params_str) = rest.strip_suffix(')')
        && let Some((p_str, s_str)) = params_str.split_once(',')
        && let (Ok(p), Ok(s)) = (p_str.trim().parse::<u8>(), s_str.trim().parse::<u8>())
    {
        return quote! { sqlmodel_core::SqlType::Numeric { precision: #p, scale: #s } };
    }

    if let Some(rest) = trimmed.strip_prefix("DECIMAL(")
        && let Some(params_str) = rest.strip_suffix(')')
        && let Some((p_str, s_str)) = params_str.split_once(',')
        && let (Ok(p), Ok(s)) = (p_str.trim().parse::<u8>(), s_str.trim().parse::<u8>())
    {
        return quote! { sqlmodel_core::SqlType::Decimal { precision: #p, scale: #s } };
    }

    if let Some(rest) = trimmed.strip_prefix("BINARY(")
        && let Some(len_str) = rest.strip_suffix(')')
        && let Ok(len) = len_str.trim().parse::<u32>()
    {
        return quote! { sqlmodel_core::SqlType::Binary(#len) };
    }

    if let Some(rest) = trimmed.strip_prefix("VARBINARY(")
        && let Some(len_str) = rest.strip_suffix(')')
        && let Ok(len) = len_str.trim().parse::<u32>()
    {
        return quote! { sqlmodel_core::SqlType::VarBinary(#len) };
    }

    // Handle simple type names
    match trimmed {
        // Integer types
        "TINYINT" => quote! { sqlmodel_core::SqlType::TinyInt },
        "SMALLINT" | "INT2" => quote! { sqlmodel_core::SqlType::SmallInt },
        "INTEGER" | "INT" | "INT4" => quote! { sqlmodel_core::SqlType::Integer },
        "BIGINT" | "INT8" => quote! { sqlmodel_core::SqlType::BigInt },

        // Floating point
        "REAL" | "FLOAT4" => quote! { sqlmodel_core::SqlType::Real },
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" | "FLOAT" => {
            quote! { sqlmodel_core::SqlType::Double }
        }

        // Fixed precision (default precision)
        "NUMERIC" => quote! { sqlmodel_core::SqlType::Numeric { precision: 38, scale: 18 } },
        "DECIMAL" => quote! { sqlmodel_core::SqlType::Decimal { precision: 38, scale: 18 } },

        // Boolean
        "BOOLEAN" | "BOOL" => quote! { sqlmodel_core::SqlType::Boolean },

        // String types
        "TEXT" => quote! { sqlmodel_core::SqlType::Text },
        "VARCHAR" => quote! { sqlmodel_core::SqlType::VarChar(255) }, // Default length
        "CHAR" => quote! { sqlmodel_core::SqlType::Char(1) },

        // Binary types
        "BLOB" | "BYTEA" => quote! { sqlmodel_core::SqlType::Blob },
        "BINARY" => quote! { sqlmodel_core::SqlType::Binary(255) },
        "VARBINARY" => quote! { sqlmodel_core::SqlType::VarBinary(255) },

        // Date/time types
        "DATE" => quote! { sqlmodel_core::SqlType::Date },
        "TIME" => quote! { sqlmodel_core::SqlType::Time },
        "DATETIME" => quote! { sqlmodel_core::SqlType::DateTime },
        "TIMESTAMP" => quote! { sqlmodel_core::SqlType::Timestamp },
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
            quote! { sqlmodel_core::SqlType::TimestampTz }
        }

        // UUID
        "UUID" => quote! { sqlmodel_core::SqlType::Uuid },

        // JSON
        "JSON" => quote! { sqlmodel_core::SqlType::Json },
        "JSONB" => quote! { sqlmodel_core::SqlType::JsonB },

        // Unknown: use custom type
        _ => {
            let custom = sql_type; // Use original case for custom types
            quote! { sqlmodel_core::SqlType::Custom(#custom) }
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
        let ty: Type = parse_quote!(i32);
        let result = infer_sql_type(&ty).to_string();
        assert!(result.contains("Integer"));

        let ty: Type = parse_quote!(i64);
        let result = infer_sql_type(&ty).to_string();
        assert!(result.contains("BigInt"));

        let ty: Type = parse_quote!(bool);
        let result = infer_sql_type(&ty).to_string();
        assert!(result.contains("Boolean"));
    }

    #[test]
    fn test_infer_string() {
        let ty: Type = parse_quote!(String);
        let result = infer_sql_type(&ty).to_string();
        assert!(result.contains("Text"));
    }

    #[test]
    fn test_infer_option() {
        let ty: Type = parse_quote!(Option<i32>);
        let result = infer_sql_type(&ty).to_string();
        assert!(result.contains("Integer"));
    }

    #[test]
    fn test_parse_sql_type_varchar() {
        let result = parse_sql_type_attr("VARCHAR(100)").to_string();
        assert!(result.contains("VarChar"));
        assert!(result.contains("100"));
    }

    #[test]
    fn test_parse_sql_type_numeric() {
        let result = parse_sql_type_attr("NUMERIC(10, 2)").to_string();
        assert!(result.contains("Numeric"));
        assert!(result.contains("10"));
        assert!(result.contains('2'));
    }
}
