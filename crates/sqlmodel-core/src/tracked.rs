//! Tracked model wrapper for Pydantic-compatible `exclude_unset`.
//!
//! Rust structs do not retain "field was explicitly provided" metadata by default.
//! `TrackedModel<T>` stores a `FieldsSet` alongside `T` so dumps can implement
//! Pydantic's `exclude_unset` semantics precisely.

use std::ops::{Deref, DerefMut};

use crate::fields_set::FieldsSet;
use crate::validate::{
    DumpMode, DumpOptions, DumpResult, apply_serialization_aliases, dump_options_unsupported,
};
use crate::{FieldInfo, Model};

/// A model instance with explicit "fields set" tracking.
#[derive(Clone, Debug)]
pub struct TrackedModel<T> {
    inner: T,
    fields_set: FieldsSet,
}

impl<T> TrackedModel<T> {
    /// Wrap an instance and an explicit FieldsSet.
    #[must_use]
    pub const fn new(inner: T, fields_set: FieldsSet) -> Self {
        Self { inner, fields_set }
    }

    /// Borrow the wrapped instance.
    #[must_use]
    pub const fn inner(&self) -> &T {
        &self.inner
    }

    /// Mutably borrow the wrapped instance.
    #[must_use]
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Consume and return the wrapped instance.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Access the explicit fields-set bitset.
    #[must_use]
    pub const fn fields_set(&self) -> &FieldsSet {
        &self.fields_set
    }
}

impl<T: Model> TrackedModel<T> {
    /// Wrap an instance and mark all model fields as set.
    #[must_use]
    pub fn all_fields_set(inner: T) -> Self {
        let fields_set = FieldsSet::all(T::fields().len());
        Self { inner, fields_set }
    }

    /// Wrap an instance, marking only the provided field names as "set".
    ///
    /// Field names must match `FieldInfo.name` values (post-alias).
    #[must_use]
    pub fn from_explicit_field_names(inner: T, names: &[&str]) -> Self {
        let mut fields_set = FieldsSet::empty(T::fields().len());
        for (idx, field) in T::fields().iter().enumerate() {
            if names.contains(&field.name) {
                fields_set.set(idx);
            }
        }
        Self { inner, fields_set }
    }

    fn apply_field_exclusions(
        map: &mut serde_json::Map<String, serde_json::Value>,
        fields: &[FieldInfo],
        fields_set: &FieldsSet,
        exclude_unset: bool,
        exclude_computed_fields: bool,
        exclude_defaults: bool,
    ) {
        // Always honor per-field exclude flag (Pydantic Field(exclude=True) semantics).
        for field in fields {
            if field.exclude {
                map.remove(field.name);
            }
        }

        if exclude_unset {
            for (idx, field) in fields.iter().enumerate() {
                if !fields_set.is_set(idx) {
                    map.remove(field.name);
                }
            }
        }

        if exclude_computed_fields {
            for field in fields {
                if field.computed {
                    map.remove(field.name);
                }
            }
        }

        if exclude_defaults {
            for field in fields {
                if let Some(default_json) = field.default_json {
                    if let Some(current_value) = map.get(field.name) {
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
}

impl<T: Model + serde::Serialize> TrackedModel<T> {
    /// Model-aware dump with correct `exclude_unset` semantics.
    ///
    /// This mirrors `SqlModelDump::sql_model_dump`, but supports `exclude_unset`
    /// by consulting the stored `FieldsSet`.
    pub fn sql_model_dump(&self, options: DumpOptions) -> DumpResult {
        let DumpOptions {
            mode,
            include,
            exclude,
            by_alias,
            exclude_unset,
            exclude_defaults,
            exclude_none,
            exclude_computed_fields,
            round_trip,
            indent: _,
        } = options;

        if mode != DumpMode::Json {
            return Err(dump_options_unsupported(
                "DumpOptions.mode != Json is not supported (output is serde_json::Value)",
            ));
        }
        if round_trip {
            return Err(dump_options_unsupported(
                "DumpOptions.round_trip is not supported (tagged round-trip encoding is not available)",
            ));
        }

        let mut value = serde_json::to_value(&self.inner)?;

        if let serde_json::Value::Object(ref mut map) = value {
            Self::apply_field_exclusions(
                map,
                T::fields(),
                &self.fields_set,
                exclude_unset,
                exclude_computed_fields,
                exclude_defaults,
            );
        }

        if by_alias {
            apply_serialization_aliases(&mut value, T::fields());
        }

        if let serde_json::Value::Object(ref mut map) = value {
            if let Some(ref include_set) = include {
                map.retain(|k, _| include_set.contains(k));
            }
            if let Some(ref exclude_set) = exclude {
                map.retain(|k, _| !exclude_set.contains(k));
            }
            if exclude_none {
                map.retain(|_, v| !v.is_null());
            }
        }

        Ok(value)
    }

    pub fn sql_model_dump_json(&self) -> std::result::Result<String, serde_json::Error> {
        let value = self.sql_model_dump(DumpOptions::default())?;
        serde_json::to_string(&value)
    }

    pub fn sql_model_dump_json_pretty(&self) -> std::result::Result<String, serde_json::Error> {
        let value = self.sql_model_dump(DumpOptions::default())?;
        serde_json::to_string_pretty(&value)
    }

    pub fn sql_model_dump_json_with_options(
        &self,
        options: DumpOptions,
    ) -> std::result::Result<String, serde_json::Error> {
        let DumpOptions { indent, .. } = options.clone();
        let value = self.sql_model_dump(DumpOptions {
            indent: None,
            ..options
        })?;

        match indent {
            Some(spaces) => {
                let indent_bytes = " ".repeat(spaces).into_bytes();
                let formatter = serde_json::ser::PrettyFormatter::with_indent(&indent_bytes);
                let mut writer = Vec::new();
                let mut ser = serde_json::Serializer::with_formatter(&mut writer, formatter);
                serde::Serialize::serialize(&value, &mut ser)?;
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

impl<T> Deref for TrackedModel<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for TrackedModel<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
