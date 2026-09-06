//! Compile-time diagnostics for model fields of external types.
//!
//! A `chrono`, `uuid`, or `rust_decimal` field in a `#[derive(Model)]` struct
//! must fail to compile when the matching `sqlmodel` feature is off, with a
//! diagnostic that names the feature to enable (`.stderr` snapshots under
//! `tests/ui/`, regenerated deliberately with `TRYBUILD=overwrite`). With the
//! features on the same models compile. Which half runs follows the features
//! the test binary was built with: the default `cargo test -p sqlmodel` runs
//! the compile-fail half, `--all-features` (or the three features together)
//! runs the pass half.

#[cfg(not(any(feature = "chrono", feature = "uuid", feature = "decimal")))]
#[test]
fn external_type_fields_without_their_feature_name_the_feature() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*_without_feature.rs");
}

#[cfg(all(feature = "chrono", feature = "uuid", feature = "decimal"))]
#[test]
fn external_type_fields_compile_with_their_feature() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/*_with_feature.rs");
}

#[test]
fn multilevel_joined_inheritance_rejected() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/multilevel_joined_inheritance_rejected.rs");
}
