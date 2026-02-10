use serde::{Deserialize, Serialize};
use sqlmodel::prelude::*;
use sqlmodel::tracked;

#[derive(Model, Debug, Clone, Default, Serialize, Deserialize)]
struct User {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
    #[serde(default)]
    age: i32,
    #[sqlmodel(exclude)]
    #[serde(default)]
    secret: String,
}

#[test]
fn tracked_validate_exclude_unset_omits_defaulted_fields() {
    let u = User::sql_model_validate_tracked(
        r#"{"id": 1, "name": "Alice"}"#,
        ValidateOptions::default(),
    )
    .unwrap();

    // age/secret exist on the struct via defaults, but were not explicitly provided.
    assert_eq!(u.age, 0);
    assert_eq!(u.secret, "");

    let dumped = u
        .sql_model_dump(DumpOptions::default().exclude_unset())
        .unwrap();

    assert_eq!(dumped["id"], 1);
    assert_eq!(dumped["name"], "Alice");
    assert!(dumped.get("age").is_none());
    assert!(dumped.get("secret").is_none()); // excluded by field config
}

#[test]
fn tracked_validate_exclude_unset_keeps_explicit_defaults() {
    let u = User::sql_model_validate_tracked(
        r#"{"id": 1, "name": "Alice", "age": 0}"#,
        ValidateOptions::default(),
    )
    .unwrap();

    let dumped = u
        .sql_model_dump(DumpOptions::default().exclude_unset())
        .unwrap();

    assert_eq!(dumped["age"], 0);
}

#[test]
fn tracked_macro_marks_only_literal_fields_as_set() {
    let u = tracked!(User {
        id: 7,
        name: "Bob".to_string(),
        ..Default::default()
    });

    let dumped = u
        .sql_model_dump(DumpOptions::default().exclude_unset())
        .unwrap();

    assert_eq!(dumped["id"], 7);
    assert_eq!(dumped["name"], "Bob");
    assert!(dumped.get("age").is_none());
    assert!(dumped.get("secret").is_none());
}
