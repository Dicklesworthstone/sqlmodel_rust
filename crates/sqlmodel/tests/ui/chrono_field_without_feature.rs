//! A `chrono::NaiveDateTime` field without sqlmodel's `chrono` feature.
#[derive(sqlmodel::Model)]
#[sqlmodel(table = "events")]
struct Event {
    #[sqlmodel(primary_key)]
    id: i64,
    at: chrono::NaiveDateTime,
}

fn main() {}
