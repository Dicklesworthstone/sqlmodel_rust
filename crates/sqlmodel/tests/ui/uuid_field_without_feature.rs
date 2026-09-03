//! A `uuid::Uuid` field without sqlmodel's `uuid` feature.
#[derive(sqlmodel::Model)]
#[sqlmodel(table = "devices")]
struct Device {
    #[sqlmodel(primary_key)]
    id: i64,
    token: uuid::Uuid,
}

fn main() {}
