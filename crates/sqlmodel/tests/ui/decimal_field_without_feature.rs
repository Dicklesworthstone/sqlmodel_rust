//! A `rust_decimal::Decimal` field without sqlmodel's `decimal` feature.
#[derive(sqlmodel::Model)]
#[sqlmodel(table = "invoices")]
struct Invoice {
    #[sqlmodel(primary_key)]
    id: i64,
    total: rust_decimal::Decimal,
}

fn main() {}
