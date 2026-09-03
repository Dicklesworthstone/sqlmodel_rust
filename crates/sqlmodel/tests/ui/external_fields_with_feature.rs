//! The same fields compile once the `chrono`, `uuid`, and `decimal` features
//! are on.
#[derive(sqlmodel::Model)]
#[sqlmodel(table = "ledger")]
struct Ledger {
    #[sqlmodel(primary_key)]
    id: i64,
    at: chrono::NaiveDateTime,
    token: uuid::Uuid,
    total: rust_decimal::Decimal,
}

fn main() {
    let _ = <Ledger as sqlmodel::Model>::fields();
}
