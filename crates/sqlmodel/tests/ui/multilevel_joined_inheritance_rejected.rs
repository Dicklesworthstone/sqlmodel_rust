//! Multi-level joined inheritance must fail at compile time (bd-kzp1.5).

#[derive(sqlmodel::Model)]
#[sqlmodel(table, inheritance = "joined")]
struct Person {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
}

#[derive(sqlmodel::Model)]
#[sqlmodel(table, inherits = "Person")]
struct Employee {
    #[sqlmodel(parent)]
    person: Person,
    #[sqlmodel(primary_key)]
    id: i64,
    department: String,
}

#[derive(sqlmodel::Model)]
#[sqlmodel(table, inherits = "Employee")]
struct Manager {
    #[sqlmodel(parent)]
    employee: Employee,
    #[sqlmodel(primary_key)]
    id: i64,
    bonus: f64,
}

fn main() {}
