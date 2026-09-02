//! Every `Expr` family executed through `select!` on every driver, with
//! driver-independent expectations. Anything a driver genuinely cannot do
//! (`ILIKE` outside PostgreSQL) is reported as skipped, never passed silently.
//!
//! The unit tests pin the SQL text these expressions render; this file proves
//! the databases agree about what that SQL means.

use asupersync::Cx;
use sqlmodel::prelude::*;
use sqlmodel::{Dialect, SchemaBuilder, Select};
use sqlmodel_e2e::{DriverUnderTest, Scenario, expect_outcome, run_on_every_driver};

#[derive(sqlmodel::Model, Debug, Clone, PartialEq)]
#[sqlmodel(table = "e2e_expr_items")]
struct Item {
    #[sqlmodel(primary_key)]
    id: i64,
    name: String,
    category: String,
    price: i32,
    #[sqlmodel(nullable)]
    weight: Option<i32>,
}

fn item(id: i64, name: &str, category: &str, price: i32, weight: Option<i32>) -> Item {
    Item {
        id,
        name: name.into(),
        category: category.into(),
        price,
        weight,
    }
}

fn items() -> Vec<Item> {
    vec![
        item(1, "gear", "tools", 120, Some(10)),
        item(2, "spring", "tools", 300, None),
        item(3, "bolt", "hardware", 15, Some(2)),
        item(4, "gasket", "hardware", 45, None),
        item(5, "oil", "fluids", 800, Some(500)),
        item(6, "gel", "fluids", 60, Some(1)),
    ]
}

/// Run `query` ordered by id and return the ids it produced.
async fn ids<C: Connection>(cx: &Cx, conn: &C, query: Select<Item>, label: &str) -> Vec<i64> {
    expect_outcome(
        query.order_by(Expr::col("id").asc()).all(cx, conn).await,
        label,
    )
    .into_iter()
    .map(|i| i.id)
    .collect()
}

/// Integers come back as different `Value` widths (and MySQL returns
/// `SUM(INT)` as a DECIMAL string); compare them numerically.
fn as_i64(v: &Value) -> i64 {
    // The aggregates here are small integers; rounding a float is exact.
    #[allow(clippy::cast_possible_truncation)]
    let rounded = |f: f64| f.round() as i64;
    match v {
        Value::Text(s) | Value::Decimal(s) => rounded(
            s.parse::<f64>()
                .unwrap_or_else(|_| panic!("not numeric: {s}")),
        ),
        Value::Double(f) => rounded(*f),
        Value::Float(f) => rounded(f64::from(*f)),
        other => other
            .as_i64()
            .unwrap_or_else(|| panic!("not numeric: {other:?}")),
    }
}

struct Expressions;

impl Scenario for Expressions {
    async fn run<C: Connection>(&self, cx: &Cx, conn: &C, driver: &DriverUnderTest) {
        let d = driver.name();
        let dialect = driver.dialect();
        let table = dialect.quote_identifier(<Item as Model>::TABLE_NAME);
        expect_outcome(
            conn.execute(cx, &format!("DROP TABLE IF EXISTS {table}"), &[])
                .await,
            &format!("{d}: drop stale"),
        );
        for stmt in SchemaBuilder::new()
            .dialect(dialect)
            .create_table::<Item>()
            .build()
        {
            expect_outcome(conn.execute(cx, &stmt, &[]).await, &format!("{d}: ddl"));
        }
        for it in items() {
            expect_outcome(
                insert!(&it).execute(cx, conn).await,
                &format!("{d}: insert {}", it.id),
            );
        }

        // Comparisons.
        let price = || Expr::col("price");
        assert_eq!(
            ids(cx, conn, select!(Item).filter(price().eq(300)), "eq").await,
            [2],
            "{d}"
        );
        assert_eq!(
            ids(cx, conn, select!(Item).filter(price().ne(300)), "ne").await,
            [1, 3, 4, 5, 6],
            "{d}"
        );
        assert_eq!(
            ids(cx, conn, select!(Item).filter(price().gt(100)), "gt").await,
            [1, 2, 5],
            "{d}"
        );
        assert_eq!(
            ids(cx, conn, select!(Item).filter(price().ge(120)), "ge").await,
            [1, 2, 5],
            "{d}"
        );
        assert_eq!(
            ids(cx, conn, select!(Item).filter(price().lt(50)), "lt").await,
            [3, 4],
            "{d}"
        );
        assert_eq!(
            ids(cx, conn, select!(Item).filter(price().le(60)), "le").await,
            [3, 4, 6],
            "{d}"
        );

        // NULL tests.
        assert_eq!(
            ids(
                cx,
                conn,
                select!(Item).filter(Expr::col("weight").is_null()),
                "is_null"
            )
            .await,
            [2, 4],
            "{d}"
        );
        assert_eq!(
            ids(
                cx,
                conn,
                select!(Item).filter(Expr::col("weight").is_not_null()),
                "is_not_null"
            )
            .await,
            [1, 3, 5, 6],
            "{d}"
        );

        // Pattern matching.
        assert_eq!(
            ids(
                cx,
                conn,
                select!(Item).filter(Expr::col("name").like("g%")),
                "like"
            )
            .await,
            [1, 4, 6],
            "{d}"
        );
        assert_eq!(
            ids(
                cx,
                conn,
                select!(Item).filter(Expr::col("name").starts_with("sp")),
                "starts_with"
            )
            .await,
            [2],
            "{d}"
        );
        assert_eq!(
            ids(
                cx,
                conn,
                select!(Item).filter(Expr::col("name").contains("el")),
                "contains"
            )
            .await,
            [6],
            "{d}"
        );
        if dialect == Dialect::Postgres {
            assert_eq!(
                ids(
                    cx,
                    conn,
                    select!(Item).filter(Expr::col("name").ilike("GA%")),
                    "ilike"
                )
                .await,
                [4],
                "{d}"
            );
        } else {
            eprintln!(
                "{d}: skipped ILIKE (PostgreSQL only; `Expr::ilike` is not translated for this dialect)"
            );
        }

        // Lists and ranges.
        assert_eq!(
            ids(
                cx,
                conn,
                select!(Item).filter(Expr::col("id").in_list(vec![1, 3, 5])),
                "in_list"
            )
            .await,
            [1, 3, 5],
            "{d}"
        );
        assert_eq!(
            ids(
                cx,
                conn,
                select!(Item).filter(price().between(40, 130)),
                "between"
            )
            .await,
            [1, 4, 6],
            "{d}"
        );
        assert_eq!(
            ids(
                cx,
                conn,
                select!(Item).filter(price().not_between(40, 130)),
                "not_between"
            )
            .await,
            [2, 3, 5],
            "{d}"
        );

        // Boolean composition.
        let composed = Expr::col("category")
            .eq("tools")
            .or(price().lt(20))
            .and(Expr::col("weight").is_null().not());
        assert_eq!(
            ids(cx, conn, select!(Item).filter(composed), "and/or/not").await,
            [1, 3],
            "{d}"
        );

        // String functions.
        assert_eq!(
            ids(
                cx,
                conn,
                select!(Item).filter(Expr::col("name").upper().eq("OIL")),
                "upper"
            )
            .await,
            [5],
            "{d}"
        );
        assert_eq!(
            ids(
                cx,
                conn,
                select!(Item).filter(Expr::col("category").lower().eq("fluids")),
                "lower"
            )
            .await,
            [5, 6],
            "{d}"
        );
        assert_eq!(
            ids(
                cx,
                conn,
                select!(Item).filter(Expr::col("name").length().gt(4)),
                "length"
            )
            .await,
            [2, 4],
            "{d}"
        );
        assert_eq!(
            ids(
                cx,
                conn,
                select!(Item).filter(Expr::col("name").concat("!").eq("bolt!")),
                "concat"
            )
            .await,
            [3],
            "{d}: string concatenation must not turn into a boolean OR"
        );

        // Numeric and conditional functions.
        assert_eq!(
            ids(
                cx,
                conn,
                select!(Item).filter(Expr::col("weight").abs().gt(5)),
                "abs"
            )
            .await,
            [1, 5],
            "{d}"
        );
        assert_eq!(
            ids(
                cx,
                conn,
                select!(Item)
                    .filter(Expr::coalesce(vec![Expr::col("weight"), Expr::from(0)]).lt(5)),
                "coalesce"
            )
            .await,
            [2, 3, 4, 6],
            "{d}"
        );
        assert_eq!(
            ids(
                cx,
                conn,
                select!(Item).filter(Expr::case().when(price().gt(100), 1).otherwise(0).eq(1)),
                "case"
            )
            .await,
            [1, 2, 5],
            "{d}"
        );

        // Aggregates with GROUP BY / HAVING, read as raw rows.
        let (sql, params) = select!(Item)
            .columns(&["category", "COUNT(*) AS n", "SUM(price) AS total"])
            .group_by(&["category"])
            .having(price().sum().gt(100))
            .order_by(Expr::col("category").asc())
            .build_with_dialect(dialect);
        let rows = expect_outcome(
            conn.query(cx, &sql, &params).await,
            &format!("{d}: group by"),
        );
        let groups: Vec<(String, i64, i64)> = rows
            .iter()
            .map(|r| {
                (
                    r.get_named::<String>("category").unwrap(),
                    as_i64(&r.get_named::<Value>("n").unwrap()),
                    as_i64(&r.get_named::<Value>("total").unwrap()),
                )
            })
            .collect();
        assert_eq!(
            groups,
            vec![
                ("fluids".to_string(), 2, 860),
                ("tools".to_string(), 2, 420)
            ],
            "{d}: GROUP BY / HAVING"
        );

        // DISTINCT.
        let (sql, params) = select!(Item)
            .distinct()
            .columns(&["category"])
            .order_by(Expr::col("category").asc())
            .build_with_dialect(dialect);
        let rows = expect_outcome(
            conn.query(cx, &sql, &params).await,
            &format!("{d}: distinct"),
        );
        let cats: Vec<String> = rows
            .iter()
            .map(|r| r.get_as::<String>(0).unwrap())
            .collect();
        assert_eq!(cats, ["fluids", "hardware", "tools"], "{d}");

        // ORDER BY DESC + LIMIT/OFFSET.
        let page: Vec<i64> = expect_outcome(
            select!(Item)
                .order_by(Expr::col("price").desc())
                .limit(2)
                .offset(1)
                .all(cx, conn)
                .await,
            &format!("{d}: paging"),
        )
        .into_iter()
        .map(|i| i.id)
        .collect();
        assert_eq!(page, [2, 1], "{d}: LIMIT 2 OFFSET 1 over price DESC");

        // Result-shape helpers.
        let one: Item = expect_outcome(
            select!(Item)
                .filter(Expr::col("id").eq(3))
                .one(cx, conn)
                .await,
            &format!("{d}: one"),
        );
        assert_eq!(one.name, "bolt", "{d}");
        let none: Option<Item> = expect_outcome(
            select!(Item)
                .filter(Expr::col("id").eq(99))
                .one_or_none(cx, conn)
                .await,
            &format!("{d}: one_or_none"),
        );
        assert!(none.is_none(), "{d}");
        let first: Option<Item> = expect_outcome(
            select!(Item)
                .filter(Expr::col("category").eq("tools"))
                .order_by(Expr::col("id").asc())
                .first(cx, conn)
                .await,
            &format!("{d}: first"),
        );
        assert_eq!(first.map(|i| i.id), Some(1), "{d}");
        let n = expect_outcome(
            select!(Item)
                .filter(Expr::col("category").eq("hardware"))
                .count(cx, conn)
                .await,
            &format!("{d}: count"),
        );
        assert_eq!(n, 2, "{d}: count()");

        // EXISTS subquery.
        let any_expensive = Select::<Item>::new().filter(price().gt(700)).into_exists();
        assert_eq!(
            ids(cx, conn, select!(Item).filter(any_expensive), "exists")
                .await
                .len(),
            6,
            "{d}: EXISTS true keeps every row"
        );
        let none_that_expensive = Select::<Item>::new().filter(price().gt(9000)).into_exists();
        assert!(
            ids(
                cx,
                conn,
                select!(Item).filter(none_that_expensive),
                "not exists"
            )
            .await
            .is_empty(),
            "{d}: EXISTS false removes every row"
        );

        expect_outcome(
            conn.execute(cx, &format!("DROP TABLE {table}"), &[]).await,
            &format!("{d}: drop"),
        );
    }
}

#[test]
fn every_expression_family_agrees_on_every_available_driver() {
    let cx = Cx::for_testing();
    let ran = run_on_every_driver(&cx, &Expressions);
    assert!(ran.contains(&"frankensqlite"), "{ran:?}");
}
