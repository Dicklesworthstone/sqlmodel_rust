-- op: select_joined_child
-- dialect: sqlite
SELECT "students"."id" AS "students__id", "students"."grade" AS "students__grade", "people"."id" AS "people__id", "people"."name" AS "people__name" FROM "students" INNER JOIN "people" ON "students"."id" = "people"."id" WHERE "grade" = ?1
-- params: [Text("A")]
