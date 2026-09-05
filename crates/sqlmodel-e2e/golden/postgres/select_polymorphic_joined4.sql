-- op: select_polymorphic_joined4
-- dialect: postgres
SELECT "people"."id" AS "people__id", "people"."name" AS "people__name", "students"."id" AS "students__id", "students"."grade" AS "students__grade", "teachers"."id" AS "teachers__id", "teachers"."subject" AS "teachers__subject", "staff"."id" AS "staff__id", "staff"."office" AS "staff__office", "alumni"."id" AS "alumni__id", "alumni"."graduation_year" AS "alumni__graduation_year" FROM "people" LEFT JOIN "students" ON "people"."id" = "students"."id" LEFT JOIN "teachers" ON "people"."id" = "teachers"."id" LEFT JOIN "staff" ON "people"."id" = "staff"."id" LEFT JOIN "alumni" ON "people"."id" = "alumni"."id" WHERE "people"."name" LIKE $1 ORDER BY "people"."id" ASC
-- params: [Text("A%")]
