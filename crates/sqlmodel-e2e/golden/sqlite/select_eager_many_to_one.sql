-- op: select_eager_many_to_one
-- dialect: sqlite
SELECT "players"."id" AS "players__id", "players"."team_id" AS "players__team_id", "players"."name" AS "players__name", "players"."score" AS "players__score", "players"."active" AS "players__active", "players"."weight" AS "players__weight", "teams"."id" AS "teams__id", "teams"."team_name" AS "teams__team_name", "teams"."motto" AS "teams__motto" FROM "players" LEFT JOIN "teams" ON "players"."team_id" = "teams"."id" WHERE "players"."id" IN (?1, ?2) ORDER BY "players"."id" ASC
-- params: [Int(1), Int(2)]
