-- op: select_eager_one_to_many
-- dialect: postgres
SELECT "teams"."id" AS "teams__id", "teams"."team_name" AS "teams__team_name", "teams"."motto" AS "teams__motto", "players"."id" AS "players__id", "players"."team_id" AS "players__team_id", "players"."name" AS "players__name", "players"."score" AS "players__score", "players"."active" AS "players__active", "players"."weight" AS "players__weight" FROM "teams" LEFT JOIN "players" ON "players"."team_id" = "teams"."id" ORDER BY "teams"."id" ASC
-- params: []
