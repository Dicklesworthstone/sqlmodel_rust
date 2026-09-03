-- op: select_left_join
-- dialect: postgres
SELECT "players".* FROM "players" LEFT JOIN "teams" ON "players"."team_id" = "teams"."id" WHERE "teams"."id" IS NULL
-- params: []
