-- op: update_set_filter
-- dialect: postgres
UPDATE "players" SET "score" = $1, "active" = $2 WHERE "team_id" = $3
-- params: [Int(0), Bool(false), Int(2)]
