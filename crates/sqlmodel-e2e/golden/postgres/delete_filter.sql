-- op: delete_filter
-- dialect: postgres
DELETE FROM "players" WHERE "active" = $1 AND "score" < $2
-- params: [Bool(false), Int(5)]
