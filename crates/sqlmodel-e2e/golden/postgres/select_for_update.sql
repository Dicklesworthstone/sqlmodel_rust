-- op: select_for_update
-- dialect: postgres
SELECT * FROM "teams" WHERE "id" = $1 FOR UPDATE
-- params: [Int(1)]
