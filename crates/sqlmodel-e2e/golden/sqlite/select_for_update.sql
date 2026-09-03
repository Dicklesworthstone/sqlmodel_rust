-- op: select_for_update
-- dialect: sqlite
SELECT * FROM "teams" WHERE "id" = ?1 FOR UPDATE
-- params: [Int(1)]
