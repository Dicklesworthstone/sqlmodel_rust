-- op: update_model
-- dialect: sqlite
UPDATE "teams" SET "team_name" = ?1, "motto" = ?2 WHERE "id" = ?3
-- params: [Text("crimson"), Text("still red"), BigInt(1)]
