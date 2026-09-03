-- op: update_with_subquery_returning
-- dialect: sqlite
UPDATE "players" SET "score" = ?1 WHERE "team_id" IN (SELECT id FROM "teams" WHERE "team_name" = ?2) RETURNING *
-- params: [Int(100), Text("green")]
