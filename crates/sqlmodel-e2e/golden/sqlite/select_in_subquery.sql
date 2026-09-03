-- op: select_in_subquery
-- dialect: sqlite
SELECT * FROM "players" WHERE "name" <> ?1 AND "team_id" IN (SELECT id FROM "teams" WHERE "team_name" = ?2)
-- params: [Text("ghost"), Text("red")]
