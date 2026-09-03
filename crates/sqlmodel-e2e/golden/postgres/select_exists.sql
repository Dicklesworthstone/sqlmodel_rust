-- op: select_exists
-- dialect: postgres
SELECT * FROM "teams" WHERE EXISTS (SELECT 1 FROM "players" WHERE players.team_id = teams.id AND "score" > $1)
-- params: [Int(100)]
