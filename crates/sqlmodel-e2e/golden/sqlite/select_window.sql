-- op: select_window
-- dialect: sqlite
SELECT id, team_id, ROW_NUMBER() OVER (PARTITION BY "team_id" ORDER BY "score" DESC) FROM "players" ORDER BY "team_id" ASC
-- params: []
