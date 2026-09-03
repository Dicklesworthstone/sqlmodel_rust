-- op: cte_with
-- dialect: sqlite
WITH "top_players" ("id", "score") AS (SELECT id, score FROM players WHERE score > ?) SELECT * FROM top_players ORDER BY score DESC
-- params: [Int(50)]
