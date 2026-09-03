-- op: select_group_by_having
-- dialect: postgres
SELECT team_id, COUNT(*), SUM(score) FROM "players" GROUP BY team_id HAVING COUNT(*) > $1
-- params: [Int(1)]
