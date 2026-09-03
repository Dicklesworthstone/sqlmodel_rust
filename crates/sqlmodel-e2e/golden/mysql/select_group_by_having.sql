-- op: select_group_by_having
-- dialect: mysql
SELECT team_id, COUNT(*), SUM(score) FROM `players` GROUP BY team_id HAVING COUNT(*) > ?
-- params: [Int(1)]
