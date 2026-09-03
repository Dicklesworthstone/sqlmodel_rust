-- op: select_exists
-- dialect: mysql
SELECT * FROM `teams` WHERE EXISTS (SELECT 1 FROM `players` WHERE players.team_id = teams.id AND `score` > ?)
-- params: [Int(100)]
