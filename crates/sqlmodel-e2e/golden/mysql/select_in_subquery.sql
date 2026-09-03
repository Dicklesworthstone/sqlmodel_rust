-- op: select_in_subquery
-- dialect: mysql
SELECT * FROM `players` WHERE `name` <> ? AND `team_id` IN (SELECT id FROM `teams` WHERE `team_name` = ?)
-- params: [Text("ghost"), Text("red")]
