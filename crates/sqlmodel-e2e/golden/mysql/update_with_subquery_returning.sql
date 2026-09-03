-- op: update_with_subquery_returning
-- dialect: mysql
UPDATE `players` SET `score` = ? WHERE `team_id` IN (SELECT id FROM `teams` WHERE `team_name` = ?) RETURNING *
-- params: [Int(100), Text("green")]
