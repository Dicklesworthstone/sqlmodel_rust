-- op: insert_returning
-- dialect: mysql
INSERT INTO `teams` (`id`, `team_name`, `motto`) VALUES (?, ?, ?) RETURNING *
-- params: [BigInt(2), Text("blue"), Null]
