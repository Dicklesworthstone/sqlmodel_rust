-- op: insert_team
-- dialect: mysql
INSERT INTO `teams` (`id`, `team_name`, `motto`) VALUES (?, ?, ?)
-- params: [BigInt(1), Text("red"), Text("go")]
