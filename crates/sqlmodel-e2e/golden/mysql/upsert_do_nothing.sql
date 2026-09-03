-- op: upsert_do_nothing
-- dialect: mysql
INSERT IGNORE INTO `teams` (`id`, `team_name`, `motto`) VALUES (?, ?, ?)
-- params: [BigInt(1), Text("ignored"), Null]
