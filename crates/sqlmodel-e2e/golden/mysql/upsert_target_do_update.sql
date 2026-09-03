-- op: upsert_target_do_update
-- dialect: mysql
INSERT INTO `teams` (`id`, `team_name`, `motto`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `motto` = VALUES(`motto`)
-- params: [BigInt(9), Text("red"), Null]
