-- op: upsert_do_update
-- dialect: mysql
INSERT INTO `teams` (`id`, `team_name`, `motto`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `team_name` = VALUES(`team_name`), `motto` = VALUES(`motto`)
-- params: [BigInt(1), Text("crimson"), Null]
