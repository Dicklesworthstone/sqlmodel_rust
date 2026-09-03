-- op: update_model
-- dialect: mysql
UPDATE `teams` SET `team_name` = ?, `motto` = ? WHERE `id` = ?
-- params: [Text("crimson"), Text("still red"), BigInt(1)]
