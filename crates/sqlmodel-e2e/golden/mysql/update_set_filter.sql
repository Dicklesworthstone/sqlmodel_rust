-- op: update_set_filter
-- dialect: mysql
UPDATE `players` SET `score` = ?, `active` = ? WHERE `team_id` = ?
-- params: [Int(0), Bool(false), Int(2)]
