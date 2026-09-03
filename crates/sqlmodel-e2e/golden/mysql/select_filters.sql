-- op: select_filters
-- dialect: mysql
SELECT * FROM `players` WHERE `team_id` = ? AND (`score` > ? OR `active` = ?) AND `name` LIKE ? AND `id` IN (?, ?, ?) AND `score` BETWEEN ? AND ? AND `weight` IS NOT NULL AND NOT `motto` IS NULL
-- params: [Int(1), Int(10), Bool(true), Text("a%"), Int(1), Int(2), Int(3), Int(1), Int(100)]
