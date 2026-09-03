-- op: delete_filter
-- dialect: mysql
DELETE FROM `players` WHERE `active` = ? AND `score` < ?
-- params: [Bool(false), Int(5)]
