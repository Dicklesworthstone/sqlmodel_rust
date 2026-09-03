-- op: select_for_update
-- dialect: mysql
SELECT * FROM `teams` WHERE `id` = ? FOR UPDATE
-- params: [Int(1)]
