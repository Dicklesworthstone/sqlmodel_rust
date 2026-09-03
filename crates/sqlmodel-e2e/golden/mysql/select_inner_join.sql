-- op: select_inner_join
-- dialect: mysql
SELECT `players`.* FROM `players` INNER JOIN `teams` ON `players`.`team_id` = `teams`.`id` WHERE `teams`.`team_name` = ? ORDER BY `players`.`id` ASC
-- params: [Text("red")]
