-- op: select_polymorphic_joined
-- dialect: mysql
SELECT `people`.`id` AS `people__id`, `people`.`name` AS `people__name`, `students`.`id` AS `students__id`, `students`.`grade` AS `students__grade` FROM `people` LEFT JOIN `students` ON `people`.`id` = `students`.`id` WHERE `people`.`name` LIKE ? ORDER BY `people`.`id` ASC
-- params: [Text("A%")]
