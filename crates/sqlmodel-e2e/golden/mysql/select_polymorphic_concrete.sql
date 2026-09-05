-- op: select_polymorphic_concrete
-- dialect: mysql
SELECT `cti_articles`.`id` AS `id`, `cti_articles`.`title` AS `title`, `cti_articles`.`body` AS `body`, CAST(NULL AS SIGNED) AS `duration`, 'cti_articles' AS `__type` FROM `cti_articles` WHERE `title` LIKE ? UNION ALL SELECT `cti_videos`.`id` AS `id`, `cti_videos`.`title` AS `title`, CAST(NULL AS CHAR) AS `body`, `cti_videos`.`duration` AS `duration`, 'cti_videos' AS `__type` FROM `cti_videos` WHERE `title` LIKE ? ORDER BY `id` ASC LIMIT 25
-- params: [Text("R%"), Text("R%")]
