-- op: ddl_players
-- dialect: mysql
CREATE TABLE IF NOT EXISTS `players` (
  `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `team_id` BIGINT NOT NULL,
    `name` VARCHAR(40) NOT NULL,
    `score` INTEGER NOT NULL DEFAULT 0,
    `active` BOOLEAN NOT NULL,
    `weight` DOUBLE,
  CONSTRAINT `fk_players_team_id` FOREIGN KEY (`team_id`) REFERENCES `teams`(`id`) ON DELETE CASCADE
);
CREATE INDEX `players_name_idx` ON `players` (`name`)
-- params: []
