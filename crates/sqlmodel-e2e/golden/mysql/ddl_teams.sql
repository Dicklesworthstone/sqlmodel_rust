-- op: ddl_teams
-- dialect: mysql
CREATE TABLE IF NOT EXISTS `teams` (
  `id` BIGINT NOT NULL,
    `team_name` VARCHAR(255) NOT NULL,
    `motto` TEXT COMMENT 'free text',
  PRIMARY KEY (`id`),
  CONSTRAINT `uk_teams_team_name` UNIQUE (`team_name`)
)
-- params: []
