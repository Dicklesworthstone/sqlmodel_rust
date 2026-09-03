-- op: ddl_teams
-- dialect: sqlite
CREATE TABLE IF NOT EXISTS "teams" (
  "id" BIGINT NOT NULL,
    "team_name" TEXT NOT NULL,
    "motto" TEXT,
  PRIMARY KEY ("id"),
  CONSTRAINT "uk_teams_team_name" UNIQUE ("team_name")
)
-- params: []
