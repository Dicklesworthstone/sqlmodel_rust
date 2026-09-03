-- op: insert_team
-- dialect: sqlite
INSERT INTO "teams" ("id", "team_name", "motto") VALUES (?1, ?2, ?3)
-- params: [BigInt(1), Text("red"), Text("go")]
