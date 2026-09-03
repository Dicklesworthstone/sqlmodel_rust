-- op: upsert_do_update
-- dialect: sqlite
INSERT INTO "teams" ("id", "team_name", "motto") VALUES (?1, ?2, ?3) ON CONFLICT ("id") DO UPDATE SET "team_name" = EXCLUDED."team_name", "motto" = EXCLUDED."motto"
-- params: [BigInt(1), Text("crimson"), Null]
