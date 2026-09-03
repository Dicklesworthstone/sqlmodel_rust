-- op: upsert_target_do_update
-- dialect: postgres
INSERT INTO "teams" ("id", "team_name", "motto") VALUES ($1, $2, $3) ON CONFLICT ("team_name") DO UPDATE SET "motto" = EXCLUDED."motto"
-- params: [BigInt(9), Text("red"), Null]
