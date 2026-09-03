-- op: upsert_do_nothing
-- dialect: sqlite
INSERT INTO "teams" ("id", "team_name", "motto") VALUES (?1, ?2, ?3) ON CONFLICT DO NOTHING
-- params: [BigInt(1), Text("ignored"), Null]
