-- op: insert_player_generated_id
-- dialect: sqlite
INSERT INTO "players" ("team_id", "name", "score", "active", "weight") VALUES (?1, ?2, ?3, ?4, ?5)
-- params: [BigInt(1), Text("ann"), Int(7), Bool(true), Double(70.5)]
