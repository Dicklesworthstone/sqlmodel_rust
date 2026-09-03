-- op: insert_player_generated_id
-- dialect: postgres
INSERT INTO "players" ("id", "team_id", "name", "score", "active", "weight") VALUES (DEFAULT, $1, $2, $3, $4, $5)
-- params: [BigInt(1), Text("ann"), Int(7), Bool(true), Double(70.5)]
