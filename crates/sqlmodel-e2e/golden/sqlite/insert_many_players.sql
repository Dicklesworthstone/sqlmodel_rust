-- op: insert_many_players
-- dialect: sqlite
INSERT INTO "players" ("id", "team_id", "name", "score", "active", "weight") VALUES (?1, ?2, ?3, ?4, ?5, ?6), (?7, ?8, ?9, ?10, ?11, ?12)
-- params: [BigInt(10), BigInt(1), Text("bob"), Int(7), Bool(true), Double(70.5), BigInt(11), BigInt(2), Text("cy"), Int(7), Bool(true), Double(70.5)]
