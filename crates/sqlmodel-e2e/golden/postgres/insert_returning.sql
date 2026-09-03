-- op: insert_returning
-- dialect: postgres
INSERT INTO "teams" ("id", "team_name", "motto") VALUES ($1, $2, $3) RETURNING *
-- params: [BigInt(2), Text("blue"), Null]
