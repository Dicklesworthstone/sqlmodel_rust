-- op: select_filters
-- dialect: postgres
SELECT * FROM "players" WHERE "team_id" = $1 AND ("score" > $2 OR "active" = $3) AND "name" LIKE $4 AND "id" IN ($5, $6, $7) AND "score" BETWEEN $8 AND $9 AND "weight" IS NOT NULL AND NOT "motto" IS NULL
-- params: [Int(1), Int(10), Bool(true), Text("a%"), Int(1), Int(2), Int(3), Int(1), Int(100)]
