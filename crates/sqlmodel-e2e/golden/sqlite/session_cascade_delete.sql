-- op: session_cascade_delete
-- dialect: sqlite
DELETE FROM "players" WHERE "team_id" IN (?1);
DELETE FROM "teams" WHERE "id" = ?1
-- params: [BigInt(1), BigInt(1)]
