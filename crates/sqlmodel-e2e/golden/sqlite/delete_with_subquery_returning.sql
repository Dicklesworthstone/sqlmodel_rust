-- op: delete_with_subquery_returning
-- dialect: sqlite
DELETE FROM "players" WHERE "team_id" NOT IN (SELECT id FROM "teams") RETURNING *
-- params: []
