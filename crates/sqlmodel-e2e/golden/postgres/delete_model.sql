-- op: delete_model
-- dialect: postgres
DELETE FROM "teams" WHERE "id" = $1
-- params: [BigInt(1)]
