-- op: insert_player_generated_id
-- dialect: mysql
INSERT INTO `players` (`id`, `team_id`, `name`, `score`, `active`, `weight`) VALUES (DEFAULT, ?, ?, ?, ?, ?)
-- params: [BigInt(1), Text("ann"), Int(7), Bool(true), Double(70.5)]
