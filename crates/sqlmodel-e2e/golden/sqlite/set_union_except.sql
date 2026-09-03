-- op: set_union_except
-- dialect: sqlite
(SELECT id FROM players WHERE team_id = ?) UNION (SELECT id FROM players WHERE active = ?) EXCEPT (SELECT id FROM players WHERE score < ?) ORDER BY "id" ASC LIMIT 50
-- params: [Int(1), Bool(true), Int(5)]
