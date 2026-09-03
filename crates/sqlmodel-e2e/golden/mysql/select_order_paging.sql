-- op: select_order_paging
-- dialect: mysql
SELECT * FROM `players` ORDER BY `score` DESC, `id` ASC LIMIT 10 OFFSET 20
-- params: []
