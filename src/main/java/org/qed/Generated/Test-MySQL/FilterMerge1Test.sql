SELECT * FROM (SELECT * FROM testdb.users WHERE id = 1) AS t0
WHERE status = 'active';