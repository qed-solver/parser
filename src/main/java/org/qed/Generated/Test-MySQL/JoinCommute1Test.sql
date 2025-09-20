SELECT * FROM (SELECT * FROM testdb.users) AS t0
INNER JOIN (SELECT * FROM testdb.users) AS t1
ON t0.id = t1.id;
