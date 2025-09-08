INSERT INTO query_rewrite.rewrite_rules
  (pattern, replacement) VALUES(
  '(SELECT * FROM testdb.users) AS t0 INNER JOIN (SELECT * FROM testdb.users) AS t1 ON t0.id = t1.id',
  '(SELECT * FROM testdb.users) AS t1 INNER JOIN (SELECT * FROM testdb.users) AS t0 ON t1.id = t0.id'
);