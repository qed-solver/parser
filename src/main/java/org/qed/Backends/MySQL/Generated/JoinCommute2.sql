INSERT INTO query_rewrite.rewrite_rules
  (pattern, replacement) VALUES(
  'SELECT * FROM (SELECT * FROM testdb.users) AS t0 INNER JOIN (SELECT * FROM testdb.users) AS t1 ON t0.status = t1.status',
  'SELECT * FROM (SELECT * FROM testdb.users) AS t1 INNER JOIN (SELECT * FROM testdb.users) AS t0 ON t1.status = t0.status'
);