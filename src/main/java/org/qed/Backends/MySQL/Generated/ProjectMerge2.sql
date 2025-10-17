INSERT INTO query_rewrite.rewrite_rules
  (pattern, replacement) VALUES(
  'SELECT status, id FROM (SELECT status, id FROM testdb.users) AS t0',
  'SELECT status, id FROM testdb.users'
);