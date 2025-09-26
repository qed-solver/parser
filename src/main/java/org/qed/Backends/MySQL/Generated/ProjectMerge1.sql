INSERT INTO query_rewrite.rewrite_rules
  (pattern, replacement) VALUES(
  'SELECT id, status FROM (SELECT id, status FROM testdb.users) AS t0',
  'SELECT id, status FROM testdb.users'
);