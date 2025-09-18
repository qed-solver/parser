INSERT INTO query_rewrite.rewrite_rules
  (pattern, replacement) VALUES(
  'SELECT * FROM (SELECT * FROM testdb.users WHERE status = ?) AS t0 WHERE id = ?',
  'SELECT * FROM testdb.users WHERE status = ? AND id = ?'
);