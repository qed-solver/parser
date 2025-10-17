INSERT INTO query_rewrite.rewrite_rules
  (pattern, replacement) VALUES(
  'SELECT * FROM (SELECT * FROM testdb.users WHERE id = ?) AS t0 WHERE status = ?',
  'SELECT * FROM testdb.users WHERE id = ? AND status = ?'
);