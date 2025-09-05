INSERT INTO query_rewrite.rewrite_rules
	(pattern, replacement) VALUES(
	'SELECT * FROM (SELECT * FROM Source WHERE ? = ?) AS t0 WHERE ? = ?',
	'SELECT * FROM Source WHERE ? = ? AND ? = ?'
);