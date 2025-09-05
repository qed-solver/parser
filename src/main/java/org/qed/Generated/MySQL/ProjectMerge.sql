INSERT INTO query_rewrite.rewrite_rules
	(pattern, replacement) VALUES(
	'SELECT ? FROM (SELECT ? FROM (SELECT * FROM Source) AS t0) AS t1',
	'SELECT ?, ? FROM Source'
);