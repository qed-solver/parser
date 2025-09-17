SET GLOBAL rewriter_enabled = ON;

INSERT INTO query_rewrite.rewrite_rules (pattern, replacement)
VALUES('SELECT ?', 'SELECT ? + 1');

CALL query_rewrite.flush_rewrite_rules();