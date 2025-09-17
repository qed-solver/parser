SET GLOBAL rewriter_enabled = ON;

INSERT INTO query_rewrite.rewrite_rules (pattern, replacement, enabled, message)
VALUES(
    'SELECT * FROM ( SELECT * FROM test_table WHERE ? ) AS t1 WHERE ?',
    'SELECT * FROM test_table WHERE ? AND ?',
    'YES',
    'FilterMerge'
);

CALL query_rewrite.flush_rewrite_rules();

