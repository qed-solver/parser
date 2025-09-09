-- MySQL Query Rewriter Rule: FilterMerge
-- Auto-generated from RRule instance

-- Enable the rewriter
SET GLOBAL rewriter_enabled = ON;

-- Clear any existing rule with same message
DELETE FROM query_rewrite.rewrite_rules WHERE message = 'FilterMerge';

-- Insert the rewrite rule
INSERT INTO query_rewrite.rewrite_rules (pattern, replacement, enabled, message)
VALUES(
    'SELECT * FROM ( SELECT * FROM test_table WHERE ? ) AS t1 WHERE ?',
    'SELECT * FROM test_table WHERE ? AND ?',
    'YES',
    'FilterMerge'
);

-- Load the rule
CALL query_rewrite.flush_rewrite_rules();

-- Verify the rule
SELECT pattern, replacement, enabled, message
FROM query_rewrite.rewrite_rules
WHERE message = 'FilterMerge';
