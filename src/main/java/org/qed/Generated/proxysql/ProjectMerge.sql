INSERT INTO mysql_query_rules (rule_id, active, match_pattern, replace_pattern)
VALUES (
  20, 1,
  '^SELECT (.*) FROM \(SELECT (.*) FROM (.*)\) AS (.*)',
  'SELECT \1 FROM \3'
);