INSERT INTO mysql_query_rules (rule_id, active, match_pattern, replace_pattern)
VALUES (
  50, 1,
  '^SELECT (.*) FROM (.*) WHERE TRUE',
  'SELECT \1 FROM \2'
);