INSERT INTO mysql_query_rules (rule_id, active, match_pattern, replace_pattern)
VALUES (
  40, 1,
  '^SELECT (.*) FROM (.*) WHERE FALSE',
  'SELECT \1 FROM \2 LIMIT 0'
);