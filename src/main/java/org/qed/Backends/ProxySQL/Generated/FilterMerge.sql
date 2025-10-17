INSERT INTO mysql_query_rules (rule_id, active, match_pattern, replace_pattern)
VALUES (
  10, 1,
  '^SELECT \* FROM \(SELECT \* FROM (.*) WHERE (.*) = (.*)\) AS (.*) WHERE (.*) = (.*)',
  'SELECT * FROM \1 WHERE \2 = \3 AND \5 = \6'
);