INSERT INTO mysql_query_rules (rule_id, active, match_pattern, replace_pattern)
VALUES (
  30, 1,
  '^SELECT \* FROM (.*) AS (.*?) INNER JOIN (.*) AS (.*?) ON (.*?)\.(.*?) = (.*?)\.(.*)',
  'SELECT * FROM \3 AS \4 INNER JOIN \1 AS \2 ON \7.\8 = \5.\6'
);