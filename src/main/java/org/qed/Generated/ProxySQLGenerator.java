package org.qed.Generated;

import org.qed.Generated.RRuleInstances.JoinCommute;
import org.qed.RelRN;
import org.qed.RexRN;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ProxySQLGenerator {

    private final Map<RexRN, Integer> predicateToGroupIndex = new HashMap<>();
    private final AtomicInteger groupCounter = new AtomicInteger(1);
    private boolean isReduceTrueRule = false;

    public String translate(int ruleId, String name, RelRN before, RelRN after) {
        predicateToGroupIndex.clear();
        groupCounter.set(1);
        this.isReduceTrueRule = false;

        String matchPattern = generateMatchPattern(before);
        String replacePattern = generateReplacePattern(after);

        return String.format(
                """
                INSERT INTO mysql_query_rules (rule_id, active, match_pattern, replace_pattern)
                VALUES (
                  %d, 1,
                  '^%s',
                  '%s'
                );""",
                ruleId, matchPattern, replacePattern
        );
    }

    private String generateMatchPattern(RelRN node) {
        return switch (node) {
            case RelRN.Filter filter -> {
                if (filter.cond() instanceof RexRN.True) {
                    this.isReduceTrueRule = true;
                    groupCounter.addAndGet(2);
                    yield "SELECT (.*) FROM (.*) WHERE TRUE";
                }

                if (filter.cond() instanceof RexRN.False) {
                    groupCounter.addAndGet(2);
                    yield "SELECT (.*) FROM (.*) WHERE FALSE";
                }

                String sourcePattern = generateMatchPattern(filter.source());
                String conditionRegex = "(.*) = (.*)";
                if (filter.source() instanceof RelRN.Scan) {
                    int conditionGroupStart = groupCounter.get();
                    groupCounter.addAndGet(2);
                    predicateToGroupIndex.put(filter.cond(), conditionGroupStart);
                    yield sourcePattern + " WHERE " + conditionRegex;
                } else {
                    groupCounter.getAndIncrement();
                    int conditionGroupStart = groupCounter.get();
                    groupCounter.addAndGet(2);
                    predicateToGroupIndex.put(filter.cond(), conditionGroupStart);
                    yield String.format("SELECT \\* FROM \\(%s\\) AS (.*) WHERE %s", sourcePattern, conditionRegex);
                }
            }
            case RelRN.Project project -> {
                if (project.source() instanceof RelRN.Project innerProject && innerProject.source() instanceof RelRN.Scan) {
                    yield "SELECT (.*) FROM \\(SELECT (.*) FROM (.*)\\) AS (.*)";
                }
                throw new UnsupportedOperationException("This generator only supports the specific Project(Project(Scan)) pattern.");
            }
            case RelRN.Join join -> {
                if (join.left() instanceof RelRN.Scan && join.right() instanceof RelRN.Scan) {
                    yield "SELECT \\* FROM (.*) AS (.*?) INNER JOIN (.*) AS (.*?) ON (.*?)\\.(.*?) = (.*?)\\.(.*)";
                }
                throw new UnsupportedOperationException("This generator only supports simple Scan-Join-Scan patterns.");
            }
            case RelRN.Scan scan -> {
                groupCounter.getAndIncrement();
                yield "SELECT \\* FROM (.*)";
            }
            default -> throw new UnsupportedOperationException("Unsupported RelRN for match pattern: " + node.getClass().getSimpleName());
        };
    }

    private String generateReplacePattern(RelRN node) {
        return switch (node) {
            case RelRN.Empty empty -> {
                yield "SELECT \\1 FROM \\2 LIMIT 0";
            }
            case JoinCommute.ProjectionRelRN proj -> {
                if (proj.source() instanceof RelRN.Join) {
                    yield "SELECT * FROM \\3 AS \\4 INNER JOIN \\1 AS \\2 ON \\7.\\8 = \\5.\\6";
                }
                throw new UnsupportedOperationException("Unsupported 'after' pattern for JoinCommute.");
            }
            case RelRN.Filter filter -> {
                String fromClause = generateReplacePattern(filter.source());
                String whereClause = buildWhereClause(filter.cond());
                yield String.format("%s WHERE %s", fromClause, whereClause);
            }
            case RelRN.Project project -> {
                if (project.source() instanceof RelRN.Scan) {
                    yield "SELECT \\1 FROM \\3";
                }
                throw new UnsupportedOperationException("Unsupported 'after' pattern for ProjectMerge.");
            }
            case RelRN.Scan scan -> {
                if (this.isReduceTrueRule) {
                    yield "SELECT \\1 FROM \\2";
                } else {
                    yield "SELECT * FROM \\1";
                }
            }
            default -> throw new UnsupportedOperationException("Unsupported RelRN for replace pattern: " + node.getClass().getSimpleName());
        };
    }

    private String buildWhereClause(RexRN condition) {
        return switch (condition) {
            case RexRN.And andNode -> andNode.sources().stream()
                    .map(this::buildWhereClause)
                    .collect(Collectors.joining(" AND "));
            case RexRN.Pred pred -> {
                Integer groupIndex = predicateToGroupIndex.get(pred);
                if (groupIndex == null) {
                    throw new IllegalStateException("Predicate from 'after' tree not found in 'before' tree: " + pred);
                }
                yield String.format("\\%d = \\%d", groupIndex, groupIndex + 1);
            }
            default -> throw new UnsupportedOperationException("Unsupported RexRN for WHERE clause: " + condition.getClass().getSimpleName());
        };
    }
}