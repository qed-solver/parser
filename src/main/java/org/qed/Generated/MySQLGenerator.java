package org.qed.Generated;

import org.qed.RelRN;
import org.qed.RexRN;

import java.util.ArrayList;
import java.util.List;

public class MySQLGenerator {

    private int subqueryCounter = 0;
    private final String tableName;
    private final List<String> columnNames;

    public MySQLGenerator(String tableName, List<String> columnNames) {
        this.tableName = tableName;
        this.columnNames = columnNames;
    }

    private static class FlattenedSQLParts {
        String fromClause = "";
        List<String> projections = new ArrayList<>();
        List<String> conditions = new ArrayList<>();
    }

    public String translate(String name, RelRN before, RelRN after) {
        subqueryCounter = 0;

        String beforeSQL = transformNested(before, true);
        String afterSQL = transformFlatten(after);

        return "INSERT INTO query_rewrite.rewrite_rules\n" +
                "  (pattern, replacement) VALUES(\n" +
                "  '" + beforeSQL + "',\n" +
                "  '" + afterSQL + "'\n" +
                ");";
    }

    public String transformNested(RelRN node, boolean isRoot) {
        return switch (node) {
            case RelRN.Scan scan -> "SELECT * FROM " + tableName;
            case RelRN.Project project -> {
                String alias = "t" + (subqueryCounter++);
                String cols = String.join(", ", columnNames);
                if (project.source() instanceof RelRN.Scan) {
                    yield "SELECT " + cols + " FROM (SELECT " + cols + " FROM " + tableName + ") AS " + alias;
                } else if (project.source() instanceof RelRN.Project) {
                    yield "SELECT " + cols + " FROM (SELECT " + cols + " FROM " + tableName + ") AS " + alias;
                } else {
                    String innerSQL = transformNested(project.source(), false);
                    yield "SELECT " + cols + " FROM (" + innerSQL + ") AS " + alias;
                }
            }
            case RelRN.Filter filter -> {
                String innerSQL = transformNested(filter.source(), false);
                String condSQL = columnNames.get(0) + " = ?";
                if (isRoot) {
                    yield innerSQL + " WHERE " + columnNames.get(1) + " = ?";
                } else {
                    String alias = "t" + (subqueryCounter++);
                    yield "SELECT * FROM (" + innerSQL + " WHERE " + condSQL + ") AS " + alias;
                }
            }
            default -> throw new UnsupportedOperationException("Unsupported RelRN: " + node);
        };
    }

    public String transformFlatten(RelRN node) {
        FlattenedSQLParts parts = new FlattenedSQLParts();
        collectFlattenedParts(node, parts);
        String selectClause = parts.projections.isEmpty()
                ? "SELECT *"
                : "SELECT " + String.join(", ", parts.projections);

        String whereClause = parts.conditions.isEmpty()
                ? ""
                : " WHERE " + String.join(" AND ", parts.conditions);
        return selectClause + " FROM " + parts.fromClause + whereClause;
    }

    private void collectFlattenedParts(RelRN node, FlattenedSQLParts parts) {
        switch (node) {
            case RelRN.Scan scan -> parts.fromClause = tableName;
            case RelRN.Project project -> {
                collectFlattenedParts(project.source(), parts);
                addColumnNames(parts.projections);
            }
            case RelRN.Filter filter -> {
                collectFlattenedParts(filter.source(), parts);
                collectPredConditions(filter.cond(), parts.conditions);
            }
            default -> throw new UnsupportedOperationException("Unsupported RelRN: " + node);
        }
    }

    private void addColumnNames(List<String> projections) {
        projections.addAll(columnNames);
    }

    private void collectPredConditions(RexRN pred, List<String> conditions) {
        switch (pred) {
            case RexRN.Pred p -> {
                if (conditions.isEmpty()) {
                    conditions.add(columnNames.get(0) + " = ?");
                } else if (conditions.size() == 1 && columnNames.size() > 1) {
                    conditions.add(columnNames.get(1) + " = ?");
                } else {
                    conditions.add(columnNames.get(0) + " = ?");
                }
            }
            case RexRN.And and -> {
                for (RexRN child : and.sources()) {
                    collectPredConditions(child, conditions);
                }
            }
            default -> throw new UnsupportedOperationException("Unsupported RexRN: " + pred);
        }
    }

}
