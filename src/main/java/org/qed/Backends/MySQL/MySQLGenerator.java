package org.qed.Backends.MySQL;

import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRuleInstances.JoinCommute;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
        String beforeSQL;
        String afterSQL;

        if (name.equals("JoinCommute")) {
            subqueryCounter = 0;
            beforeSQL = transformNested(before, true, false, new AtomicInteger(0));
            subqueryCounter = 0;
            afterSQL = transformNested(before, true, true, new AtomicInteger(0));
        } else {
            subqueryCounter = 0;
            beforeSQL = transformNested(before, true, false, new AtomicInteger(0));
            afterSQL = transformFlatten(after);
        }

        return "INSERT INTO query_rewrite.rewrite_rules\n" +
                "  (pattern, replacement) VALUES(\n" +
                "  '" + beforeSQL + "',\n" +
                "  '" + afterSQL + "'\n" +
                ");";
    }

    private String transformNested(RelRN node, boolean isRoot, boolean swapJoinSides, AtomicInteger filterIndex) {
        if (node instanceof RelRN.Scan) {
            return "SELECT * FROM " + tableName;
        } else if (node instanceof RelRN.Project project) {
            String cols = String.join(", ", columnNames);
            if (project.source() instanceof RelRN.Scan) {
                return "SELECT " + cols + " FROM " + tableName;
            }
            String innerSQL = transformNested(project.source(), false, swapJoinSides, filterIndex);
            String alias = "t" + (subqueryCounter++);
            return "SELECT " + cols + " FROM (" + innerSQL + ") AS " + alias;
        } else if (node instanceof RelRN.Filter filter) {
            String innerSQL = transformNested(filter.source(), false, swapJoinSides, filterIndex);
            int currentIndex = filterIndex.getAndIncrement();
            String condition = (currentIndex < columnNames.size())
                    ? columnNames.get(currentIndex) + " = ?"
                    : columnNames.get(0) + " = ?";

            if (isRoot) {
                return innerSQL + " WHERE " + condition;
            } else {
                String alias = "t" + (subqueryCounter++);
                return "SELECT * FROM (" + innerSQL + " WHERE " + condition + ") AS " + alias;
            }
        } else if (node instanceof RelRN.Join join) {
            String leftAlias = "t0";
            String rightAlias = "t1";

            RelRN firstNode = swapJoinSides ? join.right() : join.left();
            String firstAlias = swapJoinSides ? rightAlias : leftAlias;
            RelRN secondNode = swapJoinSides ? join.left() : join.right();
            String secondAlias = swapJoinSides ? leftAlias : rightAlias;

            String firstSQL = "(" + transformNested(firstNode, false, swapJoinSides, filterIndex) + ")";
            String secondSQL = "(" + transformNested(secondNode, false, swapJoinSides, filterIndex) + ")";

            String joinCond = renderJoinCondition(join.cond(), leftAlias, rightAlias, swapJoinSides);

            String joinExpr =
                    firstSQL + " AS " + firstAlias +
                            " " + join.ty().semantics().name() + " JOIN " +
                            secondSQL + " AS " + secondAlias +
                            " ON " + joinCond;

            if (isRoot) {
                return "SELECT * FROM " + joinExpr;
            } else {
                String alias = "t" + (subqueryCounter++);
                return "SELECT * FROM (" + joinExpr + ") AS " + alias;
            }

        } else if (node instanceof JoinCommute.ProjectionRelRN projRN) {
            return transformNested(projRN.source(), isRoot, swapJoinSides, filterIndex);
        } else {
            throw new UnsupportedOperationException("Unsupported RelRN: " + node);
        }
    }

    private String renderJoinCondition(RexRN cond, String leftAlias, String rightAlias, boolean swap) {
        if (cond instanceof RexRN.Pred p) {
            if (p.sources().get(0) instanceof RexRN.JoinField jf) {
                String colName = columnNames.get(jf.ordinal());
                String first = swap ? rightAlias : leftAlias;
                String second = swap ? leftAlias : rightAlias;
                return first + "." + colName + " = " + second + "." + colName;
            }
        }
        throw new UnsupportedOperationException("Unsupported join condition: " + cond);
    }

    public String transformFlatten(RelRN node) {
        FlattenedSQLParts parts = new FlattenedSQLParts();
        collectFlattenedParts(node, parts);
        String selectClause = parts.projections.isEmpty() ? "SELECT *" : "SELECT " + String.join(", ", parts.projections);
        String whereClause = parts.conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", parts.conditions);
        return selectClause + " FROM " + parts.fromClause + whereClause;
    }

    private void collectFlattenedParts(RelRN node, FlattenedSQLParts parts) {
        switch (node) {
            case RelRN.Scan scan -> parts.fromClause = tableName;
            case RelRN.Project project -> {
                collectFlattenedParts(project.source(), parts);
                parts.projections.addAll(columnNames);
            }
            case RelRN.Filter filter -> {
                collectFlattenedParts(filter.source(), parts);
                collectPredConditions(filter.cond(), parts.conditions);
            }
            default -> throw new UnsupportedOperationException("Unsupported RelRN for flatten: " + node);
        }
    }

    private void collectPredConditions(RexRN pred, List<String> conditions) {
        if (pred instanceof RexRN.Pred) {
            int currentConditions = conditions.size();
            if (currentConditions < columnNames.size()) {
                conditions.add(columnNames.get(currentConditions) + " = ?");
            }
        } else if (pred instanceof RexRN.And and) {
            for (RexRN child : and.sources()) {
                collectPredConditions(child, conditions);
            }
        }
    }
}