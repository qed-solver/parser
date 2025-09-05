package org.qed.Generated;

import org.qed.RelRN;
import org.qed.RexRN;

import java.util.ArrayList;
import java.util.List;

public class MySQLGenerator {

    private int subqueryCounter = 0;

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
                "\t(pattern, replacement) VALUES(\n" +
                "\t'" + beforeSQL + "',\n" +
                "\t'" + afterSQL + "'\n" +
                ");";
    }

    public String transformNested(RelRN node, boolean isRoot) {
        return switch (node) {
            case RelRN.Scan scan -> "SELECT * FROM " + scan.name();
            case RelRN.Project project -> {
                String innerSQL = transformNested(project.source(), false);
                String alias = "t" + (subqueryCounter++);
                yield "SELECT ? FROM (" + innerSQL + ") AS " + alias;
            }
            case RelRN.Filter filter -> {
                String innerSQL = transformNested(filter.source(), false);
                String condSQL = "? = ?";
                if (isRoot) {
                    yield innerSQL + " WHERE " + condSQL;
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
            case RelRN.Scan scan -> parts.fromClause = scan.name();
            case RelRN.Project project -> {
                collectFlattenedParts(project.source(), parts);
                addPlaceholdersForRex(project.map(), parts.projections);
            }
            case RelRN.Filter filter -> {
                collectFlattenedParts(filter.source(), parts);
                collectPredConditions(filter.cond(), parts.conditions);
            }
            default -> throw new UnsupportedOperationException("Unsupported RelRN: " + node);
        }
    }

    private void addPlaceholdersForRex(RexRN rex, List<String> projections) {
        if (rex instanceof RexRN.Proj proj) {
            projections.add("?");
            for (RexRN source : proj.sources()) {
                addPlaceholdersForRex(source, projections);
            }
        }
    }

    private void collectPredConditions(RexRN pred, List<String> conditions) {
        switch (pred) {
            case RexRN.Pred p -> conditions.add("? = ?");
            case RexRN.And and -> {
                for (RexRN child : and.sources()) {
                    collectPredConditions(child, conditions);
                }
            }
            default -> throw new UnsupportedOperationException("Unsupported RexRN: " + pred);
        }
    }
}

