package org.cosette;

import kala.collection.Seq;
import kala.control.Option;
import kala.control.Result;
import kala.tuple.Tuple;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;

public record RelMatcher() {

    public static Result<MatchEnv, String> check(RelNode pattern, RelNode target) {
        return relMatch(pattern, target).flatMap(MatchEnv::verify);
    }

    private static Result<MatchEnv, String> relMatch(RelNode pattern, RelNode target) {
        return switch (pattern) {
            case LogicalTableScan scan -> {
                yield Result.err("Scan not implemented.");
            }
            case LogicalFilter filter && (target instanceof LogicalFilter node) ->
                    relMatch(filter.getInput(), node.getInput()).flatMap(inputEnv ->
                            rexMatch(filter.getCondition(), node.getCondition()));
            default -> Result.err(String.format("Cannot match %s type pattern with %s target", pattern.getRelTypeName(), target.getRelTypeName()));
        };
    }

    private static Result<MatchEnv, String> rexMatch(RexNode pattern, RexNode target) {
        return Result.err("Not implemented.");
    }

    public static void main(String[] args) throws Exception {
        // Get rule
        var rule = ElevatedCoreRules.filterProjectTranspose();
        var pattern = rule.component1();
        var transform = rule.component2();
        // Create table schema and query
        var schema = Frameworks.createRootSchema(true);
        var table = RuleBuilder.create().createCosetteTable(Seq.of(
                Tuple.of(new RelType.BaseType(SqlTypeName.INTEGER, true), true),
                Tuple.of(new RelType.BaseType(SqlTypeName.VARCHAR, true), false)
        ));
        schema.add(table.getName(), table);
        var planner = new RawPlanner(schema);
        var target = planner.rel(planner.parse(String.format(
                "SELECT %s FROM %s WHERE %s > 1", table.getColumnNames().get(0), table.getName(), table.getColumnNames().get(0)
        )));
        System.out.println(pattern.explain());
        System.out.println(transform.explain());
        System.out.println(target.explain());
        var mapping = RelMatcher.check(pattern, target);
    }

}

