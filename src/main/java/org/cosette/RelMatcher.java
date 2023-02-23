package org.cosette;

import kala.collection.Seq;
import kala.control.Option;
import kala.tuple.Tuple;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;

public record RelMatcher() {

    public static Option<MatchEnv> check(RelNode pattern, RelNode target) {
        return match(pattern, target).filter(MatchEnv::verify);
    }

    private static Option<MatchEnv> match(RelNode pattern, RelNode target) {
        return switch (pattern) {
            case LogicalTableScan scan -> Option.some(MatchEnv.empty());
            case LogicalFilter filter && (target instanceof LogicalFilter node) -> {
                var inputs = match(filter.getInput(), node.getInput());
                if (!inputs.isEmpty()) {
                    var inputEnv = inputs.get();
                    yield Option.some(inputEnv);
                }
                yield Option.none();
            }
            case LogicalProject project && (target instanceof LogicalProject node) -> {
                var inputs = match(project.getInput(), node.getInput());
                if (!inputs.isEmpty()) {
                    var inputEnv = inputs.get();
                    yield Option.some(inputEnv);
                }
                yield Option.none();
            }
            default -> Option.none();
        };
    }

    public static void main(String[] args) throws Exception {
        // Get rule
        var rule = ElevatedCoreRules.filterProjectTranspose();
        var pattern = rule._1;
        var transform = rule._2;
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
        System.out.println(target.explain());
        var mapping = RelMatcher.match(pattern, target);
    }

}

