package org.cosette;

import kala.collection.Seq;
import kala.collection.Set;
import kala.collection.mutable.MutableSet;
import kala.control.Result;
import kala.tuple.Tuple;
import kala.tuple.Tuple3;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;

import java.util.stream.IntStream;

public record RelMatcher() {

    public static Result<MatchEnv, String> check(RelNode pattern, RelNode target) {
        return relMatch(pattern, target).flatMap(MatchEnv::verify);
    }

    private static Result<MatchEnv, String> relMatch(RelNode pattern, RelNode target) {
        return switch (pattern) {
            case LogicalTableScan scan -> {
                // (Typename, nullability, uniqueness)
                Set<Tuple3<SqlTypeName, Boolean, Boolean>> witness = MutableSet.create();
                // TODO: Properly handel column uniqueness
                yield Result.err("Implementation in progress");
            }
            case LogicalFilter filter when target instanceof LogicalFilter node ->
                    relMatch(filter.getInput(), node.getInput()).flatMap(inputEnv ->
                            inputEnv.assertConstraint(filter.getCondition(), Seq.of(node.getCondition())));
            case LogicalProject project when target instanceof LogicalProject node ->
                    relMatch(project.getInput(), node.getInput()).flatMap(inputEnv -> switch (project.getRowType().getFieldCount()) {
                        case 1 ->
                                inputEnv.assertConstraint(project.getProjects().get(0), Seq.from(node.getProjects())).map(env ->
                                        env.updateFieldReference(Seq.of(Set.from(IntStream.range(0, node.getProjects().size()).iterator()))));
                        default -> Result.err("TODO: Please extend project field matching mechanism");
                    });
            default ->
                    Result.err(String.format("Cannot match %s type pattern with %s target", pattern.getRelTypeName(), target.getRelTypeName()));
        };
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

