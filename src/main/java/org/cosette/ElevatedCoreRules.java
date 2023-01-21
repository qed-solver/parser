package org.cosette;

import kala.collection.Seq;
import kala.tuple.Tuple;
import kala.tuple.Tuple2;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class ElevatedCoreRules {

    public static Tuple2<RelNode, RelNode> calcMerge() {
        // A Calc is equivalent to a project above a filter
        var builder = RuleBuilder.create();
        var tableName = builder.sourceSimpleTables(Seq.of(0)).get(0);
        builder.scan(tableName);
        var bottomFilter = builder.genericPredicateOp("bottom", true);
        builder.filter(builder.call(bottomFilter, builder.fields()));
        var bottomProject = builder.genericProjectionOp("bottom", new RelType.VarType("INTER", true));
        builder.project(builder.call(bottomProject, builder.fields()));
        var topFilter = builder.genericPredicateOp("top", true);
        builder.filter(builder.call(topFilter, builder.fields()));
        var topProject = builder.genericProjectionOp("top", new RelType.VarType("RESULT", true));
        builder.project(builder.call(topProject, builder.fields()));
        var before = builder.build();
        builder.scan(tableName);
        builder.filter(builder.call(SqlStdOperatorTable.AND,
                builder.call(bottomFilter, builder.fields()),
                builder.call(topFilter, builder.call(bottomProject, builder.fields()))));
        builder.project(builder.call(topProject, builder.call(bottomProject, builder.fields())));
        var after = builder.build();
        return Tuple.of(before, after);
    }

    public static Tuple2<RelNode, RelNode> filterIntoJoin() {
        var builder = RuleBuilder.create();
        var tableNames = builder.sourceSimpleTables(Seq.of(1, 2));
        tableNames.forEach(builder::scan);
        var joinCond = builder.genericPredicateOp("join", true);
        builder.join(JoinRelType.INNER, builder.call(joinCond, builder.joinFields()));
        var filter = builder.genericPredicateOp("filter", true);
        builder.filter(builder.call(filter, builder.fields()));
        var before = builder.build();
        tableNames.forEach(builder::scan);
        builder.join(JoinRelType.INNER, builder.call(SqlStdOperatorTable.AND,
                builder.call(joinCond, builder.joinFields()),
                builder.call(filter, builder.joinFields())));
        var after = builder.build();
        return Tuple.of(before, after);
    }

    public static Tuple2<RelNode, RelNode> filterProjectTranspose() {
        var builder = RuleBuilder.create();
        var tableName = builder.sourceSimpleTables(Seq.of(0)).get(0);
        builder.scan(tableName);
        var project = builder.genericProjectionOp("select", new RelType.VarType("PROJECT", true));
        builder.project(builder.call(project, builder.fields()));
        var filter = builder.genericPredicateOp("filter", true);
        builder.filter(builder.call(filter, builder.fields()));
        var before = builder.build();
        builder.scan(tableName).filter(builder.call(filter, builder.call(project, builder.fields())));
        builder.project(builder.call(project, builder.fields()));
        var after = builder.build();
        return Tuple.of(before, after);
    }

    public static Seq<Tuple2<RelNode, RelNode>> filterCorrelate() {
        return Seq.of(JoinRelType.INNER, JoinRelType.LEFT).map(joinType -> {
            var builder = RuleBuilder.create();
            var tableNames = builder.sourceSimpleTables(Seq.of(1, 2));
            tableNames.forEach(builder::scan);
            builder.correlate(joinType, new CorrelationId(0), builder.fields(2, 0));
            var filterLeft = builder.genericPredicateOp("filterLeft", true);
            var filterRight = builder.genericPredicateOp("filterRight", true);
            var filterBoth = builder.genericPredicateOp("filterBoth", true);
            builder.filter(builder.and(
                    builder.call(filterLeft, builder.field(0)),
                    builder.call(filterRight, builder.field(1)),
                    builder.call(filterBoth, builder.fields())
            ));
            var before = builder.build();
            builder.scan(tableNames.get(0)).filter(builder.call(filterLeft, builder.fields()));
            builder.scan(tableNames.get(1)).filter(builder.call(filterRight, builder.fields()));
            builder.correlate(joinType, new CorrelationId(0), builder.fields(2, 0));
            builder.filter(builder.call(filterBoth, builder.fields()));
            var after = builder.build();
            return Tuple.of(before, after);
        });
    }

    public static Seq<Tuple2<RelNode, RelNode>> filterSetOpTranspose() {
        return Seq.of(SqlStdOperatorTable.EXCEPT, SqlStdOperatorTable.INTERSECT, SqlStdOperatorTable.UNION_ALL).map(kind -> {
            var builder = RuleBuilder.create();
            var tableNames = builder.sourceSimpleTables(Seq.of(0, 0));
            tableNames.forEach(builder::scan);
            if (kind == SqlStdOperatorTable.EXCEPT) {
                builder.minus(false);
            } else if (kind == SqlStdOperatorTable.INTERSECT) {
                builder.intersect(false);
            } else if (kind == SqlStdOperatorTable.UNION_ALL) {
                builder.union(true);
            }
            var filter = builder.genericPredicateOp("filter", true);
            builder.filter(builder.call(filter, builder.fields()));
            var before = builder.build();
            builder.scan(tableNames.get(0)).filter(builder.call(filter, builder.fields()));
            builder.scan(tableNames.get(1)).filter(builder.call(filter, builder.fields()));
            if (kind == SqlStdOperatorTable.EXCEPT) {
                builder.minus(false);
            } else if (kind == SqlStdOperatorTable.INTERSECT) {
                builder.intersect(false);
            } else if (kind == SqlStdOperatorTable.UNION_ALL) {
                builder.union(true);
            }
            var after = builder.build();
            return Tuple.of(before, after);
        });
    }

    public static Seq<Tuple2<RelNode, RelNode>> projectCorrelateTranspose() {
        return Seq.of(JoinRelType.INNER, JoinRelType.LEFT).map(joinType -> {
            var builder = RuleBuilder.create();
            var leftTable = builder.createCosetteTable(Seq.of(
                    Tuple.of(new RelType.VarType("Type_1", true), false),
                    Tuple.of(new RelType.VarType("Type_2", true), false)
            ));
            var rightTable = builder.createCosetteTable(Seq.of(
                    Tuple.of(new RelType.VarType("Type_3", true), false),
                    Tuple.of(new RelType.VarType("Type_4", true), false)
            ));
            builder.addTable(leftTable).addTable(rightTable);
            builder.scan(leftTable.getName()).scan(rightTable.getName());
            builder.correlate(joinType, new CorrelationId(0), builder.fields(2, 0));
            var project = builder.genericProjectionOp("project", new RelType.VarType("Type_5", true));
            builder.project(builder.call(project, Seq.of(builder.field(0), builder.field(2))));
            var before = builder.build();
            builder.scan(leftTable.getName()).project(builder.field(0));
            builder.scan(rightTable.getName()).project(builder.field(0));
            builder.correlate(joinType, new CorrelationId(0), builder.fields(2, 0));
            builder.project(builder.call(project, builder.fields()));
            var after = builder.build();
            return Tuple.of(before, after);
        });
    }

    public static Seq<Tuple2<RelNode, RelNode>> projectJoinRemove() {
        var builder = RuleBuilder.create();
        var leftTableName = builder.sourceSimpleTables(Seq.of(0));
        var rightTable = builder.createCosetteTable(Seq.of(
                Tuple.of(new RelType.VarType("Type_1", true), true),
                Tuple.of(new RelType.VarType("Type_2", true), false)
        ));
        builder.addTable(rightTable);
        builder.scan(leftTableName).scan(rightTable.getName());
        var targetMap = builder.genericProjectionOp("target", new RelType.VarType("Type_1", true));
        builder.join(JoinRelType.LEFT, builder.call(SqlStdOperatorTable.EQUALS,
                builder.call(targetMap, builder.fields(2, 0)),
                builder.field(2, 1, 0)));
        var project = builder.genericProjectionOp("project", new RelType.VarType("PROJECT", true));
        builder.project(builder.call(project, builder.field(0)));
        var leftBefore = builder.build();
        var leftAfter = builder.scan(leftTableName).project(builder.call(project, builder.fields())).build();
        builder.scan(rightTable.getName()).scan(leftTableName);
        builder.join(JoinRelType.RIGHT, builder.call(SqlStdOperatorTable.EQUALS,
                builder.call(targetMap, builder.fields(2, 1)),
                builder.field(2, 0, 0)));
        builder.project(builder.call(project, builder.field(2)));
        var rightBefore = builder.build();
        var rightAfter = builder.scan(leftTableName).project(builder.call(project, builder.fields())).build();
        return Seq.of(Tuple.of(leftBefore, leftAfter), Tuple.of(rightBefore, rightAfter));
    }

    public static Seq<Tuple2<RelNode, RelNode>> projectJoinTranspose() {
        return Seq.of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.RIGHT, JoinRelType.FULL).map(joinType -> {
            var builder = RuleBuilder.create();
            var leftTable = builder.createCosetteTable(Seq.of(
                    Tuple.of(new RelType.VarType("Type_1", true), false),
                    Tuple.of(new RelType.VarType("Type_2", true), false)
            ));
            var rightTable = builder.createCosetteTable(Seq.of(
                    Tuple.of(new RelType.VarType("Type_3", true), false),
                    Tuple.of(new RelType.VarType("Type_4", true), false)
            ));
            builder.addTable(leftTable).addTable(rightTable);
            builder.scan(leftTable.getName()).scan(rightTable.getName());
            var joinCond = builder.genericPredicateOp("join", true);
            builder.join(joinType, builder.call(joinCond, builder.field(2, 0, 0), builder.field(2, 1, 0)));
            var project = builder.genericProjectionOp("project", new RelType.VarType("Type_5", true));
            builder.project(builder.call(project, Seq.of(builder.field(0), builder.field(2))));
            var before = builder.build();
            builder.scan(leftTable.getName()).project(builder.field(0));
            builder.scan(rightTable.getName()).project(builder.field(0));
            builder.join(joinType, builder.call(joinCond, builder.joinFields()));
            builder.project(builder.call(project, builder.fields()));
            var after = builder.build();
            return Tuple.of(before, after);
        });
    }

    public static Tuple2<RelNode, RelNode> projectSetOpTranspose() {
        var builder = RuleBuilder.create();
        var tableNames = builder.sourceSimpleTables(Seq.of(0, 0));
        tableNames.forEach(builder::scan);
        builder.union(true);
        var project = builder.genericProjectionOp("project", new RelType.VarType("PROJECT", true));
        builder.project(builder.call(project, builder.fields()));
        var before = builder.build();
        builder.scan(tableNames.get(0)).project(builder.call(project, builder.fields()));
        builder.scan(tableNames.get(1)).project(builder.call(project, builder.fields()));
        builder.union(true);
        var after = builder.build();
        return Tuple.of(before, after);
    }

    public static Tuple2<RelNode, RelNode> joinConditionPush() {
        var builder = RuleBuilder.create();
        var tableNames = builder.sourceSimpleTables(Seq.of(1, 2));
        tableNames.forEach(builder::scan);
        var joinLeft = builder.genericPredicateOp("joinLeft", true);
        var joinRight = builder.genericPredicateOp("joinRight", true);
        var joinBoth = builder.genericPredicateOp("joinBoth", true);
        var joinCond = builder.and(
                builder.call(joinLeft, builder.fields(2, 0)),
                builder.call(joinRight, builder.fields(2, 1)),
                builder.call(joinBoth, builder.joinFields())
        );
        builder.join(JoinRelType.INNER, joinCond);
        var before = builder.build();
        builder.scan(tableNames.get(0)).filter(builder.call(joinLeft, builder.fields()));
        builder.scan(tableNames.get(1)).filter(builder.call(joinRight, builder.fields()));
        joinCond = builder.call(joinBoth, builder.joinFields());
        builder.join(JoinRelType.INNER, joinCond);
        var after = builder.build();
        return Tuple.of(before, after);
    }

//    public static Tuple2<RelNode, RelNode> joinAddRedundantSemiJoin() {
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> joinAssociate() {
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> joinCommute() {
//        // Inner/Outer joins
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> joinExtractFilter() {
//        // Inner/Outer joins
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> joinProjectBothTranspose() {
//        // Inner/Outer joins
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> joinProjectLeftTranspose() {
//        // Inner/Outer joins
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> joinProjectRightTranspose() {
//        // Inner/Outer joins
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> joinPushExpressions() {
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> joinPushTransitivePredicates() {
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> joinDeriveIsNotNullFilter() {
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> joinToCorrelate() {
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> joinToSemiJoin() {
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> joinLeftUnionTranspose() {
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> joinRightUnionTranspose() {
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> semiJoinFilterTranspose() {
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> semiJoinProjectTranspose() {
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> semiJoinJoinTranspose() {
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> semiJoinRemove() {
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> unionMerge() {
//        return null;
//    }
//
//    public static Tuple2<RelNode, RelNode> projectFilterValueMerge() {
//        return null;
//    }

    public static void dumpTransformedRule(RelNode before, RelNode after, boolean verbose, Path dumpPath) throws IOException {
        if (verbose) {
            System.out.println(">>>>>> " + dumpPath.getFileName().toString() + " <<<<<<");
            System.out.println("Before:");
            System.out.println(before.explain());
            System.out.println("After:");
            System.out.println(after.explain());
            System.out.println(">>>>>> End of rule <<<<<<\n\n");
        }
        RelJSONShuttle.dumpToJSON(List.of(before, after), dumpPath.toFile());
    }

    public static void dumpElevatedRules(Path dumpFolder, boolean verbose) throws IOException {
        Files.createDirectories(dumpFolder);
        Seq.of(ElevatedCoreRules.class.getMethods())
                .filter(method -> Modifier.isStatic(method.getModifiers()))
                .forEachUnchecked(method -> {
                    switch (method.getReturnType().getSimpleName()) {
                        case "Tuple2" -> {
                            var ruleName = method.getName();
                            var rewrite = (Tuple2<RelNode, RelNode>) method.invoke(null);
                            dumpTransformedRule(rewrite._1, rewrite._2, verbose,
                                    Paths.get(dumpFolder.toAbsolutePath().toString(), ruleName + ".json"));
                        }
                        case "Seq" -> {
                            var ruleName = method.getName();
                            var rewrites = (Seq<Tuple2<RelNode, RelNode>>) method.invoke(null);
                            rewrites.forEachIndexedUnchecked((index, rewrite) -> dumpTransformedRule(rewrite._1, rewrite._2, verbose,
                                    Paths.get(dumpFolder.toAbsolutePath().toString(), ruleName + index + ".json")));
                        }
                    }
                });
    }

    public static void main(String[] args) throws IOException {
        Path dumpFolder = Paths.get("ElevatedRules");
        FileUtils.deleteDirectory(dumpFolder.toFile());
        dumpElevatedRules(dumpFolder, true);
    }

    /**
     * Ignored rules:
     * - Aggregation related rules: unsupported for now
     *   - Aggregate*
     *   - CalcToWindow
     *   - FilterAggregateTranspose
     *   - ProjectAggregateMerge
     *   - ProjectToSemiJoin
     *   - Aggregate values
     * - Multi-join related rules: unsupported for now:
     *   - FilterMultiJoinRule
     *   - ProjectMultiJoinMerge
     *   - JoinToMultiJoin
     *   - MultiJoin*
     * - Sort related rules: unsupported for now
     *   - Sort*
     * - CalcRemove: trivially true
     * - CalcReduceDecimal: casting is not understood by the prover
     * - CalcReduceExpression: constant reduction is trivial
     * - CalcSplit: split calc into project above filter, which is exactly how calc is represented in cosette
     * - CalcToWindow: window not supported
     * - CoerceInputs: casting is not understood by the prover
     * - ExchangeRemoveConstantKeys: exchange not supported
     * - SortExchangeRemoveConstantKeys: exchange not supported
     * - FilterIntoJoinDumb: special case of FilterIntoJoin
     * - FilterMerge: special case of CalcMerge
     * - FilterCalcMerge: special case of CalcMerge
     * - FilterToCalc: special case of CalcMerge
     * - FilterTableFunctionTranspose: functionScan is not understood by prover
     * - FilterScan: filterScan not supported
     * - FilterInterpreterScan: filterScan not supported
     * - FilterExpandIsNotDistinctFrom: case when is not understood by prover
     * - FilterReduceExpression: constant reduction is trivial
     * - IntersectMerge: intersect not supported
     * - IntersectToDistinct: intersect not supported
     * - Match: match not supported
     * - MinusMerge: minus with multiple inputs not supported
     * - ProjectCalcMerge: special case of CalcMerge
     * - ProjectFilterTranspose: not meaningful transformation
     * - ProjectReduceExpressions: constant reduction is trivial
     * - ProjectSubQueryToCorrelate: complicated rule with limited use
     * - FilterSubQueryToCorrelate: complicated rule with limited use
     * - JoinSubQueryToCorrelate: complicated rule with limited use
     * - ProjectToLogicalProjectAndWindow: window not supported
     * - ProjectJoinJoinRemove: special case of ProjectJoinRemove
     * - ProjectMerge: special case of CalcMerge
     * - ProjectRemove: trivially true
     * - ProjectTableScan: bindable table-scan not supported
     * - ProjectInterpreterTableScan: bindable table-scan not supported
     * - ProjToCalc special case of CalcMerge
     * - ProjectWindowTranspose: window not supported
     * - JoinCommuteOuter: special case of JoinCommute
     * - JoinProject*TransposeIncludeOuter: special cases of JoinProject*Transpose
     * - JoinReduceExpressions: constant reduction is trivial
     * - UnionRemove: trivially true
     * - UnionPullUpConstants: trivially true
     * - UnionToDistinct: trivially true
     * - FilterValuesMerge: special case of ProjectFilterValuesMerge
     * - ProjectValuesMerge: special case of ProjectFilterValuesMerge
     * - WindowReduceExpressions: constant reduction is trivial
     */

}
