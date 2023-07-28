package org.cosette;

import kala.collection.Seq;
import kala.collection.Set;
import kala.collection.immutable.ImmutableMap;
import kala.collection.immutable.ImmutableSeq;
import kala.collection.immutable.ImmutableSet;
import kala.control.Result;
import kala.tuple.Tuple;
import kala.tuple.Tuple2;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.stream.IntStream;

public record RelMatcher() {

    public static Result<RexTranslator, String> check(RelNode pattern, RelNode target) {
        return relMatch(pattern, target).flatMap(MatchEnv::verify);
    }

    private static Result<MatchEnv, String> relMatch(RelNode pattern, RelNode target) {
        return switch (pattern) {
            case LogicalTableScan scan when scan.getTable()
                    .unwrap(CosetteTable.class) instanceof CosetteTable source -> {
                if (!source.getColumnTypes().allMatch(t -> t instanceof RelType.VarType)) {
                    yield Result.err("Types in pattern should all be Variable types.");
                }
                var scanPattern = source.getColumnTypes().mapIndexed(
                        (i, t) -> Tuple.of(i, (RelType.VarType) t, t.isNullable(),
                                source.getKeys().contains(ImmutableBitSet.of(i))));
                var vts = scanPattern.map(t -> Tuple.of(t.component3(), t.component4()));
                if (vts.size() != Set.from(vts).size()) {
                    yield Result.err(
                            "Unable to match duplicate (nullability, uniqueness) pairs for generic types.\n\t" + vts);
                }
                var tts = Seq.from(target.getRowType().getFieldList()).map(RelDataTypeField::getType)
                        .mapIndexed(Tuple::of);
                var cms = ImmutableSeq.<ImmutableSeq<Integer>>empty();
                for (var t : scanPattern) {
                    var matched = tts.filter(tt -> {
                        // TODO: Derive target column uniqueness
                        return (t.component3() || !tt.component2().isNullable()) && !t.component4();
                    }).map(Tuple2::component1);
                    cms = cms.appended(matched);
                }
                yield Result.ok(new MatchEnv(
                        new MatchEnv.FieldReference(cms, new MatchEnv.ProductType(tts.map(Tuple2::component2))),
                        ImmutableMap.empty(), ImmutableSeq.empty()));
            }
            case LogicalFilter filter when target instanceof LogicalFilter node ->
                    relMatch(filter.getInput(), node.getInput()).flatMap(
                            inputEnv -> inputEnv.assertConstraint(filter.getCondition(), Seq.of(node.getCondition())));
            case LogicalProject project when target instanceof LogicalProject node ->
                    relMatch(project.getInput(), node.getInput()).flatMap(inputEnv -> {
                        // Propagate raw references while leaving the rest for generic projection
                        var fieldPattern = Seq.from(project.getProjects());
                        if (fieldPattern.isEmpty()) {
                            return Result.ok(inputEnv.updateFieldReference(Seq.empty(), Seq.empty()));
                        } else if (fieldPattern.allMatch(p -> p.getClass() == RexInputRef.class)) {
                            return Result.ok(inputEnv.updateFieldReference(fieldPattern.map(
                                            field -> inputEnv.fieldReference().correspondence()
                                                    .get(((RexInputRef) field).getIndex())),
                                    Seq.from(node.getProjects()).map(RexNode::getType)));
                        } else if (fieldPattern.size() == 1) {
                            return inputEnv.assertConstraint(project.getProjects().get(0), Seq.from(node.getProjects()))
                                    .map(env -> env.updateFieldReference(
                                            Seq.of(Seq.from(IntStream.range(0, node.getProjects().size()).iterator())),
                                            Seq.from(node.getRowType().getFieldList()).map(RelDataTypeField::getType)));
                        } else {
                            return Result.err("TODO: Implement better field matching mechanism");
                        }
                    });
            case LogicalJoin join when target instanceof LogicalJoin node &&
                    join.getJoinType().equals(node.getJoinType()) -> relMatch(join.getLeft(), node.getLeft()).flatMap(
                            leftEnv -> relMatch(join.getRight(), node.getRight()).map(rightEnv -> Tuple.of(leftEnv, rightEnv)))
                    .map(env -> {
                        var leftEnv = env.component1();
                        var rightEnv = env.component2();
                        var leftFields = leftEnv.fieldReference();
                        var rightFields = rightEnv.fieldReference();
                        return new MatchEnv(new MatchEnv.FieldReference(leftFields.correspondence().appendedAll(
                                rightFields.correspondence()
                                        .map(corr -> corr.map(i -> i + leftFields.sourceTypes().elements().size()))),
                                new MatchEnv.ProductType(leftFields.sourceTypes().elements()
                                        .appendedAll(rightFields.sourceTypes().elements()))),
                                rightEnv.typeConstraints().toImmutableSeq().foldLeft(leftEnv.typeConstraints(),
                                        (lcs, rc) -> lcs.putted(rc.component1(),
                                                lcs.getOrDefault(rc.component1(), ImmutableSet.empty())
                                                        .addedAll(rc.component2()))),
                                leftEnv.synthConstraints().appendedAll(rightEnv.synthConstraints()));
                    }).flatMap(inputEnv -> inputEnv.assertConstraint(join.getCondition(), Seq.of(node.getCondition())));
            default -> Result.err(String.format("Cannot match %s type pattern with %s target", pattern.getRelTypeName(),
                    target.getRelTypeName()));
        };
    }

    public static void main(String[] args) throws Exception {
//        var solver = new Solver();
//        solver.setOption("produce-models", "true");
//        solver.setOption("sygus", "true");
//        solver.setLogic("ALL");
//        var i = solver.mkVar(solver.getIntegerSort(), "i");
//        var f = solver.synthFun("f", new Term[]{i}, solver.getIntegerSort());
//        var x = solver.declareSygusVar("x", solver.getIntegerSort());
//        var x2 = solver.declareSygusVar("x", solver.getIntegerSort());
//        solver.addSygusConstraint(solver.mkTerm(Kind.EQUAL, x, x2));
//        var res = solver.checkSynth();
//        if (res.hasSolution()) {
//            var synth = solver.getSynthSolution(f);
//            System.out.println(synth);
//        }
//        var table_R = new CosetteTable("R", Map.of("a", new RelType.BaseType(SqlTypeName.INTEGER, true), "b",
//                                                   new RelType.BaseType(SqlTypeName.INTEGER, true)
//        ), Set.empty(), Set.empty());
        var generator = new SchemaGenerator();
        generator.applyCreate("CREATE TABLE R (x INTEGER, y INTEGER)");
        generator.applyCreate("CREATE TABLE S (a INTEGER, b INTEGER)");
        var relBuilder = RuleBuilder.create(RawPlanner.generateConfig(generator.extractSchema()));
        var query = relBuilder.scan("R").scan("S").join(JoinRelType.INNER, relBuilder.call(SqlStdOperatorTable.AND,
                        relBuilder.call(SqlStdOperatorTable.EQUALS,
                                relBuilder.call(SqlStdOperatorTable.PLUS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)),
                                relBuilder.literal(6)),
                        relBuilder.call(SqlStdOperatorTable.LESS_THAN, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))))
                .build();
        var joinConditionPush = ElevatedCoreRules.joinConditionPush();
        var matcher = joinConditionPush.component1();
        System.out.println("Example: SELECT * FROM R INNER JOIN S ON x + a = 6 AND x < a");
        System.out.println();
        System.out.println("Plan:");
        System.out.println(query.explain());
        System.out.println("Matcher:");
        System.out.println(matcher.explain());
        var matchEnv = RelMatcher.relMatch(matcher, query).get();
        var solver = matchEnv.verify().get().solver();
        for (var constraint : solver.getSygusConstraints()) {
            System.out.println(constraint);
        }
        System.out.println(solver.checkSynth().hasSolution());
    }
}

