package org.cosette;

import io.github.cvc5.Kind;
import io.github.cvc5.Solver;
import io.github.cvc5.Term;
import kala.collection.Seq;
import kala.collection.Set;
import kala.collection.immutable.ImmutableMap;
import kala.collection.immutable.ImmutableSeq;
import kala.control.Result;
import kala.tuple.Tuple;
import kala.tuple.Tuple2;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.stream.IntStream;

public record RelMatcher() {

    public static Result<MatchEnv, String> check(RelNode pattern, RelNode target) {
        return relMatch(pattern, target).flatMap(MatchEnv::verify);
    }

    private static Result<MatchEnv, String> relMatch(RelNode pattern, RelNode target) {
        return switch (pattern) {
            case LogicalTableScan scan when scan.getTable().unwrap(CosetteTable.class) instanceof CosetteTable source -> {
                if (!source.getColumnTypes().allMatch(t -> t instanceof RelType.VarType)) {
                    yield Result.err("Types in pattern should all be Variable types.");
                }
                var scanPattern = source.getColumnTypes().mapIndexed((i, t) -> Tuple.of(i, (RelType.VarType) t, t.isNullable(), source.getKeys().contains(ImmutableBitSet.of(i))));
                var vts = scanPattern.map(t -> Tuple.of(t.component3(), t.component4()));
                if (vts.size() != Set.from(vts).size()) {
                    yield Result.err("Unable to match duplicate (nullability, uniqueness) pairs for generic types.\n\t" + vts);
                }
                var tts = Seq.from(target.getRowType().getFieldList()).map(RelDataTypeField::getType).mapIndexed(Tuple::of);
                var cms = ImmutableSeq.<ImmutableSeq<Integer>>empty();
                for (var t : scanPattern) {
                    var matched = tts.filter(tt -> {
                        // TODO: Derive target column uniqueness
                        return (t.component3() || !tt.component2().isNullable()) && !t.component4();
                    }).map(Tuple2::component1);
                    cms = cms.appended(matched);
                }
                yield Result.ok(new MatchEnv(new MatchEnv.FieldReference(cms), ImmutableMap.empty(), ImmutableSeq.empty()));
            }
            case LogicalFilter filter when target instanceof LogicalFilter node ->
                    relMatch(filter.getInput(), node.getInput()).flatMap(inputEnv ->
                            inputEnv.assertConstraint(filter.getCondition(), Seq.of(node.getCondition())));
            case LogicalProject project when target instanceof LogicalProject node ->
                    relMatch(project.getInput(), node.getInput()).flatMap(inputEnv -> {
                        // Propagate raw references while leaving the rest for generic projection
                        var fieldPattern = Seq.from(project.getProjects());
                        if (fieldPattern.isEmpty()) {
                            return Result.ok(inputEnv.updateFieldReference(Seq.empty()));
                        } else if (fieldPattern.allMatch(p -> p.getClass() == RexInputRef.class)) {
                            return Result.ok(inputEnv.updateFieldReference(fieldPattern.map(field -> inputEnv.fieldReference().correspondence().get(((RexInputRef) field).getIndex()))));
                        } else if (fieldPattern.size() == 1) {
                            return inputEnv.assertConstraint(project.getProjects().get(0), Seq.from(node.getProjects())).map(env ->
                                    env.updateFieldReference(Seq.of(Seq.from(IntStream.range(0, node.getProjects().size()).iterator()))));
                        } else {
                            return Result.err("TODO: Implement better field matching mechanism");
                        }
                    });
            default ->
                    Result.err(String.format("Cannot match %s type pattern with %s target", pattern.getRelTypeName(), target.getRelTypeName()));
        };
    }

    public static void main(String[] args) throws Exception {
        var solver = new Solver();
        solver.setOption("produce-models", "true");
        solver.setOption("sygus", "true");
        solver.setLogic("ALL");
        var i = solver.mkVar(solver.getIntegerSort(), "i");
        var f = solver.synthFun("f", new Term[]{i}, solver.getIntegerSort());
        var x = solver.declareSygusVar("x", solver.getIntegerSort());
        var fc = solver.mkTerm(Kind.APPLY_UF, f, x);
        var constraint = solver.mkTerm(Kind.GT, fc, x);
        solver.addSygusConstraint(constraint);
        var res = solver.checkSynth();
        if (res.hasSolution()) {
            var synth = solver.getSynthSolution(f);
            System.out.println(synth);
        }
    }
}

