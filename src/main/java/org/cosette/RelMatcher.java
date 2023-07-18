package org.cosette;

import io.github.cvc5.Kind;
import io.github.cvc5.Solver;
import io.github.cvc5.Sort;
import io.github.cvc5.Term;
import kala.collection.Seq;
import kala.collection.Set;
import kala.collection.immutable.ImmutableSet;
import kala.control.Result;
import kala.tuple.Tuple;
import kala.tuple.Tuple2;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.stream.IntStream;

public record RelMatcher() {

    public static Result<MatchEnv, String> check(RelNode pattern, RelNode target) {
        return relMatch(pattern, target).flatMap(MatchEnv::verify);
    }

    private static Result<MatchEnv, String> relMatch(RelNode pattern, RelNode target) {
        return switch (pattern) {
            case LogicalTableScan scan when scan.getTable().unwrap(CosetteTable.class) instanceof CosetteTable source -> {
                if (!source.getColumnTypes().allMatch(t -> t instanceof RelType)) {
                    yield Result.err("Types in pattern should all be RelTypes.");
                }
                var cts = source.getColumnTypes()
                        // (index, typename, nullability, uniqueness)
                        .mapIndexed((i, t) -> Tuple.of(i, (RelType) t, t.isNullable(), source.getKeys().contains(ImmutableBitSet.of(i))))
                        // concrete types, not nullable types, and unique types have higher precedence
                        .sorted((p, q) -> switch (p.component2()) {
                            case RelType.BaseType ignored when q.component2() instanceof RelType.VarType -> -1;
                            case RelType.VarType ignored when q.component2() instanceof RelType.BaseType -> 1;
                            case RelType.BaseType x when q.component2() instanceof RelType.BaseType y
                                    && x.getSqlTypeName() != y.getSqlTypeName() ->
                                    x.getSqlTypeName().compareTo(y.getSqlTypeName());
                            case RelType ignored when p.component3() ^ q.component3() -> p.component3() ? 1 : -1;
                            case RelType ignored when p.component4() ^ q.component4() -> p.component4() ? -1 : 1;
                            default -> 0;
                        });
                var rts = cts.filter(t -> t.component2() instanceof RelType.BaseType).map(t -> Tuple.of(t.component2().getSqlTypeName(), t.component3(), t.component4()));
                if (rts.size() != Set.from(rts).size()) {
                    yield Result.err("Unable to match duplicate (typename, nullability, uniqueness) pairs for concrete types:\n\t" + rts);
                }
                var vts = cts.filter(t -> t.component2() instanceof RelType.VarType).map(t -> Tuple.of(t.component3(), t.component4()));
                if (vts.size() != Set.from(vts).size()) {
                    yield Result.err("Unable to match duplicate (nullability, uniqueness) pairs for generic types:\n\t" + vts);
                }
                var tts = Seq.from(target.getRowType().getFieldList()).map(RelDataTypeField::getType).mapIndexed(Tuple::of);
                var cms = Seq.<Set<Integer>>empty();
                for (var t : cts) {
                    cms = cms.appended(switch (t.component2()) {
                        case RelType.BaseType ignored -> {
                            var matched = tts.filter(tt -> {
                                var rt = tt.component2();
                                // TODO: Derive target column uniqueness
                                return rt.getSqlTypeName() == t.component2().getSqlTypeName()
                                        && (t.component3() || !rt.isNullable()) && !t.component4();
                            }).map(Tuple2::component1);
                            tts = tts.filter(tt -> !matched.contains(tt.component1()));
                            yield ImmutableSet.from(matched);
                        }
                        case RelType.VarType ignored -> {
                            var matched = tts.filter(tt -> {
                                // TODO: Derive target column uniqueness
                                return (t.component3() || !tt.component2().isNullable()) && !t.component4();
                            }).map(Tuple2::component1);
                            tts = tts.filter(tt -> !matched.contains(tt.component1()));
                            yield ImmutableSet.from(matched);
                        }
                    });
                }
                yield Result.ok(MatchEnv.empty().updateFieldReference(cms));
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
        var solver = new Solver();
        solver.setOption("produce-models", "true");
        solver.setOption("produce-unsat-cores", "true");
        solver.setLogic("ALL");
        var intSort = solver.getIntegerSort();
        var x = solver.mkConst(intSort, "x");
        var y = solver.mkConst(intSort, "y");
        var xpy = solver.mkTerm(Kind.ADD, x, y);
        var three = solver.mkInteger(3);
        var xlt = solver.mkTerm(Kind.LT, x, three);
        solver.assertFormula(xlt);
        var ylt = solver.mkTerm(Kind.LT, y, three);
        solver.assertFormula(ylt);
        var six = solver.mkInteger(6);
        var ses = solver.mkTerm(Kind.EQUAL, xpy, six);
        solver.assertFormula(ses);
        var res = solver.checkSat();
        System.out.println(res);
        if (res.isUnsat()) {
            for (var t : solver.getUnsatCore()) {
                System.out.println(t);
            }
        }
    }
}

