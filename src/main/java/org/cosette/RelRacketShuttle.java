package org.cosette;

import kala.collection.Seq;
import kala.collection.immutable.ImmutableSeq;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;

public record RelRacketShuttle(Env env) {
    public SExpr visit(RelNode rel) {
        return switch (rel) {
            case TableScan scan -> SExpr.app("q-scan", SExpr.integer(env.resolve(scan.getTable())));
            case LogicalValues values -> {
                var visitor = new RexRacketVisitor(env);
                Seq<SExpr> tuples = Seq.from(values.getTuples()).map(tuple -> SExpr.app("list", Seq.from(tuple).map(visitor::visit)));
                yield SExpr.app("q-values", SExpr.integer(values.getRowType().getFieldCount()), SExpr.app("list", tuples));
            }
            case LogicalFilter filter -> {
                var condition = new RexRacketVisitor(env.recorded(filter.getVariablesSet()).advanced(filter.getInput().getRowType().getFieldCount())).visit(filter.getCondition());
                yield SExpr.app("q-filter", condition, visit(filter.getInput()));
            }
            case LogicalProject project -> {
                var visitor = new RexRacketVisitor(env.recorded(project.getVariablesSet()).advanced(project.getInput().getRowType().getFieldCount()));
                var targets = ImmutableSeq.from(project.getProjects()).map(visitor::visit);
                yield SExpr.app("q-project", SExpr.app("list", targets), visit(project.getInput()));
            }
            case LogicalJoin join -> {
                var left = join.getLeft();
                var right = join.getRight();
                var visitor = new RexRacketVisitor(env.recorded(join.getVariablesSet()).advanced(left.getRowType().getFieldCount() + right.getRowType().getFieldCount()));
                var kind = switch (join.getJoinType()) {
                    case INNER -> SExpr.quoted("inner");
                    case LEFT -> SExpr.quoted("left");
                    case RIGHT -> SExpr.quoted("right");
                    case FULL -> SExpr.quoted("full");
                    default -> throw new UnsupportedOperationException("Not supported join type: " + join.getJoinType());
                };
                yield SExpr.app("q-join", kind, visitor.visit(join.getCondition()), visit(left), visit(right));
            }
            case LogicalAggregate aggregate -> {
                var aggs = SExpr.app("list", Seq.from(aggregate.getAggCallList()).map(agg -> {
                    var name = SExpr.quoted(switch (agg.getAggregation().getName().toLowerCase()) {
                        case "count" -> agg.isDistinct() ? "aggr-count-distinct" : "aggr-count";
                        case "sum" -> "aggr-sum";
                        case "max" -> "aggr-max";
                        case "min" -> "aggr-min";
                        default -> throw new UnsupportedOperationException("Not supported aggregation function: " + agg.getName());
                    });
                    var cols = SExpr.app("list", Seq.from(agg.getArgList()).map(SExpr::integer));
                    return SExpr.app("v-agg", name, cols);
                }));
                var groupSet = SExpr.app("list", Seq.from(aggregate.getGroupSet().asSet()).map(SExpr::integer));
                yield SExpr.app("q-agg", aggs, groupSet, visit(aggregate.getInput()));
            }
            default -> throw new UnsupportedOperationException("Not implemented: " + rel.getRelTypeName());
        };
    }
}
