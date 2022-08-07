package org.cosette;

import kala.collection.Seq;
import kala.collection.immutable.ImmutableSeq;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public record RelRacketShuttle(Env env) {
    public SExpr visit(RelNode rel) {
        return switch (rel) {
            case TableScan scan -> SExpr.app("r-scan", SExpr.integer(env.resolve(scan.getTable())));
            case LogicalValues values -> {
                var visitor = new RexRacketVisitor(env);
                Seq<SExpr> tuples = Seq.from(values.getTuples()).map(tuple -> SExpr.app("list", Seq.from(tuple).map(visitor::visit)));
                yield SExpr.app("r-values", SExpr.integer(values.getRowType().getFieldCount()), SExpr.app("list", tuples));
            }
            case LogicalFilter filter -> {
                var condition = new RexRacketVisitor(env.recorded(filter.getVariablesSet()).advanced(filter.getInput().getRowType().getFieldCount())).visit(filter.getCondition());
                yield SExpr.app("r-filter", condition, visit(filter.getInput()));
            }
            case LogicalProject project -> {
                var visitor = new RexRacketVisitor(env.recorded(project.getVariablesSet()).advanced(project.getInput().getRowType().getFieldCount()));
                var targets = ImmutableSeq.from(project.getProjects()).map(visitor::visit);
                yield SExpr.app("r-project", SExpr.app("list", targets), visit(project.getInput()));
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
                yield SExpr.app("r-join", kind, visitor.visit(join.getCondition()), visit(left), visit(right));
            }
            case LogicalAggregate aggregate -> {
                var aggs = SExpr.app("list", Seq.from(aggregate.getAggCallList()).map(agg -> {
                    var name = SExpr.quoted(switch (agg.getAggregation().getName().toLowerCase()) {
                        case "count" -> "aggr-count" + (agg.isDistinct() ? "-distinct" : "") + (agg.ignoreNulls() ? "-all" : "");
                        case "sum" -> "aggr-sum";
                        case "max" -> "aggr-max";
                        case "min" -> "aggr-min";
                        case "avg" -> "aggr-avg";
                        default -> throw new UnsupportedOperationException("Not supported aggregation function: " + agg.getAggregation().getName());
                    });
                    var cols = SExpr.app("list", Seq.from(agg.getArgList()).map(SExpr::integer));
                    return SExpr.app("v-agg", name, cols);
                }));
                var groupSet = SExpr.app("list", Seq.from(aggregate.getGroupSet().asSet()).map(SExpr::integer));
                yield SExpr.app("r-agg", aggs, groupSet, visit(aggregate.getInput()));
            }
            case LogicalUnion union -> {
                // TODO: Handle non-all union
                yield SExpr.app("r-union", SExpr.app("list", Seq.from(union.getInputs()).map(this::visit)));
            }
            case LogicalSort sort -> {
                // TODO: Properly handle sorting.
                yield this.visit(sort.getInput());
            }
            default -> throw new UnsupportedOperationException("Not implemented: " + rel.getRelTypeName());
        };
    }

    public static void dumpToRacket(List<RelNode> relNodes, Path path) throws IOException {
        assert relNodes.size() == 2;
        var env = Env.empty();
        var rels = Seq.from(relNodes)
                .mapIndexed((i, rel) -> SExpr.def("r" + i, new RelRacketShuttle(env).visit(rel)));
        Seq<SExpr> tabs = env.tables().toSeq().map(t -> {
            var fullName = Seq.from(t.getQualifiedName()).joinToString(".");
            Seq<SExpr> fields = Seq.from(t.getRowType().getFieldNames()).map(SExpr::string);
            return SExpr.app("table-info", SExpr.string(fullName), SExpr.app("list", fields));
        });
        var tables = SExpr.def("tables", SExpr.app("list", tabs));
        var output = """
                #lang rosette
                                
                (require "relation.rkt" "cosette.rkt")
                
                %s
                
                %s
                
                (solve-relations r0 r1 tables println)
                """.formatted(rels.joinToString("\n"), tables);
        Files.write(path, output.getBytes());
    }
}
