package org.cosette;

import kala.collection.Seq;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public record RelRacketShuttle(Env env) {
    public static void dumpToRacket(List<RelNode> relNodes, Path path) throws IOException {
        assert relNodes.size() == 2;
        var env = Env.empty();
        var rels = Seq.from(relNodes)
                .mapIndexed((i, rel) -> SExpr.def("r" + i, new RelRacketShuttle(env).visit(rel)));
        Seq<SExpr> tabs = env.tables().toSeq().map(t -> {
            var table = t.unwrap(CosetteTable.class);
            assert table != null;
            var fullName = Seq.from(t.getQualifiedName()).joinToString(".");
            Seq<SExpr> fields = table.getColumnNames().map(SExpr::string);
            return SExpr.app("table-info", SExpr.string(fullName), SExpr.app("list", fields));
        });
        var tables = SExpr.def("tables", SExpr.app("list", tabs));
        var output = """
                #lang cosette
                                
                %s
                                
                %s
                                
                (solve-relations r0 r1 tables println)
                """.formatted(rels.joinToString("\n"), tables);
        Files.write(path, output.getBytes());
    }

    public SExpr visit(RelNode rel) {
        return switch (rel) {
            case TableScan scan -> SExpr.app("r-scan", SExpr.integer(env.resolve(scan.getTable())));
            case LogicalValues values -> {
                var visitor = new RexRacketVisitor(env);
                Seq<SExpr> tuples = Seq.from(values.getTuples()).map(tuple -> SExpr.app("list", Seq.from(tuple).map(visitor::visit)));
                yield SExpr.app("r-values", SExpr.integer(values.getRowType().getFieldCount()), SExpr.app("list", tuples));
            }
            case LogicalFilter filter -> {
                var condition = new RexRacketVisitor(env.advanced(filter.getInput().getRowType().getFieldCount()).recorded(filter.getVariablesSet())).visit(filter.getCondition());
                yield SExpr.app("r-filter", condition, visit(filter.getInput()));
            }
            case LogicalProject project -> {
                var visitor = new RexRacketVisitor(env.advanced(project.getInput().getRowType().getFieldCount()).recorded(project.getVariablesSet()));
                var targets = Seq.from(project.getProjects()).map(visitor::visit);
                yield SExpr.app("r-project", SExpr.app("list", targets), visit(project.getInput()));
            }
            case LogicalJoin join -> {
                var left = join.getLeft();
                var right = join.getRight();
                var visitor = new RexRacketVisitor(env.advanced(left.getRowType().getFieldCount() + right.getRowType().getFieldCount()).recorded(join.getVariablesSet()));
                var kind = SExpr.quoted(switch (join.getJoinType()) {
                    case INNER -> "inner";
                    case LEFT -> "left";
                    case RIGHT -> "right";
                    case FULL -> "full";
                    default ->
                            throw new UnsupportedOperationException("Not supported join type: " + join.getJoinType());
                });
                yield SExpr.app("r-join", kind, visitor.visit(join.getCondition()), visit(left), visit(right));
            }
            case LogicalAggregate aggregate -> {
                var aggs = SExpr.app("list", Seq.from(aggregate.getAggCallList()).map(agg -> {
                    var name = SExpr.quoted(switch (agg.getAggregation().getName().toLowerCase()) {
                        case "count" ->
                                "aggr-count" + (agg.isDistinct() ? "-distinct" : "") + (agg.ignoreNulls() ? "-all" : "");
                        case "sum" -> "aggr-sum";
                        case "max" -> "aggr-max";
                        case "min" -> "aggr-min";
                        case "avg" -> "aggr-avg";
                        default ->
                                throw new UnsupportedOperationException("Not supported aggregation function: " + agg.getAggregation().getName());
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

    public record RexRacketVisitor(Env env) {
        public SExpr visit(RexNode rex) {
            return switch (rex) {
                case RexInputRef inputRef -> SExpr.app("v-var", SExpr.integer(env.base() + inputRef.getIndex()));
                case RexLiteral literal -> {
                    var val = switch (literal.getValue()) {
                        case null -> SExpr.quoted("null");
                        case Float v -> SExpr.real(v);
                        case Double v -> SExpr.real(v);
                        case BigDecimal v -> {
                            try {
                                yield SExpr.integer(v.longValueExact());
                            } catch (ArithmeticException e) {
                                yield SExpr.real(v.doubleValue());
                            }
                        }
                        case Integer v -> SExpr.integer(v);
                        case Long v -> SExpr.integer(v);
                        case Boolean v -> SExpr.bool(v);
                        case String v -> SExpr.string(v);
                        case NlsString v -> SExpr.string(v.toString());
                        default -> throw new UnsupportedOperationException("Unsupported literal: " + literal);
                    };
                    yield SExpr.app("v-op", val, SExpr.app("list"));
                }
                case RexSubQuery subQuery -> {
                    var name = switch (subQuery.getOperator().getKind()) {
                        case EXISTS -> "exists";
                        case UNIQUE -> "unique";
                        case IN -> "in";
                        case SqlKind kind ->
                                throw new UnsupportedOperationException("Unsupported subquery operation: " + kind);
                    };
                    var operands = Seq.from(subQuery.getOperands()).map(this::visit);
                    var rel = new RelRacketShuttle(env.advanced(0)).visit(subQuery.rel);
                    yield SExpr.app("v-hop", SExpr.quoted(name), SExpr.app("list", operands), rel);
                }
                case RexCall call -> {
                    var operands = Seq.from(call.getOperands()).map(this::visit);
                    yield SExpr.app("v-op", SExpr.quoted(call.op.getName().replace(" ", "-").toLowerCase()), SExpr.app("list", operands));
                }
                case RexFieldAccess access -> {
                    var id = SExpr.integer(env.resolve(((RexCorrelVariable) access.getReferenceExpr()).id) + access.getField().getIndex());
                    yield SExpr.app("v-var", id);
                }
                default -> throw new UnsupportedOperationException("Unsupported value: " + rex.getKind());
            };
        }
    }
}
