package org.cosette;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import kala.collection.Map;
import kala.collection.Seq;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rex.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

public record RelJSONShuttle(Env env) {
    private final static ObjectMapper mapper = new ObjectMapper();

    private static ArrayNode array(Seq<JsonNode> objs) {
        return new ArrayNode(mapper.getNodeFactory(), objs.asJava());
    }

    private static ObjectNode object(Map<String, JsonNode> fields) {
        return new ObjectNode(mapper.getNodeFactory(), fields.asJava());
    }

    private static BooleanNode bool(boolean b) {
        return b ? BooleanNode.TRUE : BooleanNode.FALSE;
    }

    public static void dumpToJSON(List<RelNode> relNodes, File file) throws IOException {
        var shuttle = new RelJSONShuttle(Env.empty());
        var helps = array(Seq.from(relNodes).map(rel -> new TextNode(rel.explain())));
        var queries = array(Seq.from(relNodes).map(shuttle::visit));

        var tables = shuttle.env.tables().map(table -> table.unwrap(CosetteTable.class));
        var schemas = array(tables.map(table -> object(Map.of(
                "name", new TextNode(table.id),
                "fields", array(table.names.map(TextNode::new)),
                "types", array(table.types.map(type -> new TextNode(type.name()))),
                "nullable", array(table.nullabilities.map(RelJSONShuttle::bool)),
                "key", array(table.keys.map(key -> array(Seq.from(key).map(IntNode::new)))),
                "guaranteed", array(table.checkConstraints.map(check -> new RexJSONVisitor(shuttle.env.advanced(table.names.size())).visit(check)))
        ))));

        var main = object(Map.of(
                "schemas", schemas,
                "queries", queries,
                "help", helps
        ));
        mapper.writer().writeValue(file, main);
    }

    public JsonNode visit(RelNode rel) {
        return switch (rel) {
            case TableScan scan -> object(Map.of("scan", new IntNode(env.resolve(scan.getTable()))));
            case LogicalValues values -> {
                var visitor = new RexJSONVisitor(env);
                var schema = array(Seq.from(values.getRowType().getFieldList()).map(field -> new TextNode(field.getType().getSqlTypeName().name())));
                var records = array(Seq.from(values.getTuples()).map(tuple -> array(Seq.from(tuple).map(visitor::visit))));
                yield object(Map.of("values", object(Map.of(
                        "schema", schema,
                        "content", records
                ))));
            }
            case LogicalFilter filter -> {
                var visitor = new RexJSONVisitor(env.advanced(filter.getInput().getRowType().getFieldCount()).recorded(filter.getVariablesSet()));
                yield object(Map.of("filter", object(Map.of(
                        "condition", visitor.visit(filter.getCondition()),
                        "source", visit(filter.getInput())
                ))));
            }
            case LogicalProject project -> {
                var visitor = new RexJSONVisitor(env.advanced(project.getInput().getRowType().getFieldCount()).recorded(project.getVariablesSet()));
                var targets = array(Seq.from(project.getProjects()).map(visitor::visit));
                yield object(Map.of("project", object(Map.of(
                        "target", targets,
                        "source", visit(project.getInput())
                ))));
            }
            case LogicalJoin join -> {
                var left = join.getLeft();
                var right = join.getRight();
                var visitor = new RexJSONVisitor(env.advanced(left.getRowType().getFieldCount() + right.getRowType().getFieldCount()).recorded(join.getVariablesSet()));
                yield object(Map.of("join", object(Map.of(
                        "kind", new TextNode(join.getJoinType().toString()),
                        "condition", visitor.visit(join.getCondition()),
                        "left", visit(left),
                        "right", visit(right)
                ))));
            }
            case LogicalCorrelate correlate -> {
                var rightShuttle = new RelJSONShuttle(env.advanced(correlate.getLeft().getRowType().getFieldCount()).recorded(correlate.getVariablesSet()));
                yield object(Map.of("correlate", array(Seq.of(
                        visit(correlate.getLeft()),
                        rightShuttle.visit(correlate.getRight())
                ))));
            }
            case LogicalAggregate aggregate -> {
                var groupCount = aggregate.getGroupCount();
                var level = env.base();
                var types = Seq.from(aggregate.getInput().getRowType().getFieldList())
                        .map(type -> new TextNode(type.getType().getSqlTypeName().name()));
                var keyCols = array(Seq.from(aggregate.getGroupSet()).map(key -> object(Map.of(
                        "column", new IntNode(level + key),
                        "type", types.get(key)
                ))));
                var keys = object(Map.of("project", object(Map.of(
                        "target", keyCols,
                        "source", visit(aggregate.getInput())
                ))));
                var conditions = array(Seq.from(aggregate.getGroupSet()).mapIndexed((i, key) -> {
                    var type = types.get(key);
                    var leftCol = object(Map.of("column", new IntNode(level + i), "type", type));
                    var rightCol = object(Map.of("column", new IntNode(level + groupCount + key), "type", type));
                    return object(Map.of(
                            "operator", new TextNode("="),
                            "operand", array(Seq.of(leftCol, rightCol)),
                            "type", new TextNode("BOOLEAN")
                    ));
                }));
                var condition = object(Map.of(
                        "operator", new TextNode("AND"),
                        "operand", conditions,
                        "type", new TextNode("BOOLEAN")
                ));
                var aggs = array(Seq.from(aggregate.getAggCallList()).map(call -> object(Map.of(
                        "operator", new TextNode(call.getAggregation().getName()),
                        "operand", array(Seq.from(call.getArgList()).map(target -> object(Map.of(
                                "column", new IntNode(level + groupCount + target),
                                "type", types.get(target)
                        )))),
                        "distinct", bool(call.isDistinct()),
                        "ignoreNulls", bool(call.ignoreNulls()),
                        "type", new TextNode(call.getType().getSqlTypeName().name())
                ))));
                var aggregated = object(Map.of("aggregate", object(Map.of(
                        "function", aggs,
                        "source", object(Map.of("filter", object(Map.of(
                                "condition", condition,
                                "source", new RelJSONShuttle(env.lifted(groupCount)).visit(aggregate.getInput())
                        ))))
                ))));
                yield object(Map.of("distinct", object(Map.of("correlate", array(Seq.of(keys, aggregated))))));
            }
            case LogicalUnion union -> {
                var result = object(Map.of("union", array(Seq.from(union.getInputs()).map(this::visit))));
                yield union.all ? result : object(Map.of("distinct", result));
            }
            case LogicalIntersect intersect && !intersect.all ->
                    object(Map.of("intersect", array(Seq.from(intersect.getInputs()).map(this::visit))));
            case LogicalMinus minus -> {
                var result = object(Map.of("except", array(Seq.from(minus.getInputs()).map(this::visit))));
                yield minus.all ? result : object(Map.of("distinct", result));
            }
            case LogicalSort sort -> {
                var types = Seq.from(sort.getInput().getRowType().getFieldList())
                        .map(type -> new TextNode(type.getType().getSqlTypeName().name()));
                var collations = array(Seq.from(sort.collation.getFieldCollations()).map(collation -> {
                    var index = collation.getFieldIndex();
                    return array(Seq.of(new IntNode(index), types.get(index), new TextNode(collation.shortString())));
                }));
                var args = object(Map.of(
                        "collation", collations,
                        "source", visit(sort.getInput())
                ));
                var visitor = new RexJSONVisitor(env.advanced(sort.getInput().getRowType().getFieldCount()));
                if (sort.offset != null) {
                    args.set("offset", visitor.visit(sort.offset));
                }
                if (sort.fetch != null) {
                    args.set("limit", visitor.visit(sort.fetch));
                }
                yield object(Map.of("sort", args));
            }
            default -> throw new RuntimeException("Not implemented: " + rel.getRelTypeName());
        };
    }

    public record RexJSONVisitor(Env env) {
        public JsonNode visit(RexNode rex) {
            return switch (rex) {
                case RexInputRef inputRef -> object(Map.of(
                        "column", new IntNode(inputRef.getIndex() + env.base()),
                        "type", new TextNode(inputRef.getType().getSqlTypeName().name())
                ));
                case RexLiteral literal -> object(Map.of(
                        "operator", new TextNode(literal.getValue() == null ? "NULL" : literal.getValue().toString()),
                        "operand", array(Seq.empty()),
                        "type", new TextNode(literal.getType().getSqlTypeName().name())
                ));
                case RexSubQuery subQuery -> object(Map.of(
                        "operator", new TextNode(subQuery.getOperator().toString()),
                        "operand", array(Seq.from(subQuery.getOperands()).map(this::visit)
                                .appended(new RelJSONShuttle(env.advanced(0)).visit(subQuery.rel))),
                        "type", new TextNode(subQuery.getType().getSqlTypeName().name())
                ));
                case RexCall call -> object(Map.of(
                        "operator", new TextNode(call.getOperator().toString()),
                        "operand", array(Seq.from(call.getOperands()).map(this::visit)),
                        "type", new TextNode(call.getType().getSqlTypeName().name())
                ));
                case RexFieldAccess fieldAccess -> object(Map.of(
                        "column", new IntNode (fieldAccess.getField().getIndex() + env.resolve(((RexCorrelVariable) fieldAccess.getReferenceExpr()).id)),
                        "type", new TextNode(fieldAccess.getType().getSqlTypeName().name())
                ));
                default -> throw new RuntimeException("Not implemented: " + rex.getKind());
            };
        }
    }
}