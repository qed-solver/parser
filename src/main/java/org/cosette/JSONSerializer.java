package org.cosette;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import kala.collection.Map;
import kala.collection.Seq;
import kala.collection.immutable.ImmutableMap;
import kala.collection.mutable.MutableList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Set;

public record JSONSerializer(Env env) {
    private final static ObjectMapper mapper = new ObjectMapper();

    private record Rel(Env env) {
        Rel() {
            this(new Env(0, ImmutableMap.empty(), MutableList.create()));
        }

        private record Env(int lvl, ImmutableMap<CorrelationId, Integer> globals, MutableList<RelOptTable> tables) {
            Env recorded(Set<CorrelationId> ids) {
                return new Env(lvl, Seq.from(ids).foldLeft(globals, (g, id) -> g.putted(id, lvl)), tables);
            }

            Env lifted(int d) {
                return new Env(lvl + d, globals, tables);
            }

            int resolve(RelOptTable table) {
                var idx = tables.indexOf(table);
                if (idx == -1) {
                    idx = tables.size();
                    tables.append(table);
                }
                return idx;
            }

            public Rex.Env rex(int delta) {
                return new Rex.Env(lvl, delta, globals, tables);
            }
        }

        public JsonNode serialize(RelNode rel) {
            return switch (rel) {
                case TableScan scan -> object(Map.of("scan", integer(env.resolve(scan.getTable()))));
                case LogicalValues values -> {
                    var visitor = new Rex(env.rex(0));
                    var schema =
                            array(Seq.from(values.getRowType().getFieldList()).map(field -> type(field.getType())));
                    var records = array(Seq.from(values.getTuples())
                            .map(tuple -> array(Seq.from(tuple).map(visitor::serialize))));
                    yield object(Map.of("values", object(Map.of("schema", schema, "content", records))));
                }
                case LogicalFilter filter -> {
                    var input = filter.getInput();
                    var visitor =
                            new Rex(env.recorded(filter.getVariablesSet()).rex(input.getRowType().getFieldCount()));
                    yield object(Map.of("filter",
                            object(Map.of("condition", visitor.serialize(filter.getCondition()), "source",
                                    serialize(input)))));
                }
                case LogicalProject project -> {
                    var input = project.getInput();
                    var visitor =
                            new Rex(env.recorded(project.getVariablesSet()).rex(input.getRowType().getFieldCount()));
                    var targets = array(Seq.from(project.getProjects()).map(visitor::serialize));
                    yield object(Map.of("project", object(Map.of("target", targets, "source", serialize(input)))));
                }
                case LogicalJoin join -> {
                    var left = join.getLeft();
                    var right = join.getRight();
                    var visitor = new Rex(env.recorded(join.getVariablesSet())
                            .rex(left.getRowType().getFieldCount() + right.getRowType().getFieldCount()));
                    yield object(Map.of("join",
                            object(Map.of("kind", string(join.getJoinType().toString()), "condition",
                                    visitor.serialize(join.getCondition()), "left", serialize(left), "right",
                                    serialize(right)))));
                }
                case LogicalCorrelate correlate -> {
                    var left = correlate.getLeft();
                    var rightVisitor = new Rel(env.recorded(correlate.getVariablesSet())
                            .lifted(left.getRowType().getFieldCount()));
                    yield object(Map.of("correlate",
                            object(Map.of("kind", string(correlate.getJoinType().toString()), "left", serialize(left),
                                    "right", rightVisitor.serialize(correlate.getRight())))));
                }
                case LogicalAggregate aggregate -> {
                    var level = env.lvl();
                    var input = aggregate.getInput();
                    var inputTypes = Seq.from(input.getRowType().getFieldList()).map(field -> type(field.getType()));
                    var keys = array(Seq.from(aggregate.getGroupSet())
                            .map(col -> object(Map.of("column", integer(level + col), "type", inputTypes.get(col)))));
                    var aggs = array(Seq.from(aggregate.getAggCallList()).map(call -> object(
                            Map.of("operator", string(call.getAggregation().getName()), "operand",
                                    array(Seq.from(call.getArgList()).map(col -> object(
                                            Map.of("column", integer(level + col), "type", inputTypes.get(col))))),
                                    "distinct", bool(call.isDistinct()), "ignoreNulls", bool(call.ignoreNulls()),
                                    "type", type(call.getType())))));
                    yield object(Map.of("group",
                            object(Map.of("keys", keys, "function", aggs, "source", serialize(input)))));
                }
                case LogicalUnion union -> {
                    var result = object(Map.of("union", array(Seq.from(union.getInputs()).map(this::serialize))));
                    yield union.all ? result : object(Map.of("distinct", result));
                }
                case LogicalIntersect intersect when !intersect.all ->
                        object(Map.of("intersect", array(Seq.from(intersect.getInputs()).map(this::serialize))));
                case LogicalMinus minus when !minus.all ->
                        object(Map.of("except", array(Seq.from(minus.getInputs()).map(this::serialize))));
                case LogicalSort sort -> {
                    var input = sort.getInput();
                    var types = Seq.from(input.getRowType().getFieldList()).map(field -> type(field.getType()));
                    var collations = array(Seq.from(sort.collation.getFieldCollations()).map(collation -> {
                        var index = collation.getFieldIndex();
                        return array(Seq.of(integer(index), types.get(index), string(collation.getDirection().name())));
                    }));
                    var visitor = new Rex(env.rex(0));
                    yield object(Map.of("sort",
                            object(Map.of("collation", collations, "source", serialize(input), "offset",
                                    sort.offset != null ? visitor.serialize(sort.offset) : NullNode.instance, "limit",
                                    sort.fetch != null ? visitor.serialize(sort.fetch) : NullNode.instance))));
                }
                default -> throw new RuntimeException("Not implemented: " + rel.getRelTypeName());
            };
        }
    }

    private record Rex(Env env) {
        private record Env(int base, int delta, ImmutableMap<CorrelationId, Integer> globals,
                           MutableList<RelOptTable> tables) {
            public Rel.Env rel() {
                return new Rel.Env(base + delta, globals, tables);
            }

            int resolve(CorrelationId id) {
                return globals.getOrThrow(id, () -> new RuntimeException("Correlation ID not declared"));
            }
        }

        public JsonNode serialize(RexNode rex) {
            return switch (rex) {
                case RexInputRef inputRef -> object(Map.of("column", integer(inputRef.getIndex() + env.base()), "type",
                        type(inputRef.getType())));
                case RexLiteral literal -> object(Map.of("operator",
                        string(literal.getValue() == null ? "NULL" : literal.getValue().toString()), "operand",
                        array(Seq.empty()), "type", type(literal.getType())));
                case RexSubQuery subQuery ->
                        object(Map.of("operator", string(subQuery.getOperator().getName()), "operand",
                                array(Seq.from(subQuery.getOperands()).map(this::serialize)), "query",
                                new Rel(env.rel()).serialize(subQuery.rel), "type", type(subQuery.getType())));
                case RexCall call -> object(Map.of("operator", string(call.getOperator().getName()), "operand",
                        array(Seq.from(call.getOperands()).map(this::serialize)), "type", type(call.getType())));
                case RexFieldAccess fieldAccess -> object(Map.of("column", integer(fieldAccess.getField().getIndex() +
                                env.resolve(((RexCorrelVariable) fieldAccess.getReferenceExpr()).id)), "type",
                        type(fieldAccess.getType())));
                default -> throw new RuntimeException("Not implemented: " + rex.getKind());
            };
        }
    }

    private static ArrayNode array(Seq<JsonNode> objs) {
        return new ArrayNode(mapper.getNodeFactory(), objs.asJava());
    }

    private static ObjectNode object(Map<String, JsonNode> fields) {
        return new ObjectNode(mapper.getNodeFactory(), fields.asJava());
    }

    private static BooleanNode bool(boolean b) {
        return BooleanNode.valueOf(b);
    }

    private static TextNode string(String s) {
        return new TextNode(s);
    }

    private static TextNode type(RelDataType type) {
        return new TextNode(type.getSqlTypeName().getName());
    }

    private static IntNode integer(int i) {
        return new IntNode(i);
    }

    public static ObjectNode serialize(Seq<RelNode> relNodes) {
        var shuttle = new Rel();
        var helps = array(relNodes.map(rel -> new TextNode(rel.explain())));
        var queries = array(relNodes.map(shuttle::serialize));
        var tables = shuttle.env.tables();
        var schemas = array(tables.map(table -> {
            var visitor = new Rex(shuttle.env.rex(table.getRowType().getFieldCount()));
            var cosette = table.unwrap(CosetteTable.class);
            var fields = Seq.from(table.getRowType().getFieldList());
            return cosette == null ?
                    object(Map.of("name", string(Seq.from(table.getQualifiedName()).joinToString(".")), "fields",
                            array(fields.map(field -> string(field.getName()))), "types",
                            array(fields.map(field -> type(field.getType()))), "nullable",
                            array(fields.map(field -> bool(field.getType().isNullable()))), "key",
                            array((table.getKeys() != null ? Seq.from(table.getKeys()) :
                                    Seq.<ImmutableBitSet>empty()).map(
                                    key -> array(Seq.from(key).map(JSONSerializer::integer)))), "guaranteed",
                            array(Seq.empty()))) : object(Map.of("name", string(cosette.getName()), "fields",
                    array(cosette.getColumnNames().map(JSONSerializer::string)), "types",
                    array(cosette.getColumnTypes().map(JSONSerializer::type)), "nullable",
                    array(cosette.getColumnTypes().map(type -> bool(type.isNullable()))), "key",
                    array(Seq.from(cosette.getKeys().map(key -> array(Seq.from(key).map(JSONSerializer::integer))))),
                    "guaranteed", array(cosette.getConstraints().map(visitor::serialize).toImmutableSeq())));
        }));

        return object(Map.of("schemas", schemas, "queries", queries, "help", helps));
    }
}
