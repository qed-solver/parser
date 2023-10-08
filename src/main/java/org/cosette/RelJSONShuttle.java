package org.cosette;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import kala.collection.Map;
import kala.collection.Seq;
import kala.collection.Set;
import kala.collection.immutable.ImmutableSeq;
import kala.control.Result;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public record RelJSONShuttle(Env env) {
    private final static ObjectMapper mapper = new ObjectMapper();

    private static ArrayNode array(Seq<JsonNode> objs) {
        return new ArrayNode(mapper.getNodeFactory(), objs.asJava());
    }


    private static Result<ImmutableSeq<JsonNode>, String> array(JsonNode jsonNode, String field) {
        var arr = jsonNode.get(field);
        if (arr == null || !arr.isArray()) {
            return Result.err(String.format("Missing array field %s in:\n%s", field, jsonNode.toPrettyString()));
        }
        return Result.ok(ImmutableSeq.from(arr.elements()));
    }

    private static ObjectNode object(Map<String, JsonNode> fields) {
        return new ObjectNode(mapper.getNodeFactory(), fields.asJava());
    }

    private static Result<JsonNode, String> object(JsonNode jsonNode, String field) {
        var obj = jsonNode.get(field);
        if (obj == null) {
            return Result.err(String.format("Missing object field %s in:\n%s", field, jsonNode.toPrettyString()));
        }
        return Result.ok(obj);
    }

    private static BooleanNode bool(boolean b) {
        return b ? BooleanNode.TRUE : BooleanNode.FALSE;
    }

    private static <T> T unwrap(Result<T, String> res) throws Exception {
        if (res.isErr()) {
            throw new Exception(res.getErr());
        }
        return res.get();
    }

    public static void main(String[] args) throws IOException {
        var res = RelJSONShuttle.deserializeFromJson(Paths.get("ElevatedRules/filterProjectTranspose.json"));
        if (res.isErr()) {
            System.out.println(res.getErr());
        } else {
            res.get().forEach(r -> System.out.println(r.explain()));
        }
    }

    public static void serializeToJson(List<RelNode> relNodes, Path path) throws IOException {
        var shuttle = new RelJSONShuttle(Env.empty());
        var helps = array(Seq.from(relNodes).map(rel -> new TextNode(rel.explain())));
        var queries = array(Seq.from(relNodes).map(shuttle::serialize));

        var tables = shuttle.env.tables();
        var schemas = array(tables.map(table -> object(Map.of(
                "name", new TextNode(table.getName()),
                "fields", array(table.getColumnNames().map(TextNode::new)),
                "types", array(table.getColumnTypes().map(type -> new TextNode(type.toString()))),
                "nullable", array(table.getColumnTypes().map(RelDataType::isNullable).map(RelJSONShuttle::bool)),
                "key", array(Seq.from(table.getKeys().map(key -> array(Seq.from(key).map(IntNode::new))))),
                "guaranteed", array(table.getConstraints()
                        .map(check -> new RexJSONVisitor(shuttle.env.advanced(table.getColumnNames().size())).serialize(check)).toImmutableSeq())
        ))));

        var main = object(Map.of("schemas", schemas, "queries", queries, "help", helps));
        mapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), main);
    }

    public static Result<ImmutableSeq<RelNode>, String> deserializeFromJson(Path path) throws IOException {
        var node = mapper.readTree(path.toFile());
        var env = Env.empty();
        var tables = array(node, "schemas").flatMap(schemas -> {
            var collected = ImmutableSeq.<CosetteTable>empty();
            for (var schema : schemas) {
                try {
                    var tys = unwrap(array(schema, "types"));
                    var nbs = unwrap(array(schema, "nullable"));
                    var nm = unwrap(object(schema, "name"));
                    var fds = unwrap(array(schema, "fields")).map(JsonNode::asText);
                    var kys = unwrap(array(schema, "key"));
                    var kgs = Set.from(kys.map(kg -> ImmutableBitSet.of(Seq.from(kg.elements()).map(JsonNode::asInt))));
                    if (tys.size() != nbs.size()) {
                        return Result.err("Expecting corresponding types and nullabilities");
                    }
                    var sts = tys.zip(nbs).map(tn -> (RelDataType) RelType.fromString(tn.component1().asText(),
                            tn.component2().asBoolean()));
                    collected = collected.appended(new CosetteTable(nm.asText(), fds, sts, kgs, Set.empty()));
                } catch (Exception e) {
                    return Result.err(
                            String.format("Broken table schemas: %s in\n%s", e.getMessage(), schema.toPrettyString()));
                }
            }
            return Result.ok(collected);
        });
        if (tables.isErr()) {
            return Result.err(tables.getErr());
        }
        env.tables().appendAll(tables.get());
        var queries = array(node, "queries");
        if (queries.isErr()) {
            return Result.err(queries.getErr());
        }
        var shuttle = new RelJSONShuttle(env);
        return queries.get().map(q -> {
            var builder = RuleBuilder.create();
            tables.get().forEach(builder::addTable);
            return shuttle.deserialize(builder, q);
        }).foldLeft(Result.ok(ImmutableSeq.empty()), (qs, qb) -> qs.flatMap(s -> qb.map(b -> s.appended(b.build()))));
    }

    public JsonNode serialize(RelNode rel) {
        return switch (rel) {
            case TableScan scan ->
                    object(Map.of("scan", new IntNode(env.resolve(scan.getTable().unwrap(CosetteTable.class)))));
            case LogicalValues values -> {
                var visitor = new RexJSONVisitor(env);
                var schema = array(Seq.from(values.getRowType().getFieldList())
                        .map(field -> new TextNode(field.getType().toString())));
                var records = array(Seq.from(values.getTuples())
                        .map(tuple -> array(Seq.from(tuple).map(visitor::serialize))));
                yield object(Map.of("values", object(Map.of("schema", schema, "content", records))));
            }
            case LogicalFilter filter -> {
                var visitor = new RexJSONVisitor(env.advanced(filter.getInput().getRowType().getFieldCount())
                        .recorded(filter.getVariablesSet()));
                yield object(Map.of("filter",
                        object(Map.of("condition", visitor.serialize(filter.getCondition()), "source",
                                serialize(filter.getInput())))));
            }
            case LogicalProject project -> {
                var visitor = new RexJSONVisitor(env.advanced(project.getInput().getRowType().getFieldCount())
                        .recorded(project.getVariablesSet()));
                var targets = array(Seq.from(project.getProjects()).map(visitor::serialize));
                yield object(
                        Map.of("project", object(Map.of("target", targets, "source", serialize(project.getInput())))));
            }
            case LogicalJoin join -> {
                var left = join.getLeft();
                var right = join.getRight();
                var visitor = new RexJSONVisitor(
                        env.advanced(left.getRowType().getFieldCount() + right.getRowType().getFieldCount())
                                .recorded(join.getVariablesSet()));
                yield object(Map.of("join",
                        object(Map.of("kind", new TextNode(join.getJoinType().toString()), "condition",
                                visitor.serialize(join.getCondition()), "left", serialize(left), "right",
                                serialize(right)))));
            }
            case LogicalCorrelate correlate -> {
                var rightShuttle = new RelJSONShuttle(env.advanced(correlate.getLeft().getRowType().getFieldCount())
                        .recorded(correlate.getVariablesSet()).advanced(0));
                yield object(Map.of("correlate",
                        array(Seq.of(serialize(correlate.getLeft()), rightShuttle.serialize(correlate.getRight())))));
            }
            case LogicalAggregate aggregate -> {
                var groupCount = aggregate.getGroupCount();
                var level = env.base();
                var types = Seq.from(aggregate.getInput().getRowType().getFieldList())
                        .map(type -> new TextNode(type.getType().toString()));
                var keyCols = array(Seq.from(aggregate.getGroupSet())
                        .map(key -> object(Map.of("column", new IntNode(level + key), "type", types.get(key)))));
                var keys = object(Map.of("project",
                        object(Map.of("target", keyCols, "source", serialize(aggregate.getInput())))));
                var conditions = array(Seq.from(aggregate.getGroupSet()).mapIndexed((i, key) -> {
                    var type = types.get(key);
                    var leftCol = object(Map.of("column", new IntNode(level + i), "type", type));
                    var rightCol = object(Map.of("column", new IntNode(level + groupCount + key), "type", type));
                    return object(
                            Map.of("operator", new TextNode("<=>"), "operand", array(Seq.of(leftCol, rightCol)), "type",
                                    new TextNode("BOOLEAN")));
                }));
                var condition = object(Map.of("operator", new TextNode("AND"), "operand", conditions, "type",
                        new TextNode("BOOLEAN")));
                var aggs = array(Seq.from(aggregate.getAggCallList()).map(call -> object(
                        Map.of("operator", new TextNode(call.getAggregation().getName()), "operand",
                                array(Seq.from(call.getArgList()).map(target -> object(
                                        Map.of("column", new IntNode(level + groupCount + target), "type",
                                                types.get(target))))), "distinct", bool(call.isDistinct()),
                                "ignoreNulls", bool(call.ignoreNulls()), "type",
                                new TextNode(call.getType().toString())))));
                var aggregated = object(Map.of("aggregate", object(Map.of("function", aggs, "source",
                        object(Map.of("filter", object(Map.of("condition", condition, "source",
                                new RelJSONShuttle(env.lifted(groupCount)).serialize(aggregate.getInput())))))))));
                yield object(Map.of("distinct", object(Map.of("correlate", array(Seq.of(keys, aggregated))))));
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
                var types = Seq.from(sort.getInput().getRowType().getFieldList())
                        .map(type -> new TextNode(type.getType().toString()));
                var collations = array(Seq.from(sort.collation.getFieldCollations()).map(collation -> {
                    var index = collation.getFieldIndex();
                    return array(Seq.of(new IntNode(index), types.get(index), new TextNode(collation.shortString())));
                }));
                var args = object(Map.of("collation", collations, "source", serialize(sort.getInput())));
                var visitor = new RexJSONVisitor(env.advanced(sort.getInput().getRowType().getFieldCount()));
                if (sort.offset != null) {
                    args.set("offset", visitor.serialize(sort.offset));
                }
                if (sort.fetch != null) {
                    args.set("limit", visitor.serialize(sort.fetch));
                }
                yield object(Map.of("sort", args));
            }
            default -> throw new RuntimeException("Not implemented: " + rel.getRelTypeName());
        };
    }

    public Result<RuleBuilder, String> deserialize(RuleBuilder builder, JsonNode jsonNode) {
        var entry = jsonNode.fields().next();
        var kind = entry.getKey();
        var content = entry.getValue();
        return switch (kind) {
            case String k when k.equals("scan") -> {
                if (content.isInt() && 0 <= content.asInt() && content.asInt() < env.tables().size()) {
                    builder.scan(env.tables().get(content.asInt()).getName());
                    yield Result.ok(builder);
                }
                yield Result.err(String.format("Missing table with index %s", content.toPrettyString()));
            }
            case String k when k.equals("values") -> {
                try {
                    var et = unwrap(array(content, "schema"));
                    var rt = new RelRecordType(StructKind.FULLY_QUALIFIED, et.mapIndexed(
                            (i, t) -> (RelDataTypeField) new RelDataTypeFieldImpl(String.format("VALUES-%s", i), i,
                                    RelType.fromString(t.asText(), true))).asJava());
                    var vs = unwrap(array(content, "content"));
                    var vals = ImmutableSeq.<List<RexLiteral>>empty();
                    for (var v : vs) {
                        var val = ImmutableSeq.<RexLiteral>empty();
                        if (!v.isArray()) {
                            yield Result.err("Expecting tuple (JSON list) as value");
                        }
                        for (var jl : Seq.from(v.elements())) {
                            var l = unwrap(new RexJSONVisitor(env).deserialize(builder, jl));
                            if (l instanceof RexLiteral) {
                                val = val.appended((RexLiteral) l);
                            } else {
                                yield Result.err("Expecting literal expression");
                            }
                        }
                        vals = vals.appended(val.asJava());
                    }
                    builder.values(vals.asJava(), rt);
                    yield Result.ok(builder);
                } catch (Exception e) {
                    yield Result.err(e.getMessage());
                }
            }
            case String k when k.equals("filter") -> {
                try {
                    var cond = unwrap(object(content, "condition"));
                    var source = unwrap(object(content, "source"));
                    var bs = unwrap(deserialize(builder, source));
                    var c = unwrap(new RexJSONVisitor(env).deserialize(builder, cond));
                    bs.filter(c);
                    yield Result.ok(bs);
                } catch (Exception e) {
                    yield Result.err(e.getMessage());
                }
            }
            case String k when k.equals("project") -> {
                try {
                    var target = unwrap(array(content, "target"));
                    var source = unwrap(object(content, "source"));
                    var bs = unwrap(deserialize(builder, source));
                    var ps = target.mapChecked(t -> unwrap(new RexJSONVisitor(env).deserialize(builder, t)));
                    bs.project(ps);
                    yield Result.ok(bs);
                } catch (Exception e) {
                    yield Result.err(e.getMessage());
                }
            }
            case String k when k.equals("join") -> Result.err("Not implemented yet");
            case String k when k.equals("correlate") -> Result.err("Not implemented yet");
            default -> Result.err(String.format("Unrecognized node:\n%s", jsonNode.toPrettyString()));
        };
    }

    public record RexJSONVisitor(Env env) {
        public JsonNode serialize(RexNode rex) {
            return switch (rex) {
                case RexInputRef inputRef ->
                        object(Map.of("column", new IntNode(inputRef.getIndex() + env.base()), "type",
                                new TextNode(inputRef.getType().toString())));
                case RexLiteral literal -> object(Map.of("operator",
                        new TextNode(literal.getValue() == null ? "NULL" : literal.getValue().toString()), "operand",
                        array(Seq.empty()), "type", new TextNode(literal.getType().toString())));
                case RexSubQuery subQuery ->
                        object(Map.of("operator", new TextNode(subQuery.getOperator().toString()), "operand",
                                array(Seq.from(subQuery.getOperands()).map(this::serialize)), "query",
                                new RelJSONShuttle(env.advanced(0)).serialize(subQuery.rel), "type",
                                new TextNode(subQuery.getType().toString())));
                case RexCall call -> object(Map.of("operator", new TextNode(call.getOperator().toString()), "operand",
                        array(Seq.from(call.getOperands()).map(this::serialize)), "type",
                        new TextNode(call.getType().toString())));
                case RexFieldAccess fieldAccess -> object(Map.of("column", new IntNode(
                                fieldAccess.getField().getIndex() +
                                        env.resolve(((RexCorrelVariable) fieldAccess.getReferenceExpr()).id)), "type",
                        new TextNode(fieldAccess.getType().toString())));
                default -> throw new RuntimeException("Not implemented: " + rex.getKind());
            };
        }

        public Result<RexNode, String> deserialize(RuleBuilder builder, JsonNode jsonNode) {
            if (jsonNode.has("column") && jsonNode.get("column").isInt()) {
                // WARNING: THIS IS WRONG! NO ENVIRONMENT CONSIDERED!
                return Result.ok(builder.field(jsonNode.get("column").asInt()));
            } else if (jsonNode.has("operator") && jsonNode.get("operator").isTextual()) {
                var op = jsonNode.get("operator").asText();
                try {
                    var args = unwrap(array(jsonNode, "operand"));
                    var ty = RelType.fromString(unwrap(object(jsonNode, "type")).asText(), true);
                    if (args.isEmpty()) {
                        return Result.ok(RexLiteral.fromJdbcString(ty, ty.getSqlTypeName(), op));
                    } else {
                        var fields = args.mapChecked(expr -> unwrap(deserialize(builder, expr)));
                        for (var refl : Seq.from(SqlStdOperatorTable.class.getDeclaredFields())
                                .filter(f -> java.lang.reflect.Modifier.isPublic(f.getModifiers()) &&
                                        java.lang.reflect.Modifier.isStatic(f.getModifiers()))) {
                            var mist = refl.get(null);
                            if (mist instanceof SqlOperator sqlOperator && sqlOperator.getName().equals(op)) {
                                return Result.ok(builder.call(sqlOperator, fields));
                            }
                        }
                        return Result.ok(builder.call(builder.genericProjectionOp(op, ty), fields));
                    }
                } catch (Exception e) {
                    return Result.err(e.getMessage());
                }
            }
            return Result.err(String.format("Unrecognized node:\n%s", jsonNode.toPrettyString()));
        }
    }
}