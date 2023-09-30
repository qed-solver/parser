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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.util.ImmutableBitSet;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.NoSuchElementException;

public record RelJSONShuttle(Env env) {
    private final static ObjectMapper mapper = new ObjectMapper();

    private static ArrayNode array(Seq<JsonNode> objs) {
        return new ArrayNode(mapper.getNodeFactory(), objs.asJava());
    }


    private static Result<ImmutableSeq<JsonNode>, String> array(JsonNode jsonNode, String field) {
        var arr = jsonNode.get(field);
        if (arr == null || !arr.isArray()) {
            return Result.err(String.format("Missing array field %s in JSON", field));
        }
        return Result.ok(ImmutableSeq.from(arr.elements()));
    }

    private static ObjectNode object(Map<String, JsonNode> fields) {
        return new ObjectNode(mapper.getNodeFactory(), fields.asJava());
    }

    private static Result<JsonNode, String> object(JsonNode jsonNode, String field) {
        var obj = jsonNode.get(field);
        if (obj == null) {
            return Result.err(String.format("Missing object field %s in JSON", field));
        }
        return Result.ok(obj);
    }

    private static BooleanNode bool(boolean b) {
        return b ? BooleanNode.TRUE : BooleanNode.FALSE;
    }

    public static void main(String[] args) throws IOException {
        var res = RelJSONShuttle.deserializeFromJson(Paths.get("ExtractedCockroachTests/pushFilterIntoJoinLeft.json"));
        if (res.isErr()) {
            System.out.println(res.getErr());
        }
    }

    public static void serializeToJson(List<RelNode> relNodes, Path path) throws IOException {
        var shuttle = new RelJSONShuttle(Env.empty());
        var helps = array(Seq.from(relNodes).map(rel -> new TextNode(rel.explain())));
        var queries = array(Seq.from(relNodes).map(shuttle::serialize));

        var tables = shuttle.env.tables();
        var schemas = array(tables.map(table -> object(Map.of("name", new TextNode(table.getName()), "fields",
                array(table.getColumnNames().map(TextNode::new)), "types",
                array(table.getColumnTypes().map(type -> new TextNode(type.toString()))), "nullable",
                array(table.getColumnTypes().map(RelDataType::isNullable).map(RelJSONShuttle::bool)), "key",
                array(Seq.from(table.getKeys().map(key -> array(Seq.from(key).map(IntNode::new))))), "guaranteed",
                array(table.getConstraints()
                        .map(check -> new RexJSONVisitor(shuttle.env.advanced(table.getColumnNames().size())).serialize(
                                check)).toImmutableSeq())))));

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
                    var tys = array(schema, "types").get();
                    var nbs = array(schema, "nullable").get();
                    var nm = object(schema, "name").get();
                    var fds = array(schema, "fields").get().map(JsonNode::asText);
                    var kys = array(schema, "key").get();
                    var kgs = Set.from(kys.map(kg -> ImmutableBitSet.of(Seq.from(kg.elements()).map(JsonNode::asInt))));
                    if (tys.size() != nbs.size()) {
                        return Result.err("Expecting corresponding types and nullabilities");
                    }
                    var sts = tys.zip(nbs).map(tn -> (RelDataType) RelType.fromString(tn.component1().asText(),
                            tn.component2().asBoolean()));
                    collected = collected.appended(new CosetteTable(nm.asText(), fds, sts, kgs, Set.empty()));
                } catch (NoSuchElementException e) {
                    return Result.err("Broken table schemas");
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
                yield Result.err(String.format("No table with index %s", content));
            }
            case String k when k.equals("values") -> Result.err("Not implemented yet");
            case String k when k.equals("filter") -> Result.err("Not implemented yet");
            case String k when k.equals("project") -> Result.err("Not implemented yet");
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
            return Result.err("RexNode deserialization not implemented yet.");
        }
    }
}