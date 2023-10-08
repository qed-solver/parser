package org.cosette;

import com.fasterxml.jackson.databind.JsonNode;
import kala.collection.Seq;
import kala.collection.Set;
import kala.collection.immutable.ImmutableSeq;
import kala.control.Try;
import kala.function.CheckedFunction;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import java.text.NumberFormat;
import java.util.List;

public record JSONDeserializer(RuleBuilder builder) {

    private record Rel(RuleBuilder builder, ImmutableSeq<RexNode> globals, ImmutableSeq<CosetteTable> tables) implements CheckedFunction<JsonNode, RelNode, Exception> {
        Rel(RuleBuilder builder) {
            this(builder, ImmutableSeq.empty(), ImmutableSeq.empty());
        }

        RexCorrelVariable corr(RelDataType type) {
            return (RexCorrelVariable) builder().getRexBuilder().makeCorrel(type, builder().getCluster().createCorrel());
        }

        Rel lifted(RexNode corr) {
            var rex = builder().getRexBuilder();
            var vars = ImmutableSeq.fill(corr.getType().getFieldCount(), i -> rex.makeFieldAccess(corr, i));
            return new Rel(builder(), globals().appendedAll(vars), tables());
        }

        Rex rex(RexCorrelVariable local) {
            return new Rex(builder(), globals(), local, tables());
        }

        Rex rex() {
            var empty = (RexCorrelVariable) builder().getRexBuilder().makeCorrel(
                    builder().getTypeFactory().createStructType(List.of()),
                    builder().getCluster().createCorrel()
            );
            return new Rex(builder(), globals(), empty, tables());
        }

        JoinRelType kind(String k) throws Exception {
            return Enum.valueOf(JoinRelType.class, k);
        }

        public RelNode applyChecked(JsonNode node) throws Exception {
            return deserialize(node);
        }

        public RelNode deserialize(JsonNode node) throws Exception {
            var entry = node.fields().next();
            var kind = entry.getKey();
            var content = entry.getValue();
            return switch (kind) {
                case "scan" -> builder().scan(tables().get(integer(content)).getName()).build();
                case "values" -> {
                    var et = array(content, "schema");
                    var rt = new RelRecordType(StructKind.FULLY_QUALIFIED, et.mapIndexedChecked(
                            (i, t) -> (RelDataTypeField) new RelDataTypeFieldImpl(String.format("VALUES-%s", i), i,
                                    RelType.fromString(string(t), true))).asJava());
                    var tuples = array(content, "content").mapChecked(v ->
                            array(v).mapChecked(jl -> (RexLiteral) rex().deserialize(jl)).asJava()
                    );
                    yield builder().values(tuples.asJava(), rt).build();
                }
                case "filter" -> {
                    var input = deserialize(content.required("source"));
                    var corr = corr(input.getRowType());
                    var cond = rex(corr).deserialize(content.required("condition"));
                    yield builder().push(input).filter(Seq.of(corr.id), cond).build();
                }
                case "project" -> {
                    var input = deserialize(content.required("source"));
                    var corr = corr(input.getRowType());
                    var rex = rex(corr);
                    var projections = array(content, "target").mapChecked(rex);
                    yield builder().push(input).project(projections, Seq.empty(), false, Seq.of(corr.id)).build();
                }
                case "join" -> {
                    var left = deserialize(content.required("left"));
                    var right = deserialize(content.required("right"));
                    var corr = corr(builder().getTypeFactory().createJoinType(left.getRowType(), right.getRowType()));
                    var cond = rex(corr).deserialize(content.required("condition"));
                    yield builder().push(left).push(right).join(kind(string(content, "kind")), cond, java.util.Set.of(corr.id)).build();
                }
                case "correlate" -> {
                    var left = deserialize(content.required("left"));
                    var corr = corr(left.getRowType());
                    var right = lifted(corr).deserialize(content.required("right"));
                    var rex = builder().getRexBuilder();
                    var required = Seq.from(RelOptUtil.correlationColumns(corr.id, right)).map(i -> rex.makeInputRef(left, i));
                    yield builder().push(left).push(right).correlate(kind(string(content, "kind")), corr.id, required).build();
                }
                case "union" -> {
                    var inputs = array(content).mapChecked(this::deserialize);
                    yield builder().pushAll(inputs).union(true, inputs.size()).build();
                }
                case "intersect" -> {
                    var inputs = array(content).mapChecked(this::deserialize);
                    yield builder().pushAll(inputs).intersect(false, inputs.size()).build();
                }
                case "except" -> {
                    var inputs = array(content).mapChecked(this::deserialize);
                    yield builder().pushAll(inputs).minus(false, inputs.size()).build();
                }
                case "distinct" -> builder().push(deserialize(content)).distinct().build();
                case "group" -> {
                    var input = deserialize(content);
                    var rex = rex(corr(input.getRowType()));
                    var keys = builder().groupKey(array(content, "key").mapChecked(rex));
                    yield builder().aggregate(keys, array(content, "agg").mapChecked(rex::agg)).build();
                }
                default -> throw new Exception(String.format("Unrecognized node:\n%s", node.toPrettyString()));
            };
        }
    }

    private record Rex(RuleBuilder builder, ImmutableSeq<RexNode> globals, RexCorrelVariable local, ImmutableSeq<CosetteTable> tables) implements CheckedFunction<JsonNode, RexNode, Exception> {
        public RexNode resolve(int lvl) {
            assert lvl < globals().size() + local().getType().getFieldCount();
            return lvl < globals().size() ? globals().get(lvl) : builder().getRexBuilder().makeInputRef(local().getType(), lvl - globals().size());
        }

        public Rel rel() {
            var rex = builder().getRexBuilder();
            var locals = Seq.fill(local().getType().getFieldCount(), i -> rex.makeFieldAccess(local(), i));
            return new Rel(builder(), globals().appendedAll(locals), tables());
        }

        public RexNode applyChecked(JsonNode node) throws Exception {
            return deserialize(node);
        }

        RelDataType type(String name) {
            var typeName = Enum.valueOf(SqlTypeName.class, name);
            return builder().getTypeFactory().createSqlType(typeName);
        }

        SqlOperator op(String name) throws Exception {
            var candidates = new java.util.ArrayList<SqlOperator>();
            builder().getRexBuilder().getOpTab()
                    .lookupOperatorOverloads(new SqlIdentifier(name, SqlParserPos.ZERO), null, SqlSyntax.FUNCTION_STAR, candidates, SqlNameMatchers.liberal());
            if (candidates.isEmpty()) throw new Exception("Unknown operator name.");
            if (candidates.size() > 1) throw new Exception("Ambiguous operator name.");
            return candidates.get(0);
        }

        RelBuilder.AggCall agg(JsonNode node) throws Exception {
            return builder().aggregateCall((SqlAggFunction) op(string(node, "operator")), array(node, "operand").mapChecked(this::deserialize));
        }

        public RexNode deserialize(JsonNode node) throws Exception {
            var rex = builder().getRexBuilder();
            if (node.has("column")) {
                return resolve(integer(node, "column"));
            } else if (node.has("query")) {
                var operator = string(node, "operator");
                var operands = array(node, "operand").mapChecked(this);
                var query = rel().deserialize(node.required("query"));
                return switch (operator.toLowerCase()) {
                    case "exists" -> RexSubQuery.exists(query);
                    case "unique" -> RexSubQuery.unique(query);
                    case "in" -> builder().in(query, operands);
                    default -> throw new Exception("Unknown subquery");
                };
            } else {
                var operator = string(node, "operator");
                var operands = array(node, "operand");
                var type = type(string(node, "type"));
                if (operands.isEmpty()) {
                    return switch (operator.toLowerCase()) {
                        case "null" -> rex.makeNullLiteral(type);
                        case String lit -> Try.of(() -> rex.makeLiteral(Boolean.parseBoolean(lit), type))
                                .getOrElse(() -> Try.of(() -> rex.makeLiteral(NumberFormat.getInstance().parse(lit), type))
                                        .getOrElse(() -> rex.makeLiteral(lit, type)));
                    };
                } else {
                    return builder().getRexBuilder().makeCall(op(operator), operands.mapChecked(this::deserialize).asJava());
                }
            }
        }
    }

    private static ImmutableSeq<JsonNode> array(JsonNode node) throws Exception {
        if (!node.isArray()) throw new Exception();
        return ImmutableSeq.from(node.elements());
    }

    private static ImmutableSeq<JsonNode> array(JsonNode node, String path) throws Exception {
        return array(node.required(path));
    }

    private static String string(JsonNode node) throws Exception {
        if (!node.isTextual()) throw new Exception();
        return node.asText();
    }

    private static String string(JsonNode node, String path) throws Exception {
        return string(node.required(path));
    }

    private static int integer(JsonNode node) throws Exception {
        if (!node.isInt()) throw new Exception();
        return node.asInt();
    }

    private static int integer(JsonNode node, String path) throws Exception {
        return integer(node.required(path));
    }

    private static boolean bool(JsonNode node) throws Exception {
        if (!node.isBoolean()) throw new Exception();
        return node.asBoolean();
    }

    private void schema(JsonNode json) throws Exception {
        array(json, "schema").forEachChecked(schema -> {
            var types = array(schema, "types").mapChecked(JSONDeserializer::string);
            var nullabilities = array(schema, "nullable").mapChecked(JSONDeserializer::bool);
            var name = string(schema, "name");
            var fields = array(schema, "fields").mapChecked(JSONDeserializer::string);
            var keys = Set.from(array(schema, "key")
                    .map(CheckedFunction.of(key -> ImmutableBitSet.of(array(key).mapChecked(JSONDeserializer::integer)))));
            if (types.size() != nullabilities.size()) throw new Exception("Expecting corresponding types and nullabilities");
            var sts = types.zip(nullabilities).map(tn -> (RelDataType) RelType.fromString(tn.component1(), tn.component2()));
            builder.addTable(new CosetteTable(name, fields, sts, keys, Set.empty()));
        });
    }
}

