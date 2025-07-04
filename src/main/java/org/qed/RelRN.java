package org.qed;

import kala.collection.Seq;
import kala.collection.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Arrays;
import java.util.stream.IntStream;

public interface RelRN {
    static Scan scan(String id, RelType.VarType ty, boolean unique) {
        return new Scan(id, ty, unique);
    }

    static Scan scan(String id, String typeName) {
        return scan(id, RexRN.varType(typeName, true), false);
    }

    RelNode semantics();

    default RexRN field(int ordinal) {
        return new RexRN.Field(ordinal, this);
    }

    default Seq<RexRN> fields(int... ordinals) {
        return Seq.from(Arrays.stream(ordinals).iterator()).map(this::field);
    }

    default Seq<RexRN> fields() {
        return fields(IntStream.range(0, semantics().getRowType().getFieldCount()).toArray());
    }

    default RexRN joinField(int ordinal, RelRN right) {
        return new RexRN.JoinField(ordinal, this, right);
    }

    default Seq<RexRN> joinFields(RelRN right, int... ordinals) {
        return Seq.from(Arrays.stream(ordinals).iterator()).map(i -> joinField(i, right));
    }

    default Seq<RexRN> joinFields(RelRN right) {
        return joinFields(right, IntStream.range(0,
                semantics().getRowType().getFieldCount() + right.semantics().getRowType().getFieldCount()).toArray());
    }

    default RexRN.Pred pred(SqlOperator op) {
        return new RexRN.Pred(op, fields());
    }

    default RexRN.Pred pred(String name) {
        return pred(RuleBuilder.create().genericPredicateOp(name, true));
    }

    default RexRN.Pred joinPred(SqlOperator op, RelRN right) {
        return new RexRN.Pred(op, joinFields(right));
    }

    default RexRN.Pred joinPred(String name, RelRN right) {
        return joinPred(RuleBuilder.create().genericPredicateOp(name, true), right);
    }

    default RexRN.Proj proj(SqlOperator op) {
        return new RexRN.Proj(op, fields());
    }

    default RexRN.Proj proj(String name, String type_name) {
        return proj(RuleBuilder.create().genericProjectionOp(name, new RelType.VarType(type_name, true)));
    }

    default Filter filter(RexRN cond) {
        return new Filter(cond, this);
    }

    default Filter filter(String name) {
        return filter(pred(name));
    }

    default Project project(RexRN proj) {
        return new Project(proj, this);
    }

    default Project project(String name, String type_name) {
        return project(proj(name, type_name));
    }

    default Join join(Join.JoinType ty, RexRN cond, RelRN right) {
        return new Join(ty, cond, this, right);
    }

    default Join join(JoinRelType ty, RexRN cond, RelRN right) {
        return join(new Join.JoinType.ConcreteJoinType(ty), cond, right);
    }

    default Join join(JoinRelType ty, String name, RelRN right) {return join(ty, joinPred(name, right), right);}

    default Union union(boolean all, RelRN... sources) {
        return new Union(all, Seq.of(this).appendedAll(sources));
    }

    default Intersect intersect(boolean all, RelRN... sources) {
        return new Intersect(all, Seq.of(this).appendedAll(sources));
    }

    default Minus minus (boolean all, RelRN... sources) {
        return new Minus(all, Seq.of(this).appendedAll(sources));
    }

    default Empty empty() {
        return new Empty(this);
    }

    default Aggregate aggregate(Seq<RexRN> groupSet, Seq<AggCall> aggCalls) {
        return new Aggregate(this, groupSet, aggCalls);
    }

    record Scan(String name, RelType.VarType ty, boolean unique) implements RelRN {

        @Override
        public RelNode semantics() {
            var table = new QedTable(name, Seq.of("col-" + name), Seq.of(ty), unique ?
                    Set.of(ImmutableBitSet.of(0)) : Set.empty(), Set.empty());
            return RuleBuilder.create().addTable(table).scan(name).build();
        }
    }

    record Filter(RexRN cond, RelRN source) implements RelRN {
        @Override
        public RelNode semantics() {
            return RuleBuilder.create().push(source.semantics()).filter(cond.semantics()).build();
        }
    }

    record Project(RexRN map, RelRN source) implements RelRN {
        @Override
        public RelNode semantics() {
            return RuleBuilder.create().push(source.semantics()).project(map.semantics()).build();
        }
    }

    record Join(Join.JoinType ty, RexRN cond, RelRN left, RelRN right) implements RelRN {
        @Override
        public RelNode semantics() {
            return RuleBuilder.create().push(left.semantics()).push(right.semantics()).join(ty.semantics(),
                    cond.semantics()).build();
        }

        @Override
        public RexRN field(int ordinal) {
            return new RexRN.JoinField(ordinal, left, right);
        }

        public interface JoinType {
            JoinRelType semantics();

            record ConcreteJoinType(JoinRelType type) implements JoinType {
                @Override
                public JoinRelType semantics() {
                    return type;
                }
            }

            record MetaJoinType(String name) implements JoinType {
                @Override
                public JoinRelType semantics() {
                    return JoinRelType.INNER;
                }
            }
        }

    }

    record Union(boolean all, Seq<RelRN> sources) implements RelRN {

        @Override
        public RelNode semantics() {
            return RuleBuilder.create().pushAll(sources.map(RelRN::semantics)).union(all, sources.size()).build();
        }
    }

    record Intersect(boolean all, Seq<RelRN> sources) implements RelRN {

        @Override
        public RelNode semantics() {
            return RuleBuilder.create().pushAll(sources.map(RelRN::semantics)).intersect(all, sources.size()).build();
        }
    }

    record Minus(boolean all, Seq<RelRN> sources) implements RelRN {

        @Override
        public RelNode semantics() {
            return RuleBuilder.create().pushAll(sources.map(RelRN::semantics)).minus(all, sources.size()).build();
        }
    }

    record Empty(RelRN sourceType) implements RelRN {

        @Override
        public RelNode semantics() {
            return RuleBuilder.create().values(sourceType.semantics().getRowType()).build();
        }
    }

    record AggCall(String name, boolean distinct, RelType type, Seq<RexRN> operands) {
    }

    record Aggregate(RelRN source, Seq<RexRN> groupSet, Seq<AggCall> aggCalls) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(source.semantics());
            var groupKey = builder.groupKey(groupSet.map(RexRN::semantics));
            var calls = aggCalls.map(agg -> {
                var aggFunc = builder.genericAggregateOp(agg.name(), agg.type());
                return builder.aggregateCall(aggFunc, agg.distinct(), null, agg.name(), agg.operands().map(RexRN::semantics).asJava());
            });
            return builder.aggregate(groupKey, calls).build();
        }
    }

}