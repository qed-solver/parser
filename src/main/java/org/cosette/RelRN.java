package org.cosette;

import kala.collection.Seq;
import kala.collection.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.ImmutableBitSet;

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

    default Seq<RexRN> fields() {
        return Seq.from(IntStream.range(0, semantics().getRowType().getFieldCount()).iterator()).map(this::field);
    }

    default RexRN joinField(int ordinal, RelRN right) {
        return new RexRN.JoinField(ordinal, this, right);
    }

    default Seq<RexRN> joinFields(RelRN right) {
        return Seq.from(IntStream.range(0, semantics().getRowType().getFieldCount() + right.semantics().getRowType().getFieldCount()).iterator()).map(i -> joinField(i, right));
    }

    default RexRN.Pred pred(String name) {
        return new RexRN.Pred(name, true, fields());
    }

    default RexRN.Pred joinPred(String name, RelRN right) {
        return new RexRN.Pred(name, true, joinFields(right));
    }

    default RexRN.Proj proj(String name, String type_name) {
        return new RexRN.Proj(name, type_name, true, fields());
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

    default Join join(JoinRelType ty, RexRN cond, RelRN right) {
        return new Join(ty, cond, this, right);
    }

    default Join join(JoinRelType ty, String name, RelRN right) { return join(ty, joinPred(name, right), right); }

    default Union union(boolean all, RelRN... sources) {
        return new Union(all, Seq.of(this).appendedAll(sources));
    }

    default Intersect intersect(boolean all, RelRN... sources) {
        return new Intersect(all, Seq.of(this).appendedAll(sources));
    }

    record Scan(String name, RelType.VarType ty, boolean unique) implements RelRN {

        @Override
        public RelNode semantics() {
            var table = new CosetteTable(name, Seq.of(STR."col-\{name}"), Seq.of(ty), unique ? Set.of(ImmutableBitSet.of(0)) : Set.empty(), Set.empty());
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

    record Join(JoinRelType ty, RexRN cond, RelRN left, RelRN right) implements RelRN {
        @Override
        public RelNode semantics() {
            return RuleBuilder.create().push(left.semantics()).push(right.semantics()).join(ty, cond.semantics()).build();
        }

        @Override
        public RexRN field(int ordinal) {
            return new RexRN.JoinField(ordinal, left, right);
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

}
