package org.cosette;

import kala.collection.Seq;
import kala.collection.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.ImmutableBitSet;

public interface RelRN {
    RelNode semantics();

    static RelType.VarType varType(String id, boolean nullable) {
        return new RelType.VarType(id, nullable);
    }

    static Scan scan(String id, RelType.VarType ty, boolean unique) {
        return new Scan(id, ty, unique);
    }

    default Filter filter(RexRN cond) {
        return new Filter(cond, this);
    }

    default Project project(Seq<RexRN> map) {
        return new Project(map, this);
    }

    default Join join(JoinRelType ty, RexRN cond, RelRN right) {
        return new Join(ty, cond, this, right);
    }

    default Union union(boolean all, RelRN... sources) {
        return new Union(all, Seq.of(this).appendedAll(sources));
    }

    default Intersect intersect(boolean all, RelRN... sources) {
        return new Intersect(all, Seq.of(this).appendedAll(sources));
    }

    record Scan(String id, RelType.VarType ty, boolean unique) implements RelRN {

        @Override
        public RelNode semantics() {
            var table = new CosetteTable(id, Seq.of("col-" + id), Seq.of(ty), unique ? Set.of(ImmutableBitSet.of(0)) : Set.empty(), Set.empty());
            return RuleBuilder.create().addTable(table).scan(id).build();
        }
    }

    record Filter(RexRN cond, RelRN source) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(source.semantics());
            return builder.filter(cond.semantics().apply(builder)).build();
        }
    }

    record Project(Seq<RexRN> map, RelRN source) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(source.semantics());
            return builder.project(map.map(m -> m.semantics().apply(builder))).build();
        }
    }

    record Join(JoinRelType ty, RexRN cond, RelRN left, RelRN right) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(left.semantics()).push(left.semantics());
            return builder.join(ty, cond.semantics().apply(builder)).build();
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
