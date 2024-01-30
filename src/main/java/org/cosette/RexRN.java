package org.cosette;

import kala.collection.Seq;
import org.apache.calcite.rex.RexNode;

import java.util.function.Function;

public interface RexRN {
    Function<RuleBuilder, RexNode> semantics();

    static Field field(int ord) {
        return new Field(ord);
    }

    static Proj proj(String id, RelType.VarType ty, RexRN... sources) {
        return new Proj(id, ty, Seq.of(sources));
    }

    static Pred pred(String id, RexRN... sources) {
        return new Pred(id, Seq.of(sources));
    }

    record Field(int ord) implements RexRN {
        @Override
        public Function<RuleBuilder, RexNode> semantics() {
            return builder -> builder.field(ord);
        }
    }

    record Proj(String id, RelType.VarType ty, Seq<RexRN> sources) implements RexRN {

        @Override
        public Function<RuleBuilder, RexNode> semantics() {
            return builder -> builder.call(builder.genericProjectionOp(id, ty), sources.map(s -> s.semantics().apply(builder)));
        }
    }

    record Pred(String id, Seq<RexRN> sources) implements RexRN {

        @Override
        public Function<RuleBuilder, RexNode> semantics() {
            return builder -> builder.call(builder.genericPredicateOp(id, false), sources.map(s -> s.semantics().apply(builder)));
        }
    }

}
