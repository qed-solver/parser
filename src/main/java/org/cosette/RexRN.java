package org.cosette;

import kala.collection.Seq;
import org.apache.calcite.rex.RexNode;

public interface RexRN {

    static RelType.VarType varType(String id, boolean nullable) {
        return new RelType.VarType(id, nullable);
    }
    static And and(RexRN ...sources) {
        return new And(Seq.from(sources));
    }

    RexNode semantics();

    default Pred pred(String name) {
        return new Pred(name, true, Seq.of(this));
    }

    default Proj proj(String name, String type_name) {
        return new Proj(name, type_name, true, Seq.of(this));
    }

    record Field(int ordinal, RelRN source) implements RexRN {

        @Override
        public RexNode semantics() {
            return RuleBuilder.create().push(source.semantics()).field(ordinal);
        }
    }

    record JoinField(int ordinal, RelRN left, RelRN right) implements RexRN {

        @Override
        public RexNode semantics() {
            var leftCols = left.semantics().getRowType().getFieldCount();
            return RuleBuilder.create().push(left.semantics()).push(right.semantics()).field(2, ordinal < leftCols ?
                    0 : 1, ordinal < leftCols ? ordinal : ordinal - leftCols);
        }
    }

    record Pred(String name, boolean nullable, Seq<RexRN> sources) implements RexRN {

        @Override
        public RexNode semantics() {
            var builder = RuleBuilder.create();
            return builder.call(builder.genericPredicateOp(name, nullable), sources.map(RexRN::semantics));
        }
    }

    record Proj(String name, String type_name, boolean nullable, Seq<RexRN> sources) implements RexRN {

        @Override
        public RexNode semantics() {
            var builder = RuleBuilder.create();
            return builder.call(builder.genericProjectionOp(name, varType(type_name, nullable)),
                    sources.map(RexRN::semantics));
        }
    }

    record And(Seq<RexRN> sources) implements RexRN {

        @Override
        public RexNode semantics() {
            return RuleBuilder.create().and(sources.map(RexRN::semantics));
        }
    }

    record Or(Seq<RexRN> sources) implements RexRN {

        @Override
        public RexNode semantics() {
            return RuleBuilder.create().or(sources.map(RexRN::semantics));
        }
    }

    record Not(RexRN source) implements RexRN {

        @Override
        public RexNode semantics() {
            return RuleBuilder.create().not(source.semantics());
        }
    }


}
