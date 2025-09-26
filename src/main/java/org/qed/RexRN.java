package org.qed;

import kala.collection.Seq;

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.qed.RexRN.And;
import org.qed.RexRN.False;
import org.qed.RexRN.GroupBy;
import org.qed.RexRN.Pred;
import org.qed.RexRN.Proj;
import org.qed.RexRN.True;

public interface RexRN {

    static RelType.VarType varType(String id, boolean nullable) {
        return new RelType.VarType(id, nullable);
    }

    static And and(RexRN... sources) {
        return new And(Seq.from(sources));
    }

    static False falseLiteral() {
        return new False();
    }

    static True trueLiteral() {
        return new True();
    }

    RexNode semantics();

    default Pred pred(SqlOperator op) {
        return new Pred(op, Seq.of(this));
    }

    default Pred pred(String name) {
        return pred(RuleBuilder.create().genericPredicateOp(name, true));
    }

    default Proj proj(SqlOperator op) {
        return new Proj(op, Seq.of(this));
    }

    default Proj proj(String name, String type_name) {
        return proj(RuleBuilder.create().genericProjectionOp(name, new RelType.VarType(type_name, true)));
    }

    default GroupBy groupBy(SqlOperator op) {
        return new GroupBy(op, Seq.of(this));
    }

    default GroupBy groupBy(String name) {
        return groupBy(RuleBuilder.create().genericProjectionOp(name, new RelType.VarType(name + "_type", true)));
    }

    default RelRN.AggCall aggCall(String name) {
        return new RelRN.AggCall(
            name,
            RuleBuilder.create().genericAggregateOp(name, new RelType.VarType(name + "_type", true)),
            false,
            new RelType.VarType(name + "_type", true),
            Seq.of(this)
        );
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

    record Pred(SqlOperator operator, Seq<RexRN> sources) implements RexRN {

        @Override
        public RexNode semantics() {
            var builder = RuleBuilder.create();
            // builder.genericPredicateOp(name, nullable)
            return builder.call(operator, sources.map(RexRN::semantics));
        }
    }

    record Proj(SqlOperator operator, Seq<RexRN> sources) implements RexRN {

        @Override
        public RexNode semantics() {
            var builder = RuleBuilder.create();
            // builder.genericProjectionOp(name, varType(type_name, nullable))
            return builder.call(operator, sources.map(RexRN::semantics));
        }
    }

    record GroupBy(SqlOperator operator, Seq<RexRN> sources) implements RexRN {
        
        @Override
        public RexNode semantics() {
            var builder = RuleBuilder.create();
            return builder.call(operator, sources.map(RexRN::semantics));
        }
    }

    record And(Seq<RexRN> sources) implements RexRN {

        @Override
        public RexNode semantics() {
            return RuleBuilder.create().and(sources.map(RexRN::semantics));
        }
    }

    record False() implements RexRN {

        @Override
        public RexNode semantics() {
            return RuleBuilder.create().literal(false);
        }
    }

    record Not(RexRN source) implements RexRN {

        @Override
        public RexNode semantics() {
            return RuleBuilder.create().not(source.semantics());
        }
    }

    record Or(Seq<RexRN> sources) implements RexRN {

        @Override
        public RexNode semantics() {
            return RuleBuilder.create().or(sources.map(RexRN::semantics));
        }
    }

    record True() implements RexRN {

        @Override
        public RexNode semantics() {
            return RuleBuilder.create().literal(true);
        }
    }
}
