package org.qed.RRuleInstances;

import kala.collection.Seq;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RuleBuilder;

public record JoinCommute() implements RRule {
    static final RelRN left = RelRN.scan("Left", "Left_Type");
    static final RelRN right = RelRN.scan("Right", "Right_Type");
    static final String pred = "pred";

    @Override
    public RelRN before() {
        return left.join(JoinRelType.INNER, pred, right);
    }

    @Override
    public RelRN after() {
        SqlOperator predOp = RuleBuilder.create().genericPredicateOp(pred, true);
        RexRN swappedPred = new RexRN.Pred(predOp, Seq.of(
            new RexRN.JoinField(1, right, left),
            new RexRN.JoinField(0, right, left)
        ));
        RelRN swappedJoin = right.join(JoinRelType.INNER, swappedPred, left);

        return new ProjectionRelRN(swappedJoin);
    }
    public static record ProjectionRelRN(RelRN source) implements RelRN {
        @Override
        public RelNode semantics() {
            RuleBuilder builder = RuleBuilder.create();
            builder.push(source.semantics());
            
            RexNode leftField = builder.field(1);
            RexNode rightField = builder.field(0);
            
            builder.project(leftField, rightField);
            
            return builder.build();
        }
    }
}