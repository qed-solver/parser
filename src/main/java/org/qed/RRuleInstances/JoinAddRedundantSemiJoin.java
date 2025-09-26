package org.qed.RRuleInstances;

import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RelRN;
import org.qed.RRule;

public record JoinAddRedundantSemiJoin() implements RRule {
    static final RelRN left = RelRN.scan("Left", "Left_Type");
    static final RelRN right = RelRN.scan("Right", "Right_Type");
    static final String pred = "pred";

    @Override
    public RelRN before() {
        return left.join(JoinRelType.INNER, pred, right);
    }

    @Override
    public RelRN after() {
        return left.join(JoinRelType.SEMI, pred, right).join(JoinRelType.INNER, pred, right);
    }
}
