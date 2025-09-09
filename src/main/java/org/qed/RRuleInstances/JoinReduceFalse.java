package org.qed.RRuleInstances;

import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RRule;
import org.qed.RelRN;
import org.qed.RexRN;

public record JoinReduceFalse() implements RRule {
    static final RelRN left = RelRN.scan("Left", "Left_Type");
    static final RelRN right = RelRN.scan("Right", "Right_Type");
    static final RexRN joinCond = RexRN.and(left.joinPred("join", right), RexRN.falseLiteral());

    public RelRN before() {
        return left.join(JoinRelType.INNER, joinCond, right);
    }

    public RelRN after() {
        return left.join(JoinRelType.INNER, RexRN.falseLiteral(), right);
    }
}
