package org.qed.RRuleInstances;

import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RRule;
import org.qed.RelRN;
import org.qed.RexRN;

public record JoinReduceTrue() implements RRule {
    static final RelRN left = RelRN.scan("Left", "Left_Type");
    static final RelRN right = RelRN.scan("Right", "Right_Type");
    static final RexRN afterJoinCond = left.joinPred("join", right);
    static final RexRN beforeJoinCond = RexRN.and(afterJoinCond, RexRN.trueLiteral());

    public RelRN before() {
        return left.join(JoinRelType.INNER, beforeJoinCond, right);
    }

    public RelRN after() {
        return left.join(JoinRelType.INNER, afterJoinCond, right);
    }
}
