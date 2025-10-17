package org.qed.RRuleInstances;

import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;

public record JoinExtractFilter() implements RRule {
    static final RelRN left = RelRN.scan("Left", "Left_Type");
    static final RelRN right = RelRN.scan("Right", "Right_Type");
    static final RexRN joinCond = left.joinPred("join", right);

    @Override
    public RelRN before() {
        return left.join(JoinRelType.INNER, joinCond, right);
    }

    @Override
    public RelRN after() {
        return left.join(JoinRelType.INNER, RexRN.trueLiteral(), right).filter(joinCond);
    }
}
