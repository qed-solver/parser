package org.qed.Generated.RRuleInstances;

import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RRule;
import org.qed.RelRN;
import org.qed.RexRN;

public record JoinPushTransitivePredicates() implements RRule {
    static final RelRN left = RelRN.scan("Left", "Left_Type");
    static final RelRN right = RelRN.scan("Right", "Right_Type");
    static final RexRN cond1 = left.joinPred("cond1", right);
    static final RexRN cond2 = left.joinPred("cond2", right);

    @Override
    public RelRN before() {
        return left.join(JoinRelType.INNER, cond1, right).filter(cond2);
    }

    @Override
    public RelRN after() {
        return left.join(JoinRelType.INNER, RexRN.and(cond1, cond2), right);
    }
}
