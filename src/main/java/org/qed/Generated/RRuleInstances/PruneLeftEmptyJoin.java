package org.qed.Generated.RRuleInstances;

import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RRule;
import org.qed.RelRN;
import org.qed.RexRN;

public record PruneLeftEmptyJoin() implements RRule {
    static final RelRN left = RelRN.scan("Left", "Left_Type");
    static final RelRN right = RelRN.scan("Right", "Right_Type");
    static final RexRN joinCond = left.joinPred("join", right);

    @Override
    public RelRN before() {
        return left.empty().join(JoinRelType.RIGHT, joinCond, right);
    }

    @Override
    public RelRN after() {
        return right;
    }
}
