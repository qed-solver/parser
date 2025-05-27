package org.qed.Generated.RRuleInstances;

import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RRule;
import org.qed.RelRN;
import org.qed.RexRN;

record SemiJoinRemove() implements RRule {
    static final RelRN left = RelRN.scan("Left", "Left_Type");
    static final RelRN right = RelRN.scan("Right", "Right_Type");

    @Override
    public RelRN before() {
        return left.join(JoinRelType.SEMI, RexRN.trueLiteral(), right);
    }

    @Override
    public RelRN after() {
        return left;
    }
}
