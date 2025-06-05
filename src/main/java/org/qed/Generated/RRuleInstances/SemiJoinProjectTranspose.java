package org.qed.Generated.RRuleInstances;

import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RRule;
import org.qed.RelRN;
import org.qed.RexRN;

public record SemiJoinProjectTranspose() implements RRule {
    static final RelRN left = RelRN.scan("Left", "left_type");
    static final RelRN right = RelRN.scan("Right", "right_type");
    static final RexRN proj = left.proj("proj", "proj_type");
    static final RexRN semiCond = left.joinPred("semi", right);

    @Override
    public RelRN before() {
        return left.join(JoinRelType.SEMI, semiCond, right).project(proj);
    }

    @Override
    public RelRN after() {
        RelRN projectedLeft = left.project(proj);
        RelRN projectedRight = right.project(proj);
        RexRN newSemiJoin = projectedLeft.joinPred("semi", projectedRight);
        return projectedLeft.join(JoinRelType.SEMI, newSemiJoin, projectedRight);
    }
}
