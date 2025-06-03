package org.qed.Generated.RRuleInstances;

import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;

public record ProjectJoinTranspose() implements RRule {
    static final RelRN left = RelRN.scan("Left", "left_type");
    static final RelRN right = RelRN.scan("Right", "right_type");
    static final RelRN join = left.join(JoinRelType.INNER, left.joinPred("cond", right), right);
    static final RexRN proj = join.proj("proj", "proj_type");

    @Override
    public RelRN before() {
        return join.project(proj);
    }

    @Override
    public RelRN after() {
        RelRN newLeft = left.project(proj);
        RelRN newRight = right.project(proj);
        RelRN newJoin = newLeft.join(JoinRelType.INNER, newLeft.joinPred("cond", newRight), newRight);
        return newJoin.project(proj);
    }
}

