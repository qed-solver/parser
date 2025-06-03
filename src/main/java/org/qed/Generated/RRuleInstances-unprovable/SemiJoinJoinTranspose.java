package org.qed.Generated.RRuleInstances;

import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RRule;
import org.qed.RelRN;
import org.qed.RexRN;


public record SemiJoinJoinTranspose() implements RRule {
    static final RelRN left = RelRN.scan("left", "left_Type");
    static final RelRN middle = RelRN.scan("middle", "middle_Type");
    static final RelRN right = RelRN.scan("right", "right_Type");
    static final RexRN semiCond = left.joinPred("semi", middle);
    static final RexRN joinCond = left.joinPred("join", right);

    @Override
    public RelRN before() {
        return left.join(JoinRelType.SEMI, semiCond, middle).join(JoinRelType.INNER, joinCond, right);
    }

    @Override
    public RelRN after() {
        RelRN innerJoin = left.join(JoinRelType.INNER, joinCond, right);
        return innerJoin.join(JoinRelType.SEMI, semiCond, middle);
    }
}
