package org.qed.RRuleInstances;

import kala.collection.Map;
import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RuleBuilder;

public record JoinCommute() implements RRule {
    static final RelRN left = RelRN.scan("Left", "Left_Type");
    static final RelRN right = RelRN.scan("Right", "Right_Type");
    static final RexRN joinCond = left.joinPred("pred", right);

    @Override
    public RelRN before() {
        return left.join(JoinRelType.INNER, joinCond, right);
    }

    @Override
    public RelRN after() {
        // We need to swap the join fields in the condition
        RexRN commutedJoinCond = right.joinPred("pred", left);
        return right.join(JoinRelType.INNER, commutedJoinCond, left);
    }
}
