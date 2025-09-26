package org.qed.RRuleInstances;

import kala.collection.Map;
import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RuleBuilder;

public record SemiJoinFilterTranspose() implements RRule {
    static final RelRN left = RelRN.scan("Left", "Left_Type");
    static final RelRN right = RelRN.scan("Right", "Right_Type");
    static final RexRN joinCond = left.joinPred("join", right);
    static final RexRN filterPred = left.pred("filter");

    @Override
    public RelRN before() {
        return left.join(JoinRelType.SEMI, joinCond, right).filter(filterPred);
    }

    @Override
    public RelRN after() {
        RelRN leftFiltered = left.filter(filterPred);
        return leftFiltered.join(JoinRelType.SEMI, leftFiltered.joinPred("join", right), right);
    }
}
