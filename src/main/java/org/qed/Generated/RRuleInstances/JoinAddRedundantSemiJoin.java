package org.qed.Generated.RRuleInstances;

import kala.collection.Map;
import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RuleBuilder;

public record JoinAddRedundantSemiJoin() implements RRule {
    static final RelRN left = RelRN.scan("Left", "Left_Type");
    static final RelRN right = RelRN.scan("Right", "Right_Type");
    static final String pred = "pred";

    @Override
    public RelRN before() {
        return left.join(JoinRelType.INNER, pred, right);
    }

    @Override
    public RelRN after() {
        return left.join(JoinRelType.SEMI, pred, right).join(JoinRelType.INNER, pred, right);
    }
}
