package org.qed.Generated.RRuleInstances;

import kala.collection.Map;
import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RuleBuilder;

public record FilterIntoJoin() implements RRule {
    static final RelRN left = RelRN.scan("Left", "Left_Type");
    static final RelRN right = RelRN.scan("Right", "Right_Type");
    static final RexRN joinCond = left.joinPred("join", right);

    @Override
    public RelRN before() {
        var join = left.join(JoinRelType.INNER, joinCond, right);
        return join.filter("outer");
    }

    @Override
    public RelRN after() {
        return left.join(JoinRelType.SEMI, RexRN.and(joinCond, left.joinPred("outer", right)), right);
    }
}
