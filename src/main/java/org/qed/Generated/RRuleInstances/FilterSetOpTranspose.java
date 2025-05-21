package org.qed.Generated.RRuleInstances;

import kala.collection.Map;
import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RuleBuilder;

public record FilterSetOpTranspose() implements RRule {
    static final RelRN left = RelRN.scan("Left", "Common_Type");
    static final RelRN right = RelRN.scan("Right", "Common_Type");
    
    @Override
    public RelRN before() {
        RelRN projTmp = left.union(false, right);
        return projTmp.filter(projTmp.pred("filter"));
    }
    
    @Override
    public RelRN after() {
        RexRN leftPred = left.pred("filter");
        RexRN rightPred = right.pred("filter");
        return left.filter(leftPred).union(false, right.filter(rightPred));
    }
}
