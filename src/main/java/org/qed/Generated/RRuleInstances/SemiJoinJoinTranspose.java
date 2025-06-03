package org.qed.Generated.RRuleInstances;

import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RRule;
import org.qed.RelRN;

public record SemiJoinJoinTranspose() implements RRule {
    static final RelRN x = RelRN.scan("X", "x_type");
    static final RelRN y = RelRN.scan("Y", "y_type");
    static final RelRN z = RelRN.scan("Z", "z_type");

    @Override
    public RelRN before() {
        RelRN joinXY = x.join(JoinRelType.INNER, x.joinPred("inner", y), y);
        return joinXY.join(JoinRelType.SEMI, joinXY.joinPred("semi", z), z);
    }

    @Override
    public RelRN after() {
        RelRN semiJoinYZ = y.join(JoinRelType.SEMI, y.joinPred("semi", z), z);
        return x.join(JoinRelType.INNER, x.joinPred("inner", semiJoinYZ), semiJoinYZ);
    }
}
