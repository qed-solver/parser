package org.qed.Generated.RRuleInstances;

import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RRule;
import org.qed.RelRN;
import org.qed.RexRN;


public record SemiJoinJoinTranspose() implements RRule {
    static final RelRN x = RelRN.scan("X", "x_type");
    static final RelRN y = RelRN.scan("Y", "y_type");
    static final RelRN z = RelRN.scan("Z", "z_type");

    static final RexRN innerCond = x.joinPred("inner", y);
    static final RelRN xyJoin = x.join(JoinRelType.INNER, innerCond, y);

    static final RexRN semiCond = xyJoin.joinPred("semi", z);

    @Override
    public RelRN before() {
        return xyJoin.join(JoinRelType.SEMI, semiCond, z);
    }

    @Override
    public RelRN after() {
        RelRN xzSemiJoin = x.join(JoinRelType.SEMI, x.joinPred("semi", z), z);
        return xzSemiJoin.join(JoinRelType.INNER, xzSemiJoin.joinPred("inner", y), y);
    }
}

