package org.qed.Generated.RRuleInstances;

import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RRule;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RuleBuilder;

public record AggregateJoinRemove() implements RRule {
    static final RelRN tblA = RelRN.scan("sourceA1", "typeA1")
            .join(JoinRelType.INNER, RexRN.trueLiteral(), RelRN.scan("sourceA2", "typeA2"));

    static final RelRN tblB = RelRN.scan("sourceB1", "typeB1")
            .join(JoinRelType.INNER, RexRN.trueLiteral(), RelRN.scan("sourceB2", "typeB2"));

    static final RexRN joinCondition = new RexRN.Pred(
            RuleBuilder.create().genericPredicateOp("=", true),
            Seq.of(tblA.field(0), tblB.field(0))
    );

    @Override
    public RelRN before() {
        RelRN leftJoin = tblA.join(JoinRelType.LEFT, joinCondition, tblB);
        return new RelRN.Aggregate(leftJoin, Seq.of(leftJoin.field(0)), Seq.empty());
    }

    @Override
    public RelRN after() {
        return new RelRN.Aggregate(tblA, Seq.of(tblA.field(0)), Seq.empty());
    }
}