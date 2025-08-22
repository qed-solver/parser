package org.qed.Generated.RRuleInstances;

import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RRule;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RuleBuilder;

public record AggregateJoinJoinRemove() implements RRule {
    static final RelRN tblA = RelRN.scan("sourceA1", "typeA1")
            .join(JoinRelType.INNER, RexRN.trueLiteral(), RelRN.scan("sourceA2", "typeA2"));
    static final RelRN tblB = RelRN.scan("sourceB1", "typeB1")
            .join(JoinRelType.INNER, RexRN.trueLiteral(), RelRN.scan("sourceB2", "typeB2"));
    static final RelRN tblC = RelRN.scan("sourceC1", "typeC1")
            .join(JoinRelType.INNER, RexRN.trueLiteral(), RelRN.scan("sourceC2", "typeC2"));

    static final RexRN bottomJoinCondition = new RexRN.Pred(
            RuleBuilder.create().genericPredicateOp("=", true),
            Seq.of(tblA.field(0), tblB.field(0))
    );

    static final RexRN topJoinCondition = new RexRN.Pred(
            RuleBuilder.create().genericPredicateOp("=", true),
            Seq.of(tblA.field(0), tblC.field(0))
    );

    @Override
    public RelRN before() {
        RelRN bottomJoin = tblA.join(JoinRelType.LEFT, bottomJoinCondition, tblB);
        RelRN topJoin = bottomJoin.join(JoinRelType.LEFT, topJoinCondition, tblC);
        return new RelRN.Aggregate(topJoin, Seq.of(topJoin.field(0), topJoin.field(4)), Seq.empty());
    }

    @Override
    public RelRN after() {
        RelRN newJoin = tblA.join(JoinRelType.LEFT, topJoinCondition, tblC);
        return new RelRN.Aggregate(newJoin, Seq.of(newJoin.field(0), newJoin.field(2)), Seq.empty());
    }
}