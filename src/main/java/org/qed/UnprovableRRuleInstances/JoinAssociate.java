package org.qed.Generated.RRuleInstances;

import kala.collection.Map;
import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RuleBuilder;

public record JoinAssociate() implements RRule.RRuleFamily {
    static final RelRN a = RelRN.scan("A", "A_Type");
    static final RelRN b = RelRN.scan("B", "B_Type");
    static final RelRN c = RelRN.scan("C", "C_Type");
    static final String pred_ab = "pred_ab";
    static final String pred_bc = "pred_bc";
    static final RelRN.Join.JoinType.MetaJoinType mjt_0 = new RelRN.Join.JoinType.MetaJoinType("mjt_0");
    static final RelRN.Join.JoinType.MetaJoinType mjt_1 = new RelRN.Join.JoinType.MetaJoinType("mjt_1");
    static final RelRN.Join.JoinType.MetaJoinType mjt_2 = new RelRN.Join.JoinType.MetaJoinType("mjt_2");
    static final RelRN.Join.JoinType.MetaJoinType mjt_3 = new RelRN.Join.JoinType.MetaJoinType("mjt_3");

    static final RelRN before_ab = a.join(mjt_0, RexRN.and(
            a.joinPred(pred_ab, b),
            new RexRN.JoinField(1, a, b).pred(SqlStdOperatorTable.IS_NOT_NULL)
    ), b);

    static final RelRN before = before_ab.join(mjt_1, RexRN.and(
            new RexRN.Pred(RuleBuilder.create().genericPredicateOp(pred_bc, true), before_ab.joinFields(c, 1, 2)),
            new RexRN.JoinField(1, before_ab, c).pred(SqlStdOperatorTable.IS_NOT_NULL)
    ), c);

    static final RelRN after_bc = b.join(mjt_2, RexRN.and(
            b.joinPred(pred_bc, c),
            new RexRN.JoinField(0, b, c).pred(SqlStdOperatorTable.IS_NOT_NULL)
    ), c);

    static final RelRN after = a.join(mjt_3, RexRN.and(
            new RexRN.Pred(RuleBuilder.create().genericPredicateOp(pred_ab, true), a.joinFields(after_bc, 0, 1)),
            new RexRN.JoinField(1, a, after_bc).pred(SqlStdOperatorTable.IS_NOT_NULL)
    ), after_bc);

    static final RRule template = new RRule() {
        @Override
        public RelRN before() {
            return before;
        }

        @Override
        public RelRN after() {
            return after;
        }

        @Override
        public String name() {
            return JoinAssociate.class.getSimpleName();
        }
    };

    static Seq<RRule.RRuleGenerator.MetaAssignment> assignments() {
        var joinTypes = Seq.of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.RIGHT, JoinRelType.FULL).map(RelRN.Join.JoinType.ConcreteJoinType::new);
        return joinTypes.flatMap(jt0 -> joinTypes.flatMap(jt1 -> joinTypes.flatMap(jt2 -> joinTypes.map(jt3 -> new RRule.RRuleGenerator.MetaAssignment(Map.of(mjt_0, jt0, mjt_1, jt1, mjt_2, jt2, mjt_3, jt3))))));
    }

    @Override
    public Seq<RRule> family() {
        return new RRule.RRuleGenerator(template, assignments()).family();
    }
}
