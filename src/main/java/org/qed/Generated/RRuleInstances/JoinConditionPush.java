package org.qed.Generated.RRuleInstances;

import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlOperator;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RuleBuilder;

/**
 * Implementation of JoinConditionPushRule.
 * This rule analyzes join conditions and pushes single-table conditions down as filters.
 * Based on Calcite's FilterJoinRule with no-filter configuration.
 */
public record JoinConditionPush() implements RRule {
    static final RelRN left = RelRN.scan("Left", "Left_Type");
    static final RelRN right = RelRN.scan("Right", "Right_Type");
    
    @Override
    public RelRN before() {
        SqlOperator joinOp = RuleBuilder.create().genericPredicateOp("joinCond", true);
        RexRN crossTableCond = new RexRN.Pred(joinOp, Seq.of(
            new RexRN.JoinField(0, left, right),   // Left table field 0 (absolute position 0)
            new RexRN.JoinField(1, left, right)    // Right table field 0 (absolute position 1)
        ));
        SqlOperator leftOp = RuleBuilder.create().genericPredicateOp("leftCond", true);
        RexRN leftOnlyCond = new RexRN.Pred(leftOp, Seq.of(
            new RexRN.JoinField(0, left, right)    // Left table field 0 (absolute position 0)
        ));
        SqlOperator rightOp = RuleBuilder.create().genericPredicateOp("rightCond", true);
        RexRN rightOnlyCond = new RexRN.Pred(rightOp, Seq.of(
            new RexRN.JoinField(1, left, right)    // Right table field 0 (absolute position 1)
        ));
        return left.join(JoinRelType.INNER, 
            RexRN.and(crossTableCond, leftOnlyCond, rightOnlyCond), right);
    }
    
    @Override
    public RelRN after() {
        SqlOperator joinOp = RuleBuilder.create().genericPredicateOp("joinCond", true);
        RexRN crossTableCond = new RexRN.Pred(joinOp, Seq.of(
            new RexRN.JoinField(0, left, right),   // Left table field 0 (absolute position 0)
            new RexRN.JoinField(1, left, right)    // Right table field 0 (absolute position 1)
        ));
    
        SqlOperator leftOp = RuleBuilder.create().genericPredicateOp("leftCond", true);
        RexRN leftFilterCond = new RexRN.Pred(leftOp, Seq.of(
            new RexRN.Field(0, left)  // Field 0 of left table directly
        ));
        RelRN filteredLeft = left.filter(leftFilterCond);
        
        SqlOperator rightOp = RuleBuilder.create().genericPredicateOp("rightCond", true);
        RexRN rightFilterCond = new RexRN.Pred(rightOp, Seq.of(
            new RexRN.Field(0, right)  // Field 0 of right table directly
        ));
        RelRN filteredRight = right.filter(rightFilterCond);
        
        return filteredLeft.join(JoinRelType.INNER, crossTableCond, filteredRight);
    }
}