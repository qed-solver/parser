package org.qed.Backends.Calcite.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.qed.Backends.Calcite.EmptyConfig;

public class JoinConditionPush extends RelRule<JoinConditionPush.Config> {
	protected JoinConditionPush(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_4 = call.builder();
		var var_5 = call.builder();
		var var_6 = org.qed.Backends.Calcite.CalciteUtilities.ConditionDecomposer.extractLeftOnlyConditions(((LogicalJoin) call.rel(0)).getCondition(), call.rel(1).getRowType().getFieldCount(), call);
		var var_7 = org.qed.Backends.Calcite.CalciteUtilities.ConditionDecomposer.extractRightOnlyConditions(((LogicalJoin) call.rel(0)).getCondition(), call.rel(1).getRowType().getFieldCount(), call.rel(1).getRowType().getFieldCount() + call.rel(2).getRowType().getFieldCount(), call);
		var var_8 = org.qed.Backends.Calcite.CalciteUtilities.ConditionDecomposer.extractJoinConditions(((LogicalJoin) call.rel(0)).getCondition(), call.rel(1).getRowType().getFieldCount(), call.rel(1).getRowType().getFieldCount() + call.rel(2).getRowType().getFieldCount(), call);
		call.transformTo(var_5.push(call.rel(1)).filter(var_6).push(call.rel(2)).filter(var_7).join(JoinRelType.INNER, var_8).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default JoinConditionPush toRule() {
			return new JoinConditionPush(this);
		}

		@Override
		default String description() {
			return "JoinConditionPush";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_2 -> s_2.operand(LogicalJoin.class).inputs(s_0 -> s_0.operand(RelNode.class).anyInputs(), s_1 -> s_1.operand(RelNode.class).anyInputs());
		}

	}
}
