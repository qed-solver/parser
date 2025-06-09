package org.qed.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;

public class SemiJoinJoinTranspose extends RelRule<SemiJoinJoinTranspose.Config> {
	protected SemiJoinJoinTranspose(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_5 = call.builder();
		call.transformTo(var_5.push(call.rel(2)).push(call.rel(4)).join(JoinRelType.SEMI, ((LogicalJoin) call.rel(0)).getCondition()).push(call.rel(3)).join(JoinRelType.INNER, ((LogicalJoin) call.rel(1)).getCondition()).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default SemiJoinJoinTranspose toRule() {
			return new SemiJoinJoinTranspose(this);
		}

		@Override
		default String description() {
			return "SemiJoinJoinTranspose";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_4 -> s_4.operand(LogicalJoin.class).inputs(s_2 -> s_2.operand(LogicalJoin.class).inputs(s_0 -> s_0.operand(RelNode.class).anyInputs(), s_1 -> s_1.operand(RelNode.class).anyInputs()), s_3 -> s_3.operand(RelNode.class).anyInputs());
		}

	}
}
