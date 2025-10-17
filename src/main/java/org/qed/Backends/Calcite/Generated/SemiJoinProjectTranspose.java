package org.qed.Backends.Calcite.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.qed.Backends.Calcite.EmptyConfig;

public class SemiJoinProjectTranspose extends RelRule<SemiJoinProjectTranspose.Config> {
	protected SemiJoinProjectTranspose(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_4 = call.builder();
		call.transformTo(var_4.push(call.rel(2)).project(((LogicalProject) call.rel(0)).getProjects()).push(call.rel(3)).join(JoinRelType.SEMI, ((LogicalJoin) call.rel(1)).getCondition()).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default SemiJoinProjectTranspose toRule() {
			return new SemiJoinProjectTranspose(this);
		}

		@Override
		default String description() {
			return "SemiJoinProjectTranspose";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_3 -> s_3.operand(LogicalProject.class).oneInput(s_2 -> s_2.operand(LogicalJoin.class).inputs(s_0 -> s_0.operand(RelNode.class).anyInputs(), s_1 -> s_1.operand(RelNode.class).anyInputs()));
		}

	}
}
