package org.qed.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;

public class JoinCommute extends RelRule<JoinCommute.Config> {
	protected JoinCommute(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_3 = call.builder();
		call.transformTo(var_3.push(call.rel(2)).push(call.rel(1)).join(JoinRelType.INNER, ((LogicalJoin) call.rel(0)).getCondition()).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default JoinCommute toRule() {
			return new JoinCommute(this);
		}

		@Override
		default String description() {
			return "JoinCommute";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_2 -> s_2.operand(LogicalJoin.class).inputs(s_0 -> s_0.operand(RelNode.class).anyInputs(), s_1 -> s_1.operand(RelNode.class).anyInputs());
		}

	}
}
