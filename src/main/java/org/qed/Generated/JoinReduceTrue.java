package org.qed.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;

public class JoinReduceTrue extends RelRule<JoinReduceTrue.Config> {
	protected JoinReduceTrue(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_5 = call.builder();
		call.transformTo(var_5.push(call.rel(1)).push(call.rel(2)).join(JoinRelType.LEFT, ((LogicalJoin) call.rel(0)).getCondition()).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default JoinReduceTrue toRule() {
			return new JoinReduceTrue(this);
		}

		@Override
		default String description() {
			return "JoinReduceTrue";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_2 -> s_2.operand(LogicalJoin.class).inputs(s_0 -> s_0.operand(RelNode.class).anyInputs(), s_1 -> s_1.operand(RelNode.class).anyInputs());
		}

	}
}
