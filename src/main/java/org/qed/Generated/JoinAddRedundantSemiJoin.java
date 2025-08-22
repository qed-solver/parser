package org.qed.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;

public class JoinAddRedundantSemiJoin extends RelRule<JoinAddRedundantSemiJoin.Config> {
	protected JoinAddRedundantSemiJoin(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_3 = call.builder();
		call.transformTo(var_3.push(call.rel(1)).push(call.rel(2)).join(JoinRelType.SEMI, ((LogicalJoin) call.rel(0)).getCondition()).push(call.rel(2)).join(JoinRelType.INNER, ((LogicalJoin) call.rel(0)).getCondition()).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default JoinAddRedundantSemiJoin toRule() {
			return new JoinAddRedundantSemiJoin(this);
		}

		@Override
		default String description() {
			return "JoinAddRedundantSemiJoin";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_2 -> s_2.operand(LogicalJoin.class).inputs(s_0 -> s_0.operand(RelNode.class).anyInputs(), s_1 -> s_1.operand(RelNode.class).anyInputs());
		}

	}
}
