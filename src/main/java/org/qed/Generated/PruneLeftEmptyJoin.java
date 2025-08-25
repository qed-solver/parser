package org.qed.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;

public class PruneLeftEmptyJoin extends RelRule<PruneLeftEmptyJoin.Config> {
	protected PruneLeftEmptyJoin(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_3 = call.builder();
		call.transformTo(var_3.push(call.rel(2)).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default PruneLeftEmptyJoin toRule() {
			return new PruneLeftEmptyJoin(this);
		}

		@Override
		default String description() {
			return "PruneLeftEmptyJoin";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_2 -> s_2.operand(LogicalJoin.class).inputs(s_0 -> s_0.operand(LogicalValues.class).noInputs(), s_1 -> s_1.operand(RelNode.class).anyInputs());
		}

	}
}
