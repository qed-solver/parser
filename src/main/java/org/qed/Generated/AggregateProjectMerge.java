package org.qed.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;

public class AggregateProjectMerge extends RelRule<AggregateProjectMerge.Config> {
	protected AggregateProjectMerge(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_3 = call.builder();
		call.transformTo(org.qed.HelperFunction.createMergedAggregateProject(call).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default AggregateProjectMerge toRule() {
			return new AggregateProjectMerge(this);
		}

		@Override
		default String description() {
			return "AggregateProjectMerge";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_2 -> s_2.operand(LogicalAggregate.class).oneInput(s_1 -> s_1.operand(LogicalProject.class).oneInput(s_0 -> s_0.operand(RelNode.class).anyInputs()));
		}

	}
}
