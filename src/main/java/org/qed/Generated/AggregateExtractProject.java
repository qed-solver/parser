package org.qed.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;

public class AggregateExtractProject extends RelRule<AggregateExtractProject.Config> {
	protected AggregateExtractProject(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_2 = call.builder();
		call.transformTo(org.qed.HelperFunction.extractProjectForAggregate(call).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default AggregateExtractProject toRule() {
			return new AggregateExtractProject(this);
		}

		@Override
		default String description() {
			return "AggregateExtractProject";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_1 -> s_1.operand(LogicalAggregate.class).oneInput(s_0 -> s_0.operand(RelNode.class).anyInputs());
		}

	}
}
