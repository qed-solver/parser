package org.qed.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;

public class PruneEmptyProject extends RelRule<PruneEmptyProject.Config> {
	protected PruneEmptyProject(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_2 = call.builder();
		call.transformTo(var_2.empty().build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default PruneEmptyProject toRule() {
			return new PruneEmptyProject(this);
		}

		@Override
		default String description() {
			return "PruneEmptyProject";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_1 -> s_1.operand(LogicalProject.class).oneInput(s_0 -> s_0.operand(LogicalValues.class).noInputs());
		}

	}
}
