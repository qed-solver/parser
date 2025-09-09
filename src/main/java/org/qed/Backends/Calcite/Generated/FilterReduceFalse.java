package org.qed.Backends.Calcite.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.qed.Backends.Calcite.EmptyConfig;

public class FilterReduceFalse extends RelRule<FilterReduceFalse.Config> {
	protected FilterReduceFalse(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_3 = call.builder();
		call.transformTo(var_3.empty().build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default FilterReduceFalse toRule() {
			return new FilterReduceFalse(this);
		}

		@Override
		default String description() {
			return "FilterReduceFalse";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_1 -> s_1.operand(LogicalFilter.class).oneInput(s_0 -> s_0.operand(RelNode.class).anyInputs());
		}

	}
}
