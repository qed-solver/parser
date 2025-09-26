package org.qed.Backends.Calcite.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.qed.Backends.Calcite.EmptyConfig;

public class FilterMerge extends RelRule<FilterMerge.Config> {
	protected FilterMerge(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_3 = call.builder();
		call.transformTo(var_3.push(call.rel(2)).filter(var_3.push(call.rel(2)).and(((LogicalFilter) call.rel(1)).getCondition(), ((LogicalFilter) call.rel(0)).getCondition())).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default FilterMerge toRule() {
			return new FilterMerge(this);
		}

		@Override
		default String description() {
			return "FilterMerge";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_2 -> s_2.operand(LogicalFilter.class).oneInput(s_1 -> s_1.operand(LogicalFilter.class).oneInput(s_0 -> s_0.operand(RelNode.class).anyInputs()));
		}

	}
}
