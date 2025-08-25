package org.qed.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;

public class FilterAggregateTranspose extends RelRule<FilterAggregateTranspose.Config> {
	protected FilterAggregateTranspose(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_3 = call.builder();
		var var_4 = ((LogicalAggregate) call.rel(1)).getGroupSet();
		var var_5 = var_3.push(call.rel(2)).filter(org.qed.HelperFunction.pushFilterPastAggregate(call)).groupKey(var_4);
		var var_6 = ((LogicalAggregate) call.rel(1)).getAggCallList();
		call.transformTo(var_3.push(call.rel(2)).filter(org.qed.HelperFunction.pushFilterPastAggregate(call)).aggregate(var_5, var_6).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default FilterAggregateTranspose toRule() {
			return new FilterAggregateTranspose(this);
		}

		@Override
		default String description() {
			return "FilterAggregateTranspose";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_2 -> s_2.operand(LogicalFilter.class).oneInput(s_1 -> s_1.operand(LogicalAggregate.class).oneInput(s_0 -> s_0.operand(RelNode.class).anyInputs()));
		}

	}
}
