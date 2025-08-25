package org.qed.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;

public class FilterIntoJoin extends RelRule<FilterIntoJoin.Config> {
	protected FilterIntoJoin(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_4 = call.builder();
		call.transformTo(var_4.push(call.rel(2)).push(call.rel(3)).join(JoinRelType.INNER, var_4.push(call.rel(2)).push(call.rel(3)).and(((LogicalJoin) call.rel(1)).getCondition(), ((LogicalFilter) call.rel(0)).getCondition())).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default FilterIntoJoin toRule() {
			return new FilterIntoJoin(this);
		}

		@Override
		default String description() {
			return "FilterIntoJoin";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_3 -> s_3.operand(LogicalFilter.class).oneInput(s_2 -> s_2.operand(LogicalJoin.class).inputs(s_0 -> s_0.operand(RelNode.class).anyInputs(), s_1 -> s_1.operand(RelNode.class).anyInputs()));
		}

	}
}
