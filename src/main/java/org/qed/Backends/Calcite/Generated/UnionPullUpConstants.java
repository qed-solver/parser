package org.qed.Backends.Calcite.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.qed.Backends.Calcite.EmptyConfig;

public class UnionPullUpConstants extends RelRule<UnionPullUpConstants.Config> {
	protected UnionPullUpConstants(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_3 = call.builder();
		call.transformTo(org.qed.Backends.Calcite.HelperFunctions.unionPullUpConstants(call).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default UnionPullUpConstants toRule() {
			return new UnionPullUpConstants(this);
		}

		@Override
		default String description() {
			return "UnionPullUpConstants";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_2 -> s_2.operand(LogicalUnion.class).predicate(union -> union.getRowType().getFieldCount() > 1).anyInputs();
		}

	}
}
