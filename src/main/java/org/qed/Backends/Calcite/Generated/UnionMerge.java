package org.qed.Backends.Calcite.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.qed.Backends.Calcite.EmptyConfig;

public class UnionMerge extends RelRule<UnionMerge.Config> {
	protected UnionMerge(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_5 = call.builder();
		call.transformTo(var_5.push(call.rel(2)).push(call.rel(3)).push(call.rel(4)).union(false, 3).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default UnionMerge toRule() {
			return new UnionMerge(this);
		}

		@Override
		default String description() {
			return "UnionMerge";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_4 -> s_4.operand(LogicalUnion.class).inputs(s_2 -> s_2.operand(LogicalUnion.class).inputs(s_0 -> s_0.operand(RelNode.class).anyInputs(), s_1 -> s_1.operand(RelNode.class).anyInputs()), s_3 -> s_3.operand(RelNode.class).anyInputs());
		}

	}
}
