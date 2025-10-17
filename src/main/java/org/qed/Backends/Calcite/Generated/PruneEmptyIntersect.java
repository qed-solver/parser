package org.qed.Backends.Calcite.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.qed.Backends.Calcite.EmptyConfig;

public class PruneEmptyIntersect extends RelRule<PruneEmptyIntersect.Config> {
	protected PruneEmptyIntersect(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_3 = call.builder();
		call.transformTo(var_3.empty().empty().intersect(false, 2).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default PruneEmptyIntersect toRule() {
			return new PruneEmptyIntersect(this);
		}

		@Override
		default String description() {
			return "PruneEmptyIntersect";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_2 -> s_2.operand(LogicalIntersect.class).inputs(s_0 -> s_0.operand(RelNode.class).anyInputs(), s_1 -> s_1.operand(LogicalValues.class).noInputs());
		}

	}
}
