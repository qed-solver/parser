package org.qed.Backends.Calcite.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.qed.Backends.Calcite.EmptyConfig;

public class PruneZeroRowsTable extends RelRule<PruneZeroRowsTable.Config> {
	protected PruneZeroRowsTable(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_1 = call.builder();
		call.transformTo(var_1.push(call.rel(0)).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default PruneZeroRowsTable toRule() {
			return new PruneZeroRowsTable(this);
		}

		@Override
		default String description() {
			return "PruneZeroRowsTable";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_0 -> s_0.operand(RelNode.class).anyInputs();
		}

	}
}
