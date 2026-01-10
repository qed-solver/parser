package org.qed.Backends.Calcite.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.qed.Backends.Calcite.EmptyConfig;

public class AggregateProjectConstantToDummyJoin extends RelRule<AggregateProjectConstantToDummyJoin.Config> {
	protected AggregateProjectConstantToDummyJoin(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_3 = call.builder();
		call.transformTo(org.qed.Backends.Calcite.HelperFunctions.aggregateProjectConstantToDummyJoin(call).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default AggregateProjectConstantToDummyJoin toRule() {
			return new AggregateProjectConstantToDummyJoin(this);
		}

		@Override
		default String description() {
			return "AggregateProjectConstantToDummyJoin";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_2 -> s_2.operand(LogicalAggregate.class).oneInput(s_1 -> s_1.operand(LogicalProject.class).oneInput(s_0 -> s_0.operand(RelNode.class).anyInputs()));
		}

	}
}
