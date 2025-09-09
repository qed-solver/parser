package org.qed.Backends.Calcite.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.qed.Backends.Calcite.EmptyConfig;

public class AggregateFilterTranspose extends RelRule<AggregateFilterTranspose.Config> {
	protected AggregateFilterTranspose(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_3 = call.builder();
		var var_4 = ((LogicalAggregate) call.rel(0)).getGroupSet();
		var var_5 = var_3.push(call.rel(2)).groupKey(var_4);
		var var_6 = ((LogicalAggregate) call.rel(0)).getAggCallList();
		call.transformTo(var_3.push(call.rel(2)).aggregate(var_5, var_6).filter(org.qed.Backends.Calcite.CalciteUtilities.mapFilterToAggregatedColumns(call)).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default AggregateFilterTranspose toRule() {
			return new AggregateFilterTranspose(this);
		}

		@Override
		default String description() {
			return "AggregateFilterTranspose";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_2 -> s_2.operand(LogicalAggregate.class).oneInput(s_1 -> s_1.operand(LogicalFilter.class).oneInput(s_0 -> s_0.operand(RelNode.class).anyInputs()));
		}

	}
}
