package org.qed.Backends.Calcite.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.qed.Backends.Calcite.EmptyConfig;

public class AggregateJoinRemove extends RelRule<AggregateJoinRemove.Config> {
	protected AggregateJoinRemove(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_10 = call.builder();
		var var_11 = ((LogicalAggregate) call.rel(0)).getGroupSet();
		var var_12 = var_10.push(call.rel(3)).push(call.rel(4)).join(JoinRelType.INNER, var_10.push(call.rel(3)).push(call.rel(4)).literal(true)).groupKey(var_11);
		var var_13 = ((LogicalAggregate) call.rel(0)).getAggCallList();
		call.transformTo(var_10.push(call.rel(3)).push(call.rel(4)).join(JoinRelType.INNER, var_10.push(call.rel(3)).push(call.rel(4)).literal(true)).aggregate(var_12, var_13).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default AggregateJoinRemove toRule() {
			return new AggregateJoinRemove(this);
		}

		@Override
		default String description() {
			return "AggregateJoinRemove";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_9 -> s_9.operand(LogicalAggregate.class).oneInput(s_8 -> s_8.operand(LogicalJoin.class).inputs(s_2 -> s_2.operand(LogicalJoin.class).inputs(s_0 -> s_0.operand(RelNode.class).anyInputs(), s_1 -> s_1.operand(RelNode.class).anyInputs()), s_6 -> s_6.operand(LogicalJoin.class).inputs(s_4 -> s_4.operand(RelNode.class).anyInputs(), s_5 -> s_5.operand(RelNode.class).anyInputs())));
		}

	}
}
