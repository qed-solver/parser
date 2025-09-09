package org.qed.Backends.Calcite.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.qed.Backends.Calcite.EmptyConfig;

public class AggregateJoinJoinRemove extends RelRule<AggregateJoinJoinRemove.Config> {
	protected AggregateJoinJoinRemove(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_15 = call.builder();
		var var_16 = ((LogicalAggregate) call.rel(0)).getGroupSet();
		var var_17 = var_15.push(call.rel(4)).push(call.rel(5)).join(JoinRelType.INNER, var_15.push(call.rel(4)).push(call.rel(5)).literal(true)).push(call.rel(10)).push(call.rel(11)).join(JoinRelType.INNER, var_15.push(call.rel(4)).push(call.rel(5)).join(JoinRelType.INNER, var_15.push(call.rel(4)).push(call.rel(5)).literal(true)).push(call.rel(10)).push(call.rel(11)).literal(true)).join(JoinRelType.LEFT, ((LogicalJoin) call.rel(1)).getCondition()).groupKey(var_16);
		var var_18 = ((LogicalAggregate) call.rel(0)).getAggCallList();
		call.transformTo(var_15.push(call.rel(4)).push(call.rel(5)).join(JoinRelType.INNER, var_15.push(call.rel(4)).push(call.rel(5)).literal(true)).push(call.rel(10)).push(call.rel(11)).join(JoinRelType.INNER, var_15.push(call.rel(4)).push(call.rel(5)).join(JoinRelType.INNER, var_15.push(call.rel(4)).push(call.rel(5)).literal(true)).push(call.rel(10)).push(call.rel(11)).literal(true)).join(JoinRelType.LEFT, ((LogicalJoin) call.rel(1)).getCondition()).aggregate(var_17, var_18).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default AggregateJoinJoinRemove toRule() {
			return new AggregateJoinJoinRemove(this);
		}

		@Override
		default String description() {
			return "AggregateJoinJoinRemove";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_14 -> s_14.operand(LogicalAggregate.class).oneInput(s_13 -> s_13.operand(LogicalJoin.class).inputs(s_8 -> s_8.operand(LogicalJoin.class).inputs(s_2 -> s_2.operand(LogicalJoin.class).inputs(s_0 -> s_0.operand(RelNode.class).anyInputs(), s_1 -> s_1.operand(RelNode.class).anyInputs()), s_6 -> s_6.operand(LogicalJoin.class).inputs(s_4 -> s_4.operand(RelNode.class).anyInputs(), s_5 -> s_5.operand(RelNode.class).anyInputs())), s_11 -> s_11.operand(LogicalJoin.class).inputs(s_9 -> s_9.operand(RelNode.class).anyInputs(), s_10 -> s_10.operand(RelNode.class).anyInputs())));
		}

	}
}
