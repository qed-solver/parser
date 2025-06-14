package org.qed.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;

public class JoinCommute extends RelRule<JoinCommute.Config> {
	protected JoinCommute(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_3 = call.builder();
		var var_4 = (LogicalJoin) call.rel(0);
		var var_5 = (org.apache.calcite.rex.RexCall) var_4.getCondition();
		var var_6 = ((org.apache.calcite.rex.RexInputRef) var_5.getOperands().get(0)).getIndex();
		var var_7 = (LogicalJoin) call.rel(0);
		var var_8 = (org.apache.calcite.rex.RexCall) var_7.getCondition();
		var var_9 = ((org.apache.calcite.rex.RexInputRef) var_8.getOperands().get(1)).getIndex();
		var var_10 = call.rel(1).getRowType().getFieldCount();
		var var_11 = var_9 - var_10;
		var var_12 = call.rel(1);
		var var_13 = call.rel(2);
		var var_14 = var_12.getRowType().getFieldCount();
		var var_15 = var_13.getRowType().getFieldCount();
		var var_16 = java.util.stream.IntStream.concat(java.util.stream.IntStream.range(var_15, var_15 + var_14), java.util.stream.IntStream.range(0, var_15)).boxed().collect(java.util.stream.Collectors.toList());
		var var_17 = var_3.push(call.rel(2)).push(call.rel(1)).join(JoinRelType.INNER, var_3.push(call.rel(2)).push(call.rel(1)).call(((org.apache.calcite.rex.RexCall) ((LogicalJoin) call.rel(0)).getCondition()).getOperator(), var_3.push(call.rel(2)).push(call.rel(1)).field(2, 1, var_6), var_3.push(call.rel(2)).push(call.rel(1)).field(2, 0, var_11))).fields(var_16);
		call.transformTo(var_3.push(call.rel(2)).push(call.rel(1)).join(JoinRelType.INNER, var_3.push(call.rel(2)).push(call.rel(1)).call(((org.apache.calcite.rex.RexCall) ((LogicalJoin) call.rel(0)).getCondition()).getOperator(), var_3.push(call.rel(2)).push(call.rel(1)).field(2, 1, var_6), var_3.push(call.rel(2)).push(call.rel(1)).field(2, 0, var_11))).project(var_17).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default JoinCommute toRule() {
			return new JoinCommute(this);
		}

		@Override
		default String description() {
			return "JoinCommute";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_2 -> s_2.operand(LogicalJoin.class).inputs(s_0 -> s_0.operand(RelNode.class).anyInputs(), s_1 -> s_1.operand(RelNode.class).anyInputs());
		}

	}
}
