package org.qed.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexShuttle;
import java.util.HashMap;

public class ProjectFilterTranspose extends RelRule<ProjectFilterTranspose.Config> {
	protected ProjectFilterTranspose(Config config) {
		super(config);
	}

	private static org.apache.calcite.rex.RexNode mapFilterToProjectedColumns(RelOptRuleCall call) {
		var filter = (LogicalFilter) call.rel(1);
		var project = (LogicalProject) call.rel(0);
		var rexBuilder = project.getCluster().getRexBuilder();
		
		// Create mapping from table column index to projected position
		var tableToProjectMapping = new HashMap<Integer, Integer>();
		for (int projectedPos = 0; projectedPos < project.getProjects().size(); projectedPos++) {
			var projectExpr = project.getProjects().get(projectedPos);
			if (projectExpr instanceof RexInputRef inputRef) {
				tableToProjectMapping.put(inputRef.getIndex(), projectedPos);
			}
		}
		
		// Rewrite filter condition to use projected positions
		return filter.getCondition().accept(new RexShuttle() {
			@Override
			public org.apache.calcite.rex.RexNode visitInputRef(RexInputRef inputRef) {
				Integer projectedPos = tableToProjectMapping.get(inputRef.getIndex());
				if (projectedPos != null) {
					return rexBuilder.makeInputRef(inputRef.getType(), projectedPos);
				}
				return inputRef;
			}
		});
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_3 = call.builder();
		call.transformTo(var_3.push(call.rel(2)).project(((LogicalProject) call.rel(0)).getProjects()).filter(mapFilterToProjectedColumns(call)).build());
	}

	public interface Config extends EmptyConfig {
		Config DEFAULT = new Config() {};

		@Override
		default ProjectFilterTranspose toRule() {
			return new ProjectFilterTranspose(this);
		}

		@Override
		default String description() {
			return "ProjectFilterTranspose";
		}

		@Override
		default RelRule.OperandTransform operandSupplier() {
			return s_2 -> s_2.operand(LogicalProject.class).oneInput(s_1 -> s_1.operand(LogicalFilter.class).oneInput(s_0 -> s_0.operand(RelNode.class).anyInputs()));
		}

	}
}
