package org.qed.Backends.Calcite.Generated;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.qed.Backends.Calcite.EmptyConfig;

public class ProjectFilterTranspose extends RelRule<ProjectFilterTranspose.Config> {
	protected ProjectFilterTranspose(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		var var_3 = call.builder();
		call.transformTo(var_3.push(call.rel(2)).project(((LogicalProject) call.rel(0)).getProjects()).filter(org.qed.Backends.Calcite.CalciteUtilities.mapFilterToProjectedColumns(call)).build());
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
