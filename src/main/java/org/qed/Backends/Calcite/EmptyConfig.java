package org.qed.Backends.Calcite;

import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface EmptyConfig extends RelRule.Config {
    @Override
    default RelRule.Config withRelBuilderFactory(RelBuilderFactory factory) {
        return this;
    }

    @Override
    default @Nullable String description() {
        return "Unspecified Config Description";
    }

    @Override
    default RelRule.Config withDescription(@Nullable String description) {
        return this;
    }

    @Override
    default RelRule.OperandTransform operandSupplier() {
        return s -> s.operand(RelNode.class).anyInputs();
    }

    @Override
    default RelRule.Config withOperandSupplier(RelRule.OperandTransform transform) {
        return this;
    }
}
