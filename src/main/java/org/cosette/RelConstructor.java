package org.cosette;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RelConstructor extends RelBuilder {
    protected RelConstructor(@Nullable Context context, RelOptCluster cluster,
                             @Nullable RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
    }

    public static RelConstructor create(FrameworkConfig config) {
        return Frameworks.withPrepare(config,
                (cluster, relOptSchema, rootSchema, statement) ->
                        new RelConstructor(config.getContext(), cluster, relOptSchema));
    }

    public RelBuilder var() {
        push(new RelVariable(this.getCluster()));
        return this;
    }
}
