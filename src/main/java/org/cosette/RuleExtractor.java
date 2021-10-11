package org.cosette;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.io.output.StringBuilderWriter;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RuleExtractor {

    public static void main(String[] args) throws IOException {
        HepProgram hepProgram = HepProgram.builder().addRuleInstance(CoreRules.FILTER_MERGE).build();
        HepPlanner hepPlanner = new HepPlanner(hepProgram);
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(rootSchema)
                .traitDefs((List<RelTraitDef>) null)
                .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2)).build();
        RelConstructor relBuilder = RelConstructor.create(config);
        RelNode original = relBuilder.var().filter(new RexVariable()).filter(new RexVariable()).build();
        System.out.println(original.explain());
        hepPlanner.setRoot(original);
        RelNode optimized = hepPlanner.findBestExp();
        List<RelNode> pair = new ArrayList<>(Arrays.asList(original, optimized));
        StringBuilderWriter display = new StringBuilderWriter();
        RelJSONShuttle.dumpToJSON(pair, display);
        System.out.println(display);
    }

}

class RelConstructor extends RelBuilder {
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
