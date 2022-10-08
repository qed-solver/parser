package org.cosette;

import kala.collection.Set;
import kala.collection.immutable.ImmutableMap;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class RuleBuilder extends RelBuilder {

    private final SchemaPlus root;
    protected RuleBuilder(@Nullable Context context, RelOptCluster cluster, RelOptSchema relOptSchema, SchemaPlus schema) {
        super(context, cluster, relOptSchema);
        root = schema;
    }

    public static RuleBuilder create() {
        SchemaPlus emptySchema = Frameworks.createRootSchema(true);
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(emptySchema)
                .build();
        return Frameworks.withPrepare(config,
                (cluster, relOptSchema, rootSchema, statement) ->
                        new RuleBuilder(config.getContext(), cluster, relOptSchema, emptySchema));
    }

    public RuleBuilder addTable(CosetteTable table) {
        root.add(table.getName(), table);
        return this;
    }

    public static void main(String[] args) throws IOException {
        CosetteTable leftTable = new CosetteTable("leftTable",
                ImmutableMap.of("leftColumn", new RelType.VarType("PRIMARY_TYPE", false),
                        "rest", new RelType.VarType("LEFT_REST_TYPE", true)),
                Set.of(), Set.empty());

        CosetteTable rightTable = new CosetteTable("rightTable",
                ImmutableMap.of("rightKey", new RelType.VarType("PRIMARY_TYPE", false),
                        "rest", new RelType.VarType("RIGHT_REST_TYPE", true)),
                Set.of(Set.of("rightKey")), Set.empty());

        RuleBuilder ruleMaker = RuleBuilder.create().addTable(leftTable).addTable(rightTable);
        RexBuilder rexBuilder = ruleMaker.getRexBuilder();

        ruleMaker.scan("leftTable");
        ruleMaker.scan("rightTable");

        ruleMaker.join(JoinRelType.LEFT, rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                ruleMaker.field(2, 0, "leftColumn"),
                ruleMaker.field(2, 1, "rightKey")
        ));

        RelNode node = ruleMaker.build();

        System.out.println(node.explain());

        RelJSONShuttle.dumpToJSON(List.of(node), new File("var.json"));
    }

}
