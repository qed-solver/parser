package org.cosette;

import kala.collection.Seq;
import kala.collection.Set;
import kala.collection.immutable.ImmutableMap;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
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

    public RexNode constructGeneric(String name, Seq<String> dependencies, RelType returnType) {
        SqlReturnTypeInference returnTypeInference = opBinding -> {
            final RelDataTypeFactory factory = opBinding.getTypeFactory();
            return factory.createTypeWithNullability(returnType, returnType.isNullable());
        };
        SqlOperator genericFunction = new SqlFunction(name, SqlKind.OTHER_FUNCTION, returnTypeInference, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
        return getRexBuilder().makeCall(genericFunction, dependencies.map(this::field).asJava());
    }

    public static void main(String[] args) throws IOException {
        CosetteTable leftTable = new CosetteTable("leftTable",
                ImmutableMap.of("leftColumn", new RelType.VarType("PRIMARY_TYPE", false),
                        "leftRest", new RelType.VarType("LEFT_REST_TYPE", true)),
                Set.of(), Set.empty());

        CosetteTable rightTable = new CosetteTable("rightTable",
                ImmutableMap.of("rightKey", new RelType.VarType("PRIMARY_TYPE", false),
                        "rightRest", new RelType.VarType("RIGHT_REST_TYPE", true)),
                Set.of(Set.of("rightKey")), Set.empty());

        RuleBuilder ruleMaker = RuleBuilder.create().addTable(leftTable).addTable(rightTable);
        RexBuilder rexBuilder = ruleMaker.getRexBuilder();

        ruleMaker.scan("leftTable");
        ruleMaker.scan("rightTable");

        ruleMaker.join(JoinRelType.LEFT, rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                ruleMaker.field(2, 0, "leftColumn"),
                ruleMaker.field(2, 1, "rightKey")
        ));

        ruleMaker.project(Seq.of(ruleMaker.constructGeneric("GenericFunction", Seq.of("leftColumn", "leftRest"),
                new RelType.VarType("RESULT_TYPE", true))), Seq.of("renamed"));

        RelNode node = ruleMaker.build();

        System.out.println(node.explain());

        RelJSONShuttle.dumpToJSON(List.of(node), new File("var.json"));
    }

}