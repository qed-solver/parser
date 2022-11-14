package org.cosette;

import kala.collection.Seq;
import kala.collection.Set;
import kala.tuple.Tuple;
import kala.tuple.Tuple2;
import kala.tuple.Tuple3;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RuleBuilder extends RelBuilder {

    private final AtomicInteger TABLE_ID_GENERATOR = new AtomicInteger();

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

    /**
     * Create a simple table given the column types and whether they are unique (i.e. can be key)
     * @param schema the list of column types and they are unique
     * @return the table created from the given schema
     */
    public CosetteTable createSimpleTable(Seq<Tuple2<RelType, Boolean>> schema) {
        String identifier = "Table_" + TABLE_ID_GENERATOR.get();
        Seq<Tuple3<String, RelType, Boolean>> cols = schema.mapIndexed((idx, tuple) -> Tuple.of(identifier + "_Column_" + idx, tuple._1, tuple._2));
        return new CosetteTable(identifier,
                cols.map(tuple -> Map.entry(tuple._1, tuple._2)).toImmutableMap(),
                Set.from(cols.filter(tuple -> tuple._3).map(tuple -> Set.of(tuple._1))),
                Set.of());
    }

    public SqlOperator constructGenericFunction(String name, RelType returnType) {
        SqlReturnTypeInference returnTypeInference = opBinding -> {
            final RelDataTypeFactory factory = opBinding.getTypeFactory();
            return factory.createTypeWithNullability(returnType, returnType.isNullable());
        };
        return new SqlFunction(name, SqlKind.OTHER_FUNCTION, returnTypeInference, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

}