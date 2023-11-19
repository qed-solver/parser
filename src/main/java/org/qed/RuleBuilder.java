package org.qed;

import kala.collection.Seq;
import kala.collection.Set;
import kala.tuple.Tuple;
import kala.tuple.Tuple2;
import kala.tuple.Tuple3;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Optionality;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RuleBuilder extends RelBuilder {

    private final AtomicInteger TABLE_ID_GENERATOR = new AtomicInteger();

    private final SchemaPlus root;

    protected RuleBuilder(@Nullable Context context, RelOptCluster cluster, RelOptSchema relOptSchema,
                          SchemaPlus schema) {
        super(context, cluster, relOptSchema);
        root = schema;
    }

    public static RuleBuilder create() {
        var emptySchema = Frameworks.createRootSchema(true);
        var config = Frameworks.newConfigBuilder().defaultSchema(emptySchema).build();
        return Frameworks.withPrepare(config,
                (cluster, relOptSchema, rootSchema, statement) -> new RuleBuilder(config.getContext(), cluster,
                        relOptSchema, emptySchema));
    }

    public RuleBuilder addTable(QedTable table) {
        root.add(table.getName(), table);
        return this;
    }

    /**
     * Create a qed table given the column types and whether they are unique (i.e. can be key)
     *
     * @param schema the list of column types and they are unique
     * @return the table created from the given schema
     */
    public QedTable createQedTable(Seq<Tuple2<RelType, Boolean>> schema) {
        var identifier = "Table_" + TABLE_ID_GENERATOR.getAndIncrement();
        var cols = schema.mapIndexed(
                (idx, tuple) -> Tuple.of(identifier + "_Column_" + idx, tuple.component1(), tuple.component2()));
        return new QedTable(identifier,
                cols.map(tuple -> Map.entry(tuple.component1(), tuple.component2())).toImmutableMap(),
                Set.from(cols.filter(Tuple3::component3).map(tuple -> Set.of(tuple.component1()))), Set.of());
    }

    /**
     * Create and return the names of the created simple tables after registering them to the builder
     *
     * @param typeIds the absolute value represents type id, while the sign indicates the uniqueness
     * @return the names for the created tables
     */
    public Seq<String> sourceSimpleTables(Seq<Integer> typeIds) {
        return typeIds.map(id -> {
            var identifier = "Table_" + TABLE_ID_GENERATOR.getAndIncrement();
            var colName = identifier + "_Column";
            var colType = new RelType.VarType("Type_" + (id < 0 ? -id : id), true);
            var table = new QedTable(identifier, kala.collection.Map.of(colName, colType),
                    id < 0 ? Set.of(Set.of(colName)) : Set.of(), Set.empty());
            addTable(table);
            return table.getName();
        });
    }

    public Seq<RexNode> joinFields() {
        return Seq.from(fields(2, 0)).concat(fields(2, 1));
    }


    public SqlAggFunction genericAggregateOp(String name, RelType aggregation) {
        return new QedAggregateFunction(name, aggregation);
    }

    public SqlOperator genericPredicateOp(String name, boolean nullable) {
        return new QedFunction(name, new RelType.BaseType(SqlTypeName.BOOLEAN, nullable));
    }

    public SqlOperator genericProjectionOp(String name, RelType projection) {
        return new QedFunction(name, projection);
    }

    public static class QedFunction extends SqlFunction {

        private final RelType codomain;

        public QedFunction(String name, RelType returnType) {
            super(name, SqlKind.OTHER_FUNCTION, opBinding -> {
                var factory = opBinding.getTypeFactory();
                return factory.createTypeWithNullability(returnType, returnType.isNullable());
            }, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
            codomain = returnType;
        }

        public RelType getReturnType() {
            return codomain;
        }
    }

    public static class QedAggregateFunction extends SqlAggFunction {

        public QedAggregateFunction(String name, RelType returnType) {
            super(name, null, SqlKind.OTHER_FUNCTION, opBinding -> {
                var factory = opBinding.getTypeFactory();
                return factory.createTypeWithNullability(returnType, returnType.isNullable());
            }, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION, false, false, Optionality.OPTIONAL);
        }
    }

}