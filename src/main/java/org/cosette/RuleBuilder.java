package org.cosette;

import kala.collection.immutable.ImmutableMap;
import kala.collection.immutable.ImmutableSet;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import java.io.IOException;

public class RuleBuilder {

    private final SchemaPlus schemaPlus = Frameworks.createRootSchema(true);

    private final RelBuilder relBuilder = RelBuilder.create(
            Frameworks.newConfigBuilder()
                    .defaultSchema(schemaPlus)
                    .build()
    );

    public RuleBuilder(ImmutableSet<RelVariableTable> tables) {
        tables.stream().sorted().forEach(table -> schemaPlus.add(table.getName(), table));
    }

    public static void main(String[] args) throws IOException {
        RelVariableTable variableTable = new RelVariableTable("variableTable", ImmutableMap.of("id", new RelType.BaseType(SqlTypeName.INTEGER, false), "rest", new RelType.VarType("VAR", true)));
        RuleBuilder ruleMaker = new RuleBuilder(ImmutableSet.of(variableTable));
        System.out.println(ruleMaker.relBuilder.scan("variableTable").build().getRowType());
    }


}
