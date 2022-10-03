package org.cosette;

import kala.collection.Set;
import kala.collection.immutable.ImmutableMap;
import kala.collection.immutable.ImmutableSet;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class RuleBuilder {

    private final SchemaPlus schemaPlus = Frameworks.createRootSchema(true);

    private final RelBuilder relBuilder = RelBuilder.create(
            Frameworks.newConfigBuilder()
                    .defaultSchema(schemaPlus)
                    .build()
    );

    public RuleBuilder(Set<CosetteTable> tables) {
        tables.stream().sorted().forEach(table -> schemaPlus.add(table.getName(), table));
    }

    public static void main(String[] args) throws IOException {
        CosetteTable variableTable = new CosetteTable("variableTable",
                ImmutableMap.of("id", new RelType.BaseType(SqlTypeName.INTEGER, true),
                        "rest", new RelType.VarType("VAR", true)),
                Set.of(Set.of("id")), Set.empty());
        RuleBuilder ruleMaker = new RuleBuilder(ImmutableSet.of(variableTable));
        RelJSONShuttle.dumpToJSON(List.of(ruleMaker.relBuilder.scan("variableTable").build()), new File("var.json"));
    }

}
