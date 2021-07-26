package org.cosette;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.ValidationException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A SQLParse instance can parse DDL statements and valid DML statements into JSON format.
 */
public class SQLParse {

    private final SchemaGenerator schemaGenerator;
    private final List<RelRoot> rootList;

    /**
     * Create a new instance by setting up the SchemaGenerator instance and the list of RelRoot within.
     */
    public SQLParse() throws SQLException {
        schemaGenerator = new SchemaGenerator();
        rootList = new ArrayList<>();
    }

    /**
     * Apply a DDL statement to generate schema.
     *
     * @param ddl The DDL statement to be applied.
     */
    public void applyDDL(String ddl) throws SQLException, SqlParseException {
        schemaGenerator.applyDDL(ddl);
    }

    /**
     * Parse a DML statement with current schema.
     *
     * @param dml The DML statement to be parsed.
     */
    public void parseDML(String dml) throws SqlParseException, ValidationException {
        RawPlanner planner = schemaGenerator.createPlanner();
        SqlNode sqlNode = planner.parse(dml);
        RelRoot relRoot = planner.rel(sqlNode);
        rootList.add(relRoot);
    }

    /**
     * Dump schema and relational expressions in JSON format.
     */
    public String dumpToJSON() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();

        ObjectNode mainObject = mapper.createObjectNode();

        ArrayNode schemaArray = mainObject.putArray("schemas");

        ArrayNode queryArray = mainObject.putArray("queries");

        ArrayNode helpArray = mainObject.putArray("help");

        List<RelOptTable> tableList = new ArrayList<>();

        for (RelRoot root : rootList) {
            Environment environment = new Environment(mapper, tableList);
            RelJSONShuttle relJsonShuttle = new RelJSONShuttle(environment);
            RelNode relNode = root.project();

            helpArray.add(relNode.explain());

            relNode.accept(relJsonShuttle);
            queryArray.add(relJsonShuttle.getRelNode());

            tableList = environment.getRelOptTables();
        }

        Map<String, Map<SqlKind, List<String>>> constraints = schemaGenerator.getConstraints();

        for (RelOptTable table : tableList) {
            List<String> reference = table.getRowType().getFieldNames();
            Map<SqlKind, List<String>> constraint = constraints.get(table.getQualifiedName().iterator().next());
            ObjectNode tableObject = mapper.createObjectNode();

            ArrayNode typeArray = tableObject.putArray("types");
            for (RelDataTypeField field : table.getRowType().getFieldList()) {
                typeArray.add(field.getType().toString());
            }

            ArrayNode strategyArray = tableObject.putArray("strategy");
            for (ColumnStrategy columnStrategy : table.getColumnStrategies()) {
                strategyArray.add(columnStrategy.toString());
            }

            for (SqlKind sqlKind : constraint.keySet()) {
                ArrayNode constraintArray = tableObject.putArray(sqlKind.lowerName);
                for (String column : constraint.get(sqlKind)) {
                    constraintArray.add(reference.indexOf(column.substring(1, column.length() - 1)));
                }
            }

            schemaArray.add(tableObject);
        }

        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mainObject);

    }

    /**
     * Close the SchemaGenerator instance within.
     */
    public void done() throws SQLException {
        schemaGenerator.close();
    }

}