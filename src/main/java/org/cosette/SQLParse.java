package org.cosette;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.ValidationException;
import java.sql.*;
import java.util.*;

public class SQLParse {

    public static void main( String[] args ) throws SQLException, SqlParseException, ValidationException, JsonProcessingException {
        SQLParse sqlParse = new SQLParse();
        sqlParse.applyDDL("""
                                  CREATE TABLE EMP (
                                    EMP_ID INTEGER NOT NULL,
                                    EMP_NAME VARCHAR,
                                    DEPT_ID INTEGER
                                 )
                             """);
        sqlParse.applyDDL("""
                                CREATE TABLE DEPT (
                                      DEPT_ID INTEGER,
                                      DEPT_NAME VARCHAR NOT NULL
                                  )
                                """);

        String sql1 = """
                SELECT * FROM
                (SELECT * FROM EMP WHERE DEPT_ID = 10) AS T
                WHERE T.DEPT_ID + 5 > T.EMP_ID
                    """;

        String sql2 = """
                SELECT * FROM
                (SELECT * FROM EMP WHERE DEPT_ID = 10) AS T
                WHERE 15 > T.EMP_ID
                """;

        sqlParse.parseDML(sql1);
        sqlParse.parseDML(sql2);

        sqlParse.dumpToJSON();

        sqlParse.done();

    }

    private final SchemaGenerator schemaGenerator;
    private final List<RelRoot> rootList;
    private final Set<RelOptTable> tableSet;

    public SQLParse() throws SQLException {
        schemaGenerator = new SchemaGenerator();
        rootList = new ArrayList<>();
        tableSet = new HashSet<>();
    }

    public void applyDDL(String ddl) throws SQLException {
        schemaGenerator.applyDDL(ddl);
    }

    public void parseDML(String dml) throws SqlParseException, ValidationException {
        RawPlanner planner = schemaGenerator.createPlanner();
        SqlNode sqlNode = planner.parse(dml);
        RelRoot relRoot = planner.rel(sqlNode);
        rootList.add(relRoot);
        tableSet.addAll(RelOptUtil.findTables(relRoot.project()));
    }

    public void dumpToJSON() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();

        ObjectNode mainObject = mapper.createObjectNode();

        ArrayNode schemaArray = mainObject.putArray("schemas");
        List<RelOptTable> tableList = new ArrayList<>(tableSet);
        for (RelOptTable table: tableList) {
            ObjectNode tableObject = mapper.createObjectNode();
            ArrayNode typeArray = tableObject.putArray("types");
            for (RelDataTypeField field: table.getRowType().getFieldList()) {
                typeArray.add(field.getType().toString());
            }
            // TODO: Support referential constraints (e.g. PRIMARY, UNIQUE)

            table.getReferentialConstraints();
            schemaArray.add(tableObject);
        }

        ArrayNode queryArray = mainObject.putArray("queries");

        for (RelRoot root: rootList) {
            RelJsonShuttle relJsonShuttle = new RelJsonShuttle(mapper, tableList);
            RelNode relNode = root.project();
            System.out.println(relNode.explain());
            relNode.accept(relJsonShuttle);
            queryArray.add(relJsonShuttle.getRelNode());
        }

        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mainObject);
        System.out.println(json);

    }

    public void done() throws SQLException {
        schemaGenerator.close();
    }

}