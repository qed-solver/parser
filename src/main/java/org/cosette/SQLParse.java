package org.cosette;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class SQLParse {

    private final SchemaGenerator schemaGenerator;
    private final List<RelRoot> rootList;

    public SQLParse() throws SQLException {
        schemaGenerator = new SchemaGenerator();
        rootList = new ArrayList<>();
    }

    public void applyDDL(String ddl) throws SQLException {
        schemaGenerator.applyDDL(ddl);
    }

    public void parseDML(String dml) throws SqlParseException, ValidationException {
        PlannerImpl planner = schemaGenerator.createPlanner();
        SqlNode sqlNode = planner.parse(dml);
        planner.validate(sqlNode);
        RelRoot relRoot = planner.rel(sqlNode);
        rootList.add(relRoot);
    }

    public void dumpToJSON() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        SchemaPlus schema = schemaGenerator.extractSchema();

        ObjectNode mainJSON = mapper.createObjectNode();

        ObjectNode schemaJSON = mapper.createObjectNode();
        for (String tableName: schema.getTableNames()) {
            Table table = Objects.requireNonNull(schema.getTable(tableName));
            ObjectNode tableJSON = mapper.createObjectNode();
            ObjectNode fieldJSON = mapper.createObjectNode();
            for (RelDataTypeField field: table.getRowType(new JavaTypeFactoryImpl()).getFieldList()) {
                fieldJSON.put(String.valueOf(field.getIndex()), field.getType().toString());
            }
            tableJSON.set("type", fieldJSON);
            schemaJSON.set(tableName, tableJSON);
        }
        mainJSON.set("schema", schemaJSON);

        for (RelRoot root: rootList) {
            RelNode relNode = root.project();
        }

        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mainJSON);
        System.out.println(json);
    }

    public void done() throws SQLException {
        schemaGenerator.close();
    }

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
}