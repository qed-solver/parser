package org.cosette;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelJsonWriter;
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

    public List<RelRoot> dump() {
        return rootList;
    }

    public void done() throws SQLException {
        schemaGenerator.close();
    }

    public static void main( String[] args ) throws SQLException, SqlParseException, ValidationException {
        SQLParse parse = new SQLParse();
        parse.applyDDL("""
                                  CREATE TABLE EMP (
                                    EMP_ID INTEGER,
                                    EMP_NAME VARCHAR,
                                    DEPT_ID INTEGER
                                 )
                             """);
        parse.applyDDL("""
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

        parse.parseDML(sql1);
        parse.parseDML(sql2);

        for (RelRoot root: parse.dump()) {
            RelNode relNode = root.project();
            System.out.println(relNode.explain());
        }

    }
}

class RelJsonCustomizedWriter implements RelWriter {

    @Override
    public void explain(RelNode relNode, List<Pair<String, @Nullable Object>> list) {

    }

    @Override
    public SqlExplainLevel getDetailLevel() {
        return SqlExplainLevel.EXPPLAN_ATTRIBUTES;
    }

    @Override
    public RelWriter item(String s, @Nullable Object o) {
        return null;
    }

    @Override
    public RelWriter done(RelNode relNode) {
        return null;
    }
}