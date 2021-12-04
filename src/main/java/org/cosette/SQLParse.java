package org.cosette;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.ValidationException;

import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A SQLParse instance can parse DDL statements and valid DML statements into JSON format.
 */
public class SQLParse {

    private final SchemaGenerator schemaGenerator;
    private final List<RelRoot> rootList;
    private List<SqlNode> sqlNodeList;
    private String ddlString;

    /**
     * Create a new instance by setting up the SchemaGenerator instance and the list of RelRoot within.
     */
    public SQLParse() throws SQLException {
        schemaGenerator = new SchemaGenerator();
        rootList = new ArrayList<>();
        // There's no constructor to instantiate a SQLNode.
        sqlNodeList = new ArrayList<>();
        ddlString = "";
    }

    /**
     * Apply a DDL statement to generate schema.
     *
     * @param ddl The DDL statement to be applied.
     */
    public void applyDDL(String ddl) throws SQLException, SqlParseException {
        schemaGenerator.applyDDL(ddl);
        ddlString = ddlString + ddl + "<END_TOKEN>";
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
        sqlNodeList.add(sqlNode);
    }

    /**
     * Dump the parsed statements to a writer.
     *
     * @param writer The given writer.
     */
    public void dumpToJSON(Writer writer) throws IOException {
        ArrayList<RelNode> nodeList = new ArrayList<>();
        for (RelRoot root : rootList) {
            nodeList.add(root.project());
        }
        RelJSONShuttle.dumpToJSON(nodeList, writer);
    }

    /**
     * Dump the parsed statements to a writer.
     *
     * @param writer The given writer.
     */
    public void dumpToRacket(Writer writer) throws IOException {
        SQLRacketShuttle.dumpToRacket(sqlNodeList, ddlString, writer);
    }

    /**
     * Close the SchemaGenerator instance within.
     */
    public void done() throws SQLException {
        schemaGenerator.close();
    }

}