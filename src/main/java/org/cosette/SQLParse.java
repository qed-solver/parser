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
     * Close the SchemaGenerator instance within.
     */
    public void done() throws SQLException {
        schemaGenerator.close();
    }

}