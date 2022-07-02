package org.cosette;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A SQLParse instance can parse DDL statements and valid DML statements into JSON format.
 */
public class SQLJSONParser {

    private final List<RelNode> relNodes;

    /**
     * Create a new instance by setting up the SchemaGenerator instance and the list of RelRoot within.
     */
    public SQLJSONParser() {
        relNodes = new ArrayList<>();
    }

    /**
     * Parse a DML statement with current schema.
     *
     * @param dml The DML statement to be parsed.
     */
    public void parseDML(SchemaPlus context, String dml) throws Exception {
        RawPlanner planner = new RawPlanner(context);
        planner.parse(dml);
        relNodes.add(planner.rel());
    }

    /**
     * Dump the parsed statements to a file.
     *
     * @param file The given file.
     */
    public void dumpToJSON(File file) throws IOException {
        RelJSONShuttle.dumpToJSON(relNodes, file);
    }

}
