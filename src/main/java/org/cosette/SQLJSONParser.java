package org.cosette;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
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
     * @param path The given file.
     */
    public void dumpOuput(String path) throws IOException {
        RelJSONShuttle.dumpToJSON(relNodes, new File(path + ".json"));
        RelRacketShuttle.dumpToRacket(relNodes, Paths.get(path + ".rkt"));
    }

}
