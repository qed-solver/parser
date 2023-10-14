package org.cosette;

import com.fasterxml.jackson.databind.ObjectMapper;
import kala.collection.mutable.MutableArrayList;
import kala.collection.mutable.MutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

/**
 * A SQLParse instance can parse DDL statements and valid DML statements into JSON format.
 */
public class SQLJSONParser {

    private final MutableList<RelNode> relNodes;

    /**
     * Create a new instance with no RelNodes.
     */
    public SQLJSONParser() {
        relNodes = new MutableArrayList<>();
    }

    /**
     * Create a new instance with the list of RelNodes within.
     */
    public SQLJSONParser(List<RelNode> nodes) {
        relNodes = MutableArrayList.from(nodes);
    }

    /**
     * Parse a DML statement with current schema.
     *
     * @param dml The DML statement to be parsed.
     */
    public void parseDML(SchemaPlus context, String dml) throws Exception {
        RawPlanner planner = new RawPlanner(context);
        relNodes.append(planner.rel(planner.parse(dml)));
    }

    /**
     * Dump the parsed statements to a file.
     *
     * @param path The given file.
     */
    public void dumpOutput(RelBuilder builder, String path) throws IOException {
        var trimmer = new RelFieldTrimmer(null, builder);
        var nodes = relNodes.map(trimmer::trim);
        var scanner = new RelScanner();
        nodes.forEach(scanner::scan);
        var pruner = new RelPruner(scanner.usages().toImmutableMap());
        var rNodes = nodes.map(pruner);
        new ObjectMapper().writerWithDefaultPrettyPrinter()
                .writeValue(new File(path + ".json"), JSONSerializer.serialize(rNodes));
        RelRacketShuttle.dumpToRacket(rNodes.asJava(), Paths.get(path + ".rkt"));
    }
}
