package org.cosette;

import kala.collection.Seq;
import kala.collection.mutable.MutableArrayList;
import kala.collection.mutable.MutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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
    public void dumpOuput(RelBuilder builder, String path) throws IOException {
        var trimmer = new RelFieldTrimmer(null, builder);
        var nodes = relNodes.map(trimmer::trim);
        var scanner = new RelScanner();
        nodes.forEach(scanner::scan);
        var pruner = new RelPruner(scanner.usages().toImmutableMap());
        var rNodes = nodes.map(pruner).asJava();
        File batch = new File(path + ".batch");
        if (!batch.exists() || !batch.isDirectory()) {
            batch.mkdir();
        }
        if (!rNodes.isEmpty()) {
            var origin = rNodes.get(0);
            Seq.fill(rNodes.size() - 1, i -> i + 1).forEachChecked(i -> {
                var pair = List.of(origin, rNodes.get(i));
                RelJSONShuttle.dumpToJSON(pair, Paths.get(batch.getPath(), i + ".json").toFile());
                RelRacketShuttle.dumpToRacket(pair, Paths.get(batch.getPath(), i + ".rkt"));
            });
        }
    }
}
