package org.cosette;

import kala.collection.immutable.ImmutableSet;
import kala.collection.mutable.MutableArrayList;
import kala.collection.mutable.MutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.ImmutableBitSet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * A SQLParse instance can parse DDL statements and valid DML statements into JSON format.
 */
public class SQLJSONParser {

    private final MutableList<RelNode> relNodes;
    private final MutableList<String> dmls;

    /**
     * Create a new instance by setting up the SchemaGenerator instance and the list of RelRoot within.
     */
    public SQLJSONParser() {
        relNodes = new MutableArrayList<>();
        dmls = new MutableArrayList<>();
    }

    /**
     * Parse a DML statement with current schema.
     *
     * @param dml The DML statement to be parsed.
     */
    public void parseDML(SchemaPlus context, String dml) throws Exception {
        dmls.append(dml);
        RawPlanner planner = new RawPlanner(context);
        relNodes.append(planner.rel(planner.parse(dml)));
    }

    public void pruneWith(CosetteSchema schema) throws Exception {
        var pruner = new RelPruner();
        relNodes.forEach(pruner::scan);
        schema.tables.replaceAll((name, table) -> switch (table) {
            case CosetteTable tab -> pruner.usages().getOption(name).flatMap(e -> e.map(f -> prune(tab, f))).getOrDefault(tab);
            default -> table;
        });
        var planner = new RawPlanner(schema.plus());
        dmls.forEachIndexedChecked((i, dml) -> {
            relNodes.set(i, planner.rel(planner.parse(dml)));
        });
    }

    private static CosetteTable prune(CosetteTable table, ImmutableSet<Integer> usedFields) {
        var newTable = new CosetteTable(table.owner, table.id);
        table.columnNames.forEachIndexed((i, n) -> {
            if (usedFields.contains(i)) {
                newTable.columnNames.append(n);
            }
        });
        table.columnTypeNames.forEachIndexed((i, n) -> {
            if (usedFields.contains(i)) {
                newTable.columnTypeNames.append(n);
            }
        });
        table.columnNullabilities.forEachIndexed((i, n) -> {
            if (usedFields.contains(i)) {
                newTable.columnNullabilities.append(n);
            }
        });
        var fields = usedFields.toSeq().sorted();
        table.columnKeys.forEach(key -> {
            var keySet = ImmutableSet.from(key);
            if (keySet.removedAll(fields).isEmpty()) {
                newTable.columnKeys.add(ImmutableBitSet.of(keySet.map(fields::indexOf)));
            }
        });
        // TODO: Handle other constraints
        return newTable;
    }

    /**
     * Dump the parsed statements to a file.
     *
     * @param path The given file.
     */
    public void dumpOuput(String path) throws IOException {
        RelJSONShuttle.dumpToJSON(relNodes.asJava(), new File(path + ".json"));
        RelRacketShuttle.dumpToRacket(relNodes.asJava(), Paths.get(path + ".rkt"));
    }

}
