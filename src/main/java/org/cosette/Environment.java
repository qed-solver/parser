package org.cosette;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rex.RexNode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Environment {

    private final List<RelOptTable> relOptTables;
    private final ObjectMapper relMapper;
    private final HashMap<CorrelationId, Integer> environment;
    private final int level;

    private Environment(ObjectMapper mapper, List<RelOptTable> schemas, HashMap<CorrelationId, Integer> existing, int base) {
        relMapper = mapper;
        relOptTables = schemas;
        environment = existing;
        level = base;
    }

    public Environment(ObjectMapper mapper, List<RelOptTable> schemas) {
        this(mapper, schemas, new HashMap<>(), 0);
    }

    public ObjectNode createNode() {
        return relMapper.createObjectNode();
    }

    public int identifyTable(RelOptTable table) {
        if (!relOptTables.contains(table)) {
            relOptTables.add(table);
        }
        return relOptTables.indexOf(table);
    }

    public List<RelOptTable> getRelOptTables() {
        return relOptTables;
    }

    public Environment amend(CorrelationId id, int delta) {
        HashMap<CorrelationId, Integer> copy = new HashMap<>(environment);
        if (id != null) {
            copy.put(id, level);
        }
        return new Environment(relMapper, relOptTables, copy, level + delta);
    }

    public CorrelationId delta(Set<CorrelationId> variableSet) {
        HashSet<CorrelationId> copy = new HashSet<>(variableSet);
       copy.removeAll(environment.keySet());
       if (copy.size() == 1) {
           return copy.iterator().next();
       }
       return null;
    }

    public int findLevel(CorrelationId id) {
        if (environment.containsKey(id)) {
            return environment.get(id);
        }
        return 0;
    }

    public int getLevel() {
        return level;
    }

}
