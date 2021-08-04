package org.cosette;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.CorrelationId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * AN Environment instance keeps track of the environment when a RelJSONShuttle instance is travelling through a RelNode
 * or when a RexJSONVisitor is visiting a RexNode. It can record all RelOptTable instances it has seen, and can provide
 * all level information for correlation variables.
 */
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

    /**
     * Create a new environment with no correlation information. Default global level is 0.
     *
     * @param mapper  A ObjectMapper instance that could be used to generate JSON.
     * @param schemas A list of tables as input reference.
     */
    public Environment(ObjectMapper mapper, List<RelOptTable> schemas) {
        this(mapper, schemas, new HashMap<>(), 0);
    }

    /**
     * @return A new ObjectNode instance
     */
    public ObjectNode createNode() {
        return relMapper.createObjectNode();
    }

    /**
     * Find the index of the given table in the reference. If it is not currently in the reference, append it to the end
     * and return its index.
     *
     * @param table The RelOptTable instance to be located.
     * @return The index of the given RelOptTable instance.
     */
    public int identifyTable(RelOptTable table) {
        if (!relOptTables.contains(table)) {
            relOptTables.add(table);
        }
        return relOptTables.indexOf(table);
    }

    /**
     * @return The RelOptTable list that includes the given reference and all additional RelOptTable instances that has
     * been looked up for.
     */
    public List<RelOptTable> getRelOptTables() {
        return relOptTables;
    }

    /**
     * Record the level for a correlation variable and increment the global level in a new Environment instance based on
     * the current Environment instance. This will not change the current environment.
     *
     * @param id    The correlation variable. If this is null, nothing will be recorded.
     * @param delta The change in global level.
     * @return A new Environment instance after the change.
     */
    public Environment amend(CorrelationId id, int delta) {
        HashMap<CorrelationId, Integer> copy = new HashMap<>(environment);
        if (id != null) {
            copy.put(id, level);
        }
        return new Environment(relMapper, relOptTables, copy, level + delta);
    }

    /**
     * Find the new correlation variable in the given correlation variable set.
     *
     * @param variableSet The input variable set for comparison.
     * @return The new correlation variable. If there is zero or more than one such variables, nothing will be returned.
     */
    public CorrelationId delta(Set<CorrelationId> variableSet) {
        HashSet<CorrelationId> copy = new HashSet<>(variableSet);
        copy.removeAll(environment.keySet());
        if (copy.size() == 1) {
            return copy.iterator().next();
        }
        return null;
    }


    /**
     * Find the level for a correlation variable.
     *
     * @param id The correlation variable.
     * @return The level of the correlation variable. If the given correlation variable is not recorded, the global
     * level will be returned.
     */
    public int findLevel(CorrelationId id) {
        if (environment.containsKey(id)) {
            return environment.get(id);
        }
        return level;
    }

    /**
     * @return The global level.
     */
    public int getLevel() {
        return level;
    }

}
