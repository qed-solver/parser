package org.cosette;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * AN implementation of RelShuttle interface that could convert a RelNode instance to a ObjectNode instance.
 */
public class RelJSONShuttle implements RelShuttle {

    private final Environment environment;
    private ObjectNode relNode;

    /**
     * Initialize the shuttle with a given environment.
     *
     * @param existing The given environment.
     */
    public RelJSONShuttle(Environment existing) {
        environment = existing;
        relNode = environment.createNode();
    }

    /**
     * Dump a list of RelRoot to a writer in JSON format .
     *
     * @param relNodes The given list of RelRoot.
     * @param writer   The given writer.
     */
    public static void dumpToJSON(List<RelNode> relNodes, Writer writer) throws IOException {

        ObjectMapper mapper = new ObjectMapper();

        ObjectNode mainObject = mapper.createObjectNode();

        ArrayNode schemaArray = mainObject.putArray("schemas");

        ArrayNode queryArray = mainObject.putArray("queries");

        ArrayNode helpArray = mainObject.putArray("help");

        List<RelOptTable> tableList = new ArrayList<>();

        for (RelNode relNode : relNodes) {
            Environment environment = new Environment(mapper, tableList);
            RelJSONShuttle relJsonShuttle = new RelJSONShuttle(environment);

            helpArray.add(relNode.explain());

            relNode.accept(relJsonShuttle);
            queryArray.add(relJsonShuttle.getRelNode());

            tableList = environment.getRelOptTables();
        }

        List<List<String>> tableNames = new ArrayList<>();
        for (RelOptTable table : tableList) {
            tableNames.add(table.getQualifiedName());
        }

        for (RelOptTable table : tableList) {
            ObjectNode tableObject = mapper.createObjectNode();

            ArrayNode typeArray = tableObject.putArray("types");
            for (RelDataTypeField field : table.getRowType().getFieldList()) {
                typeArray.add(field.getType().toString());
            }

            ArrayNode strategyArray = tableObject.putArray("strategy");
            for (ColumnStrategy columnStrategy : table.getColumnStrategies()) {
                strategyArray.add(columnStrategy.toString());
            }

            ArrayNode keyArray = tableObject.putArray("key");
            List<ImmutableBitSet> keys = table.getKeys();
            if (keys != null) {
                for (ImmutableBitSet key : keys) {
                    ArrayNode components = keyArray.addArray();
                    for (int unique : key) {
                        components.add(unique);
                    }
                }
            }

            ArrayNode foreignArray = tableObject.putArray("foreign");
            List<RelReferentialConstraint> constraints = table.getReferentialConstraints();
            if (constraints != null) {
                for (RelReferentialConstraint constraint : constraints) {
                    ArrayNode foreignMap = foreignArray.addArray();
                    int source = tableNames.indexOf(constraint.getSourceQualifiedName());
                    int target = tableNames.indexOf(constraint.getTargetQualifiedName());
                    ArrayNode sourceArray = foreignMap.addArray();
                    ArrayNode targetArray = foreignMap.addArray();
                    foreignMap.addArray().add(source).add(target);
                    for (IntPair intPair : constraint.getColumnPairs()) {
                        sourceArray.add(intPair.source);
                        targetArray.add(intPair.target);
                    }
                }
            }

            schemaArray.add(tableObject);
        }

        mapper.writerWithDefaultPrettyPrinter().writeValue(writer, mainObject);

    }

    /**
     * @return The ObjectNode instance corresponding to the RelNode instance.
     */
    public ObjectNode getRelNode() {
        return relNode;
    }

    /**
     * Visit a RexNode instance with the given environment and input.
     *
     * @param rex     The RexNode instance to be visited.
     * @param context The given environment.
     * @param input   The number of columns in the input, which could be viewed as additional environment.
     * @return A RexJSONVisitor instance that has visited the given RexNode.
     */
    private RexJSONVisitor visitRexNode(RexNode rex, Environment context, int input) {
        RexJSONVisitor rexJSONVisitor = new RexJSONVisitor(context, input);
        rex.accept(rexJSONVisitor);
        return rexJSONVisitor;
    }

    /**
     * Visit a RelNode instance with the given environment.
     *
     * @param child   The RelNode instance to be visited.
     * @param context The given environment.
     * @return A RelJSONShuttle that has traversed through the given RelNode instance.
     */
    private RelJSONShuttle visitChild(RelNode child, Environment context) {
        RelJSONShuttle childShuttle = new RelJSONShuttle(context);
        child.accept(childShuttle);
        return childShuttle;
    }

    /**
     * Visit a LogicalAggregation node. <br>
     * Format: {distinct: {correlate: [{project: {target: [[group, type]], source: {input}}},
     * {aggregate: {function: [functions], source: {filter: {condition: {groups}, source: {inputCopy}}}}}]}}
     *
     * @param aggregate The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(LogicalAggregate aggregate) {

        RelJSONShuttle childShuttle = visitChild(aggregate.getInput(), environment);
        int groupCount = aggregate.getGroupCount();
        int level = environment.getLevel();

        ObjectNode inputProject = environment.createNode();
        ObjectNode inputProjectArguments = environment.createNode();
        ArrayNode inputProjectTargets = inputProjectArguments.putArray("target");
        List<RelDataTypeField> inputTypes = aggregate.getInput().getRowType().getFieldList();

        ObjectNode filter = environment.createNode();
        ObjectNode filterArguments = environment.createNode();
        ObjectNode condition = environment.createNode();
        condition.put("operator", "TRUE");
        condition.putArray("operand");
        List<Integer> groups = new ArrayList<>(aggregate.getGroupSet().asList());
        for (int index = 0; index < groups.size(); index++) {
            ArrayNode target = inputProjectTargets.addArray();
            target.add(environment.createNode().put("column", level + groups.get(index)));
            target.add(inputTypes.get(groups.get(index)).getType().toString());
            ObjectNode leftColumn = environment.createNode().put("column", level + index);
            ObjectNode rightColumn = environment.createNode().put("column", level + groups.get(index) + groupCount);
            ObjectNode equivalence = environment.createNode();
            equivalence.put("operator", "=");
            equivalence.putArray("operand").add(leftColumn).add(rightColumn);
            ObjectNode and = environment.createNode();
            and.put("operator", "AND");
            and.putArray("operand").add(condition).add(equivalence);
            condition = and;
        }
        inputProjectArguments.set("source", childShuttle.getRelNode());
        inputProject.set("project", inputProjectArguments);
        RelJSONShuttle filterChildShuttle = visitChild(aggregate.getInput(), environment.amend(null, groupCount));
        filterArguments.set("condition", condition);
        filterArguments.set("source", filterChildShuttle.getRelNode());
        filter.set("filter", filterArguments);

        ObjectNode aggregation = environment.createNode();
        ObjectNode aggregationArguments = environment.createNode();
        ArrayNode aggregationFunctions = aggregationArguments.putArray("function");
        for (AggregateCall call : aggregate.getAggCallList()) {
            ObjectNode function = environment.createNode();
            function.put("operator", call.getAggregation().toString());
            ArrayNode operands = function.putArray("operand");
            for (int target : call.getArgList()) {
                operands.add(environment.createNode().put("column", level + groupCount + target));
            }
            aggregationFunctions.addArray().add(function).add(call.type.toString());
        }
        aggregationArguments.set("source", filter);
        aggregation.set("aggregate", aggregationArguments);

        relNode.putArray("correlate").add(inputProject).add(aggregation);
        distinct();

        return null;
    }

    /**
     * Wrap the current ObjectNode with "distinct" keyword.
     */
    private void distinct() {
        ObjectNode distinct = environment.createNode();
        distinct.set("distinct", relNode);
        relNode = distinct;
    }

    /**
     * A placeholder indicating that the translation rules have not been implemented yet.
     *
     * @param node The given RelNode instance.
     */
    private void notImplemented(RelNode node) {
        relNode.put("error", "Not implemented: " + node.getRelTypeName());
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        notImplemented(match);
        return null;
    }

    /**
     * Visit a TableScan node. <br>
     * Format: {scan: table}
     *
     * @param scan The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(TableScan scan) {
        relNode.put("scan", environment.identifyTable(scan.getTable()));
        return null;
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        notImplemented(scan);
        return null;
    }

    /**
     * Visit a LogicalValues node. <br>
     * Format: {value: {schema: [types], content: [[element]]}}
     *
     * @param values The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(LogicalValues values) {
        ObjectNode value = environment.createNode();
        ArrayNode schema = value.putArray("schema");
        ArrayNode content = value.putArray("content");
        for (RelDataTypeField relDataTypeField : values.getRowType().getFieldList()) {
            schema.add(relDataTypeField.getType().toString());
        }
        for (List<RexLiteral> tuple : values.getTuples()) {
            ArrayNode record = content.addArray();
            for (RexLiteral rexLiteral : tuple) {
                record.add(visitRexNode(rexLiteral, environment, 0).getRexNode());
            }
        }
        relNode.set("value", value);
        return null;
    }

    /**
     * Visit a LogicalFilter node. <br>
     * Format: {filter: {condition: {condition}, source: {input}}}
     *
     * @param filter The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(LogicalFilter filter) {
        ObjectNode arguments = environment.createNode();
        RelJSONShuttle childShuttle = visitChild(filter.getInput(), environment);
        Set<CorrelationId> variableSet = filter.getVariablesSet();
        CorrelationId correlationId = environment.delta(variableSet);
        Environment inputEnvironment = environment.amend(correlationId, 0);
        arguments.set("condition", visitRexNode(filter.getCondition(), inputEnvironment, filter.getInput().getRowType().getFieldCount()).getRexNode());
        arguments.set("source", childShuttle.getRelNode());
        relNode.set("filter", arguments);
        return null;
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        notImplemented(calc);
        return null;
    }

    /**
     * Visit a LogicalProject node. <br>
     * Format: {project: {target: [[column, type]], source: {input}}}
     *
     * @param project The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(LogicalProject project) {
        // TODO: Correlation in targets?
        ObjectNode arguments = environment.createNode();
        ArrayNode parameters = arguments.putArray("target");
        RelJSONShuttle childShuttle = visitChild(project.getInput(), environment);
        List<RexNode> projects = project.getProjects();
        List<RelDataTypeField> types = project.getRowType().getFieldList();
        for (int index = 0; index < types.size(); index++) {
            ArrayNode field = parameters.addArray();
            field.add(visitRexNode(projects.get(index), environment, types.size()).getRexNode());
            field.add(types.get(index).getType().toString());
        }
        arguments.set("source", childShuttle.getRelNode());
        relNode.set("project", arguments);
        return null;
    }

    /**
     * Visit a LogicalJoin node. <br>
     * Format: {join: {type: {type}, condition: {condition}, left: {left}, right: {right}}}
     *
     * @param join The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */

    @Override
    public RelNode visit(LogicalJoin join) {
        // TODO: Correlation in condition?
        ObjectNode arguments = environment.createNode();
        arguments.put("type", join.getJoinType().toString());
        arguments.set("condition", visitRexNode(join.getCondition(), environment, 0).getRexNode());
        arguments.set("left", visitChild(join.getLeft(), environment).getRelNode());
        arguments.set("right", visitChild(join.getRight(), environment).getRelNode());
        relNode.set("join", arguments);
        return null;
    }

    /**
     * Visit a LogicalCorrelate node. <br>
     * Format: {correlate: [{left}, {right}]}
     *
     * @param correlate The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        ArrayNode arguments = relNode.putArray("correlate");
        RelJSONShuttle leftShuttle = visitChild(correlate.getLeft(), environment);
        Environment correlateEnvironment = environment.amend(correlate.getCorrelationId(), correlate.getLeft().getRowType().getFieldCount());
        RelJSONShuttle rightShuttle = visitChild(correlate.getRight(), correlateEnvironment);
        arguments.add(leftShuttle.getRelNode());
        arguments.add(rightShuttle.getRelNode());
        return null;
    }

    /**
     * Visit a LogicalUnion node. Will wrap with "distinct" if necessary. <br>
     * Format: {union: [inputs]}
     *
     * @param union The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(LogicalUnion union) {
        ArrayNode arguments = relNode.putArray("union");
        for (RelNode input : union.getInputs()) {
            arguments.add(visitChild(input, environment).getRelNode());
        }
        if (!union.all) {
            distinct();
        }
        return null;
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        notImplemented(intersect);
        return null;
    }

    /**
     * Visit a LogicalUnion node. Will wrap with "distinct" if necessary. <br>
     * Format: {except: [inputs]}
     *
     * @param minus The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(LogicalMinus minus) {
        ArrayNode arguments = relNode.putArray("except");
        for (RelNode input : minus.getInputs()) {
            arguments.add(visitChild(input, environment).getRelNode());
        }
        if (!minus.all) {
            distinct();
        }
        return null;
    }

    /**
     * Visit a LogicalSort node. <br>
     * Format: {sort: {collation: [[column, type, order]], offset: count, limit: count, source: {input}}}
     *
     * @param sort The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(LogicalSort sort) {
        // TODO: Correlations everywhere?
        ObjectNode arguments = environment.createNode();
        RelJSONShuttle childShuttle = visitChild(sort.getInput(), environment);
        List<RelDataTypeField> types = sort.getRowType().getFieldList();
        ArrayNode collations = arguments.putArray("collation");
        for (RelFieldCollation collation : sort.collation.getFieldCollations()) {
            ArrayNode column = collations.addArray();
            int index = collation.getFieldIndex();
            column.add(index + environment.getLevel()).add(types.get(index).getType().toString()).add(collation.shortString());
        }
        if (sort.offset != null) {
            arguments.set("offset", visitRexNode(sort.offset, environment, 0).getRexNode());
        } else {
            arguments.put("offset", "null");
        }
        if (sort.fetch != null) {
            arguments.set("limit", visitRexNode(sort.fetch, environment, 0).getRexNode());
        } else {
            arguments.put("limit", "null");
        }
        arguments.set("source", childShuttle.getRelNode());
        relNode.set("sort", arguments);
        return null;
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        notImplemented(exchange);
        return null;
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        notImplemented(modify);
        return null;
    }

    @Override
    public RelNode visit(RelNode other) {
        notImplemented(other);
        return null;
    }

}
