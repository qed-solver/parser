package org.cosette;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rex.RexNode;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A implementation of RelShuttle interface that could convert a RelNode instance to a ObjectNode instance.
 */
public class RelJSONShuttle implements RelShuttle {

    private final Environment environment;
    private ObjectNode relNode;
    private int columns;

    /**
     * Initialize the shuttle with a given environment.
     *
     * @param existing The given environment.
     */
    public RelJSONShuttle(Environment existing) {
        environment = existing;
        relNode = environment.createNode();
        columns = 0;
    }

    /**
     * @return The ObjectNode instance corresponding to the RelNode instance.
     */
    public ObjectNode getRelNode() {
        return relNode;
    }

    /**
     * @return The number of columns corresponding to the RelNode instance.
     */
    public int getColumns() {
        return columns;
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
        columns += childShuttle.getColumns();
        return childShuttle;
    }

    /**
     * Visit a LogicalAggregation node. <br>
     * Format: {distinct: {correlate: [{project: [[groups], {input}]}, {aggregate: [[functions], {project: [[sources], {filter: [{groups}, {inputCopy}]}]}]}]}}
     *
     * @param aggregate The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(LogicalAggregate aggregate) {

        RelJSONShuttle childShuttle = visitChild(aggregate.getInput(), environment);
        columns = aggregate.getAggCallList().size();
        int groupCount = aggregate.getGroupCount();
        int level = environment.getLevel();

        ObjectNode inputProject = environment.createNode();
        ArrayNode inputProjectArguments = inputProject.putArray("project");
        ArrayNode inputProjectColumns = inputProjectArguments.addArray();

        ObjectNode filter = environment.createNode();
        ObjectNode condition = environment.createNode();
        condition.put("type", "BOOLEAN");
        condition.put("literal", "true");
        List<Integer> groups = new ArrayList<>(aggregate.getGroupSet().asList());
        for (int index = 0; index < groups.size(); index++) {
            inputProjectColumns.add(environment.createNode().put("column", level + groups.get(index)));
            ObjectNode leftColumn = environment.createNode().put("column", level + index);
            ObjectNode rightColumn = environment.createNode().put("column", level + groups.get(index) + groupCount);
            ObjectNode equivalence = environment.createNode();
            equivalence.put("operator", "=");
            equivalence.putArray("operands").add(leftColumn).add(rightColumn);
            ObjectNode and = environment.createNode();
            and.put("operator", "AND");
            and.putArray("operands").add(condition).add(equivalence);
            condition = and;
        }
        inputProjectArguments.add(childShuttle.getRelNode());
        RelJSONShuttle filterChildShuttle = visitChild(aggregate.getInput(), environment.amend(null, groupCount));
        filter.putArray("filter").add(condition).add(filterChildShuttle.getRelNode());

        ObjectNode aggregation = environment.createNode();
        ArrayNode aggregationArguments = aggregation.putArray("aggregation");
        ArrayNode aggregationFunctions = aggregationArguments.addArray();
        ObjectNode projectFilter = environment.createNode();
        ArrayNode projectFilterArguments = projectFilter.putArray("project");
        ArrayNode projectFilterColumns = projectFilterArguments.addArray();
        for (AggregateCall call : aggregate.getAggCallList()) {
            aggregationFunctions.add(call.getAggregation().toString());
            projectFilterColumns.add(environment.createNode().put("column", level + groupCount + call.getArgList().iterator().next()));
        }
        projectFilterArguments.add(filter);
        aggregationArguments.add(projectFilter);

        ObjectNode correlation = environment.createNode();
        correlation.putArray("correlate").add(inputProject).add(aggregation);
        relNode = correlation;
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
        columns = scan.getTable().getRowType().getFieldCount();
        relNode.put("scan", environment.identifyTable(scan.getTable()));
        return null;
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        notImplemented(scan);
        return null;
    }

    @Override
    public RelNode visit(LogicalValues values) {
        return values;
    }

    /**
     * Visit a LogicalFilter node. <br>
     * Format: {filter: [{condition}, {input}]}
     *
     * @param filter The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(LogicalFilter filter) {
        RelJSONShuttle childShuttle = visitChild(filter.getInput(), environment);
        ArrayNode arguments = relNode.putArray("filter");
        Set<CorrelationId> variableSet = filter.getVariablesSet();
        CorrelationId correlationId = environment.delta(variableSet);
        arguments.add(visitRexNode(filter.getCondition(), environment.amend(correlationId, 0), childShuttle.getColumns()).getRexNode());
        arguments.add(childShuttle.getRelNode());
        return null;
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        notImplemented(calc);
        return null;
    }

    /**
     * Visit a LogicalProject node. <br>
     * Format: {project: [[projects], {input}]}
     *
     * @param project The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(LogicalProject project) {
        RelJSONShuttle childShuttle = visitChild(project.getInput(), environment);
        ArrayNode arguments = relNode.putArray("project");
        ArrayNode parameters = arguments.addArray();
        List<RexNode> projects = project.getProjects();
        columns = projects.size();
        for (RexNode target : projects) {
            parameters.add(visitRexNode(target, environment, childShuttle.getColumns()).getRexNode());
        }
        arguments.add(childShuttle.getRelNode());
        return null;
    }

    /**
     * Visit a LogicalJoin node. <br>
     * Format: {join: [[type, {condition}], {left}, {right}]}
     *
     * @param join The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */

    @Override
    public RelNode visit(LogicalJoin join) {
        ArrayNode arguments = relNode.putArray("join");
        ArrayNode parameters = arguments.addArray();
        ObjectNode type = environment.createNode().put("type", join.getJoinType().toString());
        ObjectNode condition = visitRexNode(join.getCondition(), environment, 0).getRexNode();
        RelJSONShuttle leftShuttle = visitChild(join.getLeft(), environment);
        RelJSONShuttle rightShuttle = visitChild(join.getRight(), environment);
        parameters.add(type);
        parameters.add(condition);
        arguments.add(leftShuttle.getRelNode());
        arguments.add(rightShuttle.getRelNode());
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
        RelJSONShuttle rightShuttle = visitChild(correlate.getRight(), environment.amend(correlate.getCorrelationId(), leftShuttle.getColumns()));
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
            columns = 0;
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
            columns = 0;
            arguments.add(visitChild(input, environment).getRelNode());
        }
        if (!minus.all) {
            distinct();
        }
        return null;
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        notImplemented(sort);
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