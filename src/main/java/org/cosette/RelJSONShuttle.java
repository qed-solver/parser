package org.cosette;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A implementation of RelShuttle interface that could convert a RelNode instance to a ObjectNode instance.
 */
public class RelJSONShuttle implements RelShuttle {

    private final Environment environment;
    private final ObjectNode relNode;
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

    private List<RelJSONShuttle> visitChildren(RelNode rel) {
        List<RelJSONShuttle> childrenShuttles = new ArrayList<>();
        for (RelNode child : rel.getInputs()) {
            childrenShuttles.add(visitChild(child, environment));
        }
        return childrenShuttles;
    }

    /**
     * Visit a LogicalAggregation node. <br>
     * Format: {aggregate: [[[groups], [types]], {input}]}
     *
     * @param aggregate The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        ObjectNode childShuttle = visitChild(aggregate.getInput(), environment).getRelNode();
        ArrayNode arguments = relNode.putArray("aggregate");
        ArrayNode parameters = arguments.addArray();
        ArrayNode groups = parameters.addArray();
        ArrayNode types = parameters.addArray();
        for (int group : aggregate.getGroupSet()) {
            groups.add(group);
        }
        for (AggregateCall call : aggregate.getAggCallList()) {
            ObjectNode aggregation = environment.createNode().put("type", call.getAggregation().toString());
            ArrayNode on = aggregation.putArray("on");
            for (int element : call.getArgList()) {
                on.add(element);
            }
            types.add(aggregation);
        }
        arguments.add(childShuttle);
        return null;
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        visitChild(match.getInput(), environment);
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
        visitChildren(scan);
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
        visitChildren(calc);
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
     * Visit a LogicalCorrelate node. Will convert all join types to left join. <br>
     * Format: {correlate: [{left}, {right}]}
     *
     * @param correlate The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        if (correlate.getJoinType() != JoinRelType.LEFT) {
            System.err.println("Join type is not LEFT.");
            System.exit(-1);
        }
        ArrayNode arguments = relNode.putArray("correlate");
        RelJSONShuttle leftShuttle = visitChild(correlate.getLeft(), environment);
        RelJSONShuttle rightShuttle = visitChild(correlate.getRight(), environment.amend(correlate.getCorrelationId(), leftShuttle.getColumns()));
        arguments.add(leftShuttle.getRelNode());
        arguments.add(rightShuttle.getRelNode());
        return null;
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        visitChildren(union);
        return null;
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        visitChildren(intersect);
        return null;
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        visitChildren(minus);
        return null;
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        visitChildren(sort);
        return null;
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        visitChildren(exchange);
        return null;
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        visitChildren(modify);
        return null;
    }

    @Override
    public RelNode visit(RelNode other) {
        visitChildren(other).clear();
        return null;
    }

}