package org.cosette;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Set;

/**
 * A implementation of RelShuttle interface that could convert a RelNode instance to a ObjectNode instance.
 */
public class RelJSONShuttle implements RelShuttle {

    private final ObjectNode relNode;
    private final Environment environment;
    private int columns;

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
        columns = childShuttle.getColumns();
        return childShuttle;
    }

    private RelNode visitChildren(RelNode rel) {
        for (RelNode child : rel.getInputs()) {
            visitChild(child, environment);
        }
        return null;
    }

    /**
     * Visit a LogicalAggregation node.
     *
     * @param aggregate The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        ObjectNode childShuttle = visitChild(aggregate.getInput(), environment).getRelNode();
        ArrayNode arguments = relNode.putArray("aggregate");
        ArrayNode parameters = arguments.addArray();
        // TODO: Group by?
        for (AggregateCall call : aggregate.getAggCallList()) {
            ObjectNode aggregation = environment.createNode();
            aggregation.put("type", call.getAggregation().toString());
            ArrayNode on = aggregation.putArray("on");
            for (int element : call.getArgList()) {
                on.add(element);
            }
            parameters.add(aggregation);
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
     * Visit a TableScan node.
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
        return visitChildren(scan);
    }

    @Override
    public RelNode visit(LogicalValues values) {
        return values;
    }

    /**
     * Visit a LogicalFilter node.
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
        return visitChildren(calc);
    }

    /**
     * Visit a LogicalProject node.
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

    @Override
    public RelNode visit(LogicalJoin join) {
        return visitChildren(join);
    }

    /**
     * Visit a LogicalCorrelate node.
     *
     * @param correlate The given RelNode instance.
     * @return Null, a placeholder required by interface.
     */
    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        if (correlate.getJoinType() != JoinRelType.LEFT) {
            System.err.println("Join type is not LEFT.");
        }
        RelJSONShuttle leftShuttle = visitChild(correlate.getLeft(), environment);
        RelJSONShuttle rightShuttle = visitChild(correlate.getRight(), environment.amend(correlate.getCorrelationId(), leftShuttle.getColumns()));
        ArrayNode arguments = relNode.putArray("correlate");
        arguments.add(leftShuttle.getRelNode());
        arguments.add(rightShuttle.getRelNode());
        return null;
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        return visitChildren(union);
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        return visitChildren(intersect);
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        return visitChildren(minus);
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        return visitChildren(sort);
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        return visitChildren(exchange);
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        return visitChildren(modify);
    }

    @Override
    public RelNode visit(RelNode other) {
        return visitChildren(other);
    }

}