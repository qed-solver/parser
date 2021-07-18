package org.cosette;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rex.*;

import java.util.List;
import java.util.Set;

public class RelJsonShuttle implements RelShuttle {

    private final ObjectNode relNode;
    private final Environment environment;
    private int columns;

    public RelJsonShuttle(Environment existing) {
        environment = existing;
        relNode = environment.createNode();
    }

    public ObjectNode getRelNode() {
        return relNode;
    }

    public int getColumns() { return columns; }

    private RexJSONVisitor visitRexNode(RexNode rex, Environment context, int input) {
        RexJSONVisitor rexJSONVisitor = new RexJSONVisitor(context, input);
        rex.accept(rexJSONVisitor);
        return rexJSONVisitor;
    }

    private RelJsonShuttle visitChild(RelNode child, Environment context) {
        RelJsonShuttle childShuttle = new RelJsonShuttle(context);
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

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        ObjectNode childNode = visitChild(aggregate.getInput(), environment).getRelNode();
        ArrayNode arguments = relNode.putArray("aggregate");
        ArrayNode parameters = arguments.addArray();
        // TODO: Group by?
        for (AggregateCall call : aggregate.getAggCallList()) {
            ObjectNode aggregation = environment.createNode();
            aggregation.put("type", call.getAggregation().toString());
            ArrayNode on = aggregation.putArray("on");
            for (int element: call.getArgList()) {
                on.add(element);
            }
            parameters.add(aggregation);
        }
        arguments.add(childNode);
        return null;
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        visitChild(match.getInput(), environment);
        return null;
    }

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

    @Override
    public RelNode visit(LogicalFilter filter) {
        RelJsonShuttle childNode = visitChild(filter.getInput(), environment);
        ArrayNode arguments = relNode.putArray("filter");
        Set<CorrelationId> variableSet = filter.getVariablesSet();
        CorrelationId correlationId = environment.delta(variableSet);
        RexNode rexNode = filter.getCondition();
        arguments.add(visitRexNode(rexNode, environment.amend(correlationId, 0), childNode.getColumns()).getRexNode());
        arguments.add(childNode.getRelNode());
        return null;
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        return visitChildren(calc);
    }

    @Override
    public RelNode visit(LogicalProject project) {
        RelJsonShuttle childNode = visitChild(project.getInput(), environment);
        ArrayNode arguments = relNode.putArray("project");
        ArrayNode parameters = arguments.addArray();
        List<RexNode> projects = project.getProjects();
        columns = projects.size();
        for (RexNode target : projects) {
            parameters.add(visitRexNode(target, environment, childNode.getColumns()).getRexNode());
        }
        arguments.add(childNode.getRelNode());
        return null;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        return visitChildren(join);
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        if (correlate.getJoinType() != JoinRelType.LEFT) {
            System.err.println("Join type is not LEFT.");
        }
        RelJsonShuttle leftNode = visitChild(correlate.getLeft(), environment);
        RelJsonShuttle rightNode = visitChild(correlate.getRight(), environment.amend(correlate.getCorrelationId(), leftNode.getColumns()));
        ArrayNode arguments = relNode.putArray("correlate");
        arguments.add(leftNode.getRelNode());
        arguments.add(rightNode.getRelNode());
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

class RexJSONVisitor implements RexVisitor<ObjectNode> {

    private final ObjectNode rexNode;
    private final Environment environment;
    private final int input;

    public RexJSONVisitor(Environment existing, int provided) {
        environment = existing;
        rexNode = environment.createNode();
        input = provided;
    }

    public ObjectNode getRexNode() {
        return rexNode;
    }

    private ObjectNode visitChild(RexNode rex) {
        RexJSONVisitor childVisitor = new RexJSONVisitor(environment, input);
        return rex.accept(childVisitor);
    }

    @Override
    public ObjectNode visitInputRef(RexInputRef inputRef) {
        return rexNode.put("column", inputRef.getIndex() + environment.getLevel());
    }

    @Override
    public ObjectNode visitLocalRef(RexLocalRef localRef) {
        return rexNode.put("local", localRef.getIndex() + environment.getLevel());
    }

    @Override
    public ObjectNode visitLiteral(RexLiteral literal) {
        rexNode.put("type", literal.getType().toString());
        return rexNode.put("literal", literal.toString());
    }

    @Override
    public ObjectNode visitCall(RexCall call) {
        rexNode.put("operator", call.getOperator().toString());
        ArrayNode arguments = rexNode.putArray("operands");
        for (RexNode operand: call.getOperands()) {
            arguments.add(visitChild(operand));
        }
        return rexNode;
    }

    @Override
    public ObjectNode visitOver(RexOver over) {
        return null;
    }

    @Override
    public ObjectNode visitCorrelVariable(RexCorrelVariable correlVariable) {
        rexNode.put("correlation", correlVariable.toString());
        return rexNode;
    }

    @Override
    public ObjectNode visitDynamicParam(RexDynamicParam dynamicParam) {
        return null;
    }

    @Override
    public ObjectNode visitRangeRef(RexRangeRef rangeRef) {
        return null;
    }

    @Override
    public ObjectNode visitFieldAccess(RexFieldAccess fieldAccess) {
        rexNode.put("column", fieldAccess.getField().getIndex() + environment.findLevel(((RexCorrelVariable) fieldAccess.getReferenceExpr()).id));
        return rexNode;
    }

    @Override
    public ObjectNode visitSubQuery(RexSubQuery subQuery) {
        rexNode.put("operator", subQuery.getOperator().toString());
        ArrayNode arguments = rexNode.putArray("operands");
        RelJsonShuttle relJsonShuttle = new RelJsonShuttle(environment.amend(null, input));
        subQuery.rel.accept(relJsonShuttle);
        arguments.add(relJsonShuttle.getRelNode());
        return rexNode;
    }

    @Override
    public ObjectNode visitTableInputRef(RexTableInputRef fieldRef) {
        return null;
    }

    @Override
    public ObjectNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        return null;
    }
}