package org.cosette;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rex.*;

import java.util.List;

public class RelJsonShuttle implements RelShuttle {

    private final List<RelOptTable> relOptTables;
    private final ObjectMapper relMapper;
    private final ObjectNode relNode;

    public RelJsonShuttle(ObjectMapper mapper, List<RelOptTable> environment) {
        relMapper = mapper;
        relNode = relMapper.createObjectNode();
        relOptTables = environment;
    }

    public ObjectNode getRelNode() {
        return relNode;
    }

    private ObjectNode visitChild(RelNode child) {
        RelJsonShuttle childShuttle = new RelJsonShuttle(relMapper, relOptTables);
        child.accept(childShuttle);
        return childShuttle.getRelNode();
    }

    private RelNode visitChildren(RelNode rel) {
        for (RelNode child : rel.getInputs()) {
            visitChild(child);
        }
        return null;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        visitChild(aggregate.getInput());
        return null;
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        visitChild(match.getInput());
        return null;
    }

    @Override
    public RelNode visit(TableScan scan) {
        relNode.put("scan", relOptTables.indexOf(scan.getTable()));
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
        ObjectNode childNode = visitChild(filter.getInput());
        ArrayNode arguments = relNode.putArray("filter");
        RexNode rexNode = filter.getCondition();

        arguments.add(rexNode.toString());
        arguments.add(childNode);
        return null;
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        return visitChildren(calc);
    }

    @Override
    public RelNode visit(LogicalProject project) {
        ObjectNode childNode = visitChild(project.getInput());
        ArrayNode arguments = relNode.putArray("project");
        ArrayNode targets = arguments.addArray();
        for (RexNode target : project.getProjects()) {
            targets.add(target.toString());
        }
        arguments.add(childNode);
        return null;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        return visitChildren(join);
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        return visitChildren(correlate);
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

    @Override
    public ObjectNode visitInputRef(RexInputRef inputRef) {
        return null;
    }

    @Override
    public ObjectNode visitLocalRef(RexLocalRef localRef) {
        return null;
    }

    @Override
    public ObjectNode visitLiteral(RexLiteral literal) {
        return null;
    }

    @Override
    public ObjectNode visitCall(RexCall call) {
        return null;
    }

    @Override
    public ObjectNode visitOver(RexOver over) {
        return null;
    }

    @Override
    public ObjectNode visitCorrelVariable(RexCorrelVariable correlVariable) {
        return null;
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
        return null;
    }

    @Override
    public ObjectNode visitSubQuery(RexSubQuery subQuery) {
        return null;
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