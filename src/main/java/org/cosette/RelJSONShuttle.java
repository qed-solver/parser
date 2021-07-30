package org.cosette;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * A implementation of RelShuttle interface that could convert a RelNode instance to a ObjectNode instance.
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
        condition.put("type", "BOOLEAN");
        condition.put("literal", "true");
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
            aggregationFunctions.add(function);
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
                record.add(rexLiteral.toString().toUpperCase(Locale.ROOT));
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