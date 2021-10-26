package org.cosette;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.*;

import java.util.Locale;

/**
 * AN implementation of RexVisitor interface that could convert a RelNode instance to a ObjectNode instance.
 */
public class RexJSONVisitor implements RexVisitor<ObjectNode> {

    private final ObjectNode rexNode;
    private final Environment environment;
    private final int input;

    /**
     * Initialize the visitor with given environment and input.
     *
     * @param context  The given environment.
     * @param provided The given input.
     */
    public RexJSONVisitor(Environment context, int provided) {
        environment = context;
        rexNode = environment.createNode();
        input = provided;
    }

    /**
     * @return The ObjectNode instance corresponding to the RexNode instance.
     */
    public ObjectNode getRexNode() {
        return rexNode;
    }

    /**
     * Visit a RexNode instance using the current environment and input.
     *
     * @param rex The given RexNode instance.
     * @return A ObjectNode instance corresponding to the given RexNode instance.
     */
    private ObjectNode visitChild(RexNode rex) {
        RexJSONVisitor childVisitor = new RexJSONVisitor(environment, input);
        return rex.accept(childVisitor);
    }

    /**
     * A placeholder indicating that the translation rules have not been implemented yet.
     *
     * @param node The given RexNode instance.
     */
    private ObjectNode notImplemented(RexNode node) {
        return rexNode.put("error", "Not implemented: " + node.getKind());
    }

    /**
     * Visit a RexVariable node. <br>
     * Format: {rexNode: id}
     * @param variable The given RexNode instance.
     * @return The ObjectNode corresponding to the given RexNode instance.
     */
    public ObjectNode visit(RexVariable variable) {
        return rexNode.put("rexNode", variable.getId()).put("type", "ANY");
    }

    /**
     * Visit a RexInputRef node. <br>
     * Format: {column: level}
     *
     * @param inputRef The given RexNode instance.
     * @return The ObjectNode corresponding to the given RexNode instance.
     */
    @Override
    public ObjectNode visitInputRef(RexInputRef inputRef) {
        rexNode.put("column", inputRef.getIndex() + environment.getLevel());
        rexNode.put("type", inputRef.getType().toString());
        return rexNode;
    }

    @Override
    public ObjectNode visitLocalRef(RexLocalRef localRef) {
        return notImplemented(localRef);
    }

    /**
     * Visit a RexLiteral node. <br>
     * Format: {operator: value, operand: []}
     *
     * @param literal The given RexNode instance.
     * @return The ObjectNode corresponding to the given RexNode instance.
     */
    @Override
    public ObjectNode visitLiteral(RexLiteral literal) {
        rexNode.put("operator", literal.toString().toUpperCase(Locale.ROOT));
        rexNode.putArray("operand");
        rexNode.put("type", literal.getType().toString());
        return rexNode;
    }

    /**
     * Visit a RexCall node. <br>
     * Format: {operator: operator, operand: [operands]}
     *
     * @param call The given RexNode instance.
     * @return The ObjectNode corresponding to the given RexNode instance.
     */
    @Override
    public ObjectNode visitCall(RexCall call) {
        rexNode.put("operator", call.getOperator().toString().toUpperCase(Locale.ROOT));
        ArrayNode arguments = rexNode.putArray("operand");
        for (RexNode operand : call.getOperands()) {
            arguments.add(visitChild(operand));
        }
        rexNode.put("type", call.getType().toString());
        return rexNode;
    }

    @Override
    public ObjectNode visitOver(RexOver over) {
        return notImplemented(over);
    }

    @Override
    public ObjectNode visitCorrelVariable(RexCorrelVariable variable) {
        return notImplemented(variable);
    }

    @Override
    public ObjectNode visitDynamicParam(RexDynamicParam dynamicParam) {
        return notImplemented(dynamicParam);
    }

    @Override
    public ObjectNode visitRangeRef(RexRangeRef rangeRef) {
        return notImplemented(rangeRef);
    }

    /**
     * Visit a FieldAccess node. <br>
     * Format: {column: level}
     *
     * @param fieldAccess The given RexNode instance.
     * @return The ObjectNode corresponding to the given RexNode instance.
     */
    @Override
    public ObjectNode visitFieldAccess(RexFieldAccess fieldAccess) {
        rexNode.put("column", fieldAccess.getField().getIndex() + environment.findLevel(((RexCorrelVariable) fieldAccess.getReferenceExpr()).id));
        rexNode.put("type", fieldAccess.getType().toString());
        return rexNode;
    }

    /**
     * Visit a RexSubQuery node. <br>
     * Format: {operator: operator, operand: {query}}
     *
     * @param subQuery The given RexNode instance.
     * @return The ObjectNode corresponding to the given RexNode instance.
     */
    @Override
    public ObjectNode visitSubQuery(RexSubQuery subQuery) {
        rexNode.put("operator", subQuery.getOperator().toString().toUpperCase(Locale.ROOT));
        ArrayNode arguments = rexNode.putArray("operand");
        RelJSONShuttle relJsonShuttle = new RelJSONShuttle(environment.amend(null, input));
        subQuery.rel.accept(relJsonShuttle);
        arguments.add(relJsonShuttle.getRelNode());
        rexNode.put("type", subQuery.getType().toString());
        return rexNode;
    }

    @Override
    public ObjectNode visitTableInputRef(RexTableInputRef fieldRef) {
        return notImplemented(fieldRef);
    }

    @Override
    public ObjectNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        return notImplemented(fieldRef);
    }
}
