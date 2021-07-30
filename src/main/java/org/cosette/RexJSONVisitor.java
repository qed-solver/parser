package org.cosette;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rex.*;

import java.util.Locale;

/**
 * A implementation of RexVisitor interface that could convert a RelNode instance to a ObjectNode instance.
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
     * Visit a RexInputRef node. <br>
     * Format: {column: level}
     *
     * @param inputRef The given RexNode instance.
     * @return The ObjectNode corresponding to the given RexNode instance.
     */
    @Override
    public ObjectNode visitInputRef(RexInputRef inputRef) {
        return rexNode.put("column", inputRef.getIndex() + environment.getLevel());
    }

    @Override
    public ObjectNode visitLocalRef(RexLocalRef localRef) {
        return null;
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
        return rexNode;
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
