package org.cosette;

import kala.collection.Seq;
import kala.collection.immutable.ImmutableMap;
import kala.collection.immutable.ImmutableSeq;
import kala.collection.immutable.ImmutableSet;
import kala.control.Result;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

/**
 * A matching environment should contain all matching information between the pattern node and the target node
 * >>> WARNING: Must have initial FieldReference, which should be introduced by variable table scan in pattern node <<<
 *
 * @param typeConstraints  the mapping between existing variable types and product types
 * @param synthConstraints the sequence of constraints for SyGuS solver ordered by point of introduction
 */
public record MatchEnv(
        ImmutableMap<RelType.VarType, ImmutableSet<ProductType>> typeConstraints,
        ImmutableSeq<SynthesisConstraint> synthConstraints

) {

    /**
     * Return an empty matching environment
     *
     * @return empty matching environment
     */
    public static MatchEnv empty() {
        return new MatchEnv(ImmutableMap.empty(), ImmutableSeq.empty());
    }

    /**
     * Get the output column mapping between the pattern node and the target node
     *
     * @return the column mapping
     */
    public FieldReference outputReference() {
        return synthConstraints.last().reference();
    }

    public Result<MatchEnv, String> rexTypeInfer(RexNode pattern, Seq<RexNode> targets) {
        return switch (pattern) {
            case RexCall call when call.getOperator() instanceof RuleBuilder.CosetteFunction operator ->
                    switch (operator.getReturnType()) {
                        case RelType.VarType varType ->
                                Result.ok(updateTypeConstraint(varType, Seq.from(targets).map(RexNode::getType)));
                        case RelType.BaseType baseType -> targets.map(RexNode::getType)
                                .allMatch(target -> target.getSqlTypeName() == baseType.getSqlTypeName() && target.isNullable() == baseType.isNullable()) ?
                                Result.ok(this) :
                                Result.err(String.format("Type %s in pattern cannot be matched with targets", baseType.getSqlTypeName().getName()));
                    };
            case RexCall call when Seq.from(targets).allMatch(target -> target instanceof RexCall) -> Result.err("???");
            default -> Result.err(String.format("%s is not supported in pattern", pattern.getClass().getName()));
        };
    }

    public Result<MatchEnv, String> verify() {
        return Result.err("Have not implemented verification.");
    }

    /**
     * Update the type constraint
     *
     * @param variable the spotted variable type
     * @param unfold   the corresponding product type
     * @return a new matching environment containing this type constraint
     */
    private MatchEnv updateTypeConstraint(RelType.VarType variable, Seq<RelDataType> unfold) {
        return new MatchEnv(typeConstraints.putted(variable, typeConstraints.getOrDefault(variable, ImmutableSet.empty()).added(new ProductType(unfold.toImmutableSeq()))), synthConstraints);
    }

    public record FieldReference(ImmutableSeq<ImmutableSet<Integer>> correspondence) {
    }

    public record ProductType(ImmutableSeq<RelDataType> elements) {
    }

    public record SynthesisConstraint(RexNode pattern, RexNode target, FieldReference reference) {
    }

}
