package org.cosette;

import kala.collection.Seq;
import kala.collection.Set;
import kala.collection.immutable.ImmutableMap;
import kala.collection.immutable.ImmutableSeq;
import kala.collection.immutable.ImmutableSet;
import kala.control.Result;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

/**
 * A matching environment should contain all matching information between the pattern node and the target node
 *
 * @param fieldReference   the output column mapping between the pattern node and the target node
 * @param typeConstraints  the mapping between existing variable types and product types
 * @param synthConstraints the sequence of constraints for SyGuS solver ordered by point of introduction
 */
public record MatchEnv(
        FieldReference fieldReference,
        ImmutableMap<RelType.VarType, ImmutableSet<ProductType>> typeConstraints,
        ImmutableSeq<SynthesisConstraint> synthConstraints
) {

    /**
     * Update the field reference
     *
     * @param mapping the given field reference
     * @return a new matching environment with updated field reference
     */
    public MatchEnv updateFieldReference(Seq<Set<Integer>> mapping) {
        return new MatchEnv(new FieldReference(mapping.map(ImmutableSet::from)), typeConstraints, synthConstraints);
    }

    /**
     * Add a constraint given a pattern expression and the sequence of expressioons to be matched
     *
     * @param pattern the pattern expression
     * @param targets the nodes to be matched
     * @return the updated matching environment if successful
     */
    public Result<MatchEnv, String> assertConstraint(RexNode pattern, Seq<RexNode> targets) {
        return rexTypeInfer(pattern, targets).map(env -> env.updateSynthConstraint(pattern, targets.toImmutableSeq()));
    }

    /**
     * Verify if the constraints can be satisfied
     *
     * @return self if verification is successful
     */
    public Result<MatchEnv, String> verify() {
        return typeCheck().flatMap(typeDerivation -> {
            return Result.err("Have not implemented verification.");
        });
    }

    /**
     * Infer the constraints on variable types given pattern and targets
     *
     * @param pattern the pattern expression
     * @param targets the nodes to be matched
     * @return the inferred matching environment if successful
     */
    private Result<MatchEnv, String> rexTypeInfer(RexNode pattern, Seq<RexNode> targets) {
        return switch (pattern) {
            case RexCall call when call.getOperator() instanceof RuleBuilder.CosetteFunction operator ->
                    switch (operator.getReturnType()) {
                        case RelType.VarType varType ->
                                Result.ok(updateTypeConstraint(varType, Seq.from(targets).map(RexNode::getType)));
                        case RelType.BaseType baseType when targets.map(RexNode::getType)
                                .allMatch(target -> target.getSqlTypeName() == baseType.getSqlTypeName() && target.isNullable() == baseType.isNullable()) ->
                                Result.ok(this);
                        default ->
                                Result.err(String.format("Type %s in pattern cannot be matched with targets", operator.getReturnType()));
                    };
            case RexCall call when Seq.from(targets).allMatch(target -> target instanceof RexCall node && node.getOperator() == call.getOperator() && node.getOperands().size() == call.getOperands().size()) ->
                    Seq.from(call.getOperands()).foldLeftIndexed(Result.<MatchEnv, String>ok(this), (i, res, p) ->
                            res.flatMap(env -> env.rexTypeInfer(p, targets.map(c -> ((RexCall) c).getOperands().get(i)))));
            default -> Result.err(String.format("%s is not supported in pattern", pattern.getClass().getName()));
        };
    }

    /**
     * Update the type constraints
     *
     * @param variable the spotted variable type
     * @param unfold   the corresponding product type
     * @return a new matching environment containing this type constraint
     */
    private MatchEnv updateTypeConstraint(RelType.VarType variable, Seq<RelDataType> unfold) {
        return new MatchEnv(fieldReference, typeConstraints.putted(variable, typeConstraints.getOrDefault(variable, ImmutableSet.empty()).added(new ProductType(unfold.toImmutableSeq()))), synthConstraints);
    }

    /**
     * Update the synth constraints
     *
     * @param pattern the pattern expression
     * @param targets the nodes to be matched
     * @return a new matching environment containing this synth constraint
     */
    private MatchEnv updateSynthConstraint(RexNode pattern, ImmutableSeq<RexNode> targets) {
        return new MatchEnv(fieldReference, typeConstraints, synthConstraints.appended(new SynthesisConstraint(pattern, targets, fieldReference)));
    }

    /**
     * Type check the given constraints
     * @return the result mapping if the type check is successful
     */
    private Result<ImmutableMap<RelType.VarType, ProductType>, String> typeCheck() {
        // TODO: Improve type checking capabilities
        var proceed = true;
        var derivation = ImmutableMap.<RelType.VarType, ProductType>empty();
        while (proceed) {
            proceed = false;
            for (var vt: typeConstraints.keysView()) {
                var opts = typeConstraints.get(vt);
                for (var c: opts) {
                    var pt = typeExpand(c, derivation);
                    if (!pt.elements.anyMatch(t -> t instanceof RelType.VarType)) {
                        if (!derivation.containsKey(vt)) {
                            derivation = derivation.putted(vt, pt);
                            proceed = true;
                        }
                    }
                }
            }
        }
        if (!derivation.keysView().containsAll(typeConstraints.keysView())) {
            return Result.err("Cannot type check with insufficient type constraints");
        }
        return Result.ok(derivation);
    }

    /**
     * Expand product type using information in the derivation
     * @param product the given product type
     * @param derivation the information about variable types
     * @return the expanded product type
     */
    private ProductType typeExpand(ProductType product, ImmutableMap<RelType.VarType, ProductType> derivation) {
        return new ProductType(product.elements.foldLeft(ImmutableSeq.empty(), (p, e) -> switch (e) {
            case RelType.VarType v when derivation.containsKey(v) -> p.appendedAll(derivation.get(v).elements);
            default -> p.appended(e);
        }));
    }

    /**
     * Translate the constraints to CVC5 SyGuS description
     * @param typeDerivation
     * @return
     */
    private boolean translate(ImmutableMap<RelType.VarType, ProductType> typeDerivation) {
        return false;
    }


    public record FieldReference(ImmutableSeq<ImmutableSet<Integer>> correspondence) {
    }

    public record ProductType(ImmutableSeq<RelDataType> elements) {
    }

    public record SynthesisConstraint(RexNode pattern, ImmutableSeq<RexNode> target, FieldReference reference) {
    }

}
