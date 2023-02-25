package org.cosette;

import kala.collection.Map;
import kala.collection.Seq;
import kala.collection.Set;
import kala.collection.immutable.ImmutableMap;
import kala.collection.immutable.ImmutableSet;
import kala.control.Option;
import kala.control.Result;
import kala.tuple.Tuple;
import kala.tuple.Tuple2;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

public record MatchEnv(
        Seq<Set<Integer>> reference,
        Map<SqlOperator, Tuple2<RelDataType, Seq<RelDataType>>> declaration,
        Set<Tuple2<RexNode, RexNode>> constraint

) {

    /**
     * Return an empty matching environment
     * @return empty matching environment
     */
    public static MatchEnv empty() {
        return new MatchEnv(Seq.empty(), Map.empty(), Set.empty());
    }

    /**
     * Update the column reference of the environment, which occurs when a pattern is successfully matched with a target
     * @param result the column references (e.g. [{1, 2}, {0}] implies column 1 in pattern is column 0 in target)
     * @return the updated matching environment
     */
    public MatchEnv elevate(Seq<Set<Integer>> result) {
        return new MatchEnv(result, declaration, constraint);
    }

    /**
     * Declare or verify a function declaration, which should occur in expression matching
     * @param operator the function identifier
     * @param result the function return type
     * @param operands the function operand types
     * @return the resulting environment, which contains the given function signature if there is no conflict
     */
    public Result<MatchEnv, String> declare(SqlOperator operator, RelDataType result, Seq<RelDataType> operands) {
        return switch (declaration.getOption(operator)) {
            case Option<Tuple2<RelDataType, Seq<RelDataType>>> registered && registered.isDefined() && (
                    result != registered.get().component1() || operands.zip(registered.get().component2()).anyMatch(pair -> pair.component1() != pair.component2())) ->
                Result.err(String.format("Function signature mismatch for %s", operator.toString()));
            default -> Result.ok(new MatchEnv(reference, ImmutableMap.from(declaration).putted(operator, Tuple.of(result, operands)), constraint));
        };
    }

    /**
     * Declare a constraint that should be satisfied
     * @param pattern the pattern node
     * @param target the target node
     * @return the updated matching environment
     */
    public MatchEnv restrict(RexNode pattern, RexNode target) {
        return new MatchEnv(reference, declaration, ImmutableSet.from(constraint).added(Tuple.of(pattern, target)));
    }

    /**
     * Join two matching environments, which is helpful in join operations
     * @param other the other environment to be joined
     * @return the merged matching environment
     */
    public Result<MatchEnv, String> join(MatchEnv other) {
        return Seq.from(other.declaration().iterator()).foldLeft(
                Result.<MatchEnv, String>ok(this), (register, decl) -> register.flatMap(env -> env.declare(decl.component1(), decl.component2().component1(), decl.component2().component2()))
        ).map(env -> new MatchEnv(reference.appendedAll(other.reference), env.declaration, constraint));
    }

    public Result<MatchEnv, String> verify() {
        return Result.err("Have not implemented verification.");
    }

}
