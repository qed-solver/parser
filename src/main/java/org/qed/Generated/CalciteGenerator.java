package org.qed.Generated;

import kala.collection.Seq;
import kala.collection.immutable.ImmutableMap;
import kala.tuple.Tuple;
import kala.tuple.Tuple2;
import org.qed.CodeGenerator;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.Generated.CalciteGenerator.Env;

import java.util.concurrent.atomic.AtomicInteger;

public class CalciteGenerator implements CodeGenerator<CalciteGenerator.Env> {

    @Override
    public Env preMatch() {
        return Env.empty();
    }

    @Override
    public Env preTransform(Env env) {
        var buildEnv = env.declare("call.builder()");
        return buildEnv.getValue().focus(buildEnv.getKey());
    }

    @Override
    public Env postTransform(Env env) {
        return env.state("call.transformTo(" + env.current() + ".build());");
    }

    @Override
    public String translate(String name, Env onMatch, Env transform) {
        var builder = new StringBuilder("package org.qed.Generated;\n\n");
        builder.append("import org.apache.calcite.plan.RelOptRuleCall;\n");
        builder.append("import org.apache.calcite.plan.RelRule;\n");
        builder.append("import org.apache.calcite.rel.RelNode;\n");
        builder.append("import org.apache.calcite.rel.core.JoinRelType;\n");
        builder.append("import org.apache.calcite.rel.logical.*;\n\n");
        builder.append("public class " + name + " extends RelRule<" + name + ".Config> {\n");
        builder.append("\tprotected " + name + "(Config config) {\n");
        builder.append("\t\tsuper(config);\n");
        builder.append("\t}\n\n");
        builder.append("\t@Override\n\tpublic void onMatch(RelOptRuleCall call) {\n");
        transform.statements().forEach(statement -> builder.append("\t\t").append(statement).append("\n"));
        builder.append("\t}\n\n");
        builder.append("\tpublic interface Config extends EmptyConfig {\n");
        builder.append("\t\tConfig DEFAULT = new Config() {};\n\n");
        builder.append("\t\t@Override\n\t\tdefault " + name + " toRule() {\n");
        builder.append("\t\t\treturn new " + name + "(this);\n");
        builder.append("\t\t}\n\n");
        builder.append("\t\t@Override\n\t\tdefault String description() {\n");
        builder.append("\t\t\treturn \"" + name + "\";\n");
        builder.append("\t\t}\n\n");
        builder.append("\t\t@Override\n\t\tdefault RelRule.OperandTransform operandSupplier() {\n");
        builder.append("\t\t\treturn " + onMatch.skeleton() + ";\n");
        builder.append("\t\t}\n\n");
        builder.append("\t}\n");
        builder.append("}\n");
        return builder.toString();
    }

    @Override
    public Env onMatchScan(Env env, RelRN.Scan scan) {
        return env.symbol(scan.name(), env.current()).grow("operand(RelNode.class).anyInputs()");
    }

    @Override
    public Env onMatchFilter(Env env, RelRN.Filter filter) {
        var source_match = onMatch(env.next(), filter.source());
        var operator_match = source_match.grow("operand(LogicalFilter.class).oneInput(" + source_match.skeleton() + ")");
        var condition_match = operator_match.focus("((LogicalFilter) " + env.current() + ").getCondition()");
        return onMatch(condition_match, filter.cond());
    }

    @Override
    public Env onMatchProject(Env env, RelRN.Project project) {
        var source_match = onMatch(env.next(), project.source());
        var operator_match =
                source_match.grow("operand(LogicalProject.class).oneInput(" + source_match.skeleton() + ")");
        var map_match = operator_match.focus("((LogicalProject) " + env.current() + ").getProjects()");
        return onMatch(map_match, project.map());
    }

    @Override
    public Env onMatchPred(Env env, RexRN.Pred pred) {
        return env.symbol(pred.operator().getName(), env.current());
    }

    @Override
    public Env onMatchProj(Env env, RexRN.Proj proj) {
        return env.symbol(proj.operator().getName(), env.current());
    }

    @Override
    public Env onMatchJoin(Env env, RelRN.Join join) {
        var current_join = "((LogicalJoin) " + env.current() + ")";
        // STR."\{join_env.current()}.getJoinType()"
        var left_source_env = env.next();
        var left_match_env = onMatch(left_source_env, join.left());
        var right_source_env = left_match_env.next();
        var right_match_env = onMatch(right_source_env, join.right());
        var operator_match =
                right_match_env.grow("operand(LogicalJoin.class).inputs(" + left_match_env.skeleton() + ", " + right_match_env.skeleton() + ")");
        var cond_source_env = operator_match.focus(current_join + ".getCondition()");
        return onMatch(cond_source_env, join.cond());
    }

    @Override
    public Env onMatchAnd(Env env, RexRN.And and) {
        // Process each source in the And condition
        var current_env = env;
        // Use a unique symbol name for the AND condition
        String andSymbol = "and_" + env.varId.getAndIncrement();
        // Store the current expression as this AND node's symbol
        current_env = current_env.symbol(andSymbol, current_env.current());
        
        // Process each child source in the AND condition
        for (var source : and.sources()) {
            current_env = onMatch(current_env, source);
        }
        
        return current_env;
    }

    @Override
    public Env onMatchUnion(Env env, RelRN.Union union) {
        // Get the all flag from the union
        boolean all = union.all();
        
        // Process each source in the union
        var current_env = env;
        var skeletons = Seq.empty();
        
        // Process all sources in the sequence
        for (var source : union.sources()) {
            var next_env = current_env.next();
            var source_env = onMatch(next_env, source);
            skeletons = skeletons.appended(source_env.skeleton());
            current_env = source_env;
        }
        
        // Build the input skeletons string for the operand
        StringBuilder inputsBuilder = new StringBuilder();
        for (int i = 0; i < skeletons.size(); i++) {
            if (i > 0) {
                inputsBuilder.append(", ");
            }
            inputsBuilder.append(skeletons.get(i).toString());
        }
        
        // Create the union operand with the appropriate class based on the all flag
        String operatorClass = all ? "LogicalUnionAll" : "LogicalUnion";
        return current_env.grow("operand(" + operatorClass + ".class).inputs(" + inputsBuilder.toString() + ")");
    }

    @Override
    public Env onMatchIntersect(Env env, RelRN.Intersect intersect) {
        // Get the all flag from the intersect
        boolean all = intersect.all();
        
        // Process each source in the intersect
        var current_env = env;
        var skeletons = Seq.empty();
        
        // Process all sources in the sequence
        for (var source : intersect.sources()) {
            var next_env = current_env.next();
            var source_env = onMatch(next_env, source);
            skeletons = skeletons.appended(source_env.skeleton());
            current_env = source_env;
        }
        
        // Build the input skeletons string for the operand
        StringBuilder inputsBuilder = new StringBuilder();
        for (int i = 0; i < skeletons.size(); i++) {
            if (i > 0) {
                inputsBuilder.append(", ");
            }
            inputsBuilder.append(skeletons.get(i).toString());
        }
        
        // Create the intersect operand with the appropriate class based on the all flag
        String operatorClass = all ? "LogicalIntersectAll" : "LogicalIntersect";
        return current_env.grow("operand(" + operatorClass + ".class).inputs(" + inputsBuilder.toString() + ")");
    }

    @Override
    public Env onMatchMinus(Env env, RelRN.Minus minus) {
        // Get the all flag from the minus
        boolean all = minus.all();
        
        // Process each source in the minus
        var current_env = env;
        var skeletons = Seq.empty();
        
        // Process all sources in the sequence
        for (var source : minus.sources()) {
            var next_env = current_env.next();
            var source_env = onMatch(next_env, source);
            skeletons = skeletons.appended(source_env.skeleton());
            current_env = source_env;
        }
        
        // Build the input skeletons string for the operand
        StringBuilder inputsBuilder = new StringBuilder();
        for (int i = 0; i < skeletons.size(); i++) {
            if (i > 0) {
                inputsBuilder.append(", ");
            }
            inputsBuilder.append(skeletons.get(i).toString());
        }
        
        // Create the minus operand
        return current_env.grow("operand(LogicalMinus.class).inputs(" + inputsBuilder.toString() + ")");
    }

    @Override
    public Env onMatchField(Env env, RexRN.Field field) {
        // Generate a unique symbolic name for this field
        String fieldSymbol = "field_" + env.varId.getAndIncrement();
        
        // Store the field expression in the environment's symbol table
        return env.symbol(fieldSymbol, env.current());
    }

    @Override
    public Env onMatchTrue(Env env, RexRN literal) {
        // Create a unique symbol name for this true literal
        String trueSymbol = "true_" + env.varId.getAndIncrement();
        
        // Store the current expression as this true literal's symbol
        return env.symbol(trueSymbol, env.current());
    }

    @Override
    public Env onMatchFalse(Env env, RexRN literal) {
        // Create a unique symbol name for this false literal
        String falseSymbol = "false_" + env.varId.getAndIncrement();
        
        // Store the current expression as this false literal's symbol
        return env.symbol(falseSymbol, env.current());
    }

    @Override
    public Env onMatchEmpty(Env env, RelRN.Empty empty) {
        return env.grow("operand(LogicalValues.class).noInputs()");
    }


    @Override
    public Env transformScan(Env env, RelRN.Scan scan) {
        return env.focus(env.current() + ".push(" + env.symbols().get(scan.name()) + ")");
    }

//    @Override
//    public Env onMatchCustom(Env env, RexRN custom) {
//        return switch (custom) {
//            case RRule.JoinConditionPush.JoinPred joinPred -> {
//                var pred = env.expressions().first();
//                var breakdown_env = assignVariable(env, STR."customSplitFilter(\{pred})");
//                var breakdown = breakdown_env.expressions().first();
//                yield breakdown_env
//                        .symbol(joinPred.bothPred(), STR."\{breakdown}.getBoth()")
//                        .symbol(joinPred.leftPred(), STR."\{breakdown}.getLeft()")
//                        .symbol(joinPred.rightPred(), STR."\{breakdown}.getRight()");
//            }
//            default -> CodeGenerator.super.onMatchCustom(env, custom);
//        };
//    }

    @Override
    public Env transformFilter(Env env, RelRN.Filter filter) {
        var source_transform = transform(env, filter.source());
        var source_expression = source_transform.current();
        var cond_transform = transform(source_transform, filter.cond());
        return cond_transform.focus(source_expression + ".filter(" + cond_transform.current() + ")");
    }

    @Override
    public Env transformPred(Env env, RexRN.Pred pred) {
        return env.focus(env.symbols().get(pred.operator().getName()));
    }

    @Override
    public Env transformJoin(Env env, RelRN.Join join) {
        var left_source_transform = transform(env, join.left());
        var right_source_transform = transform(left_source_transform, join.right());
        var source_expression = right_source_transform.current();
        var cond_transform = transform(right_source_transform, join.cond());
        var join_type = switch (join.ty().semantics()) {
            case INNER -> "JoinRelType.INNER";
            case LEFT -> "JoinRelType.LEFT";
            case RIGHT -> "JoinRelType.RIGHT";
            case FULL -> "JoinRelType.FULL";
            case SEMI -> "JoinRelType.SEMI";
            case ANTI -> "JoinRelType.ANTI";
        };
        return cond_transform.focus(source_expression + ".join(" + join_type + ", " + cond_transform.current() + ")");
    }

    @Override
    public Env transformAnd(Env env, RexRN.And and) {
        var source_transform = env;
        var operands = Seq.empty();
        for (var source : and.sources()) {
            source_transform = transform(source_transform, source);
            operands = operands.appended(source_transform.current());
            source_transform = source_transform.focus(env.current());
        }
        return source_transform.focus(env.current() + ".and(" + operands.joinToString(", ") + ")");
    }

    @Override
    public Env transformUnion(Env env, RelRN.Union union) {
        // Get the all flag from the union
        boolean all = union.all();
        
        // The number of sources
        int sourceCount = union.sources().size();
        
        // Transform each source
        var current_env = env;
        for (var source : union.sources()) {
            current_env = transform(current_env, source);
        }
        
        // Use the union method with the all flag and source count
        // This matches the Calcite RelBuilder.union(boolean all, int n) signature
        return current_env.focus(current_env.current() + ".union(" + all + ", " + sourceCount + ")");
    }

    @Override
    public Env transformIntersect(Env env, RelRN.Intersect intersect) {
        // Get the all flag from the intersect
        boolean all = intersect.all();
        
        // The number of sources
        int sourceCount = intersect.sources().size();
        
        // Transform each source
        var current_env = env;
        for (var source : intersect.sources()) {
            current_env = transform(current_env, source);
        }
        
        // Use the intersect method with the all flag and source count
        // This matches the expected Calcite RelBuilder.intersect(boolean all, int n) signature
        String methodName = all ? "intersectAll" : "intersect";
        return current_env.focus(current_env.current() + "." + methodName + "(" + all + ", " + sourceCount + ")");
    }

    @Override
    public Env transformMinus(Env env, RelRN.Minus minus) {
        // Get the all flag from the minus
        boolean all = minus.all();
        
        // The number of sources
        int sourceCount = minus.sources().size();
        
        // Transform each source
        var current_env = env;
        for (var source : minus.sources()) {
            current_env = transform(current_env, source);
        }
        
        // Use the minus method with the all flag and source count
        // This matches the expected Calcite RelBuilder.minus(boolean all, int n) signature
        return current_env.focus(current_env.current() + ".minus(" + all + ", " + sourceCount + ")");
    }

    @Override
    public Env transformField(Env env, RexRN.Field field) {
        // In Calcite, field references are typically created with a "field" method
        // We'll need to pass some identifier for the field - use toString() if no specific field accessor is available
        
        // Assuming field has a method that returns some kind of identifier or name
        // If not, we may need to adjust this implementation
        return env.focus(env.current() + ".field(" + field + ")");
    }

    @Override
    public Env transformProj(Env env, RexRN.Proj proj) {
        // In Calcite, projections are typically created using the operator name
        // This is similar to your transformPred implementation
        
        // Look up the symbol from the matching phase
        if (!env.symbols().containsKey(proj.operator().getName())) {
            throw new RuntimeException("Operator symbol not found: " + proj.operator().getName() + 
                                    ". Make sure onMatchProj is properly implemented.");
        }
        
        // Return an environment focused on the expression for this projection
        return env.focus(env.symbols().get(proj.operator().getName()));
    }

    @Override
    public Env transformProject(Env env, RelRN.Project project) {
        // First transform the source relation
        var source_transform = transform(env, project.source());
        var source_expression = source_transform.current();
        
        // Then transform the projection map
        var map_transform = transform(source_transform, project.map());
        
        // Combine the source and projection using the project operation
        // This creates a projection on top of the source relation
        return map_transform.focus(source_expression + ".project(" + map_transform.current() + ")");
    }

    @Override
    public Env transformTrue(Env env, RexRN literal) {
        // In Calcite, true literals are typically represented using the 
        // rexBuilder.makeLiteral(true) method or just "TRUE"
        return env.focus(env.current() + ".literal(true)");
    }

    @Override
    public Env transformFalse(Env env, RexRN literal) {
        // In Calcite, false literals are represented using the 
        // rexBuilder.makeLiteral(false) method or just "FALSE"
        return env.focus(env.current() + ".literal(false)");
    }

    @Override
    public Env transformEmpty(Env env, RelRN.Empty empty) {
        // In Calcite, empty relations are created using the values() method with no tuples
        // This creates a LogicalValues node with no rows
        return env.focus(env.current() + ".empty()");
    }

    public record Env(AtomicInteger varId, int rel, String current, String skeleton, Seq<String> statements,
                      ImmutableMap<String, String> symbols) {
        public static Env empty() {
            return new Env(new AtomicInteger(), 0, "call.rel(0)", "/* Unspecified skeleton */", Seq.empty(),
                    ImmutableMap.empty());
        }

        public Env next() {
            return new Env(varId, rel + 1, "call.rel(" + (rel + 1) + ")", skeleton, statements, symbols);
        }

        public Env focus(String target) {
            return new Env(varId, rel, target, skeleton, statements, symbols);
        }

        public Env state(String statement) {
            return new Env(varId, rel, current, skeleton, statements.appended(statement), symbols);
        }

        public Env symbol(String symbol, String expression) {
            return new Env(varId, rel, current, skeleton, statements, symbols.putted(symbol, expression));
        }

        public Tuple2<String, Env> declare(String expression) {
            var name = "var_" + varId.getAndIncrement();
            return Tuple.of(name, state("var " + name + " = " + expression + ";"));
        }

        public Env grow(String requirement) {
            var vn = "s_" + varId.getAndIncrement();
            return new Env(varId, rel, current, vn + " -> " + vn + "." + requirement, statements, symbols);
        }
    }
}
