package org.qed.Backends.Calcite;

import kala.collection.Seq;
import kala.collection.immutable.ImmutableMap;
import kala.tuple.Tuple;
import kala.tuple.Tuple2;
import org.qed.CodeGenerator;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.Backends.Calcite.CalciteGenerator.Env;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.processing.Generated;

public class CalciteGenerator implements CodeGenerator<CalciteGenerator.Env> {
    
    @Override
    public Env preMatch(String rulename) {
        return Env.empty(rulename);
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
        var builder = new StringBuilder("package org.qed.Backends.Calcite.Generated;\n\n");
        builder.append("import org.apache.calcite.plan.RelOptRuleCall;\n");
        builder.append("import org.apache.calcite.plan.RelRule;\n");
        builder.append("import org.apache.calcite.plan.RelOptUtil;\n");
        builder.append("import org.apache.calcite.rel.RelNode;\n");
        builder.append("import org.apache.calcite.rel.core.JoinRelType;\n");
        builder.append("import org.apache.calcite.rel.logical.*;\n");
        builder.append("import org.qed.Backends.Calcite.EmptyConfig;\n");
        builder.append("\n");
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
    public Env onMatchJoinWithSeparateConds(Env env, RelRN.JoinWithSeparateConds join) {
        var current_join = "((LogicalJoin) " + env.current() + ")";
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
    public Env onMatchJoin(Env env, RelRN.Join join) {
        var current_join = "((LogicalJoin) " + env.current() + ")";
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
        var current_env = env;
        String andSymbol = "and_" + env.varId.getAndIncrement();
        current_env = current_env.symbol(andSymbol, current_env.current());
        for (var source : and.sources()) {
            current_env = onMatch(current_env, source);
        }
        return current_env;
    }

    @Override
    public Env onMatchUnion(Env env, RelRN.Union union) {
        boolean all = union.all();
        var current_env = env;
        var skeletons = Seq.empty();
        for (var source : union.sources()) {
            var next_env = current_env.next();
            var source_env = onMatch(next_env, source);
            skeletons = skeletons.appended(source_env.skeleton());
            current_env = source_env;
        }
        StringBuilder inputsBuilder = new StringBuilder();
        for (int i = 0; i < skeletons.size(); i++) {
            if (i > 0) {
                inputsBuilder.append(", ");
            }
            inputsBuilder.append(skeletons.get(i).toString());
        }
        String operatorClass = all ? "LogicalUnionAll" : "LogicalUnion";
        return current_env.grow("operand(" + operatorClass + ".class).inputs(" + inputsBuilder.toString() + ")");
    }

    @Override
    public Env onMatchIntersect(Env env, RelRN.Intersect intersect) {
        boolean all = intersect.all();
        var current_env = env;
        var skeletons = Seq.empty();
        for (var source : intersect.sources()) {
            var next_env = current_env.next();
            var source_env = onMatch(next_env, source);
            skeletons = skeletons.appended(source_env.skeleton());
            current_env = source_env;
        }
        StringBuilder inputsBuilder = new StringBuilder();
        for (int i = 0; i < skeletons.size(); i++) {
            if (i > 0) {
                inputsBuilder.append(", ");
            }
            inputsBuilder.append(skeletons.get(i).toString());
        }
        String operatorClass = all ? "LogicalIntersectAll" : "LogicalIntersect";
        return current_env.grow("operand(" + operatorClass + ".class).inputs(" + inputsBuilder.toString() + ")");
    }

    @Override
    public Env onMatchMinus(Env env, RelRN.Minus minus) {
        boolean all = minus.all();
        var current_env = env;
        var skeletons = Seq.empty();
        for (var source : minus.sources()) {
            var next_env = current_env.next();
            var source_env = onMatch(next_env, source);
            skeletons = skeletons.appended(source_env.skeleton());
            current_env = source_env;
        }
        StringBuilder inputsBuilder = new StringBuilder();
        for (int i = 0; i < skeletons.size(); i++) {
            if (i > 0) {
                inputsBuilder.append(", ");
            }
            inputsBuilder.append(skeletons.get(i).toString());
        }
        return current_env.grow("operand(LogicalMinus.class).inputs(" + inputsBuilder.toString() + ")");
    }

    @Override
    public Env onMatchField(Env env, RexRN.Field field) {
        String fieldSymbol = "field_" + env.varId.getAndIncrement();
        return env.symbol(fieldSymbol, env.current());
    }

    @Override
    public Env onMatchTrue(Env env, RexRN literal) {
        String trueSymbol = "true_" + env.varId.getAndIncrement();
        return env.symbol(trueSymbol, env.current());
    }

    @Override
    public Env onMatchFalse(Env env, RexRN literal) {
        String falseSymbol = "false_" + env.varId.getAndIncrement();
        return env.symbol(falseSymbol, env.current());
    }

    @Override
    public Env onMatchEmpty(Env env, RelRN.Empty empty) {
        return env.grow("operand(LogicalValues.class).noInputs()");
    }

    @Override
    public Env onMatchAggregate(Env env, RelRN.Aggregate aggregate) {
        var sourceMatch = onMatch(env.next(), aggregate.source());
        return sourceMatch.grow("operand(LogicalAggregate.class).oneInput(" + sourceMatch.skeleton() + ")");
    }

    @Override
    public Env onMatchCustom(Env env, RelRN custom) {
        if (env.rulename.equals("AggregateProjectConstantToDummyJoin")) {
            return switch (custom) {
                case org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.SourceTable st -> env.next().grow("operand(RelNode.class).anyInputs()");
                case org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.ProjectWithConstantLiterals p -> { var sourceMatch = onMatch(env, p.input()); yield sourceMatch.grow("operand(LogicalProject.class).oneInput(" + sourceMatch.skeleton() + ")");}
                case org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.AggregateGroupingByConstants agg -> { var sourceMatch = onMatch(env, agg.input()); yield sourceMatch.grow("operand(LogicalAggregate.class).oneInput(" + sourceMatch.skeleton() + ")");}
                default -> env;
            };
        }
        if (env.rulename.equals("UnionToDistinct")) {
            return switch (custom) { 
                case org.qed.RRuleInstances.UnionToDistinct.DistinctUnion u -> { var leftMatch = onMatch(env.next(), u.left()); var rightMatch = onMatch(leftMatch.next(), u.right()); yield rightMatch.grow("operand(LogicalUnion.class)" + ".predicate(union -> !union.all)" + ".anyInputs()");}
                default -> env;
            };
        }
        if (env.rulename.equals("UnionPullUpConstants")) {
            return switch (custom) {
                case org.qed.RRuleInstances.UnionPullUpConstants.UnionWithConstantColumns u -> { var leftMatch = onMatch(env.next(), u.left()); var rightMatch = onMatch(leftMatch.next(), u.right()); yield rightMatch.grow("operand(LogicalUnion.class)" + ".predicate(union -> union.getRowType().getFieldCount() > 1)" + ".anyInputs()");}
                case org.qed.RRuleInstances.UnionPullUpConstants.LeftProjectionWithConstants left -> { var sourceMatch = onMatch(env, left.input()); yield sourceMatch.grow("operand(LogicalProject.class).oneInput(" + sourceMatch.skeleton() + ")"); }
                case org.qed.RRuleInstances.UnionPullUpConstants.RightProjectionWithConstants right -> { var sourceMatch = onMatch(env, right.input()); yield sourceMatch.grow("operand(LogicalProject.class).oneInput(" + sourceMatch.skeleton() + ")");}
                default -> env;
            };
        }
        if (env.rulename.equals("ProjectAggregateMerge")) {
            return switch (custom) {
                case org.qed.RRuleInstances.ProjectAggregateMerge.ProjectUsingSubsetOfAggregates p -> { var sourceMatch = onMatch(env, p.input()); yield sourceMatch.grow("operand(LogicalProject.class).oneInput(" + sourceMatch.skeleton() + ")");}
                case org.qed.RRuleInstances.ProjectAggregateMerge.AggregateWithMultipleCalls a -> { var sourceMatch = onMatch(env, a.input()); yield sourceMatch.grow("operand(LogicalAggregate.class).oneInput(" + sourceMatch.skeleton() + ")");}
                case org.qed.RRuleInstances.ProjectAggregateMerge.SourceTable st -> { yield env.next().grow("operand(RelNode.class).anyInputs()");}
                default -> env;
            };
        }
        return CodeGenerator.super.onMatchCustom(env, custom);
    }

    @Override
    public Env transformScan(Env env, RelRN.Scan scan) {
        return env.focus(env.current() + ".push(" + env.symbols().get(scan.name()) + ")");
    }

    @Override
    public Env transformFilter(Env env, RelRN.Filter filter) {
        var source_transform = transform(env, filter.source());
        var source_expression = source_transform.current();
        var cond_transform = transform(source_transform, filter.cond());
        return cond_transform.focus(source_expression + ".filter(" + cond_transform.current() + ")");
    }

    @Override
    public Env transformPred(Env env, RexRN.Pred pred) {
        if (env.rulename.equals("JoinCommute")) {
            var currentEnv = env; var transformedArgs = Seq.<String>empty(); var sources = pred.sources(); var reversedSources = Seq.of(sources.get(1), sources.get(0));
            for (var arg : reversedSources) {
                currentEnv = transform(currentEnv, arg); transformedArgs = transformedArgs.appended(currentEnv.current()); currentEnv = currentEnv.focus(env.current());
            }
            String argsString = transformedArgs.joinToString(", ");
            String operatorCall = "((org.apache.calcite.rex.RexCall) ((LogicalJoin) call.rel(0)).getCondition()).getOperator()";
            return currentEnv.focus(env.current() + ".call(" + operatorCall + ", " + argsString + ")");
        }
        if (env.rulename.equals("ProjectFilterTranspose")) {return env.focus("org.qed.Backends.Calcite.HelperFunctions.mapFilterToProjectedColumns(call)");} 
        if (env.rulename.equals("FilterProjectTranspose")) {return env.focus("RelOptUtil.pushFilterPastProject(((LogicalFilter) call.rel(0)).getCondition(), " + "((LogicalProject) call.rel(1)))");}
        if (env.rulename.equals("AggregateFilterTranspose")) {return env.focus("org.qed.Backends.Calcite.HelperFunctions.mapFilterToAggregatedColumns(call)");}
        if (env.rulename.equals("FilterAggregateTranspose")) {return env.focus("org.qed.Backends.Calcite.HelperFunctions.pushFilterPastAggregate(call)");}
        return env.focus(env.symbols().get(pred.operator().getName()));
    }

    @Override
    public Env transformJoinField(Env env, RexRN.JoinField joinField) {
        var origJoinDecl = env.declare("(LogicalJoin) call.rel(0)");
        var envWithOrigJoin = origJoinDecl.getValue();
        var conditionDecl = envWithOrigJoin.declare("(org.apache.calcite.rex.RexCall) " + origJoinDecl.getKey() + ".getCondition()");
        var envWithCondition = conditionDecl.getValue();
        if (joinField.ordinal() == 0) {
            var leftFieldDecl = envWithCondition.declare("((org.apache.calcite.rex.RexInputRef) " + conditionDecl.getKey() + ".getOperands().get(0)).getIndex()");
            var envWithLeftField = leftFieldDecl.getValue();
            return envWithLeftField.focus(env.current() + ".field(2, 1, " + leftFieldDecl.getKey() + ")");
        }
        else if (joinField.ordinal() == 1) {
            var rightFieldDecl = envWithCondition.declare("((org.apache.calcite.rex.RexInputRef) " + conditionDecl.getKey() + ".getOperands().get(1)).getIndex()");
            var envWithRightField = rightFieldDecl.getValue();
            var leftColCountDecl = envWithRightField.declare("call.rel(1).getRowType().getFieldCount()");
            var envWithLeftCount = leftColCountDecl.getValue();
            var adjustedRightFieldDecl = envWithLeftCount.declare(rightFieldDecl.getKey() + " - " + leftColCountDecl.getKey());
            var envWithAdjustedRightField = adjustedRightFieldDecl.getValue();
            return envWithAdjustedRightField.focus(env.current() + ".field(2, 0, " + adjustedRightFieldDecl.getKey() + ")");
        } else {
            throw new UnsupportedOperationException("Unsupported join field ordinal: " + joinField.ordinal());
        }
    }

    @Override
    public Env transformJoinWithPushedConds(Env env, RelRN.JoinWithPushedConds join) {
        var builderDecl = env.declare("call.builder()");
        var envWithBuilder = builderDecl.getValue();
        var leftCondDecl = envWithBuilder.declare("org.qed.Backends.Calcite.HelperFunctions.ConditionDecomposer.extractLeftOnlyConditions(" + "((LogicalJoin) call.rel(0)).getCondition(), " + "call.rel(1).getRowType().getFieldCount(), call)");
        var envWithLeftCond = leftCondDecl.getValue();
        var rightCondDecl = envWithLeftCond.declare("org.qed.Backends.Calcite.HelperFunctions.ConditionDecomposer.extractRightOnlyConditions(" + "((LogicalJoin) call.rel(0)).getCondition(), " + "call.rel(1).getRowType().getFieldCount(), " + "call.rel(1).getRowType().getFieldCount() + call.rel(2).getRowType().getFieldCount(), call)");
        var envWithRightCond = rightCondDecl.getValue();
        var joinCondDecl = envWithRightCond.declare("org.qed.Backends.Calcite.HelperFunctions.ConditionDecomposer.extractJoinConditions(" + "((LogicalJoin) call.rel(0)).getCondition(), " + "call.rel(1).getRowType().getFieldCount(), " + "call.rel(1).getRowType().getFieldCount() + call.rel(2).getRowType().getFieldCount(), call)");
        var envWithJoinCond = joinCondDecl.getValue();
        return envWithJoinCond.focus(builderDecl.getKey() + ".push(call.rel(1))" + ".filter(" + leftCondDecl.getKey() + ")" + ".push(call.rel(2))" + ".filter(" + rightCondDecl.getKey() + ")" + ".join(JoinRelType.INNER, " + joinCondDecl.getKey() + ")");
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
        boolean all = union.all();
        int sourceCount = union.sources().size();
        var current_env = env;
        for (var source : union.sources()) {
            current_env = transform(current_env, source);
        }
        return current_env.focus(current_env.current() + ".union(" + all + ", " + sourceCount + ")");
    }

    @Override
    public Env transformIntersect(Env env, RelRN.Intersect intersect) {
        if (env.rulename.equals("PruneEmptyIntersect")) {
            String builderVar = env.statements().get(0).split(" ")[1];
            return env.focus(builderVar + ".push(call.rel(1)).empty()" + ".push(call.rel(2))" + ".intersect(false, 2)");
        }
        boolean all = intersect.all();
        int sourceCount = intersect.sources().size();
        var current_env = env;
        for (var source : intersect.sources()) {
            current_env = transform(current_env, source);
        }
        String methodName = all ? "intersectAll" : "intersect";
        return current_env.focus(current_env.current() + "." + methodName + "(" + all + ", " + sourceCount + ")");
    }

    @Override
    public Env transformMinus(Env env, RelRN.Minus minus) {
        boolean all = minus.all();
        int sourceCount = minus.sources().size();
        var current_env = env;
        for (var source : minus.sources()) {
            current_env = transform(current_env, source);
        }
        return current_env.focus(current_env.current() + ".minus(" + all + ", " + sourceCount + ")");
    }

    @Override
    public Env transformField(Env env, RexRN.Field field) {
        return env.focus(env.current() + ".field(" + field + ")");
    }

    @Override
    public Env transformProj(Env env, RexRN.Proj proj) {
        if (!env.symbols().containsKey(proj.operator().getName())) {
            throw new RuntimeException("Operator symbol not found: " + proj.operator().getName() + ". Make sure onMatchProj is properly implemented.");
        }
        return env.focus(env.symbols().get(proj.operator().getName()));
    }

    @Override
    public Env transformProject(Env env, RelRN.Project project) {
        if (env.rulename.equals("ProjectMerge")) {return env.focus("org.qed.Backends.Calcite.HelperFunctions.mergeProjections(call)");}
        var source_transform = transform(env, project.source());
        var source_expression = source_transform.current();
        var map_transform = transform(source_transform, project.map());
        return map_transform.focus(source_expression + ".project(" + map_transform.current() + ")");
    }

    @Override
    public Env transformTrue(Env env, RexRN literal) {
        return env.focus(env.current() + ".literal(true)");
    }

    @Override
    public Env transformFalse(Env env, RexRN literal) {
        return env.focus(env.current() + ".literal(false)");
    }

    @Override
    public Env transformEmpty(Env env, RelRN.Empty empty) {
        return env.focus(env.current() + ".empty()");
    }

    @Override
    public Env transformAggregate(Env env, RelRN.Aggregate aggregate) {
        if (env.rulename.equals("AggregateProjectMerge")) {return env.focus("org.qed.Backends.Calcite.HelperFunctions.createMergedAggregateProject(call)");}
        if (env.rulename.equals("AggregateExtractProject")) {return env.focus("org.qed.Backends.Calcite.HelperFunctions.extractProjectForAggregate(call)");}
        if (env.rulename.equals("AggregateJoinRemove")) {
            var groupSetDecl = env.declare("((LogicalAggregate) call.rel(0)).getGroupSet()");
            var envWithGroupSet = groupSetDecl.getValue();
            var aggCallsDecl = envWithGroupSet.declare("((LogicalAggregate) call.rel(0)).getAggCallList()");
            var envWithAggCalls = aggCallsDecl.getValue();
            String builderVar = env.statements().get(0).split(" ")[1];
            return envWithAggCalls.focus(builderVar + ".push(call.rel(3)).push(call.rel(4))" + ".join(JoinRelType.INNER, " + builderVar + ".literal(true))" + ".aggregate(" + builderVar + ".groupKey(" + groupSetDecl.getKey() + "), " + aggCallsDecl.getKey() + ")");
        }
        if (env.rulename.equals("AggregateJoinJoinRemove")) {return env.focus("org.qed.Backends.Calcite.HelperFunctions.aggregateJoinJoinRemove(call)");}
        
        var sourceTransform = transform(env, aggregate.source());
        String builderWithSource = sourceTransform.current();
        String originalAgg;
        if (env.rulename.equals("FilterAggregateTranspose")) {originalAgg = "((LogicalAggregate) call.rel(1))";}
        else originalAgg = "((LogicalAggregate) call.rel(0))";
        var groupSetDecl = sourceTransform.declare(originalAgg + ".getGroupSet()");
        var envWithGroupSet = groupSetDecl.getValue();
        var groupKeyDecl = envWithGroupSet.declare(builderWithSource + ".groupKey(" + groupSetDecl.getKey() + ")");
        var envWithGroupKey = groupKeyDecl.getValue();
        var aggCallsDecl = envWithGroupKey.declare(originalAgg + ".getAggCallList()");
        var envWithAggCalls = aggCallsDecl.getValue();
        return envWithAggCalls.focus(builderWithSource + ".aggregate(" + groupKeyDecl.getKey() + ", " + aggCallsDecl.getKey() + ")");
    }

    @Override
    public Env transformCustom(Env env, RelRN custom) {
        if (env.rulename.equals("AggregateProjectConstantToDummyJoin")) {return env.focus("org.qed.Backends.Calcite.HelperFunctions.aggregateProjectConstantToDummyJoin(call)");}
        if (env.rulename.equals("UnionToDistinct")) {return env.focus("org.qed.Backends.Calcite.HelperFunctions.unionToDistinct(call)");}
        if (env.rulename.equals("UnionPullUpConstants")) {return env.focus("org.qed.Backends.Calcite.HelperFunctions.unionPullUpConstants(call)");}
        if (env.rulename.equals("ProjectAggregateMerge")) {return env.focus("org.qed.Backends.Calcite.HelperFunctions.projectAggregateMerge(call)");}
        return switch (custom) {
            case org.qed.RRuleInstances.JoinCommute.ProjectionRelRN projection -> {
                var sourceEnv = transform(env, projection.source());
                var leftTableDecl = sourceEnv.declare("call.rel(1)");
                var envWithLeftTable = leftTableDecl.getValue();
                var rightTableDecl = envWithLeftTable.declare("call.rel(2)");
                var envWithRightTable = rightTableDecl.getValue();
                var leftColCountDecl = envWithRightTable.declare(leftTableDecl.getKey() + ".getRowType().getFieldCount()");
                var envWithLeftCount = leftColCountDecl.getValue();
                var rightColCountDecl = envWithLeftCount.declare(rightTableDecl.getKey() + ".getRowType().getFieldCount()");
                var envWithRightCount = rightColCountDecl.getValue();
                var projectionIndicesDecl = envWithRightCount.declare("java.util.stream.IntStream.concat(" + "java.util.stream.IntStream.range(" + rightColCountDecl.getKey() + ", " + rightColCountDecl.getKey() + " + " + leftColCountDecl.getKey() + "), " + "java.util.stream.IntStream.range(0, " + rightColCountDecl.getKey() + ")" + ").boxed().collect(java.util.stream.Collectors.toList())");
                var envWithProjectionIndices = projectionIndicesDecl.getValue();
                var fieldRefsDecl = envWithProjectionIndices.declare(sourceEnv.current() + ".fields(" + projectionIndicesDecl.getKey() + ")");
                var envWithFieldRefs = fieldRefsDecl.getValue();
                yield envWithFieldRefs.focus(sourceEnv.current() + ".project(" + fieldRefsDecl.getKey() + ")");
            }
            default -> unimplementedTransform(env, custom);
        };
    }

    public record Env(AtomicInteger varId, int rel, String current, String skeleton, Seq<String> statements,
                      ImmutableMap<String, String> symbols, String rulename) {
        public static Env empty(String rulename) {
            return new Env(new AtomicInteger(), 0, "call.rel(0)", "/* Unspecified skeleton */", Seq.empty(),
                    ImmutableMap.empty(), rulename);
        }
        public Env next() {
            return new Env(varId, rel + 1, "call.rel(" + (rel + 1) + ")", skeleton, statements, symbols, rulename);
        }
        public Env focus(String target) {
            return new Env(varId, rel, target, skeleton, statements, symbols, rulename);
        }
        public Env state(String statement) {
            return new Env(varId, rel, current, skeleton, statements.appended(statement), symbols, rulename);
        }
        public Env symbol(String symbol, String expression) {
            return new Env(varId, rel, current, skeleton, statements, symbols.putted(symbol, expression), rulename);
        }
        public Tuple2<String, Env> declare(String expression) {
            var name = "var_" + varId.getAndIncrement();
            return Tuple.of(name, state("var " + name + " = " + expression + ";"));
        }
        public Env grow(String requirement) {
            var vn = "s_" + varId.getAndIncrement();
            return new Env(varId, rel, current, vn + " -> " + vn + "." + requirement, statements, symbols, rulename);
        }
    }
}
