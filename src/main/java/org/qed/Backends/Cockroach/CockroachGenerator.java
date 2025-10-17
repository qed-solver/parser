package org.qed.Backends.Cockroach;

import kala.collection.Seq;
import kala.collection.immutable.ImmutableMap;
import org.qed.CodeGenerator;
import org.qed.RelRN;
import org.qed.RexRN;
import java.util.concurrent.atomic.AtomicInteger;

public class CockroachGenerator implements CodeGenerator<CockroachGenerator.Env> {

    @Override
    public Env preMatch(String rulename) {
        return Env.empty(rulename);
    }

    @Override
    public Env onMatchScan(Env env, RelRN.Scan scan) {
        String varName = env.generateVar("input");
        return env.addBinding(scan.name(), varName)
                .focus("$" + varName + ":*");
    }

    @Override
    public Env onMatchFilter(Env env, RelRN.Filter filter) {
        Env sourceEnv = onMatch(env, filter.source());
        String sourcePattern = sourceEnv.current();
        Env condEnv = onMatch(sourceEnv, filter.cond());
        String condPattern;
        if (filter.cond() instanceof RexRN.True) {
            String pattern = "(Select\n    " + sourcePattern + "\n    []\n)";
            return condEnv.setPattern(pattern).focus(pattern);
        } else if (filter.cond() instanceof RexRN.False) {
            condPattern = condEnv.pattern();
        } else {
            condPattern = condEnv.current();
        }
        String pattern = "(Select\n    " + sourcePattern + "\n    " + condPattern + "\n)";
        return condEnv.setPattern(pattern).focus(pattern);
    }

    public Env onMatchProject(Env env, RelRN.Project project) {
        if (env.rulename.equals("ProjectMerge") && project.source() instanceof RelRN.Project) {
            Env outerProjEnv = onMatch(env, project.map());
            String outerProjPattern = outerProjEnv.current();
            RelRN.Project innerProject = (RelRN.Project) project.source();
            Env innerInputEnv = onMatch(outerProjEnv, innerProject.source());
            String innerInputPattern = innerInputEnv.current();          
            Env innerProjEnv = onMatch(innerInputEnv, innerProject.map());
            String innerProjPattern = innerProjEnv.current();          
            String innerPassthroughVar = innerProjEnv.generateVar("innerPassthrough");
            Env innerPassthroughEnv = innerProjEnv.addBinding("innerPassthrough", innerPassthroughVar);            
            String outerPassthroughVar = innerPassthroughEnv.generateVar("passthrough");
            Env outerPassthroughEnv = innerPassthroughEnv.addBinding("passthrough", outerPassthroughVar);
            String innerProjectPattern = "Project\n    " + innerInputPattern + "\n    " + innerProjPattern + "\n    $" + innerPassthroughVar + ":*";
            String outerProjVar = outerProjPattern.replace(":*", "");
            String innerProjVar = innerProjPattern.replace(":*", "");
            String pattern = "(Project\n    $input:(" + innerProjectPattern + ")\n    " + outerProjPattern + " &\n        (CanMergeProjections " + outerProjVar + " " + innerProjVar + ")\n    $" + outerPassthroughVar + ":*\n)";
            return outerPassthroughEnv.setPattern(pattern).focus(pattern);
        }
        Env sourceEnv = onMatch(env, project.source());
        String sourcePattern = sourceEnv.current();
        Env projEnv = onMatch(sourceEnv, project.map());
        String projPattern = projEnv.current();
        String passthroughVar = projEnv.generateVar("passthrough");
        Env passthroughEnv = projEnv.addBinding("passthrough", passthroughVar);
        String pattern = "(Project\n    " + sourcePattern + "\n    " + projPattern + "\n    $" + passthroughVar + ":*\n)";
        return passthroughEnv.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env onMatchJoin(Env env, RelRN.Join join) {
        if (env.rulename.equals("AggregateJoinRemove") || env.rulename.equals("AggregateJoinJoinRemove")) {
            if (isUnusedJoinInAggregateRules(join)) {
                String joinType = getJoinType(join.ty().semantics());
                String pattern = "(" + joinType + "\n    *:*\n    *:*\n    *:*\n    *:*\n)";
                return env.setPattern(pattern).focus(pattern);
            }
        }   
        Env leftEnv = onMatch(env, join.left());
        String leftPattern = leftEnv.current();
        Env rightEnv = onMatch(leftEnv, join.right());
        String rightPattern = rightEnv.current();
        Env condEnv = onMatch(rightEnv, join.cond());
        String condPattern = condEnv.current();
        String privateVar = condEnv.generateVar("private");
        Env privateEnv = condEnv.addBinding("private_" + System.identityHashCode(join), privateVar)
                .addBinding("last_private", privateVar);
        String joinType = getJoinType(join.ty().semantics());
        String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    " + condPattern + "\n    $" + privateVar + ":*\n)";
        return privateEnv.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformJoin(Env env, RelRN.Join join) {
        Env leftEnv = transform(env, join.left());
        String leftPattern = leftEnv.current();
        Env rightEnv = transform(leftEnv, join.right());
        String rightPattern = rightEnv.current();
        Env condEnv = transform(rightEnv, join.cond());
        String condPattern = condEnv.current();
        String privateVar = condEnv.bindings().getOrDefault("private_" + System.identityHashCode(join), 
                condEnv.bindings().getOrDefault("last_private", "private"));
        String joinType = getJoinType(join.ty().semantics());
        if (env.rulename.equals("JoinCommute")) {
            String pattern = "(" + joinType + "\n    $input_1\n    $input_0\n    " + condPattern + "\n    $" + privateVar + "\n)";
            return condEnv.setPattern(pattern).focus(pattern);
        }
        String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    " + condPattern + "\n    $" + privateVar + "\n)";
        return condEnv.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env onMatchUnion(Env env, RelRN.Union union) {
        Env currentEnv = env;
        Seq<String> sourcePatterns = Seq.empty();
        for (RelRN source : union.sources()) {
            Env sourceEnv = onMatch(currentEnv, source);
            if (source instanceof RelRN.Union) {
                String subPrivate = sourceEnv.bindings().get("union_private");
                if (subPrivate != null) {
                    sourceEnv = sourceEnv.addBinding("inner_union_private", subPrivate);
                }
            }
            sourcePatterns = sourcePatterns.appended(sourceEnv.current());
            currentEnv = sourceEnv;
        }
        String privateVar = currentEnv.generateVar("private");
        Env privateEnv = currentEnv.addBinding("union_private", privateVar);
        String unionType = union.all() ? "UnionAll" : "Union";
        String pattern;
        if (sourcePatterns.size() == 2) {
            pattern = "(" + unionType + "\n    " + sourcePatterns.get(0) + "\n    " + sourcePatterns.get(1) + "\n    $" + privateVar + ":*\n)";
        } else {
            pattern = buildNestedUnion(unionType, sourcePatterns, privateVar + ":*");
        }
        return privateEnv.setPattern(pattern).focus(pattern);
    }

    private String buildNestedUnion(String unionType, Seq<String> sources, String privatePattern) {
        if (sources.size() == 2) {
            return "(" + unionType + "\n    " + sources.get(0) + "\n    " + sources.get(1) + "\n    $" + privatePattern + "\n)";
        }
        String first = sources.get(0);
        String nested = buildNestedUnion(unionType, sources.drop(1), privatePattern);
        return "(" + unionType + "\n    " + first + "\n    " + nested + "\n    $" + privatePattern + "\n)";
    }

    @Override
    public Env onMatchIntersect(Env env, RelRN.Intersect intersect) {
        Env currentEnv = env;
        Seq<String> sourcePatterns = Seq.empty();
        for (RelRN source : intersect.sources()) {
            Env sourceEnv = onMatch(currentEnv, source);
            if (source instanceof RelRN.Intersect) {
                String subPrivate = sourceEnv.bindings().get("intersect_private");
                if (subPrivate != null) {
                    sourceEnv = sourceEnv.addBinding("inner_intersect_private", subPrivate);
                }
            }
            sourcePatterns = sourcePatterns.appended(sourceEnv.current());
            currentEnv = sourceEnv;
        }
        String privateVar = currentEnv.generateVar("private");
        Env privateEnv = currentEnv.addBinding("intersect_private", privateVar);
        String intersectType = intersect.all() ? "IntersectAll" : "Intersect";
        String pattern;
        if (sourcePatterns.size() == 2) {
            pattern = "(" + intersectType + "\n    " + sourcePatterns.get(0) + "\n    " + sourcePatterns.get(1) + "\n    $" + privateVar + ":*\n)";
        } else {
            pattern = buildNestedIntersect(intersectType, sourcePatterns, privateVar + ":*");
        }
        return privateEnv.setPattern(pattern).focus(pattern);
    }


    private String buildNestedIntersect(String intersectType, Seq<String> sources, String privatePattern) {
        if (sources.size() == 2) {
            return "(" + intersectType + "\n    " + sources.get(0) + "\n    " + sources.get(1) + "\n    $" + privatePattern + "\n)";
        }
        String first = sources.get(0);
        String nested = buildNestedIntersect(intersectType, sources.drop(1), privatePattern);
        return "(" + intersectType + "\n    " + first + "\n    " + nested + "\n    $" + privatePattern + "\n)";
    }

    @Override
    public Env onMatchAggregate(Env env, RelRN.Aggregate aggregate) {
        Env sourceEnv = onMatch(env, aggregate.source());
        String sourcePattern = sourceEnv.current();
        Env aggsEnv = onMatchAggCalls(sourceEnv, aggregate.aggCalls());
        String aggsPattern = aggsEnv.current();
        Env groupingEnv = onMatchGroupSet(aggsEnv, aggregate.groupSet());
        String groupingPattern = groupingEnv.current();
        String privateVar = groupingEnv.generateVar("private");
        Env privateEnv = groupingEnv.addBinding("aggregate_private", privateVar);
        String aggregateType = determineAggregateType(aggregate);
        String pattern = "(" + aggregateType + "\n    " + sourcePattern + "\n    " + aggsPattern + "\n    $" + privateVar + ":*\n)";
        return privateEnv.setPattern(pattern).focus(pattern);
    }

    private Env onMatchAggCalls(Env env, Seq<RelRN.AggCall> aggCalls) {
        Env currentEnv = env;
        Seq<String> aggPatterns = Seq.empty();
        for (RelRN.AggCall aggCall : aggCalls) {
            String aggVar = currentEnv.generateVar("agg");
            Env aggEnv = currentEnv.addBinding(aggCall.name(), aggVar);
            aggPatterns = aggPatterns.appended("$" + aggVar + ":*");
            currentEnv = aggEnv;
        }
        String pattern;
        if (aggCalls.size() == 1) {
            String aggVar = currentEnv.generateVar("aggregations");
            Env boundEnv = currentEnv.addBinding("aggregations", aggVar);
            pattern = "$" + aggVar + ":*";
            return boundEnv.setPattern(pattern).focus(pattern);
        } else {
            pattern = "[" + aggPatterns.joinToString(" ") + "]";
            return currentEnv.setPattern(pattern).focus(pattern);
        }
    }

    private Env onMatchGroupSet(Env env, Seq<RexRN> groupSet) {
        Env currentEnv = env;
        Seq<String> groupPatterns = Seq.empty();
        for (RexRN groupCol : groupSet) {
            Env groupEnv = onMatch(currentEnv, groupCol);
            groupPatterns = groupPatterns.appended(groupEnv.current());
            currentEnv = groupEnv;
        }
        String pattern = "[" + groupPatterns.joinToString(" ") + "]";
        return currentEnv.setPattern(pattern).focus(pattern);
    }

    private String determineAggregateType(RelRN.Aggregate aggregate) {
        return "GroupBy";
    }

    @Override
    public Env onMatchEmpty(Env env, RelRN.Empty empty) {
        String varName = env.generateVar("empty");
        return env.addBinding("empty", varName)
                .focus("$" + varName + ":(Values)");
    }

    @Override
    public Env onMatchField(Env env, RexRN.Field field) {
        String varName = env.generateVar("field");
        return env.addBinding("field_" + field.ordinal(), varName)
                .focus("$" + varName + ":*");
    }

    @Override
    public Env onMatchPred(Env env, RexRN.Pred pred) {
        String varName = env.generateVar("cond");
        return env.addBinding(pred.operator().getName(), varName)
                .focus("$" + varName + ":*");
    }

    @Override
    public Env onMatchProj(Env env, RexRN.Proj proj) {
        String varName = env.generateVar("proj");
        return env.addBinding(proj.operator().getName(), varName)
                .focus("$" + varName + ":*");
    }

    public Env onMatchGroupBy(Env env, RexRN.GroupBy groupBy) {
        String varName = env.generateVar("groupBy");
        return env.addBinding(groupBy.operator().getName(), varName)
                .focus("$" + varName + ":*");
    }

    @Override
    public Env onMatchAnd(Env env, RexRN.And and) {
        Env currentEnv = env;
        Seq<String> operandPatterns = Seq.empty();
        for (RexRN operand : and.sources()) {
            Env operandEnv = onMatch(currentEnv, operand);
            operandPatterns = operandPatterns.appended(operandEnv.current());
            currentEnv = operandEnv;
        }
        String pattern = buildNestedAndPattern(operandPatterns);
        return currentEnv.setPattern(pattern).focus(pattern);
    }

    private String buildNestedAndPattern(Seq<String> operands) {
        if (operands.isEmpty()) {
            return "(And)";
        }
        if (operands.size() == 1) {
            return operands.get(0);
        }
        String left = operands.get(0);
        String right = buildNestedAndPattern(operands.drop(1));
        return "(And " + left + " " + right + ")";
    }

    @Override
    public Env onMatchTrue(Env env, RexRN literal) {
        String varName = env.generateVar("true");
        return env.addBinding("true_" + System.identityHashCode(literal), varName)
                .focus("$" + varName + ":(True)")
                .setPattern("$" + varName + ":(True)");
    }

    @Override
    public Env onMatchFalse(Env env, RexRN literal) {
        return env.focus("(False)")
                .setPattern("(False)");
    }

    @Override
    public Env onMatchCustom(Env env, RexRN custom) {
        if (custom instanceof RexRN.GroupBy groupBy) {
            return onMatchGroupBy(env, groupBy);
        }
        return unimplementedOnMatch(env, custom);
    }

    @Override
    public Env transformScan(Env env, RelRN.Scan scan) {
        String varName = env.bindings().getOrDefault(scan.name(), "input");
        String pattern = "$" + varName;
        return env.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformFilter(Env env, RelRN.Filter filter) {
        if (filter.cond() instanceof RexRN.True) {
            return transform(env, filter.source());
        }
        if (filter.source() instanceof RelRN.Empty) {
            return transform(env, filter.source());
        }
        Env sourceEnv = transform(env, filter.source());
        String sourcePattern = sourceEnv.current();
        Env condEnv = transform(sourceEnv, filter.cond());
        String condPattern = condEnv.current();
        String filterPattern;
        if (condPattern.startsWith("(ConcatFilters")) {
            filterPattern = condPattern;
        } else if (condPattern.startsWith("$") && !condPattern.contains(" ")) {
            filterPattern = condPattern;
        } else {
            filterPattern = "[" + condPattern + "]";
        }
        String pattern = "(Select\n    " + sourcePattern + "\n    " + filterPattern + "\n)";
        return condEnv.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformProject(Env env, RelRN.Project project) {
        if (env.rulename.equals("ProjectMerge")) {
            String pattern = "(Project\n    $input_1\n    (MergeProjections\n        $proj_0\n        $proj_2\n        $passthrough_4\n    )\n    (DifferenceCols\n        $innerPassthrough_3\n        (ProjectionCols $proj_2)\n    )\n)";
            return env.setPattern(pattern).focus(pattern);
        }
        Env sourceEnv = transform(env, project.source());
        String sourcePattern = sourceEnv.current();
        Env projEnv = transform(sourceEnv, project.map());
        String projPattern = projEnv.current();
        String passthroughVar = projEnv.bindings().get("passthrough");
        if (passthroughVar == null) {
            passthroughVar = "passthrough";
        }
        String pattern = "(Project\n    " + sourcePattern + "\n    " + projPattern + "\n    $" + passthroughVar + "\n)";
        return projEnv.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformUnion(Env env, RelRN.Union union) {
        Env currentEnv = env;
        Seq<String> sourcePatterns = Seq.empty();
        for (RelRN source : union.sources()) {
            Env sourceEnv = transform(currentEnv, source);
            sourcePatterns = sourcePatterns.appended(sourceEnv.current());
            currentEnv = sourceEnv;
        }
        String privateVar = currentEnv.bindings().getOrDefault("union_private", "private");
        String unionType = union.all() ? "UnionAll" : "Union";
        String pattern;
        if (sourcePatterns.size() == 2) {
            pattern = "(" + unionType + "\n    " + sourcePatterns.get(0) + "\n    " + sourcePatterns.get(1) + "\n    $" + privateVar + "\n)";
        } else {
            String nestedPrivate = currentEnv.bindings().getOrDefault("inner_union_private", privateVar);
            String nested = buildNestedUnionTransform(unionType, sourcePatterns.drop(1), nestedPrivate);
            pattern = "(" + unionType + "\n    " + sourcePatterns.get(0) + "\n    " + nested + "\n    $" + privateVar + "\n)";
        }
        return currentEnv.setPattern(pattern).focus(pattern);
    }

    private String buildNestedUnionTransform(String unionType, Seq<String> sources, String privateVar) {
        if (sources.size() == 2) {
            return "(" + unionType + "\n    " + sources.get(0) + "\n    " + sources.get(1) + "\n    $" + privateVar + "\n)";
        }
        String first = sources.get(0);
        String nested = buildNestedUnionTransform(unionType, sources.drop(1), privateVar);
        return "(" + unionType + "\n    " + first + "\n    " + nested + "\n    $" + privateVar + "\n)";
    }

    @Override
    public Env transformIntersect(Env env, RelRN.Intersect intersect) {
        Env currentEnv = env;
        Seq<String> sourcePatterns = Seq.empty();
        for (RelRN source : intersect.sources()) {
            Env sourceEnv = transform(currentEnv, source);
            sourcePatterns = sourcePatterns.appended(sourceEnv.current());
            currentEnv = sourceEnv;
        }
        String privateVar = currentEnv.bindings().get("intersect_private");
        if (privateVar == null) {
            privateVar = "private";
        }
        String intersectType = intersect.all() ? "IntersectAll" : "Intersect";
        String pattern;
        if (sourcePatterns.size() == 2) {
            pattern = "(" + intersectType + "\n    " + sourcePatterns.get(0) + "\n    " + sourcePatterns.get(1) + "\n    $" + privateVar + "\n)";
        } else {
            String nestedPrivate = currentEnv.bindings().get("inner_intersect_private");
            if (nestedPrivate == null) {
                nestedPrivate = privateVar;
            }
            String nested = buildNestedIntersectTransform(intersectType, sourcePatterns.drop(1), nestedPrivate);
            pattern = "(" + intersectType + "\n    " + sourcePatterns.get(0) + "\n    " + nested + "\n    $" + privateVar + "\n)";
        }
        return currentEnv.setPattern(pattern).focus(pattern);
    }


    private String buildNestedIntersectTransform(String intersectType, Seq<String> sources, String privateVar) {
        if (sources.size() == 2) {
            return "(" + intersectType + "\n    " + sources.get(0) + "\n    " + sources.get(1) + "\n    $" + privateVar + "\n)";
        }
        String first = sources.get(0);
        String nested = buildNestedIntersectTransform(intersectType, sources.drop(1), privateVar);
        return "(" + intersectType + "\n    " + first + "\n    " + nested + "\n    $" + privateVar + "\n)";
    }

    @Override
    public Env transformAggregate(Env env, RelRN.Aggregate aggregate) {
        Env sourceEnv = transform(env, aggregate.source());
        String sourcePattern = sourceEnv.current();
        Env aggsEnv = transformAggCalls(sourceEnv, aggregate.aggCalls());
        String aggsPattern = aggsEnv.current();
        Env groupingEnv = transformGroupSet(aggsEnv, aggregate.groupSet());
        String groupingPattern = groupingEnv.current();
        String privateVar = groupingEnv.bindings().getOrDefault("aggregate_private", "private");
        String aggregateType = determineAggregateType(aggregate);  
        String pattern = "(" + aggregateType + "\n    " + sourcePattern + "\n    " + aggsPattern + "\n    $" + privateVar + "\n)";
        return groupingEnv.setPattern(pattern).focus(pattern);
    }

    private Env transformAggCalls(Env env, Seq<RelRN.AggCall> aggCalls) {
        Env currentEnv = env;
        Seq<String> aggPatterns = Seq.empty();
        for (RelRN.AggCall aggCall : aggCalls) {
            String aggVar = currentEnv.bindings().getOrDefault(aggCall.name(), "agg");
            aggPatterns = aggPatterns.appended("$" + aggVar);
            currentEnv = currentEnv.focus("$" + aggVar);
        }
        String pattern;
        if (aggCalls.size() == 1) {
            String aggVar = currentEnv.bindings().getOrDefault("aggregations", "aggregations");
            pattern = "$" + aggVar;
        } else {
            pattern = "[" + aggPatterns.joinToString(" ") + "]";
        }
        return currentEnv.setPattern(pattern).focus(pattern);
    }

    private Env transformGroupSet(Env env, Seq<RexRN> groupSet) {
        Env currentEnv = env;
        Seq<String> groupPatterns = Seq.empty();
        for (RexRN groupCol : groupSet) {
            Env groupEnv = transform(currentEnv, groupCol);
            groupPatterns = groupPatterns.appended(groupEnv.current());
            currentEnv = groupEnv;
        }
        String pattern = "[" + groupPatterns.joinToString(" ") + "]";
        return currentEnv.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformEmpty(Env env, RelRN.Empty empty) {
        String pattern = "(Values)";
        return env.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformField(Env env, RexRN.Field field) {
        String varName = env.bindings().get("field_" + field.ordinal());
        if (varName == null) {
            varName = "field";
        }
        String pattern = "$" + varName;
        return env.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformPred(Env env, RexRN.Pred pred) {
        String varName = env.bindings().get(pred.operator().getName());
        if (varName == null) {
            varName = "cond";
        }
        String pattern = "$" + varName;
        return env.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformProj(Env env, RexRN.Proj proj) {
        String varName = env.bindings().get(proj.operator().getName());
        if (varName == null) {
            varName = "proj";
        }
        String pattern = "$" + varName;
        return env.setPattern(pattern).focus(pattern);
    }

    public Env transformGroupBy(Env env, RexRN.GroupBy groupBy) {
        String varName = env.bindings().get(groupBy.operator().getName());
        if (varName == null) {
            varName = "groupBy";
        }
        String pattern = "$" + varName;
        return env.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformAnd(Env env, RexRN.And and) {
        Env currentEnv = env;
        Seq<String> operandPatterns = Seq.empty();

        for (RexRN operand : and.sources()) {
            Env operandEnv = transform(currentEnv, operand);
            operandPatterns = operandPatterns.appended(operandEnv.current());
            currentEnv = operandEnv;
        }

        String pattern = "(ConcatFilters " + operandPatterns.joinToString(" ") + ")";
        return currentEnv.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformTrue(Env env, RexRN literal) {
        String varName = env.bindings().getOrDefault("true_" + System.identityHashCode(literal), "true");
        return env.setPattern("$" + varName).focus("$" + varName);
    }

    @Override
    public Env transformFalse(Env env, RexRN literal) {
        return env.setPattern("(False)").focus("(False)");
    }

    @Override
    public Env transformCustom(Env env, RelRN custom) {
        if (custom instanceof org.qed.RRuleInstances.JoinCommute.ProjectionRelRN projection) {
            return transform(env, projection.source());
        }
        return unimplementedTransform(env, custom);
    }

    @Override
    public Env transformCustom(Env env, RexRN custom) {
        if (custom instanceof RexRN.GroupBy groupBy) {
            return transformGroupBy(env, groupBy);
        }
        return unimplementedTransform(env, custom);
    }

    @Override
    public String translate(String name, Env onMatch, Env transform) {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(name).append(", Normalize]\n");
        sb.append(onMatch.pattern()).append("\n");
        sb.append("=>\n");
        sb.append(transform.pattern()).append("\n");
        return sb.toString();
    }

    private String getJoinType(org.apache.calcite.rel.core.JoinRelType joinType) {
        return switch (joinType) {
            case INNER -> "InnerJoin";
            case LEFT -> "LeftJoin";
            case RIGHT -> "RightJoin";
            case FULL -> "FullJoin";
            case SEMI -> "SemiJoin";
            case ANTI -> "AntiJoin";
            default -> "InnerJoin";
        };
    }

    public record Env(
            AtomicInteger varId,
            String pattern,
            ImmutableMap<String, String> bindings,
            String currentVar,
            String rulename
    ) {
        public static Env empty(String rulename) {
            return new Env(new AtomicInteger(), "", ImmutableMap.empty(), "", rulename);
        }
        public Env focus(String target) {
            return new Env(varId, pattern, bindings, target, rulename);
        }
        public Env setPattern(String newPattern) {
            return new Env(varId, newPattern, bindings, currentVar, rulename);
        }
        public Env addBinding(String key, String value) {
            return new Env(varId, pattern, bindings.putted(key, value), currentVar, rulename);
        }
        public String generateVar(String prefix) {
            return prefix + "_" + varId.getAndIncrement();
        }
        public String current() {
            return currentVar;
        }
        public String pattern() {
            return pattern;
        }
        public ImmutableMap<String, String> bindings() {
            return bindings;
        }
        public String rulename() {
            return rulename;
        }
    }
}