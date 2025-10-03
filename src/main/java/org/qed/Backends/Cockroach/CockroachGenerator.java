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
            condPattern = "[]";
        } else if (filter.cond() instanceof RexRN.False) {
            condPattern = condEnv.pattern();
        } else {
            condPattern = condEnv.current();
        }

        String pattern = "(Select\n    " + sourcePattern + "\n    " + condPattern + "\n)";
        return condEnv.setPattern(pattern).focus(pattern);
    }

    public Env onMatchProject(Env env, RelRN.Project project) {
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
        Env leftEnv = onMatch(env, join.left());
        String leftPattern = leftEnv.current();
        Env rightEnv = onMatch(leftEnv, join.right());
        String rightPattern = rightEnv.current();
        Env condEnv = onMatch(rightEnv, join.cond());
        String condPattern = condEnv.current();

        String privateVar = condEnv.generateVar("private");
        Env privateEnv = condEnv.addBinding("private", privateVar);

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

        String privateVar = condEnv.bindings().get("private");
        if (privateVar == null) {
            privateVar = "private";
        }

        String joinType = getJoinType(join.ty().semantics());
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

        // Generate private parameter variable for SetPrivate
        String privateVar = currentEnv.generateVar("private");
        Env privateEnv = currentEnv.addBinding("intersect_private", privateVar);

        String intersectType = intersect.all() ? "IntersectAll" : "Intersect";

        // Intersect has binary structure: (Intersect left right private)
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

    @Override
    public Env onMatchAnd(Env env, RexRN.And and) {
        Env currentEnv = env;
        Seq<String> operandPatterns = Seq.empty();
        for (RexRN operand : and.sources()) {
            Env operandEnv = onMatch(currentEnv, operand);
            operandPatterns = operandPatterns.appended(operandEnv.current());
            currentEnv = operandEnv;
        }
        String pattern = "(And " + operandPatterns.joinToString(" ") + ")";
        return currentEnv.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env onMatchTrue(Env env, RexRN literal) {
        String varName = env.generateVar("true");
        return env.addBinding("true", varName)
                .focus("$" + varName + ":(True)")
                .setPattern("(True)");
    }

    @Override
    public Env onMatchFalse(Env env, RexRN literal) {
        return env.focus("(False)")
                .setPattern("(False)");
    }

    @Override
    public Env transformScan(Env env, RelRN.Scan scan) {
        String varName = env.bindings().get(scan.name());
        if (varName == null) {
            varName = "input";
        }
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
        String pattern = "(Select\n    " + sourcePattern + "\n    " + condPattern + "\n)";
        return condEnv.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformProject(Env env, RelRN.Project project) {
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

        String privateVar = currentEnv.bindings().get("union_private");
        if (privateVar == null) {
            privateVar = "private";
        }

        String unionType = union.all() ? "UnionAll" : "Union";

        String pattern;
        if (sourcePatterns.size() == 2) {
            pattern = "(" + unionType + "\n    " + sourcePatterns.get(0) + "\n    " + sourcePatterns.get(1) + "\n    $" + privateVar + "\n)";
        } else {
            String nestedPrivate = currentEnv.bindings().get("inner_union_private");
            if (nestedPrivate == null) {
                nestedPrivate = privateVar;
            }
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
        return env.setPattern("(True)").focus("(True)");
    }

    @Override
    public Env transformFalse(Env env, RexRN literal) {
        return env.setPattern("(False)").focus("(False)");
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