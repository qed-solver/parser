package org.qed;

public interface CodeGenerator<E> {

    default String unimplemented(String context, Object object) {
        return "<--" + context + object.getClass().getName() + "-->";
    }

    default E unimplementedOnMatch(E env, Object object) {
        System.err.println(unimplemented("Unspecified onMatch codegen: ", object));
        return env;
    }

    default E unimplementedTransform(E env, Object object) {
        System.err.println(unimplemented("Unspecified transform codegen: ", object));
        return env;
    }

    E preMatch(String rulename);

    default E onMatch(E env, RelRN pattern) {
        return switch (pattern) {
            case RelRN.Scan scan -> onMatchScan(env, scan);
            case RelRN.Filter filter -> onMatchFilter(env, filter);
            case RelRN.Project project -> onMatchProject(env, project);
            case RelRN.Join join -> onMatchJoin(env, join);
            case RelRN.Union union -> onMatchUnion(env, union);
            case RelRN.Intersect intersect -> onMatchIntersect(env, intersect);
            case RelRN.Minus minus -> onMatchMinus(env, minus);
            case RelRN.Empty empty -> onMatchEmpty(env, empty);
            case RelRN.Aggregate aggregate -> onMatchAggregate(env, aggregate);
            default -> onMatchCustom(env, pattern);
        };
    }

    default E onMatch(E env, RexRN pattern) {
        return switch (pattern) {
            case RexRN.Field field -> onMatchField(env, field);
            case RexRN.JoinField joinField -> onMatchJoinField(env, joinField);
            case RexRN.Proj proj -> onMatchProj(env, proj);
            case RexRN.Pred pred -> onMatchPred(env, pred);
            case RexRN.And and -> onMatchAnd(env, and);
            case RexRN.Or or -> onMatchOr(env, or);
            case RexRN.Not not -> onMatchNot(env, not);
            case RexRN.True literal -> onMatchTrue(env, literal);
            case RexRN.False literal -> onMatchFalse(env, literal);
            default -> onMatchCustom(env, pattern);
        };
    }

    default E postMatch(E env) {
        return env;
    }

    default E preTransform(E env) {
        return env;
    }

    default E transform(E env, RelRN target) {
        return switch (target) {
            case RelRN.Scan scan -> transformScan(env, scan);
            case RelRN.Filter filter -> transformFilter(env, filter);
            case RelRN.Project project -> transformProject(env, project);
            case RelRN.Join join -> transformJoin(env, join);
            case RelRN.Union union -> transformUnion(env, union);
            case RelRN.Intersect intersect -> transformIntersect(env, intersect);
            case RelRN.Minus minus -> transformMinus(env, minus);
            case RelRN.Empty empty -> transformEmpty(env, empty);
            case RelRN.Aggregate aggregate -> transformAggregate(env, aggregate);
            default -> transformCustom(env, target);
        };
    }

    default E transform(E env, RexRN target) {
        return switch (target) {
            case RexRN.Field field -> transformField(env, field);
            case RexRN.JoinField joinField -> transformJoinField(env, joinField);
            case RexRN.Pred pred -> transformPred(env, pred);
            case RexRN.Proj proj -> transformProj(env, proj);
            case RexRN.And and -> transformAnd(env, and);
            case RexRN.Or or -> transformOr(env, or);
            case RexRN.Not not -> transformNot(env, not);
            case RexRN.True literal -> transformTrue(env, literal);
            case RexRN.False literal -> transformFalse(env, literal);
            default -> transformCustom(env, target);
        };
    }

    default E postTransform(E env) {
        return env;
    }

    default String translate(String name, E onMatch, E transform) {
        return "Unspecified translation to target language";
    }

    default String generate(RRule rule) {
        System.out.printf("Generating Rule: %s\n", rule.name());
        var onMatch = postMatch(onMatch(preMatch(rule.name()), rule.before()));
        var transform = postTransform(transform(preTransform(onMatch), rule.after()));
        return translate(rule.name(), onMatch, transform);
    }

    default E onMatchScan(E env, RelRN.Scan scan) {
        return unimplementedOnMatch(env, scan);
    }

    default E onMatchFilter(E env, RelRN.Filter filter) {
        return unimplementedOnMatch(env, filter);
    }

    default E onMatchProject(E env, RelRN.Project project) {
        return unimplementedOnMatch(env, project);
    }

    default E onMatchJoin(E env, RelRN.Join join) {
        return unimplementedOnMatch(env, join);
    }

    default E onMatchUnion(E env, RelRN.Union union) {
        return unimplementedOnMatch(env, union);
    }

    default E onMatchIntersect(E env, RelRN.Intersect intersect) {
        return unimplementedOnMatch(env, intersect);
    }

    default E onMatchMinus(E env, RelRN.Minus minus) {
        return unimplementedOnMatch(env, minus);
    }

    default E onMatchCustom(E env, RelRN custom) {
        return unimplementedOnMatch(env, custom);
    }

    default E onMatchField(E env, RexRN.Field field) {
        return unimplementedOnMatch(env, field);
    }

    default E onMatchJoinField(E env, RexRN.JoinField joinField) {
        return unimplementedOnMatch(env, joinField);
    }

    default E onMatchPred(E env, RexRN.Pred pred) {
        return unimplementedOnMatch(env, pred);
    }

    default E onMatchProj(E env, RexRN.Proj proj) {
        return unimplementedOnMatch(env, proj);
    }

    default E onMatchAnd(E env, RexRN.And and) {
        return unimplementedOnMatch(env, and);
    }

    default E onMatchOr(E env, RexRN.Or or) {
        return unimplementedOnMatch(env, or);
    }

    default E onMatchNot(E env, RexRN.Not not) {
        return unimplementedOnMatch(env, not);
    }

    default E onMatchCustom(E env, RexRN custom) {
        return unimplementedOnMatch(env, custom);
    }

    default E onMatchTrue(E env, RexRN literal) {
        return unimplementedOnMatch(env, literal);
    }

    default E onMatchFalse(E env, RexRN literal) {
        return unimplementedOnMatch(env, literal);
    }

    default E onMatchEmpty(E env, RelRN.Empty empty) {
        return unimplementedOnMatch(env, empty);
    }

    default E transformScan(E env, RelRN.Scan scan) {
        return unimplementedTransform(env, scan);
    }

    default E transformFilter(E env, RelRN.Filter filter) {
        return unimplementedTransform(env, filter);
    }

    default E transformProject(E env, RelRN.Project project) {
        return unimplementedTransform(env, project);
    }

    default E transformJoin(E env, RelRN.Join join) {
        return unimplementedTransform(env, join);
    }

    default E transformUnion(E env, RelRN.Union union) {
        return unimplementedTransform(env, union);
    }

    default E transformIntersect(E env, RelRN.Intersect intersect) {
        return unimplementedTransform(env, intersect);
    }

    default E transformMinus(E env, RelRN.Minus minus) {
        return unimplementedTransform(env, minus);
    }

    default E transformCustom(E env, RelRN custom) {
        return unimplementedTransform(env, custom);
    }

    default E transformField(E env, RexRN.Field field) {
        return unimplementedTransform(env, field);
    }

    default E transformJoinField(E env, RexRN.JoinField joinField) {
        return unimplementedTransform(env, joinField);
    }

    default E transformProj(E env, RexRN.Proj proj) {
        return unimplementedTransform(env, proj);
    }

    default E transformPred(E env, RexRN.Pred pred) {
        return unimplementedTransform(env, pred);
    }

    default E transformAnd(E env, RexRN.And and) {
        return unimplementedTransform(env, and);
    }

    default E transformOr(E env, RexRN.Or or) {
        return unimplementedTransform(env, or);
    }

    default E transformNot(E env, RexRN.Not not) {
        return unimplementedTransform(env, not);
    }

    default E transformCustom(E env, RexRN custom) {
        return unimplementedTransform(env, custom);
    }

    default E transformTrue(E env, RexRN literal) {
        return unimplementedTransform(env, literal);
    }

    default E transformFalse(E env, RexRN literal) {
        return unimplementedTransform(env, literal);
    }

    default E transformEmpty(E env, RelRN.Empty empty) {
        return unimplementedTransform(env, empty);
    }

    default E onMatchAggregate(E env, RelRN.Aggregate aggregate) {
        return unimplementedOnMatch(env, aggregate);
    }

    default E transformAggregate(E env, RelRN.Aggregate aggregate) {
        return unimplementedTransform(env, aggregate);
    }
}
