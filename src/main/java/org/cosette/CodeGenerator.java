package org.cosette;

import kala.collection.Map;
import kala.collection.Seq;

import java.util.concurrent.atomic.AtomicInteger;

public interface CodeGenerator {
    static void main(String[] args) {
        // Calcite:
        // - onMatch: Check RelNode type, register uninterpreted symbols
        // - transform: Recursively build (depth first) with builder, keep builder expression
        var calciteCodeGen = new CodeGenerator() {
            final AtomicInteger variableIndex = new AtomicInteger();

            Env assignVariable(Env env, String expression) {
                var name = STR."var_\{variableIndex.getAndIncrement()}";
                return env.state(STR."var \{name} = \{expression};").express(Seq.of(name));
            }

            @Override
            public Env preMatch() {
                return assignVariable(Env.empty(), "call.rel(0)");
            }

            @Override
            public Env preTransform(Env env) {
                return assignVariable(env, "call.builder()");
            }

            @Override
            public Env postTransform(Env env) {
                var result_env = assignVariable(env, STR."\{env.expressions().first()}.build()");
                return result_env.state(STR."call.transformTo(\{result_env.expressions().first()});");
            }

            @Override
            public String translate(Env ignored, Env transform) {
                StringBuilder builder = new StringBuilder("@Override public void onMatch(RelOptRuleCall call) {\n");
                transform.statements().forEach(statement -> builder.append("\t").append(statement).append("\n"));
                builder.append("}\n");
                return builder.toString();
            }

            @Override
            public Env onMatchScan(Env env, RelRN.Scan scan) {
                return env.symbol(scan.name(), env.expressions().first());
            }

            @Override
            public Env onMatchFilter(Env env, RelRN.Filter filter) {
                var rel = env.expressions().first();
                env = env.state(STR."if (!(\{rel} instanceof LogicalFilter)) { return; }");
                var rel_filter_env = assignVariable(env, STR."(LogicalFilter) \{rel}");
                var rel_filter = rel_filter_env.expressions().first();
                var source_env = assignVariable(rel_filter_env, STR."\{rel_filter}.getInput()");
                var source_match = onMatch(source_env, filter.source());
                var cond_env = assignVariable(source_match, STR."\{rel_filter}.getCondition()");
                return onMatch(cond_env, filter.cond());
            }

            @Override
            public Env onMatchJoin(Env env, RelRN.Join join) {
                var rel = env.expressions().first();
                env = env.state(STR."if (!(\{rel} instanceof LogicalJoin)) { return; }");
                var rel_join_env = assignVariable(env, STR."(LogicalJoin) \{rel}");
                var rel_join = rel_join_env.expressions().first();
                var join_type_env = assignVariable(rel_join_env, STR."\{rel_join}.getJoinType()");
                var join_type = join_type_env.expressions().first();
                var type_cond_expression = switch (join.ty()) {

                    case INNER -> STR."\{join_type} == JoinRelType.INNER";
                    case LEFT -> STR."\{join_type} == JoinRelType.LEFT";
                    case RIGHT -> STR."\{join_type} == JoinRelType.RIGHT";
                    case FULL -> STR."\{join_type} == JoinRelType.FULL";
                    case SEMI -> STR."\{join_type} == JoinRelType.SEMI";
                    case ANTI -> STR."\{join_type} == JoinRelType.ANTI";
                };
                var type_cond_env = assignVariable(join_type_env, type_cond_expression);
                var type_match = type_cond_env.state(STR."if (!\{type_cond_env.expressions().first()}) { return; }");
                var left_source_env = assignVariable(type_match, STR."\{rel_join}.getLeft()");
                var left_match_env = onMatch(left_source_env, join.left());
                var right_source_env = assignVariable(left_match_env, STR."\{rel_join}.getRight()");
                var right_match_env = onMatch(right_source_env, join.right());
                var cond_source_env = assignVariable(right_match_env, STR."\{rel_join}.getCondition()");
                return onMatch(cond_source_env, join.cond());
            }

            @Override
            public Env onMatchPred(Env env, RexRN.Pred pred) {
                return env.symbol(pred.name(), env.expressions().first());
            }

            @Override
            public Env onMatchCustom(Env env, RexRN custom) {
                return switch (custom) {
                    case RRule.JoinConditionPush.JoinPred joinPred -> {
                        var pred = env.expressions().first();
                        var breakdown_env = assignVariable(env, STR."customSplitFilter(\{pred})");
                        var breakdown = breakdown_env.expressions().first();
                        yield breakdown_env
                                .symbol(joinPred.bothPred(), STR."\{breakdown}.getBoth()")
                                .symbol(joinPred.leftPred(), STR."\{breakdown}.getLeft()")
                                .symbol(joinPred.rightPred(), STR."\{breakdown}.getRight()");
                    }
                    default -> CodeGenerator.super.onMatchCustom(env, custom);
                };
            }

            @Override
            public Env transformScan(Env env, RelRN.Scan scan) {
                var builder = env.expressions().first();
                var source_transform = STR."\{builder}.push(\{env.symbols().get(scan.name())})";
                return assignVariable(env, source_transform);
            }

            @Override
            public Env transformFilter(Env env, RelRN.Filter filter) {
                var source_transform = transform(env, filter.source());
                var source_expression = source_transform.expressions().first();
                var cond_transform = transform(source_transform, filter.cond());
                return assignVariable(cond_transform, STR."\{source_expression}.filter(\{cond_transform.expressions().first()})");
            }

            @Override
            public Env transformJoin(Env env, RelRN.Join join) {
                var left_source_transform = transform(env, join.left());
                var right_source_transform = transform(left_source_transform, join.right());
                var source_expression = right_source_transform.expressions().first();
                var join_expression = switch (join.ty()) {
                    case INNER -> "JoinRelType.INNER";
                    case LEFT -> "JoinRelType.LEFT";
                    case RIGHT -> "JoinRelType.RIGHT";
                    case FULL -> "JoinRelType.FULL";
                    case SEMI -> "JoinRelType.SEMI";
                    case ANTI -> "JoinRelType.ANTI";
                };
                var cond_transform = transform(right_source_transform, join.cond());
                return assignVariable(cond_transform, STR."\{source_expression}.join(\{join_expression}, \{cond_transform.expressions().first()})");
            }

            @Override
            public Env transformPred(Env env, RexRN.Pred pred) {
                return env.express(Seq.of(env.symbols().get(pred.name())));
            }

            @Override
            public Env transformAnd(Env env, RexRN.And and) {
                var source_transform = env;
                var operands = Seq.empty();
                for (var source : and.sources()) {
                    source_transform = transform(source_transform, source);
                    operands = operands.appended(source_transform.expressions().first());
                    source_transform = source_transform.express(env.expressions());
                }
                return assignVariable(source_transform, STR."\{env.expressions().first()}.and(\{operands.joinToString(", ")})");
            }
        };

        // Cockroach: Recursively (depth-first) generate DSL
        var cockroachCodeGen = new CodeGenerator() {
            @Override
            public String translate(Env onMatch, Env transform) {
                return STR."\{onMatch.expressions().first()}\n=>\n\{transform.expressions().first()}\n";
            }

            @Override
            public Env onMatchScan(Env env, RelRN.Scan scan) {
                return env.express(Seq.of(STR."$\{scan.name()}:*"));
            }

            @Override
            public Env onMatchFilter(Env env, RelRN.Filter filter) {
                var source_match = onMatch(env, filter.source());
                var source_expression = source_match.expressions().first();
                var cond_match = onMatch(source_match, filter.cond());
                var cond_expression = cond_match.expressions().first();
                return cond_match.express(Seq.of(STR."(Select \{source_expression} \{cond_expression})"));
            }

            @Override
            public Env onMatchJoin(Env env, RelRN.Join join) {
                var left_source_match = onMatch(env, join.left());
                var left_source_expression = left_source_match.expressions().first();
                var right_source_match = onMatch(left_source_match, join.right());
                var right_source_expression = right_source_match.expressions().first();
                var join_expression = switch (join.ty()) {
                    case INNER -> "InnerJoin";
                    case LEFT -> "LeftJoin";
                    case RIGHT -> "RightJoin";
                    case FULL -> "FullJoin";
                    case SEMI -> "SemiJoin";
                    case ANTI -> "AntiJoin";
                };
                var cond_match = onMatch(right_source_match, join.cond());
                var cond_expression = cond_match.expressions().first();
                return cond_match.express(Seq.of(STR."(\{join_expression} \{left_source_expression} \{right_source_expression} \{cond_expression} $private:*)"));
            }

            @Override
            public Env onMatchPred(Env env, RexRN.Pred pred) {
                return env.symbol(pred.name(), STR."$\{pred.name()}").express(Seq.of(STR."$\{pred.name()}:*"));
            }

            @Override
            public Env onMatchCustom(Env env, RexRN custom) {
                return switch (custom) {
                    case RRule.JoinConditionPush.JoinPred joinPred -> env
                            .symbol(joinPred.bothPred(), STR."(RemoveFiltersItem $\{joinPred.bothPred()} $item)")
                            .symbol(joinPred.leftPred(), "[(FiltersItem (MapJoinOpFilter $item $leftCols $equivSet))]")
                            .symbol(joinPred.rightPred(), "[(FiltersItem (MapJoinOpFilter $item $rightCols $equivSet))]")
                            .express(Seq.of("$on:[... $item:* & (CanMapJoinOpFilter $item $leftCols $equivSet) & (CanMapJoinOpFilter $item $rightCols $equivSet)...]"));
                    default -> CodeGenerator.super.onMatchCustom(env, custom);
                };
            }

            @Override
            public Env transformScan(Env env, RelRN.Scan scan) {
                return env.express(Seq.of(STR."$\{scan.name()}"));
            }

            @Override
            public Env transformFilter(Env env, RelRN.Filter filter) {
                var source_expression = transform(env, filter.source()).expressions().first();
                var cond_expression = transform(env, filter.cond()).expressions().first();
                return env.express(Seq.of(STR."(Select \{source_expression} \{cond_expression})"));
            }

            @Override
            public Env transformJoin(Env env, RelRN.Join join) {
                var left_source_expression = transform(env, join.left()).expressions().first();
                var right_source_expression = transform(env, join.right()).expressions().first();
                var cond_expression = transform(env, join.cond()).expressions.first();
                return env.express(Seq.of(STR."((OpName) \{left_source_expression} \{right_source_expression} \{cond_expression} $private)"));
            }

            @Override
            public Env transformPred(Env env, RexRN.Pred pred) {
                return env.express(Seq.of(env.symbols().get(pred.name())));
            }

            @Override
            public Env transformAnd(Env env, RexRN.And and) {
                var operands = and.sources().map(source -> transform(env, source).expressions().first());
                return env.express(Seq.of(STR."(ConcatFilters \{operands.joinToString(", ")})"));
            }
        };

        // Note: Join is treated as if it is a custom operator
        var filterMerge = new RRule.FilterMerge();
        var joinConditionPush = new RRule.JoinConditionPush();
        var calciteFilterMerge = calciteCodeGen.compose(filterMerge);
        var calciteJoinConditionPush = calciteCodeGen.compose(joinConditionPush);
        var cockroachFilterMerge = cockroachCodeGen.compose(filterMerge);
        var cockroachJoinConditionPush = cockroachCodeGen.compose(joinConditionPush);
        System.out.println(filterMerge.explain());
        System.out.println(calciteFilterMerge);
        System.out.println(cockroachFilterMerge);
        System.out.println();
        System.out.println(joinConditionPush.explain());
        System.out.println(calciteJoinConditionPush);
        System.out.println(cockroachJoinConditionPush);
    }

    default String unimplemented(String context, Object object) {
        return STR."<--\{context}\{object.getClass().getName()}-->";
    }

    default Env unimplementedOnMatch(Env env, Object object) {
        return env.express(Seq.of(unimplemented("Unspecified onMatch codegen: ", object)));
    }

    default Env unimplementedTransform(Env env, Object object) {
        return env.express(Seq.of(unimplemented("Unspecified transform codegen: ", object)));
    }

    default Env preMatch() {
        return Env.empty();
    }

    default Env onMatch(Env env, RelRN pattern) {
        return switch (pattern) {
            case RelRN.Scan scan -> onMatchScan(env, scan);
            case RelRN.Filter filter -> onMatchFilter(env, filter);
            case RelRN.Project project -> onMatchProject(env, project);
            case RelRN.Join join -> onMatchJoin(env, join);
            case RelRN.Union union -> onMatchUnion(env, union);
            case RelRN.Intersect intersect -> onMatchIntersect(env, intersect);
            default -> onMatchCustom(env, pattern);
        };
    }

    default Env onMatch(Env env, RexRN pattern) {
        return switch (pattern) {
            case RexRN.Field field -> onMatchField(env, field);
            case RexRN.JoinField joinField -> onMatchJoinField(env, joinField);
            case RexRN.Proj proj -> onMatchProj(env, proj);
            case RexRN.Pred pred -> onMatchPred(env, pred);
            case RexRN.And and -> onMatchAnd(env, and);
            case RexRN.Or or -> onMatchOr(env, or);
            case RexRN.Not not -> onMatchNot(env, not);
            default -> onMatchCustom(env, pattern);
        };
    }

    default Env postMatch(Env env) {
        return env;
    }

    default Env preTransform(Env env) {
        return env;
    }

    default Env transform(Env env, RelRN target) {
        return switch (target) {
            case RelRN.Scan scan -> transformScan(env, scan);
            case RelRN.Filter filter -> transformFilter(env, filter);
            case RelRN.Project project -> transformProject(env, project);
            case RelRN.Join join -> transformJoin(env, join);
            case RelRN.Union union -> transformUnion(env, union);
            case RelRN.Intersect intersect -> transformIntersect(env, intersect);
            default -> transformCustom(env, target);
        };
    }

    default Env transform(Env env, RexRN target) {
        return switch (target) {
            case RexRN.Field field -> transformField(env, field);
            case RexRN.JoinField joinField -> transformJoinField(env, joinField);
            case RexRN.Proj proj -> transformProj(env, proj);
            case RexRN.Pred pred -> transformPred(env, pred);
            case RexRN.And and -> transformAnd(env, and);
            case RexRN.Or or -> transformOr(env, or);
            case RexRN.Not not -> transformNot(env, not);
            default -> transformCustom(env, target);
        };
    }

    default Env postTransform(Env env) {
        return env;
    }

    default String translate(Env onMatch, Env transform) {
        return unimplemented("Unspecified translation to target language: ", Env.empty());
    }

    default String compose(RRule rule) {
        var onMatch = postMatch(onMatch(preMatch(), rule.before()));
        var transform = postTransform(transform(preTransform(onMatch), rule.after()));
        return translate(onMatch, transform);
    }

    default Env onMatchScan(Env env, RelRN.Scan scan) {
        return unimplementedOnMatch(env, scan);
    }

    default Env onMatchFilter(Env env, RelRN.Filter filter) {
        return unimplementedOnMatch(env, filter);
    }

    default Env onMatchProject(Env env, RelRN.Project project) {
        return unimplementedOnMatch(env, project);
    }

    default Env onMatchJoin(Env env, RelRN.Join join) {
        return unimplementedOnMatch(env, join);
    }

    default Env onMatchUnion(Env env, RelRN.Union union) {
        return unimplementedOnMatch(env, union);
    }

    default Env onMatchIntersect(Env env, RelRN.Intersect intersect) {
        return unimplementedOnMatch(env, intersect);
    }

    default Env onMatchCustom(Env env, RelRN custom) {
        return unimplementedOnMatch(env, custom);
    }

    default Env onMatchField(Env env, RexRN.Field field) {
        return unimplementedOnMatch(env, field);
    }

    default Env onMatchJoinField(Env env, RexRN.JoinField joinField) {
        return unimplementedOnMatch(env, joinField);
    }

    default Env onMatchProj(Env env, RexRN.Proj proj) {
        return unimplementedOnMatch(env, proj);
    }

    default Env onMatchPred(Env env, RexRN.Pred pred) {
        return unimplementedOnMatch(env, pred);
    }

    default Env onMatchAnd(Env env, RexRN.And and) {
        return unimplementedOnMatch(env, and);
    }

    default Env onMatchOr(Env env, RexRN.Or or) {
        return unimplementedOnMatch(env, or);
    }

    default Env onMatchNot(Env env, RexRN.Not not) {
        return unimplementedOnMatch(env, not);
    }

    default Env onMatchCustom(Env env, RexRN custom) {
        return unimplementedOnMatch(env, custom);
    }

    default Env transformScan(Env env, RelRN.Scan scan) {
        return unimplementedTransform(env, scan);
    }

    default Env transformFilter(Env env, RelRN.Filter filter) {
        return unimplementedTransform(env, filter);
    }

    default Env transformProject(Env env, RelRN.Project project) {
        return unimplementedTransform(env, project);
    }

    default Env transformJoin(Env env, RelRN.Join join) {
        return unimplementedTransform(env, join);
    }

    default Env transformUnion(Env env, RelRN.Union union) {
        return unimplementedTransform(env, union);
    }

    default Env transformIntersect(Env env, RelRN.Intersect intersect) {
        return unimplementedTransform(env, intersect);
    }

    default Env transformCustom(Env env, RelRN custom) {
        return unimplementedTransform(env, custom);
    }

    default Env transformField(Env env, RexRN.Field field) {
        return unimplementedTransform(env, field);
    }

    default Env transformJoinField(Env env, RexRN.JoinField joinField) {
        return unimplementedTransform(env, joinField);
    }

    default Env transformProj(Env env, RexRN.Proj proj) {
        return unimplementedTransform(env, proj);
    }

    default Env transformPred(Env env, RexRN.Pred pred) {
        return unimplementedTransform(env, pred);
    }

    default Env transformAnd(Env env, RexRN.And and) {
        return unimplementedTransform(env, and);
    }

    default Env transformOr(Env env, RexRN.Or or) {
        return unimplementedTransform(env, or);
    }

    default Env transformNot(Env env, RexRN.Not not) {
        return unimplementedTransform(env, not);
    }

    default Env transformCustom(Env env, RexRN custom) {
        return unimplementedTransform(env, custom);
    }

    record Env(Seq<String> expressions, Seq<String> statements, Map<String, String> symbols) {
        public static Env empty() {
            return new Env(Seq.empty(), Seq.empty(), Map.empty());
        }

        public Env express(Seq<String> expressions) {
            return new Env(expressions, statements, symbols);
        }

        public Env state(String statement) {
            return new Env(expressions, statements.appended(statement), symbols);
        }

        public Env symbol(String symbol, String expression) {
            return new Env(expressions, statements, symbols.toImmutableMap().putted(symbol, expression));
        }
    }

}
