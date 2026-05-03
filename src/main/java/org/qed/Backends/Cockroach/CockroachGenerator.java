package org.qed.Backends.Cockroach;
import kala.collection.Seq;
import kala.collection.immutable.ImmutableMap;
import org.qed.CodeGenerator;
import org.qed.RelRN;
import org.qed.RexRN;
import java.util.concurrent.atomic.AtomicInteger;
public class CockroachGenerator implements CodeGenerator<CockroachGenerator.Env> {
    private static String b(String var) {
        return "$" + var + ":*";
    }
    private static String r(String var) {
        return "$" + var;
    }
    private static String N(String type, String... children) {
        StringBuilder sb = new StringBuilder("(").append(type);
        for (String c : children) sb.append("\n    ").append(c);
        return sb.append("\n)").toString();
    }
    private static String filtersBoundBy(String condVar, String ref) {
        return "(FiltersBoundBy " + r(condVar) + " " + ref + ")";
    }
    private static boolean flag(Env env, String key) {
        return env.bindings().containsKey(key);
    }
    private static String get(Env env, String key) {
        return env.bindings().get(key);
    }
    private static String get(Env env, String key, String def) {
        return env.bindings().getOrDefault(key, def);
    }
    private static String joinType(org.apache.calcite.rel.core.JoinRelType ty) {
        return switch (ty) {
            case INNER -> "InnerJoin";
            case LEFT -> "LeftJoin";
            case RIGHT -> "RightJoin";
            case FULL -> "FullJoin";
            case SEMI -> "SemiJoin";
            case ANTI -> "AntiJoin";
            default -> "InnerJoin";
        };
    }
    private String getJoinType(org.apache.calcite.rel.core.JoinRelType ty) {
        return joinType(ty);
    }
    @Override
    public Env preMatch(String rulename) {
        return Env.empty(rulename);
    }
    @Override
    public Env preTransform(Env env) {
        String p = env.pattern();
        if (p == null) return env;
        int idx = p.indexOf("(HasZeroRows $");
        if (idx < 0) return env;
        int start = idx + "(HasZeroRows $".length();
        int end = p.indexOf(")", start);
        if (end <= start) return env;
        String var = p.substring(start, end).trim();
        if (var.isEmpty()) return env;
        return env.addBinding("hasZeroRows", "true").addBinding("zeroInput", var);
    }
    @Override
    public Env onMatchScan(Env env, RelRN.Scan scan) {
        String var = env.generateVar("input");
        return env.addBinding(scan.name(), var).focus(b(var));
    }
    @Override
    public Env onMatchFilter(Env env, RelRN.Filter filter) {
        if (filter.source() instanceof RelRN.Project) {
            return matchFilterOverProject(env);
        }
        if (filter.source() instanceof RelRN.Union) {
            return matchFilterOverUnion(env);
        }
        Env sourceEnv = onMatch(env, filter.source());
        String sourcePattern = sourceEnv.current();
        Env condEnv = onMatch(sourceEnv, filter.cond());
        if (filter.source() instanceof RelRN.Empty) {
            String inputVar = condEnv.generateVar("input");
            String filtersVar = condEnv.generateVar("filters");
            String pattern = N("Select", b(inputVar) + " & (HasZeroRows " + r(inputVar) + ")", b(filtersVar) );
            return condEnv .addBinding("isPruneEmptyFilter", "true") .addBinding("pruneEmptyInput", inputVar) .setPattern(pattern).focus(pattern);
        }
        if (filter.cond() instanceof RexRN.True) {
            String pattern = N("Select", sourcePattern, "[]");
            return condEnv.setPattern(pattern).focus(pattern);
        }
        if (filter.cond() instanceof RexRN.False) {
            String onVar = condEnv.generateVar("on");
            Env onEnv = condEnv.addBinding("on", onVar);
            String itemVar = onEnv.generateVar("item");
            Env itemEnv = onEnv.addBinding("item", itemVar);
            String listPat = r(onVar) + ":[\n" + "        ...\n" + "        " + r(itemVar) + ":(FiltersItem (False))\n" + "        ...\n" + "    ]";
            String pattern = N("Select", sourcePattern, listPat);
            return itemEnv.setPattern(pattern).focus(pattern);
        }
        String condPattern = condEnv.current();
        if (filter.source() instanceof RelRN.Aggregate) {
            String condVarName = extractVar(condPattern);
            String privateVar = get(sourceEnv, "aggregate_private", null);
            if (condVarName != null && privateVar != null) {
                condPattern = condPattern + " & " + filtersBoundBy(condVarName, "(GroupingCols " + r(privateVar) + ")");
            }
        }
        String filterCondVarName = extractVar(condEnv.current());
        if (filterCondVarName != null) {
            condEnv = condEnv.addBinding("filterCondVar", filterCondVarName);
        }
        String pattern = N("Select", sourcePattern, condPattern);
        return condEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchFilterOverProject(Env env) {
        String inputVar = env.generateVar("input");
        Env inputEnv = env.addBinding("input", inputVar);
        String projVar = inputEnv.generateVar("proj");
        Env projEnv = inputEnv.addBinding("proj", projVar);
        String passthroughVar = projEnv.generateVar("passthrough");
        Env passEnv = projEnv.addBinding("passthrough", passthroughVar);
        String condVar = passEnv.generateVar("cond");
        Env condEnv = passEnv.addBinding("cond", condVar);
        String inputColsVar = condEnv.generateVar("inputCols");
        Env resultEnv = condEnv.addBinding("inputCols", inputColsVar);
        String projectPat = "(Project\n" + "    " + b(inputVar) + "\n" + "    " + b(projVar) + "\n" + "    " + b(passthroughVar) + "\n" + ")";
        String condLine = b(condVar) + " &\n" + "    " + filtersBoundBy(condVar, r(inputColsVar) + ":(OutputCols " + r(inputVar) + ")");
        String pattern = N("Select", projectPat, condLine);
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchFilterOverUnion(Env env) {
        String inputVar = env.generateVar("input");
        Env inputEnv = env.addBinding("input", inputVar);
        String leftVar = inputEnv.generateVar("left");
        Env leftEnv = inputEnv.addBinding("left", leftVar);
        String rightVar = leftEnv.generateVar("right");
        Env rightEnv = leftEnv.addBinding("right", rightVar);
        String colmapVar = rightEnv.generateVar("colmap");
        Env colmapEnv = rightEnv.addBinding("colmap", colmapVar);
        String filterVar = colmapEnv.generateVar("filter");
        Env filterEnv = colmapEnv.addBinding("filter", filterVar);
        String itemVar = filterEnv.generateVar("item");
        Env itemEnv = filterEnv.addBinding("item", itemVar);
        String inputColsVar = itemEnv.generateVar("inputCols");
        Env resultEnv = itemEnv.addBinding("inputCols", inputColsVar) .addBinding("isFilterSetOpTranspose", "true");
        String unionPat = r(inputVar) + ":(Union " + r(leftVar) + ":* " + r(rightVar) + ":* " + r(colmapVar) + ":*)";
        String listPat = r(filterVar) + ":[\n" + "        ...\n" + "        " + r(itemVar) + ":* &\n" + "            (CanMapOnSetOp " + r(itemVar) + ") &\n" + "            (IsBoundBy " + r(itemVar) + " " + r(inputColsVar) + ":(OutputCols " + r(inputVar) + "))\n" + "        ...\n" + "    ]";
        String pattern = N("Select", unionPat, listPat);
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    public Env onMatchProject(Env env, RelRN.Project project) {
        if (project.source() instanceof RelRN.Empty) {
            return matchProjectOverEmpty(env);
        }
        if (project.source() instanceof RelRN.Aggregate aggregate) {
            return matchProjectOverAggregate(env, aggregate);
        }
        if (project.source() instanceof RelRN.Project) {
            return matchProjectOverProject(env, project);
        }
        return matchProjectGeneral(env, project);
    }
    private Env matchProjectOverEmpty(Env env) {
        String inputVar = env.generateVar("input");
        Env inputEnv = env.addBinding("zeroInput", inputVar) .addBinding("hasZeroRows", "true");
        String projectionsVar = inputEnv.generateVar("projections");
        Env projEnv = inputEnv.addBinding("projections", projectionsVar);
        String passthroughVar = projEnv.generateVar("passthrough");
        Env passEnv = projEnv.addBinding("passthrough", passthroughVar);
        String pattern = N("Project", b(inputVar) + " & (HasZeroRows " + r(inputVar) + ")", b(projectionsVar), b(passthroughVar) );
        return passEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchProjectOverAggregate(Env env, RelRN.Aggregate aggregate) {
        String inputVar = env.generateVar("input");
        Env inputEnv = env.addBinding("input", inputVar);
        Env aggInputEnv = onMatch(inputEnv, aggregate.source());
        String innerInputVar = aggInputEnv.current().replace("$", "").replace(":*", "").trim();
        Env aggBound = aggInputEnv.addBinding("innerInput", innerInputVar);
        Env aggsEnv = onMatchAggCalls(aggBound, aggregate.aggCalls());
        String aggregationsVar = aggsEnv.generateVar("aggregations");
        Env aggsBindEnv = aggsEnv.addBinding("aggregations", aggregationsVar);
        Env groupEnv = onMatchGroupSet(aggsBindEnv, aggregate.groupSet());
        String groupingPrivateVar = groupEnv.generateVar("groupingPrivate");
        Env gpBindEnv = groupEnv.addBinding("groupingPrivate", groupingPrivateVar);
        Env projEnv = onMatch(gpBindEnv, aggregate.source());
        String projectionsVar = projEnv.generateVar("projections");
        Env projBindEnv = projEnv.addBinding("projections", projectionsVar);
        String passthroughVar = projBindEnv.generateVar("passthrough");
        Env passEnv = projBindEnv.addBinding("passthrough", passthroughVar);
        String neededVar = passEnv.generateVar("needed");
        Env resultEnv = passEnv.addBinding("needed", neededVar);
        String groupByPat = "(GroupBy\n" + "        " + b(innerInputVar) + "\n" + "        " + b(aggregationsVar) + "\n" + "        " + b(groupingPrivateVar) + "\n" + "    )";
        String passCond = b(passthroughVar) + " &\n" + "        (CanPruneAggCols\n" + "            " + r(aggregationsVar) + "\n" + "            " + r(neededVar) + ":(UnionCols\n" + "                (ProjectionOuterCols " + r(projectionsVar) + ")\n" + "                " + r(passthroughVar) + "\n" + "            )\n" + "        )";
        String pattern = "(Project\n" + "    " + r(inputVar) + ":(" + groupByPat.trim() + "\n" + "    " + b(projectionsVar) + "\n" + "    " + passCond + "\n" + ")";
        pattern = "(Project\n" + "    " + r(inputVar) + ":(GroupBy\n" + "        " + b(innerInputVar) + "\n" + "        " + b(aggregationsVar) + "\n" + "        " + b(groupingPrivateVar) + "\n" + "    )\n" + "    " + b(projectionsVar) + "\n" + "    " + b(passthroughVar) + " &\n" + "        (CanPruneAggCols\n" + "            " + r(aggregationsVar) + "\n" + "            " + r(neededVar) + ":(UnionCols\n" + "                (ProjectionOuterCols " + r(projectionsVar) + ")\n" + "                " + r(passthroughVar) + "\n" + "            )\n" + "        )\n" + ")";
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchProjectOverProject(Env env, RelRN.Project project) {
        RelRN.Project innerProject = (RelRN.Project) project.source();
        Env outerProjEnv = onMatch(env, project.map());
        String outerProjPat = outerProjEnv.current();
        Env innerInputEnv = onMatch(outerProjEnv, innerProject.source());
        String innerInputPat = innerInputEnv.current();
        Env innerProjEnv = onMatch(innerInputEnv, innerProject.map());
        String innerProjPat = innerProjEnv.current();
        String innerPassVar = innerProjEnv.generateVar("innerPassthrough");
        Env innerPassEnv = innerProjEnv.addBinding("innerPassthrough", innerPassVar);
        String outerPassVar = innerPassEnv.generateVar("passthrough");
        Env resultEnv = innerPassEnv.addBinding("passthrough", outerPassVar);
        String outerProjRef = outerProjPat.replace(":*", "");
        String innerProjRef = innerProjPat.replace(":*", "");
        String innerProjBlock = "Project\n    " + innerInputPat + "\n    " + innerProjPat + "\n    " + b(innerPassVar);
        String condLine = outerProjPat + " &\n" + "        (CanMergeProjections " + outerProjRef + " " + innerProjRef + ")";
        String pattern = "(Project\n" + "    $input:(" + innerProjBlock + ")\n" + "    " + condLine + "\n" + "    " + b(outerPassVar) + "\n" + ")";
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchProjectGeneral(Env env, RelRN.Project project) {
        Env sourceEnv = onMatch(env, project.source());
        String sourcePat = sourceEnv.current();
        Env projEnv = onMatch(sourceEnv, project.map());
        String projPat = projEnv.current();
        String passthroughVar = projEnv.generateVar("passthrough");
        Env passEnv = projEnv.addBinding("passthrough", passthroughVar);
        String passthroughCond = "";
        if (project.source() instanceof RelRN.Filter) {
            String condVarName = get(sourceEnv, "filterCondVar", null);
            if (condVarName != null) {
                passthroughCond = " & " + filtersBoundBy(condVarName, r(passthroughVar));
            }
        }
        String pattern = N("Project", sourcePat, projPat, b(passthroughVar) + passthroughCond );
        return passEnv.setPattern(pattern).focus(pattern);
    }
    @Override
    public Env onMatchJoin(Env env, RelRN.Join join) {
        if (join.cond() instanceof RexRN.And and && and.sources().size() == 2) {
            boolean hasTrue = and.sources().stream().anyMatch(s -> s instanceof RexRN.True);
            boolean hasFalse = and.sources().stream().anyMatch(s -> s instanceof RexRN.False);
            RexRN other = and.sources().stream() .filter(s -> !(s instanceof RexRN.True) && !(s instanceof RexRN.False)) .findFirst().orElse(null);
            if (hasTrue && other != null) {
                return matchJoinReduceTrue(env, join);
            }
            if (hasFalse && other != null) {
                return matchJoinReduceFalse(env, join);
            }
        }
        Env leftEnv = onMatch(env, join.left());
        String leftPat = leftEnv.current();
        Env rightEnv = onMatch(leftEnv, join.right());
        String rightPat = rightEnv.current();
        Env condEnv = onMatch(rightEnv, join.cond());
        String condPat = condEnv.current();
        String privateVar = condEnv.generateVar("private");
        Env privateEnv = condEnv .addBinding("private_" + System.identityHashCode(join), privateVar) .addBinding("last_private", privateVar);
        if (join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.INNER) {
            String leftVar = privateEnv.generateVar("left");
            String rightVar = privateEnv.generateVar("right");
            String onVar = privateEnv.generateVar("on");
            Env bound = privateEnv .addBinding("left", leftVar) .addBinding("right", rightVar) .addBinding("on", onVar) .addBinding("private", privateVar);
            if (env.rulename.equals("JoinAddRedundantSemiJoin")) {
                return matchJoinAddRedundantSemiJoin(bound, privateVar, leftVar, rightVar);
            }
            if (env.rulename.equals("JoinCommute")) {
                return matchJoinCommute(bound, privateVar, leftVar, rightVar, onVar);
            }
        }
        if (env.rulename.equals("JoinExtractFilter") && join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.INNER && !(join.cond() instanceof RexRN.And)) {
            return matchJoinExtractFilter(privateEnv, join, privateVar);
        }
        if (env.rulename.equals("JoinPushTransitivePredicates") && join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.INNER) {
            String condVarName = extractVar(condPat);
            if (condVarName != null) {
                condPat = condPat + " & ^(IsFilterEmpty " + r(condVarName) + ")";
            }
        }
        String jType = joinType(join.ty().semantics());
        String pattern = N(jType, leftPat, rightPat, condPat, b(privateVar));
        return privateEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchJoinReduceTrue(Env env, RelRN.Join join) {
        Env leftEnv = onMatch(env, join.left());
        Env rightEnv = onMatch(leftEnv, join.right());
        String onVar = rightEnv.generateVar("on");
        Env onEnv = rightEnv.addBinding("on", onVar);
        String itemVar = onEnv.generateVar("item");
        Env itemEnv = onEnv.addBinding("item", itemVar);
        String privateVar = itemEnv.generateVar("private");
        Env resultEnv = itemEnv .addBinding("private_" + System.identityHashCode(join), privateVar) .addBinding("last_private", privateVar) .addBinding("joinReduceTrue", "true");
        String listPat = r(onVar) + ":[\n" + "        ...\n" + "        " + r(itemVar) + ":(FiltersItem (True))\n" + "        ...\n" + "    ]";
        String pattern = N(joinType(join.ty().semantics()), leftEnv.current(), rightEnv.current(), listPat, b(privateVar));
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchJoinReduceFalse(Env env, RelRN.Join join) {
        Env leftEnv = onMatch(env, join.left());
        Env rightEnv = onMatch(leftEnv, join.right());
        String onVar = rightEnv.generateVar("on");
        Env onEnv = rightEnv.addBinding("on", onVar);
        String itemVar = onEnv.generateVar("item");
        Env itemEnv = onEnv.addBinding("item", itemVar);
        String privateVar = itemEnv.generateVar("private");
        Env resultEnv = itemEnv .addBinding("private_" + System.identityHashCode(join), privateVar) .addBinding("last_private", privateVar) .addBinding("joinReduceFalse", "true");
        String listPat = r(onVar) + ":[\n" + "        ...\n" + "        " + r(itemVar) + ":(FiltersItem\n" + "            (And * (False))\n" + "        )\n" + "        ...\n" + "    ] &\n" + "        ^(IsFilterFalse " + r(onVar) + ")";
        String pattern = N(joinType(join.ty().semantics()), leftEnv.current(), rightEnv.current(), listPat, b(privateVar));
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchJoinAddRedundantSemiJoin(Env bound, String privateVar, String leftVar, String rightVar) {
        bound = bound.addBinding("isJoinAddRedundantSemiJoin", "true");
        String filtersVar = bound.generateVar("filters");
        Env resultEnv = bound.addBinding("filters", filtersVar);
        String privLine = b(privateVar) + " & ^(IsRedundantSemiJoin " + r(leftVar) + " " + r(rightVar) + " " + r(filtersVar) + ")";
        String pattern = N("InnerJoin", r(leftVar) + ":^(Values)", b(rightVar), b(filtersVar), privLine );
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchJoinCommute(Env bound, String privateVar, String leftVar, String rightVar, String onVar) {
        bound = bound.addBinding("isJoinCommute", "true");
        String privLine = b(privateVar) + " &\n" + "        (CanCommuteJoin " + r(leftVar) + " " + r(rightVar) + ")";
        String pattern = N("InnerJoin", b(leftVar), b(rightVar), b(onVar), privLine);
        return bound.setPattern(pattern).focus(pattern);
    }
    private Env matchJoinExtractFilter(Env privateEnv, RelRN.Join join, String privateVar) {
        String leftVar = privateEnv.generateVar("left");
        String rightVar = privateEnv.generateVar("right");
        String onVar = privateEnv.generateVar("on");
        Env bound = privateEnv .addBinding("left", leftVar) .addBinding("right", rightVar) .addBinding("on", onVar) .addBinding("private", privateVar) .addBinding("isJoinExtractFilter", "true");
        if (join.cond() instanceof RexRN.Pred pred) {
            bound = bound.addBinding(pred.operator().getName(), onVar);
        }
        String condLine = b(onVar) + " &\n" + "        (CanExtractJoinFilter " + r(leftVar) + " " + r(rightVar) + " " + r(onVar) + ")";
        String pattern = N(joinType(join.ty().semantics()), b(leftVar), b(rightVar), condLine, b(privateVar));
        return bound.setPattern(pattern).focus(pattern);
    }
    @Override
    public Env onMatchJoinWithSeparateConds(Env env, RelRN.JoinWithSeparateConds join) {
        if (join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.INNER && join.cond() instanceof RexRN.And and && and.sources().size() > 2) {
            String leftVar = env.generateVar("left");
            Env leftEnv = env.addBinding("left", leftVar);
            String rightVar = leftEnv.generateVar("right");
            Env rightEnv = leftEnv.addBinding("right", rightVar);
            String onVar = rightEnv.generateVar("on");
            Env onEnv = rightEnv.addBinding("on", onVar);
            String privateVar = onEnv.generateVar("private");
            Env resultEnv = onEnv .addBinding("private", privateVar) .addBinding("isJoinConditionPush", "true");
            String leftLine = b(leftVar) + " & ^(HasOuterCols " + r(leftVar) + ")";
            String rightLine = b(rightVar) + " & ^(HasOuterCols " + r(rightVar) + ")";
            String onLine = b(onVar) + " &\n" + "        (HasBoundConditions\n" + "            " + r(onVar) + "\n" + "            (OutputCols " + r(leftVar) + ")\n" + "            (OutputCols " + r(rightVar) + ")\n" + "        )";
            String pattern = N("InnerJoin", leftLine, rightLine, onLine, b(privateVar));
            return resultEnv.setPattern(pattern).focus(pattern);
        }
        Env leftEnv = onMatch(env, join.left());
        Env rightEnv = onMatch(leftEnv, join.right());
        Env condEnv = onMatch(rightEnv, join.cond());
        String privateVar = condEnv.generateVar("private");
        Env resultEnv = condEnv .addBinding("private_" + System.identityHashCode(join), privateVar) .addBinding("last_private", privateVar);
        String jType = joinType(join.ty().semantics());
        String pattern = N(jType, leftEnv.current(), rightEnv.current(), condEnv.current(), b(privateVar));
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    @Override
    public Env onMatchUnion(Env env, RelRN.Union union) {
        if (union.sources().size() == 2) {
            RelRN l = union.sources().get(0), r = union.sources().get(1);
            if (l instanceof RelRN.Empty && r instanceof RelRN.Empty) {
                return matchUnionBothEmpty(env, union);
            }
        }
        if (union.sources().size() == 2 && union.sources().get(0) instanceof RelRN.Union inner && inner.sources().size() == 2) {
            return matchNestedUnion(env, union);
        }
        if (union.sources().size() == 2 && !union.all()) {
            RelRN ls = union.sources().get(0), rs = union.sources().get(1);
            if (!(ls instanceof RelRN.Union)) {
                return matchDistinctUnion(env, union);
            }
        }
        if (union.all() && union.sources().size() == 2) {
            RelRN ls = union.sources().get(0), rs = union.sources().get(1);
            RelRN.Project leftProj = toProjectIfPossible(ls);
            RelRN.Project rightProj = toProjectIfPossible(rs);
            if (leftProj != null && rightProj != null) {
                return matchUnionPullUpConstants(env, union, leftProj, rightProj);
            }
        }
        return matchUnionGeneral(env, union);
    }
    private static RelRN.Project toProjectIfPossible(RelRN node) {
        if (node instanceof RelRN.Project p) return p;
        if (node instanceof org.qed.RRuleInstances.UnionPullUpConstants.LeftProjectionWithConstants lp)
            return new RelRN.Project(lp.input().field(0), lp.input());
        if (node instanceof org.qed.RRuleInstances.UnionPullUpConstants.RightProjectionWithConstants rp)
            return new RelRN.Project(rp.input().field(0), rp.input());
        return null;
    }
    private Env matchUnionBothEmpty(Env env, RelRN.Union union) {
        String leftVar = env.generateVar("left");
        Env leftEnv = env.addBinding("left", leftVar);
        String rightVar = leftEnv.generateVar("right");
        Env rightEnv = leftEnv.addBinding("right", rightVar);
        String privateVar = rightEnv.generateVar("private");
        Env privEnv = rightEnv.addBinding("private", privateVar);
        String outColsVar = privEnv.generateVar("outCols");
        Env resultEnv = privEnv.addBinding("outCols", outColsVar) .addBinding("hasZeroRows", "true");
        String uType = union.all() ? "UnionAll" : "Union";
        String leftLine = b(leftVar) + " & (HasZeroRows " + r(leftVar) + ")";
        String rightLine = b(rightVar) + " & (HasZeroRows " + r(rightVar) + ")";
        String privLine = r(privateVar) + ":(SetPrivate * * " + r(outColsVar) + ":*)";
        String pattern = N(uType, leftLine, rightLine, privLine);
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchNestedUnion(Env env, RelRN.Union union) {
        String leftLeftVar = env.generateVar("leftLeft");
        Env llEnv = env.addBinding("leftLeft", leftLeftVar);
        String leftRightVar = llEnv.generateVar("leftRight");
        Env lrEnv = llEnv.addBinding("leftRight", leftRightVar);
        String innerPrivateVar = lrEnv.generateVar("innerPrivate");
        Env ipEnv = lrEnv.addBinding("innerPrivate", innerPrivateVar);
        String innerLeftCols = ipEnv.generateVar("innerLeftCols");
        Env ilcEnv = ipEnv.addBinding("innerLeftCols", innerLeftCols);
        String innerRightCols = ilcEnv.generateVar("innerRightCols");
        Env ircEnv = ilcEnv.addBinding("innerRightCols", innerRightCols);
        String innerOutCols = ircEnv.generateVar("innerOutCols");
        Env iocEnv = ircEnv.addBinding("innerOutCols", innerOutCols);
        String leftVar = iocEnv.generateVar("left");
        Env lEnv = iocEnv.addBinding("left", leftVar);
        String rightVar = lEnv.generateVar("right");
        Env rEnv = lEnv.addBinding("right", rightVar);
        String outerPrivate = rEnv.generateVar("outerPrivate");
        Env opEnv = rEnv.addBinding("outerPrivate", outerPrivate);
        String outerRightCols = opEnv.generateVar("outerRightCols");
        Env orcEnv = opEnv.addBinding("outerRightCols", outerRightCols);
        String outerOutCols = orcEnv.generateVar("outerOutCols");
        Env resultEnv = orcEnv .addBinding("outerOutCols", outerOutCols) .addBinding("isUnionMerge", "true");
        String uType = union.all() ? "UnionAll" : "Union";
        String innerSetPriv = r(innerPrivateVar) + ":(SetPrivate " + b(innerLeftCols) + " " + b(innerRightCols) + " " + b(innerOutCols) + ")";
        String innerUnion = "(" + uType + "\n" + "        " + b(leftLeftVar) + "\n" + "        " + b(leftRightVar) + "\n" + "        " + innerSetPriv + "\n" + "    )";
        String outerSetPriv = r(outerPrivate) + ":(SetPrivate * " + b(outerRightCols) + " " + b(outerOutCols) + ")";
        String pattern = N(uType, r(leftVar) + ":(" + innerUnion.trim(), b(rightVar), outerSetPriv );
        pattern = "(" + uType + "\n" + "    " + r(leftVar) + ":(" + uType + "\n" + "        " + b(leftLeftVar) + "\n" + "        " + b(leftRightVar) + "\n" + "        " + r(innerPrivateVar) + ":(SetPrivate " + b(innerLeftCols) + " " + b(innerRightCols) + " " + b(innerOutCols) + ")\n" + "    )\n" + "    " + b(rightVar) + "\n" + "    " + r(outerPrivate) + ":(SetPrivate * " + b(outerRightCols) + " " + b(outerOutCols) + ")\n" + ")";
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchDistinctUnion(Env env, RelRN.Union union) {
        Env leftEnv = onMatch(env, union.sources().get(0));
        String leftVar = leftEnv.generateVar("left");
        Env lBound = leftEnv.addBinding("left", leftVar);
        Env rightEnv = onMatch(lBound, union.sources().get(1));
        String rightVar = rightEnv.generateVar("right");
        Env rBound = rightEnv.addBinding("right", rightVar);
        String privateVar = rBound.generateVar("private");
        Env privEnv = rBound.addBinding("private", privateVar);
        String leftCols = privEnv.generateVar("leftCols");
        Env lcEnv = privEnv.addBinding("leftCols", leftCols);
        String rightCols = lcEnv.generateVar("rightCols");
        Env rcEnv = lcEnv.addBinding("rightCols", rightCols);
        String outCols = rcEnv.generateVar("outCols");
        Env ocEnv = rcEnv.addBinding("outCols", outCols);
        String keyColsVar = ocEnv.generateVar("keyCols");
        Env kcEnv = ocEnv.addBinding("keyCols", keyColsVar);
        String okVar = kcEnv.generateVar("ok");
        Env resultEnv = kcEnv.addBinding("ok", okVar);
        String setPriv = r(privateVar) + ":(SetPrivate " + b(leftCols) + " " + b(rightCols) + " " + b(outCols) + ") &\n" + "        (Let\n" + "            (" + r(keyColsVar) + " " + r(okVar) + "):(CanConvertUnionToDistinctUnionAll\n" + "                " + r(leftCols) + "\n" + "                " + r(rightCols) + "\n" + "            )\n" + "            " + r(okVar) + "\n" + "        )";
        String pattern = N("Union", b(leftVar), b(rightVar), setPriv);
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchUnionPullUpConstants(Env env, RelRN.Union union, RelRN.Project leftProject, RelRN.Project rightProject) {
        Env liEnv = onMatch(env, leftProject.source());
        String leftInputVar = liEnv.generateVar("leftInput");
        Env liBound = liEnv.addBinding("leftInput", leftInputVar);
        String leftProjVar = liBound.generateVar("leftProjections");
        Env lpEnv = liBound.addBinding("leftProjections", leftProjVar);
        String leftPassVar = lpEnv.generateVar("leftPassthrough");
        Env lpassEnv = lpEnv.addBinding("leftPassthrough", leftPassVar);
        String leftVar = lpassEnv.generateVar("left");
        Env lBound = lpassEnv.addBinding("left", leftVar);
        Env riEnv = onMatch(lBound, rightProject.source());
        String rightInputVar = riEnv.generateVar("rightInput");
        Env riBound = riEnv.addBinding("rightInput", rightInputVar);
        String rightProjVar = riBound.generateVar("rightProjections");
        Env rpEnv = riBound.addBinding("rightProjections", rightProjVar);
        String rightPassVar = rpEnv.generateVar("rightPassthrough");
        Env rpassEnv = rpEnv.addBinding("rightPassthrough", rightPassVar);
        String rightVar = rpassEnv.generateVar("right");
        Env rBound = rpassEnv.addBinding("right", rightVar);
        String privateVar = rBound.generateVar("private");
        Env privEnv = rBound.addBinding("private", privateVar);
        String leftCols = privEnv.generateVar("leftCols");
        Env lcEnv = privEnv.addBinding("leftCols", leftCols);
        String rightCols = lcEnv.generateVar("rightCols");
        Env rcEnv = lcEnv.addBinding("rightCols", rightCols);
        String outCols = rcEnv.generateVar("outCols");
        Env resultEnv = rcEnv.addBinding("outCols", outCols);
        String uType = union.all() ? "UnionAll" : "Union";
        String setPriv = r(privateVar) + ":(SetPrivate " + b(leftCols) + " " + b(rightCols) + " " + b(outCols) + ") &\n" + "        (HasMatchingConstantsFromUnion\n" + "            " + r(leftProjVar) + "\n" + "            " + r(rightProjVar) + "\n" + "            " + r(leftCols) + "\n" + "            " + r(rightCols) + "\n" + "            " + r(outCols) + "\n" + "        )";
        String pattern = "(" + uType + "\n" + "    " + r(leftVar) + ":(Project\n" + "        " + b(leftInputVar) + "\n" + "        " + b(leftProjVar) + "\n" + "        " + b(leftPassVar) + "\n" + "    )\n" + "    " + r(rightVar) + ":(Project\n" + "        " + b(rightInputVar) + "\n" + "        " + b(rightProjVar) + "\n" + "        " + b(rightPassVar) + "\n" + "    )\n" + "    " + setPriv + "\n" + ")";
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchUnionGeneral(Env env, RelRN.Union union) {
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
        Env resultEnv = currentEnv.addBinding("union_private", privateVar);
        String uType = union.all() ? "UnionAll" : "Union";
        String pattern = sourcePatterns.size() == 2
            ? N(uType, sourcePatterns.get(0), sourcePatterns.get(1), b(privateVar))
            : buildNestedUnion(uType, sourcePatterns, privateVar + ":*");
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private String buildNestedUnion(String uType, Seq<String> sources, String privatePattern) {
        if (sources.size() == 2) {
            return N(uType, sources.get(0), sources.get(1), "$" + privatePattern);
        }
        return N(uType, sources.get(0), buildNestedUnion(uType, sources.drop(1), privatePattern), "$" + privatePattern );
    }
    @Override
    public Env onMatchIntersect(Env env, RelRN.Intersect intersect) {
        if (intersect.sources().size() == 2 && intersect.sources().get(0) instanceof RelRN.Intersect inner && inner.sources().size() == 2) {
            return matchNestedIntersect(env, intersect);
        }
        if (intersect.sources().size() == 2 && intersect.sources().get(1) instanceof RelRN.Empty) {
            String leftVar = env.generateVar("left");
            String rightVar = env.generateVar("right");
            String iType = intersect.all() ? "IntersectAll" : "Intersect";
            String pattern = "(" + iType + "\n" + "    " + b(leftVar) + "\n" + "    " + b(rightVar) + " & (HasZeroRows " + r(rightVar) + ")\n" + ")";
            return env.addBinding("isPruneEmptyIntersect", "true") .addBinding("pruneEmptyLeft", leftVar) .setPattern(pattern).focus(pattern);
        }
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
        Env resultEnv = currentEnv.addBinding("intersect_private", privateVar);
        String iType = intersect.all() ? "IntersectAll" : "Intersect";
        String pattern = sourcePatterns.size() == 2
            ? N(iType, sourcePatterns.get(0), sourcePatterns.get(1), b(privateVar))
            : buildNestedIntersect(iType, sourcePatterns, privateVar + ":*");
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchNestedIntersect(Env env, RelRN.Intersect intersect) {
        String leftLeftVar = env.generateVar("leftLeft");
        Env llEnv = env.addBinding("leftLeft", leftLeftVar);
        String leftRightVar = llEnv.generateVar("leftRight");
        Env lrEnv = llEnv.addBinding("leftRight", leftRightVar);
        String innerPrivate = lrEnv.generateVar("innerPrivate");
        Env ipEnv = lrEnv.addBinding("innerPrivate", innerPrivate);
        String innerLeftCols = ipEnv.generateVar("innerLeftCols");
        Env ilcEnv = ipEnv.addBinding("innerLeftCols", innerLeftCols);
        String innerRightCols = ilcEnv.generateVar("innerRightCols");
        Env ircEnv = ilcEnv.addBinding("innerRightCols", innerRightCols);
        String leftVar = ircEnv.generateVar("left");
        Env lEnv = ircEnv.addBinding("left", leftVar);
        String rightVar = lEnv.generateVar("right");
        Env rEnv = lEnv.addBinding("right", rightVar);
        String outerPrivate = rEnv.generateVar("outerPrivate");
        Env opEnv = rEnv.addBinding("outerPrivate", outerPrivate);
        String outerRightCols = opEnv.generateVar("outerRightCols");
        Env orcEnv = opEnv.addBinding("outerRightCols", outerRightCols);
        String outerOutCols = orcEnv.generateVar("outerOutCols");
        Env resultEnv = orcEnv .addBinding("outerOutCols", outerOutCols) .addBinding("isIntersectMerge", "true");
        String iType = intersect.all() ? "IntersectAll" : "Intersect";
        String innerSetPriv = r(innerPrivate) + ":(SetPrivate " + b(innerLeftCols) + " " + b(innerRightCols) + " *)";
        String outerSetPriv = r(outerPrivate) + ":(SetPrivate * " + b(outerRightCols) + " " + b(outerOutCols) + ")";
        String pattern = "(" + iType + "\n" + "    " + r(leftVar) + ":(" + iType + "\n" + "        " + b(leftLeftVar) + "\n" + "        " + b(leftRightVar) + "\n" + "        " + innerSetPriv + "\n" + "    )\n" + "    " + b(rightVar) + "\n" + "    " + outerSetPriv + "\n" + ")";
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private String buildNestedIntersect(String iType, Seq<String> sources, String privatePattern) {
        if (sources.size() == 2) {
            return N(iType, sources.get(0), sources.get(1), "$" + privatePattern);
        }
        return N(iType, sources.get(0), buildNestedIntersect(iType, sources.drop(1), privatePattern), "$" + privatePattern );
    }
    @Override
    public Env onMatchMinus(Env env, RelRN.Minus minus) {
        if (env.rulename.equals("MinusMerge") && minus.sources().size() == 2 && minus.sources().get(0) instanceof RelRN.Minus) {
            return matchMinusMerge(env, minus);
        }
        if (minus.sources().size() == 2 && minus.sources().get(0) instanceof RelRN.Empty) {
            String leftVar = env.generateVar("left");
            String rightVar = "right";
            String pattern = "(Except\n" + "    " + b(leftVar) + " & (HasZeroRows " + r(leftVar) + ")\n" + "    " + r(rightVar) + ":*\n" + ")";
            return env.addBinding("isPruneEmptyMinus", "true") .addBinding("pruneEmptyLeft", leftVar) .addBinding("right", rightVar) .setPattern(pattern).focus(pattern);
        }
        Env currentEnv = env;
        Seq<String> sourcePatterns = Seq.empty();
        for (RelRN source : minus.sources()) {
            Env sourceEnv = onMatch(currentEnv, source);
            sourcePatterns = sourcePatterns.appended(sourceEnv.current());
            currentEnv = sourceEnv;
        }
        String privateVar = currentEnv.generateVar("private");
        Env resultEnv = currentEnv.addBinding("minus_private", privateVar);
        String pattern;
        if (minus.sources().size() == 2) {
            pattern = N("Except", sourcePatterns.get(0), sourcePatterns.get(1), b(privateVar));
        } else {
            pattern = "(Except\n    " + sourcePatterns.joinToString("\n    ") + "\n    " + b(privateVar) + "\n)";
        }
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchMinusMerge(Env env, RelRN.Minus minus) {
        String leftVar = env.generateVar("left");
        Env lEnv = env.addBinding("left", leftVar);
        String leftLeftVar = lEnv.generateVar("leftLeft");
        Env llEnv = lEnv.addBinding("leftLeft", leftLeftVar);
        String leftRightVar = llEnv.generateVar("leftRight");
        Env lrEnv = llEnv.addBinding("leftRight", leftRightVar);
        String innerPriv = lrEnv.generateVar("innerPrivate");
        Env ipEnv = lrEnv.addBinding("innerPrivate", innerPriv);
        String rightVar = ipEnv.generateVar("right");
        Env rEnv = ipEnv.addBinding("right", rightVar);
        String outerPriv = rEnv.generateVar("outerPrivate");
        Env resultEnv = rEnv .addBinding("outerPrivate", outerPriv) .addBinding("isMinusMerge", "true");
        String pattern = "(Except\n" + "    " + r(leftVar) + ":(Except\n" + "        " + b(leftLeftVar) + "\n" + "        " + b(leftRightVar) + "\n" + "        " + b(innerPriv) + "\n" + "    )\n" + "    " + b(rightVar) + "\n" + "    " + b(outerPriv) + "\n" + ")";
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    @Override
    public Env onMatchAggregate(Env env, RelRN.Aggregate aggregate) {
        if (aggregate.source() instanceof RelRN.Join topJoin && topJoin.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.LEFT && topJoin.left() instanceof RelRN.Join bottomJoin && bottomJoin.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.LEFT) {
            return matchAggregateDoubleJoin(env);
        }
        if (aggregate.source() instanceof RelRN.Join j && j.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.LEFT) {
            return matchAggregateLeftJoin(env, aggregate);
        }
        if (aggregate.source() instanceof RelRN.Project) {
            return matchAggregateProjectSource(env, aggregate);
        }
        Env sourceEnv = onMatch(env, aggregate.source());
        String sourcePat = sourceEnv.current();
        Env aggsEnv = onMatchAggCalls(sourceEnv, aggregate.aggCalls());
        String aggsPat = aggsEnv.current();
        Env groupEnv = onMatchGroupSet(aggsEnv, aggregate.groupSet());
        String privateVar = groupEnv.generateVar("private");
        Env privEnv = groupEnv.addBinding("aggregate_private", privateVar);
        String aggType = determineAggregateType(aggregate);
        if (hasProjectionExpressionsInAggregate(aggregate)) {
            String inputVar = privEnv.generateVar("input");
            Env inputEnv = privEnv.addBinding("input", inputVar);
            String aggregationsVar = inputEnv.generateVar("aggregations");
            Env aggsBindEnv = inputEnv.addBinding("aggregations", aggregationsVar);
            String groupingPriv = aggsBindEnv.generateVar("groupingPrivate");
            Env gpEnv = aggsBindEnv.addBinding("groupingPrivate", groupingPriv);
            String condLine = b(aggregationsVar) + " & (CanExtractProjectFromAggregate " + r(aggregationsVar) + ")";
            String pattern = N(aggType, b(inputVar), condLine, b(groupingPriv));
            return gpEnv.addBinding("isAggregateExtractProject", "true") .setPattern(pattern).focus(pattern);
        }
        String filterBoundBy = "";
        if (aggregate.source() instanceof RelRN.Filter) {
            String condVarName = get(sourceEnv, "filterCondVar", null);
            if (condVarName != null) {
                filterBoundBy = " & " + filtersBoundBy(condVarName, "(GroupingCols " + r(privateVar) + ")");
            }
        }
        String pattern = N(aggType, sourcePat, aggsPat, b(privateVar) + filterBoundBy);
        return privEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchAggregateDoubleJoin(Env env) {
        String leftVar = env.generateVar("left");
        Env lEnv = env.addBinding("left", leftVar);
        String middleVar = lEnv.generateVar("middle");
        Env mEnv = lEnv.addBinding("middle", middleVar);
        String rightVar = mEnv.generateVar("right");
        Env rEnv = mEnv.addBinding("right", rightVar);
        String rightFiltersVar = rEnv.generateVar("rightFilters");
        Env rfEnv = rEnv.addBinding("rightFilters", rightFiltersVar);
        String aggregationsVar = rfEnv.generateVar("aggregations");
        Env aggsEnv = rfEnv.addBinding("aggregations", aggregationsVar);
        String groupingPrivVar = aggsEnv.generateVar("groupingPrivate");
        Env gpEnv = aggsEnv.addBinding("groupingPrivate", groupingPrivVar);
        String groupingColsVar = gpEnv.generateVar("groupingCols");
        Env gcEnv = gpEnv.addBinding("groupingCols", groupingColsVar);
        String orderingVar = gcEnv.generateVar("ordering");
        Env resultEnv = gcEnv.addBinding("ordering", orderingVar);
        String pattern = "(DistinctOn\n" + "    (LeftJoin\n" + "        (LeftJoin\n" + "            " + b(leftVar) + "\n" + "            " + b(middleVar) + "\n" + "            *\n" + "        )\n" + "        " + b(rightVar) + "\n" + "        " + b(rightFiltersVar) + "\n" + "    )\n" + "    " + r(aggregationsVar) + ":[]\n" + "    " + r(groupingPrivVar) + ":(GroupingPrivate " + b(groupingColsVar) + " " + b(orderingVar) + ") &\n" + "        (ColsAreEmpty\n" + "            (IntersectionCols\n" + "                (OutputCols " + r(middleVar) + ")\n" + "                (UnionCols\n" + "                    (FilterOuterCols " + r(rightFiltersVar) + ")\n" + "                    " + r(groupingColsVar) + "\n" + "                )\n" + "            )\n" + "        ) &\n" + "        (OrderingCanProjectCols\n" + "            " + r(orderingVar) + "\n" + "            (UnionCols (OutputCols " + r(leftVar) + ") (OutputCols " + r(rightVar) + "))\n" + "        )\n" + ")";
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchAggregateLeftJoin(Env env, RelRN.Aggregate aggregate) {
        String leftVar = env.generateVar("left");
        Env lEnv = env.addBinding("left", leftVar);
        String aggsVar = lEnv.generateVar("aggregations");
        Env aggsEnv = lEnv.addBinding("aggregations", aggsVar);
        String groupingPrivVar = aggsEnv.generateVar("groupingPrivate");
        Env gpEnv = aggsEnv.addBinding("groupingPrivate", groupingPrivVar);
        String groupingColsVar = gpEnv.generateVar("groupingCols");
        Env gcEnv = gpEnv.addBinding("groupingCols", groupingColsVar);
        String orderingVar = gcEnv.generateVar("ordering");
        Env ordEnv = gcEnv.addBinding("ordering", orderingVar);
        String leftColsVar = ordEnv.generateVar("leftCols");
        Env resultEnv = ordEnv.addBinding("leftCols", leftColsVar);
        String pattern = "(DistinctOn\n" + "    (LeftJoin\n" + "        " + b(leftVar) + "\n" + "        *\n" + "        *\n" + "    )\n" + "    " + r(aggsVar) + ":[]\n" + "    " + r(groupingPrivVar) + ":(GroupingPrivate " + b(groupingColsVar) + " " + b(orderingVar) + ") &\n" + "        (ColsAreSubset\n" + "            " + r(groupingColsVar) + "\n" + "            " + r(leftColsVar) + ":(OutputCols " + r(leftVar) + ")\n" + "        ) &\n" + "        (OrderingCanProjectCols\n" + "            " + r(orderingVar) + "\n" + "            " + r(leftColsVar) + "\n" + "        )\n" + ")";
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private Env matchAggregateProjectSource(Env env, RelRN.Aggregate aggregate) {
        String aggType = determineAggregateType(aggregate);
        if (env.rulename.equals("AggregateProjectConstantToDummyJoin")) {
            String inputVar = env.generateVar("input");
            Env inputEnv = env.addBinding("input", inputVar);
            String aggsVar = inputEnv.generateVar("aggregations");
            Env aggsEnv = inputEnv.addBinding("aggregations", aggsVar);
            String gpVar = aggsEnv.generateVar("groupingPrivate");
            Env resultEnv = aggsEnv.addBinding("groupingPrivate", gpVar);
            String gpLine = b(gpVar) + " & (HasConstantGroupingCols " + r(inputVar) + " " + r(gpVar) + ")";
            String pattern = N(aggType, r(inputVar) + ":(Project * * *)", b(aggsVar), gpLine);
            return resultEnv.setPattern(pattern).focus(pattern);
        }
        if (env.rulename.equals("AggregateProjectMerge")) {
            String inputVar = env.generateVar("input");
            Env inputEnv = env.addBinding("input", inputVar);
            String sourceVar = inputEnv.generateVar("input");
            Env sBound = inputEnv.addBinding("source", sourceVar);
            String aggsVar = sBound.generateVar("aggregations");
            Env aggsEnv = sBound.addBinding("aggregations", aggsVar);
            String gpVar = aggsEnv.generateVar("groupingPrivate");
            Env resultEnv = aggsEnv.addBinding("groupingPrivate", gpVar);
            String innerProject = "(Project\n" + "        " + b(sourceVar) + "\n" + "        *\n" + "        *\n" + "    )";
            String gpLine = b(gpVar) + " & (CanMergeProjectIntoAggregate " + r(inputVar) + " " + r(gpVar) + ")";
            String pattern = "(" + aggType + "\n" + "    " + r(inputVar) + ":(" + innerProject.trim() + "\n" + "    " + b(aggsVar) + "\n" + "    " + gpLine + "\n" + ")";
            pattern = "(" + aggType + "\n" + "    " + r(inputVar) + ":(Project\n" + "        " + b(sourceVar) + "\n" + "        *\n" + "        *\n" + "    )\n" + "    " + b(aggsVar) + "\n" + "    " + gpLine + "\n" + ")";
            return resultEnv.setPattern(pattern).focus(pattern);
        }
        String inputVar = env.generateVar("input");
        Env inputEnv = env.addBinding("input", inputVar);
        String projectionsVar = inputEnv.generateVar("projections");
        Env projEnv = inputEnv.addBinding("projections", projectionsVar);
        String passthroughVar = projEnv.generateVar("passthrough");
        Env passEnv = projEnv.addBinding("passthrough", passthroughVar);
        String aggsVar = passEnv.generateVar("aggregations");
        Env aggsEnv = passEnv.addBinding("aggregations", aggsVar);
        String gpVar = aggsEnv.generateVar("groupingPrivate");
        Env resultEnv = aggsEnv.addBinding("groupingPrivate", gpVar);
        String gpLine = b(gpVar) + " & (CanRemapGroupingColsThroughProject " + r(gpVar) + " " + r(projectionsVar) + " " + r(passthroughVar) + ")";
        String innerProject = "(Project\n" + "        " + b(inputVar) + "\n" + "        " + b(projectionsVar) + "\n" + "        " + b(passthroughVar) + "\n" + "    )";
        String pattern = "(" + aggType + "\n" + "    " + innerProject + "\n" + "    " + b(aggsVar) + "\n" + "    " + gpLine + "\n" + ")";
        return resultEnv.setPattern(pattern).focus(pattern);
    }
    private Env onMatchAggCalls(Env env, Seq<RelRN.AggCall> aggCalls) {
        Env currentEnv = env;
        Seq<String> aggPatterns = Seq.empty();
        boolean hasProjOp = false;
        for (RelRN.AggCall aggCall : aggCalls) {
            if (aggCall.operands().size() == 1 && aggCall.operands().get(0) instanceof RexRN.Proj proj) {
                String projVar = currentEnv.bindings().getOrDefault(proj.operator().getName(), null);
                if (projVar != null) {
                    aggPatterns = aggPatterns.appended(b(projVar));
                    hasProjOp = true;
                    continue;
                }
            }
            String aggVar = currentEnv.generateVar("agg");
            currentEnv = currentEnv.addBinding(aggCall.name(), aggVar);
            aggPatterns = aggPatterns.appended(b(aggVar));
        }
        String pattern;
        if (aggCalls.size() == 1 && hasProjOp) {
            pattern = aggPatterns.get(0);
        } else if (aggCalls.size() == 1) {
            String aggVar = currentEnv.generateVar("aggregations");
            currentEnv = currentEnv.addBinding("aggregations", aggVar);
            pattern = b(aggVar);
        } else {
            pattern = "[" + aggPatterns.joinToString(" ") + "]";
        }
        return currentEnv.setPattern(pattern).focus(pattern);
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
    private static String determineAggregateType(RelRN.Aggregate aggregate) {
        return "GroupBy";
    }
    private static boolean hasProjectionExpressionsInAggregate(RelRN.Aggregate aggregate) {
        for (RexRN g : aggregate.groupSet()) {
            if (g instanceof RexRN.Proj) return true;
        }
        for (RelRN.AggCall c : aggregate.aggCalls()) {
            for (RexRN op : c.operands()) {
                if (op instanceof RexRN.Proj) return true;
            }
        }
        return false;
    }
    @Override
    public Env onMatchEmpty(Env env, RelRN.Empty empty) {
        String var = env.generateVar("empty");
        return env.addBinding("empty", var).focus(r(var) + ":(Values)");
    }
    @Override
    public Env onMatchField(Env env, RexRN.Field field) {
        String var = env.generateVar("field");
        return env.addBinding("field_" + field.ordinal(), var).focus(b(var));
    }
    @Override
    public Env onMatchPred(Env env, RexRN.Pred pred) {
        String var = env.generateVar("cond");
        return env.addBinding(pred.operator().getName(), var).focus(b(var));
    }
    @Override
    public Env onMatchProj(Env env, RexRN.Proj proj) {
        String var = env.generateVar("proj");
        return env.addBinding(proj.operator().getName(), var).focus(b(var));
    }
    public Env onMatchGroupBy(Env env, RexRN.GroupBy groupBy) {
        if (groupBy.sources().size() == 1 && groupBy.sources().get(0) instanceof RexRN.Proj proj) {
            String projVar = env.bindings().getOrDefault(proj.operator().getName(), null);
            if (projVar != null) return env.focus(b(projVar));
        }
        String var = env.generateVar("groupBy");
        return env.addBinding(groupBy.operator().getName(), var).focus(b(var));
    }
    @Override
    public Env onMatchAnd(Env env, RexRN.And and) {
        Env currentEnv = env;
        Seq<String> operandPats = Seq.empty();
        for (RexRN op : and.sources()) {
            Env opEnv = onMatch(currentEnv, op);
            operandPats = operandPats.appended(opEnv.current());
            currentEnv = opEnv;
        }
        String pattern = buildNestedAnd(operandPats);
        return currentEnv.setPattern(pattern).focus(pattern);
    }
    private static String buildNestedAnd(Seq<String> operands) {
        if (operands.isEmpty()) return "(And)";
        if (operands.size() == 1) return operands.get(0);
        return "(And " + operands.get(0) + " " + buildNestedAnd(operands.drop(1)) + ")";
    }
    @Override
    public Env onMatchTrue(Env env, RexRN literal) {
        String var = env.generateVar("true");
        return env.addBinding("true_" + System.identityHashCode(literal), var) .focus(r(var) + ":True") .setPattern(r(var) + ":True");
    }
    @Override
    public Env onMatchFalse(Env env, RexRN literal) {
        return env.focus("(False)").setPattern("(False)");
    }
    @Override
    public Env onMatchCustom(Env env, RelRN custom) {
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin .AggregateGroupingByConstants aggGrouping) {
            if (aggGrouping.input() instanceof org.qed.RRuleInstances .AggregateProjectConstantToDummyJoin.ProjectWithConstantLiterals) {
                String inputVar = env.generateVar("input");
                Env inputEnv = env.addBinding("input", inputVar);
                String aggsVar = inputEnv.generateVar("aggregations");
                Env aggsEnv = inputEnv.addBinding("aggregations", aggsVar);
                String gpVar = aggsEnv.generateVar("groupingPrivate");
                Env resultEnv = aggsEnv.addBinding("groupingPrivate", gpVar);
                String gpLine = b(gpVar) + " & (HasConstantGroupingCols " + r(inputVar) + " " + r(gpVar) + ")";
                String pattern = N("GroupBy", r(inputVar) + ":(Project * * *)", b(aggsVar), gpLine);
                return resultEnv.setPattern(pattern).focus(pattern);
            }
            return onMatch(env, aggGrouping.input());
        }
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin .ProjectWithConstantLiterals projectWithConstants) {
            if (projectWithConstants.input() instanceof org.qed.RRuleInstances .AggregateProjectConstantToDummyJoin.SourceTable) {
                return onMatchScan(env, new RelRN.Scan("Source", org.qed.RexRN.varType("Source_Type", true), false));
            }
            return onMatch(env, projectWithConstants.input());
        }
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin .SourceTable) {
            return onMatchScan(env, new RelRN.Scan("Source", org.qed.RexRN.varType("Source_Type", true), false));
        }
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge .ProjectUsingSubsetOfAggregates pusa) {
            if (pusa.input() instanceof org.qed.RRuleInstances.ProjectAggregateMerge .AggregateWithMultipleCalls amc) {
                Env srcEnv = onMatch(env, amc.input());
                String aggInputVar = srcEnv.current().replace("$", "").replace(":*", "").trim();
                Env aiBound = srcEnv.addBinding("aggInput", aggInputVar);
                String aggsVar = aiBound.generateVar("aggregations");
                Env aggsEnv = aiBound.addBinding("aggregations", aggsVar);
                String gpVar = aggsEnv.generateVar("groupingPrivate");
                Env gpEnv = aggsEnv.addBinding("groupingPrivate", gpVar);
                String inputVar = gpEnv.generateVar("input");
                Env inputEnv = gpEnv.addBinding("input", inputVar);
                String projVar = inputEnv.generateVar("projections");
                Env projEnv = inputEnv.addBinding("projections", projVar);
                String passVar = projEnv.generateVar("passthrough");
                Env passEnv = projEnv.addBinding("passthrough", passVar);
                String neededVar = passEnv.generateVar("needed");
                Env resultEnv = passEnv.addBinding("needed", neededVar);
                String passCond = b(passVar) + " &\n" + "        (CanPruneAggCols\n" + "            " + r(aggsVar) + "\n" + "            " + r(neededVar) + ":(UnionCols\n" + "                (ProjectionOuterCols " + r(projVar) + ")\n" + "                " + r(passVar) + "\n" + "            )\n" + "        )";
                String pattern = "(Project\n" + "    " + r(inputVar) + ":(GroupBy\n" + "        " + b(aggInputVar) + "\n" + "        " + b(aggsVar) + "\n" + "        " + b(gpVar) + "\n" + "    )\n" + "    " + b(projVar) + "\n" + "    " + passCond + "\n" + ")";
                return resultEnv.setPattern(pattern).focus(pattern);
            }
            return onMatch(env, pusa.input());
        }
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge .AggregateWithMultipleCalls amc) {
            return onMatch(env, amc.input());
        }
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge.SourceTable) {
            return onMatchScan(env, new RelRN.Scan("Source", org.qed.RexRN.varType("Source_Type", true), false));
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants .UnionWithConstantColumns uwcc) {
            return onMatchUnion(env, new RelRN.Union(true, Seq.of(uwcc.left(), uwcc.right())));
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants .LeftProjectionWithConstants lp) {
            return onMatchProject(env, new RelRN.Project(lp.input().field(0), lp.input()));
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants .RightProjectionWithConstants rp) {
            return onMatchProject(env, new RelRN.Project(rp.input().field(0), rp.input()));
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.SourceTable) {
            return onMatchScan(env, new RelRN.Scan("Source", org.qed.RexRN.varType("Source_Type", true), false));
        }
        if (custom instanceof org.qed.RRuleInstances.UnionToDistinct.DistinctUnion du) {
            return onMatchUnion(env, new RelRN.Union(false, Seq.of(du.left(), du.right())));
        }
        if (custom instanceof org.qed.RRuleInstances.UnionToDistinct.UnionAll ua) {
            return onMatchUnion(env, new RelRN.Union(true, Seq.of(ua.left(), ua.right())));
        }
        return unimplementedOnMatch(env, custom);
    }
    @Override
    public Env onMatchCustom(Env env, RexRN custom) {
        if (custom instanceof RexRN.GroupBy groupBy) return onMatchGroupBy(env, groupBy);
        return unimplementedOnMatch(env, custom);
    }
    @Override
    public Env transformScan(Env env, RelRN.Scan scan) {
        String var = get(env, scan.name(), "input");
        String pattern = r(var);
        return env.setPattern(pattern).focus(pattern);
    }
    @Override
    public Env transformFilter(Env env, RelRN.Filter filter) {
        if (flag(env, "isFilterSetOpTranspose") && filter.source() instanceof RelRN.Union && env.bindings().containsKey("left") && env.bindings().containsKey("right") && env.bindings().containsKey("colmap") && env.bindings().containsKey("filter") && env.bindings().containsKey("item")) {
            return transformFilterSetOpTranspose(env);
        }
        if (env.rulename.equals("JoinExtractFilter") && filter.source() instanceof RelRN.Join && env.bindings().containsKey("left") && env.bindings().containsKey("right") && env.bindings().containsKey("on") && env.bindings().containsKey("private")) {
            String pattern = N("ConstructJoinExtractFilterResult", r(get(env, "left")), r(get(env, "right")), r(get(env, "on")), r(get(env, "private")) );
            return env.setPattern(pattern).focus(pattern);
        }
        if (flag(env, "isPruneEmptyFilter")) {
            String pattern = r(get(env, "pruneEmptyInput"));
            return env.setPattern(pattern).focus(pattern);
        }
        if (filter.cond() instanceof RexRN.True) {
            return transform(env, filter.source());
        }
        if (filter.source() instanceof RelRN.Empty) {
            return transform(env, filter.source());
        }
        if (filter.cond() instanceof RexRN.False) {
            Env srcEnv = transform(env, filter.source());
            String pattern = "(ConstructEmptyValues (OutputCols " + srcEnv.current() + "))";
            return srcEnv.setPattern(pattern).focus(pattern);
        }
        Env srcEnv = transform(env, filter.source());
        Env condEnv = transform(srcEnv, filter.cond());
        String condPat = condEnv.current();
        String filterPat;
        if (condPat.startsWith("(ConcatFilters") || (condPat.startsWith("$") && !condPat.contains(" "))) {
            filterPat = condPat;
        } else {
            filterPat = "[" + condPat + "]";
        }
        String pattern = N("Select", srcEnv.current(), filterPat);
        return condEnv.setPattern(pattern).focus(pattern);
    }
    private Env transformFilterSetOpTranspose(Env env) {
        String leftVar = get(env, "left");
        String rightVar = get(env, "right");
        String colmapVar = get(env, "colmap");
        String filterVar = get(env, "filter");
        String itemVar = get(env, "item");
        String leftSelect = "(Select\n" + "            " + r(leftVar) + "\n" + "            [ (FiltersItem (MapSetOpFilterLeft " + r(itemVar) + " " + r(colmapVar) + ")) ]\n" + "        )";
        String rightSelect = "(Select\n" + "            " + r(rightVar) + "\n" + "            [ (FiltersItem (MapSetOpFilterRight " + r(itemVar) + " " + r(colmapVar) + ")) ]\n" + "        )";
        String innerUnion = "(Union\n" + "        " + leftSelect + "\n" + "        " + rightSelect + "\n" + "        " + r(colmapVar) + "\n" + "    )";
        String pattern = N("Select", innerUnion, "(RemoveFiltersItem " + r(filterVar) + " " + r(itemVar) + ")" );
        return env.setPattern(pattern).focus(pattern);
    }
    @Override
    public Env transformProject(Env env, RelRN.Project project) {
        if (env.bindings().containsKey("input") && env.bindings().containsKey("cond") && env.bindings().containsKey("proj") && env.bindings().containsKey("passthrough")) {
            String inputVar = get(env, "input");
            String condVar = get(env, "cond");
            String projVar = get(env, "proj");
            String passthroughVar = get(env, "passthrough");
            String innerSelect = "(Select\n" + "    " + r(inputVar) + "\n" + "    " + r(condVar) + "\n" + ")";
            String pattern = N("Project", innerSelect, r(projVar), r(passthroughVar));
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.rulename.equals("ProjectMerge") && env.bindings().containsKey("innerPassthrough") && env.bindings().containsKey("passthrough")) {
            String innerPassVar = get(env, "innerPassthrough");
            String outerPassVar = get(env, "passthrough");
            java.util.List<String> vars = new java.util.ArrayList<>();
            java.util.regex.Matcher m = java.util.regex.Pattern .compile("\\$([a-zA-Z_][a-zA-Z0-9_]*):\\*").matcher(env.pattern());
            while (m.find()) vars.add(m.group(1));
            String input1Var = vars.size() > 0 ? vars.get(0) : "input_1";
            String proj2Var = vars.size() > 1 ? vars.get(1) : "proj_2";
            String proj0Var = vars.size() > 3 ? vars.get(3) : "proj_0";
            String pattern = "(Project\n" + "    " + r(input1Var) + "\n" + "    (MergeProjections\n" + "        " + r(proj0Var) + "\n" + "        " + r(proj2Var) + "\n" + "        " + r(outerPassVar) + "\n" + "    )\n" + "    (DifferenceCols\n" + "        " + r(innerPassVar) + "\n" + "        (ProjectionCols " + r(proj2Var) + ")\n" + "    )\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        Env srcEnv = transform(env, project.source());
        Env projEnv = transform(srcEnv, project.map());
        String passVar = get(projEnv, "passthrough", "passthrough");
        String pattern = N("Project", srcEnv.current(), projEnv.current(), r(passVar));
        return projEnv.setPattern(pattern).focus(pattern);
    }
    private static String findFirstProjVar(Env env) {
        for (var e : env.bindings().asJava().entrySet()) {
            String k = e.getKey(), v = e.getValue();
            if ((k.startsWith("proj") || k.equals("proj")) && !k.contains("innerPass") && !k.contains("pass")) {
                try {
                    int n = Integer.parseInt(v.substring(v.lastIndexOf('_') + 1));
                    if (n < 3) return v;
                } catch (NumberFormatException ignored) {}
            }
        }
        return "proj_0";
    }
    private static String findSecondProjVar(Env env) {
        for (var e : env.bindings().asJava().entrySet()) {
            String k = e.getKey(), v = e.getValue();
            if ((k.startsWith("proj") || k.equals("proj")) && !k.contains("innerPass") && !k.contains("pass")) {
                try {
                    int n = Integer.parseInt(v.substring(v.lastIndexOf('_') + 1));
                    if (n >= 2) return v;
                } catch (NumberFormatException ignored) {}
            }
        }
        return "proj_2";
    }
    private static String findInputVar(Env env) {
        for (var e : env.bindings().asJava().entrySet()) {
            String k = e.getKey(), v = e.getValue();
            if (k.equals("input") || k.startsWith("input_")) {
                return v;
            }
        }
        return "input_1";
    }
    @Override
    public Env transformJoin(Env env, RelRN.Join join) {
        String jType = joinType(join.ty().semantics());
        String privateVar = get(env, "private_" + System.identityHashCode(join), get(env, "last_private", "private"));
        if (flag(env, "joinReduceTrue")) {
            Env leftEnv = transform(env, join.left());
            Env rightEnv = transform(leftEnv, join.right());
            String onVar = get(rightEnv, "on");
            String itemVar = get(rightEnv, "item");
            String pattern = N(jType, leftEnv.current(), rightEnv.current(), "(RemoveFiltersItem " + r(onVar) + " " + r(itemVar) + ")", r(privateVar) );
            return rightEnv.setPattern(pattern).focus(pattern);
        }
        if (flag(env, "joinReduceFalse")) {
            Env leftEnv = transform(env, join.left());
            Env rightEnv = transform(leftEnv, join.right());
            String pattern = N(jType, leftEnv.current(), rightEnv.current(), "[ (FiltersItem (False)) ]", r(privateVar) );
            return rightEnv.setPattern(pattern).focus(pattern);
        }
        if (flag(env, "isJoinAddRedundantSemiJoin") && env.bindings().containsKey("left") && env.bindings().containsKey("right") && env.bindings().containsKey("filters") && env.bindings().containsKey("private")) {
            String leftVar = get(env, "left");
            String rightVar = get(env, "right");
            String filtersVar = get(env, "filters");
            String privVar = get(env, "private");
            String semiJoin = "(SemiJoin\n" + "        " + r(leftVar) + "\n" + "        " + r(rightVar) + "\n" + "        " + r(filtersVar) + "\n" + "        (EmptyJoinPrivate)\n" + "    )";
            String pattern = N("InnerJoin", semiJoin, r(rightVar), r(filtersVar), r(privVar) );
            return env.setPattern(pattern).focus(pattern);
        }
        if (flag(env, "isJoinCommute")) {
            String leftVar = get(env, "left");
            String rightVar = get(env, "right");
            String onVar = get(env, "on");
            String privVar = get(env, "private");
            String innerJoin = "(InnerJoin\n" + "        " + r(rightVar) + "\n" + "        " + r(leftVar) + "\n" + "        " + r(onVar) + "\n" + "        (CommuteJoinFlags " + r(privVar) + ")\n" + "    )";
            String swapCols = "(SwapJoinOutputColumns\n" + "        (OutputCols " + r(leftVar) + ")\n" + "        (OutputCols " + r(rightVar) + ")\n" + "    )";
            String pattern = N("Project", innerJoin, swapCols, "(MakeEmptyColSet)");
            return env.setPattern(pattern).focus(pattern);
        }
        if (flag(env, "isJoinConditionPush") && join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.INNER && env.bindings().containsKey("left") && env.bindings().containsKey("right") && env.bindings().containsKey("on") && env.bindings().containsKey("private") && !flag(env, "joinReduceTrue") && !flag(env, "joinReduceFalse") && !flag(env, "isJoinCommute") && !flag(env, "isJoinExtractFilter")) {
            String leftVar = get(env, "left");
            String rightVar = get(env, "right");
            String onVar = get(env, "on");
            String privVar = get(env, "private");
            String leftSel = "(Select " + r(leftVar) + " (ExtractBoundConditions " + r(onVar) + " (OutputCols " + r(leftVar) + ")))";
            String rightSel = "(Select " + r(rightVar) + " (ExtractBoundConditions " + r(onVar) + " (OutputCols " + r(rightVar) + ")))";
            String unboundCond = "(ExtractUnboundConditions\n" + "        (ExtractUnboundConditions " + r(onVar) + " (OutputCols " + r(leftVar) + "))\n" + "        (OutputCols " + r(rightVar) + ")\n" + "    )";
            String pattern = N("InnerJoin", leftSel, rightSel, unboundCond, r(privVar));
            return env.setPattern(pattern).focus(pattern);
        }
        Env leftEnv = transform(env, join.left());
        Env rightEnv = transform(leftEnv, join.right());
        Env condEnv = transform(rightEnv, join.cond());
        String pattern = N(jType, leftEnv.current(), rightEnv.current(), condEnv.current(), r(privateVar));
        return condEnv.setPattern(pattern).focus(pattern);
    }
    @Override
    public Env transformJoinWithPushedConds(Env env, RelRN.JoinWithPushedConds join) {
        if (flag(env, "isJoinConditionPush") && env.bindings().containsKey("left") && env.bindings().containsKey("right") && env.bindings().containsKey("on") && env.bindings().containsKey("private")) {
            String leftVar = get(env, "left");
            String rightVar = get(env, "right");
            String onVar = get(env, "on");
            String privVar = get(env, "private");
            String leftSel = "(Select " + r(leftVar) + " (ExtractBoundConditions " + r(onVar) + " (OutputCols " + r(leftVar) + ")))";
            String rightSel = "(Select " + r(rightVar) + " (ExtractBoundConditions " + r(onVar) + " (OutputCols " + r(rightVar) + ")))";
            String unboundCond = "(ExtractUnboundConditions\n" + "        (ExtractUnboundConditions " + r(onVar) + " (OutputCols " + r(leftVar) + "))\n" + "        (OutputCols " + r(rightVar) + ")\n" + "    )";
            String pattern = N("InnerJoin", leftSel, rightSel, unboundCond, r(privVar));
            return env.setPattern(pattern).focus(pattern);
        }
        Env leftEnv = transform(env, join.left());
        Env rightEnv = transform(leftEnv, join.right());
        Env condEnv = transform(rightEnv, join.cond());
        String privVar = condEnv.generateVar("private");
        String jType = joinType(join.ty().semantics());
        String pattern = N(jType, leftEnv.current(), rightEnv.current(), condEnv.current(), r(privVar));
        return condEnv.setPattern(pattern).focus(pattern);
    }
    @Override
    public Env transformUnion(Env env, RelRN.Union union) {
        if (flag(env, "isFilterSetOpTranspose")) {
            return transformFilterSetOpTranspose(env);
        }
        if (flag(env, "isUnionMerge") && env.bindings().containsKey("leftLeft") && env.bindings().containsKey("leftRight") && env.bindings().containsKey("right") && env.bindings().containsKey("innerLeftCols") && env.bindings().containsKey("innerRightCols") && env.bindings().containsKey("innerOutCols") && env.bindings().containsKey("outerRightCols") && env.bindings().containsKey("outerOutCols")) {
            String llVar = get(env, "leftLeft");
            String lrVar = get(env, "leftRight");
            String rVar = get(env, "right");
            String ilcVar = get(env, "innerLeftCols");
            String ircVar = get(env, "innerRightCols");
            String iocVar = get(env, "innerOutCols");
            String orcVar = get(env, "outerRightCols");
            String oocVar = get(env, "outerOutCols");
            String uType = union.all() ? "UnionAll" : "Union";
            String inner = "(" + uType + "\n" + "        " + r(lrVar) + "\n" + "        " + r(rVar) + "\n" + "        (MakeSetPrivate " + r(ircVar) + " " + r(orcVar) + " " + r(iocVar) + ")\n" + "    )";
            String pattern = "(" + uType + "\n" + "    " + r(llVar) + "\n" + "    " + inner + "\n" + "    (MakeSetPrivate " + r(ilcVar) + " " + r(iocVar) + " " + r(oocVar) + ")\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.bindings().containsKey("left") && env.bindings().containsKey("right") && env.bindings().containsKey("leftProjections") && env.bindings().containsKey("rightProjections") && env.bindings().containsKey("private") && env.bindings().containsKey("leftInput") && env.bindings().containsKey("rightInput") && env.bindings().containsKey("leftPassthrough") && env.bindings().containsKey("rightPassthrough")) {
            String leftVar = get(env, "left");
            String rightVar = get(env, "right");
            String leftProjectionsVar = get(env, "leftProjections");
            String rightProjectionsVar = get(env, "rightProjections");
            String privateVar = get(env, "private");
            String leftInputVar = get(env, "leftInput");
            String rightInputVar = get(env, "rightInput");
            String leftPassthroughVar = get(env, "leftPassthrough");
            String rightPassthroughVar = get(env, "rightPassthrough");
            String pattern = "(UnionPullUpConstantsReplace\n" + "    " + r(leftVar) + "\n" + "    " + r(rightVar) + "\n" + "    " + r(leftProjectionsVar) + "\n" + "    " + r(rightProjectionsVar) + "\n" + "    " + r(privateVar) + "\n" + "    " + r(leftInputVar) + "\n" + "    " + r(rightInputVar) + "\n" + "    " + r(leftPassthroughVar) + "\n" + "    " + r(rightPassthroughVar) + "\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (flag(env, "hasZeroRows")) {
            String leftVar = get(env, "zeroInput", "input");
            String pattern = "(ConstructEmptyValues (OutputCols " + r(leftVar) + "))";
            return env.setPattern(pattern).focus(pattern);
        }
        Env currentEnv = env;
        Seq<String> sourcePatterns = Seq.empty();
        for (RelRN source : union.sources()) {
            Env srcEnv = transform(currentEnv, source);
            sourcePatterns = sourcePatterns.appended(srcEnv.current());
            currentEnv = srcEnv;
        }
        String privateVar = get(currentEnv, "union_private", "private");
        String uType = union.all() ? "UnionAll" : "Union";
        String pattern;
        if (sourcePatterns.size() == 2) {
            pattern = N(uType, sourcePatterns.get(0), sourcePatterns.get(1), r(privateVar));
        } else {
            String nestedPrivate = get(currentEnv, "inner_union_private", privateVar);
            String nested = buildNestedUnionTransform(uType, sourcePatterns.drop(1), nestedPrivate);
            pattern = N(uType, sourcePatterns.get(0), nested, r(privateVar));
        }
        return currentEnv.setPattern(pattern).focus(pattern);
    }
    private String buildNestedUnionTransform(String uType, Seq<String> sources, String privVar) {
        if (sources.size() == 2) return N(uType, sources.get(0), sources.get(1), r(privVar));
        return N(uType, sources.get(0), buildNestedUnionTransform(uType, sources.drop(1), privVar), r(privVar));
    }
    @Override
    public Env transformIntersect(Env env, RelRN.Intersect intersect) {
        if (flag(env, "isIntersectMerge") && env.bindings().containsKey("leftLeft") && env.bindings().containsKey("leftRight") && env.bindings().containsKey("right") && env.bindings().containsKey("innerLeftCols") && env.bindings().containsKey("innerRightCols") && env.bindings().containsKey("outerRightCols") && env.bindings().containsKey("outerOutCols")) {
            String llVar = get(env, "leftLeft");
            String lrVar = get(env, "leftRight");
            String rVar = get(env, "right");
            String ilcVar = get(env, "innerLeftCols");
            String ircVar = get(env, "innerRightCols");
            String orcVar = get(env, "outerRightCols");
            String oocVar = get(env, "outerOutCols");
            String iType = intersect.all() ? "IntersectAll" : "Intersect";
            String inner = "(" + iType + "\n" + "        " + r(lrVar) + "\n" + "        " + r(rVar) + "\n" + "        (MakeSetPrivate " + r(ircVar) + " " + r(orcVar) + " " + r(ircVar) + ")\n" + "    )";
            String pattern = N(iType, r(llVar), inner, "(MakeSetPrivate " + r(ilcVar) + " " + r(ircVar) + " " + r(oocVar) + ")" );
            return env.setPattern(pattern).focus(pattern);
        }
        if (flag(env, "isPruneEmptyIntersect")) {
            String leftVar = get(env, "pruneEmptyLeft");
            String pattern = "(ConstructEmptyValues (OutputCols " + r(leftVar) + "))";
            return env.setPattern(pattern).focus(pattern);
        }
        Env currentEnv = env;
        Seq<String> sourcePatterns = Seq.empty();
        for (RelRN source : intersect.sources()) {
            Env srcEnv = transform(currentEnv, source);
            sourcePatterns = sourcePatterns.appended(srcEnv.current());
            currentEnv = srcEnv;
        }
        String privateVar = get(currentEnv, "intersect_private", "private");
        String iType = intersect.all() ? "IntersectAll" : "Intersect";
        String pattern;
        if (sourcePatterns.size() == 2) {
            pattern = N(iType, sourcePatterns.get(0), sourcePatterns.get(1), r(privateVar));
        } else {
            String nestedPrivate = get(currentEnv, "inner_intersect_private", privateVar);
            String nested = buildNestedIntersectTransform(iType, sourcePatterns.drop(1), nestedPrivate);
            pattern = N(iType, sourcePatterns.get(0), nested, r(privateVar));
        }
        return currentEnv.setPattern(pattern).focus(pattern);
    }
    private String buildNestedIntersectTransform(String iType, Seq<String> sources, String privVar) {
        if (sources.size() == 2) return N(iType, sources.get(0), sources.get(1), r(privVar));
        return N(iType, sources.get(0), buildNestedIntersectTransform(iType, sources.drop(1), privVar), r(privVar));
    }
    @Override
    public Env transformMinus(Env env, RelRN.Minus minus) {
        if (flag(env, "isPruneEmptyMinus")) {
            String leftVar = get(env, "pruneEmptyLeft");
            String pattern = "(ConstructEmptyValues (OutputCols " + r(leftVar) + "))";
            return env.setPattern(pattern).focus(pattern);
        }
        if (flag(env, "isMinusMerge") && env.bindings().containsKey("leftLeft") && env.bindings().containsKey("leftRight") && env.bindings().containsKey("right") && env.bindings().containsKey("innerPrivate") && env.bindings().containsKey("outerPrivate")) {
            String llVar = get(env, "leftLeft");
            String lrVar = get(env, "leftRight");
            String rVar = get(env, "right");
            String ipVar = get(env, "innerPrivate");
            String opVar = get(env, "outerPrivate");
            String pattern = N("ConstructMinusMergeResult", r(llVar), r(lrVar), r(rVar), r(ipVar), r(opVar));
            return env.setPattern(pattern).focus(pattern);
        }
        String pattern = "(Except\n" + "    $left\n" + "    (Union\n" + "        $rightB\n" + "        $rightC\n" + "        (MakeUnionPrivateForExcept $pInner $pOuter)\n" + "    )\n" + "    $pOuter\n" + ")";
        return env.setPattern(pattern).focus(pattern);
    }
    @Override
    public Env transformAggregate(Env env, RelRN.Aggregate aggregate) {
        String aggType = determineAggregateType(aggregate);
        if (env.bindings().containsKey("left") && env.bindings().containsKey("aggregations") && env.bindings().containsKey("groupingPrivate") && !env.bindings().containsKey("topOn") && !env.bindings().containsKey("topPrivate")) {
            String leftVar = get(env, "left");
            String aggsVar = get(env, "aggregations");
            String gpVar = get(env, "groupingPrivate");
            if (env.bindings().containsKey("rightFilters")) {
                String rightVar = get(env, "right");
                String rfVar = get(env, "rightFilters");
                String lj = "(LeftJoin\n" + "        " + r(leftVar) + "\n" + "        " + r(rightVar) + "\n" + "        " + r(rfVar) + "\n" + "        (EmptyJoinPrivate)\n" + "    )";
                String pattern = N("DistinctOn", lj, r(aggsVar), r(gpVar));
                return env.setPattern(pattern).focus(pattern);
            }
            String pattern = N("DistinctOn", r(leftVar), r(aggsVar), r(gpVar));
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.rulename.equals("AggregateProjectConstantToDummyJoin") && env.bindings().containsKey("input") && env.bindings().containsKey("aggregations") && env.bindings().containsKey("groupingPrivate") && !env.bindings().containsKey("projectInput")) {
            String inputVar = get(env, "input");
            String aggsVar = get(env, "aggregations");
            String gpVar = get(env, "groupingPrivate");
            String pattern = N("ConstructAggregateProjectConstantToDummyJoin", r(inputVar), r(aggsVar), r(gpVar));
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.rulename.equals("AggregateProjectMerge") && env.bindings().containsKey("input") && env.bindings().containsKey("source") && env.bindings().containsKey("aggregations") && env.bindings().containsKey("groupingPrivate") && !env.bindings().containsKey("projections")) {
            String inputVar = get(env, "input");
            String srcVar = get(env, "source");
            String aggsVar = get(env, "aggregations");
            String gpVar = get(env, "groupingPrivate");
            String pattern = N(aggType, r(srcVar), "(MergeProjectIntoAggregate " + r(inputVar) + " " + r(aggsVar) + ")", r(gpVar) );
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.bindings().containsKey("input") && env.bindings().containsKey("projections") && env.bindings().containsKey("passthrough") && env.bindings().containsKey("aggregations") && env.bindings().containsKey("groupingPrivate")) {
            String inputVar = get(env, "input");
            String projVar = get(env, "projections");
            String passVar = get(env, "passthrough");
            String aggsVar = get(env, "aggregations");
            String gpVar = get(env, "groupingPrivate");
            String remapAggs = "(RemapAggregationsThroughProject " + r(aggsVar) + " " + r(projVar) + ")";
            String remapGp = "(RemapGroupingColsThroughProject " + r(gpVar) + " " + r(projVar) + " " + r(passVar) + ")";
            String pattern = N(aggType, r(inputVar), remapAggs, remapGp);
            return env.setPattern(pattern).focus(pattern);
        }
        if (flag(env, "isAggregateExtractProject")) {
            String inputVar = get(env, "input", "input");
            String aggsVar = get(env, "aggregations", "aggregations");
            String gpVar = get(env, "groupingPrivate", "groupingPrivate");
            String pattern = "(ConstructAggregateExtractProject\n" + "    " + r(inputVar) + "\n" + "    " + r(aggsVar) + "\n" + "    " + r(gpVar) + "\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        Env srcEnv = transform(env, aggregate.source());
        Env groupEnv = transformGroupSet(srcEnv, aggregate.groupSet());
        Env aggsEnv = transformAggCalls(groupEnv, aggregate.aggCalls());
        String privVar = get(aggsEnv, "aggregate_private", "private");
        String pattern = N(aggType, srcEnv.current(), aggsEnv.current(), r(privVar));
        return aggsEnv.setPattern(pattern).focus(pattern);
    }
    private Env transformAggCalls(Env env, Seq<RelRN.AggCall> aggCalls) {
        Env currentEnv = env;
        Seq<String> aggPatterns = Seq.empty();
        for (RelRN.AggCall aggCall : aggCalls) {
            String aggVar = get(currentEnv, aggCall.name(), "agg");
            aggPatterns = aggPatterns.appended(r(aggVar));
            currentEnv = currentEnv.focus(r(aggVar));
        }
        String pattern;
        if (aggCalls.size() == 1) {
            String aggVar = get(currentEnv, "aggregations", "aggregations");
            pattern = r(aggVar);
        } else {
            pattern = "[" + aggPatterns.joinToString(" ") + "]";
        }
        return currentEnv.setPattern(pattern).focus(pattern);
    }
    private Env transformGroupSet(Env env, Seq<RexRN> groupSet) {
        Env currentEnv = env;
        Seq<String> groupPatterns = Seq.empty();
        for (RexRN g : groupSet) {
            Env ge = transform(currentEnv, g);
            groupPatterns = groupPatterns.appended(ge.current());
            currentEnv = ge;
        }
        String pattern = "[" + groupPatterns.joinToString(" ") + "]";
        return currentEnv.setPattern(pattern).focus(pattern);
    }
    @Override
    public Env transformEmpty(Env env, RelRN.Empty empty) {
        if (flag(env, "isPruneEmptyMinus")) {
            String leftVar = get(env, "pruneEmptyLeft");
            return env.setPattern("(ConstructEmptyValues (OutputCols " + r(leftVar) + "))") .focus("(ConstructEmptyValues (OutputCols " + r(leftVar) + "))");
        }
        if (flag(env, "hasZeroRows")) {
            if (env.bindings().containsKey("projections") && env.bindings().containsKey("passthrough")) {
                String projVar = get(env, "projections");
                String passVar = get(env, "passthrough");
                String pattern = "(ConstructEmptyValues (UnionCols (ProjectionCols " + r(projVar) + ") " + r(passVar) + "))";
                return env.setPattern(pattern).focus(pattern);
            }
            String inputVar = get(env, "zeroInput", "input");
            String matchPat = env.pattern();
            if (matchPat != null && matchPat.contains("Union") && matchPat.contains("$left")) {
                String pattern = "(ConstructEmptyValues (OutputCols " + r(inputVar) + "))";
                return env.setPattern(pattern).focus(pattern);
            }
            String pattern = r(inputVar);
            return env.setPattern(pattern).focus(pattern);
        }
        if (flag(env, "isPruneEmptyFilter")) {
            String inputVar = get(env, "pruneEmptyInput");
            return env.setPattern(r(inputVar)).focus(r(inputVar));
        }
        String pattern = "(ConstructEmptyValues (OutputCols $input_0))";
        return env.setPattern(pattern).focus(pattern);
    }
    @Override
    public Env transformField(Env env, RexRN.Field field) {
        String var = get(env, "field_" + field.ordinal(), "field");
        return env.setPattern(r(var)).focus(r(var));
    }
    @Override
    public Env transformPred(Env env, RexRN.Pred pred) {
        String var = get(env, pred.operator().getName(), "cond");
        return env.setPattern(r(var)).focus(r(var));
    }
    @Override
    public Env transformProj(Env env, RexRN.Proj proj) {
        String var = get(env, proj.operator().getName(), "proj");
        return env.setPattern(r(var)).focus(r(var));
    }
    public Env transformGroupBy(Env env, RexRN.GroupBy groupBy) {
        if (groupBy.sources().size() == 1 && groupBy.sources().get(0) instanceof RexRN.Proj proj) {
            String projVar = env.bindings().get(proj.operator().getName());
            if (projVar != null) return env.setPattern(r(projVar)).focus(r(projVar));
        }
        String var = get(env, groupBy.operator().getName(), "groupBy");
        return env.setPattern(r(var)).focus(r(var));
    }
    @Override
    public Env transformAnd(Env env, RexRN.And and) {
        Env currentEnv = env;
        Seq<String> operandPats = Seq.empty();
        for (RexRN op : and.sources()) {
            Env opEnv = transform(currentEnv, op);
            operandPats = operandPats.appended(opEnv.current());
            currentEnv = opEnv;
        }
        String pattern = "(ConcatFilters " + operandPats.joinToString(" ") + ")";
        return currentEnv.setPattern(pattern).focus(pattern);
    }
    @Override
    public Env transformTrue(Env env, RexRN literal) {
        String var = get(env, "true_" + System.identityHashCode(literal), "true");
        return env.setPattern(r(var)).focus(r(var));
    }
    @Override
    public Env transformFalse(Env env, RexRN literal) {
        return env.setPattern("(False)").focus("(False)");
    }
    @Override
    public Env transformCustom(Env env, RelRN custom) {
        if (custom instanceof org.qed.RRuleInstances.JoinCommute.ProjectionRelRN proj) {
            return transform(env, proj.source());
        }
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin .AggregateGroupingByDummyFields aggGrouping) {
            if (env.rulename.equals("AggregateProjectConstantToDummyJoin") && env.bindings().containsKey("input") && env.bindings().containsKey("aggregations") && env.bindings().containsKey("groupingPrivate") && !env.bindings().containsKey("projectInput")) {
                String inputVar = get(env, "input");
                String aggsVar = get(env, "aggregations");
                String gpVar = get(env, "groupingPrivate");
                String pattern = N("ConstructAggregateProjectConstantToDummyJoin", r(inputVar), r(aggsVar), r(gpVar));
                return env.setPattern(pattern).focus(pattern);
            }
            return transform(env, aggGrouping.input());
        }
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin .ProjectWithDummyFields pwd) {
            return transform(env, pwd.input());
        }
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin .SourceTable) {
            return transformScan(env, new RelRN.Scan("Source", org.qed.RexRN.varType("Source_Type", true), false));
        }
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge .ProjectOptimized projectOptimized) {
            if (env.rulename.equals("ProjectAggregateMerge") && env.bindings().containsKey("aggInput") && env.bindings().containsKey("aggregations") && env.bindings().containsKey("groupingPrivate") && env.bindings().containsKey("projections") && env.bindings().containsKey("passthrough") && env.bindings().containsKey("needed")) {
                if (projectOptimized.input() instanceof org.qed.RRuleInstances.ProjectAggregateMerge .AggregateWithUsedCallsOnly aggregateOptimized) {
                    Env aggInputEnv = transform(env, aggregateOptimized.input());
                    String aggInputPat = aggInputEnv.current();
                    String aggsVar = get(env, "aggregations");
                    String gpVar = get(env, "groupingPrivate");
                    String projVar = get(env, "projections");
                    String passVar = get(env, "passthrough");
                    String neededVar = get(env, "needed");
                    String pattern = "(Project\n" + "    (GroupBy\n" + "        " + aggInputPat + "\n" + "        (PruneAggCols " + r(aggsVar) + " " + r(neededVar) + ")\n" + "        " + r(gpVar) + "\n" + "    )\n" + "    " + r(projVar) + "\n" + "    " + r(passVar) + "\n" + ")";
                    return aggInputEnv.setPattern(pattern).focus(pattern);
                }
            }
            return transform(env, projectOptimized.input());
        }
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge .ProjectUsingSubsetOfAggregates pusa) {
            if (env.bindings().containsKey("input") && env.bindings().containsKey("aggregations") && env.bindings().containsKey("groupingPrivate") && env.bindings().containsKey("projections") && env.bindings().containsKey("passthrough")) {
                String inputVar = get(env, "input");
                String projVar = get(env, "projections");
                String passVar = get(env, "passthrough");
                String aggsVar = get(env, "aggregations");
                String gpVar = get(env, "groupingPrivate");
                String remapAggs = "(RemapAggregationsThroughProject " + r(aggsVar) + " " + r(projVar) + ")";
                String remapGp = "(RemapGroupingColsThroughProject " + r(gpVar) + " " + r(projVar) + " " + r(passVar) + ")";
                String pattern = N("GroupBy", r(inputVar), remapAggs, remapGp);
                return env.setPattern(pattern).focus(pattern);
            }
            return transform(env, pusa.input());
        }
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge .AggregateWithMultipleCalls amc) {
            return transform(env, amc.input());
        }
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge.SourceTable) {
            return transformScan(env, new RelRN.Scan("Source", org.qed.RexRN.varType("Source_Type", true), false));
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants .TopProjectionWithConstants topProj) {
            if (env.rulename.equals("UnionPullUpConstants") && env.bindings().containsKey("left") && env.bindings().containsKey("right") && env.bindings().containsKey("leftProjections") && env.bindings().containsKey("rightProjections") && env.bindings().containsKey("private") && env.bindings().containsKey("leftInput") && env.bindings().containsKey("rightInput") && env.bindings().containsKey("leftPassthrough") && env.bindings().containsKey("rightPassthrough")) {
                String leftVar = get(env, "left");
                String rightVar = get(env, "right");
                String leftProjVar = get(env, "leftProjections");
                String rightProjVar = get(env, "rightProjections");
                String privateVar = get(env, "private");
                String leftInputVar = get(env, "leftInput");
                String rightInputVar = get(env, "rightInput");
                String leftPassVar = get(env, "leftPassthrough");
                String rightPassVar = get(env, "rightPassthrough");
                String pattern = "(UnionPullUpConstantsReplace\n" + "    " + r(leftVar) + "\n" + "    " + r(rightVar) + "\n" + "    " + r(leftProjVar) + "\n" + "    " + r(rightProjVar) + "\n" + "    " + r(privateVar) + "\n" + "    " + r(leftInputVar) + "\n" + "    " + r(rightInputVar) + "\n" + "    " + r(leftPassVar) + "\n" + "    " + r(rightPassVar) + "\n" + ")";
                return env.setPattern(pattern).focus(pattern);
            }
            return transform(env, topProj.input());
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants .UnionWithConstantColumns uwcc) {
            return transformUnion(env, new RelRN.Union(true, Seq.of(uwcc.left(), uwcc.right())));
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants .LeftProjectionWithConstants lp) {
            return transformProject(env, new RelRN.Project(lp.input().field(0), lp.input()));
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants .RightProjectionWithConstants rp) {
            return transformProject(env, new RelRN.Project(rp.input().field(0), rp.input()));
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.SourceTable) {
            return transformScan(env, new RelRN.Scan("Source", org.qed.RexRN.varType("Source_Type", true), false));
        }
        if (custom instanceof org.qed.RRuleInstances.UnionToDistinct.DistinctAggregate da) {
            if (env.rulename.equals("UnionToDistinct") && env.bindings().containsKey("left") && env.bindings().containsKey("right") && env.bindings().containsKey("private") && env.bindings().containsKey("leftCols") && env.bindings().containsKey("rightCols") && env.bindings().containsKey("outCols") && env.bindings().containsKey("keyCols")) {
                String leftVar = get(env, "left");
                String rightVar = get(env, "right");
                String privVar = get(env, "private");
                String lcVar = get(env, "leftCols");
                String rcVar = get(env, "rightCols");
                String ocVar = get(env, "outCols");
                String kcVar = get(env, "keyCols");
                String translateCols = "(TranslateColSet\n" + "            (DifferenceCols (OutputCols " + r(leftVar) + ") " + r(kcVar) + ")\n" + "            " + r(lcVar) + "\n" + "            " + r(ocVar) + "\n" + "        )";
                String makeAgg = "(MakeAggCols\n" + "        ConstAgg\n" + "        " + translateCols + "\n" + "    )";
                String translateKeyCols = "(TranslateColSet " + r(kcVar) + " " + r(lcVar) + " " + r(ocVar) + ")";
                String makeGrouping = "(MakeGrouping\n" + "        " + translateKeyCols + "\n" + "        (EmptyOrdering)\n" + "    )";
                String unionAllPat = "(UnionAll " + r(leftVar) + " " + r(rightVar) + " " + r(privVar) + ")";
                String pattern = N("DistinctOn", unionAllPat, makeAgg, makeGrouping);
                return env.setPattern(pattern).focus(pattern);
            }
            return transform(env, da.input());
        }
        if (custom instanceof org.qed.RRuleInstances.UnionToDistinct.UnionAll ua) {
            Env leftEnv = transform(env, ua.left());
            Env rightEnv = transform(leftEnv, ua.right());
            return rightEnv;
        }
        return unimplementedTransform(env, custom);
    }
    @Override
    public Env transformCustom(Env env, RexRN custom) {
        if (custom instanceof RexRN.GroupBy groupBy) return transformGroupBy(env, groupBy);
        return unimplementedTransform(env, custom);
    }
    @Override
    public String translate(String name, Env onMatch, Env transform) {
        String match = postProcessMatch(onMatch.pattern());
        String out = postProcessTransform(transform.pattern(), match);
        return "[" + name + ", Normalize]\n" + match + "\n=>\n" + out + "\n";
    }
    private static String postProcessMatch(String match) {
        if (match == null) return "";
        if (match.contains("HasZeroRows")) {
            match = normalizeVars(match, "projections", "passthrough", "filters");
        }
        if (match.startsWith("(Union\n") || match.startsWith("(UnionAll\n")) {
            String[] lines = match.split("\n");
            if (lines.length >= 3 && lines[1].contains(":(Values)") && lines[2].contains(":(Values)")) {
                String leftVar = extractVar(lines[1]);
                String rightVar = extractVar(lines[2]);
                String uType = lines[0].startsWith("(UnionAll") ? "UnionAll" : "Union";
                match = "(" + uType + "\n" + "    " + b(leftVar) + " & (HasZeroRows " + r(leftVar) + ")\n" + "    " + b(rightVar) + " & (HasZeroRows " + r(rightVar) + ")\n" + ")";
            }
        }
        if (match.contains("HasZeroRows") && match.contains("SetPrivate")) {
            match = match.replaceAll("\\s+\\$private_\\d+:\\*\\s*\\)", "\n)") .replaceAll("\\s+\\$private_\\d+:\\*\\)", ")");
        }
        return match;
    }
    private static String postProcessTransform(String out, String match) {
        if (out == null) return "";
        if (out.startsWith("(ConstructEmptyValues (OutputCols $")) {
            int startIdx = "(ConstructEmptyValues (OutputCols $".length();
            String var = extractVarFromPos(out, startIdx);
            if (var.equals("input")) {
                String numbered = findFirstVar(match);
                if (numbered != null) {
                    out = out.replace( "(ConstructEmptyValues (OutputCols $input)", "(ConstructEmptyValues (OutputCols $" + numbered + ")");
                }
            }
        } else if (out.equals("$input")) {
            String numbered = findFirstVar(match);
            if (numbered != null) out = "$" + numbered;
        }
        if (match.contains("HasZeroRows") && match.contains("SetPrivate") && match.contains("outCols") && out.contains("ConstructEmptyValues")) {
            java.util.regex.Matcher m = java.util.regex.Pattern .compile("\\$outCols_(\\d+)").matcher(match);
            if (m.find()) {
                String outColsVar = "$outCols_" + m.group(1);
                int si = out.indexOf("(ConstructEmptyValues (OutputCols $");
                if (si >= 0) {
                    int vi = si + "(ConstructEmptyValues (OutputCols $".length();
                    int vend = vi + extractVarFromPos(out, vi).length();
                    out = out.substring(0, si) + "(ConstructEmptyValues (ColListToSet " + outColsVar + "))" + out.substring(vend + 2);
                }
            }
        } else if (match.contains("HasZeroRows") && match.contains("$left") && out.contains("ConstructEmptyValues")) {
            int leftIdx = match.indexOf("$left");
            if (leftIdx >= 0) {
                String leftVar = extractVarFromPos(match, leftIdx + 1);
                out = out.replaceAll( "(OutputCols \\$)[a-zA-Z_][a-zA-Z0-9_]*", "$1" + leftVar);
            }
        }
        java.util.Map<String, String> varMap = extractNumberedVarMap(match);
        if (!varMap.isEmpty()) {
            var sorted = new java.util.ArrayList<>(varMap.entrySet());
            sorted.sort((a, bx) -> Integer.compare(bx.getKey().length(), a.getKey().length()));
            for (var e : sorted) {
                out = out.replaceAll( "\\$" + java.util.regex.Pattern.quote(e.getKey()) + "(?![A-Za-z0-9_])", java.util.regex.Matcher.quoteReplacement(e.getValue()));
            }
        }
        if (match.contains("HasZeroRows")) {
            out = normalizeVars(out, "projections", "passthrough", "filters");
        }
        return out;
    }
    private static String normalizeVars(String str, String... varNames) {
        for (String v : varNames) {
            if (str.contains("$" + v + "_")) {
                str = str.replaceAll( "\\$" + v + "_\\d+", java.util.regex.Matcher.quoteReplacement("$" + v));
            }
        }
        return str;
    }
    private static String extractVar(String line) {
        int i = line.indexOf('$');
        if (i < 0) return null;
        return extractVarFromPos(line, i + 1);
    }
    private static String extractVarFromPos(String str, int pos) {
        int end = pos;
        while (end < str.length() && (Character.isLetterOrDigit(str.charAt(end)) || str.charAt(end) == '_')) {
            end++;
        }
        return str.substring(pos, end);
    }
    private static String findFirstVar(String match) {
        for (String line : match.split("\n")) {
            if (line.contains("$private")) continue;
            if (line.contains("$")) {
                String v = extractVar(line);
                if (v != null) return v;
            }
        }
        return null;
    }
    private static java.util.Map<String, String> extractNumberedVarMap(String match) {
        java.util.Map<String, String> map = new java.util.HashMap<>();
        java.util.regex.Matcher m =
            java.util.regex.Pattern.compile("\\$([A-Za-z][A-Za-z0-9_]*)_([0-9]+)").matcher(match);
        while (m.find()) {
            String base = m.group(1);
            String numbered = "$" + base + "_" + m.group(2);
            map.putIfAbsent(base, numbered);
        }
        return map;
    }
    public record Env( AtomicInteger varId, String pattern, ImmutableMap<String, String> bindings, String currentVar, String rulename ) {
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
        public String current() { return currentVar; }
        public String pattern() { return pattern; }
        public ImmutableMap<String, String> bindings() { return bindings; }
        public String rulename() { return rulename; }
    }
}
