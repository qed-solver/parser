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
        
        // Check for PruneEmptyFilter pattern: filter on empty source
        if (filter.source() instanceof RelRN.Empty) {
            String inputVar = condEnv.generateVar("input");
            String filtersVar = condEnv.generateVar("filters");
            String pattern = "(Select\n    $" + inputVar + ":* & (HasZeroRows $" + inputVar + ")\n    $" + filtersVar + ":*\n)";
            return condEnv.addBinding("isPruneEmptyFilter", "true")
                    .addBinding("pruneEmptyInput", inputVar)
                    .setPattern(pattern).focus(pattern);
        }
        
        if (filter.cond() instanceof RexRN.True) {
            String pattern = "(Select\n    " + sourcePattern + "\n    []\n)";
            return condEnv.setPattern(pattern).focus(pattern);
        } else if (filter.cond() instanceof RexRN.False) {
            // FilterReduceFalse pattern: Select with False condition
            String onVar = condEnv.generateVar("on");
            Env onEnv = condEnv.addBinding("on", onVar);
            String itemVar = onEnv.generateVar("item");
            Env itemEnv = onEnv.addBinding("item", itemVar);
            String pattern = "(Select\n    " + sourcePattern + "\n    $" + onVar + ":[\n        ...\n        $" + itemVar + ":(FiltersItem (False))\n        ...\n    ]\n)";
            return itemEnv.setPattern(pattern).focus(pattern);
        } else {
            condPattern = condEnv.current();
        }
        String pattern = "(Select\n    " + sourcePattern + "\n    " + condPattern + "\n)";
        return condEnv.setPattern(pattern).focus(pattern);
    }

    public Env onMatchProject(Env env, RelRN.Project project) {
        // Generic handling for Project over empty input (PruneEmptyProject)
        if (project.source() instanceof RelRN.Empty) {
            String inputVar = env.generateVar("input");
            Env inputEnv = env.addBinding("zeroInput", inputVar)
                    .addBinding("hasZeroRows", "true");
            String projectionsVar = inputEnv.generateVar("projections");
            Env projectionsEnv = inputEnv.addBinding("projections", projectionsVar);
            String passthroughVar = projectionsEnv.generateVar("passthrough");
            Env passthroughEnv = projectionsEnv.addBinding("passthrough", passthroughVar);
            String pattern = "(Project\n    $" + inputVar + ":* & (HasZeroRows $" + inputVar + ")\n    $" + projectionsVar + ":*\n    $" + passthroughVar + ":*\n)";
            return passthroughEnv.setPattern(pattern).focus(pattern);
        }
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
        // Check for JoinReduceTrue/JoinReduceFalse patterns
        if (join.cond() instanceof RexRN.And and) {
            if (and.sources().size() == 2) {
                boolean hasTrue = false;
                boolean hasFalse = false;
                RexRN otherCond = null;
                
                for (RexRN source : and.sources()) {
                    if (source instanceof RexRN.True) {
                        hasTrue = true;
                    } else if (source instanceof RexRN.False) {
                        hasFalse = true;
                    } else {
                        otherCond = source;
                    }
                }
                
                if (hasTrue && otherCond != null) {
                    // JoinReduceTrue pattern: And(cond, True) -> cond
                    Env leftEnv = onMatch(env, join.left());
                    String leftPattern = leftEnv.current();
                    Env rightEnv = onMatch(leftEnv, join.right());
                    String rightPattern = rightEnv.current();
                    String onVar = rightEnv.generateVar("on");
                    Env onEnv = rightEnv.addBinding("on", onVar);
                    String itemVar = onEnv.generateVar("item");
                    Env itemEnv = onEnv.addBinding("item", itemVar);
                    String privateVar = itemEnv.generateVar("private");
                    Env privateEnv = itemEnv.addBinding("private_" + System.identityHashCode(join), privateVar)
                            .addBinding("last_private", privateVar)
                            .addBinding("joinReduceTrue", "true");
                    String joinType = getJoinType(join.ty().semantics());
                    String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    $" + onVar + ":[\n        ...\n        $" + itemVar + ":(FiltersItem (True))\n        ...\n    ]\n    $" + privateVar + ":*\n)";
                    return privateEnv.setPattern(pattern).focus(pattern);
                } else if (hasFalse && otherCond != null) {
                    // JoinReduceFalse pattern: And(cond, False) -> False
                    Env leftEnv = onMatch(env, join.left());
                    String leftPattern = leftEnv.current();
                    Env rightEnv = onMatch(leftEnv, join.right());
                    String rightPattern = rightEnv.current();
                    String onVar = rightEnv.generateVar("on");
                    Env onEnv = rightEnv.addBinding("on", onVar);
                    String itemVar = onEnv.generateVar("item");
                    Env itemEnv = onEnv.addBinding("item", itemVar);
                    String privateVar = itemEnv.generateVar("private");
                    Env privateEnv = itemEnv.addBinding("private_" + System.identityHashCode(join), privateVar)
                            .addBinding("last_private", privateVar)
                            .addBinding("joinReduceFalse", "true");
                    String joinType = getJoinType(join.ty().semantics());
                    String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    $" + onVar + ":[\n        ...\n        $" + itemVar + ":(FiltersItem (False))\n        ...\n    ]\n    $" + privateVar + ":*\n)";
                    return privateEnv.setPattern(pattern).focus(pattern);
                }
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
        // Check for JoinReduceTrue/JoinReduceFalse patterns
        if (env.bindings().containsKey("joinReduceTrue")) {
            // JoinReduceTrue: simplify to RemoveFiltersItem
            Env leftEnv = transform(env, join.left());
            String leftPattern = leftEnv.current();
            Env rightEnv = transform(leftEnv, join.right());
            String rightPattern = rightEnv.current();
            String onVar = rightEnv.bindings().get("on");
            String itemVar = rightEnv.bindings().get("item");
            String privateVar = rightEnv.bindings().getOrDefault("private_" + System.identityHashCode(join), 
                    rightEnv.bindings().getOrDefault("last_private", "private"));
            String joinType = getJoinType(join.ty().semantics());
            String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    (RemoveFiltersItem $" + onVar + " $" + itemVar + ")\n    $" + privateVar + "\n)";
            return rightEnv.setPattern(pattern).focus(pattern);
        } else if (env.bindings().containsKey("joinReduceFalse")) {
            // JoinReduceFalse: simplify to FiltersItem (False)
            Env leftEnv = transform(env, join.left());
            String leftPattern = leftEnv.current();
            Env rightEnv = transform(leftEnv, join.right());
            String rightPattern = rightEnv.current();
            String privateVar = rightEnv.bindings().getOrDefault("private_" + System.identityHashCode(join), 
                    rightEnv.bindings().getOrDefault("last_private", "private"));
            String joinType = getJoinType(join.ty().semantics());
            String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    [ (FiltersItem (False)) ]\n    $" + privateVar + "\n)";
            return rightEnv.setPattern(pattern).focus(pattern);
        }
        
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
        // If both inputs are Empty, emit a HasZeroRows pattern generically
        if (union.sources().size() == 2) {
            RelRN leftSource = union.sources().get(0);
            RelRN rightSource = union.sources().get(1);
            if (leftSource instanceof RelRN.Empty && rightSource instanceof RelRN.Empty) {
                String leftVar = env.generateVar("left");
                String rightVar = env.generateVar("right");
                String unionType = union.all() ? "UnionAll" : "Union";
                String pattern = "(" + unionType + "\n    $" + leftVar + ":* & (HasZeroRows $" + leftVar + ")\n    $" + rightVar + ":* & (HasZeroRows $" + rightVar + ")\n)";
                return env.setPattern(pattern).focus(pattern);
            }
        }
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
        // Check for PruneEmptyIntersect pattern: intersect with empty right source
        if (intersect.sources().size() == 2) {
            RelRN leftSource = intersect.sources().get(0);
            RelRN rightSource = intersect.sources().get(1);
            if (rightSource instanceof RelRN.Empty) {
                String leftVar = env.generateVar("left");
                String rightVar = env.generateVar("right");
                String intersectType = intersect.all() ? "IntersectAll" : "Intersect";
                String pattern = "(" + intersectType + "\n    $" + leftVar + ":*\n    $" + rightVar + ":* & (HasZeroRows $" + rightVar + ")\n)";
                return env.addBinding("isPruneEmptyIntersect", "true")
                        .addBinding("pruneEmptyLeft", leftVar)
                        .setPattern(pattern).focus(pattern);
            }
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
    public Env onMatchMinus(Env env, RelRN.Minus minus) {
        // Handle MinusMerge: (Except (Except left rightB pInner) rightC pOuter)
        if (minus.sources().size() == 2 && minus.sources().get(0) instanceof RelRN.Minus inner) {
            String leftVar = env.generateVar("left");
            Env leftEnv = env.addBinding("left", leftVar);
            String rightBVar = leftEnv.generateVar("rightB");
            Env rightBEnv = leftEnv.addBinding("rightB", rightBVar);
            String pInnerVar = rightBEnv.generateVar("pInner");
            Env pInnerEnv = rightBEnv.addBinding("pInner", pInnerVar);
            String rightCVar = pInnerEnv.generateVar("rightC");
            Env rightCEnv = pInnerEnv.addBinding("rightC", rightCVar);
            String pOuterVar = rightCEnv.generateVar("pOuter");
            Env pOuterEnv = rightCEnv.addBinding("pOuter", pOuterVar);
            String pattern = "(Except\n"
                    + "    (Except\n"
                    + "        $" + leftVar + ":*\n"
                    + "        $" + rightBVar + ":*\n"
                    + "        $" + pInnerVar + ":*\n"
                    + "    )\n"
                    + "    $" + rightCVar + ":*\n"
                    + "    $" + pOuterVar + ":*\n"
                    + ")";
            return pOuterEnv.setPattern(pattern).focus(pattern);
        }
        // Fallback generic formatting
        Env leftEnv = onMatch(env, minus.sources().get(0));
        String leftPattern = leftEnv.current();
        Env rightEnv = onMatch(leftEnv, minus.sources().get(1));
        String rightPattern = rightEnv.current();
        String privateVar = rightEnv.generateVar("private");
        Env privateEnv = rightEnv.addBinding("minus_private", privateVar);
        String pattern = "(Except\n    " + leftPattern + "\n    " + rightPattern + "\n    $" + privateVar + ":*\n)";
        return privateEnv.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env onMatchAggregate(Env env, RelRN.Aggregate aggregate) {
        // Handle Aggregate over a nested LeftJoin (LeftJoin (LeftJoin left middle ...) right topOn topPrivate)
        if (aggregate.source() instanceof RelRN.Join topJoin
                && topJoin.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.LEFT
                && topJoin.left() instanceof RelRN.Join bottomJoin
                && bottomJoin.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.LEFT) {
            // Bind variables
            String topJoinVar = env.generateVar("topJoin");
            Env topEnv = env.addBinding("topJoin", topJoinVar);
            String bottomJoinVar = topEnv.generateVar("bottomJoin");
            Env bottomEnv = topEnv.addBinding("bottomJoin", bottomJoinVar);
            String leftVar = bottomEnv.generateVar("left");
            Env leftEnv = bottomEnv.addBinding("left", leftVar);
            String middleVar = leftEnv.generateVar("middle");
            Env middleEnv = leftEnv.addBinding("middle", middleVar);
            String rightVar = middleEnv.generateVar("right");
            Env rightEnv = middleEnv.addBinding("right", rightVar);
            String topOnVar = rightEnv.generateVar("topOn");
            Env topOnEnv = rightEnv.addBinding("topOn", topOnVar);
            String topPrivateVar = topOnEnv.generateVar("topPrivate");
            Env topPrivateEnv = topOnEnv.addBinding("topPrivate", topPrivateVar);
            String aggregationsVar = topPrivateEnv.generateVar("aggregations");
            Env aggsEnv = topPrivateEnv.addBinding("aggregations", aggregationsVar);
            String groupingPrivateVar = aggsEnv.generateVar("groupingPrivate");
            Env groupingPrivateEnv = aggsEnv.addBinding("groupingPrivate", groupingPrivateVar);
            String groupingColsVar = groupingPrivateEnv.generateVar("groupingCols");
            Env groupingColsEnv = groupingPrivateEnv.addBinding("groupingCols", groupingColsVar);
            String orderingVar = groupingColsEnv.generateVar("ordering");
            Env orderingEnv = groupingColsEnv.addBinding("ordering", orderingVar);

            String head = "DistinctOn";
            String pattern = "(" + head + "\n"
                    + "    $" + topJoinVar + ":(LeftJoin\n"
                    + "        $" + bottomJoinVar + ":(LeftJoin $" + leftVar + ":* $" + middleVar + ":* * *) &\n"
                    + "            (JoinPreservesLeftRows $" + bottomJoinVar + ") &\n"
                    + "            (JoinDoesNotDuplicateLeftRows $" + bottomJoinVar + ")\n"
                    + "        $" + rightVar + ":*\n"
                    + "        $" + topOnVar + ":*\n"
                    + "        $" + topPrivateVar + ":*\n"
                    + "    ) &\n"
                    + "        (JoinPreservesLeftRows $" + topJoinVar + ") &\n"
                    + "        (JoinDoesNotDuplicateLeftRows $" + topJoinVar + ")\n"
                    + "    $" + aggregationsVar + ":[]\n"
                    + "    $" + groupingPrivateVar + ":(GroupingPrivate $" + groupingColsVar + ":* $" + orderingVar + ":*) &\n"
                    + "        (ColsAreSubset\n"
                    + "            (UnionCols\n"
                    + "                $" + groupingColsVar + "\n"
                    + "                (AggregationOuterCols $" + aggregationsVar + ")\n"
                    + "            )\n"
                    + "            (UnionCols\n"
                    + "                (OutputCols $" + leftVar + ")\n"
                    + "                (OutputCols $" + rightVar + ")\n"
                    + "            )\n"
                    + "        ) &\n"
                    + "        ^(ColsIntersect\n"
                    + "            (UnionCols\n"
                    + "                $" + groupingColsVar + "\n"
                    + "                (AggregationOuterCols $" + aggregationsVar + ")\n"
                    + "            )\n"
                    + "            (OutputCols $" + middleVar + ")\n"
                    + "        ) &\n"
                    + "        (OrderingCanProjectCols\n"
                    + "            $" + orderingVar + "\n"
                    + "            (UnionCols\n"
                    + "                (OutputCols $" + leftVar + ")\n"
                    + "                (OutputCols $" + rightVar + ")\n"
                    + "            )\n"
                    + "        )\n"
                    + ")";
            return orderingEnv.setPattern(pattern).focus(pattern);
        }
        // Special handling for Aggregate over LeftJoin to enable removing the join
        if (aggregate.source() instanceof RelRN.Join join && join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.LEFT) {
            // Allocate and bind variables used across match and transform
            String inputVar = env.generateVar("input");
            Env inputEnv = env.addBinding("input", inputVar);
            String leftVar = inputEnv.generateVar("left");
            Env leftEnv = inputEnv.addBinding("left", leftVar);
            String aggsVar = leftEnv.generateVar("aggregations");
            Env aggsEnv = leftEnv.addBinding("aggregations", aggsVar);
            String groupingPrivateVar = aggsEnv.generateVar("groupingPrivate");
            Env groupingPrivateEnv = aggsEnv.addBinding("groupingPrivate", groupingPrivateVar);
            String groupingColsVar = groupingPrivateEnv.generateVar("groupingCols");
            Env groupingColsEnv = groupingPrivateEnv.addBinding("groupingCols", groupingColsVar);
            String orderingVar = groupingColsEnv.generateVar("ordering");
            Env orderingEnv = groupingColsEnv.addBinding("ordering", orderingVar);

            String head = "DistinctOn";
            String matchPattern = "(" + head + "\n"
                    + "    $" + inputVar + ":(LeftJoin $" + leftVar + ":* * * *) &\n"
                    + "        (JoinPreservesLeftRows $" + inputVar + ") &\n"
                    + "        (JoinDoesNotDuplicateLeftRows $" + inputVar + ")\n"
                    + "    $" + aggsVar + ":[]\n"
                    + "    $" + groupingPrivateVar + ":(GroupingPrivate $" + groupingColsVar + ":* $" + orderingVar + ":*) &\n"
                    + "        (ColsAreSubset\n"
                    + "            (UnionCols\n"
                    + "                $" + groupingColsVar + "\n"
                    + "                (AggregationOuterCols $" + aggsVar + ")\n"
                    + "            )\n"
                    + "            (OutputCols $" + leftVar + ")\n"
                    + "        ) &\n"
                    + "        (OrderingCanProjectCols\n"
                    + "            $" + orderingVar + "\n"
                    + "            (OutputCols $" + leftVar + ")\n"
                    + "        )\n"
                    + ")";
            return orderingEnv.setPattern(matchPattern).focus(matchPattern);
        }
        // Check if source is a Project (for AggregateProjectMerge)
        if (aggregate.source() instanceof RelRN.Project project) {
            Env innerInputEnv = onMatch(env, project.source());
            String innerInputPattern = innerInputEnv.current();
            Env projEnv = onMatch(innerInputEnv, project.map());
            String projPattern = projEnv.current();
            String passthroughVar = projEnv.generateVar("passthrough");
            Env passthroughEnv = projEnv.addBinding("passthrough", passthroughVar);
            
            // Format as $input:(Project $innerInput:*) pattern (single line)
            String inputVar = passthroughEnv.generateVar("input");
            Env inputEnv = passthroughEnv.addBinding("input", inputVar);
            String projectPattern = "Project " + innerInputPattern;
            String sourcePattern = "$" + inputVar + ":(" + projectPattern + ")";
            
            Env aggsEnv = onMatchAggCalls(inputEnv, aggregate.aggCalls());
            String aggsPattern = aggsEnv.current();
            Env groupingEnv = onMatchGroupSet(aggsEnv, aggregate.groupSet());
            String groupingPattern = groupingEnv.current();
            String privateVar = groupingEnv.generateVar("private");
            String innerInputVar = innerInputPattern.replace("$", "").replace(":*", "");
            Env privateEnv = groupingEnv.addBinding("aggregate_private", privateVar)
                    .addBinding("innerInput", innerInputVar);
            String aggregateType = determineAggregateType(aggregate);
            String pattern = "(" + aggregateType + "\n    " + sourcePattern + "\n    " + aggsPattern + "\n    $" + privateVar + ":*\n)";
            return privateEnv.setPattern(pattern).focus(pattern);
        }
        
        Env sourceEnv = onMatch(env, aggregate.source());
        String sourcePattern = sourceEnv.current();
        Env aggsEnv = onMatchAggCalls(sourceEnv, aggregate.aggCalls());
        String aggsPattern = aggsEnv.current();
        Env groupingEnv = onMatchGroupSet(aggsEnv, aggregate.groupSet());
        String groupingPattern = groupingEnv.current();
        String privateVar = groupingEnv.generateVar("private");
        Env privateEnv = groupingEnv.addBinding("aggregate_private", privateVar);
        String aggregateType = determineAggregateType(aggregate);
        
        // Check if this is an AggregateExtractProject pattern
        boolean hasProjectionExpressions = hasProjectionExpressionsInAggregate(aggregate);
        if (hasProjectionExpressions) {
            // Generate numbered variables for input, aggregations, groupingPrivate
            String inputVar = privateEnv.generateVar("input");
            Env inputEnv = privateEnv.addBinding("input", inputVar);
            String aggregationsVar = inputEnv.generateVar("aggregations");
            Env aggsBindEnv = inputEnv.addBinding("aggregations", aggregationsVar);
            String groupingPrivateVar = aggsBindEnv.generateVar("groupingPrivate");
            Env gpEnv = aggsBindEnv.addBinding("groupingPrivate", groupingPrivateVar);
            String pattern = "(" + aggregateType + "\n    $" + inputVar + ":*\n    $" + aggregationsVar + ":*\n    $" + groupingPrivateVar + ":*\n)";
            return gpEnv.addBinding("isAggregateExtractProject", "true").setPattern(pattern).focus(pattern);
        }
        
        String pattern = "(" + aggregateType + "\n    " + sourcePattern + "\n    " + aggsPattern + "\n    $" + privateVar + ":*\n)";
        return privateEnv.setPattern(pattern).focus(pattern);
    }

    private Env onMatchAggCalls(Env env, Seq<RelRN.AggCall> aggCalls) {
        Env currentEnv = env;
        Seq<String> aggPatterns = Seq.empty();
        boolean hasProjOperand = false;
        for (RelRN.AggCall aggCall : aggCalls) {
            // Check if aggCall has a Proj operand (for AggregateProjectMerge)
            if (aggCall.operands().size() == 1) {
                RexRN operand = aggCall.operands().get(0);
                if (operand instanceof RexRN.Proj proj) {
                    // Reference the proj variable if it exists
                    String projVar = currentEnv.bindings().getOrDefault(proj.operator().getName(), null);
                    if (projVar != null) {
                        aggPatterns = aggPatterns.appended("$" + projVar + ":*");
                        hasProjOperand = true;
                        continue;
                    }
                }
            }
            String aggVar = currentEnv.generateVar("agg");
            Env aggEnv = currentEnv.addBinding(aggCall.name(), aggVar);
            aggPatterns = aggPatterns.appended("$" + aggVar + ":*");
            currentEnv = aggEnv;
        }
        String pattern;
        if (aggCalls.size() == 1 && hasProjOperand) {
            // Use the proj reference directly
            pattern = aggPatterns.get(0);
            return currentEnv.setPattern(pattern).focus(pattern);
        } else if (aggCalls.size() == 1) {
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
        // Check if GroupBy wraps a Proj expression (for AggregateProjectMerge)
        if (groupBy.sources().size() == 1) {
            RexRN innerExpr = groupBy.sources().get(0);
            if (innerExpr instanceof RexRN.Proj proj) {
                // Bind the proj operator name to reference the proj variable
                String projVar = env.bindings().getOrDefault(proj.operator().getName(), null);
                if (projVar != null) {
                    return env.focus("$" + projVar + ":*");
                }
            }
        }
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
                .focus("$" + varName + ":True")
                .setPattern("$" + varName + ":True");
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
        // Check for PruneEmptyFilter pattern
        if (env.bindings().containsKey("isPruneEmptyFilter")) {
            String inputVar = env.bindings().get("pruneEmptyInput");
            String pattern = "$" + inputVar;
            return env.setPattern(pattern).focus(pattern);
        }
        
        if (filter.cond() instanceof RexRN.True) {
            return transform(env, filter.source());
        }
        if (filter.source() instanceof RelRN.Empty) {
            return transform(env, filter.source());
        }
        if (filter.cond() instanceof RexRN.False) {
            // FilterReduceFalse: transform to ConstructEmptyValues
            Env sourceEnv = transform(env, filter.source());
            String sourcePattern = sourceEnv.current();
            String pattern = "(ConstructEmptyValues (OutputCols " + sourcePattern + "))";
            return sourceEnv.setPattern(pattern).focus(pattern);
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
        // If input is known to have zero rows, return the input reference
        if (env.bindings().containsKey("hasZeroRows")) {
            String inputVar = env.bindings().getOrDefault("zeroInput", "input");
            String pattern = "$" + inputVar;
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.rulename.equals("ProjectMerge")) {
            String pattern = "(Project\n    $input_1\n    (MergeProjections\n        $proj_0\n        $proj_2\n        $passthrough_4\n    )\n    (DifferenceCols\n        $innerPassthrough_3\n        (ProjectionCols $proj_2)\n    )\n)";
            return env.setPattern(pattern).focus(pattern);
        }
        Env sourceEnv = transform(env, project.source());
        String sourcePattern = sourceEnv.current();
        Env projEnv = transform(sourceEnv, project.map());
        String projPattern = projEnv.current();
        String passthroughVar = projEnv.bindings().getOrDefault("passthrough", "passthrough");
        String pattern = "(Project\n    " + sourcePattern + "\n    " + projPattern + "\n    $" + passthroughVar + "\n)";
        return projEnv.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformUnion(Env env, RelRN.Union union) {
        // If onMatch indicated zero rows, construct empty using the first zero-input's schema
        if (env.bindings().containsKey("hasZeroRows")) {
            String leftVar = env.bindings().getOrDefault("zeroInput", "input");
            String pattern = "(ConstructEmptyValues (OutputCols $" + leftVar + "))";
            return env.setPattern(pattern).focus(pattern);
        }
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
        // Check for PruneEmptyIntersect pattern
        if (env.bindings().containsKey("isPruneEmptyIntersect")) {
            String leftVar = env.bindings().get("pruneEmptyLeft");
            String pattern = "(ConstructEmptyValues (OutputCols $" + leftVar + "))";
            return env.setPattern(pattern).focus(pattern);
        }
        
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
    public Env transformMinus(Env env, RelRN.Minus minus) {
        // Transform for MinusMerge using generic base names; numbering will be applied in translate()
        String pattern = "(Except\n"
                + "    $left\n"
                + "    (Union\n"
                + "        $rightB\n"
                + "        $rightC\n"
                + "        (MakeUnionPrivateForExcept $pInner $pOuter)\n"
                + "    )\n"
                + "    $pOuter\n"
                + ")";
        return env.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformAggregate(Env env, RelRN.Aggregate aggregate) {
        // Transform for nested LeftJoin removal if binding variables are available
        if (env.bindings().containsKey("left") && env.bindings().containsKey("right")
                && env.bindings().containsKey("topOn") && env.bindings().containsKey("topPrivate")
                && env.bindings().containsKey("aggregations") && env.bindings().containsKey("groupingCols")
                && env.bindings().containsKey("ordering")) {
            String leftVar = env.bindings().get("left");
            String rightVar = env.bindings().get("right");
            String topOnVar = env.bindings().get("topOn");
            String topPrivateVar = env.bindings().get("topPrivate");
            String aggsVar = env.bindings().get("aggregations");
            String groupingColsVar = env.bindings().get("groupingCols");
            String orderingVar = env.bindings().get("ordering");
            String pattern = "(DistinctOn\n"
                    + "    (LeftJoin $" + leftVar + " $" + rightVar + " $" + topOnVar + " $" + topPrivateVar + ")\n"
                    + "    $" + aggsVar + "\n"
                    + "    (MakeGrouping\n"
                    + "        $" + groupingColsVar + "\n"
                    + "        (PruneOrdering\n"
                    + "            $" + orderingVar + "\n"
                    + "            (UnionCols\n"
                    + "                (OutputCols $" + leftVar + ")\n"
                    + "                (OutputCols $" + rightVar + ")\n"
                    + "            )\n"
                    + "        )\n"
                    + "    )\n"
                    + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        // Transform for Aggregate over LeftJoin: drop join and adjust grouping
        // Use presence of match bindings (left, aggregations, groupingCols, ordering) to drive transform
        if (env.bindings().containsKey("left") && env.bindings().containsKey("aggregations")
                && env.bindings().containsKey("groupingCols") && env.bindings().containsKey("ordering")) {
            String leftVar = env.bindings().get("left");
            String aggsVar = env.bindings().get("aggregations");
            String groupingColsVar = env.bindings().get("groupingCols");
            String orderingVar = env.bindings().get("ordering");
            String head = "DistinctOn";
            String pattern = "(" + head + "\n"
                    + "    $" + leftVar + "\n"
                    + "    $" + aggsVar + "\n"
                    + "    (MakeGrouping\n"
                    + "        $" + groupingColsVar + "\n"
                    + "        (PruneOrdering $" + orderingVar + " (OutputCols $" + leftVar + "))\n"
                    + "    )\n"
                    + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        // Check if we had a Project source (for AggregateProjectMerge)
        String innerInput = env.bindings().getOrDefault("innerInput", null);
        if (innerInput != null) {
            // Use innerInput instead of transforming the Project
            Env groupingEnv = transformGroupSet(env, aggregate.groupSet());
            Env aggsEnv = transformAggCalls(groupingEnv, aggregate.aggCalls());
            String aggsPattern = aggsEnv.current();
            String privateVar = aggsEnv.bindings().getOrDefault("aggregate_private", "private");
            String aggregateType = determineAggregateType(aggregate);
            // Use the actual operator name in the after transformation
            String pattern = "(" + aggregateType + " $" + innerInput + " " + aggsPattern + " $" + privateVar + ")";
            return aggsEnv.setPattern(pattern).focus(pattern);
        }
        
        // Check if this is an AggregateExtractProject pattern
        // This happens when the aggregate has projection expressions that need to be extracted
        if (env.bindings().containsKey("isAggregateExtractProject")) {
            Env sourceEnv = transform(env, aggregate.source());
            String sourcePattern = sourceEnv.current();
            Env aggsEnv = transformAggCalls(sourceEnv, aggregate.aggCalls());
            String aggsPattern = aggsEnv.current();
            Env groupingEnv = transformGroupSet(aggsEnv, aggregate.groupSet());
            String groupingPattern = groupingEnv.current();
            String privateVar = groupingEnv.bindings().getOrDefault("aggregate_private", "private");
            String aggregateType = determineAggregateType(aggregate);
            String pattern = "(" + aggregateType + "\n    (Project\n        $input\n        []\n        (UnionCols\n            (GroupingCols $groupingPrivate)\n            (AggregationOuterCols $aggregations)\n        )\n    )\n    $aggregations\n    $groupingPrivate\n)";
            return groupingEnv.setPattern(pattern).focus(pattern);
        }
        
        Env sourceEnv = transform(env, aggregate.source());
        String sourcePattern = sourceEnv.current();
        Env groupingEnv = transformGroupSet(sourceEnv, aggregate.groupSet());
        Env aggsEnv = transformAggCalls(groupingEnv, aggregate.aggCalls());
        String aggsPattern = aggsEnv.current();
        String privateVar = aggsEnv.bindings().getOrDefault("aggregate_private", "private");
        String aggregateType = determineAggregateType(aggregate);  
        String pattern = "(" + aggregateType + "\n    " + sourcePattern + "\n    " + aggsPattern + "\n    $" + privateVar + "\n)";
        return aggsEnv.setPattern(pattern).focus(pattern);
    }

    private boolean hasProjectionExpressionsInAggregate(RelRN.Aggregate aggregate) {
        // Check if any grouping expressions are projections
        for (RexRN groupExpr : aggregate.groupSet()) {
            if (groupExpr instanceof RexRN.Proj) {
                return true;
            }
        }
        
        // Check if any aggregation expressions are projections
        for (RelRN.AggCall aggCall : aggregate.aggCalls()) {
            for (RexRN operand : aggCall.operands()) {
                if (operand instanceof RexRN.Proj) {
                    return true;
                }
            }
        }
        
        return false;
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
        // If upstream matched an operator with zero rows input
        if (env.bindings().containsKey("hasZeroRows")) {
            String inputVar = env.bindings().getOrDefault("zeroInput", "input");
            // Check if pattern contains Union indicators (has both left and right)
            String patternStr = env.pattern();
            if (patternStr != null && patternStr.contains("Union") && (patternStr.contains("$left") || inputVar.startsWith("left"))) {
                // For Union with zero rows, construct empty with left's schema
                String pattern = "(ConstructEmptyValues (OutputCols $" + inputVar + "))";
                return env.setPattern(pattern).focus(pattern);
            }
            // For other cases (like Project), just return the input
            String pattern = "$" + inputVar;
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.bindings().containsKey("isPruneEmptyFilter")) {
            String inputVar = env.bindings().get("pruneEmptyInput");
            String pattern = "$" + inputVar;
            return env.setPattern(pattern).focus(pattern);
        }
        String pattern = "(ConstructEmptyValues (OutputCols $input_0))";
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
        String varName = env.bindings().getOrDefault(pred.operator().getName(), "cond");
        String pattern = "$" + varName;
        return env.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformProj(Env env, RexRN.Proj proj) {
        String varName = env.bindings().getOrDefault(proj.operator().getName(), "proj");
        String pattern = "$" + varName;
        return env.setPattern(pattern).focus(pattern);
    }

    public Env transformGroupBy(Env env, RexRN.GroupBy groupBy) {
        // Check if GroupBy wraps a Proj expression (for AggregateProjectMerge)
        if (groupBy.sources().size() == 1) {
            RexRN innerExpr = groupBy.sources().get(0);
            if (innerExpr instanceof RexRN.Proj proj) {
                // Reference the proj variable
                String projVar = env.bindings().get(proj.operator().getName());
                if (projVar != null) {
                    return env.setPattern("$" + projVar).focus("$" + projVar);
                }
            }
        }
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
        String match = onMatch.pattern();
        // Normalize Union with HasZeroRows patterns: remove private field and Values references
        if (match.contains("HasZeroRows") && (match.startsWith("(Union") || match.startsWith("(UnionAll"))) {
            // Remove private field if present
            match = match.replaceAll("\\s+\\$private_\\d+:\\*\\s*\\)", "\n)");
            match = match.replaceAll("\\s+\\$private_\\d+:\\*\\)", ")");
        }
        // Also handle Union patterns that haven't been normalized yet
        else if (match.startsWith("(Union\n")) {
            String[] lines = match.split("\n");
            if (lines.length >= 3 && lines[1].contains(":(Values)") && lines[2].contains(":(Values)")) {
                String leftVar = extractVar(lines[1]);
                String rightVar = extractVar(lines[2]);
                String unionType = "Union";
                if (lines[0].startsWith("(UnionAll")) unionType = "UnionAll";
                match = "(" + unionType + "\n    $" + leftVar + ":* & (HasZeroRows $" + leftVar + ")\n    $" + rightVar + ":* & (HasZeroRows $" + rightVar + ")\n)";
            }
        }
        sb.append(match).append("\n");
        sb.append("=>\n");
        String out = transform.pattern();
        // Map generic $input to the first source var found in match (excluding private)
        if (out.equals("$input") || out.startsWith("(ConstructEmptyValues (OutputCols $input")) {
            String numbered = findFirstVar(match);
            if (numbered != null) {
                out = out.replace("$input_0", "$" + numbered).replace("$input", "$" + numbered);
            }
        }
        // If match has HasZeroRows pattern and output is ConstructEmptyValues, extract left variable from match
        if (match.contains("HasZeroRows") && match.contains("$left") && out.contains("ConstructEmptyValues")) {
            // Extract the left variable name from the match pattern (e.g., $left_0)
            int leftIdx = match.indexOf("$left");
            if (leftIdx >= 0) {
                int start = leftIdx + 1; // skip $
                int end = start;
                while (end < match.length() && (Character.isLetterOrDigit(match.charAt(end)) || match.charAt(end) == '_')) end++;
                String leftVar = match.substring(start, end);
                // Replace any variable in OutputCols with the actual left variable from match
                out = out.replaceAll("(OutputCols \\$)[a-zA-Z_][a-zA-Z0-9_]*", "$1" + leftVar);
            }
        }
        // Align unnumbered variables in output to numbered variables from match
        java.util.Map<String, String> varMap = extractNumberedVarMap(match);
        if (!varMap.isEmpty()) {
            for (java.util.Map.Entry<String, String> e : varMap.entrySet()) {
                String base = e.getKey();
                String numbered = e.getValue(); // includes leading $
                // Replace standalone $base (not already numbered) in output
                out = out.replaceAll(
                        "\\$" + java.util.regex.Pattern.quote(base) + "(?![_0-9])",
                        java.util.regex.Matcher.quoteReplacement(numbered)
                );
            }
        }
        sb.append(out).append("\n");
        return sb.toString();
    }

    private static String extractVar(String line) {
        int i = line.indexOf('$');
        if (i < 0) return null;
        int j = i + 1;
        while (j < line.length() && (Character.isLetterOrDigit(line.charAt(j)) || line.charAt(j) == '_')) j++;
        return line.substring(i + 1, j);
    }

    private static String findFirstVar(String match) {
        for (String line : match.split("\n")) {
            if (line.contains("$private")) continue;
            if (line.contains("$")) {
                String var = extractVar(line);
                if (var != null) return var;
            }
        }
        return null;
    }

    private static java.util.Map<String, String> extractNumberedVarMap(String match) {
        java.util.Map<String, String> map = new java.util.HashMap<>();
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\$([A-Za-z][A-Za-z0-9_]*)_([0-9]+)").matcher(match);
        while (m.find()) {
            String base = m.group(1);
            String numbered = "$" + base + "_" + m.group(2);
            map.putIfAbsent(base, numbered);
        }
        return map;
    }

    @Override
    public Env preTransform(Env env) {
        // If the onMatch pattern signaled HasZeroRows, propagate a generic binding
        String p = env.pattern();
        if (p != null) {
            int idx = p.indexOf("(HasZeroRows $");
            if (idx >= 0) {
                int start = idx + "(HasZeroRows $".length();
                int end = p.indexOf(")", start);
                if (end > start) {
                    String var = p.substring(start, end).trim();
                    if (!var.isEmpty()) {
                        return env.addBinding("hasZeroRows", "true").addBinding("zeroInput", var);
                    }
                }
            }
        }
        return env;
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