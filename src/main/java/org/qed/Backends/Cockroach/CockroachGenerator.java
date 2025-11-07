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
        if (env.rulename.equals("ProjectAggregateMerge") && project.source() instanceof RelRN.Aggregate aggregate) {
            String inputVar = env.generateVar("input");
            Env inputEnv = env.addBinding("input", inputVar);
            
            Env aggInputEnv = onMatch(inputEnv, aggregate.source());
            String aggInputPattern = aggInputEnv.current();
            String aggInputVar = aggInputPattern.replace("$", "").replace(":*", "").trim();
            Env aggInputBoundEnv = aggInputEnv.addBinding("aggInput", aggInputVar);
            
            Env aggsEnv = onMatchAggCalls(aggInputBoundEnv, aggregate.aggCalls());
            String aggsPattern = aggsEnv.current();
            String aggregationsVar = aggsEnv.generateVar("aggregations");
            Env aggregationsBindEnv = aggsEnv.addBinding("aggregations", aggregationsVar);
            
            Env groupingEnv = onMatchGroupSet(aggregationsBindEnv, aggregate.groupSet());
            String groupingPattern = groupingEnv.current();
            String groupingPrivateVar = groupingEnv.generateVar("groupingPrivate");
            Env groupingPrivateBindEnv = groupingEnv.addBinding("groupingPrivate", groupingPrivateVar);
            
            Env projEnv = onMatch(groupingPrivateBindEnv, project.map());
            String projPattern = projEnv.current();
            String projectionsVar = projEnv.generateVar("projections");
            Env projectionsBindEnv = projEnv.addBinding("projections", projectionsVar);
            
            String passthroughVar = projectionsBindEnv.generateVar("passthrough");
            Env passthroughBindEnv = projectionsBindEnv.addBinding("passthrough", passthroughVar);
            
            String neededVar = passthroughBindEnv.generateVar("needed");
            Env neededBindEnv = passthroughBindEnv.addBinding("needed", neededVar);
            
            String pattern = "(Project\n"
                    + "    $" + inputVar + ":(GroupBy\n"
                    + "        $" + aggInputVar + ":*\n"
                    + "        $" + aggregationsVar + ":*\n"
                    + "        $" + groupingPrivateVar + ":*\n"
                    + "    )\n"
                    + "    $" + projectionsVar + ":*\n"
                    + "    $" + passthroughVar + ":* &\n"
                    + "        (CanPruneAggCols\n"
                    + "            $" + aggregationsVar + "\n"
                    + "            $" + neededVar + ":(UnionCols\n"
                    + "                (ProjectionOuterCols $" + projectionsVar + ")\n"
                    + "                $" + passthroughVar + "\n"
                    + "            )\n"
                    + "        )\n"
                    + ")";
            return neededBindEnv.setPattern(pattern).focus(pattern);
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
        if (join.cond() instanceof RexRN.And and) {
            if (and.sources().size() == 2) {
                boolean hasTrue = false;
                boolean hasFalse = false;
                RexRN otherCond = null;
                
                for (RexRN side : and.sources()) {
                    if (side instanceof RexRN.True) {
                        hasTrue = true;
                    } else if (side instanceof RexRN.False) {
                        hasFalse = true;
                    } else {
                        otherCond = side;
                    }
                }
                
                if (hasTrue && otherCond != null) {
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
            if (join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.INNER
                    && and.sources().size() > 2) {
                String leftVar = env.generateVar("left");
                Env leftEnv = env.addBinding("left", leftVar);
                String rightVar = leftEnv.generateVar("right");
                Env rightEnv = leftEnv.addBinding("right", rightVar);
                String onVar = rightEnv.generateVar("on");
                Env onEnv = rightEnv.addBinding("on", onVar);
                String privateVar = onEnv.generateVar("private");
                Env privateEnv = onEnv.addBinding("private", privateVar);
                String pattern = "(InnerJoin\n"
                        + "    $" + leftVar + ":* & ^(HasOuterCols $" + leftVar + ")\n"
                        + "    $" + rightVar + ":* & ^(HasOuterCols $" + rightVar + ")\n"
                        + "    $" + onVar + ":*\n"
                        + "    $" + privateVar + ":*\n"
                        + ")";
                return privateEnv.setPattern(pattern).focus(pattern);
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
        
        if (env.rulename.equals("JoinExtractFilter") && 
            join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.INNER && 
            !(join.cond() instanceof RexRN.And)) {
            String leftVar = privateEnv.generateVar("left");
            String rightVar = privateEnv.generateVar("right");
            String onVar = privateEnv.generateVar("on");
            Env boundEnv = privateEnv.addBinding("left", leftVar)
                    .addBinding("right", rightVar)
                    .addBinding("on", onVar)
                    .addBinding("private", privateVar);
            if (join.cond() instanceof RexRN.Pred pred) {
                boundEnv = boundEnv.addBinding(pred.operator().getName(), onVar);
            }
            String joinType = getJoinType(join.ty().semantics());
            String pattern = "(" + joinType + "\n    $" + leftVar + ":*\n    $" + rightVar + ":*\n    $" + onVar + ":*\n    $" + privateVar + ":*\n)";
            return boundEnv.setPattern(pattern).focus(pattern);
        }
        
        String joinType = getJoinType(join.ty().semantics());
        String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    " + condPattern + "\n    $" + privateVar + ":*\n)";
        return privateEnv.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformJoin(Env env, RelRN.Join join) {
        if (env.bindings().containsKey("joinReduceTrue")) {
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
        if (join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.INNER
                && env.bindings().containsKey("left")
                && env.bindings().containsKey("right")
                && env.bindings().containsKey("on")
                && env.bindings().containsKey("private")
                && !env.bindings().containsKey("joinReduceTrue")
                && !env.bindings().containsKey("joinReduceFalse")) {
            String leftVar = env.bindings().get("left");
            String rightVar = env.bindings().get("right");
            String onVar = env.bindings().get("on");
            String privateVar = env.bindings().get("private");
            String pattern = "(InnerJoin\n"
                    + "    (Select $" + leftVar + " (ExtractBoundConditions $" + onVar + " (OutputCols $" + leftVar + ")))\n"
                    + "    (Select $" + rightVar + " (ExtractBoundConditions $" + onVar + " (OutputCols $" + rightVar + ")))\n"
                    + "    (ExtractUnboundConditions\n"
                    + "        (ExtractUnboundConditions $" + onVar + " (OutputCols $" + leftVar + "))\n"
                    + "        (OutputCols $" + rightVar + ")\n"
                    + "    )\n"
                    + "    $" + privateVar + "\n"
                    + ")";
            return env.setPattern(pattern).focus(pattern);
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
        if (env.rulename.equals("UnionToDistinct") && union.sources().size() == 2 && !union.all()) {
            RelRN leftSource = union.sources().get(0);
            RelRN rightSource = union.sources().get(1);
            
            Env leftEnv = onMatch(env, leftSource);
            String leftVar = leftEnv.generateVar("left");
            Env leftBoundEnv = leftEnv.addBinding("left", leftVar);
            
            Env rightEnv = onMatch(leftBoundEnv, rightSource);
            String rightVar = rightEnv.generateVar("right");
            Env rightBoundEnv = rightEnv.addBinding("right", rightVar);
            
            String privateVar = rightBoundEnv.generateVar("private");
            Env privateBindEnv = rightBoundEnv.addBinding("private", privateVar);
            String outColsVar = privateBindEnv.generateVar("outCols");
            Env outColsEnv = privateBindEnv.addBinding("outCols", outColsVar);
            
            String unionType = "Union";
            String pattern = "(" + unionType + "\n"
                    + "    $" + leftVar + ":*\n"
                    + "    $" + rightVar + ":*\n"
                    + "    $" + privateVar + ":(SetPrivate * * $" + outColsVar + ":*)\n"
                    + ")";
            return outColsEnv.setPattern(pattern).focus(pattern);
        }
        if (env.rulename.equals("UnionPullUpConstants") && union.sources().size() == 2) {
            RelRN leftSource = union.sources().get(0);
            RelRN rightSource = union.sources().get(1);
            
            Env leftEnv = onMatch(env, leftSource);
            String leftVar = leftEnv.generateVar("left");
            Env leftBoundEnv = leftEnv.addBinding("left", leftVar);
            
            String leftProjectionsVar;
            if (leftSource instanceof RelRN.Project) {
                leftProjectionsVar = leftBoundEnv.bindings().getOrDefault("projections", leftBoundEnv.generateVar("leftProjections"));
            } else {
                leftProjectionsVar = leftBoundEnv.generateVar("leftProjections");
            }
            Env leftProjectionsEnv = leftBoundEnv.addBinding("leftProjections", leftProjectionsVar);
            
            Env rightEnv = onMatch(leftProjectionsEnv, rightSource);
            String rightVar = rightEnv.generateVar("right");
            Env rightBoundEnv = rightEnv.addBinding("right", rightVar);
            
            String rightProjectionsVar;
            if (rightSource instanceof RelRN.Project) {
                rightProjectionsVar = rightBoundEnv.bindings().getOrDefault("projections", rightBoundEnv.generateVar("rightProjections"));
            } else {
                rightProjectionsVar = rightBoundEnv.generateVar("rightProjections");
            }
            Env rightProjectionsEnv = rightBoundEnv.addBinding("rightProjections", rightProjectionsVar);
            
            String privateVar = rightProjectionsEnv.generateVar("private");
            Env privateBindEnv = rightProjectionsEnv.addBinding("private", privateVar);
            String leftColsVar = privateBindEnv.generateVar("leftCols");
            Env leftColsEnv = privateBindEnv.addBinding("leftCols", leftColsVar);
            String rightColsVar = leftColsEnv.generateVar("rightCols");
            Env rightColsEnv = leftColsEnv.addBinding("rightCols", rightColsVar);
            String outColsVar = rightColsEnv.generateVar("outCols");
            Env outColsEnv = rightColsEnv.addBinding("outCols", outColsVar);
            
            String constantPositionsVar = outColsEnv.generateVar("constantPositions");
            Env constantPositionsEnv = outColsEnv.addBinding("constantPositions", constantPositionsVar);
            String constantValuesVar = constantPositionsEnv.generateVar("constantValues");
            Env constantValuesEnv = constantPositionsEnv.addBinding("constantValues", constantValuesVar);
            String okVar = constantValuesEnv.generateVar("ok");
            Env okEnv = constantValuesEnv.addBinding("ok", okVar);
            
            String unionType = union.all() ? "UnionAll" : "Union";
            String pattern = "(" + unionType + "\n"
                    + "    $" + leftVar + ":(Project * $" + leftProjectionsVar + ":* *)\n"
                    + "    $" + rightVar + ":(Project * $" + rightProjectionsVar + ":* *)\n"
                    + "    $" + privateVar + ":(SetPrivate $" + leftColsVar + ":* $" + rightColsVar + ":* $" + outColsVar + ":*) &\n"
                    + "        (Let\n"
                    + "            ($" + constantPositionsVar + " $" + constantValuesVar + " $" + okVar + "):(ExtractMatchingConstantsFromUnion\n"
                    + "                $" + leftProjectionsVar + "\n"
                    + "                $" + rightProjectionsVar + "\n"
                    + "                $" + leftColsVar + "\n"
                    + "                $" + rightColsVar + "\n"
                    + "                $" + outColsVar + "\n"
                    + "            )\n"
                    + "            $" + okVar + "\n"
                    + "        )\n"
                    + ")";
            return okEnv.setPattern(pattern).focus(pattern);
        }
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
        if (aggregate.source() instanceof RelRN.Join topJoin
                && topJoin.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.LEFT
                && topJoin.left() instanceof RelRN.Join bottomJoin
                && bottomJoin.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.LEFT) {
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
        if (aggregate.source() instanceof RelRN.Join join && join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.LEFT) {
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
        if (aggregate.source() instanceof RelRN.Project project) {
            if (env.rulename.equals("AggregateProjectConstantToDummyJoin")) {
                String inputVar = env.generateVar("input");
                Env inputEnv = env.addBinding("input", inputVar);
                
                Env projectInputEnv = onMatch(inputEnv, project.source());
                String currentPattern = projectInputEnv.current();
                String projectInputVar = currentPattern.replace("$", "").replace(":*", "").trim();
                if (project.source() instanceof RelRN.Scan scan) {
                    String scanVar = projectInputEnv.bindings().get(scan.name());
                    if (scanVar != null) {
                        projectInputVar = scanVar;
                    }
                }
                Env projectInputBoundEnv = projectInputEnv.addBinding("projectInput", projectInputVar);
                
                String projectionsVar = projectInputBoundEnv.generateVar("projections");
                Env projectionsEnv = projectInputBoundEnv.addBinding("projections", projectionsVar);
                String passthroughVar = projectionsEnv.generateVar("passthrough");
                Env passthroughEnv = projectionsEnv.addBinding("passthrough", passthroughVar);
                
                String aggregationsVar = passthroughEnv.generateVar("aggregations");
                Env aggregationsBindEnv = passthroughEnv.addBinding("aggregations", aggregationsVar);
                String groupingPrivateVar = aggregationsBindEnv.generateVar("groupingPrivate");
                Env groupingPrivateBindEnv = aggregationsBindEnv.addBinding("groupingPrivate", groupingPrivateVar);
                
                String constantGroupingColsVar = groupingPrivateBindEnv.generateVar("constantGroupingCols");
                Env constantGroupingColsEnv = groupingPrivateBindEnv.addBinding("constantGroupingCols", constantGroupingColsVar);
                String constantValuesVar = constantGroupingColsEnv.generateVar("constantValues");
                Env constantValuesEnv = constantGroupingColsEnv.addBinding("constantValues", constantValuesVar);
                String okVar = constantValuesEnv.generateVar("ok");
                Env okEnv = constantValuesEnv.addBinding("ok", okVar);
                
                String aggregateType = determineAggregateType(aggregate);
                String pattern = "(" + aggregateType + "\n"
                        + "    $" + inputVar + ":(Project $" + projectInputVar + ":* $" + projectionsVar + ":* $" + passthroughVar + ":*)\n"
                        + "    $" + aggregationsVar + ":*\n"
                        + "    $" + groupingPrivateVar + ":* &\n"
                        + "        (Let\n"
                        + "            ($" + constantGroupingColsVar + " $" + constantValuesVar + " $" + okVar + "):(ExtractConstantGroupingCols\n"
                        + "                $" + inputVar + "\n"
                        + "                $" + groupingPrivateVar + "\n"
                        + "            )\n"
                        + "            $" + okVar + "\n"
                        + "        )\n"
                        + ")";
                return okEnv.setPattern(pattern).focus(pattern);
            }
            
            String inputVar = env.generateVar("input");
            Env inputEnv = env.addBinding("input", inputVar);
            String projectionsVar = inputEnv.generateVar("projections");
            Env projectionsEnv = inputEnv.addBinding("projections", projectionsVar);
            String passthroughVar = projectionsEnv.generateVar("passthrough");
            Env passthroughEnv = projectionsEnv.addBinding("passthrough", passthroughVar);
            
            String aggregationsVar = passthroughEnv.generateVar("aggregations");
            Env aggregationsBindEnv = passthroughEnv.addBinding("aggregations", aggregationsVar);
            String groupingPrivateVar = aggregationsBindEnv.generateVar("groupingPrivate");
            Env groupingPrivateBindEnv = aggregationsBindEnv.addBinding("groupingPrivate", groupingPrivateVar);
            
            String aggregateType = determineAggregateType(aggregate);
            String pattern = "(" + aggregateType + "\n"
                    + "    (Project\n"
                    + "        $" + inputVar + ":*\n"
                    + "        $" + projectionsVar + ":*\n"
                    + "        $" + passthroughVar + ":*\n"
                    + "    )\n"
                    + "    $" + aggregationsVar + ":*\n"
                    + "    $" + groupingPrivateVar + ":* & (CanRemapGroupingColsThroughProject $" + groupingPrivateVar + " $" + projectionsVar + " $" + passthroughVar + ")\n"
                    + ")";
            return groupingPrivateBindEnv.setPattern(pattern).focus(pattern);
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
        
        boolean hasProjectionExpressions = hasProjectionExpressionsInAggregate(aggregate);
        if (hasProjectionExpressions) {
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
            if (aggCall.operands().size() == 1) {
                RexRN operand = aggCall.operands().get(0);
                if (operand instanceof RexRN.Proj proj) {
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
        if (groupBy.sources().size() == 1) {
            RexRN innerExpr = groupBy.sources().get(0);
            if (innerExpr instanceof RexRN.Proj proj) {
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
    public Env onMatchCustom(Env env, RelRN custom) {
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.AggregateGroupingByConstants aggGrouping) {
            if (aggGrouping.input() instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.ProjectWithConstantLiterals projectWithConstants) {
                RelRN sourceRN = projectWithConstants.input();
                Env sourceEnv = onMatch(env, sourceRN);
                String projectInputVar = sourceEnv.current().replace("$", "").replace(":*", "");
                String projectionsVar = sourceEnv.generateVar("projections");
                Env projectionsEnv = sourceEnv.addBinding("projections", projectionsVar);
                String passthroughVar = projectionsEnv.generateVar("passthrough");
                Env passthroughEnv = projectionsEnv.addBinding("passthrough", passthroughVar);
                String inputVar = passthroughEnv.generateVar("input");
                Env inputEnv = passthroughEnv.addBinding("input", inputVar);
                String aggregationsVar = inputEnv.generateVar("aggregations");
                Env aggregationsBindEnv = inputEnv.addBinding("aggregations", aggregationsVar);
                String groupingPrivateVar = aggregationsBindEnv.generateVar("groupingPrivate");
                Env groupingPrivateBindEnv = aggregationsBindEnv.addBinding("groupingPrivate", groupingPrivateVar);
                String constantGroupingColsVar = groupingPrivateBindEnv.generateVar("constantGroupingCols");
                Env constantGroupingColsEnv = groupingPrivateBindEnv.addBinding("constantGroupingCols", constantGroupingColsVar);
                String constantValuesVar = constantGroupingColsEnv.generateVar("constantValues");
                Env constantValuesEnv = constantGroupingColsEnv.addBinding("constantValues", constantValuesVar);
                String dummyColsVar = constantValuesEnv.generateVar("dummyCols");
                Env dummyColsEnv = constantValuesEnv.addBinding("dummyCols", dummyColsVar);
                String okVar = dummyColsEnv.generateVar("ok");
                Env okEnv = dummyColsEnv.addBinding("ok", okVar).addBinding("projectInput", projectInputVar);
                
                String aggregateType = "GroupBy";
                String pattern = "(" + aggregateType + "\n"
                        + "    $" + inputVar + ":(Project $" + projectInputVar + ":* $" + projectionsVar + ":* $" + passthroughVar + ":*)\n"
                        + "    $" + aggregationsVar + ":*\n"
                        + "    $" + groupingPrivateVar + ":* &\n"
                        + "        (Let\n"
                        + "            ($" + constantGroupingColsVar + " $" + constantValuesVar + " $" + dummyColsVar + " $" + okVar + "):(ExtractConstantGroupingColsAndBuildDummy\n"
                        + "                $" + inputVar + "\n"
                        + "                $" + groupingPrivateVar + "\n"
                        + "            )\n"
                        + "            $" + okVar + "\n"
                        + "        )\n"
                        + ")";
                return okEnv.setPattern(pattern).focus(pattern);
            }
            return onMatch(env, aggGrouping.input());
        }
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.ProjectWithConstantLiterals projectWithConstants) {
            if (projectWithConstants.input() instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.SourceTable sourceTable) {
                var scanRN = new RelRN.Scan("Source", org.qed.RexRN.varType("Source_Type", true), false);
                return onMatchScan(env, scanRN);
            }
            return onMatch(env, projectWithConstants.input());
        }
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.SourceTable sourceTable) {
            var scanRN = new RelRN.Scan("Source", org.qed.RexRN.varType("Source_Type", true), false);
            return onMatchScan(env, scanRN);
        }
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge.ProjectUsingSubsetOfAggregates projectUsingSubset) {
            if (projectUsingSubset.input() instanceof org.qed.RRuleInstances.ProjectAggregateMerge.AggregateWithMultipleCalls aggregateWithMultiple) {
                RelRN sourceRN = aggregateWithMultiple.input();
                Env sourceEnv = onMatch(env, sourceRN);
                String aggInputVar = sourceEnv.current().replace("$", "").replace(":*", "").trim();
                Env aggInputBoundEnv = sourceEnv.addBinding("aggInput", aggInputVar);
                
                String aggregationsVar = aggInputBoundEnv.generateVar("aggregations");
                Env aggregationsBindEnv = aggInputBoundEnv.addBinding("aggregations", aggregationsVar);
                String groupingPrivateVar = aggregationsBindEnv.generateVar("groupingPrivate");
                Env groupingPrivateBindEnv = aggregationsBindEnv.addBinding("groupingPrivate", groupingPrivateVar);
                
                String inputVar = groupingPrivateBindEnv.generateVar("input");
                Env inputEnv = groupingPrivateBindEnv.addBinding("input", inputVar);
                String projectionsVar = inputEnv.generateVar("projections");
                Env projectionsBindEnv = inputEnv.addBinding("projections", projectionsVar);
                String passthroughVar = projectionsBindEnv.generateVar("passthrough");
                Env passthroughBindEnv = projectionsBindEnv.addBinding("passthrough", passthroughVar);
                
                String neededVar = passthroughBindEnv.generateVar("needed");
                Env neededBindEnv = passthroughBindEnv.addBinding("needed", neededVar);
                
                String pattern = "(Project\n"
                        + "    $" + inputVar + ":(GroupBy\n"
                        + "        $" + aggInputVar + ":*\n"
                        + "        $" + aggregationsVar + ":*\n"
                        + "        $" + groupingPrivateVar + ":*\n"
                        + "    )\n"
                        + "    $" + projectionsVar + ":*\n"
                        + "    $" + passthroughVar + ":* &\n"
                        + "        (CanPruneAggCols\n"
                        + "            $" + aggregationsVar + "\n"
                        + "            $" + neededVar + ":(UnionCols\n"
                        + "                (ProjectionOuterCols $" + projectionsVar + ")\n"
                        + "                $" + passthroughVar + "\n"
                        + "            )\n"
                        + "        )\n"
                        + ")";
                return neededBindEnv.setPattern(pattern).focus(pattern);
            }
            return onMatch(env, projectUsingSubset.input());
        }
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge.AggregateWithMultipleCalls aggregateWithMultiple) {
            return onMatch(env, aggregateWithMultiple.input());
        }
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge.SourceTable sourceTable) {
            var scanRN = new RelRN.Scan("Source", org.qed.RexRN.varType("Source_Type", true), false);
            return onMatchScan(env, scanRN);
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.UnionWithConstantColumns unionWithConstants) {
            return onMatchUnion(env, new RelRN.Union(true, Seq.of(unionWithConstants.left(), unionWithConstants.right())));
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.LeftProjectionWithConstants leftProj) {
            var projectRN = new RelRN.Project(leftProj.input().field(0), leftProj.input());
            return onMatchProject(env, projectRN);
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.RightProjectionWithConstants rightProj) {
            var projectRN = new RelRN.Project(rightProj.input().field(0), rightProj.input());
            return onMatchProject(env, projectRN);
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.SourceTable sourceTable) {
            var scanRN = new RelRN.Scan("Source", org.qed.RexRN.varType("Source_Type", true), false);
            return onMatchScan(env, scanRN);
        }
        if (custom instanceof org.qed.RRuleInstances.UnionToDistinct.DistinctUnion distinctUnion) {
            return onMatchUnion(env, new RelRN.Union(false, Seq.of(distinctUnion.left(), distinctUnion.right())));
        }
        if (custom instanceof org.qed.RRuleInstances.UnionToDistinct.UnionAll unionAll) {
            return onMatchUnion(env, new RelRN.Union(true, Seq.of(unionAll.left(), unionAll.right())));
        }
        return unimplementedOnMatch(env, custom);
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
        if (env.bindings().containsKey("hasZeroRows")) {
            String inputVar = env.bindings().getOrDefault("zeroInput", "input");
            String pattern = "$" + inputVar;
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.rulename.equals("ProjectAggregateMerge") 
                && env.bindings().containsKey("aggInput")
                && env.bindings().containsKey("aggregations")
                && env.bindings().containsKey("groupingPrivate")
                && env.bindings().containsKey("projections")
                && env.bindings().containsKey("passthrough")
                && env.bindings().containsKey("needed")) {
            
            String aggInputVar = env.bindings().get("aggInput");
            String aggregationsVar = env.bindings().get("aggregations");
            String groupingPrivateVar = env.bindings().get("groupingPrivate");
            String projectionsVar = env.bindings().get("projections");
            String passthroughVar = env.bindings().get("passthrough");
            String neededVar = env.bindings().get("needed");
            
            if (project.source() instanceof RelRN.Aggregate aggregate) {
                Env aggInputEnv = transform(env, aggregate.source());
                String aggInputPattern = aggInputEnv.current();
                
                String pattern = "(Project\n"
                        + "    (GroupBy\n"
                        + "        " + aggInputPattern + "\n"
                        + "        (PruneAggCols $" + aggregationsVar + " $" + neededVar + ")\n"
                        + "        $" + groupingPrivateVar + "\n"
                        + "    )\n"
                        + "    $" + projectionsVar + "\n"
                        + "    $" + passthroughVar + "\n"
                        + ")";
                return aggInputEnv.setPattern(pattern).focus(pattern);
            }
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
        if (env.rulename.equals("UnionToDistinct") 
                && env.bindings().containsKey("left")
                && env.bindings().containsKey("right")
                && env.bindings().containsKey("private")
                && env.bindings().containsKey("outCols")) {
            
            String leftVar = env.bindings().get("left");
            String rightVar = env.bindings().get("right");
            String privateVar = env.bindings().get("private");
            String outColsVar = env.bindings().get("outCols");
            
            String pattern = "(DistinctOn\n"
                    + "    (UnionAll $" + leftVar + " $" + rightVar + " $" + privateVar + ")\n"
                    + "    []\n"
                    + "    (MakeGrouping (ColListToSet $" + outColsVar + ") (EmptyOrdering))\n"
                    + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.rulename.equals("UnionPullUpConstants") 
                && env.bindings().containsKey("left")
                && env.bindings().containsKey("right")
                && env.bindings().containsKey("leftProjections")
                && env.bindings().containsKey("rightProjections")
                && env.bindings().containsKey("private")
                && env.bindings().containsKey("leftCols")
                && env.bindings().containsKey("rightCols")
                && env.bindings().containsKey("outCols")
                && env.bindings().containsKey("constantPositions")
                && env.bindings().containsKey("constantValues")) {
            
            String leftVar = env.bindings().get("left");
            String rightVar = env.bindings().get("right");
            String privateVar = env.bindings().get("private");
            String leftColsVar = env.bindings().get("leftCols");
            String rightColsVar = env.bindings().get("rightCols");
            String outColsVar = env.bindings().get("outCols");
            String constantPositionsVar = env.bindings().get("constantPositions");
            String constantValuesVar = env.bindings().get("constantValues");
            
            Env leftEnv = transform(env, union.sources().get(0));
            String leftPattern = leftEnv.current();
            Env rightEnv = transform(leftEnv, union.sources().get(1));
            String rightPattern = rightEnv.current();
            
            String unionType = union.all() ? "UnionAll" : "Union";
            String pattern = "(Project\n"
                    + "    (" + unionType + "\n"
                    + "        (PruneCols $" + leftVar + " (ComputeNeededColsForUnionPullUp $" + constantPositionsVar + " $" + outColsVar + "))\n"
                    + "        (PruneCols $" + rightVar + " (ComputeNeededColsForUnionPullUp $" + constantPositionsVar + " $" + outColsVar + "))\n"
                    + "        (PruneSetPrivate (ComputeNeededColsForUnionPullUp $" + constantPositionsVar + " $" + outColsVar + ") $" + privateVar + ")\n"
                    + "    )\n"
                    + "    (AddConstantsToProjections $" + constantPositionsVar + " $" + constantValuesVar + " $" + outColsVar + ")\n"
                    + "    (MakeEmptyColSet)\n"
                    + ")";
            return rightEnv.setPattern(pattern).focus(pattern);
        }
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
        if (env.rulename.equals("AggregateProjectConstantToDummyJoin") 
                && env.bindings().containsKey("projectInput")
                && env.bindings().containsKey("projections")
                && env.bindings().containsKey("passthrough")
                && env.bindings().containsKey("aggregations")
                && env.bindings().containsKey("groupingPrivate")
                && env.bindings().containsKey("constantGroupingCols")
                && env.bindings().containsKey("constantValues")) {
            if (aggregate.source() instanceof RelRN.Project project) {
                Env projectInputEnv = transform(env, project.source());
                String projectInputPattern = projectInputEnv.current();
                
                String projectInputVar = env.bindings().get("projectInput");
                String projectionsVar = env.bindings().get("projections");
                String passthroughVar = env.bindings().get("passthrough");
                String aggregationsVar = env.bindings().get("aggregations");
                String groupingPrivateVar = env.bindings().get("groupingPrivate");
                String constantGroupingColsVar = env.bindings().get("constantGroupingCols");
                String constantValuesVar = env.bindings().get("constantValues");
                String aggregateType = determineAggregateType(aggregate);
                
                String pattern = "(ConstructGroupByWithDummyJoin\n"
                        + "    " + projectInputPattern + "\n"
                        + "    $" + projectionsVar + "\n"
                        + "    $" + passthroughVar + "\n"
                        + "    $" + aggregationsVar + "\n"
                        + "    $" + groupingPrivateVar + "\n"
                        + "    $" + constantGroupingColsVar + "\n"
                        + "    $" + constantValuesVar + "\n"
                        + "    " + aggregateType + "\n"
                        + ")";
                return projectInputEnv.setPattern(pattern).focus(pattern);
            }
        }
        
        if (env.bindings().containsKey("input") && env.bindings().containsKey("projections")
                && env.bindings().containsKey("passthrough") && env.bindings().containsKey("aggregations")
                && env.bindings().containsKey("groupingPrivate")) {
            String inputVar = env.bindings().get("input");
            String projectionsVar = env.bindings().get("projections");
            String passthroughVar = env.bindings().get("passthrough");
            String aggregationsVar = env.bindings().get("aggregations");
            String groupingPrivateVar = env.bindings().get("groupingPrivate");
            String aggregateType = determineAggregateType(aggregate);
            String pattern = "(" + aggregateType + "\n"
                    + "    $" + inputVar + "\n"
                    + "    (RemapAggregationsThroughProject $" + aggregationsVar + " $" + projectionsVar + ")\n"
                    + "    (RemapGroupingColsThroughProject $" + groupingPrivateVar + " $" + projectionsVar + " $" + passthroughVar + ")\n"
                    + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        
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
        for (RexRN groupExpr : aggregate.groupSet()) {
            if (groupExpr instanceof RexRN.Proj) {
                return true;
            }
        }
        
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
        if (env.bindings().containsKey("hasZeroRows")) {
            String inputVar = env.bindings().getOrDefault("zeroInput", "input");
            String patternStr = env.pattern();
            if (patternStr != null && patternStr.contains("Union") && (patternStr.contains("$left") || inputVar.startsWith("left"))) {
                String pattern = "(ConstructEmptyValues (OutputCols $" + inputVar + "))";
                return env.setPattern(pattern).focus(pattern);
            }
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
        if (groupBy.sources().size() == 1) {
            RexRN innerExpr = groupBy.sources().get(0);
            if (innerExpr instanceof RexRN.Proj proj) {
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
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.AggregateGroupingByDummyFields aggGrouping) {
            if (env.rulename.equals("AggregateProjectConstantToDummyJoin") 
                    && env.bindings().containsKey("projectInput")
                    && env.bindings().containsKey("projections")
                    && env.bindings().containsKey("passthrough")
                    && env.bindings().containsKey("aggregations")
                    && env.bindings().containsKey("groupingPrivate")
                    && env.bindings().containsKey("constantGroupingCols")
                    && env.bindings().containsKey("constantValues")
                    && env.bindings().containsKey("dummyCols")) {
                
                if (aggGrouping.input() instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.ProjectWithDummyFields projectWithDummy) {
                    if (projectWithDummy.input() instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.JoinWithDummyTable joinWithDummy) {
                        Env projectInputEnv = transform(env, joinWithDummy.baseTable());
                        String projectInputPattern = projectInputEnv.current();
                        
                        String projectInputVar = env.bindings().get("projectInput");
                        String projectionsVar = env.bindings().get("projections");
                        String passthroughVar = env.bindings().get("passthrough");
                        String aggregationsVar = env.bindings().get("aggregations");
                        String groupingPrivateVar = env.bindings().get("groupingPrivate");
                        String constantGroupingColsVar = env.bindings().get("constantGroupingCols");
                        String constantValuesVar = env.bindings().get("constantValues");
                        String dummyColsVar = env.bindings().get("dummyCols");
                        String aggregateType = "GroupBy";
                        
                        String pattern = "(" + aggregateType + "\n"
                                + "    (Project\n"
                                + "        (InnerJoin\n"
                                + "            " + projectInputPattern + "\n"
                                + "            (ConstructDummyValuesTable $" + constantValuesVar + " $" + dummyColsVar + ")\n"
                                + "            [ (FiltersItem (True)) ]\n"
                                + "            (EmptyJoinPrivate)\n"
                                + "        )\n"
                                + "        (RemapProjectionsForDummyJoin $" + projectionsVar + " $" + constantGroupingColsVar + " $" + dummyColsVar + ")\n"
                                + "        (IntersectionCols $" + passthroughVar + " (OutputCols " + projectInputPattern + "))\n"
                                + "    )\n"
                                + "    $" + aggregationsVar + "\n"
                                + "    (AddColsToGrouping\n"
                                + "        (RemoveGroupingCols $" + groupingPrivateVar + " $" + constantGroupingColsVar + ")\n"
                                + "        (ColListToSet $" + dummyColsVar + ")\n"
                                + "    )\n"
                                + ")";
                        return projectInputEnv.setPattern(pattern).focus(pattern);
                    }
                }
            }
            return transform(env, aggGrouping.input());
        }
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.ProjectWithDummyFields projectWithDummy) {
            return transform(env, projectWithDummy.input());
        }
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.JoinWithDummyTable joinWithDummy) {
            return transform(env, joinWithDummy.baseTable());
        }
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.DummyConstantsTable dummyTable) {
            return env;
        }
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.SourceTable sourceTable) {
            String varName = env.bindings().getOrDefault("projectInput", "input_0");
            return env.setPattern("$" + varName).focus("$" + varName);
        }
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge.ProjectOptimized projectOptimized) {
            if (env.rulename.equals("ProjectAggregateMerge") 
                    && env.bindings().containsKey("aggInput")
                    && env.bindings().containsKey("aggregations")
                    && env.bindings().containsKey("groupingPrivate")
                    && env.bindings().containsKey("projections")
                    && env.bindings().containsKey("passthrough")
                    && env.bindings().containsKey("needed")) {
                
                if (projectOptimized.input() instanceof org.qed.RRuleInstances.ProjectAggregateMerge.AggregateWithUsedCallsOnly aggregateOptimized) {
                    Env aggInputEnv = transform(env, aggregateOptimized.input());
                    String aggInputPattern = aggInputEnv.current();
                    
                    String aggInputVar = env.bindings().get("aggInput");
                    String aggregationsVar = env.bindings().get("aggregations");
                    String groupingPrivateVar = env.bindings().get("groupingPrivate");
                    String projectionsVar = env.bindings().get("projections");
                    String passthroughVar = env.bindings().get("passthrough");
                    String neededVar = env.bindings().get("needed");
                    
                    String pattern = "(Project\n"
                            + "    (GroupBy\n"
                            + "        " + aggInputPattern + "\n"
                            + "        (PruneAggCols $" + aggregationsVar + " $" + neededVar + ")\n"
                            + "        $" + groupingPrivateVar + "\n"
                            + "    )\n"
                            + "    $" + projectionsVar + "\n"
                            + "    $" + passthroughVar + "\n"
                            + ")";
                    return aggInputEnv.setPattern(pattern).focus(pattern);
                }
            }
            return transform(env, projectOptimized.input());
        }
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge.AggregateWithUsedCallsOnly aggregateOptimized) {
            return transform(env, aggregateOptimized.input());
        }
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge.SourceTable sourceTable) {
            String varName = env.bindings().getOrDefault("aggInput", "input_0");
            return env.setPattern("$" + varName).focus("$" + varName);
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.TopProjectionWithConstants topProj) {
            if (env.rulename.equals("UnionPullUpConstants") 
                    && env.bindings().containsKey("left")
                    && env.bindings().containsKey("right")
                    && env.bindings().containsKey("private")
                    && env.bindings().containsKey("constantPositions")
                    && env.bindings().containsKey("constantValues")
                    && env.bindings().containsKey("outCols")) {
                
                if (topProj.input() instanceof org.qed.RRuleInstances.UnionPullUpConstants.UnionReducedColumns unionReduced) {
                    Env unionEnv = transform(env, new RelRN.Union(true, Seq.of(unionReduced.left(), unionReduced.right())));
                    String unionPattern = unionEnv.current();
                    
                    String leftVar = env.bindings().get("left");
                    String rightVar = env.bindings().get("right");
                    String privateVar = env.bindings().get("private");
                    String constantPositionsVar = env.bindings().get("constantPositions");
                    String constantValuesVar = env.bindings().get("constantValues");
                    String outColsVar = env.bindings().get("outCols");
                    
                    String pattern = "(Project\n"
                            + "    (UnionAll\n"
                            + "        (PruneCols $" + leftVar + " (ComputeNeededColsForUnionPullUp $" + constantPositionsVar + " $" + outColsVar + "))\n"
                            + "        (PruneCols $" + rightVar + " (ComputeNeededColsForUnionPullUp $" + constantPositionsVar + " $" + outColsVar + "))\n"
                            + "        (PruneSetPrivate (ComputeNeededColsForUnionPullUp $" + constantPositionsVar + " $" + outColsVar + ") $" + privateVar + ")\n"
                            + "    )\n"
                            + "    (AddConstantsToProjections $" + constantPositionsVar + " $" + constantValuesVar + " $" + outColsVar + ")\n"
                            + "    (MakeEmptyColSet)\n"
                            + ")";
                    return unionEnv.setPattern(pattern).focus(pattern);
                }
            }
            return transform(env, topProj.input());
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.UnionReducedColumns unionReduced) {
            Env leftEnv = transform(env, unionReduced.left());
            Env rightEnv = transform(leftEnv, unionReduced.right());
            return rightEnv;
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.LeftProjectionNonConstants leftProj) {
            return transform(env, leftProj.input());
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.RightProjectionNonConstants rightProj) {
            return transform(env, rightProj.input());
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.SourceTable sourceTable) {
            String varName = env.bindings().getOrDefault("left", 
                    env.bindings().getOrDefault("right", "input_0"));
            return env.setPattern("$" + varName).focus("$" + varName);
        }
        if (custom instanceof org.qed.RRuleInstances.UnionToDistinct.DistinctAggregate distinctAgg) {
            if (env.rulename.equals("UnionToDistinct") 
                    && env.bindings().containsKey("left")
                    && env.bindings().containsKey("right")
                    && env.bindings().containsKey("private")
                    && env.bindings().containsKey("outCols")) {
                
                String leftVar = env.bindings().get("left");
                String rightVar = env.bindings().get("right");
                String privateVar = env.bindings().get("private");
                String outColsVar = env.bindings().get("outCols");
                
                String pattern = "(DistinctOn\n"
                        + "    (UnionAll $" + leftVar + " $" + rightVar + " $" + privateVar + ")\n"
                        + "    []\n"
                        + "    (MakeGrouping (ColListToSet $" + outColsVar + ") (EmptyOrdering))\n"
                        + ")";
                return env.setPattern(pattern).focus(pattern);
            }
            return transform(env, distinctAgg.input());
        }
        if (custom instanceof org.qed.RRuleInstances.UnionToDistinct.UnionAll unionAll) {
            Env leftEnv = transform(env, unionAll.left());
            Env rightEnv = transform(leftEnv, unionAll.right());
            return rightEnv;
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
        if (name.equals("PruneEmptyProject")) {
            match = match.replaceAll("\\$projections_\\d+", java.util.regex.Matcher.quoteReplacement("$projections"));
            match = match.replaceAll("\\$passthrough_\\d+", java.util.regex.Matcher.quoteReplacement("$passthrough"));
        }
        if (name.equals("PruneEmptyFilter")) {
            match = match.replaceAll("\\$filters_\\d+", java.util.regex.Matcher.quoteReplacement("$filters"));
        }
        if (match.contains("HasZeroRows") && (match.startsWith("(Union") || match.startsWith("(UnionAll"))) {
            match = match.replaceAll("\\s+\\$private_\\d+:\\*\\s*\\)", "\n)");
            match = match.replaceAll("\\s+\\$private_\\d+:\\*\\)", ")");
        }
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
        if (out.startsWith("(ConstructEmptyValues (OutputCols $")) {
            int startIdx = "(ConstructEmptyValues (OutputCols $".length();
            int endIdx = startIdx;
            while (endIdx < out.length() && (Character.isLetterOrDigit(out.charAt(endIdx)) || out.charAt(endIdx) == '_')) {
                endIdx++;
            }
            String varInOutput = out.substring(startIdx, endIdx);
            if (varInOutput.equals("input")) {
                String numbered = findFirstVar(match);
                if (numbered != null) {
                    out = out.replace("(ConstructEmptyValues (OutputCols $input)", 
                                     "(ConstructEmptyValues (OutputCols $" + numbered + ")");
                }
            }
        } else if (out.equals("$input")) {
            String numbered = findFirstVar(match);
            if (numbered != null) {
                out = "$" + numbered;
            }
        }
        if (match.contains("HasZeroRows") && match.contains("$left") && out.contains("ConstructEmptyValues")) {
            int leftIdx = match.indexOf("$left");
            if (leftIdx >= 0) {
                int start = leftIdx + 1;
                int end = start;
                while (end < match.length() && (Character.isLetterOrDigit(match.charAt(end)) || match.charAt(end) == '_')) end++;
                String leftVar = match.substring(start, end);
                out = out.replaceAll("(OutputCols \\$)[a-zA-Z_][a-zA-Z0-9_]*", "$1" + leftVar);
            }
        }
        java.util.Map<String, String> varMap = extractNumberedVarMap(match);
        if (!varMap.isEmpty()) {
            for (java.util.Map.Entry<String, String> e : varMap.entrySet()) {
                String base = e.getKey();
                String numbered = e.getValue();
                out = out.replaceAll(
                        "\\$" + java.util.regex.Pattern.quote(base) + "(?![_0-9])",
                        java.util.regex.Matcher.quoteReplacement(numbered)
                );
            }
        }
        if (name.equals("PruneEmptyProject")) {
            out = out.replaceAll("\\$projections_\\d+", java.util.regex.Matcher.quoteReplacement("$projections"));
            out = out.replaceAll("\\$passthrough_\\d+", java.util.regex.Matcher.quoteReplacement("$passthrough"));
        }
        if (name.equals("PruneEmptyFilter")) {
            out = out.replaceAll("\\$filters_\\d+", java.util.regex.Matcher.quoteReplacement("$filters"));
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