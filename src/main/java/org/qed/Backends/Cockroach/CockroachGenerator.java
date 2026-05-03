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
        return env.addBinding(scan.name(), varName).focus("$" + varName + ":*");
    }

    @Override
    public Env onMatchFilter(Env env, RelRN.Filter filter) {
        if (filter.source() instanceof RelRN.Project project) {
            String inputVar = env.generateVar("input");
            Env inputEnv = env.addBinding("input", inputVar);
            String projVar = inputEnv.generateVar("proj");
            Env projEnv = inputEnv.addBinding("proj", projVar);
            String passthroughVar = projEnv.generateVar("passthrough");
            Env passthroughEnv = projEnv.addBinding("passthrough", passthroughVar);
            String condVar = passthroughEnv.generateVar("cond");
            Env condBoundEnv = passthroughEnv.addBinding("cond", condVar);
            String inputColsVar = condBoundEnv.generateVar("inputCols");
            Env inputColsEnv = condBoundEnv.addBinding("inputCols", inputColsVar);
            String pattern = "(Select\n" + "    (Project\n" + "    $" + inputVar + ":*\n" + "    $" + projVar + ":*\n" + "    $" + passthroughVar + ":*\n" + ")\n" + "    $" + condVar + ":* &\n" + "    (FiltersBoundBy $" + condVar + " $" + inputColsVar + ":(OutputCols $" + inputVar + "))\n" + ")";
            return inputColsEnv.setPattern(pattern).focus(pattern);
        }
        if (filter.source() instanceof RelRN.Union union) {
            String inputVar = env.generateVar("input");
            Env inputEnv = env.addBinding("input", inputVar);
            String leftVar = inputEnv.generateVar("left");
            Env leftEnv = inputEnv.addBinding("left", leftVar);
            String rightVar = leftEnv.generateVar("right");
            Env rightEnv = leftEnv.addBinding("right", rightVar);
            String colmapVar = rightEnv.generateVar("colmap");
            Env colmapEnv = rightEnv.addBinding("colmap", colmapVar);
            String filterVar = colmapEnv.generateVar("filter");
            Env filterBindEnv = colmapEnv.addBinding("filter", filterVar);
            String itemVar = filterBindEnv.generateVar("item");
            Env itemBindEnv = filterBindEnv.addBinding("item", itemVar);
            String inputColsVar = itemBindEnv.generateVar("inputCols");
            Env inputColsBindEnv = itemBindEnv.addBinding("inputCols", inputColsVar);
            String pattern = "(Select\n" + "    $" + inputVar + ":(Union $" + leftVar + ":* $" + rightVar + ":* $" + colmapVar + ":*)\n" + "    $" + filterVar + ":[\n" + "        ...\n" + "        $" + itemVar + ":* &\n" + "            (CanMapOnSetOp $" + itemVar + ") &\n" + "            (IsBoundBy $" + itemVar + " $" + inputColsVar + ":(OutputCols $" + inputVar + "))\n" + "        ...\n" + "    ]\n" + ")";
            return inputColsBindEnv.addBinding("isFilterSetOpTranspose", "true").setPattern(pattern).focus(pattern);
        }
        
        Env sourceEnv = onMatch(env, filter.source());
        String sourcePattern = sourceEnv.current();
        Env condEnv = onMatch(sourceEnv, filter.cond());
        String condPattern;
        
        if (filter.source() instanceof RelRN.Empty) {
            String inputVar = condEnv.generateVar("input");
            String filtersVar = condEnv.generateVar("filters");
            String pattern = "(Select\n    $" + inputVar + ":* & (HasZeroRows $" + inputVar + ")\n    $" + filtersVar + ":*\n)";             return condEnv.addBinding("isPruneEmptyFilter", "true")                     .addBinding("pruneEmptyInput", inputVar)                     .setPattern(pattern).focus(pattern);         }                  if (filter.cond() instanceof RexRN.True) {             String pattern = "(Select\n    " + sourcePattern + "\n    []\n)";             return condEnv.setPattern(pattern).focus(pattern);         } else if (filter.cond() instanceof RexRN.False) {             String onVar = condEnv.generateVar("on");             Env onEnv = condEnv.addBinding("on", onVar);             String itemVar = onEnv.generateVar("item");             Env itemEnv = onEnv.addBinding("item", itemVar);             String pattern = "(Select\n    " + sourcePattern + "\n    $" + onVar + ":[\n        ...\n        $" + itemVar + ":(FiltersItem (False))\n        ...\n    ]\n)";             return itemEnv.setPattern(pattern).focus(pattern);         } else {             condPattern = condEnv.current();             if (filter.source() instanceof RelRN.Aggregate) {                 String condVarName = extractVar(condPattern);                 String privateVar = sourceEnv.bindings().getOrDefault("aggregate_private", null);                 if (condVarName != null && privateVar != null) {                     condPattern = condPattern + " & (FiltersBoundBy $" + condVarName + " (GroupingCols $" + privateVar + "))";                 }             }             String filterCondVarName = extractVar(condEnv.current());             if (filterCondVarName != null) { condEnv = condEnv.addBinding("filterCondVar", filterCondVarName); }         }         String pattern = "(Select\n    " + sourcePattern + "\n    " + condPattern + "\n)";         return condEnv.setPattern(pattern).focus(pattern);     }      public Env onMatchProject(Env env, RelRN.Project project) {         if (project.source() instanceof RelRN.Empty) {             String inputVar = env.generateVar("input");             Env inputEnv = env.addBinding("zeroInput", inputVar)                     .addBinding("hasZeroRows", "true");             String projectionsVar = inputEnv.generateVar("projections");             Env projectionsEnv = inputEnv.addBinding("projections", projectionsVar);             String passthroughVar = projectionsEnv.generateVar("passthrough");             Env passthroughEnv = projectionsEnv.addBinding("passthrough", passthroughVar);             String pattern = "(Project\n    $" + inputVar + ":* & (HasZeroRows $" + inputVar + ")\n    $" + projectionsVar + ":*\n    $" + passthroughVar + ":*\n)";             return passthroughEnv.setPattern(pattern).focus(pattern);         }         if (project.source() instanceof RelRN.Aggregate aggregate) {             String inputVar = env.generateVar("input");             Env inputEnv = env.addBinding("input", inputVar);                          Env aggInputEnv = onMatch(inputEnv, aggregate.source());             String aggInputPattern = aggInputEnv.current();             String innerInputVar = aggInputPattern.replace("$", "").replace(":*", "").trim();             Env aggInputBoundEnv = aggInputEnv.addBinding("innerInput", innerInputVar);                          Env aggsEnv = onMatchAggCalls(aggInputBoundEnv, aggregate.aggCalls());             String aggsPattern = aggsEnv.current();             String aggregationsVar = aggsEnv.generateVar("aggregations");             Env aggregationsBindEnv = aggsEnv.addBinding("aggregations", aggregationsVar);                          Env groupingEnv = onMatchGroupSet(aggregationsBindEnv, aggregate.groupSet());             String groupingPattern = groupingEnv.current();             String groupingPrivateVar = groupingEnv.generateVar("groupingPrivate");             Env groupingPrivateBindEnv = groupingEnv.addBinding("groupingPrivate", groupingPrivateVar);                          Env projEnv = onMatch(groupingPrivateBindEnv, project.map());             String projPattern = projEnv.current();             String projectionsVar = projEnv.generateVar("projections");             Env projectionsBindEnv = projEnv.addBinding("projections", projectionsVar);                          String passthroughVar = projectionsBindEnv.generateVar("passthrough");             Env passthroughBindEnv = projectionsBindEnv.addBinding("passthrough", passthroughVar);                          String neededVar = passthroughBindEnv.generateVar("needed");             Env neededBindEnv = passthroughBindEnv.addBinding("needed", neededVar);                          String pattern = "(Project\n" + "    $" + inputVar + ":(GroupBy\n" + "        $" + innerInputVar + ":*\n" + "        $" + aggregationsVar + ":*\n" + "        $" + groupingPrivateVar + ":*\n" + "    )\n" + "    $" + projectionsVar + ":*\n" + "    $" + passthroughVar + ":* &\n" + "        (CanPruneAggCols\n" + "            $" + aggregationsVar + "\n" + "            $" + neededVar + ":(UnionCols\n" + "                (ProjectionOuterCols $" + projectionsVar + ")\n" + "                $" + passthroughVar + "\n" + "            )\n" + "        )\n" + ")";
            return neededBindEnv.setPattern(pattern).focus(pattern);
        }
        if (project.source() instanceof RelRN.Project) {
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
            String pattern = "(Project\n    $input:(" + innerProjectPattern + ")\n    " + outerProjPattern + " &\n        (CanMergeProjections " + outerProjVar + " " + innerProjVar + ")\n    $" + outerPassthroughVar + ":*\n)";             return outerPassthroughEnv.setPattern(pattern).focus(pattern);         }         Env sourceEnv = onMatch(env, project.source());         String sourcePattern = sourceEnv.current();         Env projEnv = onMatch(sourceEnv, project.map());         String projPattern = projEnv.current();         String passthroughVar = projEnv.generateVar("passthrough");         Env passthroughEnv = projEnv.addBinding("passthrough", passthroughVar);         String passthroughCond = "";         if (project.source() instanceof RelRN.Filter) {             String condVarName = sourceEnv.bindings().getOrDefault("filterCondVar", null);             if (condVarName != null) {                 passthroughCond = " & (FiltersBoundBy $" + condVarName + " $" + passthroughVar + ")";             }         }         String pattern = "(Project\n    " + sourcePattern + "\n    " + projPattern + "\n    $" + passthroughVar + ":*" + passthroughCond + "\n)";         return passthroughEnv.setPattern(pattern).focus(pattern);     }      @Override     public Env onMatchJoin(Env env, RelRN.Join join) {         if (join.cond() instanceof RexRN.And and) {             if (and.sources().size() == 2) {                 boolean hasTrue = false;                 boolean hasFalse = false;                 RexRN otherCond = null;                                  for (RexRN side : and.sources()) {                     if (side instanceof RexRN.True) {                         hasTrue = true;                     } else if (side instanceof RexRN.False) {                         hasFalse = true;                     } else {                         otherCond = side;                     }                 }                                  if (hasTrue && otherCond != null) {                     Env leftEnv = onMatch(env, join.left());                     String leftPattern = leftEnv.current();                     Env rightEnv = onMatch(leftEnv, join.right());                     String rightPattern = rightEnv.current();                     String onVar = rightEnv.generateVar("on");                     Env onEnv = rightEnv.addBinding("on", onVar);                     String itemVar = onEnv.generateVar("item");                     Env itemEnv = onEnv.addBinding("item", itemVar);                     String privateVar = itemEnv.generateVar("private");                     Env privateEnv = itemEnv.addBinding("private_" + System.identityHashCode(join), privateVar)                             .addBinding("last_private", privateVar)                             .addBinding("joinReduceTrue", "true");                     String joinType = getJoinType(join.ty().semantics());                     String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    $" + onVar + ":[\n        ...\n        $" + itemVar + ":(FiltersItem (True))\n        ...\n    ]\n    $" + privateVar + ":*\n)";                     return privateEnv.setPattern(pattern).focus(pattern);                 } else if (hasFalse && otherCond != null) {                     Env leftEnv = onMatch(env, join.left());                     String leftPattern = leftEnv.current();                     Env rightEnv = onMatch(leftEnv, join.right());                     String rightPattern = rightEnv.current();                     String onVar = rightEnv.generateVar("on");                     Env onEnv = rightEnv.addBinding("on", onVar);                     String itemVar = onEnv.generateVar("item");                     Env itemEnv = onEnv.addBinding("item", itemVar);                     String privateVar = itemEnv.generateVar("private");                     Env privateEnv = itemEnv.addBinding("private_" + System.identityHashCode(join), privateVar)                             .addBinding("last_private", privateVar)                             .addBinding("joinReduceFalse", "true");                     String joinType = getJoinType(join.ty().semantics());                     String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    $" + onVar + ":[\n        ...\n        $" + itemVar + ":(FiltersItem\n            (And * (False))\n        )\n        ...\n    ] &\n        ^(IsFilterFalse $" + onVar + ")\n    $" + privateVar + ":*\n)";                     return privateEnv.setPattern(pattern).focus(pattern);                 }             }             if (join.cond() instanceof RexRN.And andCond && andCond.sources().contains(RexRN.falseLiteral())) {                 Env leftEnv = onMatch(env, join.left());                 String leftPattern = leftEnv.current();                 Env rightEnv = onMatch(leftEnv, join.right());                 String rightPattern = rightEnv.current();                 String onVar = rightEnv.generateVar("on");                 Env onEnv = rightEnv.addBinding("on", onVar);                 String itemVar = onEnv.generateVar("item");                 Env itemEnv = onEnv.addBinding("item", itemVar);                 String privateVar = itemEnv.generateVar("private");                 Env privateEnv = itemEnv.addBinding("private_" + System.identityHashCode(join), privateVar)                         .addBinding("last_private", privateVar)                         .addBinding("joinReduceFalse", "true");                 String joinType = getJoinType(join.ty().semantics());                 String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    $" + onVar + ":[\n        ...\n        $" + itemVar + ":(FiltersItem\n            (And * (False))\n        )\n        ...\n    ] &\n        ^(IsFilterFalse $" + onVar + ")\n    $" + privateVar + ":*\n)";                 return privateEnv.setPattern(pattern).focus(pattern);             }             if (env.rulename.equals("JoinConditionPush")                     && join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.INNER                     && and.sources().size() > 2) {                 String leftVar = env.generateVar("left");                 Env leftEnv = env.addBinding("left", leftVar);                 String rightVar = leftEnv.generateVar("right");                 Env rightEnv = leftEnv.addBinding("right", rightVar);                 String onVar = rightEnv.generateVar("on");                 Env onEnv = rightEnv.addBinding("on", onVar);                 String privateVar = onEnv.generateVar("private");                 Env privateEnv = onEnv.addBinding("private", privateVar);                 String pattern = "(InnerJoin\n" + "    $" + leftVar + ":* & ^(HasOuterCols $" + leftVar + ")\n" + "    $" + rightVar + ":* & ^(HasOuterCols $" + rightVar + ")\n" + "    $" + onVar + ":*\n" + "    $" + privateVar + ":*\n" + ")";
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
        Env privateEnv = condEnv.addBinding("private_" + System.identityHashCode(join), privateVar).addBinding("last_private", privateVar);
        
        if (join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.INNER) {
            String leftVar = privateEnv.generateVar("left");
            String rightVar = privateEnv.generateVar("right");
            String onVar = privateEnv.generateVar("on");
            Env boundEnv = privateEnv.addBinding("left", leftVar).addBinding("right", rightVar).addBinding("on", onVar).addBinding("private", privateVar);
            if (env.rulename.equals("JoinAddRedundantSemiJoin")) {
                boundEnv = boundEnv.addBinding("isJoinAddRedundantSemiJoin", "true");
                String filtersVar = boundEnv.generateVar("filters");
                Env filtersEnv = boundEnv.addBinding("filters", filtersVar);
                String pattern = "(InnerJoin\n" + "    $" + leftVar + ":^(Values)\n" + "    $" + rightVar + ":*\n" + "    $" + filtersVar + ":*\n" + "    $" + privateVar + ":* & ^(IsRedundantSemiJoin $" + leftVar + " $" + rightVar + " $" + filtersVar + ")\n" + ")";
                return filtersEnv.setPattern(pattern).focus(pattern);
            }
            if (env.rulename.equals("JoinCommute")) {
                boundEnv = boundEnv.addBinding("isJoinCommute", "true");
                String pattern = "(InnerJoin\n" + "    $" + leftVar + ":*\n" + "    $" + rightVar + ":*\n" + "    $" + onVar + ":*\n" + "    $" + privateVar + ":* &\n" + "        (CanCommuteJoin $" + leftVar + " $" + rightVar + ")\n" + ")";
                return boundEnv.setPattern(pattern).focus(pattern);
            }
        }
        if (env.rulename.equals("JoinExtractFilter") && join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.INNER && !(join.cond() instanceof RexRN.And)) {
            String leftVar = privateEnv.generateVar("left");
            String rightVar = privateEnv.generateVar("right");
            String onVar = privateEnv.generateVar("on");
            Env boundEnv = privateEnv.addBinding("left", leftVar).addBinding("right", rightVar).addBinding("on", onVar).addBinding("private", privateVar).addBinding("isJoinExtractFilter", "true");
            if (join.cond() instanceof RexRN.Pred pred) {
                boundEnv = boundEnv.addBinding(pred.operator().getName(), onVar);
            }
            String joinType = getJoinType(join.ty().semantics());
            String pattern = "(" + joinType + "\n" + "    $" + leftVar + ":*\n" + "    $" + rightVar + ":*\n" + "    $" + onVar + ":* &\n" + "        (CanExtractJoinFilter $" + leftVar + " $" + rightVar + " $" + onVar + ")\n" + "    $" + privateVar + ":*\n" + ")";
            return boundEnv.setPattern(pattern).focus(pattern);
        }
        
        String joinType = getJoinType(join.ty().semantics());         if (env.rulename.equals("JoinPushTransitivePredicates") && join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.INNER) {             String condVarName = extractVar(condPattern);             if (condVarName != null) {                 condPattern = condPattern + " & ^(IsFilterEmpty $" + condVarName + ")";             }         }         String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    " + condPattern + "\n    $" + privateVar + ":*\n)";         return privateEnv.setPattern(pattern).focus(pattern);     }      @Override     public Env onMatchJoinWithSeparateConds(Env env, RelRN.JoinWithSeparateConds join) {         if (join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.INNER &&             join.cond() instanceof RexRN.And and &&             and.sources().size() > 2) {             String leftVar = env.generateVar("left");             Env leftEnv = env.addBinding("left", leftVar);             String rightVar = leftEnv.generateVar("right");             Env rightEnv = leftEnv.addBinding("right", rightVar);             String onVar = rightEnv.generateVar("on");             Env onEnv = rightEnv.addBinding("on", onVar);             String privateVar = onEnv.generateVar("private");             Env privateEnv = onEnv.addBinding("private", privateVar)                     .addBinding("isJoinConditionPush", "true");             String pattern = "(InnerJoin\n" + "    $" + leftVar + ":* & ^(HasOuterCols $" + leftVar + ")\n" + "    $" + rightVar + ":* & ^(HasOuterCols $" + rightVar + ")\n" + "    $" + onVar + ":* &\n" + "        (HasBoundConditions\n" + "            $" + onVar + "\n" + "            (OutputCols $" + leftVar + ")\n" + "            (OutputCols $" + rightVar + ")\n" + "        )\n" + "    $" + privateVar + ":*\n" + ")";
            return privateEnv.setPattern(pattern).focus(pattern);
        }
        Env leftEnv = onMatch(env, join.left());
        String leftPattern = leftEnv.current();
        Env rightEnv = onMatch(leftEnv, join.right());
        String rightPattern = rightEnv.current();
        Env condEnv = onMatch(rightEnv, join.cond());
        String condPattern = condEnv.current();
        String privateVar = condEnv.generateVar("private");
        Env privateEnv = condEnv.addBinding("private_" + System.identityHashCode(join), privateVar).addBinding("last_private", privateVar);
        String joinType = getJoinType(join.ty().semantics());
        String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    " + condPattern + "\n    $" + privateVar + ":*\n)";         return privateEnv.setPattern(pattern).focus(pattern);     }      @Override     public Env transformJoin(Env env, RelRN.Join join) {         if (env.bindings().containsKey("joinReduceTrue")) {             Env leftEnv = transform(env, join.left());             String leftPattern = leftEnv.current();             Env rightEnv = transform(leftEnv, join.right());             String rightPattern = rightEnv.current();             String onVar = rightEnv.bindings().get("on");             String itemVar = rightEnv.bindings().get("item");             String privateVar = rightEnv.bindings().getOrDefault("private_" + System.identityHashCode(join),                      rightEnv.bindings().getOrDefault("last_private", "private"));             String joinType = getJoinType(join.ty().semantics());             String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    (RemoveFiltersItem $" + onVar + " $" + itemVar + ")\n    $" + privateVar + "\n)";             return rightEnv.setPattern(pattern).focus(pattern);         } else if (env.bindings().containsKey("joinReduceFalse")) {             Env leftEnv = transform(env, join.left());             String leftPattern = leftEnv.current();             Env rightEnv = transform(leftEnv, join.right());             String rightPattern = rightEnv.current();             String privateVar = rightEnv.bindings().getOrDefault("private_" + System.identityHashCode(join),                      rightEnv.bindings().getOrDefault("last_private", "private"));             String joinType = getJoinType(join.ty().semantics());             String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    [ (FiltersItem (False)) ]\n    $" + privateVar + "\n)";             return rightEnv.setPattern(pattern).focus(pattern);         }         if (env.bindings().containsKey("isJoinAddRedundantSemiJoin") &&             env.bindings().containsKey("left") &&             env.bindings().containsKey("right") &&             env.bindings().containsKey("filters") &&             env.bindings().containsKey("private")) {             String leftVar = env.bindings().get("left");             String rightVar = env.bindings().get("right");             String filtersVar = env.bindings().get("filters");             String privateVar = env.bindings().get("private");             String pattern = "(InnerJoin\n" + "    (SemiJoin\n" + "        $" + leftVar + "\n" + "        $" + rightVar + "\n" + "        $" + filtersVar + "\n" + "        (EmptyJoinPrivate)\n" + "    )\n" + "    $" + rightVar + "\n" + "    $" + filtersVar + "\n" + "    $" + privateVar + "\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.bindings().containsKey("isJoinCommute")) {
            String leftVar = env.bindings().get("left");
            String rightVar = env.bindings().get("right");
            String onVar = env.bindings().get("on");
            String privateVar = env.bindings().get("private");
            String pattern = "(Project\n" + "    (InnerJoin\n" + "        $" + rightVar + "\n" + "        $" + leftVar + "\n" + "        $" + onVar + "\n" + "        (CommuteJoinFlags $" + privateVar + ")\n" + "    )\n" + "    (SwapJoinOutputColumns\n" + "        (OutputCols $" + leftVar + ")\n" + "        (OutputCols $" + rightVar + ")\n" + "    )\n" + "    (MakeEmptyColSet)\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.bindings().containsKey("isJoinConditionPush") && join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.INNER && env.bindings().containsKey("left") && env.bindings().containsKey("right") && env.bindings().containsKey("on") && env.bindings().containsKey("private") && !env.bindings().containsKey("joinReduceTrue") && !env.bindings().containsKey("joinReduceFalse") && !env.bindings().containsKey("isJoinCommute") && !env.bindings().containsKey("isJoinExtractFilter")) {
            String leftVar = env.bindings().get("left");
            String rightVar = env.bindings().get("right");
            String onVar = env.bindings().get("on");
            String privateVar = env.bindings().get("private");
            String pattern = "(InnerJoin\n" + "    (Select $" + leftVar + " (ExtractBoundConditions $" + onVar + " (OutputCols $" + leftVar + ")))\n" + "    (Select $" + rightVar + " (ExtractBoundConditions $" + onVar + " (OutputCols $" + rightVar + ")))\n" + "    (ExtractUnboundConditions\n" + "        (ExtractUnboundConditions $" + onVar + " (OutputCols $" + leftVar + "))\n" + "        (OutputCols $" + rightVar + ")\n" + "    )\n" + "    $" + privateVar + "\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        
        Env leftEnv = transform(env, join.left());
        String leftPattern = leftEnv.current();
        Env rightEnv = transform(leftEnv, join.right());
        String rightPattern = rightEnv.current();
        Env condEnv = transform(rightEnv, join.cond());
        String condPattern = condEnv.current();
        String privateVar = condEnv.bindings().getOrDefault("private_" + System.identityHashCode(join), condEnv.bindings().getOrDefault("last_private", "private"));
        String joinType = getJoinType(join.ty().semantics());
        String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    " + condPattern + "\n    $" + privateVar + "\n)";         return condEnv.setPattern(pattern).focus(pattern);     }      @Override     public Env transformJoinWithPushedConds(Env env, RelRN.JoinWithPushedConds join) {         if (env.bindings().containsKey("isJoinConditionPush") &&             env.bindings().containsKey("left") &&             env.bindings().containsKey("right") &&             env.bindings().containsKey("on") &&             env.bindings().containsKey("private")) {             String leftVar = env.bindings().get("left");             String rightVar = env.bindings().get("right");             String onVar = env.bindings().get("on");             String privateVar = env.bindings().get("private");             String pattern = "(InnerJoin\n" + "    (Select $" + leftVar + " (ExtractBoundConditions $" + onVar + " (OutputCols $" + leftVar + ")))\n" + "    (Select $" + rightVar + " (ExtractBoundConditions $" + onVar + " (OutputCols $" + rightVar + ")))\n" + "    (ExtractUnboundConditions\n" + "        (ExtractUnboundConditions $" + onVar + " (OutputCols $" + leftVar + "))\n" + "        (OutputCols $" + rightVar + ")\n" + "    )\n" + "    $" + privateVar + "\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        Env leftEnv = transform(env, join.left());
        String leftPattern = leftEnv.current();
        Env rightEnv = transform(leftEnv, join.right());
        String rightPattern = rightEnv.current();
        Env condEnv = transform(rightEnv, join.cond());
        String condPattern = condEnv.current();
        String privateVar = condEnv.generateVar("private");
        String joinType = getJoinType(join.ty().semantics());
        String pattern = "(" + joinType + "\n    " + leftPattern + "\n    " + rightPattern + "\n    " + condPattern + "\n    $" + privateVar + "\n)";         return condEnv.setPattern(pattern).focus(pattern);     }      @Override     public Env onMatchUnion(Env env, RelRN.Union union) {         if (union.sources().size() == 2) {             RelRN leftSource = union.sources().get(0);             RelRN rightSource = union.sources().get(1);             if (leftSource instanceof RelRN.Empty && rightSource instanceof RelRN.Empty) {                 String leftVar = env.generateVar("left");                 Env leftEnv = env.addBinding("left", leftVar);                 String rightVar = leftEnv.generateVar("right");                 Env rightEnv = leftEnv.addBinding("right", rightVar);                 String privateVar = rightEnv.generateVar("private");                 Env privateEnv = rightEnv.addBinding("private", privateVar);                 String outColsVar = privateEnv.generateVar("outCols");                 Env outColsEnv = privateEnv.addBinding("outCols", outColsVar)                         .addBinding("hasZeroRows", "true");                 String unionType = union.all() ? "UnionAll" : "Union";                 String pattern = "(" + unionType + "\n    $" + leftVar + ":* & (HasZeroRows $" + leftVar + ")\n    $" + rightVar + ":* & (HasZeroRows $" + rightVar + ")\n    $" + privateVar + ":(SetPrivate * * $" + outColsVar + ":*)\n)";                 return outColsEnv.setPattern(pattern).focus(pattern);             }         }         if (union.sources().size() == 2 && union.sources().get(0) instanceof RelRN.Union innerUnion && innerUnion.sources().size() == 2) {             String leftLeftVar = env.generateVar("leftLeft");             Env leftLeftEnv = env.addBinding("leftLeft", leftLeftVar);             String leftRightVar = leftLeftEnv.generateVar("leftRight");             Env leftRightEnv = leftLeftEnv.addBinding("leftRight", leftRightVar);             String innerPrivateVar = leftRightEnv.generateVar("innerPrivate");             Env innerPrivateEnv = leftRightEnv.addBinding("innerPrivate", innerPrivateVar);             String innerLeftColsVar = innerPrivateEnv.generateVar("innerLeftCols");             Env innerLeftColsEnv = innerPrivateEnv.addBinding("innerLeftCols", innerLeftColsVar);             String innerRightColsVar = innerLeftColsEnv.generateVar("innerRightCols");             Env innerRightColsEnv = innerLeftColsEnv.addBinding("innerRightCols", innerRightColsVar);             String innerOutColsVar = innerRightColsEnv.generateVar("innerOutCols");             Env innerOutColsEnv = innerRightColsEnv.addBinding("innerOutCols", innerOutColsVar);             String leftVar = innerOutColsEnv.generateVar("left");             Env leftEnv = innerOutColsEnv.addBinding("left", leftVar);             String rightVar = leftEnv.generateVar("right");             Env rightEnv = leftEnv.addBinding("right", rightVar);             String outerPrivateVar = rightEnv.generateVar("outerPrivate");             Env outerPrivateEnv = rightEnv.addBinding("outerPrivate", outerPrivateVar);             String outerRightColsVar = outerPrivateEnv.generateVar("outerRightCols");             Env outerRightColsEnv = outerPrivateEnv.addBinding("outerRightCols", outerRightColsVar);             String outerOutColsVar = outerRightColsEnv.generateVar("outerOutCols");             Env outerOutColsEnv = outerRightColsEnv.addBinding("outerOutCols", outerOutColsVar)                     .addBinding("isUnionMerge", "true");             String unionType = union.all() ? "UnionAll" : "Union";             String pattern = "(" + unionType + "\n" + "    $" + leftVar + ":(" + unionType + "\n" + "        $" + leftLeftVar + ":*\n" + "        $" + leftRightVar + ":*\n" + "        $" + innerPrivateVar + ":(SetPrivate $" + innerLeftColsVar + ":* $" + innerRightColsVar + ":* $" + innerOutColsVar + ":*)\n" + "    )\n" + "    $" + rightVar + ":*\n" + "    $" + outerPrivateVar + ":(SetPrivate * $" + outerRightColsVar + ":* $" + outerOutColsVar + ":*)\n" + ")";             return outerOutColsEnv.setPattern(pattern).focus(pattern);         }         if (union.sources().size() == 2 && !union.all()) {             RelRN leftSource = union.sources().get(0);             RelRN rightSource = union.sources().get(1);                          Env leftEnv = onMatch(env, leftSource);             String leftVar = leftEnv.generateVar("left");             Env leftBoundEnv = leftEnv.addBinding("left", leftVar);                          Env rightEnv = onMatch(leftBoundEnv, rightSource);             String rightVar = rightEnv.generateVar("right");             Env rightBoundEnv = rightEnv.addBinding("right", rightVar);                          String privateVar = rightBoundEnv.generateVar("private");             Env privateBindEnv = rightBoundEnv.addBinding("private", privateVar);             String leftColsVar = privateBindEnv.generateVar("leftCols");             Env leftColsEnv = privateBindEnv.addBinding("leftCols", leftColsVar);             String rightColsVar = leftColsEnv.generateVar("rightCols");             Env rightColsEnv = leftColsEnv.addBinding("rightCols", rightColsVar);             String outColsVar = rightColsEnv.generateVar("outCols");             Env outColsEnv = rightColsEnv.addBinding("outCols", outColsVar);             String keyColsVar = outColsEnv.generateVar("keyCols");             Env keyColsEnv = outColsEnv.addBinding("keyCols", keyColsVar);             String okVar = keyColsEnv.generateVar("ok");             Env okEnv = keyColsEnv.addBinding("ok", okVar);                          String unionType = "Union";             String pattern = "(" + unionType + "\n" + "    $" + leftVar + ":*\n" + "    $" + rightVar + ":*\n" + "    $" + privateVar + ":(SetPrivate $" + leftColsVar + ":* $" + rightColsVar + ":* $" + outColsVar + ":*) &\n" + "        (Let\n" + "            ($" + keyColsVar + " $" + okVar + "):(CanConvertUnionToDistinctUnionAll\n" + "                $" + leftColsVar + "\n" + "                $" + rightColsVar + "\n" + "            )\n" + "            $" + okVar + "\n" + "        )\n" + ")";
            return okEnv.setPattern(pattern).focus(pattern);
        }
        if (union.all() && union.sources().size() == 2) {
            RelRN leftSource = union.sources().get(0);
            RelRN rightSource = union.sources().get(1);
            RelRN.Project leftProject = null;
            RelRN.Project rightProject = null;
            
            if (leftSource instanceof RelRN.Project) {leftProject = (RelRN.Project) leftSource;} 
            else if (leftSource instanceof org.qed.RRuleInstances.UnionPullUpConstants.LeftProjectionWithConstants leftProj) {leftProject = new RelRN.Project(leftProj.input().field(0), leftProj.input());}
            
            if (rightSource instanceof RelRN.Project) {rightProject = (RelRN.Project) rightSource;} 
            else if (rightSource instanceof org.qed.RRuleInstances.UnionPullUpConstants.RightProjectionWithConstants rightProj) {rightProject = new RelRN.Project(rightProj.input().field(0), rightProj.input());}
            
            if (leftProject != null && rightProject != null) {
                Env leftInputEnv = onMatch(env, leftProject.source());
                String leftInputVar = leftInputEnv.generateVar("leftInput");
                Env leftInputBoundEnv = leftInputEnv.addBinding("leftInput", leftInputVar);                
                String leftProjectionsVar = leftInputBoundEnv.generateVar("leftProjections");
                Env leftProjectionsEnv = leftInputBoundEnv.addBinding("leftProjections", leftProjectionsVar);
                String leftPassthroughVar = leftProjectionsEnv.generateVar("leftPassthrough");
                Env leftPassthroughEnv = leftProjectionsEnv.addBinding("leftPassthrough", leftPassthroughVar);                
                String leftVar = leftPassthroughEnv.generateVar("left");
                Env leftBoundEnv = leftPassthroughEnv.addBinding("left", leftVar);                
                Env rightInputEnv = onMatch(leftBoundEnv, rightProject.source());
                String rightInputVar = rightInputEnv.generateVar("rightInput");
                Env rightInputBoundEnv = rightInputEnv.addBinding("rightInput", rightInputVar);                
                String rightProjectionsVar = rightInputBoundEnv.generateVar("rightProjections");
                Env rightProjectionsEnv = rightInputBoundEnv.addBinding("rightProjections", rightProjectionsVar);
                String rightPassthroughVar = rightProjectionsEnv.generateVar("rightPassthrough");
                Env rightPassthroughEnv = rightProjectionsEnv.addBinding("rightPassthrough", rightPassthroughVar);                
                String rightVar = rightPassthroughEnv.generateVar("right");
                Env rightBoundEnv = rightPassthroughEnv.addBinding("right", rightVar);     
                String privateVar = rightBoundEnv.generateVar("private");
                Env privateBindEnv = rightBoundEnv.addBinding("private", privateVar);
                String leftColsVar = privateBindEnv.generateVar("leftCols");
                Env leftColsEnv = privateBindEnv.addBinding("leftCols", leftColsVar);
                String rightColsVar = leftColsEnv.generateVar("rightCols");
                Env rightColsEnv = leftColsEnv.addBinding("rightCols", rightColsVar);
                String outColsVar = rightColsEnv.generateVar("outCols");
                Env outColsEnv = rightColsEnv.addBinding("outCols", outColsVar);
                String unionType = union.all() ? "UnionAll" : "Union";
                String pattern = "(" + unionType + "\n" + "    $" + leftVar + ":(Project\n" + "        $" + leftInputVar + ":*\n" + "        $" + leftProjectionsVar + ":*\n" + "        $" + leftPassthroughVar + ":*\n" + "    )\n" + "    $" + rightVar + ":(Project\n" + "        $" + rightInputVar + ":*\n" + "        $" + rightProjectionsVar + ":*\n" + "        $" + rightPassthroughVar + ":*\n" + "    )\n" + "    $" + privateVar + ":(SetPrivate $" + leftColsVar + ":* $" + rightColsVar + ":* $" + outColsVar + ":*) &\n" + "        (HasMatchingConstantsFromUnion\n" + "            $" + leftProjectionsVar + "\n" + "            $" + rightProjectionsVar + "\n" + "            $" + leftColsVar + "\n" + "            $" + rightColsVar + "\n" + "            $" + outColsVar + "\n" + "        )\n" + ")";
                return outColsEnv.setPattern(pattern).focus(pattern);
            }
        }
        if (union.sources().size() == 2 && union.sources().get(0) instanceof RelRN.Union innerUnion && innerUnion.sources().size() == 2) {
            String leftLeftVar = env.generateVar("leftLeft");
            Env leftLeftEnv = env.addBinding("leftLeft", leftLeftVar);
            String leftRightVar = leftLeftEnv.generateVar("leftRight");
            Env leftRightEnv = leftLeftEnv.addBinding("leftRight", leftRightVar);
            String innerPrivateVar = leftRightEnv.generateVar("innerPrivate");
            Env innerPrivateEnv = leftRightEnv.addBinding("innerPrivate", innerPrivateVar);
            String innerLeftColsVar = innerPrivateEnv.generateVar("innerLeftCols");
            Env innerLeftColsEnv = innerPrivateEnv.addBinding("innerLeftCols", innerLeftColsVar);
            String innerRightColsVar = innerLeftColsEnv.generateVar("innerRightCols");
            Env innerRightColsEnv = innerLeftColsEnv.addBinding("innerRightCols", innerRightColsVar);
            String innerOutColsVar = innerRightColsEnv.generateVar("innerOutCols");
            Env innerOutColsEnv = innerRightColsEnv.addBinding("innerOutCols", innerOutColsVar);
            String leftVar = innerOutColsEnv.generateVar("left");
            Env leftEnv = innerOutColsEnv.addBinding("left", leftVar);
            String rightVar = leftEnv.generateVar("right");
            Env rightEnv = leftEnv.addBinding("right", rightVar);
            String outerPrivateVar = rightEnv.generateVar("outerPrivate");
            Env outerPrivateEnv = rightEnv.addBinding("outerPrivate", outerPrivateVar);
            String outerRightColsVar = outerPrivateEnv.generateVar("outerRightCols");
            Env outerRightColsEnv = outerPrivateEnv.addBinding("outerRightCols", outerRightColsVar);
            String outerOutColsVar = outerRightColsEnv.generateVar("outerOutCols");
            Env outerOutColsEnv = outerRightColsEnv.addBinding("outerOutCols", outerOutColsVar).addBinding("isUnionMerge", "true");
            String unionType = union.all() ? "UnionAll" : "Union";
            String pattern = "(" + unionType + "\n" + "    $" + leftVar + ":(" + unionType + "\n" + "        $" + leftLeftVar + ":*\n" + "        $" + leftRightVar + ":*\n" + "        $" + innerPrivateVar + ":(SetPrivate $" + innerLeftColsVar + ":* $" + innerRightColsVar + ":* $" + innerOutColsVar + ":*)\n" + "    )\n" + "    $" + rightVar + ":*\n" + "    $" + outerPrivateVar + ":(SetPrivate * $" + outerRightColsVar + ":* $" + outerOutColsVar + ":*)\n" + ")";
            return outerOutColsEnv.setPattern(pattern).focus(pattern);
        }
        Env currentEnv = env;         Seq<String> sourcePatterns = Seq.empty();         for (RelRN source : union.sources()) {             Env sourceEnv = onMatch(currentEnv, source);             if (source instanceof RelRN.Union) {                 String subPrivate = sourceEnv.bindings().get("union_private");                 if (subPrivate != null) {                     sourceEnv = sourceEnv.addBinding("inner_union_private", subPrivate);                 }             }             sourcePatterns = sourcePatterns.appended(sourceEnv.current());             currentEnv = sourceEnv;         }         String privateVar = currentEnv.generateVar("private");         Env privateEnv = currentEnv.addBinding("union_private", privateVar);         String unionType = union.all() ? "UnionAll" : "Union";         String pattern;         if (sourcePatterns.size() == 2) {             pattern = "(" + unionType + "\n    " + sourcePatterns.get(0) + "\n    " + sourcePatterns.get(1) + "\n    $" + privateVar + ":*\n)";         } else {             pattern = buildNestedUnion(unionType, sourcePatterns, privateVar + ":*");         }         return privateEnv.setPattern(pattern).focus(pattern);     }      private String buildNestedUnion(String unionType, Seq<String> sources, String privatePattern) {         if (sources.size() == 2) {             return "(" + unionType + "\n    " + sources.get(0) + "\n    " + sources.get(1) + "\n    $" + privatePattern + "\n)";         }         String first = sources.get(0);         String nested = buildNestedUnion(unionType, sources.drop(1), privatePattern);         return "(" + unionType + "\n    " + first + "\n    " + nested + "\n    $" + privatePattern + "\n)";     }      @Override     public Env onMatchIntersect(Env env, RelRN.Intersect intersect) {         if (intersect.sources().size() == 2 && intersect.sources().get(0) instanceof RelRN.Intersect innerIntersect && innerIntersect.sources().size() == 2) {             String leftLeftVar = env.generateVar("leftLeft");             Env leftLeftEnv = env.addBinding("leftLeft", leftLeftVar);             String leftRightVar = leftLeftEnv.generateVar("leftRight");             Env leftRightEnv = leftLeftEnv.addBinding("leftRight", leftRightVar);             String innerPrivateVar = leftRightEnv.generateVar("innerPrivate");             Env innerPrivateEnv = leftRightEnv.addBinding("innerPrivate", innerPrivateVar);             String innerLeftColsVar = innerPrivateEnv.generateVar("innerLeftCols");             Env innerLeftColsEnv = innerPrivateEnv.addBinding("innerLeftCols", innerLeftColsVar);             String innerRightColsVar = innerLeftColsEnv.generateVar("innerRightCols");             Env innerRightColsEnv = innerLeftColsEnv.addBinding("innerRightCols", innerRightColsVar);             String leftVar = innerRightColsEnv.generateVar("left");             Env leftEnv = innerRightColsEnv.addBinding("left", leftVar);             String rightVar = leftEnv.generateVar("right");             Env rightEnv = leftEnv.addBinding("right", rightVar);             String outerPrivateVar = rightEnv.generateVar("outerPrivate");             Env outerPrivateEnv = rightEnv.addBinding("outerPrivate", outerPrivateVar);             String outerRightColsVar = outerPrivateEnv.generateVar("outerRightCols");             Env outerRightColsEnv = outerPrivateEnv.addBinding("outerRightCols", outerRightColsVar);             String outerOutColsVar = outerRightColsEnv.generateVar("outerOutCols");             Env outerOutColsEnv = outerRightColsEnv.addBinding("outerOutCols", outerOutColsVar)                     .addBinding("isIntersectMerge", "true");             String intersectType = intersect.all() ? "IntersectAll" : "Intersect";             String pattern = "(" + intersectType + "\n" + "    $" + leftVar + ":(" + intersectType + "\n" + "        $" + leftLeftVar + ":*\n" + "        $" + leftRightVar + ":*\n" + "        $" + innerPrivateVar + ":(SetPrivate $" + innerLeftColsVar + ":* $" + innerRightColsVar + ":* *)\n" + "    )\n" + "    $" + rightVar + ":*\n" + "    $" + outerPrivateVar + ":(SetPrivate * $" + outerRightColsVar + ":* $" + outerOutColsVar + ":*)\n" + ")";
            return outerOutColsEnv.setPattern(pattern).focus(pattern);
        }
        if (intersect.sources().size() == 2) {
            RelRN leftSource = intersect.sources().get(0);
            RelRN rightSource = intersect.sources().get(1);
            if (rightSource instanceof RelRN.Empty) {
                String leftVar = env.generateVar("left");
                String rightVar = env.generateVar("right");
                String intersectType = intersect.all() ? "IntersectAll" : "Intersect";
                String pattern = "(" + intersectType + "\n    $" + leftVar + ":*\n    $" + rightVar + ":* & (HasZeroRows $" + rightVar + ")\n)";                 return env.addBinding("isPruneEmptyIntersect", "true")                         .addBinding("pruneEmptyLeft", leftVar)                         .setPattern(pattern).focus(pattern);             }         }                  Env currentEnv = env;         Seq<String> sourcePatterns = Seq.empty();         for (RelRN source : intersect.sources()) {             Env sourceEnv = onMatch(currentEnv, source);             if (source instanceof RelRN.Intersect) {                 String subPrivate = sourceEnv.bindings().get("intersect_private");                 if (subPrivate != null) {                     sourceEnv = sourceEnv.addBinding("inner_intersect_private", subPrivate);                 }             }             sourcePatterns = sourcePatterns.appended(sourceEnv.current());             currentEnv = sourceEnv;         }         String privateVar = currentEnv.generateVar("private");         Env privateEnv = currentEnv.addBinding("intersect_private", privateVar);         String intersectType = intersect.all() ? "IntersectAll" : "Intersect";         String pattern;         if (sourcePatterns.size() == 2) {             pattern = "(" + intersectType + "\n    " + sourcePatterns.get(0) + "\n    " + sourcePatterns.get(1) + "\n    $" + privateVar + ":*\n)";         } else {             pattern = buildNestedIntersect(intersectType, sourcePatterns, privateVar + ":*");         }         return privateEnv.setPattern(pattern).focus(pattern);     }       private String buildNestedIntersect(String intersectType, Seq<String> sources, String privatePattern) {         if (sources.size() == 2) {             return "(" + intersectType + "\n    " + sources.get(0) + "\n    " + sources.get(1) + "\n    $" + privatePattern + "\n)";         }         String first = sources.get(0);         String nested = buildNestedIntersect(intersectType, sources.drop(1), privatePattern);         return "(" + intersectType + "\n    " + first + "\n    " + nested + "\n    $" + privatePattern + "\n)";     }      @Override     public Env onMatchMinus(Env env, RelRN.Minus minus) {         if (env.rulename.equals("MinusMerge") && minus.sources().size() == 2 && minus.sources().get(0) instanceof RelRN.Minus inner) {             String leftVar = env.generateVar("left");             Env leftEnv = env.addBinding("left", leftVar);             String leftLeftVar = leftEnv.generateVar("leftLeft");             Env leftLeftEnv = leftEnv.addBinding("leftLeft", leftLeftVar);             String leftRightVar = leftLeftEnv.generateVar("leftRight");             Env leftRightEnv = leftLeftEnv.addBinding("leftRight", leftRightVar);             String innerPrivateVar = leftRightEnv.generateVar("innerPrivate");             Env innerPrivateEnv = leftRightEnv.addBinding("innerPrivate", innerPrivateVar);             String rightVar = innerPrivateEnv.generateVar("right");             Env rightEnv = innerPrivateEnv.addBinding("right", rightVar);             String outerPrivateVar = rightEnv.generateVar("outerPrivate");             Env outerPrivateEnv = rightEnv.addBinding("outerPrivate", outerPrivateVar);             String pattern = "(Except\n" + "    $" + leftVar + ":(Except\n" + "        $" + leftLeftVar + ":*\n" + "        $" + leftRightVar + ":*\n" + "        $" + innerPrivateVar + ":*\n" + "    )\n" + "    $" + rightVar + ":*\n" + "    $" + outerPrivateVar + ":*\n" + ")";
            return outerPrivateEnv.addBinding("isMinusMerge", "true").setPattern(pattern).focus(pattern);
        }
        if (minus.sources().size() == 2) {
            RelRN leftSource = minus.sources().get(0);
            RelRN rightSource = minus.sources().get(1);
            if (leftSource instanceof RelRN.Empty) {
                String leftVar = env.generateVar("left");
                String rightVar = "right";
                String pattern = "(Except\n" + "    $" + leftVar + ":* & (HasZeroRows $" + leftVar + ")\n" + "    $" + rightVar + ":*\n" + ")";
                return env.addBinding("isPruneEmptyMinus", "true").addBinding("pruneEmptyLeft", leftVar).addBinding("right", rightVar).setPattern(pattern).focus(pattern);
            }
        }
        Env leftEnv = onMatch(env, minus.sources().get(0));
        String leftPattern = leftEnv.current();
        Env rightEnv = onMatch(leftEnv, minus.sources().get(1));
        String rightPattern = rightEnv.current();
        String privateVar = rightEnv.generateVar("private");
        Env privateEnv = rightEnv.addBinding("minus_private", privateVar);
        String pattern = "(Except\n    " + leftPattern + "\n    " + rightPattern + "\n    $" + privateVar + ":*\n)";         return privateEnv.setPattern(pattern).focus(pattern);     }      @Override     public Env onMatchAggregate(Env env, RelRN.Aggregate aggregate) {         if (aggregate.source() instanceof RelRN.Join topJoin                 && topJoin.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.LEFT                 && topJoin.left() instanceof RelRN.Join bottomJoin                 && bottomJoin.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.LEFT) {             String leftVar = env.generateVar("left");             Env leftEnv = env.addBinding("left", leftVar);             String middleVar = leftEnv.generateVar("middle");             Env middleEnv = leftEnv.addBinding("middle", middleVar);             String rightVar = middleEnv.generateVar("right");             Env rightEnv = middleEnv.addBinding("right", rightVar);             String rightFiltersVar = rightEnv.generateVar("rightFilters");             Env rightFiltersEnv = rightEnv.addBinding("rightFilters", rightFiltersVar);             String aggregationsVar = rightFiltersEnv.generateVar("aggregations");             Env aggsEnv = rightFiltersEnv.addBinding("aggregations", aggregationsVar);             String groupingPrivateVar = aggsEnv.generateVar("groupingPrivate");             Env groupingPrivateEnv = aggsEnv.addBinding("groupingPrivate", groupingPrivateVar);             String groupingColsVar = groupingPrivateEnv.generateVar("groupingCols");             Env groupingColsEnv = groupingPrivateEnv.addBinding("groupingCols", groupingColsVar);             String orderingVar = groupingColsEnv.generateVar("ordering");             Env orderingEnv = groupingColsEnv.addBinding("ordering", orderingVar);             String pattern = "(DistinctOn\n" + "    (LeftJoin\n" + "        (LeftJoin\n" + "            $" + leftVar + ":*\n" + "            $" + middleVar + ":*\n" + "            *\n" + "        )\n" + "        $" + rightVar + ":*\n" + "        $" + rightFiltersVar + ":*\n" + "    )\n" + "    $" + aggregationsVar + ":[]\n" + "    $" + groupingPrivateVar + ":(GroupingPrivate $" + groupingColsVar + ":* $" + orderingVar + ":*) &\n" + "        (ColsAreEmpty\n" + "            (IntersectionCols\n" + "                (OutputCols $" + middleVar + ")\n" + "                (UnionCols\n" + "                    (FilterOuterCols $" + rightFiltersVar + ")\n" + "                    $" + groupingColsVar + "\n" + "                )\n" + "            )\n" + "        ) &\n" + "        (OrderingCanProjectCols\n" + "            $" + orderingVar + "\n" + "            (UnionCols (OutputCols $" + leftVar + ") (OutputCols $" + rightVar + "))\n" + "        )\n" + ")";
            return orderingEnv.setPattern(pattern).focus(pattern);
        }
        if (aggregate.source() instanceof RelRN.Join join && join.ty().semantics() == org.apache.calcite.rel.core.JoinRelType.LEFT) {
            String leftVar = env.generateVar("left");
            Env leftEnv = env.addBinding("left", leftVar);
            String aggsVar = leftEnv.generateVar("aggregations");
            Env aggsEnv = leftEnv.addBinding("aggregations", aggsVar);
            String groupingPrivateVar = aggsEnv.generateVar("groupingPrivate");
            Env groupingPrivateEnv = aggsEnv.addBinding("groupingPrivate", groupingPrivateVar);
            String groupingColsVar = groupingPrivateEnv.generateVar("groupingCols");
            Env groupingColsEnv = groupingPrivateEnv.addBinding("groupingCols", groupingColsVar);
            String orderingVar = groupingColsEnv.generateVar("ordering");
            Env orderingEnv = groupingColsEnv.addBinding("ordering", orderingVar);
            String leftColsVar = orderingEnv.generateVar("leftCols");
            Env leftColsEnv = orderingEnv.addBinding("leftCols", leftColsVar);
            String pattern = "(DistinctOn\n" + "    (LeftJoin\n" + "        $" + leftVar + ":*\n" + "        *\n" + "        *\n" + "    )\n" + "    $" + aggsVar + ":[]\n" + "    $" + groupingPrivateVar + ":(GroupingPrivate $" + groupingColsVar + ":* $" + orderingVar + ":*) &\n" + "        (ColsAreSubset\n" + "            $" + groupingColsVar + "\n" + "            $" + leftColsVar + ":(OutputCols $" + leftVar + ")\n" + "        ) &\n" + "        (OrderingCanProjectCols\n" + "            $" + orderingVar + "\n" + "            $" + leftColsVar + "\n" + "        )\n" + ")";
            return leftColsEnv.setPattern(pattern).focus(pattern);
        }
        if (aggregate.source() instanceof RelRN.Project project) {
            if (env.rulename.equals("AggregateProjectConstantToDummyJoin")) {
                String inputVar = env.generateVar("input");
                Env inputEnv = env.addBinding("input", inputVar);
                String aggregationsVar = inputEnv.generateVar("aggregations");
                Env aggsEnv = inputEnv.addBinding("aggregations", aggregationsVar);
                String groupingPrivateVar = aggsEnv.generateVar("groupingPrivate");
                Env groupingPrivateEnv = aggsEnv.addBinding("groupingPrivate", groupingPrivateVar);
                String aggregateType = determineAggregateType(aggregate);
                String pattern = "(" + aggregateType + "\n" + "    $" + inputVar + ":(Project * * *)\n" + "    $" + aggregationsVar + ":*\n" + "    $" + groupingPrivateVar + ":* & (HasConstantGroupingCols $" + inputVar + " $" + groupingPrivateVar + ")\n" + ")";
                return groupingPrivateEnv.setPattern(pattern).focus(pattern);
            }
            if (env.rulename.equals("AggregateProjectMerge")) {
                String inputVar = env.generateVar("input"); Env inputEnv = env.addBinding("input", inputVar);
                String sourceVar = inputEnv.generateVar("input"); Env sourceBoundEnv = inputEnv.addBinding("source", sourceVar);
                String aggregationsVar = sourceBoundEnv.generateVar("aggregations"); Env aggsEnv = sourceBoundEnv.addBinding("aggregations", aggregationsVar);
                String groupingPrivateVar = aggsEnv.generateVar("groupingPrivate"); Env groupingPrivateEnv = aggsEnv.addBinding("groupingPrivate", groupingPrivateVar);
                String aggregateType = determineAggregateType(aggregate);
                String pattern = "(" + aggregateType + "\n" + "    $" + inputVar + ":(Project\n" + "        $" + sourceVar + ":*\n" + "        *\n" + "        *\n" + "    )\n" + "    $" + aggregationsVar + ":*\n" + "    $" + groupingPrivateVar + ":* & (CanMergeProjectIntoAggregate $" + inputVar + " $" + groupingPrivateVar + ")\n" + ")";
                return groupingPrivateEnv.setPattern(pattern).focus(pattern);
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
            String pattern = "(" + aggregateType + "\n" + "    (Project\n" + "        $" + inputVar + ":*\n" + "        $" + projectionsVar + ":*\n" + "        $" + passthroughVar + ":*\n" + "    )\n" + "    $" + aggregationsVar + ":*\n" + "    $" + groupingPrivateVar + ":* & (CanRemapGroupingColsThroughProject $" + groupingPrivateVar + " $" + projectionsVar + " $" + passthroughVar + ")\n" + ")";
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
            String pattern = "(" + aggregateType + "\n    $" + inputVar + ":*\n    $" + aggregationsVar + ":* & (CanExtractProjectFromAggregate $" + aggregationsVar + ")\n    $" + groupingPrivateVar + ":*\n)";             return gpEnv.addBinding("isAggregateExtractProject", "true").setPattern(pattern).focus(pattern);         }                  String filterBoundBy = "";         if (aggregate.source() instanceof RelRN.Filter) {             String condVarName = sourceEnv.bindings().getOrDefault("filterCondVar", null);             if (condVarName != null) {                 filterBoundBy = " & (FiltersBoundBy $" + condVarName + " (GroupingCols $" + privateVar + "))";             }         }         String pattern = "(" + aggregateType + "\n    " + sourcePattern + "\n    " + aggsPattern + "\n    $" + privateVar + ":*" + filterBoundBy + "\n)";         return privateEnv.setPattern(pattern).focus(pattern);     }      private Env onMatchAggCalls(Env env, Seq<RelRN.AggCall> aggCalls) {         Env currentEnv = env;         Seq<String> aggPatterns = Seq.empty();         boolean hasProjOperand = false;         for (RelRN.AggCall aggCall : aggCalls) {             if (aggCall.operands().size() == 1) {                 RexRN operand = aggCall.operands().get(0);                 if (operand instanceof RexRN.Proj proj) {                     String projVar = currentEnv.bindings().getOrDefault(proj.operator().getName(), null);                     if (projVar != null) {                         aggPatterns = aggPatterns.appended("$" + projVar + ":*");                         hasProjOperand = true;                         continue;                     }                 }             }             String aggVar = currentEnv.generateVar("agg");             Env aggEnv = currentEnv.addBinding(aggCall.name(), aggVar);             aggPatterns = aggPatterns.appended("$" + aggVar + ":*");             currentEnv = aggEnv;         }         String pattern;         if (aggCalls.size() == 1 && hasProjOperand) {             pattern = aggPatterns.get(0);             return currentEnv.setPattern(pattern).focus(pattern);         } else if (aggCalls.size() == 1) {             String aggVar = currentEnv.generateVar("aggregations");             Env boundEnv = currentEnv.addBinding("aggregations", aggVar);             pattern = "$" + aggVar + ":*";             return boundEnv.setPattern(pattern).focus(pattern);         } else {             pattern = "[" + aggPatterns.joinToString(" ") + "]";             return currentEnv.setPattern(pattern).focus(pattern);         }     }      private Env onMatchGroupSet(Env env, Seq<RexRN> groupSet) {         Env currentEnv = env;         Seq<String> groupPatterns = Seq.empty();         for (RexRN groupCol : groupSet) {             Env groupEnv = onMatch(currentEnv, groupCol);             groupPatterns = groupPatterns.appended(groupEnv.current());             currentEnv = groupEnv;         }         String pattern = "[" + groupPatterns.joinToString(" ") + "]";         return currentEnv.setPattern(pattern).focus(pattern);     }      private String determineAggregateType(RelRN.Aggregate aggregate) {         return "GroupBy";     }      @Override     public Env onMatchEmpty(Env env, RelRN.Empty empty) {         String varName = env.generateVar("empty");         return env.addBinding("empty", varName)                 .focus("$" + varName + ":(Values)");     }      @Override     public Env onMatchField(Env env, RexRN.Field field) {         String varName = env.generateVar("field");         return env.addBinding("field_" + field.ordinal(), varName)                 .focus("$" + varName + ":*");     }      @Override     public Env onMatchPred(Env env, RexRN.Pred pred) {         String varName = env.generateVar("cond");         return env.addBinding(pred.operator().getName(), varName)                 .focus("$" + varName + ":*");     }      @Override     public Env onMatchProj(Env env, RexRN.Proj proj) {         String varName = env.generateVar("proj");         return env.addBinding(proj.operator().getName(), varName)                 .focus("$" + varName + ":*");     }      public Env onMatchGroupBy(Env env, RexRN.GroupBy groupBy) {         if (groupBy.sources().size() == 1) {             RexRN innerExpr = groupBy.sources().get(0);             if (innerExpr instanceof RexRN.Proj proj) {                 String projVar = env.bindings().getOrDefault(proj.operator().getName(), null);                 if (projVar != null) {                     return env.focus("$" + projVar + ":*");                 }             }         }         String varName = env.generateVar("groupBy");         return env.addBinding(groupBy.operator().getName(), varName)                 .focus("$" + varName + ":*");     }      @Override     public Env onMatchAnd(Env env, RexRN.And and) {         Env currentEnv = env;         Seq<String> operandPatterns = Seq.empty();         for (RexRN operand : and.sources()) {             Env operandEnv = onMatch(currentEnv, operand);             operandPatterns = operandPatterns.appended(operandEnv.current());             currentEnv = operandEnv;         }         String pattern = buildNestedAndPattern(operandPatterns);         return currentEnv.setPattern(pattern).focus(pattern);     }      private String buildNestedAndPattern(Seq<String> operands) {         if (operands.isEmpty()) {             return "(And)";         }         if (operands.size() == 1) {             return operands.get(0);         }         String left = operands.get(0);         String right = buildNestedAndPattern(operands.drop(1));         return "(And " + left + " " + right + ")";
    }

    @Override
    public Env onMatchTrue(Env env, RexRN literal) {
        String varName = env.generateVar("true");
        return env.addBinding("true_" + System.identityHashCode(literal), varName).focus("$" + varName + ":True").setPattern("$" + varName + ":True");
    }

    @Override
    public Env onMatchFalse(Env env, RexRN literal) {
        return env.focus("(False)").setPattern("(False)");
    }

    @Override
    public Env onMatchCustom(Env env, RelRN custom) {
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.AggregateGroupingByConstants aggGrouping) {
            if (aggGrouping.input() instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.ProjectWithConstantLiterals projectWithConstants) {
                String inputVar = env.generateVar("input");
                Env inputEnv = env.addBinding("input", inputVar);
                String aggregationsVar = inputEnv.generateVar("aggregations");
                Env aggsEnv = inputEnv.addBinding("aggregations", aggregationsVar);
                String groupingPrivateVar = aggsEnv.generateVar("groupingPrivate");
                Env groupingPrivateEnv = aggsEnv.addBinding("groupingPrivate", groupingPrivateVar);
                String aggregateType = "GroupBy";
                String pattern = "(" + aggregateType + "\n" + "    $" + inputVar + ":(Project * * *)\n" + "    $" + aggregationsVar + ":*\n" + "    $" + groupingPrivateVar + ":* & (HasConstantGroupingCols $" + inputVar + " $" + groupingPrivateVar + ")\n" + ")";
                return groupingPrivateEnv.setPattern(pattern).focus(pattern);
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
                String pattern = "(Project\n" + "    $" + inputVar + ":(GroupBy\n" + "        $" + aggInputVar + ":*\n" + "        $" + aggregationsVar + ":*\n" + "        $" + groupingPrivateVar + ":*\n" + "    )\n" + "    $" + projectionsVar + ":*\n" + "    $" + passthroughVar + ":* &\n" + "        (CanPruneAggCols\n" + "            $" + aggregationsVar + "\n" + "            $" + neededVar + ":(UnionCols\n" + "                (ProjectionOuterCols $" + projectionsVar + ")\n" + "                $" + passthroughVar + "\n" + "            )\n" + "        )\n" + ")";
                return neededBindEnv.setPattern(pattern).focus(pattern);
            }
            return onMatch(env, projectUsingSubset.input());
        }
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge.AggregateWithMultipleCalls aggregateWithMultiple) {return onMatch(env, aggregateWithMultiple.input());}
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge.SourceTable sourceTable) {var scanRN = new RelRN.Scan("Source", org.qed.RexRN.varType("Source_Type", true), false); return onMatchScan(env, scanRN);}
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.UnionWithConstantColumns unionWithConstants) {return onMatchUnion(env, new RelRN.Union(true, Seq.of(unionWithConstants.left(), unionWithConstants.right())));}
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.LeftProjectionWithConstants leftProj) {var projectRN = new RelRN.Project(leftProj.input().field(0), leftProj.input());return onMatchProject(env, projectRN);}
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.RightProjectionWithConstants rightProj) {var projectRN = new RelRN.Project(rightProj.input().field(0), rightProj.input());return onMatchProject(env, projectRN);}
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.SourceTable sourceTable) {var scanRN = new RelRN.Scan("Source", org.qed.RexRN.varType("Source_Type", true), false);return onMatchScan(env, scanRN);}
        if (custom instanceof org.qed.RRuleInstances.UnionToDistinct.DistinctUnion distinctUnion) {return onMatchUnion(env, new RelRN.Union(false, Seq.of(distinctUnion.left(), distinctUnion.right())));}
        if (custom instanceof org.qed.RRuleInstances.UnionToDistinct.UnionAll unionAll) {return onMatchUnion(env, new RelRN.Union(true, Seq.of(unionAll.left(), unionAll.right())));}
        return unimplementedOnMatch(env, custom);
    }

    @Override
    public Env onMatchCustom(Env env, RexRN custom) {
        if (custom instanceof RexRN.GroupBy groupBy) {return onMatchGroupBy(env, groupBy);}
        return unimplementedOnMatch(env, custom);
    }

    @Override
    public Env transformScan(Env env, RelRN.Scan scan) {
        String varName = env.bindings().getOrDefault(scan.name(), "input");
        String pattern = "$" + varName;         return env.setPattern(pattern).focus(pattern);     }      @Override     public Env transformFilter(Env env, RelRN.Filter filter) {         if (env.bindings().containsKey("isFilterSetOpTranspose")                 && filter.source() instanceof RelRN.Union                 && env.bindings().containsKey("input")                 && env.bindings().containsKey("left")                 && env.bindings().containsKey("right")                 && env.bindings().containsKey("colmap")                 && env.bindings().containsKey("filter")                 && env.bindings().containsKey("item")) {             String leftVar = env.bindings().get("left");             String rightVar = env.bindings().get("right");             String colmapVar = env.bindings().get("colmap");             String filterVar = env.bindings().get("filter");             String itemVar = env.bindings().get("item");                          String pattern = "(Select\n" + "    (Union\n" + "        (Select\n" + "            $" + leftVar + "\n" + "            [ (FiltersItem (MapSetOpFilterLeft $" + itemVar + " $" + colmapVar + ")) ]\n" + "        )\n" + "        (Select\n" + "            $" + rightVar + "\n" + "            [ (FiltersItem (MapSetOpFilterRight $" + itemVar + " $" + colmapVar + ")) ]\n" + "        )\n" + "        $" + colmapVar + "\n" + "    )\n" + "    (RemoveFiltersItem $" + filterVar + " $" + itemVar + ")\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.rulename.equals("JoinExtractFilter") && filter.source() instanceof RelRN.Join && env.bindings().containsKey("left") && env.bindings().containsKey("right") && env.bindings().containsKey("on") && env.bindings().containsKey("private")) {
            String leftVar = env.bindings().get("left");
            String rightVar = env.bindings().get("right");
            String onVar = env.bindings().get("on");
            String privateVar = env.bindings().get("private");
            String pattern = "(ConstructJoinExtractFilterResult\n" + "    $" + leftVar + "\n" + "    $" + rightVar + "\n" + "    $" + onVar + "\n" + "    $" + privateVar + "\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.bindings().containsKey("isPruneEmptyFilter")) {
            String inputVar = env.bindings().get("pruneEmptyInput");
            String pattern = "$" + inputVar;             return env.setPattern(pattern).focus(pattern);         }                  if (filter.cond() instanceof RexRN.True) {             return transform(env, filter.source());         }         if (filter.source() instanceof RelRN.Empty) {             return transform(env, filter.source());         }         if (filter.cond() instanceof RexRN.False) {             Env sourceEnv = transform(env, filter.source());             String sourcePattern = sourceEnv.current();             String pattern = "(ConstructEmptyValues (OutputCols " + sourcePattern + "))";             return sourceEnv.setPattern(pattern).focus(pattern);         }         Env sourceEnv = transform(env, filter.source());         String sourcePattern = sourceEnv.current();         Env condEnv = transform(sourceEnv, filter.cond());         String condPattern = condEnv.current();         String filterPattern;         if (condPattern.startsWith("(ConcatFilters")) {             filterPattern = condPattern;         } else if (condPattern.startsWith("$") && !condPattern.contains(" ")) {             filterPattern = condPattern;         } else {             filterPattern = "[" + condPattern + "]";         }         String pattern = "(Select\n    " + sourcePattern + "\n    " + filterPattern + "\n)";         return condEnv.setPattern(pattern).focus(pattern);     }      @Override     public Env transformProject(Env env, RelRN.Project project) {         if (env.bindings().containsKey("input")                 && env.bindings().containsKey("cond")                 && env.bindings().containsKey("proj")                 && env.bindings().containsKey("passthrough")) {             String inputVar = env.bindings().get("input");             String condVar = env.bindings().get("cond");             String projVar = env.bindings().get("proj");             String passthroughVar = env.bindings().get("passthrough");                          String pattern = "(Project\n" + "    (Select\n" + "    $" + inputVar + "\n" + "    $" + condVar + "\n" + ")\n" + "    $" + projVar + "\n" + "    $" + passthroughVar + "\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.bindings().containsKey("hasZeroRows")) {
            String inputVar = env.bindings().getOrDefault("zeroInput", "input");
            String pattern = "$" + inputVar;             return env.setPattern(pattern).focus(pattern);         }         if (env.bindings().containsKey("innerInput")                 && env.bindings().containsKey("aggregations")                 && env.bindings().containsKey("groupingPrivate")                 && env.bindings().containsKey("projections")                 && env.bindings().containsKey("passthrough")                 && env.bindings().containsKey("needed")) {                          String innerInputVar = env.bindings().get("innerInput");             String aggregationsVar = env.bindings().get("aggregations");             String groupingPrivateVar = env.bindings().get("groupingPrivate");             String projectionsVar = env.bindings().get("projections");             String passthroughVar = env.bindings().get("passthrough");             String neededVar = env.bindings().get("needed");                          if (project.source() instanceof RelRN.Aggregate aggregate) {                 String pattern = "(Project\n" + "    (GroupBy\n" + "        $" + innerInputVar + "\n" + "        (PruneAggCols $" + aggregationsVar + " $" + neededVar + ")\n" + "        $" + groupingPrivateVar + "\n" + "    )\n" + "    $" + projectionsVar + "\n" + "    $" + passthroughVar + "\n" + ")";
                return env.setPattern(pattern).focus(pattern);
            }
        }
        if (env.rulename.equals("ProjectMerge")) {
            String pattern = "(Project\n    $input_1\n    (MergeProjections\n        $proj_0\n        $proj_2\n        $passthrough_4\n    )\n    (DifferenceCols\n        $innerPassthrough_3\n        (ProjectionCols $proj_2)\n    )\n)";             return env.setPattern(pattern).focus(pattern);         }         Env sourceEnv = transform(env, project.source());         String sourcePattern = sourceEnv.current();         Env projEnv = transform(sourceEnv, project.map());         String projPattern = projEnv.current();         String passthroughVar = projEnv.bindings().getOrDefault("passthrough", "passthrough");         String pattern = "(Project\n    " + sourcePattern + "\n    " + projPattern + "\n    $" + passthroughVar + "\n)";         return projEnv.setPattern(pattern).focus(pattern);     }      @Override     public Env transformUnion(Env env, RelRN.Union union) {         if (env.bindings().containsKey("isFilterSetOpTranspose")                 && env.bindings().containsKey("left")                 && env.bindings().containsKey("right")                 && env.bindings().containsKey("colmap")                 && env.bindings().containsKey("filter")                 && env.bindings().containsKey("item")) {             String leftVar = env.bindings().get("left");             String rightVar = env.bindings().get("right");             String colmapVar = env.bindings().get("colmap");             String filterVar = env.bindings().get("filter");             String itemVar = env.bindings().get("item");                          String pattern = "(Select\n" + "    (Union\n" + "        (Select\n" + "            $" + leftVar + "\n" + "            [ (FiltersItem (MapSetOpFilterLeft $" + itemVar + " $" + colmapVar + ")) ]\n" + "        )\n" + "        (Select\n" + "            $" + rightVar + "\n" + "            [ (FiltersItem (MapSetOpFilterRight $" + itemVar + " $" + colmapVar + ")) ]\n" + "        )\n" + "        $" + colmapVar + "\n" + "    )\n" + "    (RemoveFiltersItem $" + filterVar + " $" + itemVar + ")\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.bindings().containsKey("left") && env.bindings().containsKey("right") && env.bindings().containsKey("private") && env.bindings().containsKey("leftCols") && env.bindings().containsKey("rightCols") && env.bindings().containsKey("outCols") && env.bindings().containsKey("keyCols")) {
            String leftVar = env.bindings().get("left");
            String rightVar = env.bindings().get("right");
            String privateVar = env.bindings().get("private");
            String leftColsVar = env.bindings().get("leftCols");
            String rightColsVar = env.bindings().get("rightCols");
            String outColsVar = env.bindings().get("outCols");
            String keyColsVar = env.bindings().get("keyCols");
            String pattern = "(DistinctOn\n" + "    (UnionAll $" + leftVar + " $" + rightVar + " $" + privateVar + ")\n" + "    (MakeAggCols\n" + "        ConstAgg\n" + "        (TranslateColSet\n" + "            (DifferenceCols (OutputCols $" + leftVar + ") $" + keyColsVar + ")\n" + "            $" + leftColsVar + "\n" + "            $" + outColsVar + "\n" + "        )\n" + "    )\n" + "    (MakeGrouping\n" + "        (TranslateColSet $" + keyColsVar + " $" + leftColsVar + " $" + outColsVar + ")\n" + "        (EmptyOrdering)\n" + "    )\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.bindings().containsKey("left") && env.bindings().containsKey("right") && env.bindings().containsKey("leftProjections") && env.bindings().containsKey("rightProjections") && env.bindings().containsKey("private") && env.bindings().containsKey("leftInput") && env.bindings().containsKey("rightInput") && env.bindings().containsKey("leftPassthrough") && env.bindings().containsKey("rightPassthrough")) {
            String leftVar = env.bindings().get("left");
            String rightVar = env.bindings().get("right");
            String leftProjectionsVar = env.bindings().get("leftProjections");
            String rightProjectionsVar = env.bindings().get("rightProjections");
            String privateVar = env.bindings().get("private");
            String leftInputVar = env.bindings().get("leftInput");
            String rightInputVar = env.bindings().get("rightInput");
            String leftPassthroughVar = env.bindings().get("leftPassthrough");
            String rightPassthroughVar = env.bindings().get("rightPassthrough");
            String pattern = "(UnionPullUpConstantsReplace\n" + "    $" + leftVar + "\n" + "    $" + rightVar + "\n" + "    $" + leftProjectionsVar + "\n" + "    $" + rightProjectionsVar + "\n" + "    $" + privateVar + "\n" + "    $" + leftInputVar + "\n" + "    $" + rightInputVar + "\n" + "    $" + leftPassthroughVar + "\n" + "    $" + rightPassthroughVar + "\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.bindings().containsKey("isUnionMerge") && env.bindings().containsKey("leftLeft") && env.bindings().containsKey("leftRight") && env.bindings().containsKey("right") && env.bindings().containsKey("innerLeftCols") && env.bindings().containsKey("innerRightCols") && env.bindings().containsKey("innerOutCols") && env.bindings().containsKey("outerRightCols") && env.bindings().containsKey("outerOutCols")) {
            String leftLeftVar = env.bindings().get("leftLeft");
            String leftRightVar = env.bindings().get("leftRight");
            String rightVar = env.bindings().get("right");
            String innerLeftColsVar = env.bindings().get("innerLeftCols");
            String innerRightColsVar = env.bindings().get("innerRightCols");
            String innerOutColsVar = env.bindings().get("innerOutCols");
            String outerRightColsVar = env.bindings().get("outerRightCols");
            String outerOutColsVar = env.bindings().get("outerOutCols");
            String unionType = union.all() ? "UnionAll" : "Union";
            String pattern = "(" + unionType + "\n" + "    $" + leftLeftVar + "\n" + "    (" + unionType + "\n" + "        $" + leftRightVar + "\n" + "        $" + rightVar + "\n" + "        (MakeSetPrivate $" + innerRightColsVar + " $" + outerRightColsVar + " $" + innerOutColsVar + ")\n" + "    )\n" + "    (MakeSetPrivate $" + innerLeftColsVar + " $" + innerOutColsVar + " $" + outerOutColsVar + ")\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.bindings().containsKey("hasZeroRows")) {
            String patternStr = env.pattern();
            if (patternStr != null && patternStr.contains("SetPrivate") && patternStr.contains("outCols")) {
                java.util.regex.Pattern outColsPattern = java.util.regex.Pattern.compile("\\$outCols_(\\d+)");
                java.util.regex.Matcher matcher = outColsPattern.matcher(patternStr);
                if (matcher.find()) {
                    String outColsVar = "$outCols_" + matcher.group(1);
                    String pattern = "(ConstructEmptyValues (ColListToSet " + outColsVar + "))";                     return env.setPattern(pattern).focus(pattern);                 }                 String pattern = "(ConstructEmptyValues (ColListToSet $outCols))";                 return env.setPattern(pattern).focus(pattern);             }             String leftVar = env.bindings().getOrDefault("zeroInput", "input");             String pattern = "(ConstructEmptyValues (OutputCols $" + leftVar + "))";             return env.setPattern(pattern).focus(pattern);         }         Env currentEnv = env;         Seq<String> sourcePatterns = Seq.empty();         for (RelRN source : union.sources()) {             Env sourceEnv = transform(currentEnv, source);             sourcePatterns = sourcePatterns.appended(sourceEnv.current());             currentEnv = sourceEnv;         }         String privateVar = currentEnv.bindings().getOrDefault("union_private", "private");         String unionType = union.all() ? "UnionAll" : "Union";         String pattern;         if (sourcePatterns.size() == 2) {             pattern = "(" + unionType + "\n    " + sourcePatterns.get(0) + "\n    " + sourcePatterns.get(1) + "\n    $" + privateVar + "\n)";         } else {             String nestedPrivate = currentEnv.bindings().getOrDefault("inner_union_private", privateVar);             String nested = buildNestedUnionTransform(unionType, sourcePatterns.drop(1), nestedPrivate);             pattern = "(" + unionType + "\n    " + sourcePatterns.get(0) + "\n    " + nested + "\n    $" + privateVar + "\n)";         }         return currentEnv.setPattern(pattern).focus(pattern);     }      private String buildNestedUnionTransform(String unionType, Seq<String> sources, String privateVar) {         if (sources.size() == 2) {             return "(" + unionType + "\n    " + sources.get(0) + "\n    " + sources.get(1) + "\n    $" + privateVar + "\n)";         }         String first = sources.get(0);         String nested = buildNestedUnionTransform(unionType, sources.drop(1), privateVar);         return "(" + unionType + "\n    " + first + "\n    " + nested + "\n    $" + privateVar + "\n)";     }      @Override     public Env transformIntersect(Env env, RelRN.Intersect intersect) {         if (env.bindings().containsKey("isIntersectMerge") &&             env.bindings().containsKey("leftLeft") &&             env.bindings().containsKey("leftRight") &&             env.bindings().containsKey("right") &&             env.bindings().containsKey("innerLeftCols") &&             env.bindings().containsKey("innerRightCols") &&             env.bindings().containsKey("outerRightCols") &&             env.bindings().containsKey("outerOutCols")) {             String leftLeftVar = env.bindings().get("leftLeft");             String leftRightVar = env.bindings().get("leftRight");             String rightVar = env.bindings().get("right");             String innerLeftColsVar = env.bindings().get("innerLeftCols");             String innerRightColsVar = env.bindings().get("innerRightCols");             String outerRightColsVar = env.bindings().get("outerRightCols");             String outerOutColsVar = env.bindings().get("outerOutCols");             String intersectType = intersect.all() ? "IntersectAll" : "Intersect";             String pattern = "(" + intersectType + "\n" + "    $" + leftLeftVar + "\n" + "    (" + intersectType + "\n" + "        $" + leftRightVar + "\n" + "        $" + rightVar + "\n" + "        (MakeSetPrivate $" + innerRightColsVar + " $" + outerRightColsVar + " $" + innerRightColsVar + ")\n" + "    )\n" + "    (MakeSetPrivate $" + innerLeftColsVar + " $" + innerRightColsVar + " $" + outerOutColsVar + ")\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.bindings().containsKey("isPruneEmptyIntersect")) {
            String leftVar = env.bindings().get("pruneEmptyLeft");
            String pattern = "(ConstructEmptyValues (OutputCols $" + leftVar + "))";             return env.setPattern(pattern).focus(pattern);         }                  Env currentEnv = env;         Seq<String> sourcePatterns = Seq.empty();         for (RelRN source : intersect.sources()) {             Env sourceEnv = transform(currentEnv, source);             sourcePatterns = sourcePatterns.appended(sourceEnv.current());             currentEnv = sourceEnv;         }         String privateVar = currentEnv.bindings().get("intersect_private");         if (privateVar == null) {             privateVar = "private";         }         String intersectType = intersect.all() ? "IntersectAll" : "Intersect";         String pattern;         if (sourcePatterns.size() == 2) {             pattern = "(" + intersectType + "\n    " + sourcePatterns.get(0) + "\n    " + sourcePatterns.get(1) + "\n    $" + privateVar + "\n)";         } else {             String nestedPrivate = currentEnv.bindings().get("inner_intersect_private");             if (nestedPrivate == null) {                 nestedPrivate = privateVar;             }             String nested = buildNestedIntersectTransform(intersectType, sourcePatterns.drop(1), nestedPrivate);             pattern = "(" + intersectType + "\n    " + sourcePatterns.get(0) + "\n    " + nested + "\n    $" + privateVar + "\n)";         }         return currentEnv.setPattern(pattern).focus(pattern);     }       private String buildNestedIntersectTransform(String intersectType, Seq<String> sources, String privateVar) {         if (sources.size() == 2) {             return "(" + intersectType + "\n    " + sources.get(0) + "\n    " + sources.get(1) + "\n    $" + privateVar + "\n)";         }         String first = sources.get(0);         String nested = buildNestedIntersectTransform(intersectType, sources.drop(1), privateVar);         return "(" + intersectType + "\n    " + first + "\n    " + nested + "\n    $" + privateVar + "\n)";     }      @Override     public Env transformMinus(Env env, RelRN.Minus minus) {         if (env.bindings().containsKey("isPruneEmptyMinus")) {             String leftVar = env.bindings().get("pruneEmptyLeft");             String pattern = "(ConstructEmptyValues (OutputCols $" + leftVar + "))";             return env.setPattern(pattern).focus(pattern);         }         if (env.bindings().containsKey("isMinusMerge")                 && env.bindings().containsKey("leftLeft")                 && env.bindings().containsKey("leftRight")                 && env.bindings().containsKey("right")                 && env.bindings().containsKey("innerPrivate")                 && env.bindings().containsKey("outerPrivate")) {             String leftLeftVar = env.bindings().get("leftLeft");             String leftRightVar = env.bindings().get("leftRight");             String rightVar = env.bindings().get("right");             String innerPrivateVar = env.bindings().get("innerPrivate");             String outerPrivateVar = env.bindings().get("outerPrivate");             String pattern = "(ConstructMinusMergeResult\n" + "    $" + leftLeftVar + "\n" + "    $" + leftRightVar + "\n" + "    $" + rightVar + "\n" + "    $" + innerPrivateVar + "\n" + "    $" + outerPrivateVar + "\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        String pattern = "(Except\n" + "    $left\n" + "    (Union\n" + "        $rightB\n" + "        $rightC\n" + "        (MakeUnionPrivateForExcept $pInner $pOuter)\n" + "    )\n" + "    $pOuter\n" + ")";
        return env.setPattern(pattern).focus(pattern);
    }

    @Override
    public Env transformAggregate(Env env, RelRN.Aggregate aggregate) {
        if (env.bindings().containsKey("left") && env.bindings().containsKey("right") && env.bindings().containsKey("rightFilters") && env.bindings().containsKey("aggregations") && env.bindings().containsKey("groupingPrivate") && !env.bindings().containsKey("topOn") && !env.bindings().containsKey("topPrivate")) {
            String leftVar = env.bindings().get("left");
            String rightVar = env.bindings().get("right");
            String rightFiltersVar = env.bindings().get("rightFilters");
            String aggsVar = env.bindings().get("aggregations");
            String groupingPrivateVar = env.bindings().get("groupingPrivate");
            String pattern = "(DistinctOn\n" + "    (LeftJoin\n" + "        $" + leftVar + "\n" + "        $" + rightVar + "\n" + "        $" + rightFiltersVar + "\n" + "        (EmptyJoinPrivate)\n" + "    )\n" + "    $" + aggsVar + "\n" + "    $" + groupingPrivateVar + "\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.bindings().containsKey("left") && env.bindings().containsKey("aggregations")
                && env.bindings().containsKey("groupingPrivate")
                && !env.bindings().containsKey("rightFilters") && !env.bindings().containsKey("topOn")) {
            String leftVar = env.bindings().get("left");
            String aggsVar = env.bindings().get("aggregations");
            String groupingPrivateVar = env.bindings().get("groupingPrivate");
            String pattern = "(DistinctOn\n" + "    $" + leftVar + "\n" + "    $" + aggsVar + "\n" + "    $" + groupingPrivateVar + "\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.rulename.equals("AggregateProjectConstantToDummyJoin") && env.bindings().containsKey("input") && env.bindings().containsKey("aggregations") && env.bindings().containsKey("groupingPrivate") && !env.bindings().containsKey("projectInput")) {
            String inputVar = env.bindings().get("input");
            String aggregationsVar = env.bindings().get("aggregations");
            String groupingPrivateVar = env.bindings().get("groupingPrivate");
            String pattern = "(ConstructAggregateProjectConstantToDummyJoin\n" + "    $" + inputVar + "\n" + "    $" + aggregationsVar + "\n" + "    $" + groupingPrivateVar + "\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.rulename.equals("AggregateProjectMerge") && env.bindings().containsKey("input") && env.bindings().containsKey("source") && env.bindings().containsKey("aggregations") && env.bindings().containsKey("groupingPrivate") && !env.bindings().containsKey("projections")) {
            String inputVar = env.bindings().get("input");
            String sourceVar = env.bindings().get("source");
            String aggregationsVar = env.bindings().get("aggregations");
            String groupingPrivateVar = env.bindings().get("groupingPrivate");
            String aggregateType = determineAggregateType(aggregate);
            String pattern = "(" + aggregateType + "\n" + "    $" + sourceVar + "\n" + "    (MergeProjectIntoAggregate $" + inputVar + " $" + aggregationsVar + ")\n" + "    $" + groupingPrivateVar + "\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.bindings().containsKey("input") && env.bindings().containsKey("projections") && env.bindings().containsKey("passthrough") && env.bindings().containsKey("aggregations") && env.bindings().containsKey("groupingPrivate")) {
            String inputVar = env.bindings().get("input");
            String projectionsVar = env.bindings().get("projections");
            String passthroughVar = env.bindings().get("passthrough");
            String aggregationsVar = env.bindings().get("aggregations");
            String groupingPrivateVar = env.bindings().get("groupingPrivate");
            String aggregateType = determineAggregateType(aggregate);
            String pattern = "(" + aggregateType + "\n" + "    $" + inputVar + "\n" + "    (RemapAggregationsThroughProject $" + aggregationsVar + " $" + projectionsVar + ")\n" + "    (RemapGroupingColsThroughProject $" + groupingPrivateVar + " $" + projectionsVar + " $" + passthroughVar + ")\n" + ")";
            return env.setPattern(pattern).focus(pattern);
        }
        if (env.bindings().containsKey("isAggregateExtractProject")) {
            String pattern = "(ConstructAggregateExtractProject\n    $input\n    $aggregations\n    $groupingPrivate\n)";             return env.setPattern(pattern).focus(pattern);         }                  Env sourceEnv = transform(env, aggregate.source());         String sourcePattern = sourceEnv.current();         Env groupingEnv = transformGroupSet(sourceEnv, aggregate.groupSet());         Env aggsEnv = transformAggCalls(groupingEnv, aggregate.aggCalls());         String aggsPattern = aggsEnv.current();         String privateVar = aggsEnv.bindings().getOrDefault("aggregate_private", "private");         String aggregateType = determineAggregateType(aggregate);           String pattern = "(" + aggregateType + "\n    " + sourcePattern + "\n    " + aggsPattern + "\n    $" + privateVar + "\n)";         return aggsEnv.setPattern(pattern).focus(pattern);     }      private boolean hasProjectionExpressionsInAggregate(RelRN.Aggregate aggregate) {         for (RexRN groupExpr : aggregate.groupSet()) {             if (groupExpr instanceof RexRN.Proj) {                 return true;             }         }                  for (RelRN.AggCall aggCall : aggregate.aggCalls()) {             for (RexRN operand : aggCall.operands()) {                 if (operand instanceof RexRN.Proj) {                     return true;                 }             }         }                  return false;     }      private Env transformAggCalls(Env env, Seq<RelRN.AggCall> aggCalls) {         Env currentEnv = env;         Seq<String> aggPatterns = Seq.empty();         for (RelRN.AggCall aggCall : aggCalls) {             String aggVar = currentEnv.bindings().getOrDefault(aggCall.name(), "agg");             aggPatterns = aggPatterns.appended("$" + aggVar);             currentEnv = currentEnv.focus("$" + aggVar);         }         String pattern;         if (aggCalls.size() == 1) {             String aggVar = currentEnv.bindings().getOrDefault("aggregations", "aggregations");             pattern = "$" + aggVar;         } else {             pattern = "[" + aggPatterns.joinToString(" ") + "]";         }         return currentEnv.setPattern(pattern).focus(pattern);     }      private Env transformGroupSet(Env env, Seq<RexRN> groupSet) {         Env currentEnv = env;         Seq<String> groupPatterns = Seq.empty();         for (RexRN groupCol : groupSet) {             Env groupEnv = transform(currentEnv, groupCol);             groupPatterns = groupPatterns.appended(groupEnv.current());             currentEnv = groupEnv;         }         String pattern = "[" + groupPatterns.joinToString(" ") + "]";         return currentEnv.setPattern(pattern).focus(pattern);     }      @Override     public Env transformEmpty(Env env, RelRN.Empty empty) {         if (env.bindings().containsKey("isPruneEmptyMinus")) {             String leftVar = env.bindings().get("pruneEmptyLeft");             String pattern = "(ConstructEmptyValues (OutputCols $" + leftVar + "))";             return env.setPattern(pattern).focus(pattern);         }         if (env.bindings().containsKey("hasZeroRows")) {             if (env.bindings().containsKey("projections") && env.bindings().containsKey("passthrough")) {                 String projectionsVar = env.bindings().get("projections");                 String passthroughVar = env.bindings().get("passthrough");                 String pattern = "(ConstructEmptyValues (UnionCols (ProjectionCols $" + projectionsVar + ") $" + passthroughVar + "))";                 return env.setPattern(pattern).focus(pattern);             }             String inputVar = env.bindings().getOrDefault("zeroInput", "input");             String patternStr = env.pattern();             if (patternStr != null && patternStr.contains("Union") && (patternStr.contains("$left") || inputVar.startsWith("left"))) {                 String pattern = "(ConstructEmptyValues (OutputCols $" + inputVar + "))";                 return env.setPattern(pattern).focus(pattern);             }             String pattern = "$" + inputVar;             return env.setPattern(pattern).focus(pattern);         }         if (env.bindings().containsKey("isPruneEmptyFilter")) {             String inputVar = env.bindings().get("pruneEmptyInput");             String pattern = "$" + inputVar;             return env.setPattern(pattern).focus(pattern);         }         String pattern = "(ConstructEmptyValues (OutputCols $input_0))";         return env.setPattern(pattern).focus(pattern);     }      @Override     public Env transformField(Env env, RexRN.Field field) {         String varName = env.bindings().get("field_" + field.ordinal());         if (varName == null) {             varName = "field";         }         String pattern = "$" + varName;         return env.setPattern(pattern).focus(pattern);     }      @Override     public Env transformPred(Env env, RexRN.Pred pred) {         String varName = env.bindings().getOrDefault(pred.operator().getName(), "cond");         String pattern = "$" + varName;         return env.setPattern(pattern).focus(pattern);     }      @Override     public Env transformProj(Env env, RexRN.Proj proj) {         String varName = env.bindings().getOrDefault(proj.operator().getName(), "proj");         String pattern = "$" + varName;         return env.setPattern(pattern).focus(pattern);     }      public Env transformGroupBy(Env env, RexRN.GroupBy groupBy) {         if (groupBy.sources().size() == 1) {             RexRN innerExpr = groupBy.sources().get(0);             if (innerExpr instanceof RexRN.Proj proj) {                 String projVar = env.bindings().get(proj.operator().getName());                 if (projVar != null) {                     return env.setPattern("$" + projVar).focus("$" + projVar);                 }             }         }         String varName = env.bindings().get(groupBy.operator().getName());         if (varName == null) {             varName = "groupBy";         }         String pattern = "$" + varName;         return env.setPattern(pattern).focus(pattern);     }      @Override     public Env transformAnd(Env env, RexRN.And and) {         Env currentEnv = env;         Seq<String> operandPatterns = Seq.empty();          for (RexRN operand : and.sources()) {             Env operandEnv = transform(currentEnv, operand);             operandPatterns = operandPatterns.appended(operandEnv.current());             currentEnv = operandEnv;         }          String pattern = "(ConcatFilters " + operandPatterns.joinToString(" ") + ")";
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
        if (custom instanceof org.qed.RRuleInstances.JoinCommute.ProjectionRelRN projection) {return transform(env, projection.source());}
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.AggregateGroupingByDummyFields aggGrouping) {
            if (env.rulename.equals("AggregateProjectConstantToDummyJoin") && env.bindings().containsKey("input") && env.bindings().containsKey("aggregations") && env.bindings().containsKey("groupingPrivate") && !env.bindings().containsKey("projectInput")) {
                String inputVar = env.bindings().get("input");
                String aggregationsVar = env.bindings().get("aggregations");
                String groupingPrivateVar = env.bindings().get("groupingPrivate");
                String pattern = "(ConstructAggregateProjectConstantToDummyJoin\n" + "    $" + inputVar + "\n" + "    $" + aggregationsVar + "\n" + "    $" + groupingPrivateVar + "\n" + ")";
                return env.setPattern(pattern).focus(pattern);
            }
            return transform(env, aggGrouping.input());
        }
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.ProjectWithDummyFields projectWithDummy) {return transform(env, projectWithDummy.input());}
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.JoinWithDummyTable joinWithDummy) {return transform(env, joinWithDummy.baseTable());}
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.DummyConstantsTable dummyTable) {return env;}
        if (custom instanceof org.qed.RRuleInstances.AggregateProjectConstantToDummyJoin.SourceTable sourceTable) {String varName = env.bindings().getOrDefault("projectInput", "input_0");return env.setPattern("$" + varName).focus("$" + varName);}
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge.ProjectOptimized projectOptimized) {
            if (env.rulename.equals("ProjectAggregateMerge") && env.bindings().containsKey("aggInput") && env.bindings().containsKey("aggregations") && env.bindings().containsKey("groupingPrivate") && env.bindings().containsKey("projections") && env.bindings().containsKey("passthrough") && env.bindings().containsKey("needed")) {
                if (projectOptimized.input() instanceof org.qed.RRuleInstances.ProjectAggregateMerge.AggregateWithUsedCallsOnly aggregateOptimized) {
                    Env aggInputEnv = transform(env, aggregateOptimized.input());
                    String aggInputPattern = aggInputEnv.current();
                    String aggInputVar = env.bindings().get("aggInput");
                    String aggregationsVar = env.bindings().get("aggregations");
                    String groupingPrivateVar = env.bindings().get("groupingPrivate");
                    String projectionsVar = env.bindings().get("projections");
                    String passthroughVar = env.bindings().get("passthrough");
                    String neededVar = env.bindings().get("needed");
                    String pattern = "(Project\n" + "    (GroupBy\n" + "        " + aggInputPattern + "\n" + "        (PruneAggCols $" + aggregationsVar + " $" + neededVar + ")\n" + "        $" + groupingPrivateVar + "\n" + "    )\n" + "    $" + projectionsVar + "\n" + "    $" + passthroughVar + "\n" + ")";
                    return aggInputEnv.setPattern(pattern).focus(pattern);
                }
            }
            return transform(env, projectOptimized.input());
        }
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge.AggregateWithUsedCallsOnly aggregateOptimized) {return transform(env, aggregateOptimized.input());}
        if (custom instanceof org.qed.RRuleInstances.ProjectAggregateMerge.SourceTable sourceTable) {String varName = env.bindings().getOrDefault("aggInput", "input_0"); return env.setPattern("$" + varName).focus("$" + varName);}
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.TopProjectionWithConstants topProj) {
            if (env.rulename.equals("UnionPullUpConstants") && env.bindings().containsKey("left") && env.bindings().containsKey("right") && env.bindings().containsKey("leftProjections") && env.bindings().containsKey("rightProjections") && env.bindings().containsKey("private") && env.bindings().containsKey("leftInput") && env.bindings().containsKey("rightInput") && env.bindings().containsKey("leftPassthrough") && env.bindings().containsKey("rightPassthrough")) {
                String leftVar = env.bindings().get("left");
                String rightVar = env.bindings().get("right");
                String leftProjectionsVar = env.bindings().get("leftProjections");
                String rightProjectionsVar = env.bindings().get("rightProjections");
                String privateVar = env.bindings().get("private");
                String leftInputVar = env.bindings().get("leftInput");
                String rightInputVar = env.bindings().get("rightInput");
                String leftPassthroughVar = env.bindings().get("leftPassthrough");
                String rightPassthroughVar = env.bindings().get("rightPassthrough");
                String pattern = "(UnionPullUpConstantsReplace\n" + "    $" + leftVar + "\n" + "    $" + rightVar + "\n" + "    $" + leftProjectionsVar + "\n" + "    $" + rightProjectionsVar + "\n" + "    $" + privateVar + "\n" + "    $" + leftInputVar + "\n" + "    $" + rightInputVar + "\n" + "    $" + leftPassthroughVar + "\n" + "    $" + rightPassthroughVar + "\n" + ")";
                return env.setPattern(pattern).focus(pattern);
            }
            return transform(env, topProj.input());
        }
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.UnionWithConstantColumns unionWithConstants) {return transformUnion(env, new RelRN.Union(true, Seq.of(unionWithConstants.left(), unionWithConstants.right())));}
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.LeftProjectionWithConstants leftProj) {return transformProject(env, new RelRN.Project(leftProj.input().field(0), leftProj.input()));}
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.RightProjectionWithConstants rightProj) {return transformProject(env, new RelRN.Project(rightProj.input().field(0), rightProj.input()));}
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.UnionReducedColumns unionReduced) {Env leftEnv = transform(env, unionReduced.left()); Env rightEnv = transform(leftEnv, unionReduced.right()); return rightEnv;}
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.LeftProjectionNonConstants leftProj) {return transform(env, leftProj.input());}
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.RightProjectionNonConstants rightProj) {return transform(env, rightProj.input());}
        if (custom instanceof org.qed.RRuleInstances.UnionPullUpConstants.SourceTable sourceTable) {String varName = env.bindings().getOrDefault("leftInput", env.bindings().getOrDefault("rightInput", env.bindings().getOrDefault("left", env.bindings().getOrDefault("right", "input_0")))); return env.setPattern("$" + varName).focus("$" + varName);}
        if (custom instanceof org.qed.RRuleInstances.UnionToDistinct.DistinctAggregate distinctAgg) {
            if (env.rulename.equals("UnionToDistinct") && env.bindings().containsKey("left") && env.bindings().containsKey("right") && env.bindings().containsKey("private") && env.bindings().containsKey("leftCols") && env.bindings().containsKey("rightCols") && env.bindings().containsKey("outCols") && env.bindings().containsKey("keyCols")) {
                String leftVar = env.bindings().get("left");
                String rightVar = env.bindings().get("right");
                String privateVar = env.bindings().get("private");
                String leftColsVar = env.bindings().get("leftCols");
                String rightColsVar = env.bindings().get("rightCols");
                String outColsVar = env.bindings().get("outCols");
                String keyColsVar = env.bindings().get("keyCols");
                String pattern = "(DistinctOn\n" + "    (UnionAll $" + leftVar + " $" + rightVar + " $" + privateVar + ")\n" + "    (MakeAggCols\n" + "        ConstAgg\n" + "        (TranslateColSet\n" + "            (DifferenceCols (OutputCols $" + leftVar + ") $" + keyColsVar + ")\n" + "            $" + leftColsVar + "\n" + "            $" + outColsVar + "\n" + "        )\n" + "    )\n" + "    (MakeGrouping\n" + "        (TranslateColSet $" + keyColsVar + " $" + leftColsVar + " $" + outColsVar + ")\n" + "        (EmptyOrdering)\n" + "    )\n" + ")";
                return env.setPattern(pattern).focus(pattern);
            }
            return transform(env, distinctAgg.input());
        }
        if (custom instanceof org.qed.RRuleInstances.UnionToDistinct.UnionAll unionAll) {Env leftEnv = transform(env, unionAll.left()); Env rightEnv = transform(leftEnv, unionAll.right()); return rightEnv;}
        return unimplementedTransform(env, custom);
    }

    @Override
    public Env transformCustom(Env env, RexRN custom) {
        if (custom instanceof RexRN.GroupBy groupBy) {return transformGroupBy(env, groupBy);}
        return unimplementedTransform(env, custom);
    }

    private static String normalizeVars(String str, String[] varNames) {
        for (String varName : varNames) {
            if (str.contains("$" + varName + "_")) {
                str = str.replaceAll("\\$" + varName + "_\\d+", java.util.regex.Matcher.quoteReplacement("$" + varName));
            }
        }
        return str;
    }

    @Override
    public String translate(String name, Env onMatch, Env transform) {
        StringBuilder sb = new StringBuilder("[").append(name).append(", Normalize]\n");
        String match = onMatch.pattern();
        if (match.contains("HasZeroRows")) {match = normalizeVars(match, new String[]{"projections", "passthrough", "filters"});}
        if (match.contains("HasZeroRows") && (match.startsWith("(Union") || match.startsWith("(UnionAll"))) {match = match.replaceAll("\\s+\\$private_\\d+:\\*\\s*\\)", "\n)").replaceAll("\\s+\\$private_\\d+:\\*\\)", ")");}
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
        String numbered = findFirstVar(match);
        if (out.startsWith("(ConstructEmptyValues (OutputCols $")) {
            int startIdx = "(ConstructEmptyValues (OutputCols $".length();
            String var = extractVarFromPos(out, startIdx);
            if (var.equals("input") && numbered != null) {
                out = out.replace("(ConstructEmptyValues (OutputCols $input)", "(ConstructEmptyValues (OutputCols $" + numbered + ")");
            }
        } 
        else if (out.equals("$input") && numbered != null) {out = "$" + numbered;}
        if (match.contains("HasZeroRows") && match.contains("SetPrivate") && match.contains("outCols") && out.contains("ConstructEmptyValues")) {
            java.util.regex.Pattern outColsPattern = java.util.regex.Pattern.compile("\\$outCols_(\\d+)");
            java.util.regex.Matcher outColsMatcher = outColsPattern.matcher(match);
            if (outColsMatcher.find()) {
                String outColsVar = "$outCols_" + outColsMatcher.group(1);
                int startIdx = out.indexOf("(ConstructEmptyValues (OutputCols $");
                if (startIdx >= 0) {
                    int varStart = startIdx + "(ConstructEmptyValues (OutputCols $".length();
                    int varEnd = varStart + extractVarFromPos(out, varStart).length();
                    out = out.substring(0, startIdx) + "(ConstructEmptyValues (ColListToSet " + outColsVar + "))" + out.substring(varEnd + 2);
                }
            }
        } 
        else if (match.contains("HasZeroRows") && match.contains("$left") && out.contains("ConstructEmptyValues")) {
            int leftIdx = match.indexOf("$left");
            if (leftIdx >= 0) {
                String leftVar = extractVarFromPos(match, leftIdx + 1);
                out = out.replaceAll("(OutputCols \\$)[a-zA-Z_][a-zA-Z0-9_]*", "$1" + leftVar);
            }
        }
        java.util.Map<String, String> varMap = extractNumberedVarMap(match);
        if (!varMap.isEmpty()) {
            var sorted = new java.util.ArrayList<>(varMap.entrySet());
            sorted.sort((a, b) -> Integer.compare(b.getKey().length(), a.getKey().length()));
            for (var e : sorted) {
                out = out.replaceAll("\\$" + java.util.regex.Pattern.quote(e.getKey()) + "(?![A-Za-z0-9_])", java.util.regex.Matcher.quoteReplacement(e.getValue()));
            }
        }
        if (match.contains("HasZeroRows")) {out = normalizeVars(out, new String[]{"projections", "passthrough", "filters"});}
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
    private static String extractVarFromPos(String str, int pos) {
        int end = pos;
        while (end < str.length() && (Character.isLetterOrDigit(str.charAt(end)) || str.charAt(end) == '_')) end++;
        return str.substring(pos, end);
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

    public record Env(AtomicInteger varId, String pattern, ImmutableMap<String, String> bindings, String currentVar, String rulename) {
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
        public Env bindVar(String key, String prefix) {
            String var = generateVar(prefix);
            return addBinding(key, var);
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