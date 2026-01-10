package org.qed.Backends.Calcite;

import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;

import org.qed.RuleBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexInputRef;

public class HelperFunctions {
    public List<RexNode> compose(RelNode base, List<RexNode> inner, List<RexNode> outer) {
        var builder = RuleBuilder.create();
        return RelOptUtil.pushPastProject(outer, (Project) builder.push(base).project(inner).build());
    }

    public static org.apache.calcite.rex.RexNode mapFilterToProjectedColumns(RelOptRuleCall call) {
        var filter = (LogicalFilter) call.rel(1);
        var project = (LogicalProject) call.rel(0);
        var rexBuilder = project.getCluster().getRexBuilder();
        var tableToProjectMapping = new HashMap<Integer, Integer>();
        for (int projectedPos = 0; projectedPos < project.getProjects().size(); projectedPos++) {
            var projectExpr = project.getProjects().get(projectedPos);
            if (projectExpr instanceof RexInputRef inputRef) {
                tableToProjectMapping.put(inputRef.getIndex(), projectedPos);
            }
        }
        return filter.getCondition().accept(new RexShuttle() {
            @Override
            public org.apache.calcite.rex.RexNode visitInputRef(RexInputRef inputRef) {
                Integer projectedPos = tableToProjectMapping.get(inputRef.getIndex());
                if (projectedPos != null) {
                    return rexBuilder.makeInputRef(inputRef.getType(), projectedPos);
                }
                return inputRef;
            }
        });
    }

    public static org.apache.calcite.rex.RexNode mapFilterToAggregatedColumns(RelOptRuleCall call) {
        var filter = (LogicalFilter) call.rel(1);
        var aggregate = (LogicalAggregate) call.rel(0);
        var rexBuilder = aggregate.getCluster().getRexBuilder();
        var inputToAggregateMapping = new HashMap<Integer, Integer>();
        int outputPos = 0;
        for (int groupCol : aggregate.getGroupSet()) {
            inputToAggregateMapping.put(groupCol, outputPos++);
        }
        return filter.getCondition().accept(new RexShuttle() {
            @Override
            public org.apache.calcite.rex.RexNode visitInputRef(RexInputRef inputRef) {
                Integer aggregatedPos = inputToAggregateMapping.get(inputRef.getIndex());
                if (aggregatedPos != null) {
                    return rexBuilder.makeInputRef(inputRef.getType(), aggregatedPos);
                }
                throw new IllegalStateException(
                    "Filter references non-group column at index " + inputRef.getIndex() + 
                    " which cannot be pushed past aggregate");
            }
        });
    }

    public static org.apache.calcite.rex.RexNode pushFilterPastAggregate(RelOptRuleCall call) {
        var filter = (LogicalFilter) call.rel(0);
        var aggregate = (LogicalAggregate) call.rel(1);
        var rexBuilder = aggregate.getCluster().getRexBuilder();
        var aggregateToInputMapping = new HashMap<Integer, Integer>();
        int outputPos = 0;
        for (int inputCol : aggregate.getGroupSet()) {
            aggregateToInputMapping.put(outputPos++, inputCol);
        }
        return filter.getCondition().accept(new RexShuttle() {
            @Override
            public org.apache.calcite.rex.RexNode visitInputRef(RexInputRef inputRef) {
                Integer originalPos = aggregateToInputMapping.get(inputRef.getIndex());
                if (originalPos != null) {
                    return rexBuilder.makeInputRef(inputRef.getType(), originalPos);
                }
                throw new IllegalStateException(
                    "Filter references non-group column at index " + inputRef.getIndex() + 
                    " which cannot be pushed past aggregate");
            }
        });
    }

    public static boolean canMergeAggregateProject(RelOptRuleCall call) {
        var aggregate = (LogicalAggregate) call.rel(0);
        var project = (LogicalProject) call.rel(1);
        var interestingFields = org.apache.calcite.plan.RelOptUtil.getAllFields(aggregate);
        for (int fieldIndex : interestingFields) {
            var projectExpr = project.getProjects().get(fieldIndex);
            if (!(projectExpr instanceof RexInputRef)) {
                return false;
            }
        }
        return true;
    }

    public static org.apache.calcite.tools.RelBuilder createMergedAggregateProject(RelOptRuleCall call) {
        var aggregate = (LogicalAggregate) call.rel(0);
        var project = (LogicalProject) call.rel(1);
        var builder = call.builder();
        var interestingFields = org.apache.calcite.plan.RelOptUtil.getAllFields(aggregate);
        var fieldMapping = new HashMap<Integer, Integer>();
        for (int fieldIndex : interestingFields) {
            var projectExpr = project.getProjects().get(fieldIndex);
            if (projectExpr instanceof RexInputRef inputRef) {
                fieldMapping.put(fieldIndex, inputRef.getIndex());
            }
        }
        builder.push(project.getInput());
        var newGroupSet = aggregate.getGroupSet().permute(fieldMapping);
        var groupKey = builder.groupKey(newGroupSet);
        var mappedAggCalls = new java.util.ArrayList<org.apache.calcite.rel.core.AggregateCall>();
        var sourceCount = aggregate.getInput().getRowType().getFieldCount();
        var targetCount = project.getInput().getRowType().getFieldCount();
        var targetMapping = org.apache.calcite.util.mapping.Mappings.target(
            fieldMapping, 
            sourceCount,
            targetCount
        );

        for (var aggCall : aggregate.getAggCallList()) {
            mappedAggCalls.add(aggCall.transform(targetMapping));
        }
        builder.aggregate(groupKey, mappedAggCalls);

        var originalGroupList = aggregate.getGroupSet().asList();
        var newGroupList = newGroupSet.asList();
        var reorderingIndices = new java.util.ArrayList<Integer>();
        for (int originalFieldIndex : originalGroupList) {
            int mappedFieldIndex = fieldMapping.get(originalFieldIndex);
            int positionInNewAggregate = newGroupList.indexOf(mappedFieldIndex);
            reorderingIndices.add(positionInNewAggregate);
        }
        for (int i = aggregate.getGroupCount(); i < aggregate.getGroupCount() + aggregate.getAggCallList().size(); i++) {
            reorderingIndices.add(i);
        }
        builder.project(builder.fields(reorderingIndices));

        return builder;
    }

	public static org.apache.calcite.tools.RelBuilder mergeProjections(RelOptRuleCall call) {
        var outerProject = (LogicalProject) call.rel(0);
        var innerProject = (LogicalProject) call.rel(1);
        var source = call.rel(2);
        var builder = call.builder();
        builder.push(source);
        var composedExpressions = new java.util.ArrayList<org.apache.calcite.rex.RexNode>();
        var rexBuilder = builder.getRexBuilder();

        for (var outerExpr : outerProject.getProjects()) {
            var composedExpr = outerExpr.accept(new org.apache.calcite.rex.RexShuttle() {
                @Override
                public org.apache.calcite.rex.RexNode visitInputRef(org.apache.calcite.rex.RexInputRef inputRef) {
                    int fieldIndex = inputRef.getIndex();
                    if (fieldIndex < innerProject.getProjects().size()) {
                        return innerProject.getProjects().get(fieldIndex);
                    }
                    return inputRef;
                }
            });
            composedExpressions.add(composedExpr);
        }
        builder.project(composedExpressions);

        return builder;
    }

    public static class ConditionDecomposer {
        public static RexNode extractLeftOnlyConditions(RexNode condition, int leftFieldCount, RelOptRuleCall call) {
            List<RexNode> leftConditions = new ArrayList<>();
            extractConditionsForSide(condition, leftConditions, 0, leftFieldCount - 1);
            if (leftConditions.isEmpty()) return null;
            if (leftConditions.size() == 1) return leftConditions.get(0);
            return RexUtil.composeConjunction(call.builder().getRexBuilder(), leftConditions);
        }

        public static RexNode extractRightOnlyConditions(RexNode condition, int leftFieldCount, int totalFieldCount, RelOptRuleCall call) {
            List<RexNode> rightConditions = new ArrayList<>();
            extractConditionsForSide(condition, rightConditions, leftFieldCount, totalFieldCount - 1);
            if (rightConditions.isEmpty()) return null;
            org.apache.calcite.rex.RexBuilder rexBuilder = call.builder().getRexBuilder();
            List<RexNode> adjustedConditions = new ArrayList<>();
            for (RexNode cond : rightConditions) {
                adjustedConditions.add(adjustFieldIndices(cond, -leftFieldCount, rexBuilder));
            }
            if (adjustedConditions.size() == 1) return adjustedConditions.get(0);
            return RexUtil.composeConjunction(rexBuilder, adjustedConditions);
        }

        public static RexNode extractJoinConditions(RexNode condition, int leftFieldCount, int totalFieldCount, RelOptRuleCall call) {
            List<RexNode> joinConditions = new ArrayList<>();
            extractCrossTableConditions(condition, joinConditions, leftFieldCount, totalFieldCount);
            if (joinConditions.isEmpty()) return null;
            if (joinConditions.size() == 1) return joinConditions.get(0);
            return RexUtil.composeConjunction(call.builder().getRexBuilder(), joinConditions);
        }

        private static void extractConditionsForSide(RexNode condition, List<RexNode> result, int minField, int maxField) {
            if (condition instanceof RexCall call && call.getOperator().getKind() == org.apache.calcite.sql.SqlKind.AND) {
                for (RexNode operand : call.getOperands()) {
                    extractConditionsForSide(operand, result, minField, maxField);
                }
            } else if (referencesOnlyFields(condition, minField, maxField)) {
                result.add(condition);
            }
        }

        private static void extractCrossTableConditions(RexNode condition, List<RexNode> result, int leftFieldCount, int totalFieldCount) {
            if (condition instanceof RexCall call && call.getOperator().getKind() == org.apache.calcite.sql.SqlKind.AND) {
                for (RexNode operand : call.getOperands()) {
                    extractCrossTableConditions(operand, result, leftFieldCount, totalFieldCount);
                }
            } else if (referencesBothSides(condition, leftFieldCount, totalFieldCount)) {
                result.add(condition);
            }
        }

        private static boolean referencesOnlyFields(RexNode condition, int minField, int maxField) {
            Set<Integer> fields = new HashSet<>();
            collectFieldReferences(condition, fields);
            return !fields.isEmpty() && fields.stream().allMatch(f -> f >= minField && f <= maxField);
        }

        private static boolean referencesBothSides(RexNode condition, int leftFieldCount, int totalFieldCount) {
            Set<Integer> fields = new HashSet<>();
            collectFieldReferences(condition, fields);
            boolean hasLeft = fields.stream().anyMatch(f -> f < leftFieldCount);
            boolean hasRight = fields.stream().anyMatch(f -> f >= leftFieldCount && f < totalFieldCount);
            return hasLeft && hasRight;
        }

        private static void collectFieldReferences(RexNode node, Set<Integer> fields) {
            if (node instanceof RexInputRef inputRef) {
                fields.add(inputRef.getIndex());
            } else if (node instanceof RexCall call) {
                for (RexNode operand : call.getOperands()) {
                    collectFieldReferences(operand, fields);
                }
            }
        }

        private static RexNode adjustFieldIndices(RexNode node, int offset, org.apache.calcite.rex.RexBuilder rexBuilder) {
            if (node instanceof RexInputRef inputRef) {
                return rexBuilder.makeInputRef(inputRef.getType(), inputRef.getIndex() + offset);
            } else if (node instanceof RexCall call) {
                List<RexNode> newOperands = new ArrayList<>();
                for (RexNode operand : call.getOperands()) {
                    newOperands.add(adjustFieldIndices(operand, offset, rexBuilder));
                }
                return rexBuilder.makeCall(call.getOperator(), newOperands);
            }
            return node;
        }
    }

    public static org.apache.calcite.tools.RelBuilder extractProjectForAggregate(RelOptRuleCall call) {
        var builder = call.builder();
        LogicalAggregate aggregate = (LogicalAggregate) call.rel(0);
        RelNode input = call.rel(1);
        Set<Integer> usedFields = new HashSet<>();
        for (int field : aggregate.getGroupSet()) {
            usedFields.add(field);
        }
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            for (int field : aggCall.getArgList()) {
                usedFields.add(field);
            }
            if (aggCall.filterArg >= 0) {
                usedFields.add(aggCall.filterArg);
            }
        }
        List<Integer> sortedFields = new ArrayList<>(usedFields);
        Collections.sort(sortedFields);
        Map<Integer, Integer> fieldMapping = new HashMap<>();
        for (int i = 0; i < sortedFields.size(); i++) {
            fieldMapping.put(sortedFields.get(i), i);
        }
        builder.push(input);
        List<RexNode> projectedFields = new ArrayList<>();
        for (int field : sortedFields) {
            projectedFields.add(builder.field(field));
        }
        builder.project(projectedFields);
        ImmutableBitSet.Builder newGroupSet = ImmutableBitSet.builder();
        for (int field : aggregate.getGroupSet()) {
            newGroupSet.set(fieldMapping.get(field));
        }

        List<AggregateCall> newAggCalls = new ArrayList<>();
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            List<Integer> newArgList = new ArrayList<>();
            for (int field : aggCall.getArgList()) {
                newArgList.add(fieldMapping.get(field));
            }
            int newFilterArg = aggCall.filterArg >= 0 ? fieldMapping.get(aggCall.filterArg) : -1;

            newAggCalls.add(aggCall.adaptTo(
                builder.peek(),
                newArgList,
                newFilterArg,
                aggregate.getGroupCount(),
                aggregate.getGroupCount()
            ));
        }

        builder.aggregate(
            builder.groupKey(newGroupSet.build()),
            newAggCalls
        );

        return builder;
    }

    public static org.apache.calcite.tools.RelBuilder aggregateJoinJoinRemove(RelOptRuleCall call) {
        var builder = call.builder();
        var originalAgg = (LogicalAggregate) call.rel(0);
        var groupSet = originalAgg.getGroupSet();
        var aggCalls = originalAgg.getAggCallList();
        
        // 计算tblA和tblB的字段数量
        int tblAFieldCount = call.rel(3).getRowType().getFieldCount();
        int tblBFieldCount = call.rel(6).getRowType().getFieldCount();
        
        // 重新映射字段索引：移除tblB的字段
        // Before: tblA[0..tblAFieldCount-1], tblB[tblAFieldCount..tblAFieldCount+tblBFieldCount-1], tblC[tblAFieldCount+tblBFieldCount..]
        // After: tblA[0..tblAFieldCount-1], tblC[tblAFieldCount..]
        ImmutableBitSet.Builder newGroupSetBuilder = ImmutableBitSet.builder();
        for (int field : groupSet) {
            if (field < tblAFieldCount) {
                // 来自tblA，索引不变
                newGroupSetBuilder.set(field);
            } else if (field >= tblAFieldCount + tblBFieldCount) {
                // 来自tblC，减去tblB的字段数
                newGroupSetBuilder.set(field - tblBFieldCount);
            }
            // 如果field在tblB范围内，跳过（不应该发生，因为规则要求aggregate不使用tblB）
        }
        
        return builder
            .push(call.rel(4))  // scanA1
            .push(call.rel(5))  // scanA2
            .join(JoinRelType.INNER, builder.literal(true))
            .push(call.rel(10))  // scanC1
            .push(call.rel(11))  // scanC2
            .join(JoinRelType.INNER, builder.literal(true))
            .join(JoinRelType.LEFT, builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0)))
            .aggregate(builder.groupKey(newGroupSetBuilder.build()), aggCalls);
    }

    public static org.apache.calcite.tools.RelBuilder aggregateProjectConstantToDummyJoin(RelOptRuleCall call) {
        final LogicalAggregate aggregate = call.rel(0);
        final LogicalProject project = call.rel(1);

        RelBuilder builder = call.builder();
        org.apache.calcite.rex.RexBuilder rexBuilder = builder.getRexBuilder();

        builder.push(project.getInput());
        int offset = project.getInput().getRowType().getFieldCount();

        org.apache.calcite.rel.type.RelDataTypeFactory.Builder valuesType = 
            rexBuilder.getTypeFactory().builder();
        java.util.List<org.apache.calcite.rex.RexLiteral> literals = new java.util.ArrayList<>();
        java.util.List<org.apache.calcite.rex.RexNode> projects = project.getProjects();
        
        int colIndex = 0;
        for (int i = 0; i < projects.size(); i++) {
            org.apache.calcite.rex.RexNode node = projects.get(i);
            if (node instanceof org.apache.calcite.rex.RexLiteral) {
                literals.add((org.apache.calcite.rex.RexLiteral) node);
                // 使用统一的命名 "col0", "col1", ...
                valuesType.add("col" + colIndex++, node.getType());
            }
        }
        
        builder.values(com.google.common.collect.ImmutableList.of(literals), valuesType.build());
        builder.join(org.apache.calcite.rel.core.JoinRelType.INNER, rexBuilder.makeLiteral(true));

        java.util.List<org.apache.calcite.rex.RexNode> newProjects = new java.util.ArrayList<>();
        int literalCounter = 0;
        for (org.apache.calcite.rex.RexNode exp : project.getProjects()) {
            if (exp instanceof org.apache.calcite.rex.RexLiteral) {
                newProjects.add(builder.field(offset + literalCounter++));
            } else {
                newProjects.add(exp);
            }
        }

        builder.project(newProjects);
        builder.aggregate(
            builder.groupKey(aggregate.getGroupSet(), aggregate.getGroupSets()),
            aggregate.getAggCallList());

        return builder;
    }

    public static org.apache.calcite.tools.RelBuilder unionToDistinct(RelOptRuleCall call) {
        final LogicalUnion union = call.rel(0);
        final RelBuilder relBuilder = call.builder();
        relBuilder.pushAll(union.getInputs());
        relBuilder.union(true, union.getInputs().size());
        relBuilder.distinct();
        return relBuilder;
    }

    public static org.apache.calcite.tools.RelBuilder unionPullUpConstants(RelOptRuleCall call) {
        final LogicalUnion union = call.rel(0);
        final org.apache.calcite.rex.RexBuilder rexBuilder = union.getCluster().getRexBuilder();
        final org.apache.calcite.rel.metadata.RelMetadataQuery mq = call.getMetadataQuery();
        final org.apache.calcite.plan.RelOptPredicateList predicates = mq.getPulledUpPredicates(union);
        
        if (org.apache.calcite.plan.RelOptPredicateList.isEmpty(predicates)) {
            return call.builder().push(union);
        }

        final java.util.Map<Integer, org.apache.calcite.rex.RexNode> constants = new java.util.HashMap<>();
        for (java.util.Map.Entry<org.apache.calcite.rex.RexNode, org.apache.calcite.rex.RexNode> e : predicates.constantMap.entrySet()) {
            if (e.getKey() instanceof org.apache.calcite.rex.RexInputRef) {
                constants.put(((org.apache.calcite.rex.RexInputRef) e.getKey()).getIndex(), e.getValue());
            }
        }

        if (constants.isEmpty()) {
            return call.builder().push(union);
        }

        java.util.List<org.apache.calcite.rel.type.RelDataTypeField> fields = union.getRowType().getFieldList();
        java.util.List<org.apache.calcite.rex.RexNode> topChildExprs = new java.util.ArrayList<>();
        java.util.List<String> topChildExprsFields = new java.util.ArrayList<>();
        java.util.List<org.apache.calcite.rex.RexNode> refs = new java.util.ArrayList<>();
        org.apache.calcite.util.ImmutableBitSet.Builder refsIndexBuilder = org.apache.calcite.util.ImmutableBitSet.builder();
        
        for (org.apache.calcite.rel.type.RelDataTypeField field : fields) {
            final org.apache.calcite.rex.RexNode constant = constants.get(field.getIndex());
            if (constant != null) {
                if (constant.getType().equals(field.getType())) {
                    topChildExprs.add(constant);
                } else {
                    topChildExprs.add(rexBuilder.makeCast(field.getType(), constant, true, false));
                }
                topChildExprsFields.add(field.getName());
            } else {
                final org.apache.calcite.rex.RexNode expr = rexBuilder.makeInputRef(union, field.getIndex());
                topChildExprs.add(expr);
                topChildExprsFields.add(field.getName());
                refs.add(expr);
                refsIndexBuilder.set(field.getIndex());
            }
        }
        org.apache.calcite.util.ImmutableBitSet refsIndex = refsIndexBuilder.build();

        final org.apache.calcite.util.mapping.Mappings.TargetMapping mapping =
            org.apache.calcite.plan.RelOptUtil.permutation(refs, union.getInput(0).getRowType()).inverse();
        topChildExprs = org.apache.calcite.rex.RexUtil.apply(mapping, topChildExprs);

        final RelBuilder relBuilder = call.builder();
        for (org.apache.calcite.rel.RelNode input : union.getInputs()) {
            java.util.List<org.apache.calcite.util.Pair<org.apache.calcite.rex.RexNode, String>> newChildExprs = 
                new java.util.ArrayList<>();
            for (int j : refsIndex) {
                newChildExprs.add(
                    org.apache.calcite.util.Pair.of(
                        rexBuilder.makeInputRef(input, j),
                        input.getRowType().getFieldList().get(j).getName()));
            }
            if (newChildExprs.isEmpty()) {
                // At least a single item in project is required.
                newChildExprs.add(
                    org.apache.calcite.util.Pair.of(topChildExprs.get(0), topChildExprsFields.get(0)));
            }
            // Add the input with project on top
            relBuilder.push(input);
            relBuilder.project(
                org.apache.calcite.util.Pair.left(newChildExprs), 
                org.apache.calcite.util.Pair.right(newChildExprs));
        }
        relBuilder.union(union.all, union.getInputs().size());
        // Create top Project fixing nullability of fields
        relBuilder.project(topChildExprs, topChildExprsFields);
        relBuilder.convert(union.getRowType(), false);

        return relBuilder;
    }

    public static org.apache.calcite.tools.RelBuilder projectAggregateMerge(RelOptRuleCall call) {
        final LogicalProject project = call.rel(0);
        final LogicalAggregate aggregate = call.rel(1);
        final org.apache.calcite.plan.RelOptCluster cluster = aggregate.getCluster();

        // Do a quick check. If all aggregate calls are used, and there are no CASE
        // expressions, there is nothing to do.
        final org.apache.calcite.util.ImmutableBitSet bits =
            org.apache.calcite.plan.RelOptUtil.InputFinder.bits(project.getProjects(), null);
        if (bits.contains(
            org.apache.calcite.util.ImmutableBitSet.range(aggregate.getGroupCount(),
                aggregate.getRowType().getFieldCount()))
            && kindCount(project.getProjects(), org.apache.calcite.sql.SqlKind.CASE) == 0) {
            return null;
        }

        // Replace 'COALESCE(SUM(x), 0)' with 'SUM0(x)' wherever it occurs.
        // Add 'SUM0(x)' to the aggregate call list, if necessary.
        final java.util.List<org.apache.calcite.rel.core.AggregateCall> aggCallList =
            new java.util.ArrayList<>(aggregate.getAggCallList());
        
        final org.apache.calcite.rex.RexShuttle shuttle = new org.apache.calcite.rex.RexShuttle() {
            @Override public org.apache.calcite.rex.RexNode visitCall(org.apache.calcite.rex.RexCall call) {
                switch (call.getKind()) {
                case CASE:
                    // Do we have "CASE(IS NOT NULL($0), CAST($0):INTEGER NOT NULL, 0)"?
                    final java.util.List<org.apache.calcite.rex.RexNode> operands = call.operands;
                    if (operands.size() == 3
                        && operands.get(0).getKind() == org.apache.calcite.sql.SqlKind.IS_NOT_NULL
                        && ((org.apache.calcite.rex.RexCall) operands.get(0)).operands.get(0).getKind()
                        == org.apache.calcite.sql.SqlKind.INPUT_REF
                        && operands.get(1).getKind() == org.apache.calcite.sql.SqlKind.CAST
                        && ((org.apache.calcite.rex.RexCall) operands.get(1)).operands.get(0).getKind()
                        == org.apache.calcite.sql.SqlKind.INPUT_REF
                        && operands.get(2).getKind() == org.apache.calcite.sql.SqlKind.LITERAL) {
                        final org.apache.calcite.rex.RexCall isNotNull = (org.apache.calcite.rex.RexCall) operands.get(0);
                        final org.apache.calcite.rex.RexInputRef ref0 = (org.apache.calcite.rex.RexInputRef) isNotNull.operands.get(0);
                        final org.apache.calcite.rex.RexCall cast = (org.apache.calcite.rex.RexCall) operands.get(1);
                        final org.apache.calcite.rex.RexInputRef ref1 = (org.apache.calcite.rex.RexInputRef) cast.operands.get(0);
                        if (ref0.getIndex() != ref1.getIndex()) {
                            break;
                        }
                        final int aggCallIndex = ref1.getIndex() - aggregate.getGroupCount();
                        if (aggCallIndex < 0) {
                            break;
                        }
                        final org.apache.calcite.rel.core.AggregateCall aggCall = aggregate.getAggCallList().get(aggCallIndex);
                        if (aggCall.getAggregation().getKind() != org.apache.calcite.sql.SqlKind.SUM) {
                            break;
                        }
                        final org.apache.calcite.rex.RexLiteral literal = (org.apache.calcite.rex.RexLiteral) operands.get(2);
                        if (java.util.Objects.equals(literal.getValueAs(java.math.BigDecimal.class), java.math.BigDecimal.ZERO)) {
                            int j = findSum0(cluster.getTypeFactory(), aggCall, aggCallList);
                            return cluster.getRexBuilder().makeInputRef(aggCallList.get(j).getType(), j);
                        }
                    }
                    break;
                default:
                    break;
                }
                return super.visitCall(call);
            }
        };
        
        final java.util.List<org.apache.calcite.rex.RexNode> projects2 = shuttle.visitList(project.getProjects());
        final org.apache.calcite.util.ImmutableBitSet bits2 =
            org.apache.calcite.plan.RelOptUtil.InputFinder.bits(projects2, null);

        // Build the mapping that we will apply to the project expressions.
        final org.apache.calcite.util.mapping.Mappings.TargetMapping mapping =
            org.apache.calcite.util.mapping.Mappings.create(
                org.apache.calcite.util.mapping.MappingType.FUNCTION,
                aggregate.getGroupCount() + aggCallList.size(), -1);
        int j = 0;
        for (int i = 0; i < mapping.getSourceCount(); i++) {
            if (i < aggregate.getGroupCount()) {
                // Field is a group key. All group keys are retained.
                mapping.set(i, j++);
            } else if (bits2.get(i)) {
                // Field is an aggregate call. It is used.
                mapping.set(i, j++);
            } else {
                // Field is an aggregate call. It is not used. Remove it.
                aggCallList.remove(j - aggregate.getGroupCount());
            }
        }

        final RelBuilder builder = call.builder();
        builder.push(aggregate.getInput());
        builder.aggregate(
            builder.groupKey(aggregate.getGroupSet(), aggregate.groupSets), aggCallList);
        builder.project(
            org.apache.calcite.rex.RexPermuteInputsShuttle.of(mapping).visitList(projects2),
            project.getRowType().getFieldNames());
        builder.convert(project.getRowType(), true);
        
        return builder;
    }

    private static int findSum0(
        org.apache.calcite.rel.type.RelDataTypeFactory typeFactory, 
        org.apache.calcite.rel.core.AggregateCall sum,
        java.util.List<org.apache.calcite.rel.core.AggregateCall> aggCallList) {
        
        final org.apache.calcite.rel.core.AggregateCall sum0 =
            org.apache.calcite.rel.core.AggregateCall.create(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM0, 
                sum.isDistinct(),
                sum.isApproximate(),
                false,                 // ignoreNulls
                sum.getArgList(),      // List<Integer>
                sum.filterArg,         // int
                sum.collation,         // RelCollation
                typeFactory.createTypeWithNullability(sum.type, false),  // RelDataType
                null);                 // String name
        
        final int i = aggCallList.indexOf(sum0);
        if (i >= 0) {
            return i;
        }
        aggCallList.add(sum0);
        return aggCallList.size() - 1;
    }

    private static int kindCount(
        Iterable<? extends org.apache.calcite.rex.RexNode> nodes,
        final org.apache.calcite.sql.SqlKind kind) {
        
        final java.util.concurrent.atomic.AtomicInteger kindCount = 
            new java.util.concurrent.atomic.AtomicInteger(0);
        
        new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
            @Override public Void visitCall(org.apache.calcite.rex.RexCall call) {
                if (call.getKind() == kind) {
                    kindCount.incrementAndGet();
                }
                return super.visitCall(call);
            }
        }.visitEach(nodes);
        
        return kindCount.get();
    }
}