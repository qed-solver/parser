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
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexInputRef;

public class CalciteUtilities {
    public List<RexNode> compose(RelNode base, List<RexNode> inner, List<RexNode> outer) {
        var builder = RuleBuilder.create();
        return RelOptUtil.pushPastProject(outer, (Project) builder.push(base).project(inner).build());
    }

    public static org.apache.calcite.rex.RexNode mapFilterToProjectedColumns(RelOptRuleCall call) {
        var filter = (LogicalFilter) call.rel(1);
        var project = (LogicalProject) call.rel(0);
        var rexBuilder = project.getCluster().getRexBuilder();
        
        // Create mapping from table column index to projected position
        var tableToProjectMapping = new HashMap<Integer, Integer>();
        for (int projectedPos = 0; projectedPos < project.getProjects().size(); projectedPos++) {
            var projectExpr = project.getProjects().get(projectedPos);
            if (projectExpr instanceof RexInputRef inputRef) {
                tableToProjectMapping.put(inputRef.getIndex(), projectedPos);
            }
        }
        
        // Rewrite filter condition to use projected positions
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
        
        // Create mapping from input column index to aggregated position
        var inputToAggregateMapping = new HashMap<Integer, Integer>();
        
        // Group columns appear first in the aggregate output (positions 0..groupCount-1)
        int outputPos = 0;
        for (int groupCol : aggregate.getGroupSet()) {
            inputToAggregateMapping.put(groupCol, outputPos++);
        }
        
        // Note: Aggregate columns follow, but filter should only reference group columns
        // for the transpose to be valid
        
        // Rewrite filter condition to use aggregated positions
        return filter.getCondition().accept(new RexShuttle() {
            @Override
            public org.apache.calcite.rex.RexNode visitInputRef(RexInputRef inputRef) {
                Integer aggregatedPos = inputToAggregateMapping.get(inputRef.getIndex());
                if (aggregatedPos != null) {
                    return rexBuilder.makeInputRef(inputRef.getType(), aggregatedPos);
                }
                // If the filter references a non-group column, this transformation is invalid
                // This shouldn't happen if the rule's applicability is checked correctly
                throw new IllegalStateException(
                    "Filter references non-group column at index " + inputRef.getIndex() + 
                    " which cannot be pushed past aggregate");
            }
        });
    }

    public static org.apache.calcite.rex.RexNode pushFilterPastAggregate(RelOptRuleCall call) {
        // For FilterAggregateTranspose pattern: Filter(0) -> Aggregate(1) -> Input(2)
        var filter = (LogicalFilter) call.rel(0);     // Filter is at position 0
        var aggregate = (LogicalAggregate) call.rel(1); // Aggregate is at position 1
        var rexBuilder = aggregate.getCluster().getRexBuilder();
        
        // Create reverse mapping: from post-aggregate position to pre-aggregate column
        var aggregateToInputMapping = new HashMap<Integer, Integer>();
        
        // In the aggregate output, group columns come first (0..groupCount-1)
        // We need to map them back to their original positions in the input
        int outputPos = 0;
        for (int inputCol : aggregate.getGroupSet()) {
            // outputPos in aggregate result -> inputCol in original table
            aggregateToInputMapping.put(outputPos++, inputCol);
        }
        
        // Rewrite filter condition to use pre-aggregate positions
        return filter.getCondition().accept(new RexShuttle() {
            @Override
            public org.apache.calcite.rex.RexNode visitInputRef(RexInputRef inputRef) {
                Integer originalPos = aggregateToInputMapping.get(inputRef.getIndex());
                if (originalPos != null) {
                    // Map from post-aggregate position to original table position
                    return rexBuilder.makeInputRef(inputRef.getType(), originalPos);
                }
                // This shouldn't happen for valid FilterAggregateTranspose
                throw new IllegalStateException(
                    "Filter references non-group column at index " + inputRef.getIndex() + 
                    " which cannot be pushed past aggregate");
            }
        });
    }
    
    public static boolean canMergeAggregateProject(RelOptRuleCall call) {
        var aggregate = (LogicalAggregate) call.rel(0);
        var project = (LogicalProject) call.rel(1);
        
        // Find all fields that the aggregate uses (group keys + aggregate arguments)
        var interestingFields = org.apache.calcite.plan.RelOptUtil.getAllFields(aggregate);
        
        // Check if all interesting fields are simple field references in the project
        for (int fieldIndex : interestingFields) {
            var projectExpr = project.getProjects().get(fieldIndex);
            if (!(projectExpr instanceof RexInputRef)) {
                return false; // Not a simple field reference
            }
        }
        return true;
    }
    
    public static org.apache.calcite.tools.RelBuilder createMergedAggregateProject(RelOptRuleCall call) {
        var aggregate = (LogicalAggregate) call.rel(0);
        var project = (LogicalProject) call.rel(1);
        var builder = call.builder();
        
        // Find all fields that the aggregate uses
        var interestingFields = org.apache.calcite.plan.RelOptUtil.getAllFields(aggregate);
        
        // Build mapping from project output position to original table position
        var fieldMapping = new HashMap<Integer, Integer>();
        for (int fieldIndex : interestingFields) {
            var projectExpr = project.getProjects().get(fieldIndex);
            if (projectExpr instanceof RexInputRef inputRef) {
                fieldMapping.put(fieldIndex, inputRef.getIndex());
            }
        }
        
        // Create new aggregate directly on project's input
        builder.push(project.getInput());
        
        // Map group set to original field positions - this will be sorted by Calcite
        var newGroupSet = aggregate.getGroupSet().permute(fieldMapping);
        var groupKey = builder.groupKey(newGroupSet);
        
        // Map aggregate calls to original field positions
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
        
        // Create the aggregate (this will have sorted group keys)
        builder.aggregate(groupKey, mappedAggCalls);
        
        // Add projection to reorder fields to match original aggregate order
        // Original aggregate had group=[{0,1}] which meant (field0, field1, agg_results...)
        // New aggregate has group=[{0,7}] which means (field0, field7, agg_results...)
        // We need to reorder to (field7, field0, agg_results...) to match original order
        
        var originalGroupList = aggregate.getGroupSet().asList(); // [0, 1]
        var newGroupList = newGroupSet.asList(); // [0, 7] (sorted)
        
        // Create mapping from original position to new position
        var reorderingIndices = new java.util.ArrayList<Integer>();
        for (int originalFieldIndex : originalGroupList) {
            int mappedFieldIndex = fieldMapping.get(originalFieldIndex);
            int positionInNewAggregate = newGroupList.indexOf(mappedFieldIndex);
            reorderingIndices.add(positionInNewAggregate);
        }
        
        // Add aggregate result fields
        for (int i = aggregate.getGroupCount(); i < aggregate.getGroupCount() + aggregate.getAggCallList().size(); i++) {
            reorderingIndices.add(i);
        }
        
        // Apply the reordering projection
        builder.project(builder.fields(reorderingIndices));
        
        return builder;
    }

	public static org.apache.calcite.tools.RelBuilder mergeProjections(RelOptRuleCall call) {
        var outerProject = (LogicalProject) call.rel(0);
        var innerProject = (LogicalProject) call.rel(1);
        var source = call.rel(2);
        var builder = call.builder();
        
        // Push the source
        builder.push(source);
        
        // Compose the two projections
        // For each expression in the outer project, substitute references to inner project fields
        var composedExpressions = new java.util.ArrayList<org.apache.calcite.rex.RexNode>();
        var rexBuilder = builder.getRexBuilder();
        
        for (var outerExpr : outerProject.getProjects()) {
            // Substitute field references in outer projection with corresponding inner expressions
            var composedExpr = outerExpr.accept(new org.apache.calcite.rex.RexShuttle() {
                @Override
                public org.apache.calcite.rex.RexNode visitInputRef(org.apache.calcite.rex.RexInputRef inputRef) {
                    // Replace reference to inner project field with the actual inner expression
                    int fieldIndex = inputRef.getIndex();
                    if (fieldIndex < innerProject.getProjects().size()) {
                        return innerProject.getProjects().get(fieldIndex);
                    }
                    return inputRef;
                }
            });
            composedExpressions.add(composedExpr);
        }
        
        // Apply the composed projection
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
            // Adjust field indices for right table (subtract leftFieldCount)
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
        
        // Collect all fields used by the aggregate
        Set<Integer> usedFields = new HashSet<>();
        
        // 1. Add fields from groupSet
        for (int field : aggregate.getGroupSet()) {
            usedFields.add(field);
        }
        
        // 2. Add fields from aggregate calls
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            for (int field : aggCall.getArgList()) {
                usedFields.add(field);
            }
            if (aggCall.filterArg >= 0) {
                usedFields.add(aggCall.filterArg);
            }
        }
        
        // Create projection with only used fields (in order)
        List<Integer> sortedFields = new ArrayList<>(usedFields);
        Collections.sort(sortedFields);
        
        // Build mapping from old index to new index
        Map<Integer, Integer> fieldMapping = new HashMap<>();
        for (int i = 0; i < sortedFields.size(); i++) {
            fieldMapping.put(sortedFields.get(i), i);
        }
        
        // Create the projection
        builder.push(input);
        List<RexNode> projectedFields = new ArrayList<>();
        for (int field : sortedFields) {
            projectedFields.add(builder.field(field));
        }
        builder.project(projectedFields);
        
        // Adjust aggregate to use new field indices
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
                builder.peek(),  // The projected input
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
}