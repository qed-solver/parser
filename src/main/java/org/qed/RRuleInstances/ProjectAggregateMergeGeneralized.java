package org.qed.RRuleInstances;

import kala.collection.Seq;
import org.apache.calcite.rel.RelNode;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RuleBuilder;

/**
 * Generalized version of ProjectAggregateMerge using abstract sources and aggregate calls.
 * 
 * This rule eliminates unused aggregate calls when a Project only uses
 * a subset of the aggregate's output fields.
 * 
 * Pattern: Project(selected fields) over Aggregate(all fields)
 * -> Aggregate(only used fields) + Project(adjusted field indices)
 * 
 * Unlike the concrete version, this uses:
 * - Abstract source (RelRN.scan) instead of concrete table
 * - Abstract aggregate calls instead of concrete SUM/AVG/COUNT/MAX
 * - Same projection pattern but with abstract field references
 */
public record ProjectAggregateMergeGeneralized() implements RRule {
    // Abstract source - no concrete schema
    static final RelRN source = RelRN.scan("Source", "Source_Type");
    
    // Group by expression
    static final RexRN.GroupBy groupExpr = source.groupBy("groupByName");
    
    // Multiple aggregate calls - some will be used, some unused
    static final RelRN.AggCall usedAgg1 = source.aggCall("usedAgg1");
    static final RelRN.AggCall unusedAgg = source.aggCall("unusedAgg");
    static final RelRN.AggCall usedAgg2 = source.aggCall("usedAgg2");
    
    @Override
    public RelRN before() {
        // Aggregate with multiple calls: [groupKey, usedAgg1, unusedAgg, usedAgg2]
        // Field layout: 0=groupKey, 1=usedAgg1, 2=unusedAgg, 3=usedAgg2
        RelRN aggregated = new RelRN.Aggregate(
            source, 
            Seq.of(groupExpr), 
            Seq.of(usedAgg1, unusedAgg, usedAgg2)
        );
        
        // Project that only uses fields 0 (group), 1 (usedAgg1), and 3 (usedAgg2)
        // Field 2 (unusedAgg) is skipped
        return new ProjectUsingSubsetOfAggregates(aggregated);
    }
    
    @Override
    public RelRN after() {
        // Optimized aggregate with only used calls: [groupKey, usedAgg1, usedAgg2]
        // Field layout: 0=groupKey, 1=usedAgg1, 2=usedAgg2
        RelRN optimizedAgg = new RelRN.Aggregate(
            source, 
            Seq.of(groupExpr), 
            Seq.of(usedAgg1, usedAgg2)
        );
        
        // Project with adjusted field indices:
        // field(0) = groupKey (was 0, still 0)
        // field(1) = usedAgg1 (was 1, still 1)
        // field(2) = usedAgg2 (was 3, now 2)
        return new ProjectOptimized(optimizedAgg);
    }

    /**
     * Project that selects a subset of aggregate output fields.
     * Uses fields 0 (group), 1 (usedAgg1), and 3 (usedAgg2), skipping field 2 (unusedAgg).
     */
    public static record ProjectUsingSubsetOfAggregates(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());

            // Project only the fields that are actually used
            builder.project(
                builder.alias(builder.field(0), "groupByOut"),
                builder.alias(builder.field(1), "usedAgg1Out"),
                builder.alias(builder.field(3), "usedAgg2Out")
            );
            
            return builder.build();
        }
    }
    
    /**
     * Optimized project with adjusted field references after removing unused aggregates.
     */
    public static record ProjectOptimized(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());

            // After optimization, field layout is:
            // field(0) = groupKey (was 0, still 0)
            // field(1) = usedAgg1 (was 1, still 1)
            // field(2) = usedAgg2 (was 3, now 2)
            builder.project(
                builder.alias(builder.field(0), "groupByOut"),
                builder.alias(builder.field(1), "usedAgg1Out"),
                builder.alias(builder.field(2), "usedAgg2Out")
            );
            
            return builder.build();
        }
    }
}

