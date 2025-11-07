package org.qed.RRuleInstances;

import org.apache.calcite.rel.RelNode;
import org.qed.RelRN;
import org.qed.RRule;
import org.qed.RuleBuilder;
import org.qed.RelType;
import kala.collection.Seq;
import kala.tuple.Tuple;

public record ProjectAggregateMerge() implements RRule {
    static final RelRN source = new SourceTable();
    
    @Override
    public RelRN before() {
        var aggregateWithUnusedCalls = new AggregateWithMultipleCalls(source);
        return new ProjectUsingSubsetOfAggregates(aggregateWithUnusedCalls);
    }
    
    @Override
    public RelRN after() {
        var aggregateOptimized = new AggregateWithUsedCallsOnly(source);
        return new ProjectOptimized(aggregateOptimized);
    }

    public static record SourceTable() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            var table = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("Source_Type", true), false),
                Tuple.of(RelType.fromString("Source_Type", true), false),
                Tuple.of(RelType.fromString("Source_Type", true), false),
                Tuple.of(RelType.fromString("Source_Type", true), false)
            ));
            
            builder.addTable(table);
            return builder.scan(table.getName()).build();
        }
    }

    public static record AggregateWithMultipleCalls(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());

            var groupKey = builder.groupKey(builder.field(0));

            var agg1 = builder.sum(false, "agg1", builder.field(1));      // Will be used
            var agg2 = builder.avg(builder.field(2));                     // Will be unused
            var agg3 = builder.count(false, "agg3", builder.field(3));     // Will be used
            var agg4 = builder.max(builder.field(1));                      // Will be unused
            
            builder.aggregate(groupKey, agg1, agg2, agg3, agg4);
            return builder.build();
        }
    }

    public static record ProjectUsingSubsetOfAggregates(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());

            builder.project(
                builder.field(0),
                builder.field(1),
                builder.field(3)
            );
            
            return builder.build();
        }
    }
    
    /**
     * Optimized aggregate with only used calls
     * agg2 and agg4 are eliminated since they're not used
     */
    public static record AggregateWithUsedCallsOnly(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            var groupKey = builder.groupKey(builder.field(0));
            
            // Only the aggregate calls that are actually used
            var agg1 = builder.sum(false, "agg1", builder.field(1));   // Used
            var agg3 = builder.count(false, "agg3", builder.field(3)); // Used
            // agg2 and agg4 removed - they were unused
            
            builder.aggregate(groupKey, agg1, agg3);
            return builder.build();
        }
    }
    
    /**
     * Optimized project with adjusted field references
     */
    public static record ProjectOptimized(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            // After optimization, field layout is:
            // field(0) = group key
            // field(1) = agg1 (was field 1, still field 1)
            // field(2) = agg3 (was field 3, now field 2)
            builder.project(
                builder.field(0),
                builder.field(1),
                builder.field(2)
            );
            
            return builder.build();
        }
    }
}