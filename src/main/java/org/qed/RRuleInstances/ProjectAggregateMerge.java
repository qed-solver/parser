package org.qed.RRuleInstances;

import org.apache.calcite.rel.RelNode;
import org.qed.RelRN;
import org.qed.RRule;
import org.qed.RuleBuilder;
import org.qed.RelType;
import kala.collection.Seq;
import kala.tuple.Tuple;

public record ProjectAggregateMerge() implements RRule {
    static final RelRN baseTable = new SalesTable();
    
    @Override
    public RelRN before() {
        var aggregateWithUnusedCalls = new AggregateWithMultipleCalls(baseTable);
        return new ProjectUsingSubsetOfAggregates(aggregateWithUnusedCalls);
    }
    
    @Override
    public RelRN after() {
        var aggregateOptimized = new AggregateWithUsedCallsOnly(baseTable);
        return new ProjectOptimized(aggregateOptimized);
    }

    public static record SalesTable() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            var table = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("INTEGER", true), false),
                Tuple.of(RelType.fromString("DECIMAL", true), false),
                Tuple.of(RelType.fromString("DECIMAL", true), false),
                Tuple.of(RelType.fromString("INTEGER", true), false)
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

            var sumSales = builder.sum(false, "sum_sales", builder.field(1));      // Will be used
            var avgCost = builder.avg(builder.field(2));                           // Will be unused
            var countQty = builder.count(false, "count_qty", builder.field(3));    // Will be used
            var maxSales = builder.max(builder.field(1));                          // Will be unused
            
            builder.aggregate(groupKey, sumSales, avgCost, countQty, maxSales);
            return builder.build();
        }
    }

    public static record ProjectUsingSubsetOfAggregates(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());

            builder.project(
                builder.alias(builder.field(0), "region_id"),
                builder.alias(builder.field(1), "total_sales"),
                builder.alias(builder.field(3), "total_count")
            );
            
            return builder.build();
        }
    }
    
    /**
     * Optimized aggregate with only used calls: GROUP BY region_id, SUM(sales), COUNT(quantity)
     * avgCost and maxSales are eliminated since they're not used
     */
    public static record AggregateWithUsedCallsOnly(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            // Same group key
            var groupKey = builder.groupKey(builder.field(0));
            
            // Only the aggregate calls that are actually used
            var sumSales = builder.sum(false, "sum_sales", builder.field(1));   // Used
            var countQty = builder.count(false, "count_qty", builder.field(3)); // Used
            // avgCost and maxSales removed - they were unused
            
            builder.aggregate(groupKey, sumSales, countQty);
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
            // field(0) = region_id (group key)
            // field(1) = sum_sales (was field 1, still field 1)
            // field(2) = count_qty (was field 3, now field 2)
            builder.project(
                builder.alias(builder.field(0), "region_id"),     // Group key
                builder.alias(builder.field(1), "total_sales"),   // sum_sales
                builder.alias(builder.field(2), "total_count")    // count_qty (field index adjusted)
            );
            
            return builder.build();
        }
    }
}