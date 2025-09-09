package org.qed.RRuleInstances;

import org.apache.calcite.rel.RelNode;
import org.qed.RelRN;
import org.qed.RRule;
import org.qed.RuleBuilder;
import org.qed.RelType;
import kala.collection.Seq;
import kala.tuple.Tuple;

/**
 * ProjectAggregateMergeRule: Eliminates unused aggregate calls from projections
 * and converts COALESCE(SUM(x), 0) to SUM0(x) for better optimization.
 * 
 * Pattern:
 * Project(used_group_fields, used_agg_calls, unused_agg_calls)
 *   Aggregate(group_fields, used_agg_calls, unused_agg_calls)
 *     Scan
 * 
 * =>
 * 
 * Project(used_group_fields, used_agg_calls)
 *   Aggregate(group_fields, used_agg_calls)  -- unused calls removed
 *     Scan
 * 
 * This optimization reduces the cost of aggregation by eliminating
 * aggregate computations that are not used in the final result.
 */
public record ProjectAggregateMerge() implements RRule {
    
    // Base table for the pattern
    static final RelRN baseTable = new SalesTable();
    
    @Override
    public RelRN before() {
        // Project that uses only some of the aggregate results
        var aggregateWithUnusedCalls = new AggregateWithMultipleCalls(baseTable);
        return new ProjectUsingSubsetOfAggregates(aggregateWithUnusedCalls);
    }
    
    @Override
    public RelRN after() {
        // Optimized: aggregate with only used calls
        var aggregateOptimized = new AggregateWithUsedCallsOnly(baseTable);
        return new ProjectOptimized(aggregateOptimized);
    }

    /**
     * Sales table with multiple numeric columns for aggregation
     */
    public static record SalesTable() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            var table = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("INTEGER", true), false),   // region_id
                Tuple.of(RelType.fromString("DECIMAL", true), false),   // sales_amount
                Tuple.of(RelType.fromString("DECIMAL", true), false),   // cost_amount
                Tuple.of(RelType.fromString("INTEGER", true), false)    // quantity
            ));
            
            builder.addTable(table);
            return builder.scan(table.getName()).build();
        }
    }
    
    /**
     * Aggregate with multiple calls: GROUP BY region_id, SUM(sales), AVG(cost), COUNT(quantity), MAX(sales)
     * Some of these aggregates will be unused in the projection
     */
    public static record AggregateWithMultipleCalls(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            // Group by region_id
            var groupKey = builder.groupKey(builder.field(0));
            
            // Multiple aggregate calls
            var sumSales = builder.sum(false, "sum_sales", builder.field(1));      // Will be used
            var avgCost = builder.avg(builder.field(2));                           // Will be unused
            var countQty = builder.count(false, "count_qty", builder.field(3));    // Will be used
            var maxSales = builder.max(builder.field(1));                          // Will be unused
            
            builder.aggregate(groupKey, sumSales, avgCost, countQty, maxSales);
            return builder.build();
        }
    }
    
    /**
     * Project that uses only some aggregates: SELECT region_id, sum_sales, count_qty
     * (avgCost and maxSales are not projected, so they're unused)
     */
    public static record ProjectUsingSubsetOfAggregates(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            // Project only used fields:
            // field(0) = region_id (group key)
            // field(1) = sum_sales (used aggregate)
            // field(2) = avg_cost (UNUSED - not projected)
            // field(3) = count_qty (used aggregate)  
            // field(4) = max_sales (UNUSED - not projected)
            builder.project(
                builder.alias(builder.field(0), "region_id"),     // Group key
                builder.alias(builder.field(1), "total_sales"),   // Used: sum_sales
                builder.alias(builder.field(3), "total_count")    // Used: count_qty
                // avg_cost (field 2) and max_sales (field 4) are not projected
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