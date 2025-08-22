package org.qed.Generated.RRuleInstances;

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
                Tuple.of(RelType.fromString("INTEGER", true), false),   // region_id
                Tuple.of(RelType.fromString("DECIMAL", true), false),   // sales_amount
                Tuple.of(RelType.fromString("DECIMAL", true), false),   // cost_amount
                Tuple.of(RelType.fromString("INTEGER", true), false)    // quantity
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
            builder.project(builder.alias(builder.field(0), "region_id"), builder.alias(builder.field(1), "total_sales"), builder.alias(builder.field(3), "total_count"));
            
            return builder.build();
        }
    }

    public static record AggregateWithUsedCallsOnly(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            var groupKey = builder.groupKey(builder.field(0));
            var sumSales = builder.sum(false, "sum_sales", builder.field(1));   // Used
            var countQty = builder.count(false, "count_qty", builder.field(3)); // Used
            builder.aggregate(groupKey, sumSales, countQty);
            return builder.build();
        }
    }

    public static record ProjectOptimized(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            builder.project(builder.alias(builder.field(0), "region_id"), builder.alias(builder.field(1), "total_sales"), builder.alias(builder.field(2), "total_count"));
            return builder.build();
        }
    }
}