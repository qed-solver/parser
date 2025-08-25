package org.qed.Generated.RRuleInstances;

import org.apache.calcite.rel.RelNode;
import org.qed.RelRN;
import org.qed.RRule;
import org.qed.RuleBuilder;
import org.qed.RelType;
import kala.collection.Seq;
import kala.tuple.Tuple;

/**
 * UnionToDistinctRule: Transforms UNION DISTINCT into UNION ALL + DISTINCT aggregate
 * 
 * Pattern: Union(all=false, inputs...) => Aggregate(DISTINCT group by all fields)(Union(all=true, inputs...))
 * 
 * This optimization can be beneficial when the underlying system can handle
 * UNION ALL more efficiently than UNION DISTINCT.
 */
public record UnionToDistinct() implements RRule {
    
    // Base tables for the union
    static final RelRN leftTable = new LeftSourceTable();
    static final RelRN rightTable = new RightSourceTable();
    
    @Override
    public RelRN before() {
        // UNION DISTINCT (all=false)
        return new DistinctUnion(leftTable, rightTable);
    }
    
    @Override
    public RelRN after() {
        // UNION ALL + DISTINCT aggregate
        var unionAll = new UnionAll(leftTable, rightTable);
        return new DistinctAggregate(unionAll);
    }

    /**
     * Left source table
     */
    public static record LeftSourceTable() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            var table = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("INTEGER", true), false),   // col0
                Tuple.of(RelType.fromString("VARCHAR", true), false)    // col1
            ));
            
            builder.addTable(table);
            return builder.scan(table.getName()).build();
        }
    }
    
    /**
     * Right source table (same schema as left for UNION compatibility)
     */
    public static record RightSourceTable() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            var table = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("INTEGER", true), false),   // col0
                Tuple.of(RelType.fromString("VARCHAR", true), false)    // col1
            ));
            
            builder.addTable(table);
            return builder.scan(table.getName()).build();
        }
    }
    
    /**
     * UNION DISTINCT operation (all=false)
     */
    public static record DistinctUnion(RelRN left, RelRN right) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            // Push both inputs
            builder.push(left.semantics());
            builder.push(right.semantics());
            
            // Create UNION with all=false (DISTINCT)
            builder.union(false, 2);
            
            return builder.build();
        }
    }
    
    /**
     * UNION ALL operation (all=true)
     */
    public static record UnionAll(RelRN left, RelRN right) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            // Push both inputs
            builder.push(left.semantics());
            builder.push(right.semantics());
            
            // Create UNION with all=true (ALL)
            builder.union(true, 2);
            
            return builder.build();
        }
    }
    
    /**
     * DISTINCT aggregate - groups by all fields to eliminate duplicates
     */
    public static record DistinctAggregate(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            // Group by all fields (creates DISTINCT effect)
            // For a 2-column table, group by field 0 and field 1
            var groupKey = builder.groupKey(builder.field(0), builder.field(1));
            
            // No aggregate functions needed - just grouping creates DISTINCT
            builder.aggregate(groupKey);
            
            return builder.build();
        }
    }
}