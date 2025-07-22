package org.qed.Generated.RRuleInstances;

import org.apache.calcite.rel.RelNode;
import org.qed.RelRN;
import org.qed.RRule;
import org.qed.RuleBuilder;
import org.qed.RelType;
import kala.collection.Seq;
import kala.tuple.Tuple;

/**
 * Abstract AggregateProjectMergeRule that represents valid transformations:
 * 
 * Aggregate(Project_with_field_references(R)) => Aggregate(R)
 * 
 * The project must contain only field references, not expressions.
 */
public record AggregateProjectMerge() implements RRule {
    
    // Multi-column base relation
    static final RelRN R = new MultiColumnRelation();
    
    @Override
    public RelRN before() {
        // Pattern: Aggregate over Project that selects/reorders fields
        var projection = new FieldReferenceProject(R);
        return new SimpleAggregate(projection);
    }
    
    @Override
    public RelRN after() {
        // Pattern: Same aggregate directly on base relation
        return new SimpleAggregate(R);
    }

    /**
     * Base relation with multiple columns
     */
    public static record MultiColumnRelation() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            var table = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("INTEGER", true), false),   // col0
                Tuple.of(RelType.fromString("INTEGER", true), false),   // col1
                Tuple.of(RelType.fromString("VARCHAR", true), false)    // col2
            ));
            
            builder.addTable(table);
            return builder.scan(table.getName()).build();
        }
    }
    
    /**
     * Project that contains ONLY field references (no expressions)
     * This represents field selection/reordering that can be eliminated
     */
    public static record FieldReferenceProject(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            // Project that selects only first 2 fields (eliminates 3rd)
            // This is pure field selection, not function application
            builder.project(
                builder.field(0)    // $f0 = $0 (field reference)
            );
            
            return builder.build();
        }
    }
    
    /**
     * Simple aggregate: GROUP BY first field, COUNT(*)
     */
    public static record SimpleAggregate(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            var groupKey = builder.groupKey(builder.field(0));
            var countCall = builder.count();
            builder.aggregate(groupKey, countCall);
            
            return builder.build();
        }
    }
}