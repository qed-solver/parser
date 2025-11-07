package org.qed.RRuleInstances;

import org.apache.calcite.rel.RelNode;
import org.qed.RelRN;
import org.qed.RRule;
import org.qed.RuleBuilder;

public record UnionToDistinct() implements RRule {

    static final RelRN left = RelRN.scan("Left", "Source_Type");
    static final RelRN right = RelRN.scan("Right", "Source_Type");
    
    @Override
    public RelRN before() {
        return new DistinctUnion(left, right);
    }
    
    @Override
    public RelRN after() {
        var unionAll = new UnionAll(left, right);
        return new DistinctAggregate(unionAll);
    }

    public static record DistinctUnion(RelRN left, RelRN right) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(left.semantics());
            builder.push(right.semantics());
            builder.union(false, 2);
            
            return builder.build();
        }
    }

    public static record UnionAll(RelRN left, RelRN right) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();

            builder.push(left.semantics());
            builder.push(right.semantics());

            builder.union(true, 2);
            
            return builder.build();
        }
    }

    public static record DistinctAggregate(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            // Group by all fields to remove duplicates
            var groupKey = builder.groupKey(builder.field(0));
            builder.aggregate(groupKey);
            
            return builder.build();
        }
    }
}