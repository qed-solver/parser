package org.qed.Generated.RRuleInstances;

import org.apache.calcite.rel.RelNode;
import org.qed.RelRN;
import org.qed.RRule;
import org.qed.RuleBuilder;
import org.qed.RelType;
import kala.collection.Seq;
import kala.tuple.Tuple;

public record AggregateProjectMerge() implements RRule {

    static final RelRN R = new MultiColumnRelation();
    
    @Override
    public RelRN before() {
        var projection = new FieldReferenceProject(R);
        return new SimpleAggregate(projection);
    }
    
    @Override
    public RelRN after() {
        return new SimpleAggregate(R);
    }

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

    public static record FieldReferenceProject(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            builder.project(builder.field(0));
            return builder.build();
        }
    }

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