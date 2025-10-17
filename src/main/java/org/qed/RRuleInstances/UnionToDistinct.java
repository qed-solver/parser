package org.qed.RRuleInstances;

import org.apache.calcite.rel.RelNode;
import org.qed.RelRN;
import org.qed.RRule;
import org.qed.RuleBuilder;
import org.qed.RelType;
import kala.collection.Seq;
import kala.tuple.Tuple;
public record UnionToDistinct() implements RRule {

    static final RelRN leftTable = new LeftSourceTable();
    static final RelRN rightTable = new RightSourceTable();
    
    @Override
    public RelRN before() {
        return new DistinctUnion(leftTable, rightTable);
    }
    
    @Override
    public RelRN after() {
        var unionAll = new UnionAll(leftTable, rightTable);
        return new DistinctAggregate(unionAll);
    }

    public static record LeftSourceTable() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            var table = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("INTEGER", true), false),
                Tuple.of(RelType.fromString("VARCHAR", true), false)
            ));
            
            builder.addTable(table);
            return builder.scan(table.getName()).build();
        }
    }

    public static record RightSourceTable() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            var table = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("INTEGER", true), false),
                Tuple.of(RelType.fromString("VARCHAR", true), false)
            ));
            
            builder.addTable(table);
            return builder.scan(table.getName()).build();
        }
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
            var groupKey = builder.groupKey(builder.field(0), builder.field(1));
            builder.aggregate(groupKey);
            
            return builder.build();
        }
    }
}