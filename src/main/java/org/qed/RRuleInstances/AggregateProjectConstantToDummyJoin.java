package org.qed.RRuleInstances;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RelRN;
import org.qed.RRule;
import org.qed.RuleBuilder;
import org.qed.RelType;
import kala.collection.Seq;
import kala.tuple.Tuple;

public record AggregateProjectConstantToDummyJoin() implements RRule {

    static final RelRN source = new SourceTable();
    
    @Override
    public RelRN before() {
        var projectWithConstants = new ProjectWithConstantLiterals(source);
        return new AggregateGroupingByConstants(projectWithConstants);
    }
    
    @Override
    public RelRN after() {
        var dummyTable = new DummyConstantsTable();
        var joinWithDummy = new JoinWithDummyTable(source, dummyTable);
        var projectWithDummyFields = new ProjectWithDummyFields(joinWithDummy);
        return new AggregateGroupingByDummyFields(projectWithDummyFields);
    }

    public static record SourceTable() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            var table = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("Source_Type", true), false),
                Tuple.of(RelType.fromString("Source_Type", true), false)
            ));
            
            builder.addTable(table);
            return builder.scan(table.getName()).build();
        }
    }

    public static record ProjectWithConstantLiterals(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            builder.project(
                builder.field(0),
                builder.literal(true),
                builder.literal("2024"),
                builder.field(1)
            );
            
            return builder.build();
        }
    }

    public static record AggregateGroupingByConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());

            var groupKey = builder.groupKey(
                builder.field(1),
                builder.field(2),
                builder.field(0)
            );

            var agg = builder.avg(builder.field(3));
            
            builder.aggregate(groupKey, agg);
            return builder.build();
        }
    }

    public static record DummyConstantsTable() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();

            builder.values(
                new String[]{"col0", "col1"},
                true,
                "2024"
            );
            
            return builder.build();
        }
    }

    public static record JoinWithDummyTable(RelRN baseTable, RelRN dummyTable) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            builder.push(baseTable.semantics());
            builder.push(dummyTable.semantics());

            builder.join(JoinRelType.INNER, builder.literal(true));
            
            return builder.build();
        }
    }

    public static record ProjectWithDummyFields(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());

            builder.project(
                builder.field(0),
                builder.field(2),
                builder.field(3),
                builder.field(1)
            );
            
            return builder.build();
        }
    }
    public static record AggregateGroupingByDummyFields(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());

            var groupKey = builder.groupKey(
                builder.field(1),
                builder.field(2),
                builder.field(0)
            );

            var agg = builder.avg(builder.field(3));
            
            builder.aggregate(groupKey, agg);
            return builder.build();
        }
    }
}