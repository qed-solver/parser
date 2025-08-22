package org.qed.Generated.RRuleInstances;

import org.apache.calcite.rel.RelNode;
import org.qed.RelRN;
import org.qed.RRule;
import org.qed.RuleBuilder;
import org.qed.RelType;
import kala.collection.Seq;
import kala.tuple.Tuple;

public record UnionPullUpConstants() implements RRule {

    static final RelRN leftTable = new LeftTableWithConstants();
    static final RelRN rightTable = new RightTableWithConstants();
    
    @Override
    public RelRN before() {
        var leftProjection = new LeftProjectionWithConstants(leftTable);
        var rightProjection = new RightProjectionWithConstants(rightTable);
        return new UnionWithConstantColumns(leftProjection, rightProjection);
    }
    
    @Override
    public RelRN after() {
        var leftProjectionReduced = new LeftProjectionNonConstants(leftTable);
        var rightProjectionReduced = new RightProjectionNonConstants(rightTable);
        var reducedUnion = new UnionReducedColumns(leftProjectionReduced, rightProjectionReduced);
        return new TopProjectionWithConstants(reducedUnion);
    }

    public static record LeftTableWithConstants() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            var table = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false), Tuple.of(RelType.fromString("VARCHAR", true), false), Tuple.of(RelType.fromString("INTEGER", true), false)));
            
            builder.addTable(table);
            return builder.scan(table.getName()).build();
        }
    }
    

    public static record RightTableWithConstants() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            var table = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false), Tuple.of(RelType.fromString("VARCHAR", true), false), Tuple.of(RelType.fromString("INTEGER", true), false)));
            
            builder.addTable(table);
            return builder.scan(table.getName()).build();
        }
    }
    public static record LeftProjectionWithConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            builder.project(builder.field(0), builder.alias(builder.literal("ACTIVE"), "status"), builder.field(2));
            
            return builder.build();
        }
    }


    public static record RightProjectionWithConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            builder.project(builder.field(0), builder.alias(builder.literal("ACTIVE"), "status"), builder.field(2));
            
            return builder.build();
        }
    }

    public static record UnionWithConstantColumns(RelRN left, RelRN right) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            builder.push(left.semantics());
            builder.push(right.semantics());
            
            builder.union(true, 2);
            
            return builder.build();
        }
    }

    public static record LeftProjectionNonConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());

            builder.project(builder.field(0), builder.field(2));
            
            return builder.build();
        }
    }

    public static record RightProjectionNonConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());

            builder.project(builder.field(0), builder.field(2));
            
            return builder.build();
        }
    }

    public static record UnionReducedColumns(RelRN left, RelRN right) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            builder.push(left.semantics());
            builder.push(right.semantics());
            
            builder.union(true, 2);
            
            return builder.build();
        }
    }

    public static record TopProjectionWithConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());

            builder.project(builder.field(0), builder.alias(builder.literal("ACTIVE"), "status"), builder.field(1));
            
            return builder.build();
        }
    }
}