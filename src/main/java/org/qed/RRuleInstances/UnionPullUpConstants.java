package org.qed.RRuleInstances;

import org.apache.calcite.rel.RelNode;
import org.qed.RelRN;
import org.qed.RRule;
import org.qed.RuleBuilder;
import org.qed.RelType;
import kala.collection.Seq;
import kala.tuple.Tuple;

public record UnionPullUpConstants() implements RRule {

    static final RelRN left = new SourceTable();
    static final RelRN right = new SourceTable();
    
    @Override
    public RelRN before() {
        var leftProjection = new LeftProjectionWithConstants(left);
        var rightProjection = new RightProjectionWithConstants(right);
        return new UnionWithConstantColumns(leftProjection, rightProjection);
    }
    
    @Override
    public RelRN after() {
        var leftProjectionReduced = new LeftProjectionNonConstants(left);
        var rightProjectionReduced = new RightProjectionNonConstants(right);
        var reducedUnion = new UnionReducedColumns(leftProjectionReduced, rightProjectionReduced);
        return new TopProjectionWithConstants(reducedUnion);
    }

    public static record SourceTable() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            var table = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("Source_Type", true), false),
                Tuple.of(RelType.fromString("Source_Type", true), false),
                Tuple.of(RelType.fromString("Source_Type", true), false)
            ));
            
            builder.addTable(table);
            return builder.scan(table.getName()).build();
        }
    }
    public static record LeftProjectionWithConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            builder.project(
                builder.field(0),
                builder.alias(builder.literal("ACTIVE"), "status"),
                builder.field(2)
            );
            
            return builder.build();
        }
    }

    public static record RightProjectionWithConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            builder.project(
                builder.field(0),
                builder.alias(builder.literal("ACTIVE"), "status"),
                builder.field(2)
            );
            
            return builder.build();
        }
    }

    public static record UnionWithConstantColumns(RelRN left, RelRN right) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            builder.push(left.semantics());
            builder.push(right.semantics());
            
            builder.union(true, 2);  // UNION ALL
            
            return builder.build();
        }
    }

    public static record LeftProjectionNonConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());

            builder.project(
                builder.field(0),
                builder.field(2)
            );
            
            return builder.build();
        }
    }

    public static record RightProjectionNonConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            builder.project(
                builder.field(0),
                builder.field(2)
            );
            
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

            builder.project(
                builder.field(0),
                builder.alias(builder.literal("ACTIVE"), "status"),
                builder.field(1)
            );
            
            return builder.build();
        }
    }
}